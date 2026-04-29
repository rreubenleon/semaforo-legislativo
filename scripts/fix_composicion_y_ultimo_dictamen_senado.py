"""
Fix urgente para comisiones del Senado:

PROBLEMA 1 — Composición incompleta:
  comisiones_stats.composicion en D1 trae solo 2-9 miembros para
  comisiones que tienen 15-30+ integrantes reales. Causa: el viejo
  scraper backfill_comisiones_senado.py falló por Incapsula para
  varias comisiones y dejó datos parciales.

PROBLEMA 2 — Último dictamen desactualizado:
  comisiones_stats.ultimo_dictamen_url apunta a marzo cuando hoy
  hay dictámenes nuevos. Causa: viene del scraping de Gaceta Senado
  que clasifica mal por comisión.

SOLUCIÓN:
  1. Reconstruir 'composicion' por comisión Senado desde la BD local
     legisladores.comisiones_cargo (que ya está limpio post-refresh).
  2. Scrapear el último dictamen REAL desde
     www.senado.gob.mx/64/frames/asuntosComisiones.php
       ?idComision={ID}&tipoAsunto=dictamenes
     (mismo iframe oculto que ya usamos para iniciativas).

Sync a D1 al final.

Uso:
  python scripts/fix_composicion_y_ultimo_dictamen_senado.py
  python scripts/fix_composicion_y_ultimo_dictamen_senado.py --dry-run
  python scripts/fix_composicion_y_ultimo_dictamen_senado.py --solo-comisiones=Salud,Medio
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib3
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
sys.path.insert(0, str(ROOT))

from scrapers.comisiones_senado_asuntos import CATALOGO_LXVI, _slug_referer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

BASE = "https://www.senado.gob.mx"
FRAME_URL = f"{BASE}/64/frames/asuntosComisiones.php"
COMISIONES_HOME = "https://comisiones.senado.gob.mx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "es-MX,es;q=0.9",
}

CARGO_RANK = {"Presidente": 0, "Secretaría": 1, "Integrante": 2}

# Meses para parsear "Martes 28 de abril de 2026"
MESES = {
    'enero':1,'febrero':2,'marzo':3,'abril':4,'mayo':5,'junio':6,
    'julio':7,'agosto':8,'septiembre':9,'octubre':10,'noviembre':11,'diciembre':12
}


def construir_composicion_desde_bd(conn):
    """
    Lee legisladores.comisiones_cargo (ya limpio) y arma:
      {nombre_comision: [{'nombre','partido','cargo'}, ...]}
    para senadores activos.
    """
    rows = conn.execute("""
        SELECT nombre, partido, comisiones_cargo
          FROM legisladores
         WHERE camara LIKE '%enado%'
           AND comisiones_cargo IS NOT NULL AND comisiones_cargo <> ''
    """).fetchall()
    out = {}
    for nombre, partido, cargo in rows:
        for parte in cargo.split('|'):
            if ':' not in parte:
                continue
            com, rol = parte.split(':', 1)
            com = com.strip()
            rol = rol.strip()
            if not com or com.upper() == 'A LAS QUE PERTENECE':
                continue
            # Normalizar rol
            rol_norm = 'Presidente' if 'presiden' in rol.lower() else \
                       'Secretaría' if 'secretar' in rol.lower() else \
                       'Integrante'
            out.setdefault(com, []).append({
                'nombre': nombre,
                'partido': partido or '',
                'cargo': rol_norm,
            })
    # Ordenar: Presidente primero, luego Secretarías, luego Integrantes (alfabético)
    for com in out:
        out[com].sort(key=lambda m: (CARGO_RANK.get(m['cargo'], 99), m['nombre']))
    return out


def parsear_fecha(texto_fecha):
    """'Martes 28 de abril de 2026' → datetime.date"""
    m = re.search(r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', (texto_fecha or '').lower())
    if not m: return None
    dia, mes_nombre, anio = m.groups()
    mes = MESES.get(mes_nombre)
    if not mes: return None
    try:
        return datetime(int(anio), mes, int(dia)).date()
    except: return None


def warmup(sess):
    try:
        sess.get(f'{COMISIONES_HOME}/agricultura/iniciativas', timeout=30, verify=False)
        return True
    except: return False


def fetch_ultimo_dictamen(sess, id_com, slug):
    """Devuelve (fecha_dict, url_doc, titulo) del más reciente o (None, None, None)."""
    sess.headers['Referer'] = f'{COMISIONES_HOME}/{slug}/dictamenes'
    try:
        r = sess.get(f"{FRAME_URL}?idComision={id_com}&tipoAsunto=dictamenes",
                     timeout=30, verify=False)
        if '<TITLE>Loading' in r.text:
            return (None, None, None, 'BLOCKED')
    except Exception as e:
        return (None, None, None, f'ERR:{e}')

    soup = BeautifulSoup(r.text, 'html.parser')
    # Cada dictamen es un panel; tomar el primero (más reciente)
    paneles = soup.find_all('div', class_='panel-default')
    # Filtrar paneles que tengan "Publicado en Gaceta"
    candidatos = []
    for p in paneles:
        txt = p.get_text(' ', strip=True)
        m_fecha = re.search(r'Publicado en Gaceta:\s*([^.]{5,80})', txt)
        if not m_fecha:
            continue
        fecha = parsear_fecha(m_fecha.group(1))
        if not fecha:
            continue
        # URL al expediente
        link_exp = p.find('a', href=re.compile(r'expedientes-legislativos-digitales|gaceta_del_senado/documento'))
        url = link_exp.get('href') if link_exp else ''
        if url and not url.startswith('http'):
            url = BASE + url
        # Título (primer enlace a documento que tenga texto largo)
        titulo = ''
        for a in p.find_all('a'):
            t = a.get_text(' ', strip=True)
            if len(t) > 30:
                titulo = t[:200]
                break
        candidatos.append((fecha, url, titulo))
    if not candidatos:
        return (None, None, None, 'OK_VACIO')
    candidatos.sort(reverse=True)
    f, u, t = candidatos[0]
    return (f, u, t, 'OK')


def sync_d1(filas):
    """filas = lista de (id_d1_lookup_nombre, ultimo_dictamen, ultimo_dictamen_url, dias_sin_dictamen, composicion_json)"""
    if not filas:
        return False
    def esc(s):
        if s is None: return 'NULL'
        if isinstance(s, (int, float)): return str(s)
        return "'" + str(s).replace("'", "''") + "'"
    sqls = []
    for nombre, ultimo, url, dias, comp_json in filas:
        sqls.append(
            f"UPDATE comisiones_stats SET "
            f"composicion={esc(comp_json)}, "
            f"total_integrantes={len(json.loads(comp_json) or [])}, "
            f"ultimo_dictamen={esc(ultimo)}, "
            f"ultimo_dictamen_url={esc(url)}, "
            f"dias_sin_dictamen={esc(dias)} "
            f"WHERE nombre={esc(nombre)} AND camara='Senado';"
        )
    BATCH = 25
    ok_total = 0
    for i in range(0, len(sqls), BATCH):
        batch = sqls[i:i+BATCH]
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
            f.write("\n".join(batch))
            sql_file = f.name
        try:
            r = subprocess.run(
                ["npx", "wrangler", "d1", "execute", "fiat-busqueda",
                 "--file", sql_file, "--remote"],
                capture_output=True, text=True, timeout=60,
                cwd=str(ROOT / "worker"),
            )
            if r.returncode == 0:
                ok_total += len(batch)
                logger.info(f"  Batch {i//BATCH+1}: {len(batch)} updates OK")
            else:
                logger.error(f"  Batch {i//BATCH+1} falló: {r.stderr[:300]}")
        finally:
            Path(sql_file).unlink(missing_ok=True)
    return ok_total


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--solo-comisiones", default="", help="Substring filter, comma-separated")
    p.add_argument("--delay", type=float, default=6.0)
    args = p.parse_args()

    conn = sqlite3.connect(str(DB))
    composiciones = construir_composicion_desde_bd(conn)
    logger.info(f"Composiciones reconstruidas desde BD local: {len(composiciones)} comisiones")

    # Iterar catálogo Senado
    sess = requests.Session()
    sess.headers.update(HEADERS)
    if not warmup(sess):
        logger.warning("Warmup falló, continuando con cuidado")
    time.sleep(args.delay)

    filtros = [s.strip().lower() for s in args.solo_comisiones.split(',') if s.strip()]
    hoy = datetime.now().date()
    filas_d1 = []

    for id_com, nombre in sorted(CATALOGO_LXVI.items()):
        if filtros and not any(f in nombre.lower() for f in filtros):
            continue

        slug = _slug_referer(nombre)
        comp = composiciones.get(nombre, [])
        # Para distintos formatos en BD (con/sin acentos), buscar también "Salud" en otros nombres
        if not comp:
            # Intento alternativo: matchear por substring
            for k, v in composiciones.items():
                if nombre.lower() in k.lower() or k.lower() in nombre.lower():
                    comp = v
                    break

        # Scrape último dictamen
        f_dict, url_dict, tit, status = fetch_ultimo_dictamen(sess, id_com, slug)
        dias = (hoy - f_dict).days if f_dict else None
        ultimo_str = f_dict.isoformat() if f_dict else None
        comp_json = json.dumps(comp, ensure_ascii=False)

        logger.info(
            f"  [{id_com}] {nombre[:40]:40} composicion={len(comp):3} | "
            f"último_dict={ultimo_str or '—'} ({dias if dias is not None else '?'}d) [{status}]"
        )

        if args.dry_run:
            continue

        filas_d1.append((nombre, ultimo_str, url_dict, dias, comp_json))
        time.sleep(args.delay)

    if not args.dry_run and filas_d1:
        logger.info(f"\nSync D1 ({len(filas_d1)} comisiones)...")
        sync_d1(filas_d1)

    print(f"\nResumen:")
    con_pres = sum(1 for v in composiciones.values()
                   if any(m['cargo']=='Presidente' for m in v))
    print(f"  Comisiones con presidente identificado: {con_pres}/{len(composiciones)}")


if __name__ == "__main__":
    main()
