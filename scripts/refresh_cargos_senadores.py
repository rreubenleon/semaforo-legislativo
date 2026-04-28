"""
Fix para los senadores que `backfill_comisiones_senado.py` dejó sin
`comisiones_cargo`.

PROBLEMA: el scraper original recorre desde la perspectiva de cada
COMISIÓN. Si una falla por Incapsula, sus integrantes quedan sin cargo.
58 de 128 senadores quedaron así (incluida Maki Ortiz que preside Med.
Amb. Senado).

SOLUCIÓN: scrapear desde la perspectiva del SENADOR. Cada perfil
/66/senador/{sitl_id} tiene la lista COMPLETA de sus comisiones con
roles en una sola página, formato:
  COMISIONES
  Presidente(a): Comisión X
  Secretario(a): Comisión Y, Comisión Z
  Integrante: Comisión A, Comisión B, ...

Mucho más robusto porque cada request es del mismo tipo y no depende de
varias páginas distintas.

Uso:
  python scripts/refresh_cargos_senadores.py --solo-vacios   (default)
  python scripts/refresh_cargos_senadores.py --todos
  python scripts/refresh_cargos_senadores.py --dry-run --limit 5
"""
from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

BASE = "https://www.senado.gob.mx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "es-MX,es;q=0.9",
}

# Catálogo de comisiones del Senado LXVI (ordinarias + especiales).
# Importado desde el scraper de asuntos para no duplicar.
try:
    from scrapers.comisiones_senado_asuntos import CATALOGO_LXVI
    CATALOGO_NOMBRES = list(CATALOGO_LXVI.values())
except Exception:
    CATALOGO_NOMBRES = []

# Cargos en BD usa formato 'Presidente', 'Secretaría', 'Integrante'
CARGO_NORM = {
    "Presidente(a)": "Presidente",
    "Secretario(a)": "Secretaría",
    "Integrante": "Integrante",
}

# Patrón cargos que delimitan bloques en el texto
RE_CARGO_LABEL = re.compile(r"(Presidente\(a\)|Secretario\(a\)|Integrante)\s*:")


def extraer_comisiones_de_perfil(html: str) -> list[tuple[str, str]]:
    """Devuelve lista de (comision, cargo) parseando la sección COMISIONES
    del perfil del senador. La página concatena los nombres de comisiones
    sin separador, por eso usamos el catálogo LXVI para tokenizar."""
    soup = BeautifulSoup(html, "html.parser")
    texto = soup.get_text(" ", strip=True)

    # Buscar todas las apariciones de los marcadores de cargo y construir
    # bloques: (cargo, texto_hasta_siguiente_marcador_o_fin)
    matches = list(RE_CARGO_LABEL.finditer(texto))
    if not matches:
        return []

    # Encontrar el fin natural de la sección: cuando aparece "Informes",
    # "Iniciativas", "Intervenciones", "Trabajos legislativos" tras los matches
    fin_seccion = len(texto)
    for token in ("Informes de Actividades", "Iniciativas legislativas",
                  "Intervenciones", "Trabajos legislativos", "INFORME DE ACTIVIDADES",
                  "Trayectoria"):
        idx = texto.find(token, matches[0].start())
        if idx > 0 and idx < fin_seccion:
            fin_seccion = idx

    # Extraer bloque por cargo
    bloques = []
    for i, m in enumerate(matches):
        cargo_raw = m.group(1)
        ini = m.end()
        fin = matches[i + 1].start() if i + 1 < len(matches) else fin_seccion
        contenido = texto[ini:fin].strip()
        bloques.append((CARGO_NORM.get(cargo_raw, cargo_raw), contenido))

    items = []
    for cargo, contenido in bloques:
        # Tokenizar contra el catálogo: encontrar TODAS las comisiones del
        # catálogo que aparecen en el contenido, en orden de aparición.
        encontradas = []
        for nombre_com in CATALOGO_NOMBRES:
            if not nombre_com:
                continue
            # Match case-insensitive con nombre completo
            pos = contenido.lower().find(nombre_com.lower())
            if pos >= 0:
                encontradas.append((pos, nombre_com))
        encontradas.sort()
        for _, nombre_com in encontradas:
            items.append((nombre_com, cargo))

        # Fallback: si NO encontramos nada del catálogo Y el cargo es
        # Presidente o Secretario (que típicamente tienen 1 sola comisión),
        # tomar el contenido crudo como nombre.
        if not encontradas and cargo in ('Presidente', 'Secretaría'):
            limpio = re.sub(r"\s+", " ", contenido).strip()
            # Cortar en una longitud razonable
            limpio = limpio[:90].strip()
            if 4 <= len(limpio):
                items.append((limpio, cargo))

    return items


def listar_senadores(conn, solo_vacios: bool, limit: int = None):
    where = "camara LIKE '%enado%' AND sitl_id IS NOT NULL AND sitl_id <> ''"
    if solo_vacios:
        where += " AND (comisiones_cargo IS NULL OR comisiones_cargo = '')"
    sql = f"SELECT id, nombre, sitl_id FROM legisladores WHERE {where} ORDER BY id"
    if limit:
        sql += f" LIMIT {int(limit)}"
    return conn.execute(sql).fetchall()


def fetch_perfil(sess, sitl_id, intentos=2):
    url = f"{BASE}/66/senador/{sitl_id}"
    for i in range(intentos):
        try:
            r = sess.get(url, timeout=30, verify=False)
            if r.status_code == 200 and len(r.text) > 5000:
                # Detectar Incapsula stub
                if "<TITLE>Loading" in r.text or "iframe" in r.text[:300]:
                    time.sleep(15)
                    continue
                return r.text
        except Exception as e:
            logger.warning(f"  HTTP error sitl={sitl_id}: {e}")
            time.sleep(5)
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--solo-vacios", action="store_true", default=True)
    p.add_argument("--todos", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--delay", type=float, default=4.0)
    args = p.parse_args()

    if args.todos:
        args.solo_vacios = False

    conn = sqlite3.connect(str(DB))
    sens = listar_senadores(conn, args.solo_vacios, args.limit)
    logger.info(f"A procesar: {len(sens)} senadores"
                f" {'(solo vacíos)' if args.solo_vacios else '(todos)'}")

    sess = requests.Session()
    sess.headers.update(HEADERS)
    # Warm-up
    sess.get(f"{BASE}/", timeout=30, verify=False)
    time.sleep(3)

    ahora = datetime.utcnow().isoformat(timespec='seconds')
    actualizados = 0
    sin_comisiones = 0

    for i, (leg_id, nombre, sitl_id) in enumerate(sens, 1):
        logger.info(f"[{i}/{len(sens)}] sitl={sitl_id} {nombre[:50]}")
        html = fetch_perfil(sess, sitl_id)
        if not html:
            logger.warning(f"  Sin HTML")
            time.sleep(args.delay)
            continue
        items = extraer_comisiones_de_perfil(html)
        if not items:
            sin_comisiones += 1
            logger.warning(f"  Sin comisiones detectadas")
            time.sleep(args.delay)
            continue

        # Construir comisiones_cargo en formato "Comisión:Cargo|Comisión:Cargo"
        cc_str = "|".join(f"{c}:{r}" for c, r in items)
        com_lista = ", ".join(c for c, _ in items)
        for c, r in items[:5]:
            logger.info(f"  · {r}: {c[:60]}")

        if not args.dry_run:
            conn.execute("""
                UPDATE legisladores
                   SET comisiones_cargo = ?, comisiones = ?
                 WHERE id = ?
            """, (cc_str, com_lista, leg_id))
            conn.commit()
        actualizados += 1
        time.sleep(args.delay)

    logger.info(f"\nActualizados: {actualizados}/{len(sens)}")
    logger.info(f"Sin comisiones detectadas: {sin_comisiones}")


if __name__ == "__main__":
    main()
