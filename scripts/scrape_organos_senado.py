"""
Scraper de Mesa Directiva + Junta de Coordinación Política del Senado (LXVI).

Fuente:
  Mesa Directiva: https://www.senado.gob.mx/66/mesa_directiva/
  JUCOPO:        https://www.senado.gob.mx/66/junta_de_coordinacion_politica/

Patrón HTML descubierto:
  <img src="/66/images/senadores/66/{sitl_id}-{nombre_url}-...jpg">
  <h3>CARGO</h3>          ← el cargo va DESPUÉS de la img

Match con BD `legisladores`:
  - Por SITL ID si está disponible (preferido — preciso)
  - Fallback a nombre normalizado

Inserta en la tabla `legisladores_organos_gobierno` (compartida con Diputados).
Sync a D1 al final.

Uso:
  python scripts/scrape_organos_senado.py
  python scripts/scrape_organos_senado.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import subprocess
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

URL_MESA = "https://www.senado.gob.mx/66/mesa_directiva/"
URL_JUCOPO = "https://www.senado.gob.mx/66/junta_de_coordinacion_politica/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "es-MX,es;q=0.9",
}

# Cargos esperados en H3
RE_CARGO_MESA = re.compile(r"^(PRESIDENCIA|VICEPRESIDENCIA|SECRETAR[ÍI]A)\s*$", re.I)
RE_CARGO_JUCOPO = re.compile(
    r"^(PRESIDENCIA|COORDINADOR|GRUPO PARLAMENTARIO)", re.I
)


def normalizar(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()


def extraer_de_pagina(html, url, organo):
    """Devuelve lista de {sitl_id, nombre, cargo}."""
    soup = BeautifulSoup(html, "html.parser")
    items = []
    # Recorrer todos los IMG de senadores en orden, y para cada uno buscar el
    # SIGUIENTE H3 que matchea cargo válido.
    for img in soup.find_all("img", src=re.compile(r"/senadores/\d+/\d+-")):
        src = img["src"]
        m = re.search(r"/(\d+)-([a-z_]+)", src)
        if not m:
            continue
        sitl_id = int(m.group(1))
        nombre_url = m.group(2).replace("_", " ")
        # Buscar el siguiente H3 con cargo
        cargo = None
        siguiente = img.find_next(["h3", "h4"])
        # A veces hay un H3 más arriba si la primera card no tiene H3 después;
        # entonces probar también el inmediato anterior en una ventana pequeña
        if siguiente:
            txt = siguiente.get_text(strip=True)
            if organo == 'mesa_directiva' and RE_CARGO_MESA.match(txt):
                cargo = _normalizar_cargo_mesa(txt)
            elif organo == 'jucopo' and RE_CARGO_JUCOPO.match(txt):
                cargo = _normalizar_cargo_jucopo(txt)
        if not cargo:
            # Fallback: buscar H3 anterior cercano (ventana 5 líneas)
            anterior = img.find_previous(["h3", "h4"])
            if anterior:
                txt = anterior.get_text(strip=True)
                if organo == 'mesa_directiva' and RE_CARGO_MESA.match(txt):
                    cargo = _normalizar_cargo_mesa(txt)
                elif organo == 'jucopo' and RE_CARGO_JUCOPO.match(txt):
                    cargo = _normalizar_cargo_jucopo(txt)
        if not cargo:
            logger.warning(f"  Sin cargo para sitl={sitl_id} {nombre_url}")
            continue
        items.append({
            "sitl_id": sitl_id,
            "nombre_url": nombre_url,
            "cargo": cargo,
        })
    return items


def _normalizar_cargo_mesa(txt):
    t = txt.strip().upper()
    if t == "PRESIDENCIA": return "Presidencia"
    if t == "VICEPRESIDENCIA": return "Vicepresidencia"
    if t in ("SECRETARÍA", "SECRETARIA"): return "Secretaría"
    return txt.strip().title()


def _normalizar_cargo_jucopo(txt):
    t = txt.strip().upper()
    if t.startswith("PRESIDENCIA"): return "Presidencia"
    if "COORDINADOR" in t: return "Coordinador"
    if "GRUPO PARLAMENTARIO" in t: return "Coordinador"   # listing por bancada
    return txt.strip().title()


# ── Match con BD ─────────────────────────────────────────────────────────────
def construir_indice_senado(conn):
    """Devuelve dict por sitl_id si está disponible y por tokens."""
    rows = conn.execute("""
        SELECT id, nombre, sitl_id FROM legisladores
        WHERE camara LIKE '%enado%'
    """).fetchall()
    by_sitl = {}
    by_tokens = {}
    for leg_id, nombre, sitl_id in rows:
        if sitl_id:
            try: by_sitl[int(sitl_id)] = leg_id
            except: pass
        toks = tokens_relevantes(nombre)
        by_tokens[leg_id] = toks
    return by_sitl, by_tokens


def tokens_relevantes(nombre):
    n = normalizar(nombre)
    n = re.sub(r"[^a-z0-9 ]", " ", n)
    return [t for t in n.split() if len(t) >= 3 and t not in {
        "de","del","la","los","las","y","san","santa"
    }]


def matchear(item, by_sitl, by_tokens):
    """Match por sitl_id (preferido) o tokens."""
    if item["sitl_id"] in by_sitl:
        return by_sitl[item["sitl_id"]]
    # Fallback: tokens
    qtoks = set(tokens_relevantes(item["nombre_url"]))
    if not qtoks: return None
    best, best_overlap = None, 0
    for leg_id, toks in by_tokens.items():
        overlap = len(qtoks & set(toks))
        if overlap > best_overlap and overlap >= 2:
            best, best_overlap = leg_id, overlap
    return best


# ── Persistencia ─────────────────────────────────────────────────────────────
def upsert(conn, items_por_organo, dry_run=False):
    by_sitl, by_tokens = construir_indice_senado(conn)
    # Verificar si la tabla existe (la creó scrape_organos_diputados.py)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS legisladores_organos_gobierno (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            legislador_id INTEGER NOT NULL,
            organo TEXT NOT NULL,
            rol TEXT NOT NULL,
            fuente_oid TEXT,
            actualizado_en TEXT NOT NULL,
            UNIQUE(legislador_id, organo)
        )
    """)
    ahora = datetime.utcnow().isoformat(timespec='seconds')
    matched, no_match = 0, []

    # Eliminar entradas previas de Senado (las de Diputados se conservan)
    if not dry_run:
        conn.execute("""
            DELETE FROM legisladores_organos_gobierno
            WHERE organo IN ('mesa_directiva_senado', 'jucopo_senado')
               OR (organo IN ('mesa_directiva','jucopo')
                   AND legislador_id IN (
                       SELECT id FROM legisladores WHERE camara LIKE '%enado%'
                   ))
        """)

    for organo, items in items_por_organo.items():
        for idx, it in enumerate(items):
            leg_id = matchear(it, by_sitl, by_tokens)
            if not leg_id:
                no_match.append((organo, it))
                continue
            matched += 1
            # Para Senado uso el sufijo _senado para distinguir de Diputados
            organo_full = f"{organo}_senado"
            cargo_final = it['cargo']
            # Heurística: en JUCOPO Senado el primer listado es el Presidente
            # actual del órgano (rota entre coordinadores). Asignar Presidencia.
            if organo == 'jucopo' and idx == 0 and cargo_final == 'Coordinador':
                cargo_final = 'Presidencia'
            if dry_run:
                logger.info(f"  [{organo}] {it['nombre_url']} (sitl={it['sitl_id']}) → leg_id={leg_id} cargo={cargo_final}")
                continue
            conn.execute("""
                INSERT INTO legisladores_organos_gobierno
                  (legislador_id, organo, rol, fuente_oid, actualizado_en)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(legislador_id, organo) DO UPDATE SET
                  rol = excluded.rol, actualizado_en = excluded.actualizado_en
            """, (leg_id, organo_full, cargo_final, str(it['sitl_id']), ahora))

    if not dry_run:
        conn.commit()
    logger.info(f"Match: {matched} OK | {len(no_match)} sin match")
    for org, it in no_match:
        logger.warning(f"  Sin match [{org}] sitl={it['sitl_id']} {it['nombre_url']}")
    return matched


def sync_d1(conn):
    """Empuja toda la tabla a D1 (incluye Diputados ya cargados)."""
    rows = conn.execute("""
        SELECT legislador_id, organo, rol, fuente_oid, actualizado_en
        FROM legisladores_organos_gobierno
    """).fetchall()
    if not rows:
        return False
    sql = ["DELETE FROM legisladores_organos_gobierno;"]
    def esc(s):
        return "'" + str(s).replace("'", "''") + "'" if s is not None else 'NULL'
    for r in rows:
        sql.append(
            f"INSERT INTO legisladores_organos_gobierno "
            f"(legislador_id, organo, rol, fuente_oid, actualizado_en) "
            f"VALUES ({r[0]}, {esc(r[1])}, {esc(r[2])}, {esc(r[3])}, {esc(r[4])});"
        )
    full = "\n".join(sql)
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
        f.write(full)
        p = f.name
    try:
        r = subprocess.run(
            ["npx", "wrangler", "d1", "execute", "fiat-busqueda",
             "--file", p, "--remote"],
            capture_output=True, text=True, timeout=60,
            cwd=str(ROOT / "worker"),
        )
        if r.returncode == 0:
            logger.info(f"Sync D1 OK ({len(rows)} filas totales)")
            return True
        logger.error(f"Sync D1 falló: {r.stderr[:300]}")
        return False
    finally:
        Path(p).unlink(missing_ok=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-sync-d1", action="store_true")
    args = p.parse_args()

    sess = requests.Session()
    sess.headers.update(HEADERS)

    items = {}
    for url, key in [(URL_MESA, "mesa_directiva"), (URL_JUCOPO, "jucopo")]:
        logger.info(f"Fetch {key}: {url}")
        r = sess.get(url, timeout=30, verify=False)
        if r.status_code != 200:
            logger.error(f"  HTTP {r.status_code}")
            continue
        items[key] = extraer_de_pagina(r.text, url, key)
        logger.info(f"  Extraídos: {len(items[key])}")

    conn = sqlite3.connect(str(DB))
    upsert(conn, items, dry_run=args.dry_run)

    if not args.dry_run and not args.no_sync_d1:
        sync_d1(conn)

    if not args.dry_run:
        for organo in ('mesa_directiva_senado', 'jucopo_senado'):
            rows = conn.execute("""
                SELECT l.nombre, l.partido, o.rol
                  FROM legisladores_organos_gobierno o
                  JOIN legisladores l ON l.id = o.legislador_id
                 WHERE o.organo = ?
                 ORDER BY o.rol, l.nombre
            """, (organo,)).fetchall()
            print(f"\n=== {organo.upper().replace('_', ' ')} ===")
            for n, par, rol in rows:
                print(f"  {rol:18} {n} ({par or '—'})")


if __name__ == "__main__":
    main()
