"""
Scraper de Mesa Directiva + JUCOPO de la Cámara de Diputados (LXVI).

Fuente: GraphQL endpoint del SPA web.diputados.gob.mx
  POST https://micrositios.diputados.gob.mx:4001/graphql
  Queries: allDiputadosMesaDirectiva(Legislacion:"LXVI")
           allDiputadosJucopo(Legislacion:"LXVI")

Lo que se extrae por persona:
  - Rol institucional: Presidencia / Vicepresidencia / Secretaría / Coordinador
  - Órgano: 'mesa_directiva' o 'jucopo'

Match con BD `legisladores`:
  - Comparación por nombre normalizado usando utils/matcher.py si existe,
    fallback a multiset de tokens

Salida:
  - Tabla nueva `legisladores_organos_gobierno` (legislador_id, organo, rol, fuente_oid, ts)
  - UNIQUE(legislador_id, organo) → un legislador puede estar en mesa Y jucopo
  - Sync a D1 al final

Uso:
  python scripts/scrape_organos_diputados.py
  python scripts/scrape_organos_diputados.py --dry-run
  python scripts/scrape_organos_diputados.py --no-sync-d1
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

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

GRAPHQL_URL = "https://micrositios.diputados.gob.mx:4001/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

QUERY_MESA_JUCOPO = """
{
  allDiputadosMesaDirectiva(Legislacion: "LXVI") {
    Oid NombreCompleto Nombre Rol Estado Distrito Suplente
  }
  allDiputadosJucopo(Legislacion: "LXVI") {
    Oid NombreCompleto Nombre Rol Estado Distrito Suplente
  }
}
"""


def fetch_organos():
    """Llama al endpoint GraphQL y devuelve dict {organo: [items]}."""
    r = requests.post(GRAPHQL_URL, headers=HEADERS,
                      data=json.dumps({"query": QUERY_MESA_JUCOPO}),
                      timeout=30)
    r.raise_for_status()
    data = r.json().get("data", {})
    return {
        "mesa_directiva": data.get("allDiputadosMesaDirectiva", []) or [],
        "jucopo":         data.get("allDiputadosJucopo", []) or [],
    }


# ── Matcher de nombres (estilo utils/matcher si existe, fallback simple) ─────
_PARTICULAS = {"de", "del", "la", "los", "las", "y", "san", "santa"}


def normalizar(nombre):
    """sin acentos, lowercase, una sola pasada"""
    s = unicodedata.normalize("NFKD", nombre or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()


def tokens_relevantes(nombre):
    """Lista de tokens >=3 chars sin partículas."""
    n = normalizar(nombre)
    n = re.sub(r"[^a-z0-9 ]", " ", n)
    return [t for t in n.split() if len(t) >= 3 and t not in _PARTICULAS]


def matchear_legislador(nombre_organo, indice_legisladores):
    """
    `nombre_organo` viene como 'López Rabadán Kenia' (apellido apellido nombre).
    `indice_legisladores` es {leg_id: [tokens_normalizados]}.
    Retorna leg_id o None.
    """
    tokens_q = set(tokens_relevantes(nombre_organo))
    if not tokens_q:
        return None
    # Match por overlap: legislador con MAYOR coincidencia de tokens >= 3
    best, best_overlap = None, 0
    for leg_id, tokens_l in indice_legisladores.items():
        overlap = len(tokens_q & set(tokens_l))
        if overlap > best_overlap and overlap >= 3:
            best, best_overlap = leg_id, overlap
    return best


def construir_indice(conn):
    """Devuelve {leg_id: [tokens_normalizados]} solo de Diputados LXVI."""
    rows = conn.execute("""
        SELECT id, nombre FROM legisladores
        WHERE camara LIKE '%Diputad%'
    """).fetchall()
    return {row[0]: tokens_relevantes(row[1]) for row in rows}


def crear_tabla(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS legisladores_organos_gobierno (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            legislador_id INTEGER NOT NULL,
            organo TEXT NOT NULL,           -- 'mesa_directiva' | 'jucopo'
            rol TEXT NOT NULL,              -- 'Presidencia', 'Coordinador', etc
            fuente_oid TEXT,                -- GUID del SPA Diputados
            actualizado_en TEXT NOT NULL,
            UNIQUE(legislador_id, organo)
        )
    """)


def upsert_estados(conn, datos, indice, dry_run=False):
    """
    `datos` = {organo_id: [items]} con items teniendo NombreCompleto y Rol
    """
    crear_tabla(conn)
    ahora = datetime.utcnow().isoformat(timespec='seconds')
    matched, no_matched = 0, []

    # Vaciar primero (la composición de Mesa/JUCOPO cambia, no acumular obsoletos)
    if not dry_run:
        conn.execute("DELETE FROM legisladores_organos_gobierno")

    for organo, items in datos.items():
        for it in items:
            nombre = it.get("NombreCompleto", "")
            rol = it.get("Rol", "")
            oid = it.get("Oid", "")
            leg_id = matchear_legislador(nombre, indice)
            if not leg_id:
                no_matched.append((organo, nombre))
                continue
            matched += 1
            if dry_run:
                logger.info(f"  [{organo}] {nombre} → leg_id={leg_id} rol={rol}")
                continue
            conn.execute("""
                INSERT INTO legisladores_organos_gobierno
                  (legislador_id, organo, rol, fuente_oid, actualizado_en)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(legislador_id, organo) DO UPDATE SET
                  rol = excluded.rol, fuente_oid = excluded.fuente_oid,
                  actualizado_en = excluded.actualizado_en
            """, (leg_id, organo, rol, oid, ahora))

    if not dry_run:
        conn.commit()
    logger.info(f"Match: {matched} OK | {len(no_matched)} sin match")
    if no_matched:
        for org, n in no_matched[:5]:
            logger.warning(f"  Sin match [{org}]: {n}")
    return matched, no_matched


def sync_d1(conn):
    """Empuja la tabla a Cloudflare D1 vía wrangler."""
    rows = conn.execute("""
        SELECT legislador_id, organo, rol, fuente_oid, actualizado_en
        FROM legisladores_organos_gobierno
    """).fetchall()
    if not rows:
        logger.warning("Sin filas para sync")
        return False

    ddl = """
        CREATE TABLE IF NOT EXISTS legisladores_organos_gobierno (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            legislador_id INTEGER NOT NULL,
            organo TEXT NOT NULL,
            rol TEXT NOT NULL,
            fuente_oid TEXT,
            actualizado_en TEXT NOT NULL,
            UNIQUE(legislador_id, organo)
        );
        DELETE FROM legisladores_organos_gobierno;
    """
    inserts = [ddl]
    def esc(s):
        return "'" + str(s).replace("'", "''") + "'" if s is not None else 'NULL'
    for r in rows:
        leg_id, organo, rol, oid, ts = r
        inserts.append(
            f"INSERT INTO legisladores_organos_gobierno "
            f"(legislador_id, organo, rol, fuente_oid, actualizado_en) "
            f"VALUES ({leg_id}, {esc(organo)}, {esc(rol)}, {esc(oid)}, {esc(ts)});"
        )
    sql = "\n".join(inserts)

    # Escribir a archivo temporal y ejecutar wrangler
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
        f.write(sql)
        sql_file = f.name
    try:
        result = subprocess.run(
            ["npx", "wrangler", "d1", "execute", "fiat-busqueda",
             "--file", sql_file, "--remote"],
            capture_output=True, text=True, timeout=60,
            cwd=str(ROOT / "worker"),
        )
        if result.returncode == 0:
            logger.info(f"Sync D1 OK ({len(rows)} filas)")
            return True
        logger.error(f"Sync D1 falló: {result.stderr[:500]}")
        return False
    finally:
        Path(sql_file).unlink(missing_ok=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-sync-d1", action="store_true")
    args = p.parse_args()

    logger.info("Fetching órganos de gobierno Diputados LXVI...")
    datos = fetch_organos()
    logger.info(f"Mesa Directiva: {len(datos['mesa_directiva'])} | JUCOPO: {len(datos['jucopo'])}")

    conn = sqlite3.connect(str(DB))
    indice = construir_indice(conn)
    logger.info(f"Diputados en BD: {len(indice)}")

    matched, _ = upsert_estados(conn, datos, indice, dry_run=args.dry_run)

    if not args.dry_run and not args.no_sync_d1:
        sync_d1(conn)

    # Resumen
    if not args.dry_run:
        for organo in ('mesa_directiva', 'jucopo'):
            rows = conn.execute("""
                SELECT l.nombre, l.partido, o.rol
                  FROM legisladores_organos_gobierno o
                  JOIN legisladores l ON l.id = o.legislador_id
                 WHERE o.organo = ?
                 ORDER BY o.rol, l.nombre
            """, (organo,)).fetchall()
            print(f"\n=== {organo.upper().replace('_', ' ')} ===")
            for n, p, r in rows:
                print(f"  {r:18} {n} ({p})")


if __name__ == "__main__":
    main()
