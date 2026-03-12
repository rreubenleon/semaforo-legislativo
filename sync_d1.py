"""
sync_d1.py — Sincroniza busqueda_fts y scores de SQLite local a Cloudflare D1.
Se ejecuta como paso del pipeline después de que fts.py actualiza el índice.

Genera un archivo SQL con DROP + CREATE + INSERTs y lo sube vía wrangler.
"""

import sqlite3
import subprocess
import sys
import os
import logging
import tempfile

logger = logging.getLogger("semaforo.sync_d1")

ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(ROOT, "semaforo.db")
D1_DATABASE_NAME = "fiat-busqueda"
BATCH_SIZE = 50  # D1 tiene límite de tamaño por statement


def escape_sql(val):
    """Escapa un valor para INSERT SQL."""
    if val is None:
        return "NULL"
    return "'" + str(val).replace("'", "''") + "'"


def generar_sql(output_path):
    """Genera archivo SQL con datos FTS + scores desde semaforo.db."""
    conn = sqlite3.connect(DB_PATH)

    with open(output_path, "w", encoding="utf-8") as f:
        # Borrar tablas existentes en D1
        f.write("DROP TABLE IF EXISTS scores;\n")
        f.write("DROP TABLE IF EXISTS busqueda_fts;\n\n")

        # Crear FTS5
        f.write("""CREATE VIRTUAL TABLE busqueda_fts USING fts5(
    titulo,
    contenido,
    fuente_tipo UNINDEXED,
    fuente_nombre UNINDEXED,
    categoria UNINDEXED,
    fecha UNINDEXED,
    url UNINDEXED,
    extra_json UNINDEXED,
    doc_id UNINDEXED,
    tokenize="unicode61 remove_diacritics 2"
);\n\n""")

        # Crear scores
        f.write("""CREATE TABLE scores (
    id INTEGER PRIMARY KEY,
    categoria TEXT,
    score_total REAL,
    score_media REAL,
    score_trends REAL,
    score_congreso REAL,
    score_urgencia REAL,
    color TEXT,
    fecha TEXT,
    detalle TEXT,
    score_mananera REAL
);\n\n""")

        # Exportar FTS
        cols_fts = "titulo, contenido, fuente_tipo, fuente_nombre, categoria, fecha, url, extra_json, doc_id"
        rows = conn.execute(f"SELECT {cols_fts} FROM busqueda_fts").fetchall()
        batch = []
        for i, r in enumerate(rows):
            vals = ", ".join(escape_sql(v) for v in r)
            batch.append(f"({vals})")
            if len(batch) >= BATCH_SIZE or i == len(rows) - 1:
                f.write(f"INSERT INTO busqueda_fts({cols_fts}) VALUES\n")
                f.write(",\n".join(batch))
                f.write(";\n\n")
                batch = []
        logger.info(f"FTS: {len(rows)} docs exportados")

        # Exportar scores
        cols_scores = "id, categoria, score_total, score_media, score_trends, score_congreso, score_urgencia, color, fecha, detalle, score_mananera"
        rows = conn.execute(f"SELECT {cols_scores} FROM scores").fetchall()
        batch = []
        for i, r in enumerate(rows):
            vals = ", ".join(escape_sql(v) for v in r)
            batch.append(f"({vals})")
            if len(batch) >= BATCH_SIZE or i == len(rows) - 1:
                f.write(f"INSERT INTO scores({cols_scores}) VALUES\n")
                f.write(",\n".join(batch))
                f.write(";\n\n")
                batch = []
        logger.info(f"Scores: {len(rows)} rows exportados")

    conn.close()
    size_mb = os.path.getsize(output_path) / 1024 / 1024
    logger.info(f"SQL generado: {size_mb:.1f} MB")
    return len(rows)


def subir_a_d1(sql_path):
    """Sube el SQL a D1 via wrangler."""
    cmd = [
        "npx", "wrangler", "d1", "execute", D1_DATABASE_NAME,
        "--remote", f"--file={sql_path}",
    ]
    logger.info(f"Ejecutando: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=os.path.join(ROOT, "worker"),
        timeout=600,
    )
    if result.returncode != 0:
        logger.error(f"Error wrangler: {result.stderr}")
        raise RuntimeError(f"wrangler d1 execute falló: {result.stderr}")
    logger.info("Sync D1 completado")
    return result.stdout


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    if not os.path.exists(DB_PATH):
        logger.error(f"No existe {DB_PATH}")
        sys.exit(1)

    with tempfile.NamedTemporaryFile(suffix=".sql", delete=False, mode="w") as tmp:
        sql_path = tmp.name

    try:
        generar_sql(sql_path)
        subir_a_d1(sql_path)
    finally:
        os.unlink(sql_path)
