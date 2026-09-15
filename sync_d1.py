"""
sync_d1.py — Sincroniza busqueda_fts y scores de SQLite local a Cloudflare D1.
Se ejecuta como paso del pipeline después de que fts.py actualiza el índice.

INCREMENTAL: en vez de DROP + reinsertar toda la tabla en cada corrida
(219K filas x 6 corridas/día = ~1.3M escrituras, muy por encima del tope
free de D1 de 100K/día), solo empuja los documentos NUEVOS o CAMBIADOS.

- busqueda_fts se sincroniza por grupo de doc_id (doc_id NO es único: hay
  filas idénticas, así que cada doc_id se maneja como unidad: se borra e
  reinserta el grupo completo cuando cambia).
- scores se sincroniza por id (INSERT OR REPLACE de los que cambian).
- El estado de lo ya sincronizado se guarda localmente en la tabla
  d1_sync_state (viaja en el mismo cache de semaforo.db en CI).
- Primera corrida (estado vacío): SIEMBRA el estado sin empujar nada,
  porque D1 ya tiene los datos de los syncs completos previos. Así corta
  el consumo de inmediato sin un empujón masivo.
"""

import sqlite3
import subprocess
import sys
import os
import logging
import tempfile
import hashlib

logger = logging.getLogger("semaforo.sync_d1")

ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(ROOT, "semaforo.db")
D1_DATABASE_NAME = "fiat-busqueda"
BATCH_SIZE = 50  # D1 tiene límite de tamaño por statement

COLS_FTS = ["titulo", "contenido", "fuente_tipo", "fuente_nombre",
            "categoria", "fecha", "url", "extra_json", "doc_id"]
COLS_SCORES = ["id", "categoria", "score_total", "score_media", "score_trends",
               "score_congreso", "score_urgencia", "color", "fecha", "detalle",
               "score_mananera"]

CREATE_FTS = """CREATE VIRTUAL TABLE IF NOT EXISTS busqueda_fts USING fts5(
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
);
"""

CREATE_SCORES = """CREATE TABLE IF NOT EXISTS scores (
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
);
"""


def escape_sql(val):
    """Escapa un valor para INSERT SQL."""
    if val is None:
        return "NULL"
    return "'" + str(val).replace("'", "''") + "'"


def _hash(*parts):
    h = hashlib.md5()
    for p in parts:
        h.update(b"\x1f")
        h.update(("" if p is None else str(p)).encode("utf-8"))
    return h.hexdigest()


def _ensure_state(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS d1_sync_state (
            tabla TEXT NOT NULL,
            clave TEXT NOT NULL,
            hash TEXT NOT NULL,
            PRIMARY KEY (tabla, clave)
        )
    """)
    conn.commit()


def _estado_previo(conn, tabla):
    return dict(conn.execute(
        "SELECT clave, hash FROM d1_sync_state WHERE tabla = ?", (tabla,)
    ).fetchall())


def _insert_batches(f, tabla, cols, filas):
    """Escribe INSERTs en lotes de BATCH_SIZE."""
    col_list = ", ".join(cols)
    for i in range(0, len(filas), BATCH_SIZE):
        lote = filas[i:i + BATCH_SIZE]
        f.write(f"INSERT INTO {tabla}({col_list}) VALUES\n")
        f.write(",\n".join("(" + ", ".join(escape_sql(v) for v in r) + ")" for r in lote))
        f.write(";\n\n")


def generar_sql(output_path):
    """
    Genera el SQL incremental (solo deltas) y devuelve el estado nuevo
    para persistirlo SOLO si el push a D1 tiene éxito.
    """
    conn = sqlite3.connect(DB_PATH)
    _ensure_state(conn)

    doc_id_idx = COLS_FTS.index("doc_id")
    cambios_fts = 0
    cambios_scores = 0

    with open(output_path, "w", encoding="utf-8") as f:
        # Asegurar que las tablas existan en D1 (sin DROP: los datos persisten).
        f.write(CREATE_FTS + "\n")
        f.write(CREATE_SCORES + "\n")

        # ---------- busqueda_fts (incremental por grupo doc_id) ----------
        filas = conn.execute(f"SELECT {', '.join(COLS_FTS)} FROM busqueda_fts").fetchall()
        grupos = {}
        for r in filas:
            grupos.setdefault(r[doc_id_idx], []).append(r)

        actual = {}
        for doc_id, rs in grupos.items():
            actual[str(doc_id)] = _hash(*sorted(_hash(*row) for row in rs))

        prev = _estado_previo(conn, "busqueda_fts")
        nuevo_estado_fts = actual

        if not prev:
            logger.info(
                f"FTS primera sync incremental: siembro estado con {len(actual)} "
                f"doc_ids sin empujar filas (D1 ya tiene los datos previos)."
            )
        else:
            nuevos_cambiados = [d for d, h in actual.items() if prev.get(d) != h]
            eliminados = [d for d in prev if d not in actual]
            for doc_id in nuevos_cambiados:
                f.write(f"DELETE FROM busqueda_fts WHERE doc_id = {escape_sql(doc_id)};\n")
                _insert_batches(f, "busqueda_fts", COLS_FTS, grupos[doc_id])
            for doc_id in eliminados:
                f.write(f"DELETE FROM busqueda_fts WHERE doc_id = {escape_sql(doc_id)};\n\n")
            cambios_fts = len(nuevos_cambiados) + len(eliminados)
            logger.info(
                f"FTS incremental: {len(nuevos_cambiados)} doc_ids nuevos/cambiados, "
                f"{len(eliminados)} eliminados."
            )

        # ---------- scores (incremental por id) ----------
        filas_s = conn.execute(f"SELECT {', '.join(COLS_SCORES)} FROM scores").fetchall()
        actual_s = {str(r[0]): _hash(*r) for r in filas_s}
        by_id = {str(r[0]): r for r in filas_s}
        prev_s = _estado_previo(conn, "scores")
        nuevo_estado_scores = actual_s

        if not prev_s:
            logger.info(
                f"Scores primera sync incremental: siembro estado con {len(actual_s)} ids "
                f"sin empujar filas."
            )
        else:
            cambiados_s = [i for i, h in actual_s.items() if prev_s.get(i) != h]
            eliminados_s = [i for i in prev_s if i not in actual_s]
            if cambiados_s:
                filas_push = [by_id[i] for i in cambiados_s]
                col_list = ", ".join(COLS_SCORES)
                for k in range(0, len(filas_push), BATCH_SIZE):
                    lote = filas_push[k:k + BATCH_SIZE]
                    f.write(f"INSERT OR REPLACE INTO scores({col_list}) VALUES\n")
                    f.write(",\n".join("(" + ", ".join(escape_sql(v) for v in r) + ")" for r in lote))
                    f.write(";\n\n")
            for i in eliminados_s:
                f.write(f"DELETE FROM scores WHERE id = {escape_sql(i)};\n")
            cambios_scores = len(cambiados_s) + len(eliminados_s)
            logger.info(
                f"Scores incremental: {len(cambiados_s)} cambiados, {len(eliminados_s)} eliminados."
            )

    conn.close()
    size_mb = os.path.getsize(output_path) / 1024 / 1024
    hay_cambios = (cambios_fts + cambios_scores) > 0 or (not prev or not prev_s)
    logger.info(f"SQL generado: {size_mb:.2f} MB · cambios FTS={cambios_fts} scores={cambios_scores}")
    # Nota: en la primera corrida (siembra) hay_cambios=True para asegurar los CREATE IF NOT EXISTS,
    # pero el archivo casi no trae INSERTs, así que el consumo de D1 es mínimo.
    return {
        "hay_cambios": hay_cambios,
        "solo_creates": (cambios_fts + cambios_scores) == 0,
        "fts": nuevo_estado_fts,
        "scores": nuevo_estado_scores,
    }


def _persistir_estado(estado):
    conn = sqlite3.connect(DB_PATH)
    _ensure_state(conn)
    conn.execute("DELETE FROM d1_sync_state WHERE tabla IN ('busqueda_fts','scores')")
    conn.executemany(
        "INSERT INTO d1_sync_state(tabla, clave, hash) VALUES ('busqueda_fts', ?, ?)",
        list(estado["fts"].items()),
    )
    conn.executemany(
        "INSERT INTO d1_sync_state(tabla, clave, hash) VALUES ('scores', ?, ?)",
        list(estado["scores"].items()),
    )
    conn.commit()
    conn.close()
    logger.info(
        f"Estado local actualizado: {len(estado['fts'])} doc_ids FTS, "
        f"{len(estado['scores'])} ids scores."
    )


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
        estado = generar_sql(sql_path)
        # Persistir el estado SOLO si el push tiene éxito.
        subir_a_d1(sql_path)
        _persistir_estado(estado)
    finally:
        os.unlink(sql_path)
