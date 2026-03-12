"""
Índice de Búsqueda Full-Text (FTS5) — FIAT
Actualización incremental: solo inserta documentos nuevos desde la última corrida.
Rebuild completo solo cuando la tabla no existe o se pide explícitamente.
Se ejecuta cada 2h en el pipeline (paso 5c).
"""

import logging
import time

from db import get_connection, sync as sync_db

logger = logging.getLogger(__name__)

CREATE_FTS = """
CREATE VIRTUAL TABLE busqueda_fts USING fts5(
    titulo,
    contenido,
    fuente_tipo UNINDEXED,
    fuente_nombre UNINDEXED,
    categoria UNINDEXED,
    fecha UNINDEXED,
    url UNINDEXED,
    extra_json UNINDEXED,
    doc_id UNINDEXED,
    tokenize='unicode61 remove_diacritics 2'
)
"""

# ─── Queries de inserción completa (para rebuild) ───

INSERT_ARTICULOS_FULL = """
INSERT INTO busqueda_fts(titulo, contenido, fuente_tipo, fuente_nombre,
                         categoria, fecha, url, extra_json, doc_id)
SELECT titulo, COALESCE(resumen, ''), 'articulo', fuente,
       COALESCE(categorias, ''), fecha, COALESCE(url, ''),
       '', 'a-' || id
FROM articulos
"""

INSERT_GACETA_FULL = """
INSERT INTO busqueda_fts(titulo, contenido, fuente_tipo, fuente_nombre,
                         categoria, fecha, url, extra_json, doc_id)
SELECT titulo, COALESCE(resumen, ''), 'gaceta',
       COALESCE(camara, 'Diputados'),
       COALESCE(tipo, ''), fecha, COALESCE(url, ''),
       json_object('autor', COALESCE(autor,''), 'comision', COALESCE(comision,''),
                   'tipo', COALESCE(tipo,''), 'camara', COALESCE(camara,'Diputados'),
                   'url_pdf', COALESCE(url_pdf,'')),
       'g-' || id
FROM gaceta
"""

INSERT_SIL_FULL = """
INSERT INTO busqueda_fts(titulo, contenido, fuente_tipo, fuente_nombre,
                         categoria, fecha, url, extra_json, doc_id)
SELECT titulo, COALESCE(sinopsis, ''), 'sil',
       COALESCE(camara, ''),
       COALESCE(categoria, ''), COALESCE(fecha_presentacion, ''), '',
       json_object('partido', COALESCE(partido,''), 'estatus', COALESCE(estatus,''),
                   'presentador', COALESCE(presentador,''), 'tipo', COALESCE(tipo,'')),
       's-' || id
FROM sil_documentos
"""

# ─── Queries de inserción incremental (solo docs nuevos) ───

INSERT_ARTICULOS_NUEVOS = """
INSERT INTO busqueda_fts(titulo, contenido, fuente_tipo, fuente_nombre,
                         categoria, fecha, url, extra_json, doc_id)
SELECT titulo, COALESCE(resumen, ''), 'articulo', fuente,
       COALESCE(categorias, ''), fecha, COALESCE(url, ''),
       '', 'a-' || id
FROM articulos WHERE id > ?
"""

INSERT_GACETA_NUEVOS = """
INSERT INTO busqueda_fts(titulo, contenido, fuente_tipo, fuente_nombre,
                         categoria, fecha, url, extra_json, doc_id)
SELECT titulo, COALESCE(resumen, ''), 'gaceta',
       COALESCE(camara, 'Diputados'),
       COALESCE(tipo, ''), fecha, COALESCE(url, ''),
       json_object('autor', COALESCE(autor,''), 'comision', COALESCE(comision,''),
                   'tipo', COALESCE(tipo,''), 'camara', COALESCE(camara,'Diputados'),
                   'url_pdf', COALESCE(url_pdf,'')),
       'g-' || id
FROM gaceta WHERE id > ?
"""

INSERT_SIL_NUEVOS = """
INSERT INTO busqueda_fts(titulo, contenido, fuente_tipo, fuente_nombre,
                         categoria, fecha, url, extra_json, doc_id)
SELECT titulo, COALESCE(sinopsis, ''), 'sil',
       COALESCE(camara, ''),
       COALESCE(categoria, ''), COALESCE(fecha_presentacion, ''), '',
       json_object('partido', COALESCE(partido,''), 'estatus', COALESCE(estatus,''),
                   'presentador', COALESCE(presentador,''), 'tipo', COALESCE(tipo,'')),
       's-' || id
FROM sil_documentos WHERE id > ?
"""


def _tabla_existe(conn, nombre):
    """Verifica si una tabla existe en la base de datos."""
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (nombre,),
    ).fetchone()
    return row is not None


def _get_meta(conn, clave):
    """Obtiene un valor de fts_meta, retorna 0 si no existe."""
    row = conn.execute(
        "SELECT valor FROM fts_meta WHERE clave=?", (clave,)
    ).fetchone()
    return row[0] if row else 0


def _set_meta(conn, clave, valor):
    """Guarda un valor en fts_meta (INSERT OR REPLACE)."""
    conn.execute(
        "INSERT OR REPLACE INTO fts_meta (clave, valor) VALUES (?, ?)",
        (clave, valor),
    )


def _rebuild_completo(conn):
    """Rebuild completo del índice FTS5 (primera vez o forzado)."""
    logger.info("  FTS5: Rebuild completo...")

    conn.execute("DROP TABLE IF EXISTS busqueda_fts")
    conn.execute(CREATE_FTS)

    conn.execute(INSERT_ARTICULOS_FULL)
    n_art = conn.execute("SELECT changes()").fetchone()[0]
    logger.info(f"  FTS5: {n_art} artículos indexados")

    conn.execute(INSERT_GACETA_FULL)
    n_gac = conn.execute("SELECT changes()").fetchone()[0]
    logger.info(f"  FTS5: {n_gac} documentos de gaceta indexados")

    conn.execute(INSERT_SIL_FULL)
    n_sil = conn.execute("SELECT changes()").fetchone()[0]
    logger.info(f"  FTS5: {n_sil} documentos SIL indexados")

    # Guardar IDs máximos para próxima corrida incremental
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fts_meta (
            clave TEXT PRIMARY KEY,
            valor INTEGER DEFAULT 0
        )
    """)
    max_art = conn.execute("SELECT COALESCE(MAX(id), 0) FROM articulos").fetchone()[0]
    max_gac = conn.execute("SELECT COALESCE(MAX(id), 0) FROM gaceta").fetchone()[0]
    max_sil = conn.execute("SELECT COALESCE(MAX(id), 0) FROM sil_documentos").fetchone()[0]

    _set_meta(conn, "ultimo_id_articulos", max_art)
    _set_meta(conn, "ultimo_id_gaceta", max_gac)
    _set_meta(conn, "ultimo_id_sil", max_sil)

    conn.commit()

    total = n_art + n_gac + n_sil
    logger.info(f"  FTS5: {total} documentos totales (rebuild completo)")
    return {"articulos": n_art, "gaceta": n_gac, "sil": n_sil, "total": total, "modo": "rebuild"}


def reconstruir_indice_fts(forzar_rebuild=False):
    """
    Actualiza el índice FTS5.
    - Si la tabla no existe o forzar_rebuild=True: rebuild completo
    - Si la tabla existe: inserción incremental (solo docs nuevos)
    """
    conn = get_connection()

    # Crear tabla de metadatos si no existe
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fts_meta (
            clave TEXT PRIMARY KEY,
            valor INTEGER DEFAULT 0
        )
    """)

    # ¿Necesitamos rebuild completo?
    if forzar_rebuild or not _tabla_existe(conn, "busqueda_fts"):
        return _rebuild_completo(conn)

    # ─── Modo incremental ───
    ultimo_art = _get_meta(conn, "ultimo_id_articulos")
    ultimo_gac = _get_meta(conn, "ultimo_id_gaceta")
    ultimo_sil = _get_meta(conn, "ultimo_id_sil")

    conn.execute(INSERT_ARTICULOS_NUEVOS, (ultimo_art,))
    n_art = conn.execute("SELECT changes()").fetchone()[0]

    conn.execute(INSERT_GACETA_NUEVOS, (ultimo_gac,))
    n_gac = conn.execute("SELECT changes()").fetchone()[0]

    conn.execute(INSERT_SIL_NUEVOS, (ultimo_sil,))
    n_sil = conn.execute("SELECT changes()").fetchone()[0]

    # Actualizar IDs máximos
    max_art = conn.execute("SELECT COALESCE(MAX(id), 0) FROM articulos").fetchone()[0]
    max_gac = conn.execute("SELECT COALESCE(MAX(id), 0) FROM gaceta").fetchone()[0]
    max_sil = conn.execute("SELECT COALESCE(MAX(id), 0) FROM sil_documentos").fetchone()[0]

    _set_meta(conn, "ultimo_id_articulos", max_art)
    _set_meta(conn, "ultimo_id_gaceta", max_gac)
    _set_meta(conn, "ultimo_id_sil", max_sil)

    conn.commit()

    total = n_art + n_gac + n_sil
    if total > 0:
        logger.info(f"  FTS5 incremental: +{n_art} artículos, +{n_gac} gaceta, +{n_sil} SIL")
    else:
        logger.info("  FTS5: sin documentos nuevos")
    return {"articulos": n_art, "gaceta": n_gac, "sil": n_sil, "total": total, "modo": "incremental"}


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Índice FTS5 — FIAT")
    parser.add_argument("--rebuild", action="store_true", help="Forzar rebuild completo")
    args = parser.parse_args()

    inicio = time.time()
    stats = reconstruir_indice_fts(forzar_rebuild=args.rebuild)
    dur = time.time() - inicio
    print(f"\nFTS5 ({stats['modo']}): {stats['total']} docs en {dur:.1f}s")
