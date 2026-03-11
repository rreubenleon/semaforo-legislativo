"""
Índice de Búsqueda Full-Text (FTS5) — FIAT
Reconstruye tabla busqueda_fts con datos de articulos, gaceta y sil_documentos.
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

INSERT_ARTICULOS = """
INSERT INTO busqueda_fts(titulo, contenido, fuente_tipo, fuente_nombre,
                         categoria, fecha, url, extra_json, doc_id)
SELECT titulo, COALESCE(resumen, ''), 'articulo', fuente,
       COALESCE(categorias, ''), fecha, COALESCE(url, ''),
       '', 'a-' || id
FROM articulos
"""

INSERT_GACETA = """
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

INSERT_SIL = """
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


def reconstruir_indice_fts():
    """Reconstruye el índice FTS5 completo desde las 3 tablas fuente."""
    conn = get_connection()

    # Drop + recreate
    conn.execute("DROP TABLE IF EXISTS busqueda_fts")
    conn.execute(CREATE_FTS)

    # Poblar desde cada fuente
    conn.execute(INSERT_ARTICULOS)
    n_art = conn.execute("SELECT changes()").fetchone()[0]
    logger.info(f"  FTS5: {n_art} artículos indexados")

    conn.execute(INSERT_GACETA)
    n_gac = conn.execute("SELECT changes()").fetchone()[0]
    logger.info(f"  FTS5: {n_gac} documentos de gaceta indexados")

    conn.execute(INSERT_SIL)
    n_sil = conn.execute("SELECT changes()").fetchone()[0]
    logger.info(f"  FTS5: {n_sil} documentos SIL indexados")

    conn.commit()
    sync_db()

    total = n_art + n_gac + n_sil
    logger.info(f"  FTS5: {total} documentos totales en índice")
    return {"articulos": n_art, "gaceta": n_gac, "sil": n_sil, "total": total}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    inicio = time.time()
    stats = reconstruir_indice_fts()
    dur = time.time() - inicio
    print(f"\nÍndice FTS5 reconstruido: {stats['total']} docs en {dur:.1f}s")
