"""
Genera el ML Evaluation Set: 100 documentos aleatorios de articulos + gaceta + sil_documentos
para etiquetado manual y posterior medición de precision/recall del clasificador.

Crea (si no existe) la tabla `eval_set` y la rellena con la muestra. La predicción
actual del clasificador se almacena junto al documento, así el etiquetado posterior
no depende del estado del modelo en ese momento.

Uso:
    python eval/sample_eval_set.py [--n 100] [--seed 42] [--reset]

    --reset borra la tabla eval_set antes de muestrear (¡pierde etiquetas!).
"""

import argparse
import logging
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import get_connection
from nlp.clasificador import clasificar_texto

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS eval_set (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fuente_tabla    TEXT NOT NULL,           -- 'articulos' | 'gaceta' | 'sil_documentos'
    fuente_id       INTEGER NOT NULL,        -- id en la tabla origen
    titulo          TEXT NOT NULL,
    resumen         TEXT,
    fecha           TEXT,
    fuente_extra    TEXT,                    -- medio (articulos), camara (gaceta/sil), etc.
    url             TEXT,
    pred_categorias TEXT,                    -- output de clasificar_texto en el momento del muestreo
    gold_relevante  INTEGER,                 -- 1=relevante, 0=no, NULL=sin etiquetar
    gold_categorias TEXT,                    -- categorías reales separadas por coma, NULL=sin etiquetar
    notas           TEXT,
    etiquetado_en   TEXT,
    UNIQUE(fuente_tabla, fuente_id)
);
CREATE INDEX IF NOT EXISTS idx_eval_set_estado ON eval_set (gold_relevante);
"""


def crear_tabla(conn):
    for stmt in SCHEMA.strip().split(";"):
        if stmt.strip():
            conn.execute(stmt)
    conn.commit()


def muestrear_articulos(conn, n):
    rows = conn.execute(
        """
        SELECT id, titulo, resumen, fecha, fuente, url
          FROM articulos
         WHERE titulo IS NOT NULL AND titulo != ''
         ORDER BY RANDOM()
         LIMIT ?
        """,
        (n,),
    ).fetchall()
    return [
        {
            "fuente_tabla": "articulos",
            "fuente_id": r[0],
            "titulo": r[1],
            "resumen": r[2] or "",
            "fecha": r[3],
            "fuente_extra": r[4],
            "url": r[5],
            "comision": None,
        }
        for r in rows
    ]


def muestrear_gaceta(conn, n):
    rows = conn.execute(
        """
        SELECT id, titulo, resumen, fecha, camara, url, comision
          FROM gaceta
         WHERE titulo IS NOT NULL AND titulo != ''
         ORDER BY RANDOM()
         LIMIT ?
        """,
        (n,),
    ).fetchall()
    return [
        {
            "fuente_tabla": "gaceta",
            "fuente_id": r[0],
            "titulo": r[1],
            "resumen": r[2] or "",
            "fecha": r[3],
            "fuente_extra": r[4],
            "url": r[5],
            "comision": r[6],
        }
        for r in rows
    ]


def muestrear_sil(conn, n):
    rows = conn.execute(
        """
        SELECT id, titulo, sinopsis, fecha_presentacion, camara, comision
          FROM sil_documentos
         WHERE titulo IS NOT NULL AND titulo != ''
         ORDER BY RANDOM()
         LIMIT ?
        """,
        (n,),
    ).fetchall()
    return [
        {
            "fuente_tabla": "sil_documentos",
            "fuente_id": r[0],
            "titulo": r[1],
            "resumen": r[2] or "",
            "fecha": r[3],
            "fuente_extra": r[4],
            "url": None,
            "comision": r[5],
        }
        for r in rows
    ]


def predecir(doc):
    cats = clasificar_texto(
        doc["titulo"],
        doc.get("resumen", ""),
        comision=doc.get("comision"),
    )
    if not cats:
        return ""
    return ",".join(f"{c}:{s}" for c, s in cats.items())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=100, help="Tamaño de la muestra (default 100)")
    parser.add_argument("--seed", type=int, default=42, help="Semilla aleatoria")
    parser.add_argument("--reset", action="store_true", help="Borra eval_set antes de muestrear")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    random.seed(args.seed)

    conn = get_connection()
    crear_tabla(conn)

    if args.reset:
        existentes = conn.execute("SELECT COUNT(*) FROM eval_set").fetchone()[0]
        confirm = input(f"⚠️  --reset va a borrar {existentes} filas (incluyendo etiquetas). Escribe 'BORRAR': ")
        if confirm.strip() == "BORRAR":
            conn.execute("DELETE FROM eval_set")
            conn.commit()
            logger.info("eval_set vaciada.")
        else:
            logger.info("Cancelado.")
            return

    ya_en_eval = conn.execute("SELECT COUNT(*) FROM eval_set").fetchone()[0]
    if ya_en_eval >= args.n:
        logger.info(f"eval_set ya tiene {ya_en_eval} filas (≥ {args.n}). Nada que hacer.")
        return

    faltan = args.n - ya_en_eval
    logger.info(f"Muestreando {faltan} documentos nuevos…")

    # Muestreo proporcional al tamaño relativo de cada tabla
    counts = {
        "articulos": conn.execute("SELECT COUNT(*) FROM articulos").fetchone()[0],
        "gaceta": conn.execute("SELECT COUNT(*) FROM gaceta").fetchone()[0],
        "sil_documentos": conn.execute("SELECT COUNT(*) FROM sil_documentos").fetchone()[0],
    }
    total = sum(counts.values())
    # Pedimos un poco de más para compensar duplicados / nulos
    margen = max(int(faltan * 0.3), 10)
    n_articulos = int(round((counts["articulos"] / total) * faltan)) + margen
    n_gaceta = int(round((counts["gaceta"] / total) * faltan)) + margen
    n_sil = int(round((counts["sil_documentos"] / total) * faltan)) + margen

    pool = (
        muestrear_articulos(conn, n_articulos)
        + muestrear_gaceta(conn, n_gaceta)
        + muestrear_sil(conn, n_sil)
    )
    random.shuffle(pool)

    insertados = 0
    for doc in pool:
        if insertados >= faltan:
            break
        pred = predecir(doc)
        try:
            conn.execute(
                """
                INSERT INTO eval_set (
                    fuente_tabla, fuente_id, titulo, resumen, fecha, fuente_extra,
                    url, pred_categorias
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    doc["fuente_tabla"],
                    doc["fuente_id"],
                    doc["titulo"],
                    doc["resumen"],
                    doc["fecha"],
                    doc["fuente_extra"],
                    doc["url"],
                    pred,
                ),
            )
            insertados += 1
        except Exception as exc:
            # Probablemente UNIQUE collision (mismo doc volvió a salir): saltar
            logger.debug(f"Skip {doc['fuente_tabla']}#{doc['fuente_id']}: {exc}")

    conn.commit()

    distribucion = conn.execute(
        "SELECT fuente_tabla, COUNT(*) FROM eval_set GROUP BY fuente_tabla"
    ).fetchall()
    sin_etiqueta = conn.execute(
        "SELECT COUNT(*) FROM eval_set WHERE gold_relevante IS NULL"
    ).fetchone()[0]

    logger.info(f"\n✅ eval_set listo: insertados {insertados} nuevos.")
    logger.info("Distribución por fuente:")
    for fuente, count in distribucion:
        logger.info(f"  {fuente:18s}  {count}")
    logger.info(f"\nPendientes de etiquetar: {sin_etiqueta}")
    logger.info("Siguiente paso: python eval/etiquetar_web.py")


if __name__ == "__main__":
    main()
