"""
Re-predice las categorías de los 100 documentos del eval set usando el clasificador
actual y actualiza la columna `pred_categorias`.

Esto permite iterar sobre el clasificador (cambios en config.py, keywords, filtros)
y medir el impacto sin volver a etiquetar manualmente. Las columnas gold_*
no se tocan.

Uso:
    python eval/repredict_eval_set.py             # re-predice y guarda
    python eval/repredict_eval_set.py --dry-run   # muestra cambios sin guardar
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import get_connection
from nlp.clasificador import clasificar_texto

logger = logging.getLogger(__name__)


def predecir(titulo, resumen, comision):
    cats = clasificar_texto(titulo, resumen or "", comision=comision)
    if not cats:
        return ""
    return ",".join(f"{c}:{s}" for c, s in cats.items())


def cargar_comision(conn, fuente_tabla, fuente_id):
    """La columna `eval_set` no guarda comisión; la traemos de la tabla origen."""
    if fuente_tabla == "gaceta":
        row = conn.execute("SELECT comision FROM gaceta WHERE id = ?", (fuente_id,)).fetchone()
        return row[0] if row else None
    if fuente_tabla == "sil_documentos":
        row = conn.execute("SELECT comision FROM sil_documentos WHERE id = ?", (fuente_id,)).fetchone()
        return row[0] if row else None
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="No guardar cambios, solo mostrar diffs")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    conn = get_connection()

    rows = conn.execute(
        "SELECT id, fuente_tabla, fuente_id, titulo, resumen, pred_categorias FROM eval_set"
    ).fetchall()

    if not rows:
        logger.error("eval_set vacío. Corre primero: python eval/sample_eval_set.py")
        return

    cambiados = 0
    iguales = 0
    diffs = []

    for row in rows:
        id_, fuente_tabla, fuente_id, titulo, resumen, pred_anterior = row
        comision = cargar_comision(conn, fuente_tabla, fuente_id)
        pred_nueva = predecir(titulo, resumen, comision)

        if pred_nueva != (pred_anterior or ""):
            cambiados += 1
            diffs.append((id_, fuente_tabla, titulo, pred_anterior or "", pred_nueva))
        else:
            iguales += 1

        if not args.dry_run:
            conn.execute(
                "UPDATE eval_set SET pred_categorias = ? WHERE id = ?",
                (pred_nueva, id_),
            )

    if not args.dry_run:
        conn.commit()

    logger.info(f"\n{'='*60}")
    logger.info(f"  RE-PREDICCIÓN — {len(rows)} documentos")
    logger.info(f"{'='*60}")
    logger.info(f"  Sin cambios: {iguales}")
    logger.info(f"  Cambiados:   {cambiados}")
    if args.dry_run:
        logger.info(f"  (DRY RUN — no se guardó nada)")
    logger.info("")

    if diffs:
        logger.info("DIFFS:")
        logger.info("─" * 60)
        for id_, ft, titulo, antes, despues in diffs[:50]:
            logger.info(f"\n  [{ft}#{id_}] {titulo[:90]}")
            logger.info(f"    antes  : {antes or '(vacío)'}")
            logger.info(f"    después: {despues or '(vacío)'}")
        if len(diffs) > 50:
            logger.info(f"\n  … +{len(diffs) - 50} más")

    if not args.dry_run and cambiados > 0:
        logger.info(f"\n✅ Pred actualizadas. Corre `python eval/calcular_metricas.py` para ver métricas.")


if __name__ == "__main__":
    main()
