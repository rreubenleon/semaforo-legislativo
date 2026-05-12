"""
Popula `estatus` para sil_documentos que lo tienen NULL/vacío.

CONTEXTO:
  Los docs creados por scrape_senadores_senado_gob.py traen seguimiento_id
  y datos básicos, pero NO el estatus (Aprobado/Pendiente/Desechado).
  El paso_scraper_sil del radar tampoco los procesa porque opera sobre
  refs descubiertas por rango incremental.

  Sin estatus, paso_conteos_estado devuelve 0 para todos, y el desglose
  por estado en el frontend queda vacío.

ESTRATEGIA:
  Por cada sil_documento sin estatus: llamar _obtener_detalle(seg_id,
  asu_id) que va a SIL Gobernación y extrae estatus + otros campos.
  UPDATE el row.

Uso:
  python3 scripts/popular_estatus_faltantes.py
  python3 scripts/popular_estatus_faltantes.py --limit 10  (debug)
"""
import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None,
                    help="Max docs a procesar (debug)")
    ap.add_argument("--delay", type=float, default=0.3,
                    help="Segundos entre requests")
    args = ap.parse_args()

    from db import get_connection
    from scrapers.sil import _obtener_detalle

    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # Buscar docs sin estatus
    query = """
        SELECT id, seguimiento_id, asunto_id, titulo
        FROM sil_documentos
        WHERE (estatus IS NULL OR estatus = '')
          AND seguimiento_id IS NOT NULL
          AND asunto_id IS NOT NULL
        ORDER BY id DESC
    """
    if args.limit:
        query += f" LIMIT {args.limit}"

    docs = conn.execute(query).fetchall()
    logger.info(f"Docs sin estatus a procesar: {len(docs)}")

    ok = 0
    sin_data = 0
    errores = 0

    for i, doc in enumerate(docs):
        if i and i % 50 == 0:
            logger.info(f"  Progreso: {i}/{len(docs)} (ok={ok}, sin_data={sin_data}, err={errores})")
            conn.commit()

        try:
            detalle = _obtener_detalle(doc["seguimiento_id"], doc["asunto_id"])
        except Exception as e:
            logger.debug(f"  Error doc id={doc['id']}: {e}")
            errores += 1
            time.sleep(args.delay)
            continue

        if not detalle or not detalle.get("estatus"):
            sin_data += 1
            time.sleep(args.delay)
            continue

        conn.execute("""
            UPDATE sil_documentos
            SET estatus = COALESCE(NULLIF(estatus, ''), ?),
                comision = COALESCE(NULLIF(comision, ''), ?),
                tipo = COALESCE(NULLIF(tipo, ''), ?),
                fecha_presentacion = COALESCE(NULLIF(fecha_presentacion, ''),
                                              NULLIF(?, '')),
                presentador = COALESCE(NULLIF(presentador, ''),
                                        NULLIF(?, ''))
            WHERE id = ?
        """, (
            detalle.get("estatus", ""),
            detalle.get("comision", ""),
            detalle.get("tipo", ""),
            detalle.get("fecha_presentacion", ""),
            detalle.get("presentador", ""),
            doc["id"],
        ))
        ok += 1
        time.sleep(args.delay)

    conn.commit()
    logger.info(f"\nFinal: {ok} actualizados, {sin_data} sin data en SIL, {errores} errores")


if __name__ == "__main__":
    main()
