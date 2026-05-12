"""
Empareja estatus_senado_panel (scraped de comisiones) con sil_documentos
de senadores y popula sil_documentos.estatus.

ESTRATEGIA:
  Para cada fila en estatus_senado_panel:
    1. Buscar sil_documentos donde seguimiento_id = 'SEN_{gaceta_doc_id}'
       (match exacto por ID estable de gaceta)
    2. Si match → UPDATE estatus
    3. Si no → log y continúa (siguiente fase: matching fuzzy por título)

  Mapea el texto del estatus del Senado a buckets canónicos para el frontend:
    'Aprobado' / 'Publicado en el DOF' / 'Resuelto' → 'Aprobado'
    'Desechado' / 'Concluido' → 'Desechado'
    'Turnado a Comisiones' / 'En comisión' / cualquier otro → 'Pendiente'

  El campo `estatus` en sil_documentos se llena así para que
  paso_conteos_estado (con sus patrones LIKE) los identifique.

Uso:
  python3 scripts/match_estatus_a_sil_documentos.py
  python3 scripts/match_estatus_a_sil_documentos.py --dry-run
"""
import argparse
import logging
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def canonicalizar_estatus(raw: str) -> str:
    """
    Mapea el texto del Senado a buckets canónicos que paso_conteos_estado
    reconoce vía LIKE '%Aprobado%' / 'Pendiente%' / 'Desechado%'.
    """
    if not raw:
        return ""
    r = raw.lower()
    if "aprobado" in r or "publicad" in r or "diario oficial" in r or "resuelto" in r:
        return "Resuelto / Aprobado"
    if "desechad" in r or "concluido" in r:
        return "Desechado"
    if "retirad" in r:
        return "Retirada"
    # Default: cualquier otro estado (Turnado a Comisiones, etc.) = pendiente
    return "Pendiente en comisión"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from db import get_connection
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # 1. Cargar índice de estatus por gaceta_doc_id.
    # Si un doc aparece en varias comisiones (ej. dictamen unidad), preferir
    # 'Aprobado' > 'Desechado' > 'Pendiente' (prioridad de resoluciones).
    PRIORIDAD = {
        "Resuelto / Aprobado": 3,
        "Desechado": 2,
        "Retirada": 2,
        "Pendiente en comisión": 1,
    }
    panel_rows = conn.execute(
        "SELECT gaceta_doc_id, estatus FROM estatus_senado_panel"
    ).fetchall()
    mejor_por_doc: dict[int, str] = {}
    for r in panel_rows:
        canon = canonicalizar_estatus(r["estatus"])
        prev = mejor_por_doc.get(r["gaceta_doc_id"])
        if prev is None or PRIORIDAD.get(canon, 0) > PRIORIDAD.get(prev, 0):
            mejor_por_doc[r["gaceta_doc_id"]] = canon
    logger.info(f"Panel rows: {len(panel_rows)}, docs únicos: {len(mejor_por_doc)}")

    # 2. Recolectar sil_documentos con SEN_* seguimiento_id
    sil_rows = conn.execute("""
        SELECT id, seguimiento_id, titulo, estatus
        FROM sil_documentos
        WHERE seguimiento_id LIKE 'SEN_%'
    """).fetchall()
    logger.info(f"sil_documentos con seg_id SEN_*: {len(sil_rows)}")

    # 3. Match exacto por SEN_{gaceta_doc_id}
    n_match = 0
    n_sin_match = 0
    actualizaciones = []
    for r in sil_rows:
        seg = r["seguimiento_id"]
        if not seg.startswith("SEN_"):
            continue
        try:
            doc_id = int(seg[4:])
        except ValueError:
            n_sin_match += 1
            continue
        estatus_nuevo = mejor_por_doc.get(doc_id)
        if not estatus_nuevo:
            n_sin_match += 1
            continue
        # Solo actualizar si está vacío o NULL
        if r["estatus"] and r["estatus"].strip():
            continue
        actualizaciones.append((estatus_nuevo, r["id"]))
        n_match += 1

    logger.info(f"Match exacto por SEN_{{doc_id}}: {n_match} sil_documentos a actualizar")
    logger.info(f"Sin match: {n_sin_match}")

    # 4. UPDATE
    if not args.dry_run:
        conn.executemany(
            "UPDATE sil_documentos SET estatus = ? WHERE id = ?",
            actualizaciones,
        )
        conn.commit()
        logger.info(f"UPDATE: {len(actualizaciones)} filas actualizadas")
    else:
        logger.info("[DRY-RUN] No se escribe")

    # 5. Resumen de estatus por bucket
    logger.info("\nDistribución estatus en sil_documentos SEN_*:")
    for r in conn.execute("""
        SELECT
            CASE
                WHEN estatus LIKE '%Aprobado%' OR estatus LIKE '%DOF%' THEN 'APROBADO'
                WHEN estatus LIKE 'Desechado%' OR estatus LIKE 'Concluido%' THEN 'DESECHADO'
                WHEN estatus LIKE 'Pendiente%' OR estatus LIKE '%comisi%' THEN 'PENDIENTE'
                WHEN estatus LIKE 'Retirad%' THEN 'RETIRADA'
                WHEN estatus IS NULL OR estatus = '' THEN 'SIN_ESTATUS'
                ELSE 'OTRO'
            END as bucket,
            COUNT(*)
        FROM sil_documentos
        WHERE seguimiento_id LIKE 'SEN_%'
        GROUP BY bucket
        ORDER BY 2 DESC
    """).fetchall():
        logger.info(f"  {r[1]:5d}× {r[0]}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
