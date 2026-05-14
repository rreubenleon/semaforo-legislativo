"""
Limpia filas de gaceta que tienen títulos malparseados de Pendientes SITL.

Bug: scrape_urls_diputados.py (anterior a 13-may-2026) metía en `titulo`
el bloque CRUDO con metadata: "- {COMISIÓN} --> [sinopsis]. Pendiente
Publicación en Gaceta: FECHA NUM TÍTULO REAL". La URL apuntaba al
cuadro_asuntos_por_comisionlxvi (página de comisión, no instrumento).

Este script:
  1. Detecta filas con esos patrones en titulo
  2. Las borra (no son recuperables con metadata limpia sin re-scrape)
  3. El próximo scrape_urls_diputados.py (con parser fijo) las re-creará
     con título limpio.

Uso:
  python3 scripts/limpiar_pendientes_malparseadas.py
  python3 scripts/limpiar_pendientes_malparseadas.py --dry-run
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from db import get_connection
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # Detectar filas malparseadas:
    # - titulo contiene "Pendiente Publicación en Gaceta:"
    # - url apunta a cuadro_asuntos_por_comisionlxvi.php
    rows = conn.execute("""
        SELECT id, titulo, url FROM gaceta
        WHERE (titulo LIKE '%Pendiente Publicaci%n en Gaceta:%'
               OR titulo LIKE '%- %-->%Pendiente%')
           OR (url LIKE '%cuadro_asuntos_por_comisionlxvi.php%'
               AND (titulo LIKE '%-->%' OR titulo LIKE '%--%-- %'))
    """).fetchall()

    logger.info(f"Encontradas {len(rows)} filas malparseadas:")
    for r in rows[:5]:
        logger.info(f"  id={r['id']} | {(r['titulo'] or '')[:100]}…")

    if not rows:
        return 0

    ids = [r["id"] for r in rows]
    placeholders = ",".join("?" * len(ids))

    if args.dry_run:
        logger.info(f"[DRY-RUN] Borraría {len(ids)} filas de gaceta")
        # Verificar referencias FK
        try:
            n_act = conn.execute(
                f"SELECT COUNT(*) FROM actividad_legislador "
                f"WHERE sil_documento_id IN ({placeholders})",
                ids,
            ).fetchone()[0]
            logger.info(f"   actividad_legislador referencias: {n_act}")
        except sqlite3.OperationalError:
            pass
        return 0

    # Borrar referencias en actividad_legislador primero (FK)
    try:
        n_act = conn.execute(
            f"DELETE FROM actividad_legislador "
            f"WHERE sil_documento_id IN ({placeholders})",
            ids,
        ).rowcount
        logger.info(f"actividad_legislador: {n_act} filas borradas")
    except sqlite3.OperationalError:
        pass

    # Borrar gaceta
    n_gac = conn.execute(
        f"DELETE FROM gaceta WHERE id IN ({placeholders})",
        ids,
    ).rowcount
    logger.info(f"gaceta: {n_gac} filas borradas")

    conn.commit()
    logger.info(f"OK: {n_gac} pendientes malparseadas eliminadas. El próximo scrape_urls_diputados las re-creará con título limpio.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
