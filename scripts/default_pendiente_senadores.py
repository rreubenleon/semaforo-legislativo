"""
Asigna estatus = 'Pendiente en comisión' a sil_documentos SEN_* que
quedaron sin estatus después del scrape de comisiones Senado.

JUSTIFICACIÓN: los docs SEN_* sin estatus son instrumentos presentados
en el Senado oficial pero que NO aparecen en las páginas de comisiones
(overlap=0 con estatus_senado_panel). En el flujo legislativo del
Senado, ese estado real es "presentado pero aún sin turno a comisión"
o "turnado pero sin dictamen", que canonicalizamos como "Pendiente".

Cuando el siguiente scrape de comisiones los capture, el script
match_estatus_a_sil_documentos.py los actualizará al estado real
(Aprobado/Desechado/etc).

Uso:
  python3 scripts/default_pendiente_senadores.py
  python3 scripts/default_pendiente_senadores.py --dry-run
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

    n_sin = conn.execute("""
        SELECT COUNT(*) FROM sil_documentos
        WHERE seguimiento_id LIKE 'SEN_%'
          AND (estatus IS NULL OR estatus = '')
    """).fetchone()[0]
    logger.info(f"SEN_* sin estatus a marcar como Pendiente: {n_sin}")

    if args.dry_run:
        logger.info("[DRY-RUN] No se aplica")
        return 0

    n = conn.execute("""
        UPDATE sil_documentos
        SET estatus = 'Pendiente en comisión'
        WHERE seguimiento_id LIKE 'SEN_%'
          AND (estatus IS NULL OR estatus = '')
    """).rowcount
    conn.commit()
    logger.info(f"OK: {n} filas actualizadas a 'Pendiente en comisión'")

    # Verificación
    n_sin_post = conn.execute("""
        SELECT COUNT(*) FROM sil_documentos
        WHERE seguimiento_id LIKE 'SEN_%'
          AND (estatus IS NULL OR estatus = '')
    """).fetchone()[0]
    logger.info(f"Sin estatus DESPUÉS: {n_sin_post} (debería ser 0)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
