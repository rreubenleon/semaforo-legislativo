"""
Re-atribuir actividad_legislador huérfana (legislador_id IS NULL).

El paso que puebla actividad_legislador (scrapers/legisladores.py) usaba un
matcher local débil que dejaba sin atribuir nombres como "María Lilly del
Carmen Téllez García" (BD: "Lilly Téllez"), "Jasmine María Bugarín Rodríguez",
etc. Este script rellena esas filas usando utils/matcher.py (fuente única,
con fallback de nombre abreviado), SIN tocar las ya atribuidas.

Idempotente: solo UPDATE de filas con legislador_id IS NULL. Los partidos
("PVEM", "Morena") y agencias se quedan NULL porque el matcher exige ≥2 tokens.

Uso:
    python scripts/reatribuir_actividad.py            # aplica
    python scripts/reatribuir_actividad.py --dry-run  # solo reporta
"""
import argparse
import logging
import sqlite3
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
sys.path.insert(0, str(ROOT))

from utils.matcher import build_bd_index, encontrar_legislador_id, normalizar_nombre

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def _camara_norm(camara_sil: str) -> str:
    return "Senado" if (camara_sil or "").startswith(("Cámara de Sen", "Senado")) else "Diputados"


def reatribuir(conn, dry_run=False):
    idx = build_bd_index(conn)
    rows = conn.execute(
        """
        SELECT al.id, al.nombre_presentador, sd.camara
        FROM actividad_legislador al
        JOIN sil_documentos sd ON sd.id = al.sil_documento_id
        WHERE al.legislador_id IS NULL
          AND al.nombre_presentador IS NOT NULL AND al.nombre_presentador != ''
        """
    ).fetchall()
    logger.info(f"Filas huérfanas (legislador_id NULL con nombre): {len(rows):,}")

    recuperadas = 0
    por_legislador = Counter()
    updates = []
    for aid, nombre, camara in rows:
        nn = normalizar_nombre(nombre or "")
        lid = encontrar_legislador_id(nn, _camara_norm(camara), idx)
        if lid:
            updates.append((lid, aid))
            por_legislador[lid] += 1
            recuperadas += 1

    logger.info(f"Re-atribuibles con matcher.py: {recuperadas:,} "
                f"(a {len(por_legislador)} legisladores distintos)")
    logger.info(f"Siguen sin match (partidos/agencias/edge cases): {len(rows) - recuperadas:,}")

    if dry_run:
        logger.info("DRY-RUN: no se escribió nada.")
        return recuperadas

    conn.executemany(
        "UPDATE actividad_legislador SET legislador_id = ? WHERE id = ?", updates
    )
    conn.commit()
    logger.info(f"✓ {recuperadas:,} filas re-atribuidas.")
    return recuperadas


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    conn = sqlite3.connect(str(DB))
    reatribuir(conn, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
