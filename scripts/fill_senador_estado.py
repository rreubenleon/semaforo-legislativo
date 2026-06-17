"""
Llena legisladores.estado + principio_eleccion para SENADORES.

El padrón tenía el estado vacío para casi todos los senadores (el perfil del
SIL de Gobernación usa un ID distinto al sitl_id de senado.gob.mx, así que el
scrape de perfiles no los alcanzó). El roster oficial LXVI del Senado
(eval/reelectos/roster_LXVI_Senado.json, obtenido del SIL Numeralia) SÍ trae
`entidad` y `tipo_eleccion` por senador. Esto los cruza con matcher.py.

Necesario para el mapa de representación (senadores por estado). Los senadores
de Representación Proporcional tienen entidad vacía en el roster (electos por
lista nacional) — correcto: se quedan sin estado y se marcan como RP.

Uso:
    python scripts/fill_senador_estado.py --dry-run
    python scripts/fill_senador_estado.py
"""
import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
ROSTER = ROOT / "eval" / "reelectos" / "roster_LXVI_Senado.json"
sys.path.insert(0, str(ROOT))

from utils.matcher import build_bd_index, encontrar_legislador_id, normalizar_nombre

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def fill(conn, dry_run=False):
    roster = json.loads(ROSTER.read_text(encoding="utf-8"))
    bd_idx = build_bd_index(conn)

    con_estado = con_rp = sin_match = 0
    updates = []
    vistos = set()
    for r in roster:
        nombre = r.get("nombre", "")
        entidad = (r.get("entidad") or "").strip()
        tipo = (r.get("tipo_eleccion") or "").strip()
        lid = encontrar_legislador_id(normalizar_nombre(nombre), "Senado", bd_idx)
        if not lid or lid in vistos:
            if not lid:
                sin_match += 1
            continue
        vistos.add(lid)
        updates.append((entidad, tipo, lid))
        if entidad:
            con_estado += 1
        else:
            con_rp += 1  # plurinominal: sin estado por diseño

    logger.info(f"Roster: {len(roster)} entradas")
    logger.info(f"  Senadores matcheados con estado: {con_estado}")
    logger.info(f"  Plurinominales (RP, sin estado, correcto): {con_rp}")
    logger.info(f"  Entradas del roster sin match en padrón: {sin_match}")

    if dry_run:
        logger.info("DRY-RUN: no se escribió nada.")
        return con_estado

    # Solo escribir estado cuando hay entidad; siempre fijar principio_eleccion.
    for entidad, tipo, lid in updates:
        if entidad:
            conn.execute(
                "UPDATE legisladores SET estado = ?, principio_eleccion = ? WHERE id = ?",
                (entidad, tipo or None, lid),
            )
        elif tipo:
            conn.execute(
                "UPDATE legisladores SET principio_eleccion = ? WHERE id = ?",
                (tipo, lid),
            )
    conn.commit()
    logger.info(f"✓ Actualizados {len(updates)} senadores ({con_estado} con estado).")
    return con_estado


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    conn = sqlite3.connect(str(DB))
    fill(conn, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
