"""
Carga ÚNICA de legisladores reelectos LXIV/LXV → LXVI.

Lee `eval/reelectos/diagnostico_unificado.json` (generado por
`eval/reelectos/diagnostico_unificado.py`) y escribe:

  1. Tabla nueva `legisladores_trayectoria`:
        UNIQUE(legislador_id, legislatura, camara)
  2. Columna nueva `legisladores.marcador_continuidad TEXT`
        ej. "Activo en LXIV y LXV"

Matching SIL Numeralia → BD usa multiset de tokens (Counter), porque
SIL trae "Rubén Ignacio Moreira Valdez" y BD trae
"Moreira Valdez Rubén Ignacio" (formato SITL apellido-nombre).

NO va en pipeline. Script idempotente: borra y recrea trayectorias.

Uso:
    .venv/bin/python scripts/cargar_reelectos.py --dry-run
    .venv/bin/python scripts/cargar_reelectos.py
"""
import argparse
import json
import logging
import sqlite3
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from db import get_connection
from utils.matcher import (
    build_bd_index,
    encontrar_legislador_id as _encontrar_legislador_id_shared,
    tokens_relevantes as _tokens,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

DIAG_PATH = ROOT / "eval" / "reelectos" / "diagnostico_unificado.json"


# Matcher: import desde utils/matcher.py (fuente única de verdad).
# Wrapper para mantener firma compatible con el resto del script.
encontrar_legislador_id = _encontrar_legislador_id_shared


# ──────────────────────────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────────────────────────
def asegurar_schema(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS legisladores_trayectoria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            legislador_id INTEGER NOT NULL,
            legislatura TEXT NOT NULL,        -- 'LXIV' | 'LXV'
            camara TEXT NOT NULL,             -- 'Diputados' | 'Senado'
            partido TEXT,
            entidad TEXT,
            tipo_eleccion TEXT,
            fecha_protesta TEXT,
            FOREIGN KEY (legislador_id) REFERENCES legisladores(id),
            UNIQUE(legislador_id, legislatura, camara)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trayectoria_leg ON legisladores_trayectoria(legislador_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trayectoria_legis ON legisladores_trayectoria(legislatura, camara)")
    # Columna derived
    try:
        conn.execute("ALTER TABLE legisladores ADD COLUMN marcador_continuidad TEXT DEFAULT ''")
        logger.info("Columna `marcador_continuidad` agregada a legisladores")
    except sqlite3.OperationalError:
        pass
    conn.commit()


# ──────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if not DIAG_PATH.exists():
        logger.error(f"No existe {DIAG_PATH}. Corre primero diagnostico_unificado.py")
        sys.exit(1)

    diag = json.loads(DIAG_PATH.read_text())
    reelectos = diag["reelectos"]
    logger.info(f"Reelectos en diagnóstico: {len(reelectos)}")

    conn = get_connection()
    if not args.dry_run:
        asegurar_schema(conn)

    # Index BD usando utils.matcher (incluye 'nombre' además de tokens)
    idx_bd = build_bd_index(conn)
    logger.info(f"Legisladores en BD: {len(idx_bd)}")

    matched = 0
    no_matched = []
    trayectoria_rows = []
    marcadores = {}  # legislador_id → marcador_continuidad

    for r in reelectos:
        lid = encontrar_legislador_id(r["nombre_normalizado"], r["camara_actual"], idx_bd)
        if lid is None:
            no_matched.append(r)
            continue
        matched += 1
        marcadores[lid] = r["marcador"]
        for prev in r["previas"]:
            trayectoria_rows.append((
                lid,
                prev["leg"],
                prev["camara"],
                prev.get("partido"),
                prev.get("entidad"),
                prev.get("tipo_eleccion"),
                prev.get("fecha_protesta"),
            ))

    pct = 100 * matched / len(reelectos) if reelectos else 0
    logger.info(f"Match: {matched}/{len(reelectos)} ({pct:.1f}%)")
    logger.info(f"Trayectorias a insertar: {len(trayectoria_rows)}")
    logger.info(f"Marcadores a actualizar: {len(marcadores)}")

    if no_matched:
        logger.warning(f"No matcheados ({len(no_matched)}):")
        for r in no_matched[:10]:
            logger.warning(f"  • {r['nombre']} ({r['camara_actual']}/{r['partido_actual']})")
        if len(no_matched) > 10:
            logger.warning(f"  … y {len(no_matched)-10} más")

    if args.dry_run:
        print("\n  (dry-run, no se escribió)")
        return

    # Escribir: idempotente (DELETE + INSERT por legislador_id)
    legs_afectados = list(marcadores.keys())
    if legs_afectados:
        placeholders = ",".join("?" * len(legs_afectados))
        conn.execute(
            f"DELETE FROM legisladores_trayectoria WHERE legislador_id IN ({placeholders})",
            legs_afectados,
        )

    conn.executemany(
        """INSERT INTO legisladores_trayectoria
           (legislador_id, legislatura, camara, partido, entidad, tipo_eleccion, fecha_protesta)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        trayectoria_rows,
    )
    conn.executemany(
        "UPDATE legisladores SET marcador_continuidad = ? WHERE id = ?",
        [(m, lid) for lid, m in marcadores.items()],
    )
    # Limpiar marcador en quien no es reelecto (por si quedó de corrida previa)
    if legs_afectados:
        conn.execute(
            f"UPDATE legisladores SET marcador_continuidad = '' "
            f"WHERE marcador_continuidad != '' AND id NOT IN ({placeholders})",
            legs_afectados,
        )
    conn.commit()

    # Verificación
    n_tray = conn.execute("SELECT COUNT(*) FROM legisladores_trayectoria").fetchone()[0]
    n_marc = conn.execute(
        "SELECT COUNT(*) FROM legisladores WHERE marcador_continuidad != ''"
    ).fetchone()[0]
    print(f"\n  ✓ legisladores_trayectoria: {n_tray} rows")
    print(f"  ✓ legisladores con marcador: {n_marc}")
    print(f"  ✓ Match: {pct:.1f}% ({matched}/{len(reelectos)})")
    if no_matched:
        no_match_path = ROOT / "eval" / "reelectos" / "no_matcheados.json"
        no_match_path.write_text(json.dumps(no_matched, ensure_ascii=False, indent=2))
        print(f"  ⚠ {len(no_matched)} no matcheados → {no_match_path.relative_to(ROOT)}")
    print()


if __name__ == "__main__":
    main()
