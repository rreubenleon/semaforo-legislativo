"""
Calcula conteos por rol (Iniciante / Adherente / De Grupo / Proponente)
desde la tabla `diputado_instrumento` y los popula como columnas en
`legisladores` para que el worker /radar los exponga.

Sin esto, el frontend solo ve l3p_iniciativas (Iniciante) y subcuenta a
diputados como Gibrán que tiene 47 iniciativas De Grupo.

Idempotente.

Uso:
    python scripts/calcular_conteos_roles_diputados.py
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))

    # Asegurar columnas
    cols = {r[1] for r in conn.execute("PRAGMA table_info(legisladores)").fetchall()}
    new_cols = [
        ("n_ini_iniciante", "INTEGER DEFAULT 0"),
        ("n_ini_adherente", "INTEGER DEFAULT 0"),
        ("n_ini_de_grupo", "INTEGER DEFAULT 0"),
        ("n_prop_proponente", "INTEGER DEFAULT 0"),
        ("n_prop_adherente", "INTEGER DEFAULT 0"),
        ("n_prop_de_grupo", "INTEGER DEFAULT 0"),
        ("n_total_vinculadas", "INTEGER DEFAULT 0"),
    ]
    for col, ddl in new_cols:
        if col not in cols:
            logger.info(f"  Agregando columna {col}")
            conn.execute(f"ALTER TABLE legisladores ADD COLUMN {col} {ddl}")
    conn.commit()

    # Verificar que tabla diputado_instrumento existe
    has_table = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='diputado_instrumento'"
    ).fetchone()
    if not has_table:
        logger.error("Tabla diputado_instrumento no existe. Corre primero integrar_diputados_oficial.py")
        sys.exit(1)

    # Leer conteos por (legislador_id, rol, tipo)
    rows = conn.execute("""
        SELECT legislador_id, rol, tipo, COUNT(*) FROM diputado_instrumento
        GROUP BY legislador_id, rol, tipo
    """).fetchall()

    from collections import defaultdict
    conteos = defaultdict(lambda: {
        "n_ini_iniciante": 0,
        "n_ini_adherente": 0,
        "n_ini_de_grupo": 0,
        "n_prop_proponente": 0,
        "n_prop_adherente": 0,
        "n_prop_de_grupo": 0,
    })

    for leg_id, rol, tipo, n in rows:
        es_ini = "Iniciativa" in (tipo or "")
        es_prop = "Proposici" in (tipo or "")
        if es_ini:
            if rol == "Iniciante":
                conteos[leg_id]["n_ini_iniciante"] += n
            elif rol == "Adherente":
                conteos[leg_id]["n_ini_adherente"] += n
            elif rol in ("De Grupo", "Suscriptor"):
                conteos[leg_id]["n_ini_de_grupo"] += n
        elif es_prop:
            if rol in ("Proponente", "Promovente"):
                conteos[leg_id]["n_prop_proponente"] += n
            elif rol == "Adherente":
                conteos[leg_id]["n_prop_adherente"] += n
            elif rol in ("De Grupo", "Suscriptor"):
                conteos[leg_id]["n_prop_de_grupo"] += n

    logger.info(f"Diputados con vinculaciones: {len(conteos)}")

    # Aplicar
    actualizados = 0
    for leg_id, c in conteos.items():
        total = sum(c.values())
        if not args.dry_run:
            conn.execute("""
                UPDATE legisladores SET
                  n_ini_iniciante = ?, n_ini_adherente = ?, n_ini_de_grupo = ?,
                  n_prop_proponente = ?, n_prop_adherente = ?, n_prop_de_grupo = ?,
                  n_total_vinculadas = ?
                WHERE id = ?
            """, (
                c["n_ini_iniciante"], c["n_ini_adherente"], c["n_ini_de_grupo"],
                c["n_prop_proponente"], c["n_prop_adherente"], c["n_prop_de_grupo"],
                total, leg_id,
            ))
            actualizados += 1

    if not args.dry_run:
        conn.commit()
    logger.info(f"Diputados actualizados: {actualizados}")

    # Top 5 por vinculaciones
    print()
    print("Top 10 diputados por iniciativas vinculadas (LXVI):")
    for r in conn.execute("""
        SELECT nombre, partido,
               n_ini_iniciante, n_ini_adherente, n_ini_de_grupo,
               (n_ini_iniciante + n_ini_adherente + n_ini_de_grupo) as total_ini
        FROM legisladores
        WHERE camara = 'Cámara de Diputados'
        ORDER BY total_ini DESC LIMIT 10
    """).fetchall():
        print(f"  {r[0][:40]:40} {(r[1] or ''):8} "
              f"total={r[5]:3} (ini={r[2]:3}, adh={r[3]:3}, grupo={r[4]:3})")


if __name__ == "__main__":
    main()
