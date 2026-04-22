"""
Sync de legisladores_elo → D1 (tabla legisladores_elo).

Lee semaforo.db local y upserta en D1 via wrangler. El Worker puede
hacer JOIN con la tabla existente `legisladores` para devolver el ELO
en la respuesta /radar.

Uso:
    python scripts/sync_elo_d1.py
    python scripts/sync_elo_d1.py --dry-run
"""
import argparse
import logging
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

BATCH = 40


def sql_escape(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def generar_sql(dry_run=False):
    conn = sqlite3.connect(str(DB))
    rows = conn.execute("""
        SELECT nombre, partido, camara, rating, partidas, aprobados,
               desechados, pendientes_largo, draws, fecha_calculo,
               legislador_id, indice, percentil_camara
        FROM legisladores_elo
        ORDER BY rating DESC
    """).fetchall()
    logger.info(f"Leyendo {len(rows)} legisladores_elo desde SQLite local")

    # Recrear tabla desde cero para incluir legislador_id aunque
    # una versión previa no tuviera la columna.
    sql_parts = [
        "DROP TABLE IF EXISTS legisladores_elo;",
        """CREATE TABLE legisladores_elo (
            nombre TEXT PRIMARY KEY,
            partido TEXT,
            camara TEXT,
            rating REAL,
            partidas INTEGER,
            aprobados INTEGER,
            desechados INTEGER,
            pendientes_largo INTEGER,
            draws INTEGER,
            fecha_calculo TEXT,
            legislador_id INTEGER,
            indice REAL,
            percentil_camara REAL
        );""",
        "CREATE INDEX idx_elo_legislador ON legisladores_elo(legislador_id);",
    ]

    # Insert batches
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        vals = []
        for r in batch:
            nombre, partido, camara, rating, partidas, apr, des, pend, draws, fecha, leg_id, indice, pct_cam = r
            vals.append(
                f"({sql_escape(nombre)}, {sql_escape(partido)}, {sql_escape(camara)}, "
                f"{rating or 0}, {partidas or 0}, {apr or 0}, {des or 0}, {pend or 0}, "
                f"{draws or 0}, {sql_escape(fecha)}, "
                f"{leg_id if leg_id is not None else 'NULL'}, "
                f"{indice if indice is not None else 'NULL'}, "
                f"{pct_cam if pct_cam is not None else 'NULL'})"
            )
        sql_parts.append(
            "INSERT INTO legisladores_elo "
            "(nombre, partido, camara, rating, partidas, aprobados, desechados, pendientes_largo, draws, fecha_calculo, legislador_id, indice, percentil_camara) VALUES\n"
            + ",\n".join(vals) + ";"
        )

    sql = "\n".join(sql_parts)
    if dry_run:
        print(sql[:2000])
        print(f"\n... ({len(sql)} caracteres totales)")
        return 0

    # Ejecutar via wrangler
    worker_dir = ROOT / "worker"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tmp:
        tmp.write(sql)
        tmp_path = tmp.name

    cmd = [
        "npx", "wrangler", "d1", "execute", "fiat-busqueda",
        "--remote", "--file", tmp_path,
    ]
    result = subprocess.run(cmd, cwd=worker_dir, capture_output=True, text=True, timeout=120)
    Path(tmp_path).unlink()

    if result.returncode != 0:
        logger.error(f"wrangler stderr:\n{result.stderr[:1500]}")
        raise RuntimeError("wrangler falló")

    logger.info(f"✓ {len(rows)} legisladores_elo sincronizados a D1")
    return len(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    n = generar_sql(dry_run=args.dry_run)
    if not args.dry_run:
        print(f"OK: {n} filas sincronizadas")


if __name__ == "__main__":
    main()
