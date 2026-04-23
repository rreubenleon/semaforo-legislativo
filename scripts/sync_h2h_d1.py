"""
Sync de legisladores_h2h → D1.

Lee semaforo.db local y upserta en D1 vía wrangler. Las filas tienen JSON
de instrumentos recientes que pueden ser ~500-1000 chars c/u, así que el
batch es chico para no pegarle al límite de payload de D1.

Uso:
    python scripts/sync_h2h_d1.py
    python scripts/sync_h2h_d1.py --dry-run
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

BATCH = 15  # JSONs grandes → chunks chicos


def sql_escape(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB))
    rows = conn.execute("""
        SELECT legislador_id, nombre_legislador, comision,
               n_instrumentos, aprobados, desechados, retirados,
               pendientes_largo, pendientes_corto, pendientes_recientes,
               tasa_aprobacion, tasa_comision_lxvi, diferencial_pp,
               dias_promedio_dictamen, instrumentos_recientes, fecha_calculo
        FROM legisladores_h2h
        ORDER BY legislador_id, n_instrumentos DESC
    """).fetchall()
    logger.info(f"Leyendo {len(rows)} h2h desde SQLite local")

    # DROP+CREATE en D1 para idempotencia
    sql_parts = [
        "DROP TABLE IF EXISTS legisladores_h2h;",
        """CREATE TABLE legisladores_h2h (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            legislador_id INTEGER NOT NULL,
            nombre_legislador TEXT,
            comision TEXT NOT NULL,
            n_instrumentos INTEGER,
            aprobados INTEGER,
            desechados INTEGER,
            retirados INTEGER,
            pendientes_largo INTEGER,
            pendientes_corto INTEGER,
            pendientes_recientes INTEGER,
            tasa_aprobacion REAL,
            tasa_comision_lxvi REAL,
            diferencial_pp REAL,
            dias_promedio_dictamen REAL,
            instrumentos_recientes TEXT,
            fecha_calculo TEXT
        );""",
        "CREATE INDEX idx_h2h_legislador ON legisladores_h2h(legislador_id);",
        "CREATE INDEX idx_h2h_comision ON legisladores_h2h(comision);",
    ]

    # Inserts en batches chicos
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        vals = []
        for r in batch:
            vals.append(
                "(" + ", ".join([
                    str(r[0]),                  # legislador_id
                    sql_escape(r[1]),           # nombre
                    sql_escape(r[2]),           # comision
                    str(r[3] or 0),             # n_instrumentos
                    str(r[4] or 0),             # aprobados
                    str(r[5] or 0),             # desechados
                    str(r[6] or 0),             # retirados
                    str(r[7] or 0),             # pen_largo
                    str(r[8] or 0),             # pen_corto
                    str(r[9] or 0),             # pen_recientes
                    sql_escape(r[10]),          # tasa_aprobacion
                    sql_escape(r[11]),          # tasa_comision_lxvi
                    sql_escape(r[12]),          # diferencial_pp
                    sql_escape(r[13]),          # dias_promedio
                    sql_escape(r[14]),          # JSON recientes
                    sql_escape(r[15]),          # fecha_calculo
                ]) + ")"
            )
        sql_parts.append(
            "INSERT INTO legisladores_h2h "
            "(legislador_id, nombre_legislador, comision, n_instrumentos, "
            "aprobados, desechados, retirados, pendientes_largo, pendientes_corto, "
            "pendientes_recientes, tasa_aprobacion, tasa_comision_lxvi, "
            "diferencial_pp, dias_promedio_dictamen, instrumentos_recientes, "
            "fecha_calculo) VALUES\n" + ",\n".join(vals) + ";"
        )

    sql = "\n".join(sql_parts)
    if args.dry_run:
        print(sql[:2000])
        print(f"\n... ({len(sql)} caracteres totales)")
        return 0

    worker_dir = ROOT / "worker"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tmp:
        tmp.write(sql)
        tmp_path = tmp.name

    cmd = ["npx", "wrangler", "d1", "execute", "fiat-busqueda", "--remote", "--file", tmp_path]
    result = subprocess.run(cmd, cwd=worker_dir, capture_output=True, text=True, timeout=180)
    Path(tmp_path).unlink()

    if result.returncode != 0:
        logger.error(f"wrangler stderr:\n{result.stderr[:1500]}")
        raise RuntimeError("wrangler falló")

    logger.info(f"✓ {len(rows)} h2h sincronizados a D1")
    print(f"OK: {len(rows)} filas sincronizadas")


if __name__ == "__main__":
    main()
