"""
Sync de `legisladores.comisiones_cargo` y `comisiones` desde SQLite local
a Cloudflare D1.

Necesario porque:
  - El Worker /radar lee `comisiones_cargo` desde D1
  - Los scripts que pueblan ese campo (backfill_comisiones_senado.py,
    refresh_cargos_senadores.py) solo escriben a SQLite local
  - Sin sync, el frontend ve datos viejos / vacíos

Uso:
  python scripts/sync_legisladores_cargo_d1.py
  python scripts/sync_legisladores_cargo_d1.py --solo-senado
  python scripts/sync_legisladores_cargo_d1.py --dry-run --limit 5
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def esc(s):
    if s is None: return 'NULL'
    return "'" + str(s).replace("'", "''") + "'"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--solo-senado", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--batch-size", type=int, default=30)
    args = p.parse_args()

    conn = sqlite3.connect(str(DB))
    where = "1=1"
    if args.solo_senado:
        where = "camara LIKE '%enado%'"
    sql = f"""
        SELECT id, comisiones_cargo, comisiones
          FROM legisladores
         WHERE {where}
           AND (comisiones_cargo IS NOT NULL AND comisiones_cargo <> '')
    """
    if args.limit:
        sql += f" LIMIT {int(args.limit)}"
    rows = conn.execute(sql).fetchall()
    logger.info(f"A sincronizar: {len(rows)} legisladores con cargo no vacío")

    if not rows:
        return

    statements = []
    for leg_id, cc, _com in rows:
        # Solo sync comisiones_cargo (la columna `comisiones` no existe en D1
        # por diseño — el frontend la deriva del cargo)
        statements.append(
            f"UPDATE legisladores SET "
            f"comisiones_cargo = {esc(cc)} "
            f"WHERE id = {leg_id};"
        )

    if args.dry_run:
        print("=== DRY RUN — primeros 3 statements ===")
        for s in statements[:3]:
            print(s[:200])
        return

    # Ejecutar en batches
    for i in range(0, len(statements), args.batch_size):
        batch = statements[i:i + args.batch_size]
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
            f.write("\n".join(batch))
            sql_file = f.name
        try:
            r = subprocess.run(
                ["npx", "wrangler", "d1", "execute", "fiat-busqueda",
                 "--file", sql_file, "--remote"],
                capture_output=True, text=True, timeout=60,
                cwd=str(ROOT / "worker"),
            )
            if r.returncode == 0:
                logger.info(f"  Batch {i // args.batch_size + 1}: {len(batch)} updates OK")
            else:
                logger.error(f"  Batch {i // args.batch_size + 1} falló: {r.stderr[:300]}")
        finally:
            Path(sql_file).unlink(missing_ok=True)

    logger.info(f"Sync D1 completado: {len(statements)} legisladores")


if __name__ == "__main__":
    main()
