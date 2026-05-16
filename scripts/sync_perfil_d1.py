"""
Sync de legisladores_perfil (biografía, año nacimiento, estudios,
profesión, foto, fuente) desde SQLite local → Cloudflare D1.

El Worker /radar hace LEFT JOIN legisladores_perfil p. Sin este sync
la tabla D1 queda vacía/vieja y la card muestra "perfil no scrapeado"
aunque BD local SÍ lo tenga (caso 15-may: 662 guardados local, D1 en 70%).

sync_legisladores_cargo_d1.py NO cubre esta tabla — solo comisiones_cargo.

Uso:
  python3 scripts/sync_perfil_d1.py
  python3 scripts/sync_perfil_d1.py --dry-run
"""
import argparse
import logging
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

BATCH = 100


def esc(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = sqlite3.connect(str(ROOT / "semaforo.db"))
    rows = conn.execute("""
        SELECT legislador_id, biografia, anio_nacimiento, genero,
               profesion, estudios, twitter_handle, web_personal,
               foto_hd_url, wikipedia_url, fuente_scraping, fecha_scraping
        FROM legisladores_perfil
        WHERE legislador_id IS NOT NULL
    """).fetchall()
    logger.info(f"Leyendo {len(rows)} legisladores_perfil desde SQLite local")

    if not rows:
        logger.warning("Sin filas en legisladores_perfil. Nada que sincronizar.")
        return 0

    # Recrear tabla en D1 desde cero (idempotente, schema fijo).
    sql_parts = [
        "DROP TABLE IF EXISTS legisladores_perfil;",
        """CREATE TABLE legisladores_perfil (
            legislador_id INTEGER PRIMARY KEY,
            biografia TEXT,
            anio_nacimiento INTEGER,
            genero TEXT,
            profesion TEXT,
            estudios TEXT,
            twitter_handle TEXT,
            web_personal TEXT,
            foto_hd_url TEXT,
            wikipedia_url TEXT,
            fuente_scraping TEXT,
            fecha_scraping TEXT
        );""",
        "CREATE INDEX idx_perfil_leg ON legisladores_perfil(legislador_id);",
    ]

    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        vals = []
        for r in batch:
            (lid, bio, anio, gen, prof, est, tw, web,
             foto, wiki, fuente, fecha) = r
            vals.append(
                f"({lid}, {esc(bio)}, {esc(anio)}, {esc(gen)}, {esc(prof)}, "
                f"{esc(est)}, {esc(tw)}, {esc(web)}, {esc(foto)}, "
                f"{esc(wiki)}, {esc(fuente)}, {esc(fecha)})"
            )
        sql_parts.append(
            "INSERT INTO legisladores_perfil "
            "(legislador_id, biografia, anio_nacimiento, genero, profesion, "
            "estudios, twitter_handle, web_personal, foto_hd_url, "
            "wikipedia_url, fuente_scraping, fecha_scraping) VALUES\n"
            + ",\n".join(vals) + ";"
        )

    sql = "\n".join(sql_parts)
    if args.dry_run:
        print(sql[:1500])
        print(f"\n... ({len(sql)} chars, {len(rows)} filas)")
        return 0

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tmp:
        tmp.write(sql)
        tmp_path = tmp.name

    result = subprocess.run(
        ["npx", "wrangler", "d1", "execute", "fiat-busqueda",
         "--remote", "--file", tmp_path],
        cwd=ROOT / "worker", capture_output=True, text=True, timeout=180,
    )
    Path(tmp_path).unlink()

    if result.returncode != 0:
        logger.error(f"wrangler stderr:\n{result.stderr[:1500]}")
        raise RuntimeError("wrangler falló")

    logger.info(f"✓ {len(rows)} legisladores_perfil sincronizados a D1")
    return 0


if __name__ == "__main__":
    sys.exit(main())
