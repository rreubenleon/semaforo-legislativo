"""
Sync de sil_documentos_historicos (LXIV+LXV de los reelectos) → D1.

Lee semaforo.db local y crea/upsertea en D1 (`fiat-busqueda`). Después
de verificar en D1, el caller puede DROP la tabla local para mantener
semaforo.db lean (no se usa en pipeline activo).

El Worker puede consumir esto via endpoint nuevo /historicos o
integrado en /radar para mostrar trayectoria histórica del legislador.

Uso:
    python scripts/sync_historicos_d1.py --dry-run
    python scripts/sync_historicos_d1.py
    python scripts/sync_historicos_d1.py --drop-local   # post-sync, libera SQLite
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

BATCH = 30  # batches conservadores (rows pueden tener mucho texto)


def sql_escape(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v).replace("'", "''")
    # D1 tiene límites de tamaño por statement; truncar campos largos
    if len(s) > 2000:
        s = s[:2000]
    return "'" + s + "'"


def generar_sql_chunks(dry_run=False):
    """Genera múltiples archivos SQL si la tabla es muy grande.
    D1 tiene límite de ~100KB por archivo via wrangler."""
    conn = sqlite3.connect(str(DB))
    rows = conn.execute("""
        SELECT legislador_id, sil_referencia, tipo_asunto, denominacion,
               sub_clasificacion, camara, fecha_presentacion, presentador,
               partido, legislatura, turnado_a, estatus, tema, url_detalle
        FROM sil_documentos_historicos
        ORDER BY legislador_id, fecha_presentacion
    """).fetchall()
    n_total = len(rows)
    logger.info(f"Leyendo {n_total:,} sil_documentos_historicos desde SQLite local")

    if n_total == 0:
        logger.warning("No hay datos en sil_documentos_historicos. Nada que sincronizar.")
        return 0

    # Schema: drop + recreate (idempotente)
    schema_sql = [
        "DROP TABLE IF EXISTS sil_documentos_historicos;",
        """CREATE TABLE sil_documentos_historicos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            legislador_id INTEGER NOT NULL,
            sil_referencia TEXT,
            tipo_asunto TEXT,
            denominacion TEXT,
            sub_clasificacion TEXT,
            camara TEXT,
            fecha_presentacion TEXT,
            presentador TEXT,
            partido TEXT,
            legislatura TEXT NOT NULL,
            turnado_a TEXT,
            estatus TEXT,
            tema TEXT,
            url_detalle TEXT
        );""",
        "CREATE INDEX idx_hist_d1_leg ON sil_documentos_historicos(legislador_id);",
        "CREATE INDEX idx_hist_d1_legis ON sil_documentos_historicos(legislatura);",
        "CREATE INDEX idx_hist_d1_fecha ON sil_documentos_historicos(fecha_presentacion);",
    ]

    # Crear primero el schema
    chunks = ["\n".join(schema_sql)]

    # Insert batches
    inserts = []
    for i in range(0, n_total, BATCH):
        batch = rows[i:i + BATCH]
        vals = []
        for r in batch:
            (lid, ref, tipo, denom, subc, cam, fecha, pres, partido,
             legis, turn, est, tema, url) = r
            vals.append(
                f"({lid}, {sql_escape(ref)}, {sql_escape(tipo)}, "
                f"{sql_escape(denom)}, {sql_escape(subc)}, {sql_escape(cam)}, "
                f"{sql_escape(fecha)}, {sql_escape(pres)}, {sql_escape(partido)}, "
                f"{sql_escape(legis)}, {sql_escape(turn)}, {sql_escape(est)}, "
                f"{sql_escape(tema)}, {sql_escape(url)})"
            )
        inserts.append(
            "INSERT INTO sil_documentos_historicos "
            "(legislador_id, sil_referencia, tipo_asunto, denominacion, "
            "sub_clasificacion, camara, fecha_presentacion, presentador, "
            "partido, legislatura, turnado_a, estatus, tema, url_detalle) VALUES\n"
            + ",\n".join(vals) + ";"
        )

    # Agrupar inserts en chunks de ~80KB cada uno
    current = []
    current_size = 0
    for ins in inserts:
        if current_size + len(ins) > 80000 and current:
            chunks.append("\n".join(current))
            current = []
            current_size = 0
        current.append(ins)
        current_size += len(ins) + 1
    if current:
        chunks.append("\n".join(current))

    logger.info(f"Generados {len(chunks)} chunks SQL ({sum(len(c) for c in chunks):,} chars total)")

    if dry_run:
        print(f"Schema chunk (primeros 1000 chars):")
        print(chunks[0][:1000])
        print(f"\nFirst insert chunk (primeros 800 chars):")
        if len(chunks) > 1:
            print(chunks[1][:800])
        print(f"\nTotal chunks: {len(chunks)}")
        return n_total

    # Ejecutar cada chunk via wrangler
    worker_dir = ROOT / "worker"
    for idx, chunk in enumerate(chunks):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tmp:
            tmp.write(chunk)
            tmp_path = tmp.name

        cmd = [
            "npx", "wrangler", "d1", "execute", "fiat-busqueda",
            "--remote", "--file", tmp_path,
        ]
        logger.info(f"  Ejecutando chunk {idx+1}/{len(chunks)} ({len(chunk):,} chars)…")
        result = subprocess.run(cmd, cwd=worker_dir, capture_output=True, text=True, timeout=180)
        Path(tmp_path).unlink()

        if result.returncode != 0:
            logger.error(f"wrangler chunk {idx+1} falló:\n{result.stderr[:1500]}")
            raise RuntimeError(f"wrangler falló en chunk {idx+1}")

    logger.info(f"✓ {n_total:,} sil_documentos_historicos sincronizados a D1")
    return n_total


def drop_local(conn: sqlite3.Connection):
    """Drop la tabla local SQLite tras sync exitoso."""
    n = conn.execute("SELECT COUNT(*) FROM sil_documentos_historicos").fetchone()[0]
    conn.execute("DROP TABLE IF EXISTS sil_documentos_historicos")
    conn.commit()
    # Reclamar espacio
    conn.execute("VACUUM")
    logger.info(f"✓ Tabla local sil_documentos_historicos eliminada ({n:,} rows liberadas)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--drop-local", action="store_true",
                       help="Después del sync, DROP la tabla local SQLite")
    args = parser.parse_args()

    n = generar_sql_chunks(dry_run=args.dry_run)
    if args.dry_run:
        print(f"\n  (dry-run) Habría sincronizado {n:,} rows")
        return

    if args.drop_local and n > 0:
        conn = sqlite3.connect(str(DB))
        drop_local(conn)


if __name__ == "__main__":
    main()
