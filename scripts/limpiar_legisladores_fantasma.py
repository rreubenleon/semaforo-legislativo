"""
Borra registros fantasma de `legisladores` cuyo nombre es texto basura.

Casos conocidos:
  - id=685: nombre = "Enrique Vargas del Villar, del Grupo Parlamentario...,
    con proyecto de decreto por el que se reforman..." (texto completo de
    iniciativa que se insertó como nombre por parser erróneo).

Estrategia: cualquier 'nombre' con > 80 caracteres no es un nombre de
persona real. También cualquiera que contenga substrings típicos de
títulos de iniciativa: "con proyecto de decreto", "se reforman", "Grupo
Parlamentario", "Se dio turno".

Limpia tanto BD local como D1 (vía wrangler).

Uso:
  python3 scripts/limpiar_legisladores_fantasma.py
  python3 scripts/limpiar_legisladores_fantasma.py --dry-run
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
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from db import get_connection
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # Buscar fantasmas
    fantasmas = conn.execute("""
        SELECT id, nombre, camara FROM legisladores
        WHERE LENGTH(nombre) > 80
           OR LOWER(nombre) LIKE '%proyecto de decreto%'
           OR LOWER(nombre) LIKE '%grupo parlamentario%, con%'
           OR LOWER(nombre) LIKE '%se dio turno%'
        ORDER BY id
    """).fetchall()

    logger.info(f"Encontrados {len(fantasmas)} legisladores fantasma:")
    for f in fantasmas:
        logger.info(f"  id={f['id']} cam={f['camara']} nombre={f['nombre'][:80]}…")

    if not fantasmas:
        return 0

    ids = [f["id"] for f in fantasmas]
    placeholders = ",".join("?" * len(ids))

    if args.dry_run:
        # Contar referencias en tablas relacionadas
        for tabla, col in [
            ("actividad_legislador", "legislador_id"),
            ("legisladores_elo", "legislador_id"),
        ]:
            try:
                n = conn.execute(
                    f"SELECT COUNT(*) FROM {tabla} WHERE {col} IN ({placeholders})",
                    ids,
                ).fetchone()[0]
                logger.info(f"  → {tabla}.{col}: {n} referencias")
            except sqlite3.OperationalError:
                pass
        logger.info("[DRY-RUN] No se borra nada")
        return 0

    # Borrar en BD local
    # 1. Referencias en actividad_legislador (pueden ser huérfanas — borrar)
    n_act = conn.execute(
        f"DELETE FROM actividad_legislador WHERE legislador_id IN ({placeholders})",
        ids,
    ).rowcount
    logger.info(f"actividad_legislador: {n_act} filas borradas")

    # 2. ELO
    try:
        n_elo = conn.execute(
            f"DELETE FROM legisladores_elo WHERE legislador_id IN ({placeholders})",
            ids,
        ).rowcount
        logger.info(f"legisladores_elo: {n_elo} filas borradas")
    except sqlite3.OperationalError:
        pass

    # 3. Trayectoria, perfil, etc (opcional, defensivo)
    for tabla in ("legisladores_trayectoria", "legisladores_perfil",
                  "legisladores_organos_gobierno"):
        try:
            conn.execute(
                f"DELETE FROM {tabla} WHERE legislador_id IN ({placeholders})",
                ids,
            )
        except sqlite3.OperationalError:
            pass

    # 4. La tabla legisladores misma
    n_leg = conn.execute(
        f"DELETE FROM legisladores WHERE id IN ({placeholders})",
        ids,
    ).rowcount
    logger.info(f"legisladores: {n_leg} filas borradas")

    conn.commit()

    # Borrar en D1 (vía wrangler)
    sql_d1 = "\n".join([
        f"DELETE FROM legisladores_elo WHERE legislador_id IN ({placeholders.replace('?', '{}').format(*ids)});",
        f"DELETE FROM legisladores_stats WHERE legislador_id IN ({placeholders.replace('?', '{}').format(*ids)});",
        f"DELETE FROM legisladores_hit_rate WHERE legislador_id IN ({placeholders.replace('?', '{}').format(*ids)});",
        f"DELETE FROM legisladores WHERE id IN ({placeholders.replace('?', '{}').format(*ids)});",
    ])

    worker_dir = ROOT / "worker"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tmp:
        tmp.write(sql_d1)
        tmp_path = tmp.name

    try:
        result = subprocess.run(
            ["npx", "wrangler", "d1", "execute", "fiat-busqueda",
             "--remote", "--file", tmp_path],
            cwd=worker_dir, capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            logger.warning(f"wrangler stderr: {result.stderr[:600]}")
        else:
            logger.info("✓ D1 actualizado: fantasmas borrados")
    finally:
        Path(tmp_path).unlink()

    return 0


if __name__ == "__main__":
    sys.exit(main())
