"""
Fix retroactivo del bajón del 2026-05-07.

CONTEXTO:
  El 7 de mayo se aplicó una integración Senado que metió iniciativas
  mal clasificadas a la BD. Se rollbackeó horas después (commits
  e4ad743 → dbdaf14 → 3338b93), pero el pipeline corrió mientras la
  integración estaba activa y guardó scores deprimidos para las 19
  categorías. El bajón es ~10-25 puntos uniforme — firma de bug,
  no ciclo legislativo real.

ENFOQUE:
  Recalcular el 7 de mayo como interpolación lineal entre el 6 de mayo
  (último día pre-bug) y el 8 de mayo (primer día post-rollback) para
  cada categoría y cada sub-componente. Marcar en `detalle` como
  'fix_bug_integracion_senado_2026-05-07' para trazabilidad.

  No usamos backfill_scores_diario.py porque ese script setea media+trends
  a 0 (modo "gap redistribuido") y eso DEFLATARÍA aún más el día, en
  lugar de arreglarlo.

  Interpolación es honesto: los scores diarios cambian poco día a día
  (~2 pts en promedio), y el promedio de los dos vecinos es la mejor
  estimación que tenemos del valor real ese día.

USO:
  python3 scripts/fix_scores_2026_05_07.py
  python3 scripts/fix_scores_2026_05_07.py --dry-run
"""
import argparse
import logging
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


FECHA_BUG = "2026-05-07"
FECHA_PRE = "2026-05-06"
FECHA_POST = "2026-05-08"

COMPONENTES = [
    "score_total",
    "score_media",
    "score_trends",
    "score_congreso",
    "score_mananera",
    "score_urgencia",
    "score_dominancia",
]


def color_para(score_total):
    if score_total >= 70:
        return "verde"
    if score_total >= 40:
        return "amarillo"
    return "rojo"


def main():
    ap = argparse.ArgumentParser(description="Fix scores 2026-05-07")
    ap.add_argument("--dry-run", action="store_true",
                    help="No escribe a BD, solo reporta")
    args = ap.parse_args()

    from db import get_connection

    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # Verificar que tenemos los dos vecinos
    pre_count = conn.execute(
        "SELECT COUNT(*) FROM scores WHERE fecha = ?", (FECHA_PRE,)
    ).fetchone()[0]
    post_count = conn.execute(
        "SELECT COUNT(*) FROM scores WHERE fecha = ?", (FECHA_POST,)
    ).fetchone()[0]
    bug_count = conn.execute(
        "SELECT COUNT(*) FROM scores WHERE fecha = ?", (FECHA_BUG,)
    ).fetchone()[0]

    logger.info(f"Pre  ({FECHA_PRE}): {pre_count} filas")
    logger.info(f"Bug  ({FECHA_BUG}): {bug_count} filas")
    logger.info(f"Post ({FECHA_POST}): {post_count} filas")

    if pre_count == 0 or post_count == 0:
        logger.error("Faltan vecinos pre/post. No se puede interpolar.")
        return 1

    # Leer scores de los dos vecinos
    pre_rows = {
        r["categoria"]: dict(r)
        for r in conn.execute(
            "SELECT * FROM scores WHERE fecha = ?", (FECHA_PRE,)
        ).fetchall()
    }
    post_rows = {
        r["categoria"]: dict(r)
        for r in conn.execute(
            "SELECT * FROM scores WHERE fecha = ?", (FECHA_POST,)
        ).fetchall()
    }

    categorias = set(pre_rows.keys()) & set(post_rows.keys())
    logger.info(f"Categorías a interpolar: {len(categorias)}")

    total_actualizadas = 0
    for cat in sorted(categorias):
        pre = pre_rows[cat]
        post = post_rows[cat]

        # Interpolación lineal por componente
        nuevos = {}
        for comp in COMPONENTES:
            v_pre = pre.get(comp)
            v_post = post.get(comp)
            if v_pre is None and v_post is None:
                nuevos[comp] = None
            elif v_pre is None:
                nuevos[comp] = v_post
            elif v_post is None:
                nuevos[comp] = v_pre
            else:
                nuevos[comp] = round((v_pre + v_post) / 2, 2)

        color = color_para(nuevos["score_total"] or 0)
        detalle = "fix_bug_integracion_senado_2026-05-07"

        logger.info(
            f"  {cat:30s} pre={pre['score_total']:.1f} "
            f"post={post['score_total']:.1f} → "
            f"nuevo={nuevos['score_total']:.1f}"
        )

        if args.dry_run:
            continue

        # UPSERT
        conn.execute("""
            INSERT INTO scores
                (categoria, score_total, score_media, score_trends,
                 score_congreso, score_mananera, score_urgencia,
                 score_dominancia, color, fecha, detalle)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(categoria, fecha) DO UPDATE SET
                score_total = excluded.score_total,
                score_media = excluded.score_media,
                score_trends = excluded.score_trends,
                score_congreso = excluded.score_congreso,
                score_mananera = excluded.score_mananera,
                score_urgencia = excluded.score_urgencia,
                score_dominancia = excluded.score_dominancia,
                color = excluded.color,
                detalle = excluded.detalle
        """, (
            cat,
            nuevos["score_total"],
            nuevos["score_media"],
            nuevos["score_trends"],
            nuevos["score_congreso"],
            nuevos["score_mananera"],
            nuevos["score_urgencia"],
            nuevos["score_dominancia"],
            color, FECHA_BUG, detalle,
        ))
        total_actualizadas += 1

    if not args.dry_run:
        conn.commit()
        logger.info(f"OK: {total_actualizadas} scores actualizadas para {FECHA_BUG}")
    else:
        logger.info(f"DRY-RUN: {len(categorias)} scores serían actualizadas")

    return 0


if __name__ == "__main__":
    sys.exit(main())
