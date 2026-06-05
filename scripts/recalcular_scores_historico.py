"""Recalcula scores históricos con la lógica nueva (fixes aplicados),
usando las FUNCIONES REALES de producción ancladas por fecha.

Componentes recalculados (afectados por los fixes):
  - media     (word-boundary, sin ISR→Israel)
  - congreso  (todos los keywords)
  - dominancia(divisores 550/150)
Componentes NO recalculados (se toman del score guardado de ese día,
no cambiaron): trends, mananera, urgencia.
Pesos: por categoría (data/pesos_por_categoria.json).

Seguridad:
  - --dry-run: solo reporta, no escribe.
  - default: escribe a tabla scores_recalc (NO toca scores).
  - --aplicar: respalda scores→scores_backup_PREv y sobrescribe scores.

Solo recalcula opción C: fecha >= 2026-02-13 (cobertura mediática rica).
NO Haiku. Idempotente.
"""
from __future__ import annotations
import argparse, json, sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from db import get_connection
from config import CATEGORIAS, SCORING, obtener_keywords_categoria
from scrapers.medios import obtener_score_media
from scrapers.gaceta import obtener_score_congreso
from api.correlacion import calcular_dominancia_discursiva, asignar_color

DESDE = "2026-02-13"
PESOS_CAT = json.loads((ROOT / "data" / "pesos_por_categoria.json").read_text())["categorias"]
PG = SCORING["pesos"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--aplicar", action="store_true", help="sobrescribe scores (con respaldo)")
    args = ap.parse_args()
    conn = get_connection()

    fechas = [r[0] for r in conn.execute(
        "SELECT DISTINCT fecha FROM scores WHERE fecha >= ? ORDER BY fecha", (DESDE,)).fetchall()]
    print(f"Fechas a recalcular: {len(fechas)} ({fechas[0]} → {fechas[-1]})")

    filas = []
    for idx, f in enumerate(fechas):
        if idx % 20 == 0:
            print(f"  {idx}/{len(fechas)} ({f})")
        for cat in CATEGORIAS:
            old = conn.execute(
                "SELECT score_total,score_media,score_trends,score_congreso,"
                "score_mananera,score_urgencia,score_dominancia FROM scores "
                "WHERE categoria=? AND fecha=?", (cat, f)).fetchone()
            if not old:
                continue
            tot_old, me_old, tr, con_old, man, urg, dom_old = [x or 0 for x in old]
            kws = obtener_keywords_categoria(cat)
            me = obtener_score_media(kws, ref_date=f)
            con = obtener_score_congreso(kws, ref_date=f)
            dom = calcular_dominancia_discursiva(cat, kws, ref_date=f)
            pesos = PESOS_CAT.get(cat, {}).get("pesos", PG)
            tot = min(
                pesos["media"] * me + pesos["trends"] * tr + pesos["congreso"] * con
                + pesos["mananera"] * man + pesos["urgencia"] * urg
                + pesos["dominancia"] * dom + pesos.get("legisladores", 0) * 0, 100)
            filas.append({
                "categoria": cat, "fecha": f, "score_total": round(tot, 2),
                "score_media": round(me, 2), "score_trends": tr, "score_congreso": round(con, 2),
                "score_mananera": man, "score_urgencia": urg, "score_dominancia": round(dom, 2),
                "color": asignar_color(tot), "tot_old": tot_old,
            })

    # Stats
    import statistics
    news = [x["score_total"] for x in filas]
    olds = [x["tot_old"] for x in filas]
    print(f"\nViejo : std={statistics.pstdev(olds):.1f} cluster68-77={sum(1 for v in olds if 68<=v<=77)}/{len(olds)}")
    print(f"Nuevo : std={statistics.pstdev(news):.1f} cluster68-77={sum(1 for v in news if 68<=v<=77)}/{len(news)}")

    if args.dry_run:
        print("[DRY-RUN] no se escribe")
        return

    # Escribir a scores_recalc (NO toca scores)
    conn.execute("DROP TABLE IF EXISTS scores_recalc")
    conn.execute("""CREATE TABLE scores_recalc(categoria TEXT, score_total REAL, score_media REAL,
        score_trends REAL, score_congreso REAL, score_mananera REAL, score_urgencia REAL,
        score_dominancia REAL, color TEXT, fecha TEXT, UNIQUE(categoria,fecha))""")
    for x in filas:
        conn.execute("INSERT INTO scores_recalc VALUES (?,?,?,?,?,?,?,?,?,?)",
                     (x["categoria"], x["score_total"], x["score_media"], x["score_trends"],
                      x["score_congreso"], x["score_mananera"], x["score_urgencia"],
                      x["score_dominancia"], x["color"], x["fecha"]))
    conn.commit()
    print(f"Escritas {len(filas)} filas a scores_recalc (NO se tocó scores)")

    if args.aplicar:
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        conn.execute(f"CREATE TABLE scores_backup_{ts} AS SELECT * FROM scores")
        for x in filas:
            conn.execute("""UPDATE scores SET score_total=?, score_media=?, score_congreso=?,
                score_dominancia=?, color=? WHERE categoria=? AND fecha=?""",
                (x["score_total"], x["score_media"], x["score_congreso"], x["score_dominancia"],
                 x["color"], x["categoria"], x["fecha"]))
        conn.commit()
        print(f"APLICADO a scores. Respaldo en scores_backup_{ts}")


if __name__ == "__main__":
    main()
