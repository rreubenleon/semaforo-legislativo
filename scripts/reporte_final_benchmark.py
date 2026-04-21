"""
Reporte consolidado final del benchmark v1 vs v2 sobre los 13 casos.

Corre los tres scripts en cadena:
  1. ground_truth_benchmark.py    → artículos, instrumentos, ratio post-pico
  2. recalcular_score_media_historico.py → V1 recalc + V2 z-score rolling

Produce una tabla única con toda la evidencia y una interpretación final.

Uso:
    python scripts/reporte_final_benchmark.py
"""
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
DB = ROOT / "semaforo.db"

from scripts.ground_truth_benchmark import (
    CASOS, contar_articulos_ventana, contar_instrumentos,
    actividad_post_pico, scores_en_ventana,
)
from scripts.recalcular_score_media_historico import recalcular_ventana


def evaluar_caso_completo(conn, key, caso):
    desde, hasta = caso["ventana_media"]
    cat = caso["categoria"]

    # 1. Artículos en medios
    art_por_fecha, fuentes = contar_articulos_ventana(
        conn, caso["keywords_media"], desde, hasta
    )
    total_art = sum(art_por_fecha.values())
    top_dias = sorted(art_por_fecha.items(), key=lambda kv: -kv[1])[:3]
    fecha_pico = top_dias[0][0] if top_dias else None

    # 2. Instrumentos legislativos
    instr = contar_instrumentos(conn, caso["keywords_legis"], desde, hasta)
    total_instr = len(instr["gaceta"]) + len(instr["sil"])

    # 3. Actividad post-pico
    ratio = None
    hit_reactivo = False
    if fecha_pico:
        act = actividad_post_pico(conn, cat, fecha_pico, ventana_dias=30)
        ratio = act["ratio"]
        hit_reactivo = act["hit"]

    # 4. V1 histórico (de tabla scores)
    scores_hist = scores_en_ventana(conn, cat, desde, hasta)
    v1_hist_medias = [s["media"] for s in scores_hist if s["media"] is not None]
    v1_hist_totales = [s["total"] for s in scores_hist if s["total"] is not None]

    # 5. V1 recalculado + V2 z-score (sobre ventana)
    try:
        serie_recalc = recalcular_ventana(conn, cat, desde, hasta, paso_dias=5)
    except Exception as e:
        serie_recalc = []
    v1_recalc = [s[1] for s in serie_recalc]
    v2_zscore = [s[2] for s in serie_recalc]

    return {
        "key": key,
        "caso": caso["titulo"],
        "tipo": caso["tipo"],
        "categoria": cat,
        "art_total": total_art,
        "art_dias": len(art_por_fecha),
        "art_fuentes": len(fuentes),
        "instr": total_instr,
        "fecha_pico": fecha_pico,
        "ratio_post": ratio,
        "hit_reactivo": hit_reactivo,
        "v1_hist_max": max(v1_hist_medias) if v1_hist_medias else 0,
        "v1_hist_avg": sum(v1_hist_medias) / len(v1_hist_medias) if v1_hist_medias else 0,
        "v1_hist_sat": 100 * sum(1 for v in v1_hist_medias if v >= 99.5) / len(v1_hist_medias) if v1_hist_medias else 0,
        "v1_total_hist_max": max(v1_hist_totales) if v1_hist_totales else 0,
        "v1_recalc_max": max(v1_recalc) if v1_recalc else 0,
        "v1_recalc_avg": sum(v1_recalc) / len(v1_recalc) if v1_recalc else 0,
        "v2_max": max(v2_zscore) if v2_zscore else 0,
        "v2_avg": sum(v2_zscore) / len(v2_zscore) if v2_zscore else 0,
    }


def clasificar_outcome(r):
    """
    Clasifica el caso en una de 5 categorías de outcome:
      - TP_detectado: positivo con hit reactivo y score alto
      - TP_subdetectado: positivo con hit reactivo pero score bajo
      - TP_saturado: positivo con saturación alta (ruido escondido)
      - TN_correcto: negativo con score bajo
      - FP: negativo con score alto (falso positivo)
    """
    max_total = max(r["v1_total_hist_max"], r["v1_recalc_max"] * 0.3 + 40)  # aproximación
    v1_total = r["v1_total_hist_max"]

    if r["tipo"] == "positivo":
        if r["hit_reactivo"] and v1_total >= 50:
            if r["v1_hist_sat"] >= 20:
                return "TP_SATURADO"
            return "TP_DETECTADO"
        elif r["hit_reactivo"] and v1_total < 50:
            return "TP_SUBDETECTADO"
        elif not r["hit_reactivo"] and v1_total >= 50:
            return "TP_INCERTO"
        else:
            return "TP_PASADO"
    else:  # negativo
        if r["hit_reactivo"] and v1_total >= 60:
            # Es "ruido con reacción": no es necesariamente falso positivo
            return "TN_CON_REACCION"
        elif v1_total >= 60:
            return "FP_RUIDO"
        else:
            return "TN_CORRECTO"


def imprimir_reporte(filas):
    print()
    print("═" * 160)
    print("  REPORTE FINAL — Benchmark ground truth (13 casos)")
    print("═" * 160)
    print()
    print(f"  {'Caso':<42}  {'T':1}  {'Art':>5} {'Fue':>3}  {'Instr':>5}  {'Ratio':>6}  {'Hit':3}  {'V1hMx':>6} {'V1hAvg':>6} {'Sat%':>4}  {'V1rMx':>6} {'V2Mx':>6}  {'Outcome':<18}")
    print("─" * 160)
    for r in filas:
        t = "+" if r["tipo"] == "positivo" else "−"
        outcome = clasificar_outcome(r)
        ratio_str = f"{r['ratio_post']:.2f}" if isinstance(r['ratio_post'], (int, float)) else str(r['ratio_post']) if r['ratio_post'] else "—"
        print(
            f"  {r['caso'][:42]:<42}  {t:1}  "
            f"{r['art_total']:>5} {r['art_fuentes']:>3}  {r['instr']:>5}  "
            f"{ratio_str:>6}  {'SI' if r['hit_reactivo'] else 'NO':3}  "
            f"{r['v1_hist_max']:>6.1f} {r['v1_hist_avg']:>6.1f} {r['v1_hist_sat']:>3.0f}%  "
            f"{r['v1_recalc_max']:>6.1f} {r['v2_max']:>6.1f}  "
            f"{outcome:<18}"
        )
    print("═" * 160)
    print()

    # Resumen de outcomes
    outcomes = {}
    for r in filas:
        o = clasificar_outcome(r)
        outcomes.setdefault(o, []).append(r["caso"][:40])

    print("  RESUMEN DE OUTCOMES:")
    for cat in ["TP_DETECTADO", "TP_SATURADO", "TP_SUBDETECTADO", "TP_PASADO", "TP_INCERTO",
                "TN_CORRECTO", "TN_CON_REACCION", "FP_RUIDO"]:
        if cat not in outcomes:
            continue
        print(f"    {cat}: {len(outcomes[cat])}")
        for c in outcomes[cat]:
            print(f"      · {c}")
    print()

    # Stats agregadas
    positivos = [r for r in filas if r["tipo"] == "positivo"]
    negativos = [r for r in filas if r["tipo"] == "negativo"]

    print("  COMPARATIVO V1 vs V2 (promedios):")
    print(f"                        V1_hist   V1_recalc  V2_zscore")
    if positivos:
        v1h = sum(r["v1_hist_max"] for r in positivos) / len(positivos)
        v1r = sum(r["v1_recalc_max"] for r in positivos) / len(positivos)
        v2 = sum(r["v2_max"] for r in positivos) / len(positivos)
        print(f"    Positivos max_avg:   {v1h:>6.1f}    {v1r:>6.1f}     {v2:>6.1f}   (debería ser alto)")
    if negativos:
        v1h = sum(r["v1_hist_max"] for r in negativos) / len(negativos)
        v1r = sum(r["v1_recalc_max"] for r in negativos) / len(negativos)
        v2 = sum(r["v2_max"] for r in negativos) / len(negativos)
        print(f"    Negativos max_avg:   {v1h:>6.1f}    {v1r:>6.1f}     {v2:>6.1f}   (debería ser bajo)")

    sat_v1 = sum(r["v1_hist_sat"] for r in filas) / len(filas)
    print(f"\n    Saturación promedio (v1_hist): {sat_v1:.1f}%")
    print()


def main():
    conn = sqlite3.connect(str(DB))
    print("  Corriendo benchmark consolidado sobre 13 casos...")
    print("  (Cada caso requiere recalcular score_media — tarda ~30s)")
    print()

    filas = []
    for i, (key, caso) in enumerate(CASOS.items(), 1):
        print(f"  [{i:2}/{len(CASOS)}] {caso['titulo'][:50]}...", end="", flush=True)
        try:
            r = evaluar_caso_completo(conn, key, caso)
            filas.append(r)
            print(" ✓")
        except Exception as e:
            print(f" ✗ error: {e}")

    imprimir_reporte(filas)


if __name__ == "__main__":
    main()
