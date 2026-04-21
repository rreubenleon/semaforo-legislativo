"""
Simulación retroactiva de score_media v2 (z-score normalizado) sobre la
serie histórica existente. No toca la DB de producción — solo calcula qué
habría dicho v2 en cada fecha usando la propia tabla `scores` como base.

Modelo v2:
    z = (score_media_actual - baseline_60d_cat) / std_60d_cat
    score_media_v2 = 50 + 50 * tanh(z / 2)

Interpretación:
    - 50 = exactamente al promedio de 60 días
    - >70 = spike significativo (>1σ sobre promedio)
    - <30 = por debajo del promedio (categoría en silencio)

Tiene el efecto deseado: una categoría que siempre está en 100 queda en 50
porque 100 ES su baseline; solo sube cuando HOY hay MÁS de lo usual.

Uso:
    python scripts/simular_score_media_v2.py --caso reforma_electoral_2026
    python scripts/simular_score_media_v2.py --tabla    # resumen de los 13 casos
"""
import argparse
import math
import sqlite3
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
sys.path.insert(0, str(ROOT))

from scripts.ground_truth_benchmark import CASOS, actividad_post_pico  # reusar estructura


def calcular_scores_v2_categoria(conn, categoria: str, ventana: int = 60):
    """
    Para cada día con score registrado en esta categoría, calcula score_media_v2
    usando z-score sobre ventana rolling de N días previos.
    Retorna dict fecha → {v1_media, v2_media, v1_total, v2_total}.
    """
    rows = conn.execute(
        """
        SELECT fecha, score_total, score_media, score_trends, score_congreso,
               score_mananera, score_urgencia, score_dominancia
        FROM scores WHERE categoria = ?
        ORDER BY fecha
        """,
        (categoria,),
    ).fetchall()

    resultado = {}
    medias_historicas = []  # rolling buffer

    # Pesos actuales del score_total (de config.py SCORING)
    PESOS = {
        "media": 0.20, "trends": 0.15, "congreso": 0.25,
        "mananera": 0.10, "urgencia": 0.15, "dominancia": 0.15,
    }

    for fecha, total, media, trends, congreso, mananera, urgencia, dominancia in rows:
        if media is None:
            continue
        medias_historicas.append(media)
        # Mantener solo últimos N días
        if len(medias_historicas) > ventana:
            medias_historicas.pop(0)

        # Necesitamos al menos 15 días de historia para calcular z-score estable
        if len(medias_historicas) < 15:
            v2_media = media  # fallback: usar v1 hasta tener baseline
        else:
            hist_previos = medias_historicas[:-1]  # excluir hoy
            baseline = statistics.mean(hist_previos)
            std = statistics.pstdev(hist_previos) if len(hist_previos) > 1 else 1.0
            std = max(std, 2.0)  # evitar div/0 y amplificación extrema

            z = (media - baseline) / std
            # tanh(z/2): mapea (-inf, inf) → (-1, 1), con tanh(2)=0.96
            # Multiplicar por 50 y centrar en 50 → rango (0, 100)
            v2_media = 50.0 + 50.0 * math.tanh(z / 2.0)
            v2_media = max(0.0, min(100.0, v2_media))

        # Recalcular score_total con v2_media sustituyendo v1_media
        v2_total = (
            PESOS["media"] * v2_media
            + PESOS["trends"] * (trends or 0)
            + PESOS["congreso"] * (congreso or 0)
            + PESOS["mananera"] * (mananera or 0)
            + PESOS["urgencia"] * (urgencia or 0)
            + PESOS["dominancia"] * (dominancia or 0)
        )

        resultado[fecha] = {
            "v1_media": media,
            "v2_media": round(v2_media, 1),
            "v1_total": total,
            "v2_total": round(v2_total, 1),
            "delta_media": round(v2_media - media, 1),
            "delta_total": round(v2_total - (total or 0), 1),
        }

    return resultado


def analizar_caso_v2(conn, key, caso):
    desde, hasta = caso["ventana_media"]
    scores_v2 = calcular_scores_v2_categoria(conn, caso["categoria"])
    # Filtrar a la ventana
    en_ventana = [(f, v) for f, v in scores_v2.items() if desde <= f <= hasta]
    if not en_ventana:
        return None

    v1_totales = [v["v1_total"] for _, v in en_ventana if v["v1_total"] is not None]
    v2_totales = [v["v2_total"] for _, v in en_ventana]
    v1_medias = [v["v1_media"] for _, v in en_ventana]
    v2_medias = [v["v2_media"] for _, v in en_ventana]
    dias_sat_v1 = sum(1 for m in v1_medias if m >= 99.5)
    dias_sat_v2 = sum(1 for m in v2_medias if m >= 99.5)

    return {
        "caso": caso["titulo"][:45],
        "tipo": caso["tipo"],
        "categoria": caso["categoria"],
        "dias": len(en_ventana),
        "v1_total_max": max(v1_totales) if v1_totales else 0,
        "v2_total_max": max(v2_totales),
        "v1_total_avg": sum(v1_totales) / len(v1_totales) if v1_totales else 0,
        "v2_total_avg": sum(v2_totales) / len(v2_totales),
        "v1_media_max": max(v1_medias),
        "v2_media_max": max(v2_medias),
        "v1_media_avg": sum(v1_medias) / len(v1_medias),
        "v2_media_avg": sum(v2_medias) / len(v2_medias),
        "v1_sat_pct": 100 * dias_sat_v1 / len(en_ventana),
        "v2_sat_pct": 100 * dias_sat_v2 / len(en_ventana),
    }


def imprimir_tabla_comparativa(filas):
    print()
    print("═" * 140)
    print(f"  {'Caso':<45} {'Tipo':<3} {'Días':>5}  {'v1_max':>7} {'v2_max':>7}  {'v1_avg':>7} {'v2_avg':>7}  {'v1_MedMx':>8} {'v2_MedMx':>8}  {'v1_sat%':>7} {'v2_sat%':>7}")
    print("═" * 140)
    for r in filas:
        tipo = "+" if r["tipo"] == "positivo" else "−"
        print(
            f"  {r['caso']:<45} {tipo:<3} {r['dias']:>5}  "
            f"{r['v1_total_max']:>7.1f} {r['v2_total_max']:>7.1f}  "
            f"{r['v1_total_avg']:>7.1f} {r['v2_total_avg']:>7.1f}  "
            f"{r['v1_media_max']:>8.1f} {r['v2_media_max']:>8.1f}  "
            f"{r['v1_sat_pct']:>6.0f}% {r['v2_sat_pct']:>6.0f}%"
        )
    print("═" * 140)
    print()
    print("  INTERPRETACIÓN:")
    print("  • v1_max / v2_max: score_total máximo en la ventana (debería subir en positivos, bajar en negativos)")
    print("  • v1_MedMx / v2_MedMx: score_media máximo (v2 no debería saturar a 100)")
    print("  • sat%: porcentaje de días con score_media ≥99.5 (v2 debería ser ~0%)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--caso")
    parser.add_argument("--tabla", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB))

    if args.caso:
        if args.caso not in CASOS:
            print(f"Casos: {list(CASOS.keys())}")
            return
        casos = {args.caso: CASOS[args.caso]}
    else:
        casos = CASOS

    filas = []
    for key, caso in casos.items():
        r = analizar_caso_v2(conn, key, caso)
        if r:
            filas.append(r)

    if args.tabla or len(filas) > 1:
        imprimir_tabla_comparativa(filas)
    else:
        for r in filas:
            for k, v in r.items():
                print(f"  {k:<20} {v}")


if __name__ == "__main__":
    main()
