"""
Recalcula score_media para los días clave de cada caso del benchmark
usando la data de artículos ACTUAL (después del backfill histórico).

No toca la tabla `scores` existente. Solo reporta:
  - score_media que FIAT "decía" ese día (v1_historico) — puede ser 0 si no tenía data
  - score_media que diría HOY con la data completa (v1_recalculado)
  - score_media v2 (z-score sobre v1_recalculado)

Esto permite evaluar el modelo con data histórica completa.

Uso:
    python scripts/recalcular_score_media_historico.py --caso reforma_judicial_2024
    python scripts/recalcular_score_media_historico.py --todos
"""
import argparse
import math
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
DB = ROOT / "semaforo.db"

from config import CATEGORIAS, obtener_keywords_categoria
from scripts.ground_truth_benchmark import CASOS, actividad_post_pico


def calcular_score_media_en_fecha(conn, categoria_clave, fecha_corte, dias=7):
    """
    Replica obtener_score_media() de scrapers/medios.py pero sobre una
    fecha_corte histórica (simula qué diría FIAT si hubiera tenido la
    data completa en ese momento). Usa ventana de N días hacia atrás.
    """
    keywords = obtener_keywords_categoria(categoria_clave)
    if not keywords:
        return 0.0

    fecha_corte_dt = datetime.strptime(fecha_corte, "%Y-%m-%d")
    fecha_limite = (fecha_corte_dt - timedelta(days=dias)).strftime("%Y-%m-%d")

    # Peso total de todos los artículos en el periodo
    total_peso = conn.execute(
        "SELECT COALESCE(SUM(peso_fuente), 0) FROM articulos "
        "WHERE DATE(fecha) >= ? AND DATE(fecha) <= ?",
        (fecha_limite, fecha_corte),
    ).fetchone()[0]

    if not total_peso:
        return 0.0

    # Artículos relevantes
    articulos_vistos = set()
    articulos_data = []
    for kw in keywords:
        rows = conn.execute("""
            SELECT id, peso_fuente, fuente, DATE(fecha) as dia FROM articulos
            WHERE DATE(fecha) >= ? AND DATE(fecha) <= ?
              AND (LOWER(titulo) LIKE ? OR LOWER(COALESCE(resumen, '')) LIKE ?)
        """, (fecha_limite, fecha_corte, f"%{kw.lower()}%", f"%{kw.lower()}%")).fetchall()
        for row in rows:
            if row[0] not in articulos_vistos:
                articulos_vistos.add(row[0])
                articulos_data.append({
                    "peso": row[1] or 1.0,
                    "fuente": row[2],
                    "dia": row[3],
                })

    if not articulos_data:
        return 0.0

    score_acum = sum(a["peso"] for a in articulos_data)

    # Subfactor 1: Volumen/Share (40%)
    share = score_acum / total_peso
    n_cat = len(CATEGORIAS)
    expected_share = 1.0 / n_cat
    ratio = share / expected_share if expected_share > 0 else 0
    if ratio <= 0:
        vol_score = 0.0
    else:
        vol_score = 50 + 50 * math.tanh(math.log(ratio) / 1.5)
    vol_score = max(0.0, min(100.0, vol_score))

    # Subfactor 2: Concentración temporal (20%)
    dias_con_cobertura = set(a["dia"] for a in articulos_data)
    conc_score = min((len(dias_con_cobertura) / dias) * 100.0, 100.0)

    # Subfactor 3: Días consecutivos recientes (20%)
    dias_consec = 0
    for i in range(dias):
        dia_check = (fecha_corte_dt - timedelta(days=i)).strftime("%Y-%m-%d")
        if dia_check in dias_con_cobertura:
            dias_consec += 1
        else:
            break
    consec_score = min((dias_consec / dias) * 100.0, 100.0)

    # Subfactor 4: Diversidad de medios (20%)
    fuentes_unicas = set(a["fuente"] for a in articulos_data)
    # Usar total de fuentes en la DB como denominador
    n_total = conn.execute("SELECT COUNT(DISTINCT fuente) FROM articulos").fetchone()[0] or 1
    div_score = min(math.sqrt(len(fuentes_unicas) / n_total) * 100, 100.0)

    v1_score = 0.40 * vol_score + 0.20 * conc_score + 0.20 * consec_score + 0.20 * div_score
    return round(max(0.0, min(100.0, v1_score)), 2)


def recalcular_ventana(conn, categoria, desde, hasta, paso_dias=3, baseline_dias=90):
    """
    Calcula score_media recalculado para días cada `paso_dias` en la ventana,
    más los `baseline_dias` días previos (para construir baseline para z-score).
    Devuelve lista de (fecha, v1_recalc, v2_zscore, baseline).
    """
    desde_dt = datetime.strptime(desde, "%Y-%m-%d")
    hasta_dt = datetime.strptime(hasta, "%Y-%m-%d")
    # Recalcular también los 90 días previos para tener baseline
    baseline_inicio = desde_dt - timedelta(days=baseline_dias)

    serie_completa = []  # incluye baseline previo
    f = baseline_inicio
    while f <= hasta_dt:
        fstr = f.strftime("%Y-%m-%d")
        v1 = calcular_score_media_en_fecha(conn, categoria, fstr, dias=7)
        serie_completa.append((fstr, v1))
        f += timedelta(days=paso_dias)

    # Para cada punto en la ventana (excluyendo baseline), calcular z-score
    # usando los puntos previos dentro del baseline rolling de 90 días.
    resultado = []
    for i, (fstr, v1) in enumerate(serie_completa):
        # Sólo reportar puntos dentro de la ventana (fstr >= desde)
        if fstr < desde:
            continue

        fdt = datetime.strptime(fstr, "%Y-%m-%d")
        # Baseline rolling: puntos previos en los últimos `baseline_dias`
        baseline_puntos = [
            v for f2, v in serie_completa[:i]
            if (fdt - datetime.strptime(f2, "%Y-%m-%d")).days <= baseline_dias
        ]

        if len(baseline_puntos) < 5:
            # No hay suficiente baseline — usar v1 como fallback
            resultado.append((fstr, v1, v1, 0))
            continue

        baseline = sum(baseline_puntos) / len(baseline_puntos)
        var = sum((v - baseline) ** 2 for v in baseline_puntos) / len(baseline_puntos)
        std = max(math.sqrt(var), 2.0)

        z = (v1 - baseline) / std
        v2 = 50.0 + 50.0 * math.tanh(z / 2.0)
        v2 = max(0.0, min(100.0, v2))
        resultado.append((fstr, v1, round(v2, 1), round(baseline, 1)))
    return resultado


def analizar_caso(conn, key, caso):
    desde, hasta = caso["ventana_media"]
    cat = caso["categoria"]
    print()
    print("━" * 90)
    print(f"  {caso['titulo']}  ({'POSITIVO' if caso['tipo']=='positivo' else 'NEGATIVO'})")
    print(f"  Cat: {cat}  ·  ventana: {desde} → {hasta}")
    print("━" * 90)

    # Score histórico de la tabla scores
    hist = conn.execute(
        "SELECT fecha, score_media FROM scores WHERE categoria=? AND fecha BETWEEN ? AND ? ORDER BY fecha",
        (cat, desde, hasta),
    ).fetchall()
    v1_hist = [m for _, m in hist if m is not None]

    # Recalcular score_media retroactivo
    serie = recalcular_ventana(conn, cat, desde, hasta, paso_dias=5)
    v1_recalc = [r[1] for r in serie]
    v2_valores = [r[2] for r in serie]

    print(f"\n  V1 histórico (tabla scores, lo que FIAT dijo en su momento):")
    print(f"     n={len(v1_hist)}  max={max(v1_hist) if v1_hist else 0:.1f}  avg={sum(v1_hist)/len(v1_hist) if v1_hist else 0:.1f}  sat100={sum(1 for v in v1_hist if v>=99.5)}")

    print(f"\n  V1 recalculado (fórmula actual + data completa):")
    print(f"     n={len(v1_recalc)}  max={max(v1_recalc):.1f}  avg={sum(v1_recalc)/len(v1_recalc):.1f}  sat100={sum(1 for v in v1_recalc if v>=99.5)}")

    print(f"\n  V2 (z-score sobre V1 recalculado):")
    print(f"     n={len(v2_valores)}  max={max(v2_valores):.1f}  avg={sum(v2_valores)/len(v2_valores):.1f}  sat100={sum(1 for v in v2_valores if v>=99.5)}")

    # Muestra días clave: top 5 de v1 recalculado
    top_v1 = sorted(serie, key=lambda x: -x[1])[:5]
    print(f"\n  Top 5 días por V1 recalc (picos reales según fórmula actual):")
    for f, v1, v2, *_ in top_v1:
        print(f"     {f}  v1_recalc={v1:>6.1f}  v2_zscore={v2:>6.1f}")

    return {
        "caso": caso["titulo"][:45],
        "tipo": caso["tipo"],
        "v1_hist_max": max(v1_hist) if v1_hist else 0,
        "v1_recalc_max": max(v1_recalc),
        "v2_max": max(v2_valores),
        "v1_recalc_sat": 100 * sum(1 for v in v1_recalc if v >= 99.5) / len(v1_recalc),
        "v2_sat": 100 * sum(1 for v in v2_valores if v >= 99.5) / len(v2_valores),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--caso")
    parser.add_argument("--todos", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB))

    if args.caso:
        casos = {args.caso: CASOS[args.caso]}
    else:
        casos = CASOS

    resultados = []
    for key, caso in casos.items():
        r = analizar_caso(conn, key, caso)
        if r:
            resultados.append(r)

    if len(resultados) > 1:
        print()
        print("═" * 110)
        print(f"  {'Caso':<48} {'Tipo':<4} {'v1_hist':>8} {'v1_recalc':>10} {'v2':>6}  {'v1_sat':>7} {'v2_sat':>7}")
        print("═" * 110)
        for r in resultados:
            t = "+" if r["tipo"] == "positivo" else "−"
            print(
                f"  {r['caso']:<48} {t:<4} "
                f"{r['v1_hist_max']:>8.1f} {r['v1_recalc_max']:>10.1f} {r['v2_max']:>6.1f}  "
                f"{r['v1_recalc_sat']:>6.0f}% {r['v2_sat']:>6.0f}%"
            )
        print("═" * 110)


if __name__ == "__main__":
    main()
