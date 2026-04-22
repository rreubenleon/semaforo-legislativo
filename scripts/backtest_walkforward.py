"""
Walk-forward backtest del score_total de FIAT.

Complementa la calibración retrospectiva (scripts/calibracion_score.py) con
validación temporal: simula qué hubiera pasado si un usuario hubiera
consultado FIAT semana a semana durante el histórico.

Pregunta específica que responde: "¿FIAT está mejorando, empeorando o
estable? ¿Hay drift? ¿Qué categorías cambian su precisión con el tiempo?"

Output: dashboard/backtest.json
  - semanas: serie cronológica (n, score_promedio, hit_rate_b, acc_alta,
    brier, log_loss por semana ISO)
  - rolling: ventana rodante de 30 días con brier, log-loss, accuracy_alta
  - drift_por_categoria: delta primer mes vs último mes por categoría
  - verdict: auto-generado (estable / mejorando / empeorando)

Uso:
    python scripts/backtest_walkforward.py
    python scripts/backtest_walkforward.py --dry-run
"""
import argparse
import json
import logging
import math
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
OUT = ROOT / "dashboard" / "backtest.json"

# Reusar funciones de calibracion_score.py para no duplicar lógica
sys.path.insert(0, str(Path(__file__).parent))
from calibracion_score import (
    parse_fecha, cargar_actividad_diaria, conteo_ventana, calcular_baseline,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

VENTANA_PREDICCION = 14     # días post-score (igual que calibración)
VENTANA_RODANTE = 30        # días para métricas rolling
THRESHOLD_DRIFT_PP = 10     # puntos porcentuales para marcar drift


def fecha_a_semana_iso(fecha):
    """2026-04-15 → '2026-W16'"""
    y, w, _ = fecha.isocalendar()
    return f"{y}-W{w:02d}"


def lunes_de_semana(fecha):
    """Lunes de la semana ISO que contiene fecha."""
    return fecha - timedelta(days=fecha.weekday())


def construir_pares_con_fecha(conn):
    """Para cada (categoria, fecha, score_total), computa hit_a, hit_b,
    brier_i, log_loss_i. Retorna lista ordenada por fecha."""
    logger.info("Cargando actividad diaria (sil + gaceta)…")
    actividad = cargar_actividad_diaria(conn)

    hoy = datetime.now().date()
    cutoff = hoy - timedelta(days=VENTANA_PREDICCION)

    rows = conn.execute("""
        SELECT categoria, fecha, score_total
        FROM scores
        ORDER BY fecha
    """).fetchall()

    pares = []
    for cat, fecha_str, score in rows:
        f = parse_fecha(fecha_str)
        if not f or f > cutoff or score is None:
            continue
        actividad_cat = actividad.get(cat, {})
        if not actividad_cat:
            continue
        conteo = conteo_ventana(actividad_cat, f + timedelta(days=1), VENTANA_PREDICCION)
        baseline = calcular_baseline(actividad_cat, f, VENTANA_PREDICCION, lookback_dias=90)
        hit_a = 1 if conteo >= 1 else 0
        hit_b = 1 if conteo > baseline else 0
        p = float(score) / 100.0
        p = max(1e-9, min(1 - 1e-9, p))
        brier_i = (p - hit_b) ** 2
        ll_i = -(hit_b * math.log(p) + (1 - hit_b) * math.log(1 - p))
        pares.append({
            "categoria": cat,
            "fecha": f,
            "score": float(score),
            "hit_a": hit_a,
            "hit_b": hit_b,
            "brier_i": brier_i,
            "ll_i": ll_i,
            "alta_confianza": float(score) >= 70,
        })
    pares.sort(key=lambda p: p["fecha"])
    logger.info(f"Pares válidos: {len(pares)} · rango {pares[0]['fecha']} → {pares[-1]['fecha']}")
    return pares


def agrupar_por_semana(pares):
    """Agrupa pares por semana ISO y computa métricas semanales."""
    por_sem = defaultdict(list)
    for p in pares:
        por_sem[p["fecha"].isocalendar()[:2]].append(p)
    semanas = []
    for (y, w), ps in sorted(por_sem.items()):
        n = len(ps)
        score_avg = sum(p["score"] for p in ps) / n
        hit_b_rate = 100 * sum(p["hit_b"] for p in ps) / n
        hit_a_rate = 100 * sum(p["hit_a"] for p in ps) / n
        brier = sum(p["brier_i"] for p in ps) / n
        ll = sum(p["ll_i"] for p in ps) / n
        # Acc alta confianza: de los scores ≥70, ¿cuántos tuvieron hit_b?
        alta = [p for p in ps if p["alta_confianza"]]
        acc_alta = (100 * sum(p["hit_b"] for p in alta) / len(alta)) if alta else None
        # Fecha de inicio de la semana (lunes)
        fecha_inicio = lunes_de_semana(ps[0]["fecha"])
        semanas.append({
            "iso": f"{y}-W{w:02d}",
            "fecha_inicio": fecha_inicio.isoformat(),
            "n": n,
            "score_promedio": round(score_avg, 2),
            "hit_rate_b": round(hit_b_rate, 2),
            "hit_rate_a": round(hit_a_rate, 2),
            "acc_alta_confianza": round(acc_alta, 2) if acc_alta is not None else None,
            "n_alta_confianza": len(alta),
            "brier": round(brier, 4),
            "log_loss": round(ll, 4),
        })
    return semanas


def rolling_ventana(pares, dias=VENTANA_RODANTE):
    """Computa métricas rolling ventana de `dias` terminando cada domingo
    de cada semana del histórico. Devuelve serie con brier, log-loss,
    acc_alta y delta_calibracion."""
    if not pares:
        return []
    min_fecha = pares[0]["fecha"]
    max_fecha = pares[-1]["fecha"]
    # Domingos cronológicos desde el primer domingo completable
    primer_domingo = min_fecha + timedelta(days=(6 - min_fecha.weekday()) % 7)
    if primer_domingo < min_fecha + timedelta(days=dias):
        # Necesitamos al menos `dias` de observaciones antes del primer punto
        primer_domingo = min_fecha + timedelta(days=dias)
        primer_domingo += timedelta(days=(6 - primer_domingo.weekday()) % 7)

    serie = []
    cur = primer_domingo
    while cur <= max_fecha:
        inicio = cur - timedelta(days=dias - 1)
        ventana = [p for p in pares if inicio <= p["fecha"] <= cur]
        if len(ventana) >= 20:  # piso de evidencia
            n = len(ventana)
            brier = sum(p["brier_i"] for p in ventana) / n
            ll = sum(p["ll_i"] for p in ventana) / n
            score_avg = sum(p["score"] for p in ventana) / n
            hit_rate = 100 * sum(p["hit_b"] for p in ventana) / n
            delta = hit_rate - score_avg  # positivo = FIAT subestima
            alta = [p for p in ventana if p["alta_confianza"]]
            acc_alta = (100 * sum(p["hit_b"] for p in alta) / len(alta)) if alta else None
            serie.append({
                "fecha": cur.isoformat(),
                "n": n,
                "brier": round(brier, 4),
                "log_loss": round(ll, 4),
                "hit_rate": round(hit_rate, 2),
                "score_promedio": round(score_avg, 2),
                "delta_calibracion": round(delta, 2),
                "acc_alta_confianza": round(acc_alta, 2) if acc_alta is not None else None,
            })
        cur += timedelta(days=7)
    return serie


def drift_por_categoria(pares):
    """Para cada categoría con volumen suficiente: compara accuracy del
    primer mes vs último mes del histórico."""
    if not pares:
        return []
    min_f = pares[0]["fecha"]
    max_f = pares[-1]["fecha"]
    if (max_f - min_f).days < 60:
        return []  # no hay suficiente rango temporal

    primer_mes_fin = min_f + timedelta(days=30)
    ultimo_mes_ini = max_f - timedelta(days=30)

    por_cat = defaultdict(lambda: {"primero": [], "ultimo": [], "total": 0})
    for p in pares:
        por_cat[p["categoria"]]["total"] += 1
        if p["fecha"] <= primer_mes_fin:
            por_cat[p["categoria"]]["primero"].append(p)
        if p["fecha"] >= ultimo_mes_ini:
            por_cat[p["categoria"]]["ultimo"].append(p)

    resultados = []
    for cat, d in por_cat.items():
        if len(d["primero"]) < 10 or len(d["ultimo"]) < 10:
            continue
        hit_prim = 100 * sum(p["hit_b"] for p in d["primero"]) / len(d["primero"])
        hit_ult = 100 * sum(p["hit_b"] for p in d["ultimo"]) / len(d["ultimo"])
        delta = hit_ult - hit_prim
        direccion = "estable"
        if delta > THRESHOLD_DRIFT_PP:
            direccion = "mejora"
        elif delta < -THRESHOLD_DRIFT_PP:
            direccion = "empeora"
        resultados.append({
            "categoria": cat,
            "n_total": d["total"],
            "n_primer_mes": len(d["primero"]),
            "n_ultimo_mes": len(d["ultimo"]),
            "hit_primer_mes": round(hit_prim, 2),
            "hit_ultimo_mes": round(hit_ult, 2),
            "delta": round(delta, 2),
            "direccion": direccion,
        })
    # Orden: los que más cambian primero
    resultados.sort(key=lambda r: -abs(r["delta"]))
    return resultados


def generar_verdict(rolling, drift_cat):
    """Verdict textual automático basado en tendencia de Brier rodante y
    composición de drift por categoría."""
    if len(rolling) < 4:
        return {
            "texto": "Histórico insuficiente para evaluar drift. Seguimos acumulando.",
            "tipo": "indeterminado",
        }

    # Comparar primer cuarto vs último cuarto del rolling
    k = max(1, len(rolling) // 4)
    brier_ini = sum(r["brier"] for r in rolling[:k]) / k
    brier_fin = sum(r["brier"] for r in rolling[-k:]) / k
    acc_ini_vals = [r["acc_alta_confianza"] for r in rolling[:k] if r["acc_alta_confianza"] is not None]
    acc_fin_vals = [r["acc_alta_confianza"] for r in rolling[-k:] if r["acc_alta_confianza"] is not None]
    acc_ini = sum(acc_ini_vals) / len(acc_ini_vals) if acc_ini_vals else None
    acc_fin = sum(acc_fin_vals) / len(acc_fin_vals) if acc_fin_vals else None

    mejoran = sum(1 for r in drift_cat if r["direccion"] == "mejora")
    empeoran = sum(1 for r in drift_cat if r["direccion"] == "empeora")

    # Brier menor = mejor
    if brier_fin < brier_ini - 0.02:
        tipo = "mejorando"
        texto = f"Calibración mejorando: Brier bajó de {brier_ini:.3f} a {brier_fin:.3f} en el período."
    elif brier_fin > brier_ini + 0.02:
        tipo = "empeorando"
        texto = f"Calibración empeorando: Brier subió de {brier_ini:.3f} a {brier_fin:.3f} en el período."
    else:
        tipo = "estable"
        texto = f"Calibración estable. Brier oscila alrededor de {brier_fin:.3f} sin drift significativo."

    if acc_ini is not None and acc_fin is not None:
        delta_acc = acc_fin - acc_ini
        if abs(delta_acc) >= 5:
            sig = "+" if delta_acc > 0 else ""
            texto += f" Accuracy ≥70: {acc_ini:.1f}% → {acc_fin:.1f}% ({sig}{delta_acc:.1f} pp)."
        else:
            texto += f" Accuracy ≥70 estable ({acc_fin:.1f}%)."

    if mejoran or empeoran:
        texto += f" {mejoran} categoría(s) mejorando, {empeoran} empeorando."

    return {"texto": texto, "tipo": tipo}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB))
    pares = construir_pares_con_fecha(conn)
    if not pares:
        logger.error("No hay pares para evaluar.")
        sys.exit(1)

    semanas = agrupar_por_semana(pares)
    rolling = rolling_ventana(pares, dias=VENTANA_RODANTE)
    drift_cat = drift_por_categoria(pares)
    verdict = generar_verdict(rolling, drift_cat)

    payload = {
        "fecha_calculo": datetime.now().isoformat(),
        "ventana_prediccion_dias": VENTANA_PREDICCION,
        "ventana_rodante_dias": VENTANA_RODANTE,
        "threshold_drift_pp": THRESHOLD_DRIFT_PP,
        "rango": {
            "inicio": pares[0]["fecha"].isoformat(),
            "fin": pares[-1]["fecha"].isoformat(),
            "n_pares": len(pares),
            "n_semanas": len(semanas),
        },
        "verdict": verdict,
        "semanas": semanas,
        "rolling_30d": rolling,
        "drift_por_categoria": drift_cat,
        "metodologia": {
            "prediccion": "Para cada score emitido en fecha D, hit se evalúa en [D+1, D+14] sólo con data temporalmente posterior (no hay data leak).",
            "hit_b": "Conteo de instrumentos en ventana +14d excede la mediana histórica de ventanas equivalentes en los 90 días previos.",
            "rolling": f"Ventana rodante de {VENTANA_RODANTE} días terminando cada domingo. Requiere ≥20 observaciones por punto.",
            "drift": f"Se compara accuracy del primer mes vs último mes por categoría. Umbral de drift: ±{THRESHOLD_DRIFT_PP}pp.",
            "acc_alta_confianza": "Porcentaje de scores ≥70 que resultaron en hit (método B).",
            "verdict": "Automático basado en delta de Brier entre primer y último cuarto de la serie rodante.",
        },
    }

    # Imprimir resumen
    print()
    print("  ═══ Walk-forward Backtest ═══")
    print(f"  Rango:         {payload['rango']['inicio']} → {payload['rango']['fin']}")
    print(f"  Pares:         {payload['rango']['n_pares']:,}")
    print(f"  Semanas:       {payload['rango']['n_semanas']}")
    print(f"  Rolling 30d:   {len(rolling)} puntos")
    print(f"  Drift cat.:    {len(drift_cat)} evaluadas")
    print()
    print(f"  Verdict ({verdict['tipo']}):")
    print(f"    {verdict['texto']}")
    print()
    if rolling:
        print("  Rolling 30d (primeros / últimos 3):")
        print(f"  {'FECHA':<12} {'N':>4} {'BRIER':>7} {'LOG-LOSS':>9} {'HIT':>6} {'SCORE':>6} {'Δ':>6}")
        for r in rolling[:3]:
            print(f"  {r['fecha']:<12} {r['n']:>4} {r['brier']:>7.4f} {r['log_loss']:>9.4f} {r['hit_rate']:>5.1f}% {r['score_promedio']:>5.1f}% {r['delta_calibracion']:>+5.1f}")
        if len(rolling) > 6:
            print(f"  …")
        for r in rolling[-3:]:
            print(f"  {r['fecha']:<12} {r['n']:>4} {r['brier']:>7.4f} {r['log_loss']:>9.4f} {r['hit_rate']:>5.1f}% {r['score_promedio']:>5.1f}% {r['delta_calibracion']:>+5.1f}")
    if drift_cat:
        print()
        print("  Drift por categoría (top 5 por |Δ|):")
        for d in drift_cat[:5]:
            signo = "+" if d["delta"] > 0 else ""
            print(f"    {d['categoria']:<22} {d['hit_primer_mes']:>5.1f}% → {d['hit_ultimo_mes']:>5.1f}% ({signo}{d['delta']:>5.1f}pp) {d['direccion']}")

    if args.dry_run:
        print("\n  (dry-run: no se escribió backtest.json)")
        return

    OUT.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    logger.info(f"✓ Escrito {OUT.relative_to(ROOT)} ({OUT.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
