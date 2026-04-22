"""
Calibración probabilística del score_total de FIAT.

Pregunta que contesta:
  Cuando FIAT dice "75% de probabilidad de actividad legislativa", ¿pasa
  el 75% de las veces? Si no, ¿en cuánto se desvía?

Metodología:
  Para cada (categoria, fecha, score_total) en la tabla scores:
    - Hit A (binario simple): ≥1 instrumento nuevo en la categoría dentro de
      los próximos N días.
    - Hit B (vs baseline): el conteo de instrumentos en la ventana +N días
      excede la mediana del conteo en ventanas históricas de los 90 días
      previos para esa categoría.

  Se agrupan los scores en bins de 10 puntos y se computa el hit rate
  observado por bin. La diferencia entre score_predicho y hit_observado
  es el error de calibración.

Output: dashboard/calibracion.json
  - global: stats agregadas (Brier, ECE, log-loss, n)
  - bins: lista de {bin, score_mean, n, hit_rate_a, hit_rate_b} por ventana
  - por_categoria: top-5 categorías con su propia curva
  - headline: frase resumen dinámica para mostrar arriba en la UI

Uso:
    python scripts/calibracion_score.py
    python scripts/calibracion_score.py --ventana 14
    python scripts/calibracion_score.py --dry-run
"""
import argparse
import json
import logging
import math
import re
import sqlite3
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
OUT = ROOT / "dashboard" / "calibracion.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

# Categorías que tienen baseline naturalmente alto — FIAT marca con ⚑ en UI
# para que el lector sepa que el hit (A) será siempre cerca de 100%.
CATEGORIAS_SIEMPRE_ACTIVAS = {
    "seguridad_justicia", "economia_hacienda", "electoral_politico",
    "derechos_humanos",
}


def parse_fecha(s):
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_categoria_field(s):
    """Devuelve lista de nombres de categoría limpios desde un valor que
    puede venir como 'cat1', 'cat1:peso', 'cat1:w1,cat2:w2' o
    'cat1:w1|cat2:w2'. Ignora pesos. Filtra vacíos."""
    if not s:
        return []
    out = []
    for token in re.split(r"[,|]", s):
        cat = token.split(":")[0].strip()
        if cat:
            out.append(cat)
    return out


def cargar_actividad_diaria(conn):
    """
    Devuelve dict[categoria_limpia][fecha_iso] = count_de_instrumentos.
    Combina sil_documentos + gaceta. Ambas tablas mezclan formato plano y
    'cat:peso[,cat:peso]', así que normalizamos por igual.
    """
    actividad = defaultdict(lambda: defaultdict(int))

    rows = conn.execute("""
        SELECT categoria, fecha_presentacion
        FROM sil_documentos
        WHERE categoria IS NOT NULL AND categoria != ''
          AND fecha_presentacion IS NOT NULL AND fecha_presentacion != ''
    """).fetchall()
    for raw, fecha in rows:
        f = parse_fecha(fecha)
        if not f:
            continue
        for cat in parse_categoria_field(raw):
            actividad[cat][f.isoformat()] += 1

    rows = conn.execute("""
        SELECT categorias, fecha
        FROM gaceta
        WHERE categorias IS NOT NULL AND categorias != ''
          AND fecha IS NOT NULL
    """).fetchall()
    for raw, fecha in rows:
        f = parse_fecha(fecha)
        if not f:
            continue
        for cat in parse_categoria_field(raw):
            actividad[cat][f.isoformat()] += 1

    return actividad


def conteo_ventana(actividad_cat, fecha_inicio, dias):
    """Suma counts en [fecha_inicio, fecha_inicio + dias - 1] inclusive."""
    total = 0
    for d in range(dias):
        fk = (fecha_inicio + timedelta(days=d)).isoformat()
        total += actividad_cat.get(fk, 0)
    return total


def calcular_baseline(actividad_cat, fecha_pivote, ventana_dias, lookback_dias=90):
    """
    Mediana de counts de ventanas de `ventana_dias` en los últimos
    `lookback_dias` previos a fecha_pivote. Si hay <5 ventanas, usa el
    promedio simple del periodo (más estable con poca data).
    """
    counts = []
    # Ventanas posibles: deslizando día a día desde (pivote - lookback) hasta (pivote - ventana)
    inicio = fecha_pivote - timedelta(days=lookback_dias)
    fin = fecha_pivote - timedelta(days=ventana_dias)
    cur = inicio
    while cur <= fin:
        counts.append(conteo_ventana(actividad_cat, cur, ventana_dias))
        cur += timedelta(days=1)
    if not counts:
        return 0
    if len(counts) < 5:
        return sum(counts) / len(counts)
    return statistics.median(counts)


def computar_hits(conn, ventana_dias=14):
    """
    Para cada fila de `scores`, computa:
      score_predicho (0..1)
      hit_a: 1 si hubo ≥1 instrumento en categoría en la ventana
      hit_b: 1 si el conteo en la ventana excede la mediana histórica
      conteo_real, baseline (debug/inspección)
    Excluye filas donde fecha + ventana > hoy (no han madurado).
    Excluye scores con score_total = 0 si la categoría no tiene
    histórico suficiente (los 0 de inicio cuando se sumaron categorías
    nuevas son ruido, no predicciones).
    """
    logger.info("Cargando actividad diaria sil + gaceta…")
    actividad = cargar_actividad_diaria(conn)
    logger.info(f"  Categorías con actividad: {len(actividad)}")

    hoy = datetime.now().date()
    cutoff = hoy - timedelta(days=ventana_dias)

    rows = conn.execute("""
        SELECT categoria, fecha, score_total
        FROM scores
        ORDER BY fecha
    """).fetchall()
    logger.info(f"  Scores totales en BD: {len(rows)}")

    pares = []
    for cat, fecha_str, score in rows:
        f = parse_fecha(fecha_str)
        if not f or f > cutoff:
            continue  # aún no madura
        if score is None:
            continue
        actividad_cat = actividad.get(cat, {})
        # Si no hay actividad nunca para esta categoría, skip
        if not actividad_cat:
            continue
        conteo = conteo_ventana(actividad_cat, f + timedelta(days=1), ventana_dias)
        baseline = calcular_baseline(actividad_cat, f, ventana_dias, lookback_dias=90)
        hit_a = 1 if conteo >= 1 else 0
        hit_b = 1 if conteo > baseline else 0
        pares.append({
            "categoria": cat,
            "fecha": fecha_str,
            "score": float(score),
            "score_norm": float(score) / 100.0,
            "hit_a": hit_a,
            "hit_b": hit_b,
            "conteo": conteo,
            "baseline": round(baseline, 2),
        })
    logger.info(f"  Pares válidos para calibración (ventana {ventana_dias}d): {len(pares)}")
    return pares


def bin_pares(pares, hit_field="hit_a", n_bins=10):
    """Agrupa pares en bins de 10 puntos: 0-10, 10-20, …, 90-100."""
    bins = [{
        "bin": i,
        "rango": [i * (100 // n_bins), (i + 1) * (100 // n_bins)],
        "scores": [],
        "hits": [],
    } for i in range(n_bins)]
    for p in pares:
        idx = min(n_bins - 1, int(p["score"] // (100 // n_bins)))
        bins[idx]["scores"].append(p["score"])
        bins[idx]["hits"].append(p[hit_field])
    out = []
    for b in bins:
        n = len(b["scores"])
        if n == 0:
            out.append({"bin": b["bin"], "rango": b["rango"], "n": 0,
                        "score_mean": None, "hit_rate": None, "delta": None})
            continue
        score_mean = sum(b["scores"]) / n
        hit_rate = 100.0 * sum(b["hits"]) / n
        out.append({
            "bin": b["bin"],
            "rango": b["rango"],
            "n": n,
            "score_mean": round(score_mean, 2),
            "hit_rate": round(hit_rate, 2),
            "delta": round(hit_rate - score_mean, 2),
        })
    return out


def metricas_globales(pares, hit_field="hit_a"):
    """Brier, log-loss, ECE."""
    if not pares:
        return {"n": 0}
    n = len(pares)
    # Brier
    brier = sum((p["score_norm"] - p[hit_field]) ** 2 for p in pares) / n
    # Log-loss (clip para evitar log(0))
    eps = 1e-9
    ll = 0.0
    for p in pares:
        sp = max(eps, min(1 - eps, p["score_norm"]))
        h = p[hit_field]
        ll += -(h * math.log(sp) + (1 - h) * math.log(1 - sp))
    log_loss = ll / n
    # ECE: Expected Calibration Error
    bins = bin_pares(pares, hit_field=hit_field)
    ece = 0.0
    for b in bins:
        if b["n"] == 0:
            continue
        ece += (b["n"] / n) * abs(b["hit_rate"] - b["score_mean"])
    # Hit rate global y score mean global
    hit_global = 100.0 * sum(p[hit_field] for p in pares) / n
    score_mean_global = sum(p["score"] for p in pares) / n
    return {
        "n": n,
        "brier": round(brier, 4),
        "log_loss": round(log_loss, 4),
        "ece": round(ece, 2),
        "hit_rate_observado": round(hit_global, 2),
        "score_promedio": round(score_mean_global, 2),
    }


def por_categoria(pares, top_n=5, hit_field="hit_a"):
    """Top-N categorías con más pares + sus métricas individuales."""
    por_cat = defaultdict(list)
    for p in pares:
        por_cat[p["categoria"]].append(p)
    rank = sorted(por_cat.items(), key=lambda kv: -len(kv[1]))
    out = []
    for cat, ps in rank[:top_n]:
        m_a = metricas_globales(ps, "hit_a")
        m_b = metricas_globales(ps, "hit_b")
        out.append({
            "categoria": cat,
            "siempre_activa": cat in CATEGORIAS_SIEMPRE_ACTIVAS,
            "n": len(ps),
            "metricas_a": m_a,
            "metricas_b": m_b,
            "bins_a": bin_pares(ps, "hit_a"),
            "bins_b": bin_pares(ps, "hit_b"),
        })
    return out


def generar_headline(global_b, bins_b):
    """
    Frase grande para la UI. Usa SIEMPRE el método (B) hit sobre baseline,
    porque (A) está saturado por la actividad permanente del Congreso y
    no es interpretable en agregado.

    Estrategia:
      1. Buscar bins bien calibrados (|delta| ≤ 10, n ≥ 30).
      2. Si hay, headline el bin de mayor confianza bien calibrado.
      3. Si no hay zona limpia, headline global con dirección del sesgo.
    """
    n = global_b["n"]
    bien_calibrados = [b for b in bins_b if b["n"] >= 30 and abs(b["delta"]) <= 10]

    if bien_calibrados:
        b = max(bien_calibrados, key=lambda x: x["score_mean"])
        principal = (
            f"En su zona de mayor confianza (~{round(b['score_mean'])}%), "
            f"FIAT acierta el {round(b['hit_rate'])}% de las veces"
        )
        sub = (
            f"Calibración limpia en bin {b['rango'][0]}–{b['rango'][1]} "
            f"(n={b['n']}). {n:,} predicciones evaluadas · ventana 14 días"
        )
    else:
        delta = global_b["hit_rate_observado"] - global_b["score_promedio"]
        if delta > 8:
            principal = (
                f"FIAT subestima: dice {round(global_b['score_promedio'])}% "
                f"en promedio, pasa el {round(global_b['hit_rate_observado'])}%"
            )
        elif delta < -8:
            principal = (
                f"FIAT sobreestima: dice {round(global_b['score_promedio'])}%, "
                f"pasa solo el {round(global_b['hit_rate_observado'])}%"
            )
        else:
            principal = (
                f"FIAT acierta el {round(global_b['hit_rate_observado'])}% "
                f"de los picos que anuncia"
            )
        sub = f"{n:,} predicciones evaluadas · ventana 14 días · método (B) hit sobre baseline"

    return {"principal": principal, "sub": sub}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ventana", type=int, default=14, help="Días post-score (default 14)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB))
    pares_14 = computar_hits(conn, ventana_dias=14)
    pares_7 = computar_hits(conn, ventana_dias=7)

    if not pares_14:
        logger.error("No hay pares válidos. Verifica datos en scores/sil/gaceta.")
        sys.exit(1)

    # Métricas
    global_a_14 = metricas_globales(pares_14, "hit_a")
    global_b_14 = metricas_globales(pares_14, "hit_b")
    global_a_7 = metricas_globales(pares_7, "hit_a")
    global_b_7 = metricas_globales(pares_7, "hit_b")
    bins_a_14 = bin_pares(pares_14, "hit_a")
    bins_b_14 = bin_pares(pares_14, "hit_b")
    bins_a_7 = bin_pares(pares_7, "hit_a")
    bins_b_7 = bin_pares(pares_7, "hit_b")
    cats_14 = por_categoria(pares_14)

    headline = generar_headline(global_b_14, bins_b_14)

    payload = {
        "fecha_calculo": datetime.now().isoformat(),
        "ventana_principal_dias": 14,
        "headline": headline,
        "ventana_14d": {
            "global_a": global_a_14,
            "global_b": global_b_14,
            "bins_a": bins_a_14,
            "bins_b": bins_b_14,
            "por_categoria": cats_14,
        },
        "ventana_7d": {
            "global_a": global_a_7,
            "global_b": global_b_7,
            "bins_a": bins_a_7,
            "bins_b": bins_b_7,
        },
        "categorias_siempre_activas": sorted(CATEGORIAS_SIEMPRE_ACTIVAS),
        "metodologia": {
            "hit_a": "Hit binario: ≥1 instrumento nuevo (sil + gaceta) en la categoría dentro de la ventana post-score.",
            "hit_b": "Hit vs baseline: conteo en la ventana excede la mediana de conteos en ventanas equivalentes en los 90 días previos para esa categoría.",
            "exclusiones": f"Scores con fecha + ventana > hoy (aún no madura) y categorías sin actividad histórica.",
            "bins": "10 bins de 10 puntos: 0-10, 10-20, …, 90-100.",
        },
    }

    # Imprimir resumen
    print()
    print("  ═══ Calibración FIAT score_total ═══")
    print(f"  Pares válidos (14d): {global_a_14['n']:,}")
    print(f"  Score promedio:      {global_a_14['score_promedio']}%")
    print(f"  Hit observado (A):   {global_a_14['hit_rate_observado']}%")
    print(f"  Hit observado (B):   {global_b_14['hit_rate_observado']}%")
    print(f"  Brier (A):           {global_a_14['brier']}")
    print(f"  ECE (A):             {global_a_14['ece']} puntos")
    print(f"  Log-loss (A):        {global_a_14['log_loss']}")
    print()
    print(f"  Headline: {headline['principal']}")
    print(f"            {headline['sub']}")
    print()
    print("  Curva de calibración (A):")
    print(f"  {'BIN':<8} {'N':>5} {'SCORE':>7} {'HIT':>7} {'Δ':>7}")
    for b in bins_a_14:
        if b["n"]:
            print(f"  {str(b['rango'])[1:-1].replace(', ','-'):<8} {b['n']:>5} {b['score_mean']:>6.1f}% {b['hit_rate']:>6.1f}% {b['delta']:>+6.1f}")

    if args.dry_run:
        print("\n  (dry-run: no se escribió calibracion.json)")
        return

    OUT.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    logger.info(f"✓ Escrito {OUT.relative_to(ROOT)} ({OUT.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
