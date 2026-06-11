"""Califica el PREDICTOR DE AUTORÍA contra la realidad (eslabón 4 de la cadena).

Pregunta de la visión: "¿el legislador que REALMENTE presentó estaba en el
top-K que predijo FIAT, prediciendo SOLO con datos previos a esa fecha?"

Backtest punto-en-el-tiempo (sin fuga): para cada instrumento histórico real
(autor A, categoría C, fecha D), corre predecir_autores(C, ref_date = D-1) y
mide si A cayó en top-1/3/5/10/20. Compara contra un baseline por volumen
(los más prolíficos en C antes de D).

Read-only, $0, sin Haiku.

Uso: python eval/validar_predictor_autoria.py [--n 200] [--desde 2025-09-01]
"""
from __future__ import annotations
import os
os.environ.pop("ANTHROPIC_API_KEY", None)
os.environ.pop("FIAT_CLAUDE_API_KEY", None)

import argparse, sys, random
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from db import get_connection
from api.predictor_autoria import predecir_autores

TOPKS = [1, 3, 5, 10, 20]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200, help="instrumentos a muestrear")
    ap.add_argument("--desde", default="2025-09-01", help="solo instrumentos desde esta fecha")
    ap.add_argument("--hasta", default="2026-04-15", help="hasta esta fecha (deja margen de datos)")
    ap.add_argument("--json", default=None, help="ruta para escribir el resultado en JSON")
    args = ap.parse_args()

    conn = get_connection()

    # Universo: instrumentos reales sustantivos con autor + categoría + fecha,
    # en el rango de prueba. Estos son los "exámenes" del predictor.
    rows = conn.execute("""
        SELECT legislador_id, categoria, fecha_presentacion
        FROM actividad_legislador
        WHERE legislador_id IS NOT NULL
          AND categoria IS NOT NULL AND categoria != ''
          AND fecha_presentacion >= ? AND fecha_presentacion <= ?
          AND (LOWER(tipo_instrumento) LIKE '%iniciativ%'
               OR LOWER(tipo_instrumento) LIKE '%proposici%')
    """, (args.desde, args.hasta)).fetchall()
    universo = [(r[0], r[1], r[2][:10]) for r in rows if r[2]]
    print(f"Instrumentos candidatos en rango: {len(universo)}")
    if not universo:
        print("Sin datos en rango."); return

    # Muestreo determinista (seed fija por reproducibilidad)
    random.seed(42)
    muestra = random.sample(universo, min(args.n, len(universo)))
    print(f"Muestra evaluada: {len(muestra)}")

    # Cache de predicciones por (categoria, ref_date) — muchos instrumentos
    # comparten cat+fecha, evita recomputar.
    cache_pred = {}
    def pred_ranking(cat, ref_date):
        key = (cat, ref_date)
        if key not in cache_pred:
            pred = predecir_autores(cat, top_n=max(TOPKS), ref_date=ref_date)
            cache_pred[key] = [p["legislador_id"] for p in pred]
        return cache_pred[key]

    # Baseline: ranking por VOLUMEN previo (los más prolíficos en C antes de D).
    cache_base = {}
    def base_ranking(cat, ref_date):
        key = (cat, ref_date)
        if key not in cache_base:
            r = conn.execute("""
                SELECT legislador_id, COUNT(*) c FROM actividad_legislador
                WHERE categoria=? AND legislador_id IS NOT NULL
                  AND fecha_presentacion <= ?
                GROUP BY legislador_id ORDER BY c DESC LIMIT ?
            """, (cat, ref_date, max(TOPKS))).fetchall()
            cache_base[key] = [x[0] for x in r]
        return cache_base[key]

    # Mapa legislador_id -> partido (para grado de PARTIDO, separado del autor)
    leg_partido = {r[0]: (r[1] or "") for r in conn.execute(
        "SELECT id, partido FROM legisladores").fetchall()}

    def partido_top(rank_ids, k=5):
        """Partido más frecuente entre los primeros k legisladores predichos."""
        from collections import Counter
        cc = Counter(leg_partido.get(i, "") for i in rank_ids[:k] if leg_partido.get(i))
        return cc.most_common(1)[0][0] if cc else None

    cache_base_part = {}
    def base_partido(cat, ref_date):
        if (cat, ref_date) not in cache_base_part:
            r = conn.execute("""
                SELECT l.partido, COUNT(*) c FROM actividad_legislador a
                JOIN legisladores l ON l.id=a.legislador_id
                WHERE a.categoria=? AND a.fecha_presentacion<=? AND l.partido!=''
                GROUP BY l.partido ORDER BY c DESC LIMIT 1
            """, (cat, ref_date)).fetchone()
            cache_base_part[(cat, ref_date)] = r[0] if r else None
        return cache_base_part[(cat, ref_date)]

    hits_pred = {k: 0 for k in TOPKS}
    hits_base = {k: 0 for k in TOPKS}
    hits_pred_nv = {k: 0 for k in TOPKS}  # solo cuando el predictor devolvió algo
    hits_part_pred = 0   # partido del autor real == partido top-1 predicho
    hits_part_base = 0   # partido del autor real == partido más prolífico (baseline)
    evaluados = 0
    pred_no_vacias = 0
    autor_novato = 0  # autor sin actividad previa en la cat → imposible de predecir

    t0 = datetime.now()
    for i, (autor, cat, fecha) in enumerate(muestra, 1):
        ref = (datetime.strptime(fecha, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        # ¿el autor tenía historial previo en la cat? (techo del predictor)
        prev = conn.execute("""SELECT COUNT(*) FROM actividad_legislador
            WHERE legislador_id=? AND categoria=? AND fecha_presentacion < ?""",
            (autor, cat, fecha)).fetchone()[0]
        if prev == 0:
            autor_novato += 1

        rank_pred = pred_ranking(cat, ref)
        rank_base = base_ranking(cat, ref)
        evaluados += 1
        if rank_pred:
            pred_no_vacias += 1
        for k in TOPKS:
            if autor in rank_pred[:k]:
                hits_pred[k] += 1
                hits_pred_nv[k] += 1
            if autor in rank_base[:k]: hits_base[k] += 1
        # Grado de PARTIDO (separado): ¿el partido del autor real coincide
        # con el partido top-1 predicho? vs baseline (partido más prolífico).
        ap = leg_partido.get(autor, "")
        if ap:
            if partido_top(rank_pred) == ap: hits_part_pred += 1
            if base_partido(cat, ref) == ap: hits_part_base += 1
        if i % 25 == 0:
            print(f"  {i}/{len(muestra)}…")

    print("\n" + "=" * 56)
    print(f"CALIFICACIÓN DEL PREDICTOR DE AUTORÍA — {evaluados} instrumentos")
    print(f"Rango: {args.desde} → {args.hasta} | {(datetime.now()-t0).seconds}s")
    print(f"Autores sin historial previo en su categoría: {autor_novato} "
          f"({100*autor_novato/evaluados:.0f}%) — techo de lo predecible")
    print("=" * 56)
    print(f"Predicciones no vacías: {pred_no_vacias}/{evaluados} "
          f"({100*pred_no_vacias/evaluados:.0f}%)")
    print("-" * 56)
    print(f"{'top-K':>6} | {'PRED(todo)':>10} | {'PRED(s/vacías)':>14} | {'baseline':>9}")
    print("-" * 56)
    for k in TOPKS:
        pp = 100 * hits_pred[k] / evaluados
        nv = 100 * hits_pred_nv[k] / pred_no_vacias if pred_no_vacias else 0
        bb = 100 * hits_base[k] / evaluados
        print(f"{('top-'+str(k)):>6} | {pp:>9.1f}% | {nv:>13.1f}% | {bb:>8.1f}%")
    pp_part = 100 * hits_part_pred / evaluados
    bb_part = 100 * hits_part_base / evaluados
    print("-" * 56)
    print(f"PARTIDO del autor: predictor {pp_part:.1f}% | baseline {bb_part:.1f}%")
    print("=" * 56)
    print("Interpretación: top-5 = de cada 100 instrumentos reales, en cuántos")
    print("el autor real estaba entre los 5 que FIAT predijo (con datos previos).")

    # Exportar JSON para que la página Métricas lo consuma (calificación
    # SEPARADA del score, como pidió el user).
    resultado = {
        "fecha_calculo": args.hasta,
        "rango": {"desde": args.desde, "hasta": args.hasta},
        "n_instrumentos": evaluados,
        "autores_sin_historial_pct": round(100 * autor_novato / evaluados, 1),
        "autor": {("top_" + str(k)): {
            "predictor": round(100 * hits_pred[k] / evaluados, 1),
            "baseline": round(100 * hits_base[k] / evaluados, 1),
        } for k in TOPKS},
        "partido": {
            "predictor": round(pp_part, 1),
            "baseline": round(bb_part, 1),
        },
        "metodologia": "Backtest punto-en-el-tiempo (ref_date = fecha-1, sin fuga). "
                       "Autoría GENERAL (no anclada a picos). Baseline = legisladores "
                       "más prolíficos en la categoría hasta esa fecha.",
    }
    if args.json:
        from pathlib import Path as _P
        _P(args.json).write_text(__import__("json").dumps(resultado, ensure_ascii=False, indent=2))
        print(f"\n✅ JSON escrito en {args.json}")


if __name__ == "__main__":
    main()
