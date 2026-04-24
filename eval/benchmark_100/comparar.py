"""
Procesa el benchmark_etiquetas.json que exportaste del HTML y genera:
  - Matriz de confusión
  - Precision / Recall / F1 por categoría
  - Accuracy global
  - Top 10 errores más descarados

Uso:
    python eval/benchmark_100/comparar.py benchmark_etiquetas.json
"""
import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
DATA_FILE = Path(__file__).resolve().parent / "data.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("etiquetas_json", help="Ruta al JSON exportado del HTML")
    args = parser.parse_args()

    # Cargar datos base y etiquetas
    datos = {str(a["id"]): a for a in json.load(open(DATA_FILE))}
    export = json.load(open(args.etiquetas_json))
    etiquetas = export["etiquetas"]

    print()
    print(f"  ═══ Benchmark de clasificador ═══")
    print(f"  Fecha export:    {export['fecha']}")
    print(f"  Etiquetados:     {export['n']}/100")
    print()

    # Filtrar solo los etiquetados
    pares = []
    for id_str, cat_real in etiquetas.items():
        if not cat_real:
            continue
        if id_str not in datos:
            continue
        a = datos[id_str]
        pares.append({
            "id": a["id"],
            "titulo": a["titulo"],
            "fuente": a["fuente"],
            "fecha": a["fecha"],
            "tipo": a["tipo"],
            "score": a["score"],
            "cat_pred": a["cat_clasificador"],
            "cat_real": cat_real,
        })

    if not pares:
        print("  Sin etiquetas para procesar.")
        return

    # Accuracy global
    aciertos = sum(1 for p in pares if p["cat_pred"] == p["cat_real"])
    # Casos que el user marcó "ninguna" o "ambigua" son especiales
    cat_real_unicas = set(p["cat_real"] for p in pares)
    especiales = {"ninguna", "ambigua"}
    eval_puros = [p for p in pares if p["cat_real"] not in especiales]

    print(f"  {'─' * 60}")
    print(f"  ACCURACY GLOBAL")
    print(f"  {'─' * 60}")
    print(f"  Total evaluados:            {len(pares)}")
    print(f"  Aciertos exactos:           {aciertos} ({100*aciertos/len(pares):.1f}%)")

    ningunas = sum(1 for p in pares if p["cat_real"] == "ninguna")
    ambiguas = sum(1 for p in pares if p["cat_real"] == "ambigua")
    print(f"  Marcados 'ninguna':         {ningunas} (FIAT no debió clasificar)")
    print(f"  Marcados 'ambigua':         {ambiguas}")
    print()

    # Por tipo
    print(f"  ACCURACY POR TIPO DE CASO")
    print(f"  {'─' * 60}")
    for tipo in ["confiado", "ambiguo", "wildcard"]:
        sub = [p for p in pares if p["tipo"] == tipo]
        if not sub:
            continue
        hits = sum(1 for p in sub if p["cat_pred"] == p["cat_real"])
        print(f"  {tipo:<12} n={len(sub):<4} aciertos={hits:<4} ({100*hits/len(sub):.1f}%)")
    print()

    # Precision/Recall/F1 por categoría
    cats = sorted(set(p["cat_pred"] for p in pares) | set(p["cat_real"] for p in pares) - especiales)

    print(f"  PRECISION / RECALL / F1 POR CATEGORÍA")
    print(f"  {'─' * 80}")
    print(f"  {'CATEGORÍA':<26} {'TP':>4} {'FP':>4} {'FN':>4}  {'PREC':>6}  {'REC':>6}  {'F1':>6}  {'SOPORTE':>8}")
    print(f"  {'─' * 80}")
    por_cat_metricas = []
    for cat in cats:
        tp = sum(1 for p in pares if p["cat_pred"] == cat and p["cat_real"] == cat)
        fp = sum(1 for p in pares if p["cat_pred"] == cat and p["cat_real"] != cat and p["cat_real"] not in especiales)
        fn = sum(1 for p in pares if p["cat_pred"] != cat and p["cat_real"] == cat)
        soporte = sum(1 for p in pares if p["cat_real"] == cat)
        prec = tp / (tp + fp) if (tp + fp) else 0
        rec = tp / (tp + fn) if (tp + fn) else 0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0
        por_cat_metricas.append({"cat": cat, "tp": tp, "fp": fp, "fn": fn,
                                  "prec": prec, "rec": rec, "f1": f1, "soporte": soporte})
        print(f"  {cat:<26} {tp:>4} {fp:>4} {fn:>4}  {prec:>5.1%}  {rec:>5.1%}  {f1:>5.1%}  {soporte:>8}")

    # Agregado macro
    if por_cat_metricas:
        prom_prec = sum(m["prec"] for m in por_cat_metricas) / len(por_cat_metricas)
        prom_rec = sum(m["rec"] for m in por_cat_metricas) / len(por_cat_metricas)
        prom_f1 = sum(m["f1"] for m in por_cat_metricas) / len(por_cat_metricas)
        print(f"  {'─' * 80}")
        print(f"  {'MACRO PROMEDIO':<26} {'':>4} {'':>4} {'':>4}  {prom_prec:>5.1%}  {prom_rec:>5.1%}  {prom_f1:>5.1%}")
    print()

    # Peores categorías
    print(f"  TOP 5 CATEGORÍAS MÁS SUCIAS (más FP)")
    print(f"  {'─' * 60}")
    peores = sorted(por_cat_metricas, key=lambda m: -m["fp"])[:5]
    for m in peores:
        if m["fp"] == 0:
            continue
        print(f"  {m['cat']:<26} FP={m['fp']}  precision={m['prec']:.1%}")
    print()

    # Errores más descarados: confiados que fallaron
    print(f"  TOP 10 ERRORES MÁS DESCARADOS (FIAT confiado pero erró)")
    print(f"  {'─' * 80}")
    errores_confiados = [p for p in eval_puros
                          if p["cat_pred"] != p["cat_real"] and p["score"] >= 0.6]
    errores_confiados.sort(key=lambda p: -p["score"])
    for p in errores_confiados[:10]:
        print(f"  [{p['score']:.2f}] {p['titulo'][:70]}")
        print(f"         FIAT: {p['cat_pred']:<22} → Real: {p['cat_real']}")
        print()

    # Guardar reporte completo
    out = Path(__file__).resolve().parent / "resultados.json"
    reporte = {
        "fecha": export["fecha"],
        "n": export["n"],
        "accuracy_global": round(100 * aciertos / len(pares), 2),
        "por_tipo": {
            t: {
                "n": sum(1 for p in pares if p["tipo"] == t),
                "hits": sum(1 for p in pares if p["tipo"] == t and p["cat_pred"] == p["cat_real"]),
            } for t in ["confiado", "ambiguo", "wildcard"]
        },
        "por_categoria": por_cat_metricas,
        "errores_confiados": [{"titulo": p["titulo"], "fiat": p["cat_pred"],
                                "real": p["cat_real"], "score": p["score"]}
                               for p in errores_confiados],
    }
    out.write_text(json.dumps(reporte, ensure_ascii=False, indent=2))
    print(f"  ✓ Reporte completo: {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
