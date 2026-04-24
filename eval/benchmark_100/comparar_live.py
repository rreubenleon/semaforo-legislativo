"""
Variante del comparador: reclasifica los 100 títulos usando el clasificador
de CÓDIGO ACTUAL (en vez de los scores almacenados en BD). Esto refleja
los fixes que están en main pero aún no se aplicaron a los artículos ya
scrappeados en la BD local.

Uso:
    python eval/benchmark_100/comparar_live.py benchmark_etiquetas.json
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
from nlp.clasificador import clasificar_texto

DATA_FILE = Path(__file__).resolve().parent / "data.json"
ESPECIALES = {"ninguna", "ambigua"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("etiquetas_json")
    args = parser.parse_args()

    datos = {str(a["id"]): a for a in json.load(open(DATA_FILE))}
    etiquetas = json.load(open(args.etiquetas_json))["etiquetas"]

    pares = []
    for id_str, cat_real in etiquetas.items():
        if not cat_real or id_str not in datos:
            continue
        a = datos[id_str]
        scores = clasificar_texto(a["titulo"], a["resumen"] or "")
        cat_pred = list(scores.keys())[0] if scores else None
        score_pred = list(scores.values())[0] if scores else 0.0
        pares.append({
            "id": a["id"],
            "titulo": a["titulo"],
            "tipo": a["tipo"],
            "score_viejo": a["score"],
            "cat_viejo": a["cat_clasificador"],
            "score_nuevo": round(score_pred, 3),
            "cat_nuevo": cat_pred,
            "cat_real": cat_real,
        })

    print(f"\n  ═══ Benchmark con clasificador LIVE (código actual) ═══")
    print(f"  Evaluados: {len(pares)}")

    # Aciertos: cat_real == cat_pred (o si real es 'ninguna', pred debe ser None)
    def acierto(p):
        if p["cat_real"] == "ninguna":
            return p["cat_nuevo"] is None
        if p["cat_real"] == "ambigua":
            return True  # no penalizamos ambiguos
        return p["cat_nuevo"] == p["cat_real"]

    hits = sum(1 for p in pares if acierto(p))

    # Métricas por tipo
    print(f"\n  ACCURACY GLOBAL (LIVE)")
    print(f"  {'─' * 60}")
    print(f"  Aciertos: {hits}/{len(pares)} ({100*hits/len(pares):.1f}%)")

    # Sin los ambiguos (que son libres)
    puros = [p for p in pares if p["cat_real"] not in ("ambigua",)]
    hits_puros = sum(1 for p in puros if acierto(p))
    print(f"  Sin casos 'ambigua': {hits_puros}/{len(puros)} ({100*hits_puros/len(puros):.1f}%)")

    # Comparativa vieja vs nueva
    print(f"\n  DELTA VS CLASIFICADOR VIEJO (scores en BD)")
    print(f"  {'─' * 60}")
    def acierto_viejo(p):
        if p["cat_real"] == "ninguna":
            return False  # el viejo clasificó todo, asumimos FP si 'ninguna'
        if p["cat_real"] == "ambigua":
            return True
        return p["cat_viejo"] == p["cat_real"]
    hits_viejo = sum(1 for p in pares if acierto_viejo(p))
    print(f"  Viejo:  {hits_viejo}/{len(pares)} ({100*hits_viejo/len(pares):.1f}%)")
    print(f"  Nuevo:  {hits}/{len(pares)} ({100*hits/len(pares):.1f}%)")
    print(f"  Delta:  {hits - hits_viejo:+d} aciertos")

    # Casos donde nuevo acertó y viejo erró
    rescates = [p for p in pares if acierto(p) and not acierto_viejo(p)]
    regresiones = [p for p in pares if not acierto(p) and acierto_viejo(p)]
    print(f"\n  Rescates (nuevo acierta, viejo erraba): {len(rescates)}")
    print(f"  Regresiones (nuevo erra, viejo acertaba): {len(regresiones)}")

    if regresiones:
        print(f"\n  REGRESIONES — casos que se perdieron con los fixes:")
        for p in regresiones[:10]:
            print(f"    - [{p['titulo'][:60]}]")
            print(f"      real={p['cat_real']}  viejo={p['cat_viejo']}  nuevo={p['cat_nuevo']}")

    # Precision/Recall por categoría con clasificador nuevo
    print(f"\n  PRECISION / RECALL POR CATEGORÍA (LIVE)")
    print(f"  {'─' * 80}")
    cats = sorted({p["cat_real"] for p in pares} | {p["cat_nuevo"] for p in pares if p["cat_nuevo"]} - ESPECIALES)
    rows = []
    for cat in cats:
        tp = sum(1 for p in pares if p["cat_nuevo"] == cat and p["cat_real"] == cat)
        fp = sum(1 for p in pares if p["cat_nuevo"] == cat and p["cat_real"] != cat and p["cat_real"] not in ESPECIALES)
        fn = sum(1 for p in pares if p["cat_nuevo"] != cat and p["cat_real"] == cat)
        soporte = sum(1 for p in pares if p["cat_real"] == cat)
        prec = tp / (tp + fp) if (tp + fp) else 0
        rec = tp / (tp + fn) if (tp + fn) else 0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0
        rows.append({"cat": cat, "tp": tp, "fp": fp, "fn": fn, "prec": prec, "rec": rec, "f1": f1, "soporte": soporte})

    print(f"  {'CAT':<26} {'TP':>3} {'FP':>3} {'FN':>3}  {'PREC':>6}  {'REC':>6}  {'F1':>6}  {'n':>4}")
    print(f"  {'─' * 80}")
    for r in rows:
        print(f"  {r['cat']:<26} {r['tp']:>3} {r['fp']:>3} {r['fn']:>3}  {r['prec']:>5.1%}  {r['rec']:>5.1%}  {r['f1']:>5.1%}  {r['soporte']:>4}")

    macro_f1 = sum(r["f1"] for r in rows) / len(rows) if rows else 0
    print(f"  {'─' * 80}")
    print(f"  Macro F1: {macro_f1:.1%}")

    # Categorías sin clasificar
    sin_clasificar = [p for p in pares if p["cat_nuevo"] is None and p["cat_real"] not in ESPECIALES]
    print(f"\n  Sin clasificar (cat_real válida pero FIAT no clasificó): {len(sin_clasificar)}")
    for p in sin_clasificar[:8]:
        print(f"    - [{p['titulo'][:60]}]")
        print(f"      real: {p['cat_real']}")


if __name__ == "__main__":
    main()
