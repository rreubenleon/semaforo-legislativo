"""
Benchmark con dos pasadas:
  1) Clasificador keyword (código actual)
  2) Haiku 4.5 como reclasificador SOLO para casos ambiguos (score 0.25–0.55)
     o sin clasificar.

Mide el delta de accuracy. El objetivo es subir del 72% actual a >80%.

Uso:
    export ANTHROPIC_API_KEY=...
    python eval/benchmark_100/comparar_con_haiku.py benchmark_etiquetas.json
"""
import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
from nlp.clasificador import clasificar_texto
from nlp.reclasificador_claude import reclasificar

DATA_FILE = Path(__file__).resolve().parent / "data.json"
ESPECIALES = {"ninguna", "ambigua"}

# Zona donde el keyword classifier es incierto → usar Haiku
AMBIGUO_MIN = 0.25
AMBIGUO_MAX = 0.55


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("etiquetas_json")
    args = parser.parse_args()

    datos = {str(a["id"]): a for a in json.load(open(DATA_FILE))}
    etiquetas = json.load(open(args.etiquetas_json))["etiquetas"]

    # Conn a BD principal para usar el cache del reclasificador
    conn = sqlite3.connect(str(ROOT / "semaforo.db"))

    pares = []
    t0 = time.time()
    llamadas_haiku = 0
    total_casos = sum(1 for id_str, cat_real in etiquetas.items() if cat_real and id_str in datos)
    procesados = 0

    for id_str, cat_real in etiquetas.items():
        if not cat_real or id_str not in datos:
            continue
        a = datos[id_str]
        procesados += 1
        if procesados % 10 == 0:
            print(f"  ... {procesados}/{total_casos} procesados ({llamadas_haiku} llamadas Haiku, {time.time()-t0:.1f}s)", flush=True)

        # 1) Keyword classifier
        scores_kw = clasificar_texto(a["titulo"], a["resumen"] or "")
        cat_kw = list(scores_kw.keys())[0] if scores_kw else None
        score_kw = list(scores_kw.values())[0] if scores_kw else 0.0

        # 2) Decidir si usar Haiku
        ambiguo = (
            cat_kw is None
            or AMBIGUO_MIN <= score_kw <= AMBIGUO_MAX
        )

        cat_final = cat_kw
        haiku_usado = False
        cat_haiku = None

        if ambiguo:
            cat_haiku = reclasificar(a["titulo"], a["resumen"] or "", conn)
            haiku_usado = True
            llamadas_haiku += 1
            if cat_haiku:
                if cat_haiku == "ninguna":
                    cat_final = None
                else:
                    cat_final = cat_haiku

        pares.append({
            "id": a["id"],
            "titulo": a["titulo"],
            "cat_real": cat_real,
            "cat_kw": cat_kw,
            "score_kw": score_kw,
            "haiku_usado": haiku_usado,
            "cat_haiku": cat_haiku,
            "cat_final": cat_final,
        })

    dur = time.time() - t0

    # ── Métricas ─────────────────────────────────────
    def acierto(p, cat_pred):
        if p["cat_real"] == "ninguna":
            return cat_pred is None
        if p["cat_real"] == "ambigua":
            return True
        return cat_pred == p["cat_real"]

    n = len(pares)
    hits_kw = sum(1 for p in pares if acierto(p, p["cat_kw"]))
    hits_final = sum(1 for p in pares if acierto(p, p["cat_final"]))

    print()
    print(f"  ═══ Benchmark con Haiku 4.5 como reclasificador ═══")
    print(f"  Casos evaluados:        {n}")
    print(f"  Llamadas a Haiku:       {llamadas_haiku}")
    print(f"  Duración total:         {dur:.1f}s ({dur/max(1, llamadas_haiku):.2f}s por llamada)")
    print()
    print(f"  Accuracy SOLO keyword:   {hits_kw}/{n} ({100*hits_kw/n:.1f}%)")
    print(f"  Accuracy keyword+Haiku:  {hits_final}/{n} ({100*hits_final/n:.1f}%)")
    print(f"  Delta:                  {hits_final - hits_kw:+d} aciertos")
    print()

    # Sin ambigua (el benchmark crudo)
    puros = [p for p in pares if p["cat_real"] != "ambigua"]
    hits_kw_puros = sum(1 for p in puros if acierto(p, p["cat_kw"]))
    hits_final_puros = sum(1 for p in puros if acierto(p, p["cat_final"]))
    print(f"  Sin casos 'ambigua' ({len(puros)}):")
    print(f"    keyword:        {hits_kw_puros}/{len(puros)} ({100*hits_kw_puros/len(puros):.1f}%)")
    print(f"    keyword+Haiku:  {hits_final_puros}/{len(puros)} ({100*hits_final_puros/len(puros):.1f}%)")
    print()

    # Desglose de lo que hizo Haiku
    ayudo = [p for p in pares if p["haiku_usado"] and acierto(p, p["cat_final"]) and not acierto(p, p["cat_kw"])]
    rompio = [p for p in pares if p["haiku_usado"] and not acierto(p, p["cat_final"]) and acierto(p, p["cat_kw"])]
    mantuvo = [p for p in pares if p["haiku_usado"] and acierto(p, p["cat_final"]) and acierto(p, p["cat_kw"])]
    sigue_mal = [p for p in pares if p["haiku_usado"] and not acierto(p, p["cat_final"])]

    print(f"  Haiku en {llamadas_haiku} casos ambiguos:")
    print(f"    Rescata:     {len(ayudo):>3}  (keyword erraba → Haiku acierta)")
    print(f"    Rompe:       {len(rompio):>3}  (keyword acertaba → Haiku yerra)")
    print(f"    Mantiene:    {len(mantuvo):>3}  (ambos aciertan)")
    print(f"    Siguen mal:  {len(sigue_mal):>3}  (ninguno acierta)")
    print(f"    Delta neto:  {len(ayudo) - len(rompio):+d}")
    print()

    # Ejemplos de rescates
    if ayudo:
        print(f"  Ejemplos de RESCATES (Haiku arregla lo que keyword falla):")
        for p in ayudo[:5]:
            print(f"    [{p['score_kw']:.2f}] {p['titulo'][:68]}")
            print(f"           keyword={p['cat_kw']}  haiku={p['cat_haiku']}  real={p['cat_real']}")
        print()

    if rompio:
        print(f"  ⚠ Regresiones (Haiku rompe lo que keyword acierta):")
        for p in rompio[:5]:
            print(f"    [{p['score_kw']:.2f}] {p['titulo'][:68]}")
            print(f"           keyword={p['cat_kw']}  haiku={p['cat_haiku']}  real={p['cat_real']}")
        print()

    # Guardar JSON de resultados
    out = Path(__file__).resolve().parent / "resultados_haiku.json"
    payload = {
        "n": n,
        "llamadas_haiku": llamadas_haiku,
        "duracion_s": round(dur, 2),
        "accuracy_keyword": round(100 * hits_kw / n, 2),
        "accuracy_final": round(100 * hits_final / n, 2),
        "delta": hits_final - hits_kw,
        "rescates": len(ayudo),
        "regresiones": len(rompio),
        "detalle": pares,
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"  ✓ Detalle: {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
