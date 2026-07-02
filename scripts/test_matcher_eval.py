"""Test de regresión del matcher evento↔instrumento contra el eval set CONGELADO
(eval/matcher_eval_set.json, etiquetado a mano — casos del usuario + auditoría).

REGLA: ningún cambio al matcher se adopta si este test empeora. El objetivo de
adopción es precisión y recall ≥ ~90% en el set (y crecer el set, no encogerlo).

Uso:  .venv/bin/python scripts/test_matcher_eval.py
Requiere: semaforo.db con articulos (22 fuentes) del periodo de los casos.
"""
import json, math, sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import sqlite3
from scripts.matcher_evento import terms

DF_ESP = 150      # término específico: raro en media
MIN_NOTAS = 3     # volumen de agenda: >=3 notas del mismo evento
MIN_SCORE = 8.0


def decision(titulo, fecha, media, mterms, mdate, df, idf, inv):
    d0 = date.fromisoformat(fecha[:10])
    ti = terms(titulo)
    esp = {x for x in ti if df.get(x, 0) < DF_ESP}
    if not esp:
        return False, []
    cand = set()
    for x in esp:
        cand.update(inv.get(x, []))
    notas = []
    for i in cand:
        if d0 - timedelta(days=21) <= mdate[i] <= d0:
            sh = {x for x in (ti & mterms[i]) if df.get(x, 0) < 1500}
            if any(df.get(x, 0) < DF_ESP for x in sh):
                notas.append((sum(idf(x) for x in sh), i, sorted(sh, key=lambda x: -idf(x))[:3]))
    notas.sort(reverse=True)
    return (len(notas) >= MIN_NOTAS and notas[0][0] >= MIN_SCORE), notas[:2]


def main():
    ev = json.loads((ROOT / "eval" / "matcher_eval_set.json").read_text())
    con = sqlite3.connect(str(ROOT / "semaforo.db"))
    media = [(r[0], (r[1] or "") + " " + (r[2] or "")) for r in con.execute(
        "SELECT substr(fecha,1,10), titulo, resumen FROM articulos "
        "WHERE fecha>='2026-01-01' AND titulo IS NOT NULL")]
    mterms = [terms(t) for _, t in media]
    mdate = [date.fromisoformat(d) for d, _ in media]
    df = Counter()
    for s in mterms:
        df.update(s)
    N = len(media)
    idf = lambda x: math.log(N / (1 + df.get(x, 0)))
    inv = defaultdict(list)
    for i, s in enumerate(mterms):
        for x in s:
            if df.get(x, 0) < DF_ESP:
                inv[x].append(i)

    tp = fp = fn = tn = 0
    fallos = []
    for c in ev["casos"]:
        got, notas = decision(c["titulo"], c["fecha"], media, mterms, mdate, df, idf, inv)
        esperado = c["verdad"] == "si"
        if got and esperado:
            tp += 1
        elif got and not esperado:
            fp += 1; fallos.append((c["id"], "FALSO POSITIVO", notas[0][2] if notas else []))
        elif not got and esperado:
            fn += 1; fallos.append((c["id"], "FALSO NEGATIVO (evento real perdido)", []))
        else:
            tn += 1
    n = tp + fp + fn + tn
    prec = tp / (tp + fp) if tp + fp else 0
    rec = tp / (tp + fn) if tp + fn else 0
    print(f"eval set: {n} casos | TP={tp} FP={fp} FN={fn} TN={tn}")
    print(f"precisión={prec:.0%}  recall={rec:.0%}")
    for fid, tipo, anc in fallos:
        print(f"  ✗ {fid}: {tipo} {anc}")
    # Umbral de adopción: ambos >= 90%
    ok = prec >= 0.90 and rec >= 0.90
    print("\n" + ("✅ PASA el umbral de adopción (≥90%/≥90%)" if ok
                  else "❌ NO pasa aún el umbral de adopción (≥90%/≥90%) — NO adoptar en reactividad"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
