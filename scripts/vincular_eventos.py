"""Pipeline de vinculación evento↔instrumento (CAPA NUEVA, aditiva).

Para cada instrumento (iniciativa/proposición con PA), busca la cobertura
mediática del MISMO evento en las 3 semanas previas (matcher por entidad,
scripts/matcher_evento). NO toca scores, comisiones, efectividad ni descripciones
— es una tabla nueva `evento_vinculos` con la atribución.

Corpus media: articulos (22 fuentes). Escribe: instrumento, si tiene precedente
mediático, fuerza del vínculo, la nota principal, y el lead (días que la media
antecede). Reusable en el pipeline.
"""
import sys, re, math
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import sqlite3
from scripts.matcher_evento import terms

DB = ROOT / "semaforo.db"
DESDE_MEDIA = "2026-01-01"        # articulos multi-fuente
DESDE_INST, HASTA_INST = "2026-02-01", "2026-05-31"
UMBRAL = 8.0                       # score mínimo para "tiene precedente"
DF_ESP = 150


def main():
    con = sqlite3.connect(str(DB)); con.row_factory = sqlite3.Row
    media = [(r[0], (r[1] or "") + " " + (r[2] or "")) for r in con.execute(
        "SELECT substr(fecha,1,10) d, titulo, resumen FROM articulos "
        "WHERE fecha>=? AND titulo IS NOT NULL", (DESDE_MEDIA,))]
    print(f"corpus media (22 fuentes): {len(media)}", flush=True)
    mterms = [terms(t) for _, t in media]
    mdate = [date.fromisoformat(d) for d, _ in media]
    df = Counter()
    for s in mterms:
        df.update(s)
    N = len(media); idf = lambda x: math.log(N / (1 + df.get(x, 0)))
    inv = defaultdict(list)
    for i, s in enumerate(mterms):
        for x in s:
            if df.get(x, 0) < DF_ESP:
                inv[x].append(i)

    def link(ti, esp, d0):
        cand = set()
        for x in esp:
            cand.update(inv.get(x, []))
        best = (0.0, None)
        for i in cand:
            if d0 - timedelta(days=21) <= mdate[i] <= d0 + timedelta(days=3):
                sh = {x for x in (ti & mterms[i]) if df.get(x, 0) < 1500}
                if any(df.get(x, 0) < DF_ESP for x in sh):
                    sc = sum(idf(x) for x in sh)
                    if sc > best[0]:
                        best = (sc, i)
        return best

    con.execute("""CREATE TABLE IF NOT EXISTS evento_vinculos (
        sil_id INTEGER PRIMARY KEY, fecha TEXT, presentador TEXT, tipo_grupo TEXT,
        tiene_precedente INTEGER, score REAL, nota_fecha TEXT, nota_titulo TEXT, lead_dias INTEGER)""")
    con.execute("DELETE FROM evento_vinculos")
    rows = con.execute("SELECT id,fecha_presentacion,presentador,tipo_grupo,titulo FROM sil_documentos "
                       "WHERE tipo_grupo IN ('Iniciativa','Proposición con Punto de Acuerdo','Proposición con PA') "
                       "AND fecha_presentacion BETWEEN ? AND ? AND length(titulo)>40",
                       (DESDE_INST, HASTA_INST)).fetchall()
    n = con_prec = 0
    for r in rows:
        d0 = date.fromisoformat(r["fecha_presentacion"][:10])
        ti = terms(r["titulo"]); esp = {x for x in ti if df.get(x, 0) < DF_ESP}
        sc, mi = (0.0, None) if not esp else link(ti, esp, d0)
        prec = 1 if sc >= UMBRAL else 0
        nf = nt = None; lead = None
        if mi is not None:
            nf = media[mi][0]; nt = media[mi][1][:120]
            lead = (d0 - mdate[mi]).days
        con.execute("INSERT OR REPLACE INTO evento_vinculos VALUES (?,?,?,?,?,?,?,?,?)",
                    (r["id"], r["fecha_presentacion"][:10], r["presentador"], r["tipo_grupo"],
                     prec, round(sc, 1), nf, nt, lead))
        n += 1; con_prec += prec
        if n % 1000 == 0:
            con.commit(); print(f"  {n}/{len(rows)}…", flush=True)
    con.commit()
    print(f"\nVinculados: {n} instrumentos | con precedente: {con_prec} = {100*con_prec//n}%", flush=True)
    return con


if __name__ == "__main__":
    main()
