"""
Reactividad legislativa — ¿el legislador reacciona a la coyuntura mediática?

Lee la fuente LIMPIA (eval/instrumentos_sil.json, con títulos) — NUNCA
sil_documentos (contaminada). Para cada proposición con punto de acuerdo
clasifica en:
  - REACCIONÓ si: (a) toca una CONDICIÓN CRÓNICA (tema siempre en medios:
    movilidad, agua, transporte, seguridad…), (b) coincide con un PICO
    mediático de su tema (>=3 notas en 35 días), o (c) es un exhorto sobre
    SU PROPIO estado (reacción a condición local — regla del usuario).
  - TRÁMITE: efeméride/interno (no es reacción).
  - CANDIDATO: tema real sin match → se revisa para ampliar filtros.

reactividad = reaccionó / total PPA.  Solo proposiciones (las iniciativas son
estructurales, no reactivas). Salida: eval/reactividad.json.

Reglas afinadas con el usuario (jun 2026). Las CRÓNICAS crecen con su input.
"""
from __future__ import annotations
import json, re, sys, unicodedata, datetime as dt
from collections import defaultdict
from pathlib import Path
import sqlite3

ROOT = Path(__file__).resolve().parent.parent
INSTR = ROOT / "eval" / "instrumentos_sil.json"
OUT = ROOT / "eval" / "reactividad.json"

CRONICAS = {"movilidad", "transporte", "agua", "desabasto", "seguridad",
            "inseguridad", "drenaje", "hidrico"}
BOIL = set("por el que se la los las de del a en y o con para al exhorta secretaria gobierno federal estatal titular poder ejecutivo autoridades republica senado camara nacional comision general materia atender acuerdo punto relativa nuevo leon medio ambiente recursos naturales hacienda salud economia diversas distintas solicita".split())
TRAMITE = re.compile(r"relativa al dia|aniversario|conmemora|natalicio|reconoce|felicita|mesa directiva|comision especial|cabildo|creacion de la comision", re.I)
ESTADOS = {  # legislador_id → estado normalizado (cohorte + se amplía con la BD)
}


def norm(s):
    s = (s or "").lower()
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def dd(s):
    y, m, d = map(int, s[:10].split("-")); return dt.date(y, m, d)


def wilson(success, n, z=1.96):
    """Wilson 95% lower bound — idéntica a efectividadIndividual() del dashboard."""
    import math
    if n <= 0:
        return None
    p = success / n
    den = 1 + z * z / n
    center = p + z * z / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return max(0.0, (center - margin) / den)


def main():
    if not INSTR.exists():
        print(f"Falta {INSTR} (corre recontar_instrumentos_sil.py con títulos)."); return 1
    instr = json.loads(INSTR.read_text())
    # ¿trae títulos?
    sample = next((v for v in instr.values() if v), [])
    if sample and "tit" not in sample[0]:
        print("instrumentos_sil.json aún sin títulos — re-corre el reconteo primero."); return 1

    con = sqlite3.connect(str(ROOT / "semaforo.db"))
    estado = {str(r[0]): norm(r[1]) for r in con.execute(
        "SELECT id, estado FROM legisladores WHERE estado IS NOT NULL AND estado!=''").fetchall()}

    # Media: doc-freq (distintividad) + texto por fecha
    media = []; df = defaultdict(int); N = 0
    for f, t, r in con.execute("SELECT substr(fecha,1,10), titulo, resumen FROM articulos WHERE fecha>='2024-09-01'"):
        txt = norm(t) + " " + norm(r); media.append((dd(f), txt)); N += 1
        for w in set(re.findall(r"[a-z]{5,}", txt)):
            df[w] += 1

    def fuerte(w):
        return w not in BOIL and 0 < df.get(w, 0) <= N * 0.006

    out = {}
    for lid, recs in instr.items():
        edo = estado.get(lid, "")
        props = [r for r in recs if r.get("t") == "prop"]
        cr = pi = lo = ca = tr = 0
        for r in props:
            cl = r.get("tit", ""); f = r.get("f", "")
            if not f:
                continue
            if TRAMITE.search(cl):
                tr += 1; continue
            toks = set(re.findall(r"[a-z]{4,}", norm(cl)))
            if CRONICAS & toks:
                cr += 1; continue
            if edo and edo in norm(cl):
                lo += 1; continue
            try:
                f0 = dd(f)
            except Exception:
                ca += 1; continue
            terms = [w for w in re.findall(r"[a-z]{5,}", norm(cl)) if fuerte(w)]
            notas = sum(1 for d, txt in media if 0 <= (f0 - d).days <= 35 and sum(1 for w in terms if w in txt) >= 2)
            if notas >= 3 and terms:
                pi += 1
            else:
                ca += 1
        reac = cr + pi + lo; tot = reac + ca + tr
        # Wilson 95% lower bound (MISMA fórmula que efectividad — fuente única).
        # Estandariza: 1/1 NO es 100% (muestra de uno no prueba sistematicidad).
        # Ej: 1/1 → 21% · 41/48 → 73%. Quien reacciona poco-y-mucho-volumen sube.
        wlb = wilson(reac, tot)
        out[lid] = {"ppa": tot, "reacciono": reac, "cronica": cr, "pico": pi,
                    "local": lo, "candidato": ca, "tramite": tr,
                    "reactividad_cruda": round(100 * reac / tot) if tot else None,
                    "reactividad": round(100 * wlb) if wlb is not None else None}

    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    conpct = [v for v in out.values() if v["reactividad"] is not None]
    print(f"Reactividad calculada: {len(out)} legisladores, {len(conpct)} con PPA → {OUT}")
    for k in ("545", "503", "588"):
        if k in out:
            print(f"  {k}: {out[k]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
