"""Matcher media↔instrumento a nivel EVENTO/ENTIDAD (no categoría).

Corrige el error de fondo del análisis por categoría: clasificar notas e
instrumentos en 19 categorías amplias (a) misclasifica (un exhorto sobre un
rector detenido caía en "trabajo") y (b) FRAGMENTA un mismo evento en 6
categorías (el derrame del Golfo cayó en energia/medio_ambiente/anticorrupcion/
economia/agro/seguridad). Eso volvía invisible la relación media→legislación.

Aquí el vínculo es por TÉRMINOS DISTINTIVOS COMPARTIDOS (entidades/evento),
cruzando categorías:
  - normaliza (minúsculas, sin acentos) y aplica stem simple (quita 's' final)
    para que 'derrames'≡'derrame', 'hidrocarburos'≡'hidrocarburo'.
  - quita boilerplate legal + términos institucionales genéricos (nombres-de-ley:
    'medio ambiente', 'seguridad industrial', 'agencia nacional'...).
  - liga si comparten ≥1 término MUY específico (df<150 en el corpus de medios),
    pesando por IDF (raro = fuerte). Ventana temporal -21d..+3d.

Validado: caso Abud Flores (instrumento 14-ene liga a la detención del rector
12-ene + Layda Sansores) y caso derrame (instrumento liga sólo a notas del
derrame vía bigrama 'derrame hidrocarburo', sin ruido de mercado/robo).

Hallazgo corregido a escala (muestra ~1000 instrumentos 2024-2026):
  60-65% de instrumentos tienen precedente mediático del MISMO evento (-21d)
  vs 45-49% en ventana placebo (azar) → lift real ~15pts. Media SÍ precede a la
  legislación a nivel evento (moderado), lo que el análisis por-categoría perdía.
Fuentes: data/jornada_hemeroteca.json + data/proceso_hemeroteca.json.
"""
import json, re, unicodedata, math
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STOP = set("""de la el los las en y a que se por para con del al un una su sus o e segun ley leyes
articulo articulos fraccion fracciones decreto proyecto reforma reforman reformar adiciona adicionan
adicionar diversas disposiciones diverso materia exhorta exhortar solicita solicitar punto acuerdo
iniciativa proposicion general nacional federal estado estados union republica camara diputados senado
dip grupo parlamentario partido com sobre titular titulares ante asi como tres mas si no este esta ser
dar dado turno comisiones unidas mexico mexicano gobierno cuenta hace haga publico medio ambiente
seguridad industrial agencia proteccion sector petroleos comision ambiental ambientales debido proceso
situacion juridica autoridades fincar responsabilidades director ejecutivo poder respecto relativo
implementar mecanismos brevedad estricto apego informe resultados ambito atribuciones""".split())
GENERICOS = {"medio ambiente", "seguridad industrial", "agencia nacional", "sector hidrocarburos",
             "petroleos mexicanos", "comision nacional", "debido proceso", "situacion juridica",
             "poder ejecutivo"}


def _na(s):
    s = (s or "").lower()
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def _stem(t):
    return t[:-1] if len(t) > 4 and t.endswith("s") else t


def terms(text):
    toks = [_stem(t) for t in re.findall(r"[a-z0-9]+", _na(text)) if t not in STOP and len(t) > 3]
    bg = [f"{toks[i]} {toks[i+1]}" for i in range(len(toks) - 1)]
    return (set(toks) | set(bg)) - GENERICOS


def cargar_media():
    LJ = json.loads((ROOT / "data" / "jornada_hemeroteca.json").read_text())
    PR = json.loads((ROOT / "data" / "proceso_hemeroteca.json").read_text())
    media = []
    for d, secs in LJ.items():
        for sec in secs.values():
            for t in sec:
                media.append((d, t))
    for d, slugs in PR.items():
        for s in slugs:
            media.append((d, s.replace("-", " ")))
    return media


def construir(media, df_especifico=150):
    """Devuelve (mterms, mdate, df, idf, inv) para linkar."""
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
            if df.get(x, 0) < df_especifico:
                inv[x].append(i)
    return mterms, mdate, df, idf, inv


def linkar(titulo, fecha, media, mterms, mdate, df, idf, inv,
           dias_antes=21, dias_despues=3, df_especifico=150):
    """Notas del MISMO evento que el instrumento, ordenadas por fuerza del vínculo."""
    ti = terms(titulo)
    d0 = date.fromisoformat(fecha[:10])
    esp = {x for x in ti if df.get(x, 0) < df_especifico}
    cand = set()
    for x in esp:
        cand.update(inv.get(x, []))
    out = []
    for i in cand:
        if d0 - timedelta(days=dias_antes) <= mdate[i] <= d0 + timedelta(days=dias_despues):
            sh = {x for x in (ti & mterms[i]) if df.get(x, 0) < 1500}
            if any(df.get(x, 0) < df_especifico for x in sh):
                out.append({"fecha": media[i][0], "titulo": media[i][1],
                            "comparten": sorted(sh, key=lambda x: -idf(x))[:4],
                            "score": round(sum(idf(x) for x in sh), 1)})
    out.sort(key=lambda r: -r["score"])
    return out
