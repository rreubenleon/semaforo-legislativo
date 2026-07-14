"""Clave de contenido de un instrumento legislativo (compartida entre el
lector de documentos, el vinculador y el detector de seriales).

nucleo()   quita numeración, prefijo de autores, encabezados de tipo y
           sufijos de trámite — deja el OBJETO.
key12()    clave de agrupación: primeros 12 tokens de contenido del núcleo.
apellidos() set de apellidos del presentador (+ "a cargo de" del título).
Medido por el metodólogo (12-jul): clave-12 con guard de autor no produce
falsos agrupamientos; Jaccard sin especificidad sí ("diversas disposiciones").
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.sanar_titulos_truncados import na, toks, FUNCIONALES

HONOR = {"dip", "sen", "diputado", "diputada", "senador", "senadora", "grupo",
         "parlamentario", "morena", "pan", "pri", "pt", "pvem", "mc", "prd",
         "na", "sp", "integrantes", "diversos", "del", "de", "la", "el",
         "los", "las", "y", "dr", "dra", "mtro", "mtra", "lic", "ing",
         "no", "identificado"}

_PRE = re.compile(
    r"^(?:\d+\s*\.?\s+)?(?:(?:proyecto de decreto|iniciativa|proposicion|minuta)\s+)?"
    r"(?:con\s+(?:proyecto de decreto|punto de acuerdo)\s+)?"
    r"(?:por el que\s+|por la que\s+|que\s+|a fin de\s+)?", re.I)
_PRE_AUTOR = re.compile(
    r"^.{0,600}?(con\s+(?:punto de acuerdo|proyecto de decreto|proposici[oó]n)\s+"
    r"|por el que\s+|que\s+(?=(?:reforma|adiciona|expide|deroga|modifica)))",
    re.I | re.S)
_SUF = re.compile(
    r"(,?\s*(?:a cargo de|recibida? de|suscrit[oa]s? por|presentad[oa] por)\s.*"
    r"|\.\s*se\s+(?:turn[oó]|dio turno|remiti[oó]|deshech[oó]|desech[oó]).*"
    r"|\.\s*(?:concluid[oa]|aprobad[oa] en votaci[oó]n).*)$", re.I | re.S)
_CARGO = re.compile(
    r"a cargo (?:del|de la) (?:diputad[oa]|senador[a]?)\s+([^,]{4,60})", re.I)


def nucleo(titulo):
    t = na(titulo).strip()
    m = _PRE_AUTOR.search(t)
    if m and m.start(1) > 40:
        t = t[m.end(1):] if m.group(1).strip() == "por el que" else t[m.start(1):]
    t = _SUF.sub("", t)
    return _PRE.sub("", t).strip()


def ctoks(titulo):
    return [x for x in toks(nucleo(titulo)) if x not in FUNCIONALES]


def key12(titulo):
    return " ".join(ctoks(titulo)[:12])


def apellidos(autor, titulo=""):
    base = na(autor or "")
    if titulo:
        m = _CARGO.search(na(titulo))
        if m:
            base += " " + m.group(1)
    return {x for x in toks(base) if len(x) > 2 and x not in HONOR}


def jaccard(a, b):
    A, B = set(a), set(b)
    return len(A & B) / max(1, len(A | B))
