"""Extractor de ENTIDADES nombradas (código puro, sin IA) para el matcher evento↔instrumento.

Idea: el ancla de un vínculo REAL es una entidad distintiva (lugar/persona/caso/
empresa), no un tema ni una institución. Un instrumento y una nota que solo
comparten 'Secretaría de Salud' o 'código penal' NO son el mismo evento; los que
comparten 'Tepeaca'+'huachicol' o 'Abud'+'Campeche' sí lo son.

Extrae secuencias en Mayúscula del título, descarta las de cabeza institucional
(Secretaría/Comisión/Instituto/...), y añade toponimos del gazetteer existente
(nlp/geo_clasificador.py). Usado por el reranker como señal de especificidad.
"""
import re
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from nlp.geo_clasificador import ESTADOS_MEXICO, CIUDADES_ESTADO


def _na(s):
    s = (s or "").lower()
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


# Cabezas institucionales: si una secuencia en Mayúscula empieza así, NO es entidad-evento
_INST = set(_na(x) for x in """secretaria comision instituto fiscalia procuraduria consejo gobierno
poder camara senado congreso guardia direccion coordinacion agencia servicio banco sistema programa
fondo ley tribunal suprema corte junta petroleos ejecutivo federacion republica presidencia mesa
comite auditoria contraloria unidad organo centro estados estado union nacional federal general
titular universidad hospital escuela conade conagua conahcyt cndh inai ine inegi""".split())

_CONECT = set(_na(x) for x in "de del la las los el y e a en por para con al su".split())

_GAZ = set()
for _k in ESTADOS_MEXICO:
    _GAZ.add(_na(_k.replace("_", " ")))
    _GAZ.add(_na(_k))
for _c in CIUDADES_ESTADO:
    _GAZ.add(_na(_c))


def _stem(t):
    return t[:-1] if len(t) > 4 and t.endswith("s") else t


def entidades(titulo):
    """Devuelve un set de tokens+bigramas normalizados que son ENTIDAD distintiva."""
    seqs = re.findall(
        r"[A-ZÁÉÍÓÚÑ][\wáéíóúñ]+(?:\s+(?:de|del|la|las|los|el|y|[A-ZÁÉÍÓÚÑ][\wáéíóúñ]+))*", titulo or "")
    ents = set()
    for seq in seqs:
        toks = [_na(t) for t in seq.split()]
        while toks and toks[0] in _CONECT:
            toks = toks[1:]
        while toks and toks[-1] in _CONECT:
            toks = toks[:-1]
        if not toks or toks[0] in _INST:
            continue
        content = [_stem(t) for t in toks if t not in _CONECT and len(t) > 2 and t not in _INST]
        for t in content:
            ents.add(t)
        for i in range(len(content) - 1):
            ents.add(f"{content[i]} {content[i+1]}")
    tn = _na(titulo)
    for g in _GAZ:
        if re.search(r"\b" + re.escape(g) + r"\b", tn):
            for t in g.split():
                if len(t) > 2:
                    ents.add(_stem(t))
            if " " in g:
                ents.add(g)
    return ents
