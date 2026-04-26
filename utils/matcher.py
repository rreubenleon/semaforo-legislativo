"""
Matcher de nombres de legisladores — fuente única de verdad.

Resuelve los problemas de matching que históricamente requirieron
copiar-pegar lógica entre scripts:
  1. Diferencia de orden: BD Diputados tiene "Apellido Nombre" pero SIL
     usa formato natural "Nombre Apellido".
  2. Variantes ortográficas: "Aracely" vs "Aracelly", "Ortíz" vs "Ortiz".
  3. Apellidos con guión: "Bolaños-Cacho" → "bolanoscacho" en BD vs
     "bolanos cacho" en SIL.
  4. BD muy abreviado: "Lilly Téllez" vs SIL completo "María Lilly del
     Carmen Téllez García".

Funciones:
  normalizar_nombre(s)       → str sin acentos, lowercase, sin chars raros
  tokens_relevantes(nn)      → Counter, ignora stopwords ('de', 'la', etc.)
  encontrar_legislador_id(nombre_normalizado, camara, bd_idx) → int | None

Uso típico:
    from utils.matcher import (
        normalizar_nombre, build_bd_index, encontrar_legislador_id
    )
    bd_idx = build_bd_index(conn)
    lid = encontrar_legislador_id("ruben ignacio moreira valdez",
                                   "Diputados", bd_idx)

Desarrollado tras incidente abr 25 2026 ($13 perdidos por bug del
matcher copy-pasted que no se actualizó). Ver MEMORY.md "POLÍTICA FIRME".
"""
from __future__ import annotations

import re
import sqlite3
import unicodedata
from collections import Counter

STOP_TOKENS = {"de", "la", "del", "y", "el", "las", "los"}


def normalizar_nombre(nombre: str) -> str:
    """
    'Dip. José Elías Lixa Abimerhi (PAN)' → 'jose elias lixa abimerhi'
    'Bolaños-Cacho Cué Raúl' → 'bolanoscacho cue raul'

    Usado tanto para guardar nombre_normalizado en BD como para
    construir queries al SIL (que falla con acentos en pp_ListLegislador).
    """
    if not nombre:
        return ""
    # Quitar prefijos
    nombre = re.sub(
        r"^(Dip\.|Sen\.|C\.|Diputad[oa]|Senador[a]?)\s*",
        "", nombre.strip(), flags=re.IGNORECASE,
    )
    # Quitar partido entre paréntesis
    nombre = re.sub(r"\s*\([^)]*\)\s*", " ", nombre)
    # Quitar acentos
    nombre = unicodedata.normalize("NFKD", nombre.lower())
    nombre = "".join(c for c in nombre if not unicodedata.combining(c))
    # Solo letras y espacios
    nombre = re.sub(r"[^a-z\s]", "", nombre)
    nombre = re.sub(r"\s+", " ", nombre).strip()
    return nombre


def tokens_relevantes(nombre_normalizado: str) -> Counter:
    """Counter de tokens útiles (≥2 chars, sin stopwords)."""
    return Counter(
        t for t in nombre_normalizado.split()
        if len(t) >= 2 and t not in STOP_TOKENS
    )


def _lev1(a: str, b: str) -> bool:
    """True si a y b difieren en a lo más 1 char."""
    if a == b:
        return True
    if abs(len(a) - len(b)) > 1:
        return False
    if len(a) > len(b):
        a, b = b, a
    if len(a) == len(b):
        return sum(c1 != c2 for c1, c2 in zip(a, b)) == 1
    for i in range(len(b)):
        if b[:i] + b[i+1:] == a:
            return True
    return False


def _tokens_match_fuzzy(t1: Counter, t2: Counter) -> int:
    """
    Cuenta tokens que coinciden permitiendo:
    - Match exacto
    - Lev-1 entre tokens largos (≥4 chars) — para Aracely/Aracelly
    - Concatenación: bolanos + cacho ↔ bolanoscacho (Bolaños-Cacho)
    """
    matched = 0
    used = set()
    t1_list = list(t1)
    for tok, n in t1.items():
        if tok in t2:
            matched += min(n, t2[tok])
            continue
        if len(tok) < 4:
            continue
        for tok2 in t2:
            if tok2 in used or len(tok2) < 4:
                continue
            if _lev1(tok, tok2):
                matched += 1
                used.add(tok2)
                break
    # Concatenación de pares
    for tok2 in t2:
        if tok2 in used or len(tok2) < 8:
            continue
        for i in range(len(t1_list)):
            for j in range(len(t1_list)):
                if i == j:
                    continue
                if t1_list[i] + t1_list[j] == tok2:
                    matched += 1
                    used.add(tok2)
                    break
            if tok2 in used:
                break
    return matched


# Cámaras consideradas equivalentes para el filtrado
CAMARA_SENADO = ("Cámara de Senadores", "Senado")
CAMARA_DIPUTADOS = ("Cámara de Diputados", "Diputados")


def build_bd_index(conn: sqlite3.Connection) -> list:
    """
    Construye índice (id, nombre, nombre_normalizado, camara, tokens_counter)
    desde tabla legisladores. Llamar una vez por proceso, reutilizar.
    """
    return [
        (row[0], row[1], row[2], row[3], tokens_relevantes(row[2]))
        for row in conn.execute(
            "SELECT id, nombre, nombre_normalizado, camara FROM legisladores"
        ).fetchall()
    ]


def encontrar_legislador_id(
    nombre_normalizado_sil: str,
    camara_sil: str,
    bd_idx: list,
) -> int | None:
    """
    Encuentra `legisladores.id` que matchee con un nombre del SIL.

    bd_idx: salida de build_bd_index(conn)
    camara_sil: 'Senado' o 'Diputados'

    Estrategia (en orden de confianza):
      1. Multiset exacto (mismos tokens, ignora orden)
      2. Containment ≥85% con ≥3 tokens fuzzy-comunes
      3. Fallback: BD muy abreviado (2 tokens, ambos en SIL)
    """
    sil_tok = tokens_relevantes(nombre_normalizado_sil)
    if sum(sil_tok.values()) < 2:
        return None

    cam_filter = CAMARA_SENADO if camara_sil == "Senado" else CAMARA_DIPUTADOS
    candidatos = [c for c in bd_idx if c[3] in cam_filter]

    # 1. Multiset exacto
    for cid, _, _, _, tok in candidatos:
        if tok == sil_tok:
            return cid

    # 2. Containment + fuzzy
    mejor, mejor_score = None, 0.0
    for cid, _, _, _, tok in candidatos:
        comunes = _tokens_match_fuzzy(sil_tok, tok)
        if comunes < 3:
            continue
        small = min(sum(sil_tok.values()), sum(tok.values()))
        contain = comunes / small if small else 0
        if contain >= 0.85 and comunes * contain > mejor_score:
            mejor, mejor_score = cid, comunes * contain
    if mejor:
        return mejor

    # 3. Fallback BD-abreviado
    for cid, _, _, _, tok in candidatos:
        if sum(tok.values()) == 2 and all(t in sil_tok for t in tok):
            return cid
    return None
