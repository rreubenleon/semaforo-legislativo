"""
Marca docs SEN_* como duplicados de docs PERM_* cuando son el mismo
instrumento legislativo presentado primero en Gaceta del Senado y
luego turnado en sesión de Comisión Permanente.

Causa del bug:
  - SEN_xxxxxx → fecha 4-28/4-29 (fecha de Gaceta del Senado)
  - PERM_NNNNNN → fecha 5-06 (fecha real de sesión)
  → en el dashboard aparece 2 veces, una con fecha incorrecta

Matcher correcto (sugerido por user):
  Match cuando coinciden los DISTINTIVOS del instrumento:
    1. APELLIDO del primer firmante (≥1 común)
    2. ARTÍCULO específico (ej "26 BIS", "61 Bis", "8o.")
    3. LEY específica (ej "Ley General de Acceso", "Código Civil Federal")
    4. MATERIA cuando aplica (ej "en materia de feminicidio")

  Si comparten al menos: 1 apellido + (1 artículo Y 1 ley) → match.
  O: 1 apellido + 1 artículo + 1 materia.

Acción: marcar SEN_ con `es_duplicado_cross_camara = 1`. La query
de detalle por categoría (main.py) puede filtrar `WHERE
es_duplicado_cross_camara = 0` para mostrar solo la versión PERM_.

Idempotente.
"""
from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def norm(s):
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s.lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^\x00-\x7f]", "", s)
    return s


def extraer_articulos(s):
    """
    Extrae números de artículo mencionados en el título.
    Patrones reales:
      'artículo 26 BIS', 'artículo 4°', 'artículo 4o.', 'artículo 8o.'
      'artículos 132 Bis y 170 Ter', 'art. 19', 'fracción V al artículo 61'
      'reforma los artículos 12 y 325'
    Devuelve set con normalizaciones tipo {"26bis", "4", "8", "12", "325"}.
    """
    if not s:
        return set()
    n = norm(s)
    arts = set()
    # Pattern: "articulo NN[ ]?(BIS|TER|QUATER|etc)?[ ]?(o\.?)?"
    for m in re.finditer(
        r"art(?:[ií]?culo|s\.?)?\s*(\d{1,4})\s*"
        r"(bis|ter|quater|quinquies|sexies|septies|octies|nonies|decies|"
        r"undecies|duodecies)?(?:\s*[oº])?",
        n,
    ):
        num = m.group(1)
        suf = m.group(2) or ""
        arts.add(f"{num}{suf}".strip())
    return arts


def extraer_leyes(s):
    """
    Extrae nombres de leyes/códigos del título.
    Patrones reales:
      'Ley General de Acceso de las Mujeres a una Vida Libre de Violencia'
      'Ley General del Sistema Nacional de Seguridad Pública'
      'Código Civil Federal', 'Ley Federal de Protección al Consumidor'
      'Constitución Política de los Estados Unidos Mexicanos'
    Devuelve set con tokens significativos del nombre normalizado.
    """
    if not s:
        return set()
    n = norm(s)
    leyes = set()
    # Match patrones de "Ley X Y Z" o "Código X Y" o "Constitución..."
    patrones = [
        r"ley\s+(?:general|federal|orgánica|reglamentaria|de\s+amparo|de\s+ingresos|del?)\s+(?:de\s+)?([a-z\s,]{8,80})",
        r"c[oó]digo\s+([a-z\s]{6,40})",
        r"constituci[oó]n\s+(?:pol[ií]tica)?",
    ]
    for p in patrones:
        for m in re.finditer(p, n):
            txt = m.group(0) if "constitu" in m.group(0) else (m.group(1) if m.lastindex else m.group(0))
            # Tokens significativos de la ley (≥4 chars)
            for tok in txt.split():
                t = tok.strip(" ,.;:")
                if len(t) >= 4 and t not in {"general", "federal", "para", "sobre", "esta", "esto"}:
                    leyes.add(t)
            # Marcar la "rama" de la ley
            if "constitu" in m.group(0):
                leyes.add("constitucion")
    return leyes


def extraer_materia(s):
    """
    Extrae el contexto 'en materia de X' que muchas veces es lo único
    que distingue dos iniciativas similares (ej feminicidio, biodiversidad,
    desperdicio de alimentos, etc.).
    """
    if not s:
        return set()
    n = norm(s)
    materia = set()
    for m in re.finditer(r"en\s+materia\s+de\s+([a-z\s,]{4,60})", n):
        for tok in m.group(1).split():
            t = tok.strip(" ,.;:")
            if len(t) >= 4 and t not in {"para", "sobre", "estos", "estas"}:
                materia.add(t)
    return materia


def extraer_apellidos_firmante(presentador_o_titulo):
    """Apellidos largos (≥4 chars) del primer firmante."""
    n = norm(presentador_o_titulo)
    # Quitar prefijos formales
    n = re.sub(r"^(?:de la|del|de los|de las)\s+(?:sen\.?|dip\.?|senador(?:a|es|as)?|diputad[oa]s?)\s+", "", n)
    n = re.sub(r"^sen\.?\s+|^dip\.?\s+", "", n)
    n = re.sub(r"^(?:senador(?:a)?|diputad[oa])\s+", "", n)
    # Cortar antes del primer "y de", ", del grupo", "del grupo parlamentario", "(PARTIDO)"
    n = re.split(r"\s+y\s+de\s+|,\s*del\s+grupo|\s+del\s+grupo\s+parlamentario|\s*\(", n, 1)[0]
    # Tomar primeros 6 tokens significativos
    toks = [t for t in n.split() if len(t) >= 4 and t not in {"para", "sobre"}]
    return set(toks[:6])


def _huella_instrumento(titulo, presentador):
    """Devuelve huella canónica del instrumento."""
    return {
        "apellidos": extraer_apellidos_firmante(presentador or titulo or ""),
        "articulos": extraer_articulos(titulo),
        "leyes": extraer_leyes(titulo),
        "materia": extraer_materia(titulo),
    }


def _diff_dias(fa, fb):
    from datetime import datetime as _dt
    try:
        return abs((_dt.fromisoformat(fa[:10]) - _dt.fromisoformat(fb[:10])).days)
    except Exception:
        return 999


def son_duplicados(h_sen, fecha_sen, h_perm, fecha_perm, max_dias=30):
    """
    Devuelve True si las dos huellas son del MISMO instrumento.

    Reglas:
      A. ≥1 apellido común
      B. fechas dentro de ±max_dias días (default 30) — la publicación
         en Gaceta del Senado y la sesión de Permanente que turna el doc
         deben estar cerca en el tiempo. Una iniciativa de octubre 2025
         no se puede 'turnar' en sesión Permanente de mayo 2026.
      C. ≥1 artículo común Y (≥1 ley común O ≥1 materia común)
    """
    if not (h_sen["apellidos"] & h_perm["apellidos"]):
        return False
    if _diff_dias(fecha_sen or "", fecha_perm or "") > max_dias:
        return False
    arts_comunes = h_sen["articulos"] & h_perm["articulos"]
    if not arts_comunes:
        return False  # sin artículo común = riesgo alto de falso positivo
    leyes_comunes = h_sen["leyes"] & h_perm["leyes"]
    materia_comun = h_sen["materia"] & h_perm["materia"]
    return bool(leyes_comunes or materia_comun)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--unmark", action="store_true",
                    help="Antes de procesar, des-marca todos los SEN_ "
                         "(útil para re-correr matcher)")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))

    cols = {r[1] for r in conn.execute("PRAGMA table_info(sil_documentos)").fetchall()}
    if "es_duplicado_cross_camara" not in cols:
        conn.execute("ALTER TABLE sil_documentos ADD COLUMN es_duplicado_cross_camara INTEGER DEFAULT 0")
        conn.commit()

    if args.unmark and not args.dry_run:
        n = conn.execute(
            "UPDATE sil_documentos SET es_duplicado_cross_camara = 0 WHERE seguimiento_id LIKE 'SEN_%'"
        ).rowcount
        conn.commit()
        logger.info(f"Des-marcados: {n} SEN_*")

    # Pre-cargar huellas de PERM_*
    perm_docs = []
    for r in conn.execute("""
        SELECT id, titulo, fecha_presentacion FROM sil_documentos
        WHERE seguimiento_id LIKE 'PERM_%'
          AND tipo_grupo IN ('Iniciativa', 'Proposición con Punto de Acuerdo')
    """).fetchall():
        h = _huella_instrumento(r[1], r[1])  # PERM_ tiene firmante en título
        perm_docs.append({"id": r[0], "titulo": r[1] or "", "fecha": r[2], "h": h})
    logger.info(f"PERM_* iniciativas/proposiciones: {len(perm_docs)}")

    sen_rows = conn.execute("""
        SELECT id, seguimiento_id, titulo, fecha_presentacion, presentador
        FROM sil_documentos
        WHERE seguimiento_id LIKE 'SEN_%'
          AND tipo_grupo IN ('Iniciativa', 'Proposición con Punto de Acuerdo')
          AND COALESCE(es_duplicado_cross_camara, 0) = 0
    """).fetchall()
    logger.info(f"SEN_* candidatos: {len(sen_rows)}")

    marcados = 0
    ejemplos = []
    for sid, seg_id, titulo, fecha, presentador in sen_rows:
        h_sen = _huella_instrumento(titulo, presentador)
        if not h_sen["apellidos"] or not h_sen["articulos"]:
            continue  # No tiene distintivos suficientes
        for p in perm_docs:
            if son_duplicados(h_sen, fecha, p["h"], p["fecha"]):
                if not args.dry_run:
                    conn.execute(
                        "UPDATE sil_documentos SET es_duplicado_cross_camara = 1 WHERE id = ?",
                        (sid,),
                    )
                marcados += 1
                if len(ejemplos) < 6:
                    ejemplos.append({
                        "sen_titulo": titulo[:80],
                        "sen_fecha": fecha,
                        "perm_titulo": p["titulo"][:80],
                        "perm_fecha": p["fecha"],
                        "arts": h_sen["articulos"] & p["h"]["articulos"],
                        "leyes": h_sen["leyes"] & p["h"]["leyes"],
                    })
                break

    if not args.dry_run:
        conn.commit()
    logger.info(f"\nSEN_ marcados como duplicado de PERM_: {marcados}/{len(sen_rows)}")
    print()
    print("Ejemplos:")
    for e in ejemplos:
        print(f"\n  SEN ({e['sen_fecha']}): {e['sen_titulo']}")
        print(f"  PERM ({e['perm_fecha']}): {e['perm_titulo']}")
        print(f"    arts: {e['arts']}, leyes: {e['leyes']}")

    if args.dry_run:
        logger.info("\n*** DRY RUN ***")


if __name__ == "__main__":
    main()
