"""
Marca docs SEN_* como duplicados cuando hay un PERM_* con el mismo
instrumento (mismo firmante + título técnico equivalente).

Causa del bug: el scraper Senado oficial captura iniciativas que se
publican en Gaceta del Senado con fecha (4-28 / 4-29 / etc.). El
scraper de Permanente captura los mismos instrumentos cuando se
turnan formalmente en sesión de Permanente (4-29, 6-may, etc.).

Resultado: el mismo instrumento aparece 2 veces:
  · SEN_xxxxxxx con fecha 4-28 / 4-29 (Cámara de Senadores)
  · PERM_NNNNNN con fecha 5-06       (Comisión Permanente, fecha real)

Esto hace que en igualdad_genero (por ej.) Paloma Sánchez aparezca
con fecha 4-29 cuando la iniciativa realmente fue presentada el 6-may.

Approach:
  1. Por cada PERM_*, extraer apellido del primer firmante del título
     descriptivo ("Del senador X y de las senadoras Y...").
  2. Buscar SEN_* con MISMO apellido + título normalizado SIMILAR
     (jaccard tokens ≥ 0.5).
  3. Marcar SEN_ con `es_duplicado_cross_camara = 1` y `comision = ''`
     (esto los hace invisibles en queries que filtran).

Idempotente. Aplicar después de scrape Permanente.
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


def core_titulo(s):
    """Quitar prefijos genéricos del título."""
    n = norm(s)
    for p in [
        r"^de la sen\.?\s+\w+\s+\w+\s*,?",
        r"^de la sen\.?\s+\w+\s+\w+\s+\w+\s*,?",
        r"^del sen\.?\s+\w+\s+\w+\s*,?",
        r"^del sen\.?\s+\w+\s+\w+\s+\w+\s*,?",
        r"^de la dip\.?\s+\w+\s+\w+\s*,?",
        r"^del dip\.?\s+\w+\s+\w+\s*,?",
        r"^del senador\s+\w+\s+\w+\s+\w+\s*y?",
        r"^de la senadora\s+\w+\s+\w+\s+\w+\s*y?",
        r"^de las senadoras\s+",
        r"^de los senadores\s+",
        r"^de las y los senadores\s+",
        r"^proyecto\s+de\s+decreto\s+",
        r"^iniciativa\s+(?:con\s+proyecto\s+de\s+decreto\s+)?",
        r"^con\s+proyecto\s+de\s+decreto\s+",
        r"^con punto de acuerdo\s+",
        r"^proposicion con punto de acuerdo\s+",
        r"del grupo parlamentario\s+\w+\s+\w+\s+\w*",
    ]:
        n = re.sub(p, "", n).strip()
    n = re.sub(r"(\d+)\s*[o°º]\.?", r"\1", n)
    n = re.sub(r"\s+", " ", n).strip(" .,;:")
    return n


def tokens_titulo(s):
    n = core_titulo(s)
    return set(t for t in n.split() if len(t) >= 5)


def extraer_apellidos(presentador_o_titulo):
    """Devuelve set de apellidos largos (>= 4 chars) del primer firmante."""
    n = norm(presentador_o_titulo)
    # Quitar prefijos
    n = re.sub(r"^(?:de la|del|de los|de las)\s+(?:sen\.?|dip\.?|senador(?:a|es|as)?|diputad[oa]s?)\s+", "", n)
    n = re.sub(r"^sen\.?\s+|^dip\.?\s+", "", n)
    # Tomar primeros 4 tokens
    toks = [t for t in n.split() if len(t) >= 4][:5]
    return set(toks)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))

    # Asegurar columna
    cols = {r[1] for r in conn.execute("PRAGMA table_info(sil_documentos)").fetchall()}
    if "es_duplicado_cross_camara" not in cols:
        conn.execute("ALTER TABLE sil_documentos ADD COLUMN es_duplicado_cross_camara INTEGER DEFAULT 0")
        conn.commit()

    # Pre-cargar todos los PERM_*
    perm_docs = []
    for r in conn.execute("""
        SELECT id, titulo, fecha_presentacion FROM sil_documentos
        WHERE seguimiento_id LIKE 'PERM_%'
          AND tipo_grupo IN ('Iniciativa', 'Proposición con Punto de Acuerdo')
    """).fetchall():
        perm_docs.append({
            "id": r[0],
            "titulo": r[1] or "",
            "fecha": r[2] or "",
            "apellidos": extraer_apellidos(r[1] or ""),
            "tokens": tokens_titulo(r[1] or ""),
        })
    logger.info(f"PERM_* iniciativas/proposiciones: {len(perm_docs)}")

    # Para cada SEN_*, buscar duplicado en PERM_
    sen_rows = conn.execute("""
        SELECT id, seguimiento_id, titulo, fecha_presentacion, presentador
        FROM sil_documentos
        WHERE seguimiento_id LIKE 'SEN_%'
          AND tipo_grupo IN ('Iniciativa', 'Proposición con Punto de Acuerdo')
          AND COALESCE(es_duplicado_cross_camara, 0) = 0
    """).fetchall()
    logger.info(f"SEN_* candidatos: {len(sen_rows)}")

    marcados = 0
    for sid, seg_id, titulo, fecha, presentador in sen_rows:
        toks_sen = tokens_titulo(titulo or "")
        if len(toks_sen) < 4:
            continue
        apellidos_sen = extraer_apellidos(presentador or "")
        if not apellidos_sen:
            continue

        # Buscar PERM_ con (a) ≥1 apellido común y (b) jaccard tokens ≥ 0.5
        for p in perm_docs:
            if not (apellidos_sen & p["apellidos"]):
                continue
            if not p["tokens"]:
                continue
            inter = len(toks_sen & p["tokens"])
            union = len(toks_sen | p["tokens"])
            jacc = inter / union if union else 0
            if jacc >= 0.5:
                # Marcar SEN_ como duplicado
                if not args.dry_run:
                    conn.execute(
                        "UPDATE sil_documentos SET es_duplicado_cross_camara = 1 WHERE id = ?",
                        (sid,),
                    )
                marcados += 1
                break

    if not args.dry_run:
        conn.commit()
    logger.info(f"SEN_ marcados como duplicado de PERM_: {marcados}/{len(sen_rows)}")

    if args.dry_run:
        logger.info("*** DRY RUN ***")


if __name__ == "__main__":
    main()
