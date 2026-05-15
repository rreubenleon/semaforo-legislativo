"""
Match fuzzy de sil_documentos.SEN_* sin estatus contra
estatus_senado_panel por TÍTULO (no por ID — esos no coinciden).

CONTEXTO:
  4,757 sil_documentos SEN_* (53% de senadores) no tienen estatus porque
  vienen del scrape oficial Senado sin enlace_gaceta — y nuestro match
  previo era por gaceta_doc_id que no existe en ellos.

ESTRATEGIA:
  1. Normalizar título (lowercase, sin acentos, sin puntuación, tokens
     >= 4 chars).
  2. Construir índice invertido por tokens en estatus_senado_panel.
  3. Para cada SEN_* sin estatus, buscar candidatos por intersección
     de tokens. Calcular jaccard + containment.
  4. Si jaccard >= 0.7 O containment >= 0.85 → UPDATE sil_documentos.estatus.
  5. Si múltiples matches con estatus distintos, priorizar:
     Resuelto/Aprobado > Desechado > Pendiente.

PROTECCIÓN ANTI-FALSO-POSITIVO:
  - Threshold conservador (0.7 jaccard / 0.85 containment).
  - Requiere ≥5 tokens compartidos significativos.
  - Logging de matches para auditoría.
  - Dry-run reporta cobertura antes de escribir.

Uso:
  python3 scripts/match_estatus_por_titulo.py --dry-run
  python3 scripts/match_estatus_por_titulo.py
"""
import argparse
import logging
import re
import sqlite3
import sys
import unicodedata
from collections import defaultdict, Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


STOPWORDS = {
    "que", "para", "por", "con", "los", "las", "del", "una", "uno", "este",
    "esta", "esto", "como", "más", "mas", "ley", "decreto", "proyecto",
    "reforma", "adiciona", "deroga", "diversa", "diversas", "disposicion",
    "disposiciones", "articulo", "articulos", "punto", "acuerdo",
    "iniciativa", "proposicion", "propone",
}


def normalizar_titulo(s: str) -> list[str]:
    """Devuelve tokens significativos (≥4 chars, sin stopwords/acentos)."""
    if not s:
        return []
    s = s.lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    tokens = re.findall(r"[a-z]+", s)
    return [t for t in tokens if len(t) >= 4 and t not in STOPWORDS]


PRIORIDAD = {
    "Resuelto / Aprobado": 3,
    "Aprobado": 3,
    "Desechado": 2,
    "Retirada": 2,
    "Pendiente en comisión": 1,
    "Pendiente": 1,
}


def canonicalizar(estatus_raw: str) -> str:
    if not estatus_raw:
        return ""
    r = estatus_raw.lower()
    if "aprobado" in r or "publicad" in r or "diario oficial" in r or "resuelto" in r:
        return "Resuelto / Aprobado"
    if "desechad" in r or "concluido" in r:
        return "Desechado"
    if "retirad" in r:
        return "Retirada"
    return "Pendiente en comisión"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--jaccard-min", type=float, default=0.7)
    ap.add_argument("--containment-min", type=float, default=0.85)
    ap.add_argument("--shared-tokens-min", type=int, default=5)
    args = ap.parse_args()

    from db import get_connection
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # 1. Verificar tabla
    n_panel = conn.execute(
        "SELECT COUNT(*) FROM estatus_senado_panel"
    ).fetchone()[0]
    logger.info(f"estatus_senado_panel: {n_panel} filas")

    # 2. Cargar paneles deduplicados por (titulo_norm). Si un título aparece
    #    en varias comisiones con estatus distintos, priorizar el más fuerte.
    panel_rows = conn.execute(
        "SELECT gaceta_doc_id, estatus, titulo FROM estatus_senado_panel"
    ).fetchall()
    panel_by_tokens: dict[frozenset, dict] = {}
    for r in panel_rows:
        toks = set(normalizar_titulo(r["titulo"] or ""))
        if len(toks) < 5:
            continue
        canon = canonicalizar(r["estatus"])
        key = frozenset(toks)
        prev = panel_by_tokens.get(key)
        if prev is None or PRIORIDAD.get(canon, 0) > PRIORIDAD.get(prev["estatus_canon"], 0):
            panel_by_tokens[key] = {
                "estatus_canon": canon,
                "titulo": r["titulo"],
                "tokens": toks,
            }
    logger.info(f"Paneles únicos por título: {len(panel_by_tokens)}")

    # 3. Índice invertido: token → list of panel keys
    inverted: dict[str, list[frozenset]] = defaultdict(list)
    for key in panel_by_tokens:
        for tok in key:
            inverted[tok].append(key)

    # 4. Cargar SEN_* sin estatus
    sin_estatus = conn.execute("""
        SELECT id, seguimiento_id, titulo
        FROM sil_documentos
        WHERE seguimiento_id LIKE 'SEN_%'
          AND (estatus IS NULL OR estatus = '')
          AND titulo IS NOT NULL AND titulo != ''
    """).fetchall()
    logger.info(f"SEN_* sin estatus: {len(sin_estatus)}")

    # 5. Para cada uno, buscar match
    matches = []  # (sil_id, estatus_canon, score, titulo_match)
    no_match = 0
    multi_match = 0
    for r in sin_estatus:
        toks_doc = set(normalizar_titulo(r["titulo"]))
        if len(toks_doc) < args.shared_tokens_min:
            no_match += 1
            continue

        # Candidatos: paneles que comparten ≥1 token con el doc
        candidatos = set()
        for tok in toks_doc:
            for key in inverted.get(tok, []):
                candidatos.add(key)

        # Evaluar candidatos
        mejor = None
        mejor_score = 0.0
        for key in candidatos:
            toks_panel = panel_by_tokens[key]["tokens"]
            inter = toks_doc & toks_panel
            if len(inter) < args.shared_tokens_min:
                continue
            union = toks_doc | toks_panel
            jacc = len(inter) / len(union)
            small = min(len(toks_doc), len(toks_panel))
            cont = len(inter) / small if small else 0
            score = max(jacc, cont)
            # Aceptar si pasa cualquier umbral
            if jacc >= args.jaccard_min or cont >= args.containment_min:
                if score > mejor_score:
                    mejor_score = score
                    mejor = panel_by_tokens[key]

        if mejor:
            matches.append({
                "sil_id": r["id"],
                "estatus_canon": mejor["estatus_canon"],
                "score": mejor_score,
                "titulo_doc": r["titulo"][:80],
                "titulo_match": mejor["titulo"][:80],
            })
        else:
            no_match += 1

    logger.info(f"Matches encontrados: {len(matches)}")
    logger.info(f"Sin match: {no_match}")

    # Distribución de estatus matcheados
    distro = Counter(m["estatus_canon"] for m in matches)
    logger.info("Distribución de estatus a aplicar:")
    for est, n in distro.most_common():
        logger.info(f"  {n:5d} × {est}")

    # Muestra de matches (auditoría)
    logger.info("\nMuestra de 8 matches:")
    for m in matches[:8]:
        logger.info(f"  score={m['score']:.2f} estatus={m['estatus_canon']}")
        logger.info(f"    doc  : {m['titulo_doc']}")
        logger.info(f"    panel: {m['titulo_match']}")

    if args.dry_run:
        logger.info(f"\n[DRY-RUN] Aplicaría {len(matches)} updates a sil_documentos.estatus")
        return 0

    # 6. Aplicar updates
    for m in matches:
        conn.execute(
            "UPDATE sil_documentos SET estatus = ? WHERE id = ?",
            (m["estatus_canon"], m["sil_id"]),
        )
    conn.commit()
    logger.info(f"OK: {len(matches)} sil_documentos.estatus actualizados")

    # 7. Reporte final
    n_sin_post = conn.execute("""
        SELECT COUNT(*) FROM sil_documentos
        WHERE seguimiento_id LIKE 'SEN_%' AND (estatus IS NULL OR estatus = '')
    """).fetchone()[0]
    cobertura = (len(sin_estatus) - n_sin_post) / max(len(sin_estatus), 1) * 100
    logger.info(f"Sin estatus DESPUÉS: {n_sin_post} (cobertura recuperada: {cobertura:.1f}%)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
