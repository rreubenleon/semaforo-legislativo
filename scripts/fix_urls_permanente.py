"""
Resuelve URLs faltantes de docs SEN_ presentados durante Comisión
Permanente cruzando con el listado público
https://www.senado.gob.mx/66/gaceta_comision_permanente/.

Bug original: el scraper del Senado no extrajo `enlace_gaceta` para
docs presentados durante receso → 2,288 SEN_* sin URL.

Approach correcto (corrige primer intento):
  El listado Permanente NO usa títulos formales del instrumento; usa
  el texto descriptivo del PROMOVENTE ("Del senador X y de las
  senadoras Y, Z..."). Por eso el match por título técnico falla.

  Match correcto: por (FECHA, PRIMER APELLIDO del primer firmante,
  TIPO Iniciativa/Proposición). Esto sí matchea entre fuentes.

Idempotente.

Uso:
    python scripts/fix_urls_permanente.py
    python scripts/fix_urls_permanente.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import sys
import unicodedata
from pathlib import Path

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
JSON_PATH = ROOT / "dashboard" / "senadores_lxvi_oficial.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

BASE = "https://www.senado.gob.mx"
LISTADO = f"{BASE}/66/gaceta_comision_permanente/"
DOC_BASE = f"{BASE}/66/gaceta_comision_permanente/documento"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"}

MESES = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}


def normalizar(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s.lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^\x00-\x7f]", "", s)
    return s


def parsear_fecha_es(fecha_str: str) -> str:
    if not fecha_str:
        return ""
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", fecha_str.lower())
    if not m:
        return ""
    dia, mes_n, anio = m.group(1), m.group(2), m.group(3)
    mes = MESES.get(mes_n, "")
    if not mes:
        return ""
    return f"{anio}-{mes}-{dia.zfill(2)}"


def extraer_apellido_promovente(texto_listado: str) -> str:
    """
    Del texto del listado de Permanente extrae el apellido del primer
    firmante. Patrones observados:
      "Del senador Alberto Anaya Gutiérrez y de las senadoras..."
        → "Anaya" (segundo token significativo)
      "De la Sen. Paloma Sánchez Ramos, del Grupo Parlamentario..."
        → "Sánchez"
      "De la Dip. Nadia Navarro Acevedo, del Grupo..."
        → "Navarro"
    """
    n = normalizar(texto_listado)
    # Quitar prefijos comunes
    n = re.sub(r"^de\s+(?:la|los|las|el)\s+(?:sen\.?|dip\.?|senador(?:a)?|diputad[oa])s?\s+", "", n)
    n = re.sub(r"^del\s+(?:sen\.?|dip\.?|senador|diputado)\s+", "", n)
    # Tomar primer 4 tokens
    toks = [t for t in n.split() if len(t) >= 4][:6]
    return " ".join(toks)


def scrape_permanente_listado() -> list[dict]:
    """Devuelve [{doc_id, titulo_listado, apellido, tipo}].

    NOTA WAF: el sitio del Senado tiene Imperva. La primera request devuelve
    challenge HTML (~850 bytes). Usamos Session + retry para que la 2da request
    venga con cookie y devuelva HTML real (~200KB).
    """
    import time
    logger.info(f"Descargando: {LISTADO}")
    sess = requests.Session()
    sess.headers.update(HEADERS)
    r = None
    for attempt in range(4):
        r = sess.get(LISTADO, timeout=30)
        if len(r.text) > 10000:
            break
        time.sleep(2 + attempt)
    if not r or len(r.text) < 10000:
        logger.error(f"WAF bloqueó: status {r.status_code if r else '?'} len {len(r.text) if r else 0}")
        return []
    soup = BeautifulSoup(r.text, "html.parser")

    # La fecha actual de la sesión: buscar header "Miércoles X de mayo de 2026"
    text_full = soup.get_text(' ', strip=True)
    m = re.search(
        r"((?:Lunes|Martes|Miércoles|Jueves|Viernes)\s+\d{1,2}\s+de\s+\w+\s+de\s+\d{4})",
        text_full,
    )
    fecha_session_iso = parsear_fecha_es(m.group(1)) if m else ""
    logger.info(f"Fecha de la sesión actual: {fecha_session_iso}")

    docs = []
    seen_ids = set()
    for a in soup.find_all("a", href=re.compile(r"/gaceta_comision_permanente/documento/(\d+)")):
        href = a['href']
        m = re.search(r'/documento/(\d+)', href)
        if not m:
            continue
        doc_id = m.group(1)
        if doc_id in seen_ids:
            continue
        seen_ids.add(doc_id)
        titulo = a.get_text(strip=True)
        if len(titulo) < 5:
            continue
        # Inferir tipo
        tn = normalizar(titulo)
        if "punto de acuerdo" in tn or "proposicion" in tn:
            tipo = "Proposición"
        elif "proyecto de decreto" in tn or "iniciativa" in tn or "decreto" in tn:
            tipo = "Iniciativa"
        else:
            tipo = "otro"
        docs.append({
            "doc_id": doc_id,
            "titulo": titulo[:500],
            "tokens_promovente": extraer_apellido_promovente(titulo),
            "tipo": tipo,
            "fecha_iso": fecha_session_iso,
        })
    return docs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    perm_docs = scrape_permanente_listado()
    logger.info(f"Docs en listado Permanente: {len(perm_docs)}")

    # 1. Update BD: SEN_ sin URL del 6-may (la fecha actual de Permanente)
    fecha_target = perm_docs[0]["fecha_iso"] if perm_docs else ""
    if not fecha_target:
        logger.error("No pude determinar la fecha de la sesión")
        sys.exit(1)

    conn = sqlite3.connect(str(DB))
    rows = conn.execute("""
        SELECT id, titulo, fecha_presentacion, presentador, seguimiento_id, tipo_grupo
        FROM sil_documentos
        WHERE seguimiento_id LIKE 'SEN_%'
          AND (url IS NULL OR url = '')
          AND fecha_presentacion = ?
    """, (fecha_target,)).fetchall()
    logger.info(f"SEN_ sin URL en fecha {fecha_target}: {len(rows)}")

    matched = 0
    perm_used = set()  # IDs PERM_ ya asignados — match 1:1
    for row_id, titulo, fecha, presentador, seg_id, tg in rows:
        m_first = re.match(r"^(?:Sen|Dip)\.\s+([^()]+?)\s*\(", presentador or "")
        if not m_first:
            continue
        nombre_pres = normalizar(m_first.group(1))
        toks_sen = set(t for t in nombre_pres.split() if len(t) >= 4)

        tipo_target = "Iniciativa" if "Iniciativa" in (tg or "") else "Proposición"

        # Match: PERM con mismo tipo, solapamiento de tokens, NO reusado.
        # Además matchear tokens del título técnico para distinguir entre
        # múltiples iniciativas del mismo firmante (ej Anaya tiene 2
        # iniciativas distintas en la sesión).
        toks_titulo_sen = set(
            t for t in normalizar(titulo).split()
            if len(t) >= 5 and t not in {"proyecto", "decreto", "para", "ley", "general", "reforma", "adiciona"}
        )

        best = None
        best_score = 0
        for d in perm_docs:
            if d["doc_id"] in perm_used:
                continue
            if d["tipo"] != tipo_target:
                continue
            toks_perm = set(t for t in d["tokens_promovente"].split() if len(t) >= 4)
            inter_pres = toks_sen & toks_perm
            if len(inter_pres) < 2:
                continue
            # Score: presentador + bonus por título matching
            toks_titulo_perm = set(
                t for t in normalizar(d["titulo"]).split() if len(t) >= 5
            )
            inter_tit = toks_titulo_sen & toks_titulo_perm
            score = len(inter_pres) + len(inter_tit) * 2  # título pesa más
            if score > best_score:
                best_score = score
                best = d

        if best:
            perm_used.add(best["doc_id"])
            url_doc = f"{DOC_BASE}/{best['doc_id']}"
            if not args.dry_run:
                conn.execute("UPDATE sil_documentos SET url = ? WHERE id = ?", (url_doc, row_id))
            matched += 1
            logger.info(f"  ✓ {seg_id[:25]:25} → {url_doc}")
            logger.info(f"     SEN: {titulo[:80]}")
            logger.info(f"     PERM: {best['titulo'][:80]}")
    if not args.dry_run:
        conn.commit()
    logger.info(f"\nBD updates: {matched}/{len(rows)}")

    if args.dry_run:
        logger.info("*** DRY RUN ***")


if __name__ == "__main__":
    main()
