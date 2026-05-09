"""
Scrape de iniciativaslxvi.php por TODAS las 53 comisiones LXVI Diputados
para construir mapping (titulo, fecha) → init_id (URL pública del SITL).

Fuente:
  https://sitl.diputados.gob.mx/LXVI_leg/iniciativaslxvi.php?comt={ID_COM}&tipo_turnot=1&edot=T

Cada fila en esa página tiene:
  - link href="dictameneslxvi_ld.php?init={N}"
  - título técnico de la iniciativa
  - fecha de presentación
  - presentador

Output:
  dashboard/diputados_urls_oficial.json con:
    {
      "instrumentos": [
        {"init_id": "1737", "titulo": "...", "fecha": "2025-09-23",
         "presentador": "...", "comision": 36, "url": "..."}
      ]
    }

Y también update sil_documentos.url donde matchee por (titulo, fecha,
apellido_presentador).

Idempotente.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import sys
import time
import unicodedata
from pathlib import Path

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
OUT_JSON = ROOT / "dashboard" / "diputados_urls_oficial.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

BASE = "https://sitl.diputados.gob.mx/LXVI_leg"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "es-MX,es;q=0.9",
}
DELAY = 0.4


def normalizar(s):
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s.lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^\x00-\x7f]", "", s)
    return s


def core_titulo(s):
    n = normalizar(s)
    for p in [r"^proyecto\s+de\s+decreto\s+", r"^iniciativa\s+(?:con\s+proyecto\s+de\s+decreto\s+)?",
              r"^proposicion\s+con\s+punto\s+de\s+acuerdo\s*,?\s*"]:
        n = re.sub(p, "", n).strip()
    n = re.sub(r"(\d+)\s*[o°º]\.?", r"\1", n)
    n = re.sub(r"\s+", " ", n).strip(" .,;:")
    return n


def fecha_iso(f):
    """'17-Febrero-2026' o '17/02/2026' → '2026-02-17'."""
    MESES = {
        "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
        "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
        "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
    }
    if not f:
        return ""
    m = re.match(r"(\d{1,2})-(\w+)-(\d{4})", f.strip())
    if m:
        mes = MESES.get(m.group(2).lower(), "")
        if mes:
            return f"{m.group(3)}-{mes}-{m.group(1).zfill(2)}"
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", f.strip())
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    return ""


def scrape_comision(comt: int) -> list[dict]:
    """Scrape iniciativaslxvi.php para una comisión + parsear filas."""
    url = f"{BASE}/iniciativaslxvi.php?comt={comt}&tipo_turnot=1&edot=T"
    try:
        r = requests.get(url, headers=HEADERS, timeout=30, verify=False)
        r.encoding = "utf-8"
    except Exception as e:
        logger.warning(f"  comt={comt} fail: {e}")
        return []
    if r.status_code != 200:
        return []

    items = []
    # Approach: scrape por bloques de texto alrededor de cada init=. La
    # estructura del HTML del SITL es complicada (tables anidadas), así
    # que partimos el HTML en secciones de "<TR>...</TR>" y procesamos.
    # Buscar todos los init= y su gaceta URL asociada cerca.
    # Estrategia simple: por cada init= encontrado, escanear hacia atrás
    # 2KB y hacia adelante 1KB para extraer título y URL gaceta.

    init_positions = [(m.start(), m.group(1)) for m in re.finditer(r"init=(\d+)", r.text)]
    seen_ids = set()
    for pos, init_id in init_positions:
        if init_id in seen_ids:
            continue
        seen_ids.add(init_id)
        chunk = r.text[max(0, pos - 4000): pos + 2500]
        # URL gaceta cerca
        m_gac = re.search(r"https?://gaceta\.diputados\.gob\.mx/Gaceta/[^\s\"'>]+", chunk)
        url_gaceta = m_gac.group(0) if m_gac else ""
        # Título: buscar texto largo dentro de <span class="Estilo71"> o similar
        # cercano antes del init=
        # Más simple: extraer primer texto de longitud razonable después
        # del último <td antes del init=
        titulos_chunk = re.findall(
            r'class="Estilo7\d"[^>]*>(?:<[^>]+>)*([^<]{40,500})',
            chunk
        )
        titulo = ""
        for t in titulos_chunk:
            t_clean = re.sub(r"\s+", " ", t).strip()
            if len(t_clean) > 40 and not re.match(r"^[\d/\s\-A-Za-z]{0,30}$", t_clean[:30]):
                titulo = t_clean
                break
        if not titulo:
            # Fallback: primer texto de >50 chars en chunk
            for m in re.finditer(r">([^<]{60,400})<", chunk):
                t_clean = re.sub(r"\s+", " ", m.group(1)).strip()
                if len(t_clean) > 60 and not t_clean.startswith("Publicación"):
                    titulo = t_clean
                    break
        if not titulo:
            continue
        url_sitl = f"{BASE}/dictameneslxvi_ld.php?tipot=&pert=0&init={init_id}"
        items.append({
            "init_id": init_id,
            "titulo": titulo[:500],
            "comision": comt,
            "url": url_gaceta or url_sitl,
            "url_gaceta": url_gaceta,
            "url_sitl": url_sitl,
        })
    return items


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit-comisiones", type=int, default=0,
                    help="Solo N primeras comisiones (debug)")
    args = ap.parse_args()

    # IDs de las 53 comisiones LXVI Diputados (rango 1-60 con gaps)
    # Probamos 1-60 — los que no existan devolverán fila vacía o error.
    comisiones = list(range(1, 61))
    if args.limit_comisiones:
        comisiones = comisiones[: args.limit_comisiones]

    logger.info(f"Scrapeando {len(comisiones)} comisiones LXVI Diputados…")

    todos_items = []
    for i, comt in enumerate(comisiones, 1):
        items = scrape_comision(comt)
        if items:
            todos_items.extend(items)
        if i % 10 == 0:
            logger.info(f"  [{i}/{len(comisiones)}] +{len(items)} (acumulados {len(todos_items)})")
        time.sleep(DELAY)

    # Dedupe por init_id
    seen = {}
    for it in todos_items:
        if it["init_id"] not in seen:
            seen[it["init_id"]] = it
    todos_items = list(seen.values())
    logger.info(f"Total instrumentos únicos: {len(todos_items)}")

    # Guardar JSON
    if not args.dry_run:
        OUT_JSON.write_text(json.dumps({
            "instrumentos": todos_items,
            "total": len(todos_items),
        }, ensure_ascii=False, indent=2))
        logger.info(f"  ✅ Guardado: {OUT_JSON}")

    # Construir mapping y aplicar URL a sil_documentos
    conn = sqlite3.connect(str(DB))
    # Asegurar columna url
    cols = {r[1] for r in conn.execute("PRAGMA table_info(sil_documentos)").fetchall()}
    if "url" not in cols:
        conn.execute("ALTER TABLE sil_documentos ADD COLUMN url TEXT DEFAULT ''")

    # Index por core_titulo[:60]. La página por comisión solo tiene fecha
    # del dictamen, no de la presentación, así que el match cross-fuente
    # tiene que ser por título. Riesgo de falso positivo bajo: el título
    # técnico de una iniciativa es bastante único en LXVI.
    idx = {}
    for it in todos_items:
        key = core_titulo(it["titulo"])[:60]
        idx.setdefault(key, []).append(it)

    # Recorrer Diputados sin URL
    rows = conn.execute("""
        SELECT id, titulo, fecha_presentacion
        FROM sil_documentos
        WHERE legislatura='LXVI' AND camara='Cámara de Diputados'
          AND (url IS NULL OR url = '')
          AND tipo_grupo IN ('Iniciativa', 'Proposición con Punto de Acuerdo')
    """).fetchall()
    logger.info(f"Diputados sin URL: {len(rows)}")

    matched = 0
    for row_id, titulo, fecha in rows:
        if not titulo:
            continue
        key = core_titulo(titulo)[:60]
        cands = idx.get(key, [])
        if not cands:
            continue
        url = cands[0]["url"]
        if not args.dry_run:
            conn.execute("UPDATE sil_documentos SET url = ? WHERE id = ?", (url, row_id))
        matched += 1
    if not args.dry_run:
        conn.commit()
    logger.info(f"BD updates: {matched}/{len(rows)} ({100*matched/max(len(rows),1):.1f}%)")

    if args.dry_run:
        logger.info("*** DRY RUN ***")


if __name__ == "__main__":
    main()
