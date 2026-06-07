"""Backfill histórico de Animal Político vía sus sitemaps por sección.

Usa los sub-sitemaps oficiales de grupoanimal.mx que listan URLs +
lastmod desde 2011. Filtra al rango sep 2024 → feb 2026 (lo que nos
falta) y guarda en tabla `articulos` con fuente='animal_politico'.

Read-only sobre la web (sin login, sin credenciales). Solo INSERT
a articulos local (dedup via UNIQUE hash).

Ejecutar con NLP_MAX_POR_RUN si quieres clasificar después:
    python3 scripts/backfill_animalpolitico.py
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# Blindaje Haiku: no debe activarse
os.environ.pop("ANTHROPIC_API_KEY", None)
os.environ.pop("FIAT_CLAUDE_API_KEY", None)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backfill_animalpolitico")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

# Secciones con más relevancia legislativa
# Para limitar tiempo, arrancar con politica solo (~3500 URLs en rango,
# ~90 min). Después se pueden agregar más con SECCIONES_EXTRA.
SECCIONES = ["politica"]
# SECCIONES_EXTRA = ["seguridad", "salud", "estados", "internacional"]

# Rango target — no duplicar lo reciente (la pipeline ya jala feb 2026 →)
FECHA_DESDE = "2024-09-01"
FECHA_HASTA = "2026-02-09"

# Rate limit
SLEEP_ENTRE_REQUESTS = 1.0
FUENTE = "animal_politico"


def fetch(url, timeout=20, retries=2):
    """Fetch con reintentos. Sitemaps son grandes (2MB) → timeout largo si la URL contiene 'sitemap'."""
    if "sitemap" in url:
        timeout = max(timeout, 45)
    for intento in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200 and len(r.text) > 100:
                return r.text
            return None
        except Exception as e:
            if intento < retries:
                time.sleep(2)
                continue
            log.debug(f"  fetch err {url[:80]}: {e}")
    return None


def parse_sitemap(xml):
    """Devuelve lista de (url, lastmod_str) del sitemap."""
    out = []
    # Buscar pares <url><loc>...<lastmod>...</url>
    pattern = re.compile(
        r"<url>\s*<loc>([^<]+)</loc>\s*(?:<lastmod>([^<]+)</lastmod>)?",
        re.DOTALL,
    )
    for m in pattern.finditer(xml):
        url = m.group(1).strip()
        lastmod = (m.group(2) or "").strip()
        out.append((url, lastmod))
    return out


def extraer_meta(html):
    """Extrae titulo, resumen, fecha_real del HTML de la nota."""
    soup = BeautifulSoup(html, "html.parser")
    titulo = ""
    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        titulo = og_title["content"].strip()
    if not titulo:
        t = soup.find("title")
        titulo = t.get_text(strip=True) if t else ""
    titulo = titulo[:500]

    resumen = ""
    og_desc = soup.find("meta", property="og:description")
    if og_desc and og_desc.get("content"):
        resumen = og_desc["content"].strip()
    if not resumen:
        m = soup.find("meta", attrs={"name": "description"})
        if m and m.get("content"):
            resumen = m["content"].strip()
    resumen = resumen[:1000] or titulo

    fecha_real = ""
    pub = soup.find("meta", property="article:published_time")
    if pub and pub.get("content"):
        fecha_real = pub["content"][:19]

    return titulo, resumen, fecha_real


def main():
    conn = get_connection()
    # Asegurar columna clasificado (por el fix del clasificador)
    try:
        conn.execute("ALTER TABLE articulos ADD COLUMN clasificado INTEGER DEFAULT 0")
        conn.commit()
    except Exception:
        pass

    log.info("Descargando sitemaps por sección...")
    todos = []
    for sec in SECCIONES:
        url = f"https://www.grupoanimal.mx/sitemap-{sec}.xml"
        xml = fetch(url)
        if not xml:
            log.warning(f"  sitemap {sec}: fallo")
            continue
        pares = parse_sitemap(xml)
        # Filtrar por fecha
        en_rango = [
            (u, lm) for u, lm in pares
            if lm and FECHA_DESDE <= lm[:10] <= FECHA_HASTA
        ]
        log.info(f"  sitemap {sec}: {len(pares)} total, {len(en_rango)} en rango {FECHA_DESDE}→{FECHA_HASTA}")
        todos.extend(en_rango)
        time.sleep(1.0)

    # Dedup
    todos = list({u: lm for u, lm in todos}.items())
    log.info(f"Total URLs únicas a fetchear: {len(todos)}")

    # URLs ya guardadas (evitar refetch)
    existentes = {
        r[0] for r in conn.execute(
            "SELECT url FROM articulos WHERE fuente=?", (FUENTE,)
        ).fetchall()
    }
    log.info(f"Ya en BD con fuente={FUENTE}: {len(existentes)}")
    pendientes = [(u, lm) for u, lm in todos if u not in existentes]
    log.info(f"Pendientes a fetchear: {len(pendientes)}")

    # Procesar
    n_ok = 0
    n_dup = 0
    n_err = 0
    inicio = time.time()
    for i, (url, lastmod) in enumerate(pendientes):
        if (i + 1) % 100 == 0:
            elapsed = time.time() - inicio
            log.info(f"  {i+1}/{len(pendientes)}  ({n_ok} ok, {n_dup} dup, {n_err} err)  "
                     f"{elapsed:.0f}s")

        html = fetch(url)
        if not html:
            n_err += 1
            time.sleep(SLEEP_ENTRE_REQUESTS)
            continue

        try:
            titulo, resumen, fecha_real = extraer_meta(html)
            if not titulo or len(titulo) < 10:
                n_err += 1
                time.sleep(SLEEP_ENTRE_REQUESTS)
                continue
            # Preferir fecha real del meta; si no, lastmod
            fecha = fecha_real or lastmod[:19]
            if not fecha:
                n_err += 1
                time.sleep(SLEEP_ENTRE_REQUESTS)
                continue

            h = hashlib.md5((FUENTE + titulo + url).encode()).hexdigest()

            try:
                conn.execute("""
                    INSERT INTO articulos
                        (hash, fuente, titulo, fecha, resumen, url, categorias,
                         peso_fuente, fecha_scraping, autor)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (h, FUENTE, titulo, fecha, resumen, url, "",
                      1.0, datetime.now().isoformat(), ""))
                conn.commit()
                n_ok += 1
            except sqlite3.IntegrityError:
                n_dup += 1
        except Exception as e:
            log.debug(f"  err procesando {url[:80]}: {e}")
            n_err += 1

        time.sleep(SLEEP_ENTRE_REQUESTS)

    log.info(f"BACKFILL TERMINADO: {n_ok} insertados, {n_dup} duplicados, "
             f"{n_err} errores ({time.time()-inicio:.0f}s)")


if __name__ == "__main__":
    main()
