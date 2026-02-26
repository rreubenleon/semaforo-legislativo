"""
Scraper HTML directo para medios con RSS roto/inaccesible.
Extrae titulares de las secciones de política/nacional
parseando el HTML de cada sitio.

Medios cubiertos:
  - Animal Político (Next.js __NEXT_DATA__)
  - El Universal (h2/h3 en /nacion/)
  - El Economista (h2/h3 en /politica/)
  - Proceso (h2/h3 en /nacional/)
  - Excélsior (h2/h3 en /nacional)
  - La Razón (h2/h3 en /mexico/)
  - El Heraldo (h2/h3 en /nacional/)
  - Crónica (a[href] en /nacional/)
  - El Sol de México (a[href] en /mexico/)
  - Bloomberg Línea (h2/h3 en /latinoamerica/mexico/)
  - El Financiero (h2/h3 en /nacional/)
  - Político.mx (h3 en /)
  - La Política Online (h2 en /mexico/)
"""

import re
import json
import logging
import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import MEDIOS, DATABASE

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

# Configuración de scraping HTML por medio
SCRAPE_CONFIGS = {
    "animal_politico": {
        "url": "https://animalpolitico.com",
        "metodo": "nextjs",  # Extraer de __NEXT_DATA__
        "base_url": "https://animalpolitico.com/",
    },
    "el_universal": {
        "url": "https://www.eluniversal.com.mx/nacion/",
        "metodo": "headings",
        "tags": ["h2", "h3"],
        "base_url": "https://www.eluniversal.com.mx",
    },
    "el_economista": {
        "url": "https://www.eleconomista.com.mx/politica/",
        "metodo": "headings",
        "tags": ["h2", "h3"],
        "base_url": "https://www.eleconomista.com.mx",
        "filtro_href": "/politica/",
    },
    "proceso": {
        "url": "https://www.proceso.com.mx/nacional/",
        "metodo": "headings",
        "tags": ["h2", "h3"],
        "base_url": "https://www.proceso.com.mx",
    },
    "excelsior": {
        "url": "https://www.excelsior.com.mx/nacional",
        "metodo": "headings",
        "tags": ["h2", "h3"],
        "base_url": "https://www.excelsior.com.mx",
    },
    "la_razon": {
        "url": "https://www.razon.com.mx/mexico/",
        "metodo": "headings",
        "tags": ["h2", "h3"],
        "base_url": "https://www.razon.com.mx",
    },
    "el_heraldo": {
        "url": "https://heraldodemexico.com.mx/nacional/",
        "metodo": "headings",
        "tags": ["h2", "h3"],
        "base_url": "https://heraldodemexico.com.mx",
    },
    "cronica": {
        "url": "https://www.cronica.com.mx/nacional/",
        "metodo": "links",
        "patron_href": r"/nacional/\d{4}/\d{2}/\d{2}/",
        "base_url": "https://www.cronica.com.mx",
    },
    "sol_de_mexico": {
        "url": "https://www.elsoldemexico.com.mx/mexico/",
        "metodo": "links",
        "patron_href": r"/elsoldemexico/mexico/",
        "base_url": "https://www.elsoldemexico.com.mx",
    },
    "bloomberg_linea": {
        "url": "https://www.bloomberglinea.com/latinoamerica/mexico/",
        "metodo": "headings",
        "tags": ["h2", "h3"],
        "base_url": "https://www.bloomberglinea.com",
    },
    "el_financiero": {
        "url": "https://www.elfinanciero.com.mx/nacional/",
        "metodo": "headings",
        "tags": ["h2", "h3"],
        "base_url": "https://www.elfinanciero.com.mx",
    },
    "politico_mx": {
        "url": "https://politico.mx/",
        "metodo": "headings",
        "tags": ["h3"],
        "base_url": "https://politico.mx",
    },
    "la_politica_online": {
        "url": "https://www.lapoliticaonline.com/mexico/",
        "metodo": "headings",
        "tags": ["h2"],
        "base_url": "https://www.lapoliticaonline.com",
        "filtro_href": "/mexico/",
    },
    "el_pais": {
        "url": "https://elpais.com/mexico/",
        "metodo": "headings",
        "tags": ["h2", "h3"],
        "base_url": "https://elpais.com",
        "filtro_href": "/mexico/",
    },
}


def generar_hash(titulo, fuente):
    """Hash para deduplicación."""
    raw = f"{titulo.lower().strip()}|{fuente}"
    return hashlib.md5(raw.encode()).hexdigest()


def fetch_page(url):
    """Descarga una página con manejo de errores."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20, verify=False)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
        return resp.text
    except requests.RequestException as e:
        logger.warning(f"Error descargando {url}: {e}")
        return None


def scrape_nextjs(config):
    """
    Extrae artículos de sitios Next.js via __NEXT_DATA__.
    Usado para Animal Político.
    """
    html = fetch_page(config["url"])
    if not html:
        return []

    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
    )
    if not match:
        logger.warning(f"No se encontró __NEXT_DATA__ en {config['url']}")
        return []

    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        logger.warning("Error parseando __NEXT_DATA__")
        return []

    props = data.get("props", {}).get("pageProps", {})
    articulos = []

    # Recolectar de todas las listas de posts
    listas_interes = ["allHome", "allHablemosDe", "allNotasDePlumaje", "allElSabueso"]
    for key in listas_interes:
        items = props.get(key, [])
        for item in items:
            titulo = item.get("title", "")
            if not titulo or len(titulo) < 15:
                continue

            slug = item.get("slug", "")
            fecha = item.get("date", "")
            excerpt = ""
            if isinstance(item.get("excerpt"), str):
                excerpt = BeautifulSoup(item["excerpt"], "html.parser").get_text(strip=True)

            url = f"{config['base_url']}{slug}" if slug else ""

            articulos.append({
                "titulo": titulo[:500],
                "url": url,
                "fecha": fecha[:19] if fecha else datetime.now().strftime("%Y-%m-%d"),
                "resumen": excerpt[:1000] or titulo,
            })

    return articulos


def scrape_headings(config):
    """
    Extrae artículos buscando h2/h3 con enlaces.
    Usado para El Universal, Proceso, Excélsior, etc.
    """
    html = fetch_page(config["url"])
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    articulos = []
    vistos = set()
    tags = config.get("tags", ["h2", "h3"])
    filtro_href = config.get("filtro_href", "")

    for tag in tags:
        for heading in soup.find_all(tag):
            a = heading.find("a", href=True)
            if not a:
                continue

            titulo = a.get_text(strip=True)
            href = a["href"]

            if not titulo or len(titulo) < 15 or len(titulo) > 300:
                continue

            if filtro_href and filtro_href not in href:
                continue

            # Deduplicar
            if titulo in vistos:
                continue
            vistos.add(titulo)

            # Construir URL completa
            if href.startswith("http"):
                url = href
            else:
                url = config["base_url"] + href

            articulos.append({
                "titulo": titulo[:500],
                "url": url,
                "fecha": datetime.now().strftime("%Y-%m-%d"),
                "resumen": titulo,
            })

    return articulos


def scrape_links(config):
    """
    Extrae artículos buscando enlaces que coincidan con un patrón.
    Usado para Crónica, Sol de México.
    """
    html = fetch_page(config["url"])
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    articulos = []
    vistos = set()
    patron = re.compile(config.get("patron_href", ""))

    for a in soup.find_all("a", href=True):
        href = a["href"]
        titulo = a.get_text(strip=True)

        if not titulo or len(titulo) < 25 or len(titulo) > 300:
            continue

        if not patron.search(href):
            continue

        if titulo in vistos:
            continue
        vistos.add(titulo)

        if href.startswith("http"):
            url = href
        else:
            url = config["base_url"] + href

        articulos.append({
            "titulo": titulo[:500],
            "url": url,
            "fecha": datetime.now().strftime("%Y-%m-%d"),
            "resumen": titulo,
        })

    return articulos


METODO_MAP = {
    "nextjs": scrape_nextjs,
    "headings": scrape_headings,
    "links": scrape_links,
}


def scrape_medio_html(clave):
    """Scrapea un medio individual via HTML."""
    config = SCRAPE_CONFIGS.get(clave)
    if not config:
        return []

    metodo = config["metodo"]
    scraper_fn = METODO_MAP.get(metodo)
    if not scraper_fn:
        logger.error(f"Método desconocido: {metodo}")
        return []

    nombre = MEDIOS[clave]["nombre"]
    peso = MEDIOS[clave]["peso"]

    logger.info(f"Scrapeando {nombre} via HTML ({config['url']})")
    articulos_raw = scraper_fn(config)

    articulos = []
    for art in articulos_raw:
        articulos.append({
            "hash": generar_hash(art["titulo"], clave),
            "fuente": clave,
            "titulo": art["titulo"],
            "fecha": art["fecha"],
            "resumen": art.get("resumen", art["titulo"]),
            "url": art["url"],
            "categorias": "",
            "peso_fuente": peso,
            "fecha_scraping": datetime.now().isoformat(),
        })

    logger.info(f"  {nombre}: {len(articulos)} artículos via HTML")
    return articulos


def scrape_todos_html():
    """
    Scrapea todos los medios con RSS roto via HTML.
    Inserta en la misma tabla 'articulos' de SQLite.
    """
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS articulos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT UNIQUE,
            fuente TEXT NOT NULL,
            titulo TEXT NOT NULL,
            fecha TEXT NOT NULL,
            resumen TEXT,
            url TEXT,
            categorias TEXT,
            peso_fuente REAL DEFAULT 1.0,
            fecha_scraping TEXT NOT NULL
        )
    """)

    total_nuevos = 0
    resultados = {}

    for clave in SCRAPE_CONFIGS:
        articulos = scrape_medio_html(clave)
        nuevos = 0

        for art in articulos:
            try:
                conn.execute("""
                    INSERT INTO articulos
                        (hash, fuente, titulo, fecha, resumen, url, categorias, peso_fuente, fecha_scraping)
                    VALUES
                        (:hash, :fuente, :titulo, :fecha, :resumen, :url, :categorias, :peso_fuente, :fecha_scraping)
                """, art)
                nuevos += 1
            except sqlite3.IntegrityError:
                pass

        conn.commit()
        total_nuevos += nuevos
        resultados[clave] = {
            "nombre": MEDIOS[clave]["nombre"],
            "obtenidos": len(articulos),
            "nuevos": nuevos,
        }

    conn.close()
    logger.info(f"HTML scraping completo: {total_nuevos} artículos nuevos")
    return resultados


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore")
    logging.basicConfig(level=logging.INFO)
    print("=== Scraper HTML de Medios ===\n")
    resultados = scrape_todos_html()
    total = 0
    for clave, info in resultados.items():
        estado = "OK" if info["obtenidos"] > 0 else "VACIO"
        print(f"  [{estado:5s}] {info['nombre']:25s} {info['obtenidos']:3d} obtenidos, {info['nuevos']:3d} nuevos")
        total += info["nuevos"]
    print(f"\n  Total nuevos: {total}")
