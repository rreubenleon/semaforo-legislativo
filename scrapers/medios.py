"""
Scraper RSS de 14 medios mexicanos + NYT
Extrae: título, fecha, resumen, fuente, URL
Almacena en SQLite para análisis posterior
"""

import logging
import ssl
import sqlite3
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

import feedparser
import requests
from bs4 import BeautifulSoup

# Fix SSL para macOS (certificados no bundled con Python)
if hasattr(ssl, "_create_unverified_context"):
    ssl._create_default_https_context = ssl._create_unverified_context

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import MEDIOS, DATABASE

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


def init_db():
    """Crea la tabla de artículos si no existe."""
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
    conn.commit()
    return conn


def generar_hash(titulo, fuente):
    """Genera hash único para deduplicación."""
    raw = f"{titulo.lower().strip()}|{fuente}"
    return hashlib.md5(raw.encode()).hexdigest()


def parsear_fecha_rss(entry):
    """
    Extrae y normaliza la fecha de un entry RSS.
    Feedparser provee published_parsed o updated_parsed.
    """
    for campo in ["published_parsed", "updated_parsed"]:
        parsed = getattr(entry, campo, None)
        if parsed:
            try:
                return datetime(*parsed[:6]).strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                continue

    # Fallback: intentar parsear el string directamente
    for campo in ["published", "updated"]:
        raw = getattr(entry, campo, None)
        if raw:
            # feedparser normaliza muchos formatos
            return raw[:19]  # Truncar a YYYY-MM-DD HH:MM:SS aprox

    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def limpiar_html(texto):
    """Remueve tags HTML del resumen."""
    if not texto:
        return ""
    soup = BeautifulSoup(texto, "html.parser")
    return soup.get_text(strip=True)[:1000]


def extraer_resumen(entry):
    """Extrae el mejor resumen disponible del entry."""
    # Prioridad: summary > description > content
    if hasattr(entry, "summary") and entry.summary:
        return limpiar_html(entry.summary)

    if hasattr(entry, "description") and entry.description:
        return limpiar_html(entry.description)

    if hasattr(entry, "content") and entry.content:
        for contenido in entry.content:
            if contenido.get("value"):
                return limpiar_html(contenido["value"])

    return ""


def scrape_medio(clave, config_medio):
    """
    Scrapea un medio individual vía RSS.
    Retorna lista de artículos parseados.
    """
    nombre = config_medio["nombre"]
    rss_url = config_medio["rss"]
    peso = config_medio["peso"]

    logger.info(f"Scrapeando {nombre} ({rss_url})")

    try:
        feed = feedparser.parse(rss_url, agent=HEADERS["User-Agent"])
    except Exception as e:
        logger.error(f"Error parseando RSS de {nombre}: {e}")
        return []

    if feed.bozo and not feed.entries:
        logger.warning(f"Feed inválido para {nombre}: {feed.bozo_exception}")
        # Fallback: intentar con requests directo
        try:
            resp = requests.get(rss_url, headers=HEADERS, timeout=20)
            feed = feedparser.parse(resp.content)
        except Exception as e:
            logger.error(f"Fallback fallido para {nombre}: {e}")
            return []

    articulos = []
    for entry in feed.entries:
        titulo = getattr(entry, "title", "").strip()
        if not titulo:
            continue

        articulo = {
            "hash": generar_hash(titulo, clave),
            "fuente": clave,
            "titulo": titulo,
            "fecha": parsear_fecha_rss(entry),
            "resumen": extraer_resumen(entry),
            "url": getattr(entry, "link", ""),
            "categorias": "",  # Se llena por el clasificador NLP
            "peso_fuente": peso,
            "fecha_scraping": datetime.now().isoformat(),
        }
        articulos.append(articulo)

    logger.info(f"  {nombre}: {len(articulos)} artículos obtenidos")
    return articulos


def scrape_todos_medios():
    """
    Scrapea todos los 14 medios configurados.
    Retorna total de artículos nuevos insertados.
    """
    conn = init_db()
    total_nuevos = 0
    total_existentes = 0
    resultados = {}

    for clave, config_medio in MEDIOS.items():
        articulos = scrape_medio(clave, config_medio)
        nuevos = 0

        for art in articulos:
            try:
                conn.execute("""
                    INSERT INTO articulos
                        (hash, fuente, titulo, fecha, resumen, url, categorias, peso_fuente, fecha_scraping)
                    VALUES
                        (:hash, :fuente, :titulo, :fecha, :resumen, :url, :categorias, :peso_fuente, :fecha_scraping)
                """, art)
                conn.commit()
                nuevos += 1
            except sqlite3.IntegrityError:
                total_existentes += 1

        total_nuevos += nuevos
        resultados[clave] = {
            "nombre": config_medio["nombre"],
            "obtenidos": len(articulos),
            "nuevos": nuevos,
        }

    conn.close()

    logger.info(f"Scraping completo: {total_nuevos} nuevos, {total_existentes} duplicados")
    return resultados


def obtener_articulos_recientes(dias=7, fuente=None):
    """Recupera artículos recientes de la BD."""
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    if fuente:
        rows = conn.execute(
            "SELECT * FROM articulos WHERE fecha >= ? AND fuente = ? ORDER BY fecha DESC",
            (fecha_limite, fuente),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM articulos WHERE fecha >= ? ORDER BY fecha DESC",
            (fecha_limite,),
        ).fetchall()

    conn.close()
    return [dict(r) for r in rows]


def contar_menciones_por_fecha(keyword, dias=30):
    """
    Cuenta artículos que mencionan un keyword por fecha.
    Retorna dict {fecha: count} para análisis temporal.
    """
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))

    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    rows = conn.execute("""
        SELECT DATE(fecha) as dia, COUNT(*) as total
        FROM articulos
        WHERE fecha >= ?
          AND (titulo LIKE ? OR resumen LIKE ?)
        GROUP BY dia
        ORDER BY dia
    """, (fecha_limite, f"%{keyword}%", f"%{keyword}%")).fetchall()

    conn.close()
    return {row[0]: row[1] for row in rows}


def obtener_score_media(categoria_keywords, dias=7):
    """
    Calcula score 0-100 de presión mediática para una categoría.
    Considera: volumen de notas, peso de fuentes, tendencia.
    """
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))

    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    # Total de artículos en el periodo
    total = conn.execute(
        "SELECT COUNT(*) FROM articulos WHERE fecha >= ?", (fecha_limite,)
    ).fetchone()[0]

    if total == 0:
        conn.close()
        return 0

    # Artículos relevantes con peso ponderado
    score_acum = 0
    for kw in categoria_keywords:
        rows = conn.execute("""
            SELECT peso_fuente FROM articulos
            WHERE fecha >= ?
              AND (titulo LIKE ? OR resumen LIKE ?)
        """, (fecha_limite, f"%{kw}%", f"%{kw}%")).fetchall()

        for row in rows:
            score_acum += row[0]  # Sumar peso de la fuente

    conn.close()

    # Normalizar: si hay >20 menciones ponderadas en una semana → 100
    score = min((score_acum / 20) * 100, 100)
    return round(score, 2)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=== Scraper de Medios (RSS) ===")
    resultados = scrape_todos_medios()
    print(f"\nResultados por medio:")
    for clave, info in resultados.items():
        print(f"  {info['nombre']}: {info['obtenidos']} obtenidos, {info['nuevos']} nuevos")
