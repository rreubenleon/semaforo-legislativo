"""
Scraper de Google Trends para México — vía SerpAPI
Obtiene interés de búsqueda para keywords de cada categoría legislativa.
Usa SerpAPI (100 búsquedas gratis/mes) en vez de pytrends (bloqueado por 429).
"""

import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CATEGORIAS, GOOGLE_TRENDS, obtener_keywords_categoria
from db import get_connection

logger = logging.getLogger(__name__)

SERPAPI_KEY = os.environ.get("SERPAPI_KEY", "")
SERPAPI_ENDPOINT = "https://serpapi.com/search.json"


def init_db():
    """Crea la tabla de trends si no existe."""
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trends (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            categoria TEXT NOT NULL,
            keyword TEXT NOT NULL,
            fecha TEXT NOT NULL,
            valor INTEGER NOT NULL,
            fecha_consulta TEXT NOT NULL,
            UNIQUE(categoria, keyword, fecha)
        )
    """)
    conn.commit()
    return conn


def consultar_trends_serpapi(categoria_clave, max_keywords=5):
    """
    Consulta Google Trends vía SerpAPI para una categoría.
    SerpAPI acepta hasta 5 keywords separadas por coma.
    Retorna {keyword: {fecha: valor}} o {} si falla.
    Consume 1 búsqueda del plan por llamada.
    """
    if not SERPAPI_KEY:
        logger.warning("SERPAPI_KEY no configurada — omitiendo Trends")
        return {}

    cat_config = CATEGORIAS[categoria_clave]
    keywords = cat_config.get(
        "trends_keywords",
        obtener_keywords_categoria(categoria_clave)[:max_keywords]
    )

    if not keywords:
        return {}

    # SerpAPI Google Trends: keywords separadas por coma
    q = ",".join(keywords)
    logger.info(f"SerpAPI Trends para {cat_config['nombre']}: {keywords}")

    try:
        resp = requests.get(SERPAPI_ENDPOINT, params={
            "engine": "google_trends",
            "q": q,
            "geo": GOOGLE_TRENDS["geo"],
            "date": "now 7-d",
            "tz": "360",       # UTC-6 CDMX
            "data_type": "TIMESERIES",
            "api_key": SERPAPI_KEY,
        }, timeout=30)

        if resp.status_code != 200:
            logger.error(f"SerpAPI error {resp.status_code}: {resp.text[:200]}")
            return {}

        data = resp.json()

        # Extraer serie temporal de interest_over_time
        timeline = data.get("interest_over_time", {}).get("timeline_data", [])
        if not timeline:
            logger.warning(f"Sin datos de Trends para {cat_config['nombre']}")
            return {}

        resultados = {}
        for point in timeline:
            # Cada point tiene "date" (texto) y "values" (lista de {query, value, extracted_value})
            # Usamos timestamp para la fecha
            timestamp = int(point.get("timestamp", "0"))
            if timestamp:
                fecha_str = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
            else:
                continue

            for val_info in point.get("values", []):
                keyword = val_info.get("query", "")
                valor = val_info.get("extracted_value", 0)
                if keyword not in resultados:
                    resultados[keyword] = {}
                resultados[keyword][fecha_str] = int(valor)

        return resultados

    except Exception as e:
        logger.error(f"Error SerpAPI Trends para {cat_config['nombre']}: {e}")
        return {}


def _preservar_trends_fallback(conn, categoria_clave):
    """
    Cuando SerpAPI falla o no hay key, replica los últimos
    valores conocidos con fecha de hoy para que los scores no caigan a 0.
    Solo replica si los datos más recientes tienen menos de 30 días.
    """
    hoy = datetime.now().strftime("%Y-%m-%d")
    hace_30d = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    ultimos = conn.execute("""
        SELECT keyword, valor, MAX(fecha) as ultima_fecha
        FROM trends
        WHERE categoria = ? AND fecha >= ?
        GROUP BY keyword
    """, (categoria_clave, hace_30d)).fetchall()

    replicados = 0
    for row in ultimos:
        keyword, valor, ultima_fecha = row
        if ultima_fecha == hoy:
            continue
        try:
            conn.execute("""
                INSERT INTO trends (categoria, keyword, fecha, valor, fecha_consulta)
                VALUES (?, ?, ?, ?, ?)
            """, (categoria_clave, keyword, hoy, valor,
                  datetime.now().isoformat() + "_fallback"))
            replicados += 1
        except (sqlite3.IntegrityError, ValueError):
            pass

    if replicados > 0:
        conn.commit()
        logger.info(f"  Trends fallback: {replicados} valores replicados para {categoria_clave}")
    return replicados


def scrape_trends_todas_categorias():
    """
    Consulta Google Trends vía SerpAPI para todas las categorías.
    Cada categoría = 1 búsqueda SerpAPI (hasta 5 keywords por consulta).
    20 categorías = 20 búsquedas/run.
    Con plan free (100/mes) alcanza para 5 runs/mes = ~1 por semana.
    """
    conn = init_db()
    resumen = {}
    categorias_ok = 0
    categorias_fallback = 0

    for cat_clave in CATEGORIAS:
        datos = consultar_trends_serpapi(cat_clave)

        registros_nuevos = 0
        if datos:
            categorias_ok += 1
            for keyword, serie in datos.items():
                for fecha, valor in serie.items():
                    try:
                        conn.execute("""
                            INSERT INTO trends (categoria, keyword, fecha, valor, fecha_consulta)
                            VALUES (?, ?, ?, ?, ?)
                        """, (cat_clave, keyword, fecha, valor, datetime.now().isoformat()))
                        registros_nuevos += 1
                    except (sqlite3.IntegrityError, ValueError):
                        conn.execute("""
                            UPDATE trends SET valor = ?, fecha_consulta = ?
                            WHERE categoria = ? AND keyword = ? AND fecha = ?
                        """, (valor, datetime.now().isoformat(), cat_clave, keyword, fecha))

            conn.commit()
        else:
            categorias_fallback += 1
            _preservar_trends_fallback(conn, cat_clave)

        resumen[cat_clave] = {
            "nombre": CATEGORIAS[cat_clave]["nombre"],
            "keywords_consultadas": len(datos),
            "registros_nuevos": registros_nuevos,
        }

        # Pausa corta entre consultas (SerpAPI no rate-limita agresivo)
        time.sleep(2)

    if categorias_fallback > 0:
        logger.warning(f"Trends: {categorias_fallback}/{len(CATEGORIAS)} categorías con fallback")
    logger.info(f"Trends: {categorias_ok}/{len(CATEGORIAS)} categorías OK vía SerpAPI")
    return resumen


def obtener_score_trends(categoria_clave, dias=7):
    """
    Calcula score 0-100 de interés en Google Trends para una categoría.
    Usa el promedio de las keywords de la categoría en los últimos N días.
    """
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trends (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            categoria TEXT NOT NULL,
            keyword TEXT NOT NULL,
            fecha TEXT NOT NULL,
            valor INTEGER NOT NULL,
            fecha_consulta TEXT NOT NULL,
            UNIQUE(categoria, keyword, fecha)
        )
    """)

    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    rows = conn.execute("""
        SELECT AVG(valor) as promedio
        FROM trends
        WHERE categoria = ? AND fecha >= ?
    """, (categoria_clave, fecha_limite)).fetchone()

    if rows and rows[0] is not None:
        return round(rows[0], 2)

    return 0


def obtener_serie_temporal(categoria_clave, dias=30):
    """
    Retorna serie temporal de Trends para análisis de correlación.
    Formato: {fecha: valor_promedio}
    """
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trends (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            categoria TEXT NOT NULL,
            keyword TEXT NOT NULL,
            fecha TEXT NOT NULL,
            valor INTEGER NOT NULL,
            fecha_consulta TEXT NOT NULL,
            UNIQUE(categoria, keyword, fecha)
        )
    """)

    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    rows = conn.execute("""
        SELECT fecha, AVG(valor) as promedio
        FROM trends
        WHERE categoria = ? AND fecha >= ?
        GROUP BY fecha
        ORDER BY fecha
    """, (categoria_clave, fecha_limite)).fetchall()
    return {row[0]: row[1] for row in rows}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=== Scraper Google Trends (SerpAPI) ===")
    resumen = scrape_trends_todas_categorias()
    for cat, info in resumen.items():
        print(f"  {info['nombre']}: {info['keywords_consultadas']} keywords, {info['registros_nuevos']} registros")
