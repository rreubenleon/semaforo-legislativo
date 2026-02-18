"""
Scraper de Google Trends para México
Obtiene interés de búsqueda para keywords de cada categoría legislativa
"""

import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CATEGORIAS, GOOGLE_TRENDS, DATABASE

logger = logging.getLogger(__name__)


def init_db():
    """Crea la tabla de trends si no existe."""
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
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


def crear_cliente_trends():
    """Crea cliente de pytrends con config de México."""
    from pytrends.request import TrendReq
    return TrendReq(
        hl=GOOGLE_TRENDS["language"],
        tz=360,  # UTC-6 (CDMX)
    )


def consultar_trends_categoria(pytrends, categoria_clave, max_keywords=5):
    """
    Consulta Google Trends para las keywords principales de una categoría.
    Google Trends acepta máximo 5 keywords por consulta.
    """
    cat_config = CATEGORIAS[categoria_clave]
    keywords = cat_config["keywords"][:max_keywords]

    logger.info(f"Consultando Trends para {cat_config['nombre']}: {keywords}")

    try:
        pytrends.build_payload(
            kw_list=keywords,
            cat=0,
            timeframe=GOOGLE_TRENDS["timeframe"],
            geo=GOOGLE_TRENDS["geo"],
        )
        df = pytrends.interest_over_time()

        if df.empty:
            logger.warning(f"Sin datos de Trends para {cat_config['nombre']}")
            return {}

        # Eliminar columna isPartial si existe
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])

        resultados = {}
        for keyword in df.columns:
            serie = {}
            for fecha, valor in df[keyword].items():
                fecha_str = fecha.strftime("%Y-%m-%d")
                serie[fecha_str] = int(valor)
            resultados[keyword] = serie

        return resultados

    except Exception as e:
        logger.error(f"Error consultando Trends para {cat_config['nombre']}: {e}")
        return {}


def scrape_trends_todas_categorias():
    """
    Consulta Google Trends para todas las 12 categorías.
    Respeta rate limits insertando pausas entre consultas.
    """
    import time

    conn = init_db()
    pytrends = crear_cliente_trends()
    resumen = {}

    for cat_clave in CATEGORIAS:
        datos = consultar_trends_categoria(pytrends, cat_clave)

        registros_nuevos = 0
        for keyword, serie in datos.items():
            for fecha, valor in serie.items():
                try:
                    conn.execute("""
                        INSERT INTO trends (categoria, keyword, fecha, valor, fecha_consulta)
                        VALUES (?, ?, ?, ?, ?)
                    """, (cat_clave, keyword, fecha, valor, datetime.now().isoformat()))
                    registros_nuevos += 1
                except sqlite3.IntegrityError:
                    # Actualizar valor si ya existe
                    conn.execute("""
                        UPDATE trends SET valor = ?, fecha_consulta = ?
                        WHERE categoria = ? AND keyword = ? AND fecha = ?
                    """, (valor, datetime.now().isoformat(), cat_clave, keyword, fecha))

        conn.commit()
        resumen[cat_clave] = {
            "nombre": CATEGORIAS[cat_clave]["nombre"],
            "keywords_consultadas": len(datos),
            "registros_nuevos": registros_nuevos,
        }

        # Pausa entre consultas para no exceder rate limits
        time.sleep(2)

    conn.close()
    logger.info(f"Trends scraping completo para {len(resumen)} categorías")
    return resumen


def obtener_score_trends(categoria_clave, dias=7):
    """
    Calcula score 0-100 de interés en Google Trends para una categoría.
    Usa el promedio de las keywords de la categoría en los últimos N días.
    """
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    # Asegurar que la tabla existe aunque no se haya corrido el scraper
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

    conn.close()

    if rows and rows[0] is not None:
        # Google Trends ya da valores 0-100
        return round(rows[0], 2)

    return 0


def obtener_serie_temporal(categoria_clave, dias=30):
    """
    Retorna serie temporal de Trends para análisis de correlación.
    Formato: {fecha: valor_promedio}
    """
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
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

    conn.close()
    return {row[0]: row[1] for row in rows}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=== Scraper Google Trends ===")
    resumen = scrape_trends_todas_categorias()
    for cat, info in resumen.items():
        print(f"  {info['nombre']}: {info['keywords_consultadas']} keywords, {info['registros_nuevos']} registros")
