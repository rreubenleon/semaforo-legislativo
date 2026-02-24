"""
Scraper de Twitter/X — API v2 (plan PPU)
Monitorea 20 cuentas: 4 periodistas + 16 coordinadores parlamentarios.
Almacena tweets, clasifica por categoría legislativa.
Provee boost al score de Media (0-15 pts).
"""

import logging
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import TWITTER_ACCOUNTS, TWITTER_BEARER_TOKEN, CATEGORIAS, DATABASE

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
API_BASE = "https://api.x.com/2"


def _headers():
    return {
        "Authorization": f"Bearer {TWITTER_BEARER_TOKEN}",
        "User-Agent": "FIAT-SemaforoLegislativo/1.0",
    }


# ─────────────────────────────────────────────
# Base de datos
# ─────────────────────────────────────────────

def _init_tabla():
    """Crea tabla de tweets si no existe."""
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tweets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tweet_id TEXT UNIQUE,
            usuario TEXT NOT NULL,
            nombre TEXT NOT NULL,
            texto TEXT NOT NULL,
            fecha TEXT NOT NULL,
            peso_cuenta REAL DEFAULT 1.0,
            categorias TEXT DEFAULT '',
            fecha_scraping TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


# ─────────────────────────────────────────────
# X API v2
# ─────────────────────────────────────────────

def _get_user_id(username):
    """Obtiene el ID numérico de un usuario desde su handle."""
    handle = username.lstrip("@")
    url = f"{API_BASE}/users/by/username/{handle}"
    try:
        resp = requests.get(url, headers=_headers(), timeout=15)
        if resp.status_code == 429:
            logger.warning("Rate limit alcanzado en user lookup")
            return None
        resp.raise_for_status()
        data = resp.json()
        if "data" in data:
            return data["data"]["id"]
        logger.warning(f"@{handle} no encontrado: {data.get('errors', data)}")
        return None
    except Exception as e:
        logger.warning(f"Error buscando @{handle}: {e}")
        return None


def _get_user_tweets(user_id, max_results=10):
    """Obtiene tweets recientes de un usuario (excluye RT y replies)."""
    url = f"{API_BASE}/users/{user_id}/tweets"
    params = {
        "max_results": min(max_results, 100),
        "tweet.fields": "created_at,text",
        "exclude": "retweets,replies",
    }
    try:
        resp = requests.get(url, headers=_headers(), params=params, timeout=15)
        if resp.status_code == 429:
            logger.warning("Rate limit alcanzado en timeline")
            return []
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else "?"
        logger.warning(f"HTTP {status} obteniendo tweets de {user_id}: {e}")
        return []
    except Exception as e:
        logger.warning(f"Error obteniendo tweets de {user_id}: {e}")
        return []


# ─────────────────────────────────────────────
# Clasificación
# ─────────────────────────────────────────────

def _clasificar_tweet(texto):
    """Clasifica un tweet por categorías legislativas usando keywords."""
    categorias = []
    texto_lower = texto.lower()
    for cat_clave, cat_config in CATEGORIAS.items():
        for kw in cat_config["keywords"]:
            if kw.lower() in texto_lower:
                categorias.append(cat_clave)
                break
    return categorias


# ─────────────────────────────────────────────
# Scraper principal
# ─────────────────────────────────────────────

def scrape_twitter(max_por_cuenta=10):
    """
    Scrapea tweets recientes de las cuentas monitoreadas.
    Retorna resumen: {"cuentas": N, "tweets_nuevos": N}
    """
    if not TWITTER_BEARER_TOKEN:
        logger.warning("TWITTER_BEARER_TOKEN vacío — omitiendo Twitter")
        return {"cuentas": 0, "tweets_nuevos": 0}

    conn = _init_tabla()
    total_nuevos = 0
    cuentas_ok = 0

    # Cache de user IDs para evitar lookups repetidos
    user_ids = {}

    for cuenta in TWITTER_ACCOUNTS:
        handle = cuenta["handle"].lstrip("@")
        nombre = cuenta["nombre"]
        peso = cuenta["peso"]

        # Obtener user ID (con cache)
        if handle not in user_ids:
            uid = _get_user_id(handle)
            if not uid:
                continue
            user_ids[handle] = uid
            time.sleep(1)

        user_id = user_ids[handle]
        tweets = _get_user_tweets(user_id, max_results=max_por_cuenta)
        if not tweets:
            time.sleep(1)
            continue

        cuentas_ok += 1
        nuevos = 0

        for tweet in tweets:
            tweet_id = tweet["id"]
            texto = tweet.get("text", "")
            fecha_raw = tweet.get("created_at", "")
            fecha = fecha_raw[:19].replace("T", " ") if fecha_raw else ""

            categorias = _clasificar_tweet(texto)

            try:
                conn.execute("""
                    INSERT INTO tweets
                        (tweet_id, usuario, nombre, texto, fecha,
                         peso_cuenta, categorias, fecha_scraping)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    tweet_id, handle, nombre, texto[:1000],
                    fecha, peso, ",".join(categorias),
                    datetime.now().isoformat(),
                ))
                conn.commit()
                nuevos += 1
            except sqlite3.IntegrityError:
                pass  # Tweet duplicado

        total_nuevos += nuevos
        logger.info(f"  @{handle}: {len(tweets)} obtenidos, {nuevos} nuevos")
        time.sleep(1.5)  # Rate limit entre cuentas

    conn.close()
    logger.info(f"Twitter: {cuentas_ok} cuentas, {total_nuevos} tweets nuevos")
    return {"cuentas": cuentas_ok, "tweets_nuevos": total_nuevos}


# ─────────────────────────────────────────────
# Score: boost para Media
# ─────────────────────────────────────────────

def obtener_boost_twitter(categoria_clave, dias=7):
    """
    Calcula boost 0-15 para el score de Media basado en tweets recientes.

    - Periodistas → presión mediática directa
    - Coordinadores parlamentarios → señal de agenda política en medios
    - Ponderado por peso de la cuenta y recencia (half-life 3 días)
    - Boost máximo: 15 pts (se suma al score_media antes del cap)
    """
    db_path = ROOT / DATABASE["archivo"]
    try:
        conn = sqlite3.connect(str(db_path))
        fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
        rows = conn.execute("""
            SELECT fecha, peso_cuenta FROM tweets
            WHERE categorias LIKE ?
              AND fecha >= ?
            ORDER BY fecha DESC
        """, (f"%{categoria_clave}%", fecha_limite)).fetchall()
        conn.close()
    except sqlite3.OperationalError:
        return 0.0

    if not rows:
        return 0.0

    hoy = datetime.now()
    boost = 0.0
    for fecha_str, peso in rows:
        try:
            fecha = datetime.strptime(fecha_str[:10], "%Y-%m-%d")
        except (ValueError, TypeError):
            continue
        dias_atras = (hoy - fecha).days
        peso_temporal = 2.0 ** (-dias_atras / 3.0)  # Half-life 3 días
        boost += 3.0 * peso * peso_temporal

    return min(round(boost, 2), 15.0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=== Scraper de Twitter/X ===")
    resultado = scrape_twitter()
    print(f"\nResultado: {resultado}")

    print("\n=== Boost por categoría ===")
    for cat_key in sorted(CATEGORIAS.keys()):
        boost = obtener_boost_twitter(cat_key)
        if boost > 0:
            print(f"  {CATEGORIAS[cat_key]['nombre']}: +{boost}")
