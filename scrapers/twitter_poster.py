"""
Twitter/X Auto-Poster — OAuth 1.0a
Publica alertas automáticas en @Fiat_MX cuando hay movimiento legislativo relevante.

Triggers:
  - Categoría sube a ROJO (score >= 65)
  - Categoría cambia de color (verde→amarillo, amarillo→rojo)
  - Nueva iniciativa de alto impacto en Gaceta/SIL

Formato de tweets:
  🔴 ALTA ACTIVIDAD | Seguridad y Justicia: 78%
  Últimas señales: [titular de nota]
  #CongresoMX #Seguridad
  fiatmx.com
"""

import os
import json
import logging
import sqlite3
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

import requests
from requests_oauthlib import OAuth1

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CATEGORIAS
from db import get_connection

logger = logging.getLogger(__name__)

# OAuth 1.0a credentials
API_KEY = os.environ.get("TWITTER_API_KEY", "")
API_SECRET = os.environ.get("TWITTER_API_SECRET", "")
ACCESS_TOKEN = os.environ.get("TWITTER_ACCESS_TOKEN", "")
ACCESS_SECRET = os.environ.get("TWITTER_ACCESS_SECRET", "")

TWEET_API = "https://api.x.com/2/tweets"

# Hashtags por categoría
HASHTAGS = {
    "seguridad_justicia": "#Seguridad #CongresoMX",
    "economia_hacienda": "#Economía #Hacienda",
    "energia": "#Energía #ReformaEnergética",
    "salud": "#Salud #SaludPública",
    "educacion": "#Educación #SEP",
    "trabajo": "#Trabajo #DerechosLaborales",
    "electoral_politico": "#CongresoMX #Política",
    "derechos_humanos": "#DerechosHumanos #DDHH",
    "infraestructura": "#Infraestructura #ObrasPúblicas",
    "agro_rural": "#CampoMexicano #Agricultura",
    "relaciones_exteriores": "#RelacionesExteriores #Diplomacia",
    "anticorrupcion": "#Anticorrupción #Transparencia",
    "medio_ambiente": "#MedioAmbiente #CambioClimático",
    "politica_social": "#PolíticaSocial #Bienestar",
    "medios_comunicacion": "#Medios #LibertadDeExpresión",
    "turismo": "#Turismo #TurismoMX",
    "inteligencia_artificial": "#IA #Tecnología",
    "igualdad_genero": "#IgualdadDeGénero #Género",
}

# Emojis por color de semáforo
EMOJI_COLOR = {
    "rojo": "🔴",
    "amarillo": "🟡",
    "verde": "🟢",
}


def _get_auth():
    """Construye OAuth1 para posting."""
    if not all([API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET]):
        return None
    return OAuth1(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET)


def _post_tweet(text):
    """Publica un tweet. Retorna True si fue exitoso."""
    auth = _get_auth()
    if not auth:
        logger.warning("Twitter poster: credenciales OAuth 1.0a no configuradas")
        return False

    try:
        resp = requests.post(
            TWEET_API,
            json={"text": text},
            auth=auth,
            timeout=15,
        )
        if resp.status_code == 201:
            tweet_id = resp.json().get("data", {}).get("id", "?")
            logger.info(f"Tweet publicado: {tweet_id}")
            return True
        else:
            logger.warning(f"Twitter poster error {resp.status_code}: {resp.text[:200]}")
            return False
    except Exception as e:
        logger.warning(f"Twitter poster excepción: {e}")
        return False


def _tweet_hash(text):
    """Hash para evitar duplicados."""
    return hashlib.md5(text.encode()).hexdigest()[:12]


def _already_posted(tweet_hash):
    """Verifica si un tweet similar ya se publicó hoy."""
    try:
        conn = get_connection()
        hoy = datetime.now().strftime("%Y-%m-%d")
        result = conn.execute(
            "SELECT COUNT(*) FROM tweets_posted WHERE hash = ? AND fecha >= ?",
            (tweet_hash, hoy)
        ).fetchone()
        return result[0] > 0 if result else False
    except Exception:
        return False


def _record_posted(tweet_hash, text):
    """Registra un tweet publicado."""
    try:
        conn = get_connection()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tweets_posted (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT,
                texto TEXT,
                fecha TEXT,
                fecha_hora TEXT
            )
        """)
        conn.execute(
            "INSERT INTO tweets_posted (hash, texto, fecha, fecha_hora) VALUES (?, ?, ?, ?)",
            (tweet_hash, text[:280], datetime.now().strftime("%Y-%m-%d"),
             datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        conn.commit()
    except Exception as e:
        logger.warning(f"Error registrando tweet: {e}")


def _get_color(score):
    if score >= 65:
        return "rojo"
    elif score >= 40:
        return "amarillo"
    return "verde"


def _get_ultima_nota(categoria):
    """Obtiene el titular más reciente para una categoría."""
    try:
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        row = conn.execute("""
            SELECT titulo, fuente_nombre FROM articulos
            WHERE categorias LIKE ? AND fecha >= date('now', '-7 days')
            ORDER BY fecha DESC LIMIT 1
        """, (f"%{categoria}%",)).fetchone()
        if row:
            titulo = row["titulo"]
            if len(titulo) > 100:
                titulo = titulo[:97] + "..."
            return titulo
        return None
    except Exception:
        return None


def generar_alertas_twitter(scores_actuales):
    """
    Genera y publica tweets basados en los scores actuales.

    Args:
        scores_actuales: dict {categoria: {score, color, nombre}} del último cálculo

    Returns:
        dict con conteo de tweets publicados
    """
    if not all([API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET]):
        logger.info("Twitter poster: sin credenciales, saltando")
        return {"publicados": 0, "motivo": "sin_credenciales"}

    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # Obtener scores previos para detectar cambios
    scores_previos = {}
    try:
        for row in conn.execute("""
            SELECT categoria, score_compuesto, color
            FROM scores
            WHERE fecha = (SELECT MAX(fecha) FROM scores WHERE fecha < date('now'))
        """).fetchall():
            scores_previos[row["categoria"]] = {
                "score": row["score_compuesto"],
                "color": row["color"],
            }
    except Exception:
        pass

    tweets_publicados = 0
    max_tweets = 3  # Máximo 3 tweets por ciclo para no spamear

    for cat_key, cat_data in scores_actuales.items():
        if tweets_publicados >= max_tweets:
            break

        score = cat_data.get("score", 0)
        color = _get_color(score)
        nombre = CATEGORIAS.get(cat_key, {}).get("nombre", cat_key)
        emoji = EMOJI_COLOR.get(color, "⚪")
        hashtags = HASHTAGS.get(cat_key, "#CongresoMX")

        # Determinar si vale la pena twittear
        should_tweet = False
        motivo = ""

        prev = scores_previos.get(cat_key, {})
        prev_color = prev.get("color", "verde")

        # Trigger 1: Categoría en ROJO
        if color == "rojo" and prev_color != "rojo":
            should_tweet = True
            motivo = "subió a rojo"

        # Trigger 2: Cambio de color significativo
        elif color == "amarillo" and prev_color == "verde":
            should_tweet = True
            motivo = "subió a amarillo"

        # Trigger 3: Score muy alto (>= 75) sin importar cambio
        elif score >= 75:
            should_tweet = True
            motivo = "score alto"

        if not should_tweet:
            continue

        # Construir tweet
        nota = _get_ultima_nota(cat_key)

        if color == "rojo":
            label = "ALTA ACTIVIDAD"
        elif color == "amarillo":
            label = "ACTIVIDAD ELEVADA"
        else:
            label = "ACTIVIDAD MODERADA"

        tweet = f"{emoji} {label} | {nombre}: {score:.0f}%\n"
        if nota:
            tweet += f"📰 {nota}\n"
        tweet += f"\n{hashtags}\nfiatmx.com"

        # Verificar que no exceda 280 chars
        if len(tweet) > 280:
            # Recortar nota
            exceso = len(tweet) - 278
            if nota:
                nota = nota[:len(nota) - exceso - 3] + "..."
                tweet = f"{emoji} {label} | {nombre}: {score:.0f}%\n📰 {nota}\n\n{hashtags}\nfiatmx.com"

        # Verificar duplicados
        th = _tweet_hash(f"{cat_key}-{color}-{datetime.now().strftime('%Y-%m-%d')}")
        if _already_posted(th):
            logger.info(f"Tweet para {nombre} ya publicado hoy, saltando")
            continue

        # Publicar
        logger.info(f"Publicando tweet: {nombre} ({motivo})")
        if _post_tweet(tweet):
            _record_posted(th, tweet)
            tweets_publicados += 1

    return {"publicados": tweets_publicados}
