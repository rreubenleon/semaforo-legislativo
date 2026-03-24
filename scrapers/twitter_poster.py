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
            SELECT categoria, score_total, color
            FROM scores
            WHERE fecha = (SELECT MAX(fecha) FROM scores WHERE fecha < date('now'))
        """).fetchall():
            scores_previos[row["categoria"]] = {
                "score": row["score_total"],
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

    # Tweets programados por día de la semana
    ahora = datetime.now()
    dia_semana = ahora.weekday()  # 0=lunes, 6=domingo
    hora = ahora.hour

    # Solo publicar tweets programados en el primer ciclo del día (6-10 AM)
    if 6 <= hora <= 10:
        programados = _tweets_programados(dia_semana, scores_actuales)
        for tweet_prog in programados:
            if tweets_publicados >= 5:  # Máximo total incluyendo programados
                break
            th = _tweet_hash(tweet_prog["id"])
            if _already_posted(th):
                continue
            logger.info(f"Publicando tweet programado: {tweet_prog['tipo']}")
            if _post_tweet(tweet_prog["texto"]):
                _record_posted(th, tweet_prog["texto"])
                tweets_publicados += 1

    return {"publicados": tweets_publicados}


# ═══════════════════════════════════════════
# TWEETS PROGRAMADOS POR DÍA DE LA SEMANA
# ═══════════════════════════════════════════

def _tweets_programados(dia_semana, scores_actuales):
    """
    Retorna lista de tweets programados según el día.
    Cada tweet es un dict con {tipo, id, texto}.

    Calendario:
      Lunes:     Top 5 legisladores + Top 3 categorías
      Miércoles: Legisladores más activos + Snapshot semanal
      Viernes:   Spotlight legislador + Actividad por partido
    """
    tweets = []
    semana = datetime.now().strftime("%Y-W%U")

    if dia_semana == 0:  # Lunes
        tweets.extend(_tweet_top5_legisladores(semana))
        tweets.extend(_tweet_top3_categorias(scores_actuales, semana))

    elif dia_semana == 2:  # Miércoles
        tweets.extend(_tweet_legisladores_activos(semana))
        tweets.extend(_tweet_snapshot_semanal(scores_actuales, semana))

    elif dia_semana == 4:  # Viernes
        tweets.extend(_tweet_spotlight_legislador(semana))
        tweets.extend(_tweet_actividad_partidos(semana))

    return tweets


def _tweet_top5_legisladores(semana):
    """Lunes: Top 5 legisladores con mayor probabilidad de presentar instrumentos."""
    try:
        from api.predictor_autoria import predecir_autores
        conn = get_connection()
        conn.row_factory = sqlite3.Row

        # Obtener la categoría con mayor score actual
        top_cat = conn.execute("""
            SELECT categoria, score_total FROM scores
            WHERE fecha = (SELECT MAX(fecha) FROM scores)
            ORDER BY score_total DESC LIMIT 1
        """).fetchone()

        if not top_cat:
            return []

        cat_key = top_cat["categoria"]
        nombre_cat = CATEGORIAS.get(cat_key, {}).get("nombre", cat_key)
        predicciones = predecir_autores(cat_key, top_n=5)

        if not predicciones:
            return []

        lineas = []
        for i, p in enumerate(predicciones, 1):
            partido = p.get("partido", "")
            score = p.get("score_total", 0)
            lineas.append(f"{i}. {p['nombre']} ({partido}) — {score:.0f}%")

        tweet = f"📊 Top 5 legisladores con mayor probabilidad de presentar instrumentos en {nombre_cat} esta semana:\n\n"
        tweet += "\n".join(lineas)
        tweet += "\n\n#CongresoMX #Legisladores\nfiatmx.com"

        if len(tweet) > 280:
            # Recortar a 3 legisladores
            lineas = lineas[:3]
            tweet = f"📊 Top 3 legisladores con mayor probabilidad en {nombre_cat}:\n\n"
            tweet += "\n".join(lineas)
            tweet += "\n\n#CongresoMX\nfiatmx.com"

        return [{"tipo": "top5_legisladores", "id": f"top5leg-{semana}", "texto": tweet}]
    except Exception as e:
        logger.warning(f"Tweet top5 legisladores falló: {e}")
        return []


def _tweet_top3_categorias(scores_actuales, semana):
    """Lunes: Las 3 categorías con mayor probabilidad legislativa."""
    try:
        # Ordenar por score
        sorted_cats = sorted(
            scores_actuales.items(),
            key=lambda x: x[1].get("score", 0),
            reverse=True
        )[:3]

        if not sorted_cats:
            return []

        lineas = []
        for i, (cat_key, data) in enumerate(sorted_cats, 1):
            nombre = CATEGORIAS.get(cat_key, {}).get("nombre", cat_key)
            score = data.get("score", 0)
            emoji = EMOJI_COLOR.get(_get_color(score), "⚪")
            lineas.append(f"{emoji} {i}. {nombre}: {score:.0f}%")

        tweet = "🏛️ Las 3 categorías con mayor probabilidad de actividad legislativa esta semana:\n\n"
        tweet += "\n".join(lineas)
        tweet += "\n\n#CongresoMX #ActividadLegislativa\nfiatmx.com"

        return [{"tipo": "top3_categorias", "id": f"top3cat-{semana}", "texto": tweet}]
    except Exception as e:
        logger.warning(f"Tweet top3 categorías falló: {e}")
        return []


def _tweet_legisladores_activos(semana):
    """Miércoles: Legisladores más activos en el Congreso."""
    try:
        conn = get_connection()
        conn.row_factory = sqlite3.Row

        rows = conn.execute("""
            SELECT l.nombre, l.partido, l.camara, COUNT(*) as total
            FROM actividad_legislador al
            JOIN legisladores l ON al.legislador_id = l.id
            WHERE al.fecha_presentacion >= date('now', '-30 days')
            GROUP BY al.legislador_id
            ORDER BY total DESC
            LIMIT 5
        """).fetchall()

        if not rows:
            return []

        lineas = []
        for i, r in enumerate(rows, 1):
            camara = "Dip." if "diputad" in (r["camara"] or "").lower() else "Sen."
            lineas.append(f"{i}. {camara} {r['nombre']} ({r['partido']}) — {r['total']} instrumentos")

        tweet = "⚡ Legisladores más activos en el Congreso de México (últimos 30 días):\n\n"
        tweet += "\n".join(lineas)
        tweet += "\n\n#CongresoMX #Legisladores\nfiatmx.com"

        if len(tweet) > 280:
            lineas = lineas[:3]
            tweet = "⚡ Top 3 legisladores más activos (30 días):\n\n"
            tweet += "\n".join(lineas)
            tweet += "\n\n#CongresoMX\nfiatmx.com"

        return [{"tipo": "legisladores_activos", "id": f"legact-{semana}", "texto": tweet}]
    except Exception as e:
        logger.warning(f"Tweet legisladores activos falló: {e}")
        return []


def _tweet_snapshot_semanal(scores_actuales, semana):
    """Miércoles: Snapshot semanal de scores."""
    try:
        sorted_cats = sorted(
            scores_actuales.items(),
            key=lambda x: x[1].get("score", 0),
            reverse=True
        )

        rojos = sum(1 for _, d in sorted_cats if _get_color(d.get("score", 0)) == "rojo")
        amarillos = sum(1 for _, d in sorted_cats if _get_color(d.get("score", 0)) == "amarillo")
        verdes = sum(1 for _, d in sorted_cats if _get_color(d.get("score", 0)) == "verde")

        top = sorted_cats[0] if sorted_cats else None
        if not top:
            return []

        nombre_top = CATEGORIAS.get(top[0], {}).get("nombre", top[0])
        score_top = top[1].get("score", 0)

        tweet = f"📈 Snapshot semanal FIAT\n\n"
        tweet += f"🔴 Alta actividad: {rojos} categorías\n"
        tweet += f"🟡 Actividad elevada: {amarillos} categorías\n"
        tweet += f"🟢 Actividad baja: {verdes} categorías\n\n"
        tweet += f"Mayor probabilidad: {nombre_top} ({score_top:.0f}%)\n\n"
        tweet += "#CongresoMX #SemáforoLegislativo\nfiatmx.com"

        return [{"tipo": "snapshot_semanal", "id": f"snapshot-{semana}", "texto": tweet}]
    except Exception as e:
        logger.warning(f"Tweet snapshot falló: {e}")
        return []


def _tweet_spotlight_legislador(semana):
    """Viernes: Spotlight de un legislador destacado."""
    try:
        from api.predictor_autoria import predecir_autores
        conn = get_connection()
        conn.row_factory = sqlite3.Row

        # Top categoría actual
        top_cat = conn.execute("""
            SELECT categoria FROM scores
            WHERE fecha = (SELECT MAX(fecha) FROM scores)
            ORDER BY score_total DESC LIMIT 1
        """).fetchone()

        if not top_cat:
            return []

        predicciones = predecir_autores(top_cat["categoria"], top_n=1)
        if not predicciones:
            return []

        leg = predicciones[0]
        nombre_cat = CATEGORIAS.get(top_cat["categoria"], {}).get("nombre", top_cat["categoria"])
        camara = "Diputado/a" if "diputad" in (leg.get("camara", "") or "").lower() else "Senador/a"

        comisiones = leg.get("comisiones_afines", [])
        com_text = f"Comisión: {comisiones[0]}" if comisiones else ""

        tweet = f"🔦 Spotlight legislativo\n\n"
        tweet += f"{camara} {leg['nombre']} ({leg.get('partido', '')})\n"
        if com_text:
            tweet += f"{com_text}\n"
        tweet += f"\nMayor probabilidad de presentar instrumentos en {nombre_cat} ({leg.get('score_total', 0):.0f}%)\n\n"
        tweet += "#CongresoMX\nfiatmx.com"

        if len(tweet) > 280:
            tweet = f"🔦 Spotlight: {camara} {leg['nombre']} ({leg.get('partido', '')})\n"
            tweet += f"Mayor probabilidad en {nombre_cat}: {leg.get('score_total', 0):.0f}%\n\n"
            tweet += "#CongresoMX\nfiatmx.com"

        return [{"tipo": "spotlight", "id": f"spot-{semana}", "texto": tweet}]
    except Exception as e:
        logger.warning(f"Tweet spotlight falló: {e}")
        return []


def _tweet_actividad_partidos(semana):
    """Viernes: Actividad por partido político."""
    try:
        conn = get_connection()
        conn.row_factory = sqlite3.Row

        rows = conn.execute("""
            SELECT l.partido, COUNT(*) as total
            FROM actividad_legislador al
            JOIN legisladores l ON al.legislador_id = l.id
            WHERE al.fecha_presentacion >= date('now', '-30 days')
              AND l.partido IS NOT NULL AND l.partido != ''
            GROUP BY l.partido
            ORDER BY total DESC
            LIMIT 6
        """).fetchall()

        if not rows:
            return []

        lineas = []
        for r in rows:
            lineas.append(f"▪️ {r['partido']}: {r['total']} instrumentos")

        tweet = "🏛️ Actividad por partido en el Congreso (últimos 30 días):\n\n"
        tweet += "\n".join(lineas)
        tweet += "\n\n#CongresoMX #PartidosPolíticos\nfiatmx.com"

        if len(tweet) > 280:
            lineas = lineas[:4]
            tweet = "🏛️ Actividad por partido (30 días):\n\n"
            tweet += "\n".join(lineas)
            tweet += "\n\n#CongresoMX\nfiatmx.com"

        return [{"tipo": "actividad_partidos", "id": f"partidos-{semana}", "texto": tweet}]
    except Exception as e:
        logger.warning(f"Tweet partidos falló: {e}")
        return []
