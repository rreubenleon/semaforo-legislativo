"""
Twitter/X Auto-Poster — OAuth 1.0a
Publica alertas automáticas en @Fiat_MX usando el modelo reactivo de correlación.

Triggers:
  - Categoría sube a ROJO (score >= 65) o cambia de color
  - Score muy alto (>= 75)

Formato de tweets (correlación reactiva):
  🔴 Electoral y Político alcanzó 71% esta semana.
  Nuestro modelo detecta que [Legislador] ([Partido]) ha reaccionado
  al X% de los picos. Históricamente presenta [instrumento] sobre [ley].
  fiatmx.com
"""

import os
import json
import logging
import sqlite3
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Zona horaria CDMX (UTC-6)
_TZ_CDMX = timezone(timedelta(hours=-6))

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
        hoy = datetime.now(_TZ_CDMX).strftime("%Y-%m-%d")
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
            (tweet_hash, text[:280], datetime.now(_TZ_CDMX).strftime("%Y-%m-%d"),
             datetime.now(_TZ_CDMX).strftime("%Y-%m-%d %H:%M:%S"))
        )
        conn.commit()
    except Exception as e:
        logger.warning(f"Error registrando tweet: {e}")


def _get_color(score):
    # En FIAT: score alto = verde (alta actividad), score bajo = rojo (baja actividad)
    if score >= 65:
        return "verde"
    elif score >= 40:
        return "amarillo"
    return "rojo"


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


def _get_top_correlacion(categoria):
    """
    Obtiene el legislador #1 del modelo reactivo para una categoría.
    Returns dict con nombre, partido, instrumento_probable, ley_probable,
    correlacion_score, veces_reaccionado, total_picos, o None.
    """
    try:
        from api.predictor_autoria import predecir_autores
        predicciones = predecir_autores(categoria, top_n=1)
        if predicciones:
            return predicciones[0]
        return None
    except Exception as e:
        logger.warning(f"Error obteniendo correlación para {categoria}: {e}")
        return None


def _construir_tweet_correlacion(cat_key, nombre_cat, score, emoji, color):
    """
    Construye un tweet usando el modelo reactivo de correlación.
    Formato: score + legislador correlacionado + instrumento/ley.
    """
    zona = "zona verde" if color == "verde" else "zona amarilla" if color == "amarillo" else "zona roja"
    verbo = "alcanzó" if color == "verde" else "está en"

    # Intentar obtener datos de correlación
    leg = _get_top_correlacion(cat_key)

    if leg and leg.get("veces_reaccionado", 0) > 0 and leg.get("total_picos", 0) > 0:
        pct_reaccion = round((leg["veces_reaccionado"] / leg["total_picos"]) * 100)
        instrumento = leg.get("instrumento_probable", "").lower() or "instrumentos"
        ley = leg.get("ley_probable")

        tweet = f"{emoji} {nombre_cat} {verbo} {score:.0f}%, {zona}.\n"
        tweet += f"Nuestro modelo detecta que {leg['nombre']} ({leg.get('partido', '')}) "
        tweet += f"ha reaccionado al {pct_reaccion}% de los picos en esta categoría."

        if instrumento and ley:
            tweet += f"\nHistóricamente presenta {instrumento} sobre {ley}."
        elif instrumento and instrumento != "instrumentos":
            tweet += f"\nHistóricamente presenta {instrumento}."

        tweet += "\nfiatmx.com"
    else:
        # Fallback sin datos de correlación: tweet simple con nota
        nota = _get_ultima_nota(cat_key)
        tweet = f"{emoji} {nombre_cat} {verbo} {score:.0f}%, {zona}.\n"
        if nota:
            tweet += f"📰 {nota}\n"
        tweet += "Seguimos monitoreando → fiatmx.com"

    # Recortar si excede 280 chars
    if len(tweet) > 280:
        # Quitar la línea de instrumento/ley y dejar solo la correlación
        lineas = tweet.split("\n")
        while len("\n".join(lineas)) > 280 and len(lineas) > 2:
            lineas.pop(-2)  # Quitar penúltima línea (antes de fiatmx.com)
        tweet = "\n".join(lineas)

    return tweet


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

    # ── Alertas por cambio de color o score alto ──
    for cat_key, cat_data in scores_actuales.items():
        if tweets_publicados >= max_tweets:
            break

        score = cat_data.get("score", 0)
        color = _get_color(score)
        nombre = CATEGORIAS.get(cat_key, {}).get("nombre", cat_key)
        emoji = EMOJI_COLOR.get(color, "⚪")

        should_tweet = False
        motivo = ""

        prev = scores_previos.get(cat_key, {})
        prev_color = prev.get("color", "rojo")  # Default rojo = inactivo

        # Trigger 1: Categoría sube a VERDE (alta actividad)
        if color == "verde" and prev_color != "verde":
            should_tweet = True
            motivo = "subió a verde (alta actividad)"

        # Trigger 2: Categoría sube a AMARILLO desde ROJO
        elif color == "amarillo" and prev_color == "rojo":
            should_tweet = True
            motivo = "subió a amarillo"

        # Trigger 3: Score muy alto (>= 75) sin importar cambio
        elif score >= 75:
            should_tweet = True
            motivo = "score alto"

        if not should_tweet:
            continue

        tweet = _construir_tweet_correlacion(cat_key, nombre, score, emoji, color)

        th = _tweet_hash(f"{cat_key}-{color}-{datetime.now(_TZ_CDMX).strftime('%Y-%m-%d')}")
        if _already_posted(th):
            logger.info(f"Tweet para {nombre} ya publicado hoy, saltando")
            continue

        logger.info(f"Publicando tweet alerta: {nombre} ({motivo})")
        if _post_tweet(tweet):
            _record_posted(th, tweet)
            tweets_publicados += 1

    # ── Trigger 4: Tweet diario de la categoría más alta ──
    # Garantiza al menos 1 tweet al día aunque no haya cambio de color
    hoy_str = datetime.now(_TZ_CDMX).strftime("%Y-%m-%d")
    th_diario = _tweet_hash(f"diario-{hoy_str}")
    if tweets_publicados == 0 and not _already_posted(th_diario):
        # Encontrar la categoría con score más alto
        top_cat = max(scores_actuales.items(), key=lambda x: x[1].get("score", 0))
        cat_key, cat_data = top_cat
        score = cat_data.get("score", 0)
        color = _get_color(score)
        nombre = CATEGORIAS.get(cat_key, {}).get("nombre", cat_key)
        emoji = EMOJI_COLOR.get(color, "⚪")

        tweet = _construir_tweet_correlacion(cat_key, nombre, score, emoji, color)
        logger.info(f"Publicando tweet diario: {nombre} (top del día)")
        if _post_tweet(tweet):
            _record_posted(th_diario, tweet)
            tweets_publicados += 1

    # ── Tweets programados por día de la semana ──
    ahora = datetime.now(_TZ_CDMX)
    dia_semana = ahora.weekday()  # 0=lunes, 6=domingo

    # Publicar tweets programados: 1 vez al día (dedup por hash semanal)
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
    semana = datetime.now(_TZ_CDMX).strftime("%Y-W%U")

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
    """Lunes: Top legisladores reactivos en la categoría más alta."""
    try:
        from api.predictor_autoria import predecir_autores
        conn = get_connection()
        conn.row_factory = sqlite3.Row

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
            instrumento = p.get("instrumento_probable", "")
            pct = ""
            if p.get("veces_reaccionado", 0) > 0 and p.get("total_picos", 0) > 0:
                pct = f" ({round((p['veces_reaccionado']/p['total_picos'])*100)}%)"
            linea = f"{i}. {p['nombre']} ({partido}){pct}"
            if instrumento:
                linea += f" → {instrumento.lower()}"
            lineas.append(linea)

        tweet = f"📊 Legisladores más reactivos en {nombre_cat} ({top_cat['score_total']:.0f}%):\n\n"
        tweet += "\n".join(lineas)
        tweet += "\nfiatmx.com"

        # Ajustar si excede 280
        while len(tweet) > 280 and len(lineas) > 3:
            lineas.pop()
            tweet = f"📊 Legisladores más reactivos en {nombre_cat} ({top_cat['score_total']:.0f}%):\n\n"
            tweet += "\n".join(lineas)
            tweet += "\nfiatmx.com"

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
        tweet += f"🟢 Alta actividad: {verdes} categorías\n"
        tweet += f"🟡 Actividad elevada: {amarillos} categorías\n"
        tweet += f"🔴 Actividad baja: {rojos} categorías\n\n"
        tweet += f"Mayor probabilidad: {nombre_top} ({score_top:.0f}%)\n\n"
        tweet += "#CongresoMX #SemáforoLegislativo\nfiatmx.com"

        return [{"tipo": "snapshot_semanal", "id": f"snapshot-{semana}", "texto": tweet}]
    except Exception as e:
        logger.warning(f"Tweet snapshot falló: {e}")
        return []


def _tweet_spotlight_legislador(semana):
    """Viernes: Spotlight de un legislador con narrativa reactiva."""
    try:
        from api.predictor_autoria import predecir_autores
        conn = get_connection()
        conn.row_factory = sqlite3.Row

        # Top categoría actual
        top_cat = conn.execute("""
            SELECT categoria, score_total FROM scores
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

        # Construir narrativa compacta
        instrumento = leg.get("instrumento_probable", "").lower()
        ley = leg.get("ley_probable", "")
        veces = leg.get("veces_reaccionado", 0)
        total = leg.get("total_picos", 0)

        tweet = f"🔦 Spotlight legislativo\n\n"
        tweet += f"{leg['nombre']} ({leg.get('partido', '')})\n"

        if veces > 0 and total > 0:
            pct = round((veces / total) * 100)
            tweet += f"Reacciona al {pct}% de los picos en {nombre_cat}.\n"

        if instrumento and ley:
            tweet += f"Patrón: presenta {instrumento} sobre {ley}.\n"
        elif instrumento:
            tweet += f"Patrón: presenta {instrumento}.\n"

        tweet += "fiatmx.com"

        if len(tweet) > 280:
            # Versión corta
            tweet = f"🔦 {leg['nombre']} ({leg.get('partido', '')})\n"
            if veces > 0 and total > 0:
                tweet += f"Reacciona al {round((veces/total)*100)}% de picos en {nombre_cat}.\n"
            tweet += "fiatmx.com"

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
