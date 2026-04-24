"""
Clasificador NLP por 17 categorías legislativas con subcategorías
Fase 1: Keyword matching con TF-IDF ponderado
Fase 2 (futuro): Transformers fine-tuned
"""

import re
import math
import logging
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CATEGORIAS, NLP_CONFIG, KEYWORDS_NEGATIVOS, KEYWORDS_MEXICO, obtener_keywords_categoria, comision_a_categoria
from db import get_connection

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# FILTRO DE CONTEXTO NO-LEGISLATIVO
# Si el artículo tiene señales fuertes de deportes o entretenimiento
# y NO tiene señales político-sociales, se descarta.
# Esto permite que temas SOCIALES pasen (feminicidios, crisis de agua,
# desastres) porque esos sí presionan al Congreso.
# ─────────────────────────────────────────────
CONTEXTO_DEPORTIVO = [
    # Fútbol mexicano
    "selección mexicana", "selección nacional", "seleccion mexicana",
    "la selección", "apoyar a la selección",
    "director técnico", "director tecnico", "convocatoria mundialista",
    "mundial 2026", "mundial de futbol", "eliminatoria mundialista",
    "liga mx", "club américa", "club america", "chivas", "pumas unam",
    "tigres uanl", "rayados", "cruz azul vs", "santos laguna",
    "copa oro", "copa del mundo", "balón de oro", "balon de oro",
    "gol de", "anotó gol", "marcador final", "medio tiempo",
    "fichaje", "transferencia de jugador", "entrenador del equipo",
    "estadio azteca", "estadio banorte", "estadio akron",
    "aficionado", "aficionados", "porra", "barra brava",
    "méxico-portugal", "mexico vs", "méxico vs",
    # Otros deportes
    "juegos olímpicos", "juegos olimpicos", "medallista olímpic",
    "sprint femenino", "sprint masculino", "clavadista",
    "torneo de tenis", "grand prix", "fórmula 1", "formula 1",
    "ufc", "pelea de box", "round de box",
    # Deportes internacionales
    "super bowl", "nfl", "nba", "mlb", "grandes ligas",
    "champions league", "premier league", "la liga española",
    "serie a italiana",
    # Maratones y atletismo (sin contexto político)
    "maratón de la ciudad", "medio maratón", "triatlón",
]

CONTEXTO_ENTRETENIMIENTO = [
    # Virales y redes sociales
    "se vuelve viral", "video viral", "se hizo viral",
    "meme de", "los memes", "horóscopo", "horoscopo",
    "signo zodiacal", "farándula", "farandula",
    "reality show", "influencer", "tiktoker", "youtuber",
    "celebridad", "paparazzi",
    # Cine, TV y streaming
    "netflix", "serie de tv", "telenovela", "estreno de cine",
    "alfombra roja", "premios oscar", "premios grammy",
    "taquilla de cine", "trailer oficial",
    "temporada de serie", "disney plus", "amazon prime video",
    # Música y conciertos
    "concierto", "gira musical", "gira de conciertos",
    "boletos de concierto", "festival musical", "sold out",
    "backstage", "meet and greet", "fan zone",
    "álbum nuevo", "sencillo musical", "videoclip",
    # Videojuegos
    "videojuego", "gamer", "esports", "playstation", "xbox",
    # Misceláneos
    "perro lobo", "mascota viral", "receta de cocina",
    "tendencia de moda", "pasarela de moda",
]

# ─────────────────────────────────────────────
# FILTRO DE CONTEXTO CLIMÁTICO / NOTA ROJA OPERATIVA
# Artículos sobre lluvias, granizo, alertas meteorológicas y afectaciones
# operativas al transporte NO son temas legislativos. Si un desastre
# natural escala a nivel de política pública (declaratoria de emergencia
# federal, presupuesto de reconstrucción, comparecencia en Congreso),
# las señales de rescate político lo salvan.
# ─────────────────────────────────────────────
CONTEXTO_CLIMATICO = [
    # Fenómenos meteorológicos
    "lluvia", "lluvias", "fuertes lluvias", "lluvia intensa", "lluvia torrencial",
    "granizo", "granizada", "tormenta eléctrica", "tormenta severa",
    "tromba", "aguacero", "chubasco",
    # Alertas meteorológicas (no confundir con protección civil legislativa)
    "alerta amarilla", "alerta naranja", "alerta roja por lluvia",
    "alerta meteorológica", "alerta metereológica",
    "pronóstico del tiempo", "pronostico del tiempo",
    "servicio meteorológico", "servicio meteorologico",
    # Afectaciones operativas (nota roja de servicios)
    "encharcamiento", "encharcamientos", "inundación vial",
    "caída de árbol", "caida de arbol", "caída de árboles",
    "corte de energía por lluvia", "apagón por tormenta",
    "retraso por lluvia", "retrasos por lluvia",
    "fallas por lluvia", "suspensión de servicio por lluvia",
    # Frío / calor extremo operativo
    "ola de calor", "golpe de calor", "temperaturas extremas",
    "helada", "nevada", "neblina densa",
]

def _es_contexto_no_legislativo(titulo, resumen=""):
    """
    Detecta si un artículo es claramente deportes, entretenimiento,
    clima operativo, o intervenciones genéricas de agenda política
    (no instrumentos legislativos).
    Retorna True si debe ser EXCLUIDO de la clasificación legislativa.

    IMPORTANTE: No excluye temas sociales (violencia, desastres, crisis)
    porque esos sí pueden generar presión legislativa. Un desastre natural
    con contexto político (declaratoria de emergencia, presupuesto de
    reconstrucción) será rescatado por las señales político-legislativas.
    """
    texto = f"{titulo} {resumen}".lower()

    # Intervenciones genéricas de Agenda Política (no son instrumentos legislativos)
    if "para referirse a la situación" in texto or "para referirse a la " in texto:
        return True

    # Matcher con word boundary para evitar falsos positivos como
    # "nfl" en "iNFLación" o "gol" en "riesGO Legal". Para frases
    # (keywords con espacios) basta substring.
    def _hit(kw, txt):
        if " " in kw:
            return kw in txt
        return bool(re.search(r'\b' + re.escape(kw) + r'\b', txt))

    # Contar señales deportivas
    hits_deporte = sum(1 for kw in CONTEXTO_DEPORTIVO if _hit(kw, texto))
    # Contar señales de entretenimiento
    hits_entretenimiento = sum(1 for kw in CONTEXTO_ENTRETENIMIENTO if _hit(kw, texto))
    # Contar señales climáticas/operativas
    hits_clima = sum(1 for kw in CONTEXTO_CLIMATICO if _hit(kw, texto))

    # Si no hay señales de ningún contexto no-legislativo, NO excluir
    if hits_deporte == 0 and hits_entretenimiento == 0 and hits_clima == 0:
        return False

    # Señales político-legislativas que RESCATAN el artículo aunque tenga
    # contexto deportivo/climático (ej: "Congreso aprueba ley para el Mundial 2026",
    # "declaratoria de emergencia federal por inundaciones")
    # NOTA: No incluir señales genéricas como "México", "CDMX" — un partido
    # de futbol en el Zócalo o una lluvia en CDMX no es tema legislativo.
    rescate_politico = [
        "congreso", "senado", "cámara de diputados", "camara de diputados",
        "iniciativa", "punto de acuerdo", "dictamen", "reforma",
        "ley ", "legisl", "presupuesto", "gobierno federal",
        "secretaría", "secretaria", "sheinbaum", "morena", "pan ",
        "pri ", "comisión", "comision", "tribunal", "corte suprema",
        "derechos humanos", "feminicidio", "desaparición", "desaparicion",
        "protesta", "manifestación", "manifestacion", "denuncia penal",
        "fiscalía general", "procurador", "ministerio público",
        # Rescate específico para desastres que escalan a política pública
        "declaratoria de emergencia", "declaratoria de desastre",
        "fondo de desastres", "fonden", "reconstrucción federal",
        "comparecencia", "exhorto",
    ]

    hits_politico = sum(1 for kw in rescate_politico if kw in texto)

    # ── Clima: cualquier señal climática sin contexto político → excluir
    # "Lluvia afecta Metro" o "alerta amarilla por granizo" no son legislativos.
    # Si un desastre escala (declaratoria de emergencia, presupuesto de
    # reconstrucción, exhorto del Senado), las señales de rescate lo salvan.
    if hits_clima >= 1 and hits_politico == 0:
        return True

    # Si tiene señales deportivas fuertes (2+), exigir al menos 2 señales políticas
    if hits_deporte >= 2 and hits_politico < 2:
        return True

    # Si tiene contexto político claro, NO excluir
    if hits_politico >= 1:
        return False

    # Si tiene 1+ señales de deporte o entretenimiento SIN contexto político: excluir
    if hits_deporte >= 1 or hits_entretenimiento >= 1:
        return True

    return False

# Stopwords del español para limpiar texto
STOPWORDS_ES = {
    "de", "la", "que", "el", "en", "y", "a", "los", "del", "se", "las",
    "por", "un", "para", "con", "no", "una", "su", "al", "lo", "como",
    "más", "pero", "sus", "le", "ya", "o", "este", "sí", "porque", "esta",
    "entre", "cuando", "muy", "sin", "sobre", "también", "me", "hasta",
    "hay", "donde", "quien", "desde", "todo", "nos", "durante", "todos",
    "uno", "les", "ni", "contra", "otros", "ese", "eso", "ante", "ellos",
    "e", "esto", "mí", "antes", "algunos", "qué", "unos", "yo", "otro",
    "otras", "otra", "él", "tanto", "esa", "estos", "mucho", "quienes",
    "nada", "muchos", "cual", "poco", "ella", "estar", "estas", "algunas",
    "algo", "nosotros", "mi", "mis", "tú", "te", "ti", "tu", "tus",
    "ellas", "nosotras", "vosotros", "vosotras", "os", "mío", "mía",
    "fue", "ser", "es", "son", "ha", "han", "era", "será", "sido",
    # Palabras estructurales de nombres de leyes mexicanas — no aportan señal temática
    # y causan sesgo hacia categorías con más leyes (economia_hacienda tiene 65 keywords con "ley")
    "ley", "federal", "general", "nacional", "organica", "orgánica",
    "reglamentaria", "codigo", "código", "constitución", "constitucion",
    "reglamento", "estatuto", "decreto",
    *NLP_CONFIG["stopwords_extra"],
}


def normalizar_texto(texto):
    """
    Normaliza texto para clasificación:
    - Minúsculas
    - Remueve acentos selectivamente (mantiene ñ)
    - Remueve puntuación
    - Tokeniza
    """
    if not texto:
        return []

    texto = texto.lower()
    # Remover URLs
    texto = re.sub(r"https?://\S+", "", texto)
    # Remover puntuación pero mantener ñ y acentos
    texto = re.sub(r"[^\w\sñáéíóú]", " ", texto)
    # Tokenizar
    tokens = texto.split()
    # Filtrar stopwords y tokens muy cortos
    tokens = [t for t in tokens if t not in STOPWORDS_ES and len(t) > 2]

    # Stemming básico de plurales en español para mejorar matching
    # "feminicidios" → "feminicidio", "reformas" → "reforma", "penas" → "pena"
    stemmed = []
    for t in tokens:
        if t.endswith("iones"):
            stemmed.append(t[:-2] + "ón")    # resoluciones → resolución
        elif t.endswith("dades"):
            stemmed.append(t[:-2])            # universidades → universidad (close enough)
        elif t.endswith("cios") or t.endswith("dios") or t.endswith("rios"):
            stemmed.append(t[:-1])            # feminicidios → feminicidio, subsidios → subsidio
        elif t.endswith("es") and len(t) > 4:
            stemmed.append(t[:-2])            # penales → penal
        elif t.endswith("s") and len(t) > 3:
            stemmed.append(t[:-1])            # reformas → reforma
        else:
            stemmed.append(t)

    return stemmed


def calcular_tf(tokens):
    """Calcula Term Frequency normalizada."""
    counter = Counter(tokens)
    total = len(tokens) if tokens else 1
    return {term: count / total for term, count in counter.items()}


def calcular_relevancia_mexico(titulo, resumen=""):
    """
    Calcula un multiplicador de relevancia-México para filtrar artículos
    que no tienen que ver con la agenda legislativa mexicana.
    Opera sobre texto crudo (antes de tokenización) para no ser afectado por stopwords.

    Retorna:
      1.0 — artículo tiene señales México (sin penalización)
      0.6 — sin señales claras (penalización leve)
      0.3 — señales negativas sin señales México (penalización fuerte)
      0.0 — múltiples señales negativas sin México (rechazo total)
    """
    texto_completo = f"{titulo} {resumen}".lower()

    def _buscar_keyword(kw, texto):
        """Busca keyword en texto. Usa word boundary para keywords cortas (≤4 chars)."""
        kw_lower = kw.lower()
        if len(kw_lower) <= 4:
            # Keywords cortas: exigir word boundary para evitar
            # falsos positivos (ej: "PRI" dentro de "primer")
            return bool(re.search(r'\b' + re.escape(kw_lower) + r'\b', texto))
        return kw_lower in texto

    # Contar señales negativas (no-México)
    hits_negativos = 0
    for neg_kw in KEYWORDS_NEGATIVOS:
        if _buscar_keyword(neg_kw, texto_completo):
            hits_negativos += 1

    # Contar señales de afinidad México
    hits_mexico = 0
    for mex_kw in KEYWORDS_MEXICO:
        if _buscar_keyword(mex_kw, texto_completo):
            hits_mexico += 1

    # Lógica de decisión
    if hits_negativos >= 2 and hits_mexico == 0:
        return 0.0   # Rechazo total: claramente internacional

    if hits_negativos >= 1 and hits_mexico == 0:
        return 0.3   # Penalización fuerte: probablemente no es México

    if hits_mexico >= 1:
        return 1.0   # Sin penalización: artículo relevante

    return 0.6       # Sin señales claras: penalización leve


# ── Señales de instrumentos legislativos ──────────────────────────────
# Cuando un artículo menciona estos términos, es evidencia directa de
# actividad legislativa real (no solo cobertura mediática). Se aplica un
# boost multiplicativo a las categorías que ya matchearon.
SEÑALES_LEGISLATIVAS = [
    "iniciativa", "proyecto de decreto", "punto de acuerdo",
    "dictamen", "reforma constitucional", "minuta",
    "proyecto de ley", "proposición con punto de acuerdo",
    "gaceta parlamentaria", "diario oficial",
]


def clasificar_texto(titulo, resumen="", comision=None):
    """
    Clasifica un texto en las 17 categorías legislativas.
    Retorna dict {categoria: score} ordenado por relevancia.

    El score combina:
    - Coincidencia directa de keywords (peso alto)
    - Coincidencia parcial / tokens compartidos (peso bajo)
    - Bonus por keywords en título vs resumen
    - Filtro de relevancia México (penaliza artículos internacionales)
    - Boost por señales de instrumentos legislativos
    - Boost por comisión dictaminadora (si se conoce)
    """
    # Filtro 1: Excluir deportes y entretenimiento
    if _es_contexto_no_legislativo(titulo, resumen):
        return {}

    # Filtro 2: Relevancia México (penaliza artículos internacionales)
    relevancia = calcular_relevancia_mexico(titulo, resumen)
    if relevancia == 0.0:
        return {}

    tokens_titulo = normalizar_texto(titulo)
    tokens_resumen = normalizar_texto(resumen)
    tokens_todo = tokens_titulo + tokens_resumen

    if not tokens_todo:
        return {}

    tf_titulo = calcular_tf(tokens_titulo)
    tf_resumen = calcular_tf(tokens_resumen)

    # Detección de señales legislativas (una vez para todo el texto)
    texto_completo = f"{titulo} {resumen}".lower()
    hits_legislativos = sum(1 for señal in SEÑALES_LEGISLATIVAS if señal in texto_completo)

    scores = {}

    for cat_clave, cat_config in CATEGORIAS.items():
        keywords = obtener_keywords_categoria(cat_clave)
        score = 0.0

        for keyword in keywords:
            # normalizar_texto filtra stopwords ("de", "y", "la"...) y tokens
            # ≤2 chars. Así, "Ley de Aguas Nacionales" → ["ley", "aguas", "nacional"].
            # Esto es intencional: queremos matchear por los tokens significativos.
            kw_tokens = normalizar_texto(keyword)
            if not kw_tokens:
                continue

            # Keywords de 1 token significativo: match directo permitido
            # Keywords compuestas (≥2 tokens significativos): SOLO cuentan si
            # TODOS los tokens significativos están presentes. Antes cada
            # token sumaba score independientemente, lo que generaba falsos
            # positivos:
            #   "inversión pública" → token "publica" matcheaba
            #   "instituciones públicas" aunque "inversion" no estuviera.
            if len(kw_tokens) == 1:
                tok = kw_tokens[0]
                if tok in tf_titulo:
                    score += tf_titulo[tok] * 3.0
                if tok in tf_resumen:
                    score += tf_resumen[tok] * 1.0
            else:
                # Compuesta: requiere todos los tokens significativos
                en_titulo = all(t in tf_titulo for t in kw_tokens)
                en_resumen = all(t in tf_resumen for t in kw_tokens)
                if en_titulo:
                    score += sum(tf_titulo[t] for t in kw_tokens) * 3.0
                elif en_resumen:
                    score += sum(tf_resumen[t] for t in kw_tokens) * 1.0
                else:
                    # Mixto título+resumen (ej. "vivienda" en título y
                    # "social" en resumen) — también cuenta pero con peso
                    # intermedio
                    en_mixto = all((t in tf_titulo or t in tf_resumen) for t in kw_tokens)
                    if en_mixto:
                        score += sum(tf_titulo.get(t, 0) + tf_resumen.get(t, 0) for t in kw_tokens) * 1.5

            # Bonus por keyword compuesta encontrada completa.
            # Word boundary con sufijo plural opcional (abr 2026):
            # - \b evita FPs como "Metro" → "Metropolitana" o "presa" → "representa"
            # - (?:s|es)? al final permite que "empleo" matchee "empleos",
            #   "crecimiento" matchee "crecimientos", etc. (plurales castellanos)
            # Las formas irregulares (raras en dominio legislativo) dependen
            # del token match que corre en paralelo con stemming.
            kw_lower = keyword.lower()
            patron = r'\b' + re.escape(kw_lower) + r'(?:s|es)?\b'
            if re.search(patron, titulo.lower()):
                score += 2.0
            if resumen and re.search(patron, resumen.lower()):
                score += 0.8

        # Normalizar score por número de keywords (evitar sesgo por categorías con más keywords)
        # Usamos log2 en vez de sqrt porque sqrt penaliza demasiado a categorías con
        # muchas keywords (ej: seguridad_justicia=148 tras agregar LEYES_FEDERALES)
        score = score / max(math.log2(len(keywords)), 1)

        # Aplicar multiplicador de relevancia México
        score = score * relevancia

        # Boost legislativo: instrumentos legislativos amplifican categorías con match
        # Un artículo con "iniciativa" o "dictamen" es actividad legislativa real,
        # señal mucho más fuerte que cualquier keyword temática
        if hits_legislativos > 0 and score > 0.15:
            boost = min(1.0 + (hits_legislativos * 0.35), 2.5)
            score = score * boost

        if score >= NLP_CONFIG["min_confianza"]:
            scores[cat_clave] = round(score, 4)

    # Boost por comisión dictaminadora: si el documento tiene comisión asignada,
    # la categoría correspondiente recibe un boost fuerte. Si la categoría no tenía
    # score suficiente por keywords, se le asigna un score mínimo garantizado.
    if comision:
        cat_comision = comision_a_categoria(comision)
        if cat_comision:
            if cat_comision in scores:
                # Boost x2 si ya tenía match por keywords
                scores[cat_comision] = round(scores[cat_comision] * 2.0, 4)
            else:
                # Asignar score mínimo garantizado por comisión
                scores[cat_comision] = round(NLP_CONFIG["min_confianza"] * 1.5, 4)

    # Ordenar por score descendente y limitar categorías
    scores_ordenados = dict(
        sorted(scores.items(), key=lambda x: x[1], reverse=True)
        [:NLP_CONFIG["max_categorias"]]
    )

    return scores_ordenados


def detectar_subcategorias(titulo, resumen, cat_clave):
    """
    Para una categoría ya clasificada, identifica qué subcategorías matchean.
    Usa la misma lógica de keyword matching que clasificar_texto pero
    a nivel subcategoría.

    Retorna dict {subcategoria_clave: score} ordenado por score desc.
    Ejemplo: {"crimen_organizado": 0.72, "fuerzas_armadas": 0.35}
    """
    cat_config = CATEGORIAS.get(cat_clave)
    if not cat_config or "subcategorias" not in cat_config:
        return {}

    tokens_titulo = normalizar_texto(titulo)
    tokens_resumen = normalizar_texto(resumen)
    tf_titulo = calcular_tf(tokens_titulo)
    tf_resumen = calcular_tf(tokens_resumen)

    resultados = {}

    for sub_clave, sub_config in cat_config["subcategorias"].items():
        keywords = sub_config["keywords"]
        score = 0.0

        for keyword in keywords:
            kw_tokens = normalizar_texto(keyword)

            for kw_token in kw_tokens:
                if kw_token in tf_titulo:
                    score += tf_titulo[kw_token] * 3.0
                if kw_token in tf_resumen:
                    score += tf_resumen[kw_token] * 1.0

            # Bonus por keyword compuesta encontrada completa
            kw_lower = keyword.lower()
            if len(kw_lower) <= 4:
                patron = r'\b' + re.escape(kw_lower) + r'\b'
                if re.search(patron, titulo.lower()):
                    score += 2.0
                if resumen and re.search(patron, resumen.lower()):
                    score += 0.8
            else:
                if kw_lower in titulo.lower():
                    score += 2.0
                if resumen and kw_lower in resumen.lower():
                    score += 0.8

        # Normalizar por número de keywords de esta subcategoría
        if keywords:
            score = score / math.sqrt(len(keywords))

        if score > 0.1:  # Umbral bajo: sólo necesita alguna señal
            resultados[sub_clave] = round(score, 4)

    # Ordenar por score descendente
    return dict(sorted(resultados.items(), key=lambda x: x[1], reverse=True))


def clasificar_y_etiquetar(articulo):
    """
    Clasifica un artículo y retorna las categorías como string separado por comas.
    Formato para almacenar en BD: "seguridad_justicia:0.85,economia_hacienda:0.42"
    """
    categorias = clasificar_texto(
        articulo.get("titulo", ""),
        articulo.get("resumen", ""),
        comision=articulo.get("comision"),
    )

    if not categorias:
        return ""

    return ",".join(f"{cat}:{score}" for cat, score in categorias.items())


def clasificar_batch(articulos):
    """
    Clasifica un lote de artículos.
    Retorna lista de tuplas (articulo, categorias_str).
    """
    resultados = []
    for art in articulos:
        cats = clasificar_y_etiquetar(art)
        resultados.append((art, cats))
    return resultados


def actualizar_categorias_en_db():
    """
    Recorre artículos sin categorizar en la BD y les asigna categorías.
    """
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # Artículos sin categoría o con categoría vacía
    sin_clasificar = conn.execute("""
        SELECT id, titulo, resumen FROM articulos
        WHERE categorias IS NULL OR categorias = ''
    """).fetchall()

    logger.info(f"Artículos por clasificar: {len(sin_clasificar)}")
    clasificados = 0

    for row in sin_clasificar:
        categorias = clasificar_y_etiquetar(dict(row))
        if categorias:
            conn.execute(
                "UPDATE articulos SET categorias = ? WHERE id = ?",
                (categorias, row["id"]),
            )
            clasificados += 1

    conn.commit()
    logger.info(f"Clasificados: {clasificados}/{len(sin_clasificar)}")

    # ── Clasificar documentos de Gaceta (usando comisión como boost) ──
    # Agregar columna categorias si no existe
    try:
        conn.execute("ALTER TABLE gaceta ADD COLUMN categorias TEXT DEFAULT ''")
        conn.commit()
        logger.info("Columna 'categorias' agregada a tabla gaceta")
    except Exception:
        pass  # Ya existe

    gaceta_sin = conn.execute("""
        SELECT id, titulo, resumen, comision FROM gaceta
        WHERE categorias IS NULL OR categorias = ''
    """).fetchall()
    logger.info(f"Gaceta por clasificar: {len(gaceta_sin)}")

    gaceta_ok = 0
    for row in gaceta_sin:
        d = dict(row)
        cats = clasificar_y_etiquetar(d)
        if cats:
            conn.execute("UPDATE gaceta SET categorias = ? WHERE id = ?", (cats, d["id"]))
            gaceta_ok += 1

    conn.commit()
    logger.info(f"Gaceta clasificados: {gaceta_ok}/{len(gaceta_sin)}")

    # ── Reclasificar artículos que ahora serían filtrados por mejoras en filtros ──
    # Revisa artículos ya clasificados que deberían ser excluidos por
    # _es_contexto_no_legislativo() o calcular_relevancia_mexico()
    articulos_existentes = conn.execute("""
        SELECT id, titulo, resumen FROM articulos
        WHERE categorias IS NOT NULL AND categorias != ''
          AND fecha >= date('now', '-90 days')
    """).fetchall()

    reclasificados_art = 0
    for row in articulos_existentes:
        d = dict(row)
        titulo = d.get("titulo", "")
        resumen = d.get("resumen", "")

        # Verificar si ahora sería excluido
        if _es_contexto_no_legislativo(titulo, resumen):
            conn.execute("UPDATE articulos SET categorias = '' WHERE id = ?", (d["id"],))
            reclasificados_art += 1
            continue

        relevancia = calcular_relevancia_mexico(titulo, resumen)
        if relevancia <= 0.3:
            conn.execute("UPDATE articulos SET categorias = '' WHERE id = ?", (d["id"],))
            reclasificados_art += 1

    if reclasificados_art > 0:
        conn.commit()
        logger.info(f"Artículos reclasificados (filtros mejorados): {reclasificados_art}")

    # ── Reclasificar gaceta con ley identificable (corrige errores históricos) ──
    try:
        from config import LEYES_FEDERALES
        from api.predictor_autoria import extraer_ley_de_titulo

        gaceta_todos = conn.execute("""
            SELECT id, titulo, categorias FROM gaceta
            WHERE categorias IS NOT NULL AND categorias != ''
        """).fetchall()

        reclasificados = 0
        for row in gaceta_todos:
            d = dict(row)
            ley = extraer_ley_de_titulo(d["titulo"])
            if not ley:
                continue

            # Buscar la ley en LEYES_FEDERALES (fuzzy: quitar artículos)
            import re
            ley_norm = re.sub(r'\b(de|del|la|las|los|el|para|sobre|en)\b', '', ley.lower()).strip()
            ley_norm = re.sub(r'\s+', ' ', ley_norm)

            cat_correcta = None
            for nombre_ley, cat in LEYES_FEDERALES.items():
                nombre_norm = re.sub(r'\b(de|del|la|las|los|el|para|sobre|en)\b', '', nombre_ley.lower()).strip()
                nombre_norm = re.sub(r'\s+', ' ', nombre_norm)
                if ley_norm == nombre_norm or ley_norm in nombre_norm or nombre_norm in ley_norm:
                    cat_correcta = cat
                    break

            if not cat_correcta:
                continue

            # Verificar si la categoría actual es diferente
            cat_actual = d["categorias"].split(":")[0] if d["categorias"] else ""
            if cat_actual != cat_correcta:
                nueva_cat = f"{cat_correcta}:0.75"
                conn.execute("UPDATE gaceta SET categorias = ? WHERE id = ?", (nueva_cat, d["id"]))
                reclasificados += 1

        conn.commit()
        if reclasificados > 0:
            logger.info(f"Gaceta reclasificados por ley: {reclasificados}")
    except Exception as e:
        logger.warning(f"Error reclasificando gaceta por ley: {e}")

    # ── Reclasificar SIL documentos sin categoría ──
    sil_sin = conn.execute("""
        SELECT id, titulo, sinopsis, comision FROM sil_documentos
        WHERE categoria IS NULL OR categoria = ''
    """).fetchall()
    logger.info(f"SIL por clasificar: {len(sil_sin)}")

    sil_ok = 0
    for row in sil_sin:
        d = dict(row)
        cats = clasificar_texto(
            d.get("titulo", ""),
            d.get("sinopsis", ""),
            comision=d.get("comision"),
        )
        if cats:
            # SIL usa categoría simple (la de mayor score)
            cat_principal = list(cats.keys())[0]
            conn.execute("UPDATE sil_documentos SET categoria = ? WHERE id = ?", (cat_principal, d["id"]))
            sil_ok += 1

    conn.commit()
    logger.info(f"SIL clasificados: {sil_ok}/{len(sil_sin)}")

    return clasificados + gaceta_ok + sil_ok


def obtener_distribucion_categorias(dias=7):
    """
    Retorna la distribución de categorías en artículos recientes.
    Útil para el dashboard.
    """
    from datetime import timedelta

    conn = get_connection()

    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    rows = conn.execute("""
        SELECT categorias FROM articulos
        WHERE fecha >= ? AND categorias IS NOT NULL AND categorias != ''
    """, (fecha_limite,)).fetchall()

    distribucion = Counter()
    for row in rows:
        cats = row[0].split(",")
        for cat_score in cats:
            if ":" in cat_score:
                cat = cat_score.split(":")[0]
                distribucion[cat] += 1

    return dict(distribucion)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=== Clasificador NLP ===\n")

    # Pruebas con textos de ejemplo
    pruebas = [
        {
            "titulo": "Guardia Nacional asegura 500 kg de fentanilo en Sinaloa",
            "resumen": "En operativo conjunto con la Fiscalía, la Guardia Nacional decomisó droga con valor de millones de pesos. El crimen organizado sigue operando en la región.",
        },
        {
            "titulo": "SAT recauda 15% más por ISR en primer trimestre",
            "resumen": "La recaudación fiscal aumentó gracias a nuevos mecanismos de fiscalización. El presupuesto federal contempla mayor inversión pública.",
        },
        {
            "titulo": "Pemex reporta pérdidas millonarias en refinería Dos Bocas",
            "resumen": "La empresa productiva del estado enfrenta problemas operativos. CFE también reporta incremento en subsidio energético.",
        },
        {
            "titulo": "INE aprueba presupuesto para elecciones intermedias",
            "resumen": "El Instituto Nacional Electoral definió el calendario electoral. Los partidos Morena y PAN debatieron sobre la reforma electoral.",
        },
        {
            "titulo": "Migración: récord de deportaciones desde Estados Unidos",
            "resumen": "La frontera norte registra niveles sin precedentes de migrantes deportados. CNDH denuncia violación a derechos humanos de refugiados.",
        },
    ]

    for prueba in pruebas:
        print(f"Título: {prueba['titulo']}")
        cats = clasificar_texto(prueba["titulo"], prueba["resumen"])
        for cat, score in cats.items():
            nombre = CATEGORIAS[cat]["nombre"]
            print(f"  → {nombre}: {score}")
        print()
