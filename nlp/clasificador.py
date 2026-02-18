"""
Clasificador NLP por 12 categorías legislativas
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
from config import CATEGORIAS, NLP_CONFIG, DATABASE, KEYWORDS_NEGATIVOS, KEYWORDS_MEXICO

logger = logging.getLogger(__name__)

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

    return tokens


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


def clasificar_texto(titulo, resumen=""):
    """
    Clasifica un texto en las 12 categorías legislativas.
    Retorna dict {categoria: score} ordenado por relevancia.

    El score combina:
    - Coincidencia directa de keywords (peso alto)
    - Coincidencia parcial / tokens compartidos (peso bajo)
    - Bonus por keywords en título vs resumen
    - Filtro de relevancia México (penaliza artículos internacionales)
    """
    # Filtro de relevancia México (sobre texto crudo)
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

    scores = {}

    for cat_clave, cat_config in CATEGORIAS.items():
        keywords = cat_config["keywords"]
        score = 0.0

        for keyword in keywords:
            kw_tokens = normalizar_texto(keyword)

            for kw_token in kw_tokens:
                # Coincidencia en título (peso 3x)
                if kw_token in tf_titulo:
                    score += tf_titulo[kw_token] * 3.0

                # Coincidencia en resumen (peso 1x)
                if kw_token in tf_resumen:
                    score += tf_resumen[kw_token] * 1.0

            # Bonus por keyword compuesta encontrada completa
            kw_lower = keyword.lower()
            if kw_lower in titulo.lower():
                score += 2.0
            if resumen and kw_lower in resumen.lower():
                score += 0.8

        # Normalizar score por número de keywords (evitar sesgo por categorías con más keywords)
        score = score / math.sqrt(len(keywords))

        # Aplicar multiplicador de relevancia México
        score = score * relevancia

        if score >= NLP_CONFIG["min_confianza"]:
            scores[cat_clave] = round(score, 4)

    # Ordenar por score descendente y limitar categorías
    scores_ordenados = dict(
        sorted(scores.items(), key=lambda x: x[1], reverse=True)
        [:NLP_CONFIG["max_categorias"]]
    )

    return scores_ordenados


def clasificar_y_etiquetar(articulo):
    """
    Clasifica un artículo y retorna las categorías como string separado por comas.
    Formato para almacenar en BD: "seguridad_justicia:0.85,economia_hacienda:0.42"
    """
    categorias = clasificar_texto(
        articulo.get("titulo", ""),
        articulo.get("resumen", ""),
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
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
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
    conn.close()
    logger.info(f"Clasificados: {clasificados}/{len(sin_clasificar)}")
    return clasificados


def obtener_distribucion_categorias(dias=7):
    """
    Retorna la distribución de categorías en artículos recientes.
    Útil para el dashboard.
    """
    from datetime import timedelta

    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))

    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    rows = conn.execute("""
        SELECT categorias FROM articulos
        WHERE fecha >= ? AND categorias IS NOT NULL AND categorias != ''
    """, (fecha_limite,)).fetchall()

    conn.close()

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
