"""
Motor de Scoring - Semáforo Legislativo
Calcula: SCORE = (0.30×Media) + (0.20×Trends) + (0.25×Congreso) + (0.15×Mañanera) + (0.10×Urgencia)
Asigna color: Verde ≥70 | Amarillo 40-69 | Rojo <40
"""

import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CATEGORIAS, SCORING, URGENCIA, DATABASE
from scrapers.medios import obtener_score_media
from scrapers.gaceta import obtener_score_congreso
from scrapers.trends import obtener_score_trends
from scrapers.mananera import obtener_score_mananera

logger = logging.getLogger(__name__)


def init_db():
    """Crea tablas de scores y alertas."""
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            categoria TEXT NOT NULL,
            score_total REAL NOT NULL,
            score_media REAL,
            score_trends REAL,
            score_congreso REAL,
            score_mananera REAL,
            score_urgencia REAL,
            color TEXT NOT NULL,
            fecha TEXT NOT NULL,
            detalle TEXT,
            UNIQUE(categoria, fecha)
        )
    """)
    # Migración: agregar columna score_mananera si no existe
    try:
        conn.execute("ALTER TABLE scores ADD COLUMN score_mananera REAL DEFAULT 0")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Columna ya existe
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alertas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            categoria TEXT NOT NULL,
            tipo_alerta TEXT NOT NULL,
            score REAL NOT NULL,
            color TEXT NOT NULL,
            mensaje TEXT,
            fecha TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def calcular_factor_urgencia():
    """
    Calcula el factor de urgencia basado en el calendario legislativo.
    Períodos ordinarios: Sep-Dic (1er), Feb-Abr (2do)
    """
    hoy = datetime.now()
    mes_dia = hoy.strftime("%m-%d")

    # Verificar si estamos en período ordinario
    for periodo in URGENCIA["periodos_ordinarios"]:
        if periodo["inicio"] <= mes_dia <= periodo["fin"]:
            factor = URGENCIA["periodo_ordinario"]
            logger.debug(f"Período ordinario activo, factor: {factor}")
            return factor

    # Receso
    factor = URGENCIA["receso"]
    logger.debug(f"Período de receso, factor: {factor}")
    return factor


def calcular_score_urgencia_historica(categoria_clave, score_media, score_trends):
    """
    Score de urgencia 0-100 basado en EVIDENCIA HISTÓRICA.

    Solo indica urgencia si hay correlación empírica entre cobertura
    mediática y actividad legislativa real para esta categoría.

    Componentes:
    1. Correlación histórica medios→congreso (¿la presión mediática
       se traduce en legislación para este tema?)
    2. Velocidad de presentación en SIL (¿se están presentando
       instrumentos legislativos recientemente?)
    3. Factor calendario (período ordinario vs receso)

    Si no hay correlación histórica → urgencia baja (sin evidencia).
    """
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    urgencia = 0.0

    # ── Componente 1: Correlación histórica (peso 40%) ──
    # ¿La presión mediática ha generado legislación antes?
    corr_row = conn.execute("""
        SELECT coeficiente, significativo, lag_optimo
        FROM correlaciones
        WHERE categoria = ? AND tipo_analisis = 'xcorr_medios_congreso'
        ORDER BY fecha_analisis DESC LIMIT 1
    """, (categoria_clave,)).fetchone()

    corr_score = 0.0
    if corr_row and corr_row["significativo"]:
        # Correlación significativa → evidencia fuerte
        r = abs(corr_row["coeficiente"])
        corr_score = min(r * 150, 100)  # r=0.67 → 100
    elif corr_row:
        # Correlación no significativa → evidencia débil
        r = abs(corr_row["coeficiente"])
        corr_score = min(r * 50, 30)  # Tope de 30 sin significancia

    # ── Componente 2: Actividad reciente en SIL (peso 40%) ──
    # ¿Se están presentando instrumentos legislativos ahora?
    sil_row = conn.execute("""
        SELECT COUNT(*) as total FROM sil_documentos
        WHERE categoria = ?
        AND fecha_presentacion >= date('now', '-14 days')
    """, (categoria_clave,)).fetchone()

    sil_row_mes = conn.execute("""
        SELECT COUNT(*) as total FROM sil_documentos
        WHERE categoria = ?
        AND fecha_presentacion >= date('now', '-60 days')
    """, (categoria_clave,)).fetchone()

    sil_reciente = sil_row["total"] if sil_row else 0
    sil_mes = sil_row_mes["total"] if sil_row_mes else 0

    # Calcular aceleración: ¿más actividad reciente que el promedio?
    if sil_mes > 0:
        promedio_quincenal = (sil_mes / 4)  # promedio por quincena en 60 días
        if promedio_quincenal > 0:
            aceleracion = sil_reciente / promedio_quincenal
            sil_score = min(aceleracion * 40, 100)  # 2.5x aceleración → 100
        else:
            sil_score = 0
    else:
        sil_score = 0

    # ── Componente 3: Calendario legislativo (peso 20%) ──
    factor_cal = calcular_factor_urgencia()
    # Solo amplifica si hay presión real (media + trends)
    presion_real = (score_media * 0.6 + score_trends * 0.4)
    calendario_score = 0.0
    if factor_cal >= 1.5 and presion_real > 50:
        calendario_score = min(presion_real * 0.8, 100)
    elif factor_cal >= 1.0:
        calendario_score = min(presion_real * 0.4, 50)
    # En receso: calendario_score permanece en 0

    # ── Score final ponderado ──
    urgencia = (
        corr_score * 0.40 +
        sil_score * 0.40 +
        calendario_score * 0.20
    )

    conn.close()

    return min(round(urgencia, 2), 100)


def asignar_color(score):
    """Asigna color del semáforo según umbrales."""
    if score >= SCORING["umbrales"]["verde"]:
        return "verde"
    elif score >= SCORING["umbrales"]["amarillo"]:
        return "amarillo"
    else:
        return "rojo"


def calcular_score_categoria(categoria_clave):
    """
    Calcula el score completo para una categoría.
    SCORE = (0.30×Media) + (0.20×Trends) + (0.25×Congreso) + (0.15×Mañanera) + (0.10×Urgencia)
    """
    cat_config = CATEGORIAS[categoria_clave]
    keywords = cat_config["keywords"]
    pesos = SCORING["pesos"]

    # Componente 1: Presión mediática (0.30)
    score_media = obtener_score_media(keywords)

    # Componente 2: Google Trends (0.20)
    score_trends = obtener_score_trends(categoria_clave)

    # Componente 3: Actividad en Congreso (0.25)
    score_congreso = obtener_score_congreso(keywords)

    # Componente 4: Mención de la Presidenta CSP (0.15)
    score_mananera = obtener_score_mananera(categoria_clave)

    # Componente 5: Urgencia basada en evidencia histórica (0.10)
    score_urgencia = calcular_score_urgencia_historica(
        categoria_clave, score_media, score_trends
    )

    # Fórmula principal
    score_total = (
        pesos["media"] * score_media
        + pesos["trends"] * score_trends
        + pesos["congreso"] * score_congreso
        + pesos["mananera"] * score_mananera
        + pesos["urgencia"] * score_urgencia
    )

    score_total = min(round(score_total, 2), 100)
    color = asignar_color(score_total)

    resultado = {
        "categoria": categoria_clave,
        "nombre": cat_config["nombre"],
        "score_total": score_total,
        "score_media": score_media,
        "score_trends": score_trends,
        "score_congreso": score_congreso,
        "score_mananera": score_mananera,
        "score_urgencia": score_urgencia,
        "color": color,
        "factor_calendario": calcular_factor_urgencia(),
        "fecha": datetime.now().strftime("%Y-%m-%d"),
    }

    logger.info(
        f"[{color.upper():8s}] {cat_config['nombre']:30s} "
        f"Score: {score_total:6.2f} "
        f"(M:{score_media:.1f} T:{score_trends:.1f} C:{score_congreso:.1f} "
        f"CSP:{score_mananera:.1f} U:{score_urgencia:.1f})"
    )

    return resultado


def calcular_todos_los_scores():
    """
    Calcula scores para las 12 categorías.
    Almacena en BD y genera alertas.
    """
    conn = init_db()
    resultados = []
    fecha_hoy = datetime.now().strftime("%Y-%m-%d")

    for cat_clave in CATEGORIAS:
        resultado = calcular_score_categoria(cat_clave)
        resultados.append(resultado)

        # Guardar score en BD
        try:
            conn.execute("""
                INSERT INTO scores
                    (categoria, score_total, score_media, score_trends,
                     score_congreso, score_mananera, score_urgencia,
                     color, fecha, detalle)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                resultado["categoria"],
                resultado["score_total"],
                resultado["score_media"],
                resultado["score_trends"],
                resultado["score_congreso"],
                resultado["score_mananera"],
                resultado["score_urgencia"],
                resultado["color"],
                resultado["fecha"],
                f"cal:{resultado['factor_calendario']}",
            ))
        except sqlite3.IntegrityError:
            # Actualizar si ya existe para hoy
            conn.execute("""
                UPDATE scores
                SET score_total=?, score_media=?, score_trends=?,
                    score_congreso=?, score_mananera=?, score_urgencia=?,
                    color=?, detalle=?
                WHERE categoria=? AND fecha=?
            """, (
                resultado["score_total"],
                resultado["score_media"],
                resultado["score_trends"],
                resultado["score_congreso"],
                resultado["score_mananera"],
                resultado["score_urgencia"],
                resultado["color"],
                f"cal:{resultado['factor_calendario']}",
                resultado["categoria"],
                resultado["fecha"],
            ))

        # Generar alerta si score es verde (alta probabilidad)
        if resultado["color"] == "verde":
            conn.execute("""
                INSERT INTO alertas (categoria, tipo_alerta, score, color, mensaje, fecha)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                resultado["categoria"],
                "score_alto",
                resultado["score_total"],
                resultado["color"],
                f"ALERTA: {resultado['nombre']} con score {resultado['score_total']:.1f} - "
                f"Alta probabilidad de actividad legislativa",
                datetime.now().isoformat(),
            ))

    conn.commit()
    conn.close()

    # Ordenar por score descendente
    resultados.sort(key=lambda x: x["score_total"], reverse=True)
    return resultados


def obtener_scores_actuales():
    """Recupera los scores más recientes de la BD."""
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT s.* FROM scores s
        INNER JOIN (
            SELECT categoria, MAX(fecha) as max_fecha
            FROM scores GROUP BY categoria
        ) latest ON s.categoria = latest.categoria AND s.fecha = latest.max_fecha
        ORDER BY s.score_total DESC
    """).fetchall()

    conn.close()
    return [dict(r) for r in rows]


def obtener_historial_scores(categoria, dias=30):
    """Recupera historial de scores para una categoría."""
    from datetime import timedelta

    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    rows = conn.execute("""
        SELECT * FROM scores
        WHERE categoria = ? AND fecha >= ?
        ORDER BY fecha
    """, (categoria, fecha_limite)).fetchall()

    conn.close()
    return [dict(r) for r in rows]


def obtener_alertas_recientes(limite=20):
    """Recupera las alertas más recientes."""
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT * FROM alertas ORDER BY fecha DESC LIMIT ?
    """, (limite,)).fetchall()

    conn.close()
    return [dict(r) for r in rows]


def obtener_historial_scores_todas(dias=180):
    """
    Recupera historial de scores para TODAS las categorías.
    Formato optimizado para gráfica temporal tipo Polymarket.

    Retorna:
        {
            "fechas": ["2026-02-13", "2026-02-14", ...],
            "categorias": {
                "seguridad_justicia": {
                    "nombre": "Seguridad y Justicia",
                    "scores": [77.5, 78.2, ...],   // alineados con fechas
                    "colores": ["verde", "verde", ...]
                },
                ...
            }
        }
    """
    from datetime import timedelta

    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    # Obtener todas las fechas disponibles
    fechas_rows = conn.execute("""
        SELECT DISTINCT fecha FROM scores
        WHERE fecha >= ?
        ORDER BY fecha
    """, (fecha_limite,)).fetchall()

    fechas = [r["fecha"] for r in fechas_rows]

    # Obtener todos los scores
    rows = conn.execute("""
        SELECT categoria, fecha, score_total, color
        FROM scores
        WHERE fecha >= ?
        ORDER BY fecha
    """, (fecha_limite,)).fetchall()

    conn.close()

    # Organizar por categoría
    por_cat = {}
    for r in rows:
        cat = r["categoria"]
        if cat not in por_cat:
            por_cat[cat] = {}
        por_cat[cat][r["fecha"]] = {
            "score": r["score_total"],
            "color": r["color"],
        }

    # Construir estructura final alineada con fechas
    categorias = {}
    for cat_clave, cat_config in CATEGORIAS.items():
        scores_alineados = []
        colores_alineados = []
        for f in fechas:
            dato = por_cat.get(cat_clave, {}).get(f)
            if dato:
                scores_alineados.append(round(dato["score"], 1))
                colores_alineados.append(dato["color"])
            else:
                scores_alineados.append(None)
                colores_alineados.append(None)

        categorias[cat_clave] = {
            "nombre": cat_config["nombre"],
            "scores": scores_alineados,
            "colores": colores_alineados,
        }

    return {
        "fechas": fechas,
        "categorias": categorias,
    }


def generar_reporte():
    """Genera un reporte en texto del estado actual del semáforo."""
    resultados = calcular_todos_los_scores()

    lineas = [
        "=" * 70,
        f"  SEMÁFORO LEGISLATIVO - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "=" * 70,
        "",
    ]

    iconos = {"verde": "🟢", "amarillo": "🟡", "rojo": "🔴"}

    for r in resultados:
        icono = iconos.get(r["color"], "⚪")
        lineas.append(
            f"  {icono} {r['nombre']:30s}  {r['score_total']:6.2f}  "
            f"[M:{r['score_media']:5.1f} T:{r['score_trends']:5.1f} "
            f"C:{r['score_congreso']:5.1f} CSP:{r.get('score_mananera',0):5.1f} "
            f"U:{r['score_urgencia']:5.1f}]"
        )

    lineas.extend([
        "",
        "-" * 70,
        f"  Fórmula: SCORE = (0.30×Media) + (0.20×Trends) + (0.25×Congreso) + (0.15×Mañanera) + (0.10×Urgencia)",
        f"  Verde ≥70 | Amarillo 40-69 | Rojo <40",
        "=" * 70,
    ])

    return "\n".join(lineas)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(generar_reporte())
