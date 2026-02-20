"""
Semáforo Legislativo - Orquestador Principal
Ejecuta el pipeline completo:
  1. Scraping de medios (RSS)
  2. Scraping de Gaceta Parlamentaria
  3. Scraping de Google Trends
  4. Clasificación NLP de artículos
  5. Cálculo de scores (semáforo)
  6. Análisis de correlación temporal
  7. Exportación JSON para dashboard
"""

import json
import logging
import argparse
import time
from datetime import datetime
from pathlib import Path

from config import LOGGING, DATABASE, CATEGORIAS, SCORING
from scrapers.medios import scrape_todos_medios, obtener_articulos_recientes
from scrapers.medios_html import scrape_todos_html
from scrapers.gaceta import scrape_gaceta_rango
from scrapers.trends import scrape_trends_todas_categorias
from scrapers.sil import (
    scrape_sil_completo,
    obtener_stats_por_partido,
    obtener_serie_temporal_sil,
    obtener_conteo_sil,
)
from scrapers.mananera import scrape_mananeras
from nlp.clasificador import actualizar_categorias_en_db, obtener_distribucion_categorias
from api.correlacion import (
    calcular_todos_los_scores,
    obtener_scores_actuales,
    obtener_alertas_recientes,
    obtener_historial_scores_todas,
    generar_reporte,
)
from api.lag import analizar_todas_categorias, obtener_prediccion
from api.predictor_autoria import predecir_autores, obtener_estadisticas_autoria
from nlp.geo_clasificador import obtener_mapa_datos

logger = logging.getLogger("semaforo")


def invertir_nombre(nombre):
    """
    Invierte nombres del formato SITL 'Apellido1 Apellido2 Nombre(s)'
    al formato natural 'Nombre(s) Apellido1 Apellido2'.

    Heuristica:
    - Si tiene 3 palabras: asume 2 apellidos + 1 nombre
    - Si tiene 4+ palabras: asume 2 apellidos + resto nombres
    - Maneja apellidos compuestos con particulas (De, Del, De La, De Los, etc.)
    """
    if not nombre or not nombre.strip():
        return nombre

    partes = nombre.strip().split()
    if len(partes) <= 2:
        return nombre  # No se puede determinar, dejarlo como esta

    # Detectar particulas de apellido compuesto al inicio
    # Ej: "De La Cruz Garcia Maria" -> apellidos = "De La Cruz Garcia", nombre = "Maria"
    particulas = {"de", "del", "la", "las", "los", "el", "san", "santa", "van", "von"}

    # Contar cuantas palabras son apellidos (minimo 2, pero puede haber particulas)
    idx_apellido_fin = 0
    i = 0
    apellidos_contados = 0
    while i < len(partes) and apellidos_contados < 2:
        if partes[i].lower() in particulas:
            # Es particula, avanzar sin contar como apellido completo
            i += 1
            continue
        else:
            apellidos_contados += 1
            i += 1
            idx_apellido_fin = i

    # Si consumimos todo, no hay nombre para invertir
    if idx_apellido_fin >= len(partes):
        return nombre

    apellidos = " ".join(partes[:idx_apellido_fin])
    nombres = " ".join(partes[idx_apellido_fin:])

    return f"{nombres} {apellidos}"

# Directorio raíz del proyecto
ROOT = Path(__file__).resolve().parent
DASHBOARD_DATA = ROOT / "dashboard" / "data.json"


def setup_logging():
    """Configura logging según config.py."""
    from logging.handlers import RotatingFileHandler

    log_path = ROOT / LOGGING["archivo"]

    handler_file = RotatingFileHandler(
        str(log_path),
        maxBytes=LOGGING["max_bytes"],
        backupCount=LOGGING["backup_count"],
    )
    handler_console = logging.StreamHandler()

    formatter = logging.Formatter(LOGGING["formato"])
    handler_file.setFormatter(formatter)
    handler_console.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, LOGGING["nivel"]))
    root_logger.addHandler(handler_file)
    root_logger.addHandler(handler_console)


def paso_1_scraping_medios():
    """Paso 1: Scrapear RSS + HTML de los 14 medios."""
    logger.info("=" * 60)
    logger.info("PASO 1: Scraping de medios (RSS + HTML)")
    logger.info("=" * 60)

    inicio = time.time()

    # 1a. RSS (medios que funcionan)
    resultados_rss = scrape_todos_medios()
    total_rss = sum(r["nuevos"] for r in resultados_rss.values())

    # 1b. HTML directo (medios con RSS roto)
    resultados_html = scrape_todos_html()
    total_html = sum(r["nuevos"] for r in resultados_html.values())

    duracion = time.time() - inicio

    resultados = {**resultados_rss, **resultados_html}
    total_nuevos = total_rss + total_html
    total_obtenidos = sum(r["obtenidos"] for r in resultados.values())

    logger.info(f"Medios: {total_obtenidos} obtenidos ({total_rss} RSS + {total_html} HTML), {total_nuevos} nuevos ({duracion:.1f}s)")
    return resultados


def paso_2_scraping_gaceta(dias=7):
    """Paso 2: Scrapear Gaceta Parlamentaria."""
    logger.info("=" * 60)
    logger.info("PASO 2: Scraping de Gaceta Parlamentaria")
    logger.info("=" * 60)

    inicio = time.time()
    documentos = scrape_gaceta_rango(dias=dias)
    duracion = time.time() - inicio

    logger.info(f"Gaceta: {len(documentos)} documentos nuevos ({duracion:.1f}s)")
    return documentos


def paso_2b_scraping_mananera():
    """Paso 2b: Scrapear conferencias matutinas de la Presidenta."""
    logger.info("=" * 60)
    logger.info("PASO 2b: Scraping de Conferencias Matutinas CSP")
    logger.info("=" * 60)

    inicio = time.time()
    try:
        resultado = scrape_mananeras(dias=14)
        duracion = time.time() - inicio
        logger.info(
            f"Mañaneras: {resultado['conferencias']} conferencias, "
            f"{resultado['menciones']} menciones ({duracion:.1f}s)"
        )
    except Exception as e:
        logger.warning(f"Mañaneras falló (no crítico): {e}")
        resultado = {"conferencias": 0, "menciones": 0}

    return resultado


def paso_3_scraping_trends():
    """Paso 3: Consultar Google Trends para las 12 categorías."""
    logger.info("=" * 60)
    logger.info("PASO 3: Scraping de Google Trends")
    logger.info("=" * 60)

    inicio = time.time()
    resumen = scrape_trends_todas_categorias()
    duracion = time.time() - inicio

    total_registros = sum(r["registros_nuevos"] for r in resumen.values())
    logger.info(f"Trends: {total_registros} registros nuevos ({duracion:.1f}s)")
    return resumen


def paso_4_clasificacion_nlp():
    """Paso 4: Clasificar artículos sin categoría."""
    logger.info("=" * 60)
    logger.info("PASO 4: Clasificación NLP")
    logger.info("=" * 60)

    inicio = time.time()
    clasificados = actualizar_categorias_en_db()
    duracion = time.time() - inicio

    logger.info(f"NLP: {clasificados} artículos clasificados ({duracion:.1f}s)")
    return clasificados


def paso_5_scoring():
    """Paso 5: Calcular scores del semáforo."""
    logger.info("=" * 60)
    logger.info("PASO 5: Cálculo de Scores")
    logger.info("=" * 60)

    inicio = time.time()
    scores = calcular_todos_los_scores()
    duracion = time.time() - inicio

    verdes = sum(1 for s in scores if s["color"] == "verde")
    amarillos = sum(1 for s in scores if s["color"] == "amarillo")
    rojos = sum(1 for s in scores if s["color"] == "rojo")

    logger.info(f"Scores: {verdes} verdes, {amarillos} amarillos, {rojos} rojos ({duracion:.1f}s)")
    return scores


def paso_6_correlacion_temporal():
    """Paso 6: Análisis de correlación temporal."""
    logger.info("=" * 60)
    logger.info("PASO 6: Análisis de Correlación Temporal")
    logger.info("=" * 60)

    inicio = time.time()
    correlaciones = analizar_todas_categorias()
    duracion = time.time() - inicio

    sig_count = 0
    for c in correlaciones:
        xcorr = c.get("xcorr_medios_congreso", {})
        if isinstance(xcorr, dict) and xcorr.get("p_value_optimo", 1) < 0.05:
            sig_count += 1

    logger.info(f"Correlaciones: {sig_count}/12 significativas ({duracion:.1f}s)")
    return correlaciones


def obtener_fuentes_por_categoria():
    """
    Extrae artículos de medios y documentos de Gaceta agrupados por categoría.
    Esto es lo que hace al dashboard transparente y verificable.
    """
    import sqlite3
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    fuentes = {}
    for cat_clave, cat_config in CATEGORIAS.items():
        keywords = cat_config["keywords"]

        # Artículos de medios que coinciden con esta categoría
        articulos = []
        rows = conn.execute("""
            SELECT fuente, titulo, url, fecha, categorias FROM articulos
            WHERE categorias LIKE ?
            ORDER BY fecha DESC LIMIT 15
        """, (f"%{cat_clave}%",)).fetchall()
        for r in rows:
            articulos.append({
                "fuente": r["fuente"],
                "titulo": r["titulo"][:150],
                "url": r["url"] or "",
                "fecha": r["fecha"][:10] if r["fecha"] else "",
            })

        # Documentos de Gaceta que coinciden
        gaceta_docs = []
        condiciones = " OR ".join(
            "(titulo LIKE ? OR resumen LIKE ? OR comision LIKE ?)"
            for _ in keywords[:5]
        )
        params = []
        for kw in keywords[:5]:
            params.extend([f"%{kw}%", f"%{kw}%", f"%{kw}%"])

        if condiciones:
            rows = conn.execute(f"""
                SELECT tipo, titulo, autor, comision, fecha, url,
                       COALESCE(url_pdf, '') as url_pdf,
                       COALESCE(numero_doc, '') as numero_doc
                FROM gaceta
                WHERE {condiciones}
                ORDER BY fecha DESC LIMIT 10
            """, params).fetchall()
            for r in rows:
                gaceta_docs.append({
                    "tipo": r["tipo"],
                    "titulo": r["titulo"][:150],
                    "autor": r["autor"][:80] if r["autor"] else "",
                    "comision": r["comision"][:80] if r["comision"] else "",
                    "fecha": r["fecha"][:10] if r["fecha"] else "",
                    "url": r["url"] or "",
                    "url_pdf": r["url_pdf"] or "",
                    "numero_doc": r["numero_doc"] or "",
                })

        # Google Trends: top keywords y sus valores
        trends_data = []
        rows = conn.execute("""
            SELECT keyword, ROUND(AVG(valor), 1) as promedio
            FROM trends
            WHERE categoria = ?
            GROUP BY keyword
            ORDER BY promedio DESC
        """, (cat_clave,)).fetchall()
        for r in rows:
            trends_data.append({
                "keyword": r["keyword"],
                "interes_promedio": r["promedio"],
            })

        # Menciones de CSP en conferencias matutinas
        menciones_csp = []
        try:
            rows_csp = conn.execute("""
                SELECT fecha, fragmento, url FROM mananera
                WHERE categoria = ?
                ORDER BY fecha DESC LIMIT 10
            """, (cat_clave,)).fetchall()
            for r in rows_csp:
                menciones_csp.append({
                    "fecha": r["fecha"],
                    "fragmento": r["fragmento"],
                    "url": r["url"],
                })
        except sqlite3.OperationalError:
            pass  # Tabla no existe aún

        fuentes[cat_clave] = {
            "articulos_medios": articulos,
            "documentos_gaceta": gaceta_docs,
            "google_trends": trends_data,
            "menciones_csp": menciones_csp,
        }

    conn.close()
    return fuentes


def paso_7_exportar_dashboard():
    """Paso 7: Exportar datos JSON para el dashboard."""
    logger.info("=" * 60)
    logger.info("PASO 7: Exportación para Dashboard")
    logger.info("=" * 60)

    scores = obtener_scores_actuales()
    alertas = obtener_alertas_recientes(50)
    distribucion = obtener_distribucion_categorias(dias=7)
    fuentes = obtener_fuentes_por_categoria()

    # Predicciones por categoría
    predicciones = {}
    for cat_clave in CATEGORIAS:
        predicciones[cat_clave] = obtener_prediccion(cat_clave)

    # Series temporales por categoría (para gráficas de línea)
    # Usamos 540 días para capturar datos desde septiembre 2024
    series_temporales = {}
    for cat_clave in CATEGORIAS:
        series_temporales[cat_clave] = obtener_serie_temporal_sil(cat_clave, dias=540)

    # Estadísticas por partido político
    try:
        partidos = obtener_stats_por_partido(dias=540)
    except Exception as e:
        logger.warning(f"Error obteniendo stats por partido: {e}")
        partidos = {}

    # Conteo SIL
    try:
        sil_stats = obtener_conteo_sil()
    except Exception:
        sil_stats = {"total": 0, "con_partido": 0, "con_fecha": 0}

    # Historial de scores para gráfica temporal (tipo Polymarket)
    historial_scores = obtener_historial_scores_todas()

    # Predicciones de autoría legislativa (¿quién presenta?)
    autoria = {}
    try:
        for cat_clave in CATEGORIAS:
            preds = predecir_autores(cat_clave, top_n=5)
            if preds:
                autoria[cat_clave] = [
                    {
                        "nombre": invertir_nombre(p["nombre"]),
                        "partido": p["partido"],
                        "camara": p["camara"],
                        "estado": p["estado"] or "",
                        "score": p["score_total"],
                        "docs_categoria": p["docs_en_categoria"],
                        "comisiones_afines": p["comisiones_afines"],
                        "desglose": p["desglose"],
                    }
                    for p in preds
                ]
        autoria_stats = obtener_estadisticas_autoria()
    except Exception as e:
        logger.warning(f"Error en predicciones de autoría: {e}")
        autoria_stats = {}

    data = {
        "metadata": {
            "generado": datetime.now().isoformat(),
            "version": "3.0",
            "formula": "SCORE = (0.35*Media) + (0.25*Trends) + (0.30*Congreso) + (0.10*Urgencia)",
            "umbrales": SCORING["umbrales"],
            "sil_docs": sil_stats.get("total", 0),
        },
        "semaforo": [],
        "alertas": alertas,
        "distribucion_categorias": distribucion,
        "predicciones": predicciones,
        "fuentes": fuentes,
        "historial_scores": historial_scores,
        "series_temporales": series_temporales,
        "partidos": partidos,
        "autoria": autoria,
        "autoria_stats": autoria_stats,
        "mapa": obtener_mapa_datos(),
    }

    # Construir datos del semáforo con nombres
    for score in scores:
        cat_clave = score.get("categoria", "")
        cat_config = CATEGORIAS.get(cat_clave, {})
        data["semaforo"].append({
            "categoria": cat_clave,
            "nombre": cat_config.get("nombre", cat_clave),
            "score_total": score.get("score_total", 0),
            "score_media": score.get("score_media", 0),
            "score_trends": score.get("score_trends", 0),
            "score_congreso": score.get("score_congreso", 0),
            "score_urgencia": score.get("score_urgencia", 0),
            "color": score.get("color", "rojo"),
            "fecha": score.get("fecha", ""),
        })

    # Escribir JSON
    DASHBOARD_DATA.parent.mkdir(parents=True, exist_ok=True)
    with open(DASHBOARD_DATA, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Dashboard JSON exportado: {DASHBOARD_DATA}")
    return data


def ejecutar_pipeline_completo(skip_trends=False, dias_gaceta=7):
    """Ejecuta el pipeline completo de 7 pasos."""
    inicio_total = time.time()

    logger.info("*" * 60)
    logger.info("  SEMÁFORO LEGISLATIVO - Pipeline Completo")
    logger.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("*" * 60)

    # Pasos secuenciales
    paso_1_scraping_medios()
    paso_2_scraping_gaceta(dias=dias_gaceta)
    paso_2b_scraping_mananera()

    if not skip_trends:
        paso_3_scraping_trends()
    else:
        logger.info("PASO 3: Google Trends OMITIDO (--skip-trends)")

    # Paso 3b: SIL (incremental — solo documentos nuevos)
    logger.info("=" * 60)
    logger.info("PASO 3b: Scraping del SIL (incremental)")
    logger.info("=" * 60)
    try:
        inicio_sil = time.time()
        sil_result = scrape_sil_completo(fecha_desde="2024-09-01", detalle_max=50)
        dur_sil = time.time() - inicio_sil
        logger.info(f"SIL: {sil_result['nuevos']} nuevos ({dur_sil:.1f}s)")
    except Exception as e:
        logger.warning(f"SIL falló (no crítico): {e}")

    paso_4_clasificacion_nlp()
    scores = paso_5_scoring()
    paso_6_correlacion_temporal()
    data = paso_7_exportar_dashboard()

    duracion_total = time.time() - inicio_total

    # Reporte final
    print("\n")
    print(generar_reporte())
    print(f"\nTiempo total: {duracion_total:.1f}s")
    print(f"Dashboard: {DASHBOARD_DATA}")

    return data


def ejecutar_solo_scoring():
    """Ejecuta solo el cálculo de scores (sin scraping)."""
    logger.info("Modo: Solo scoring (sin scraping)")
    paso_4_clasificacion_nlp()
    paso_5_scoring()
    paso_7_exportar_dashboard()
    print(generar_reporte())


def main():
    parser = argparse.ArgumentParser(
        description="Semáforo Legislativo - Sistema Predictivo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python main.py                    # Pipeline completo
  python main.py --solo-scoring     # Solo recalcular scores
  python main.py --skip-trends      # Omitir Google Trends
  python main.py --dias-gaceta 3    # Solo últimos 3 días de Gaceta
  python main.py --reporte          # Mostrar reporte actual
        """,
    )

    parser.add_argument(
        "--solo-scoring",
        action="store_true",
        help="Solo recalcular scores sin hacer scraping",
    )
    parser.add_argument(
        "--skip-trends",
        action="store_true",
        help="Omitir scraping de Google Trends (lento, rate-limited)",
    )
    parser.add_argument(
        "--dias-gaceta",
        type=int,
        default=7,
        help="Días a scrapear de la Gaceta (default: 7)",
    )
    parser.add_argument(
        "--reporte",
        action="store_true",
        help="Mostrar reporte del estado actual sin ejecutar pipeline",
    )

    args = parser.parse_args()
    setup_logging()

    if args.reporte:
        scores = obtener_scores_actuales()
        if scores:
            print(generar_reporte())
        else:
            print("No hay scores calculados. Ejecuta el pipeline primero.")
        return

    if args.solo_scoring:
        ejecutar_solo_scoring()
    else:
        ejecutar_pipeline_completo(
            skip_trends=args.skip_trends,
            dias_gaceta=args.dias_gaceta,
        )


if __name__ == "__main__":
    main()
