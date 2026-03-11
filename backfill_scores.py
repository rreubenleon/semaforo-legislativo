"""
Scoring Retrospectivo — Calcula scores históricos semana por semana.

Simula "qué habría calculado el semáforo" en cada semana desde Sep 2024,
usando los datos de artículos y gaceta que ya tenemos en la BD.

Estrategia de eficiencia:
  - Pre-carga TODOS los artículos, gaceta y SIL en memoria (~40K rows, 1 query c/u)
  - Filtra por fecha en Python (no queries por semana×categoría)
  - Total: ~5 queries a Turso en vez de ~2,700

Componentes calculables:
  ✅ score_media   — artículos de medios (Jul 2023+)
  ✅ score_congreso — docs de gaceta (Sep 2024+)
  ✅ score_urgencia — actividad SIL (Aug 2024+)
  ❌ score_trends   — sin datos históricos → 0
  ❌ score_mananera  — sin datos históricos → 0

Fórmula ajustada para retrospectivo:
  Se usan los mismos pesos oficiales pero trends=0, mananera=0.
  Esto da scores más bajos que los actuales, lo cual es honesto
  (refleja que no teníamos todas las señales en ese momento).
"""

import os
import sys
import math
import logging
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    # Configurar conexión a Turso
    from config import CATEGORIAS, SCORING, obtener_keywords_categoria
    from db import get_connection
    import sqlite3

    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # ─────────────────────────────────────────────
    # PASO 1: Pre-cargar todo en memoria
    # ─────────────────────────────────────────────
    logger.info("Cargando artículos de medios...")
    articulos_raw = conn.execute("""
        SELECT id, titulo, resumen, fecha, fuente, peso_fuente, categorias
        FROM articulos
        WHERE fecha IS NOT NULL AND fecha != ''
    """).fetchall()
    logger.info(f"  {len(articulos_raw)} artículos cargados")

    logger.info("Cargando documentos de Gaceta...")
    gaceta_raw = conn.execute("""
        SELECT id, titulo, resumen, comision, fecha, tipo, camara
        FROM gaceta
        WHERE fecha IS NOT NULL AND fecha != ''
    """).fetchall()
    logger.info(f"  {len(gaceta_raw)} docs de gaceta cargados")

    logger.info("Cargando documentos del SIL...")
    sil_raw = conn.execute("""
        SELECT id, titulo, sinopsis, categoria, fecha_presentacion, camara
        FROM sil_documentos
        WHERE fecha_presentacion IS NOT NULL AND fecha_presentacion != ''
    """).fetchall()
    logger.info(f"  {len(sil_raw)} docs del SIL cargados")

    # Verificar scores existentes para no duplicar
    scores_existentes = set()
    for row in conn.execute("SELECT categoria, fecha FROM scores").fetchall():
        scores_existentes.add((row["categoria"], row["fecha"]))
    logger.info(f"  {len(scores_existentes)} scores existentes (se omitirán)")

    # ─────────────────────────────────────────────
    # PASO 2: Indexar datos por fecha para acceso rápido
    # ─────────────────────────────────────────────
    logger.info("Indexando datos por fecha...")

    # Artículos: {fecha_str: [{titulo, resumen, peso, fuente}, ...]}
    articulos_por_dia = defaultdict(list)
    for a in articulos_raw:
        # Normalizar fecha: puede ser "2026-02-13" o "2026-02-13 07:36:23"
        fecha_str = str(a["fecha"])[:10]
        articulos_por_dia[fecha_str].append({
            "titulo": (a["titulo"] or "").lower(),
            "resumen": (a["resumen"] or "").lower(),
            "peso": a["peso_fuente"] or 1.0,
            "fuente": a["fuente"] or "",
        })

    # Gaceta: {fecha_str: [{titulo, resumen, comision, tipo}, ...]}
    gaceta_por_dia = defaultdict(list)
    for g in gaceta_raw:
        fecha_str = str(g["fecha"])[:10]
        gaceta_por_dia[fecha_str].append({
            "titulo": (g["titulo"] or "").lower(),
            "resumen": (g["resumen"] or "").lower(),
            "comision": (g["comision"] or "").lower(),
            "tipo": g["tipo"] or "",
        })

    # SIL: {categoria: {fecha_str: count}}
    sil_por_cat_dia = defaultdict(lambda: defaultdict(int))
    for s in sil_raw:
        cat = s["categoria"]
        fecha_str = str(s["fecha_presentacion"])[:10]
        if cat:
            sil_por_cat_dia[cat][fecha_str] += 1

    # Pre-calcular keywords por categoría
    keywords_por_cat = {}
    for cat_clave in CATEGORIAS:
        keywords_por_cat[cat_clave] = [kw.lower() for kw in obtener_keywords_categoria(cat_clave)]

    # ─────────────────────────────────────────────
    # PASO 3: Calcular scores semana por semana
    # ─────────────────────────────────────────────
    pesos = SCORING["pesos"]
    umbrales = SCORING["umbrales"]

    fecha_inicio = datetime(2024, 9, 1)
    fecha_fin = datetime.now() - timedelta(days=1)  # Hasta ayer
    total_semanas = (fecha_fin - fecha_inicio).days // 7
    total_insertados = 0

    logger.info(f"\nCalculando scores retrospectivos: {fecha_inicio.date()} → {fecha_fin.date()}")
    logger.info(f"Semanas a calcular: {total_semanas}")
    logger.info(f"Categorías: {len(CATEGORIAS)}")
    logger.info(f"Total cálculos: {total_semanas * len(CATEGORIAS)}\n")

    # Periodos ordinarios para factor de urgencia
    periodos_ordinarios = [
        ("09-01", "12-15"),  # Primer periodo
        ("02-01", "04-30"),  # Segundo periodo
    ]

    fecha_actual = fecha_inicio
    semana_num = 0

    while fecha_actual <= fecha_fin:
        semana_num += 1
        fecha_str = fecha_actual.strftime("%Y-%m-%d")

        # Ventana de 7 días hacia atrás
        ventana_inicio = fecha_actual - timedelta(days=7)
        dias_ventana = []
        for d in range(8):  # 7 días + día actual
            dia = ventana_inicio + timedelta(days=d)
            dias_ventana.append(dia.strftime("%Y-%m-%d"))

        # Ventana de 14 días (para urgencia SIL)
        dias_ventana_14 = []
        for d in range(15):
            dia = (fecha_actual - timedelta(days=14)) + timedelta(days=d)
            dias_ventana_14.append(dia.strftime("%Y-%m-%d"))

        # Ventana de 60 días (para urgencia SIL)
        dias_ventana_60 = []
        for d in range(61):
            dia = (fecha_actual - timedelta(days=60)) + timedelta(days=d)
            dias_ventana_60.append(dia.strftime("%Y-%m-%d"))

        # Total de artículos en la ventana (para share)
        total_peso_ventana = 0
        for dia in dias_ventana:
            for a in articulos_por_dia.get(dia, []):
                total_peso_ventana += a["peso"]

        # Total docs gaceta en la ventana
        total_gaceta_ventana = 0
        for dia in dias_ventana:
            total_gaceta_ventana += len(gaceta_por_dia.get(dia, []))

        # Factor calendario
        mes_dia = fecha_actual.strftime("%m-%d")
        en_periodo = any(inicio <= mes_dia <= fin for inicio, fin in periodos_ordinarios)
        factor_cal = 1.5 if en_periodo else 0.5

        nuevos_semana = 0
        for cat_clave in CATEGORIAS:
            # Saltar si ya existe
            if (cat_clave, fecha_str) in scores_existentes:
                continue

            keywords = keywords_por_cat[cat_clave]

            # ── score_media ──
            score_media = _calcular_score_media(
                articulos_por_dia, dias_ventana, keywords,
                total_peso_ventana, len(CATEGORIAS)
            )

            # ── score_congreso ──
            score_congreso = _calcular_score_congreso(
                gaceta_por_dia, dias_ventana, keywords,
                total_gaceta_ventana
            )

            # ── score_urgencia (simplificado) ──
            score_urgencia = _calcular_score_urgencia(
                sil_por_cat_dia, cat_clave, dias_ventana_14,
                dias_ventana_60, score_media, factor_cal
            )

            # ── score_total ──
            score_total = (
                pesos["media"] * score_media
                + pesos["trends"] * 0  # No tenemos datos
                + pesos["congreso"] * score_congreso
                + pesos["mananera"] * 0  # No tenemos datos
                + pesos["urgencia"] * score_urgencia
            )
            score_total = min(round(score_total, 2), 100)

            # Color
            if score_total >= umbrales["verde"]:
                color = "verde"
            elif score_total >= umbrales["amarillo"]:
                color = "amarillo"
            else:
                color = "rojo"

            # Insertar
            try:
                conn.execute("""
                    INSERT INTO scores
                        (categoria, score_total, score_media, score_trends,
                         score_congreso, score_mananera, score_urgencia,
                         color, fecha, detalle)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    cat_clave, score_total, round(score_media, 2), 0,
                    round(score_congreso, 2), 0, round(score_urgencia, 2),
                    color, fecha_str, "retrospectivo"
                ))
                total_insertados += 1
                nuevos_semana += 1
            except Exception:
                pass  # UNIQUE constraint — ya existe

        if nuevos_semana > 0:
            conn.commit()

        # Log progreso cada 4 semanas
        if semana_num % 4 == 0 or fecha_actual + timedelta(days=7) > fecha_fin:
            logger.info(
                f"  Semana {semana_num}/{total_semanas} ({fecha_str}): "
                f"+{nuevos_semana} scores (total: {total_insertados})"
            )

        fecha_actual += timedelta(days=7)

    # Commit final
    conn.commit()

    logger.info(f"\n{'='*60}")
    logger.info(f"SCORING RETROSPECTIVO COMPLETO")
    logger.info(f"  Scores insertados: {total_insertados}")
    logger.info(f"  Semanas procesadas: {semana_num}")
    logger.info(f"  Rango: {fecha_inicio.date()} → {fecha_fin.date()}")
    logger.info(f"{'='*60}")

    return total_insertados


def _calcular_score_media(articulos_por_dia, dias_ventana, keywords, total_peso, n_categorias):
    """
    Replica obtener_score_media() pero usando datos pre-cargados.
    Score 0-100 basado en volumen, concentración, streak y diversidad.
    """
    # Recopilar artículos relevantes
    articulos_match = []
    for dia in dias_ventana:
        for a in articulos_por_dia.get(dia, []):
            # Verificar si algún keyword aparece en titulo o resumen
            if any(kw in a["titulo"] or kw in a["resumen"] for kw in keywords):
                articulos_match.append({**a, "dia": dia})

    if not articulos_match or total_peso == 0:
        return 0.0

    score_acum = sum(a["peso"] for a in articulos_match)

    # Subfactor 1: Volumen/Share (40%)
    share = score_acum / total_peso
    expected_share = 1.0 / n_categorias
    ratio = share / expected_share if expected_share > 0 else 0
    if ratio <= 0:
        vol_score = 0.0
    else:
        vol_score = 50 + 50 * math.tanh(math.log(ratio) / 1.5)
    vol_score = max(0.0, min(100.0, vol_score))

    # Subfactor 2: Concentración temporal (20%)
    dias_con_cobertura = len(set(a["dia"] for a in articulos_match))
    total_dias = len(dias_ventana)
    concentracion = (dias_con_cobertura / total_dias * 100) if total_dias > 0 else 0

    # Subfactor 3: Diversidad de medios (20%)
    fuentes_unicas = len(set(a["fuente"] for a in articulos_match))
    diversidad = min(math.sqrt(fuentes_unicas / 14) * 100, 100)

    # Subfactor 4: Streak reciente (20%) — simplificado
    streak = min(dias_con_cobertura * 15, 100)

    return (vol_score * 0.4 + concentracion * 0.2 + diversidad * 0.2 + streak * 0.2)


def _calcular_score_congreso(gaceta_por_dia, dias_ventana, keywords, total_docs):
    """
    Replica obtener_score_congreso() con datos pre-cargados.
    Score 0-100 basado en proporción de docs relevantes.
    """
    docs_relevantes = 0
    for dia in dias_ventana:
        for doc in gaceta_por_dia.get(dia, []):
            if any(kw in doc["titulo"] or kw in doc["resumen"] or kw in doc["comision"]
                   for kw in keywords):
                docs_relevantes += 1

    if total_docs == 0:
        return 0.0

    ratio = min(docs_relevantes / max(total_docs * 0.1, 1), 1.0)
    return min(round(ratio * 100, 2), 100)


def _calcular_score_urgencia(sil_por_cat_dia, categoria, dias_14, dias_60, score_media, factor_cal):
    """
    Score de urgencia simplificado basado en actividad SIL.
    Sin correlación histórica (no la tenemos para fechas pasadas).
    """
    cat_dias = sil_por_cat_dia.get(categoria, {})

    # Actividad SIL en últimos 14 días
    sil_14 = sum(cat_dias.get(d, 0) for d in dias_14)
    # Actividad SIL en últimos 60 días
    sil_60 = sum(cat_dias.get(d, 0) for d in dias_60)

    # Score SIL basado en aceleración
    if sil_60 > 0:
        promedio_quincenal = sil_60 / 4
        if promedio_quincenal > 0:
            aceleracion = sil_14 / promedio_quincenal
            sil_score = min(aceleracion * 40, 100)
        else:
            sil_score = 0
    else:
        sil_score = 0

    # Score calendario
    calendario_score = 0
    if factor_cal >= 1.5 and score_media > 50:
        calendario_score = min(score_media * 0.8, 100)
    elif factor_cal >= 1.0:
        calendario_score = min(score_media * 0.4, 50)

    # Ponderado: SIL 70% + Calendario 30%
    return sil_score * 0.70 + calendario_score * 0.30


if __name__ == "__main__":
    main()
