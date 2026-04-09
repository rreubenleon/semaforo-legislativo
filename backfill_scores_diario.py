"""
Backfill DIARIO de scores (no semanal) para el gap de datos Mar 12 - Abr 7.

A diferencia de backfill_scores.py (que calcula semanalmente y cae a cero
porque faltan componentes), este script:

1. Calcula scores DIARIOS (uno por día x categoría)
2. REDISTRIBUYE los pesos de los componentes faltantes (media, trends)
   entre los componentes disponibles (congreso, mañanera, urgencia)
3. Así los scores reflejan la realidad de lo que SÍ sucedió
   (actividad legislativa + conferencias presidenciales)
   sin caer a cero artificialmente.

Pesos normales: media 0.20 + trends 0.15 + congreso 0.25 + mañanera 0.10 + urgencia 0.15 + dominancia 0.15
Pesos gap:      media 0.00 + trends 0.00 + congreso 0.50 + mañanera 0.20 + urgencia 0.30 + dominancia 0.00
                                         (redistribuidos proporcionalmente)

Uso:
    BACKFILL_DESDE=2026-03-13 BACKFILL_HASTA=2026-04-07 python3 backfill_scores_diario.py
"""
import logging
import math
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# Pesos redistribuidos para el periodo de gap
# (media y trends = 0 porque los datos se perdieron para siempre)
PESOS_GAP = {
    "media": 0.00,
    "trends": 0.00,
    "congreso": 0.50,
    "mananera": 0.20,
    "urgencia": 0.30,
}


def main():
    from config import CATEGORIAS, obtener_keywords_categoria
    from db import get_connection

    desde_str = os.environ.get("BACKFILL_DESDE", "2026-03-13")
    hasta_str = os.environ.get("BACKFILL_HASTA", "2026-04-07")

    desde = datetime.strptime(desde_str, "%Y-%m-%d")
    hasta = datetime.strptime(hasta_str, "%Y-%m-%d")

    logger.info(f"=== Backfill scores diario: {desde_str} → {hasta_str} ===")

    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # ─────────────────────────────────────────────
    # Pre-cargar datos en memoria
    # ─────────────────────────────────────────────
    logger.info("Cargando Gaceta...")
    gaceta_raw = conn.execute("""
        SELECT id, titulo, resumen, comision, fecha
        FROM gaceta
        WHERE fecha >= ? AND fecha <= ?
    """, (
        (desde - timedelta(days=10)).strftime("%Y-%m-%d"),
        hasta.strftime("%Y-%m-%d"),
    )).fetchall()
    logger.info(f"  {len(gaceta_raw)} docs de gaceta en ventana")

    logger.info("Cargando SIL...")
    sil_raw = conn.execute("""
        SELECT id, titulo, sinopsis, categoria, fecha_presentacion
        FROM sil_documentos
        WHERE fecha_presentacion >= ? AND fecha_presentacion <= ?
    """, (
        (desde - timedelta(days=65)).strftime("%Y-%m-%d"),
        hasta.strftime("%Y-%m-%d"),
    )).fetchall()
    logger.info(f"  {len(sil_raw)} docs del SIL en ventana")

    logger.info("Cargando mañaneras...")
    mananera_raw = conn.execute("""
        SELECT fecha, categoria, fragmento
        FROM mananera
        WHERE fecha >= ? AND fecha <= ?
    """, (
        (desde - timedelta(days=10)).strftime("%Y-%m-%d"),
        hasta.strftime("%Y-%m-%d"),
    )).fetchall()
    logger.info(f"  {len(mananera_raw)} menciones en conferencias")

    # ─────────────────────────────────────────────
    # Indexar por fecha
    # ─────────────────────────────────────────────
    gaceta_por_dia = defaultdict(list)
    for g in gaceta_raw:
        fecha_str = str(g["fecha"])[:10]
        gaceta_por_dia[fecha_str].append({
            "titulo": (g["titulo"] or "").lower(),
            "resumen": (g["resumen"] or "").lower(),
            "comision": (g["comision"] or "").lower(),
        })

    sil_por_cat_dia = defaultdict(lambda: defaultdict(int))
    for s in sil_raw:
        cat = s["categoria"]
        fecha_str = str(s["fecha_presentacion"])[:10]
        if cat:
            sil_por_cat_dia[cat][fecha_str] += 1

    # Mañaneras: contar menciones por categoría x día
    mananera_por_cat_dia = defaultdict(lambda: defaultdict(int))
    for m in mananera_raw:
        fecha_str = str(m["fecha"])[:10]
        mananera_por_cat_dia[m["categoria"]][fecha_str] += 1

    # Keywords por categoría
    keywords_por_cat = {
        cat: [kw.lower() for kw in obtener_keywords_categoria(cat)]
        for cat in CATEGORIAS
    }

    # ─────────────────────────────────────────────
    # Calcular scores día por día
    # ─────────────────────────────────────────────
    total_insertados = 0
    fecha_actual = desde
    while fecha_actual <= hasta:
        fecha_str = fecha_actual.strftime("%Y-%m-%d")

        # Ventana de 7 días hacia atrás para congreso
        ventana_7 = [
            (fecha_actual - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(7)
        ]
        # Ventana de 14 días para urgencia
        ventana_14 = [
            (fecha_actual - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(14)
        ]
        # Ventana de 60 días para baseline urgencia
        ventana_60 = [
            (fecha_actual - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(60)
        ]

        total_gaceta_ventana = sum(
            len(gaceta_por_dia.get(d, [])) for d in ventana_7
        )

        for cat_clave in CATEGORIAS:
            keywords = keywords_por_cat[cat_clave]

            # ── score_congreso (ventana de 7 días) ──
            docs_relevantes = 0
            for d in ventana_7:
                for doc in gaceta_por_dia.get(d, []):
                    if any(kw in doc["titulo"] or kw in doc["resumen"]
                           or kw in doc["comision"] for kw in keywords):
                        docs_relevantes += 1
            # Bonus: actividad SIL en últimos 7 días
            sil_7 = sum(sil_por_cat_dia.get(cat_clave, {}).get(d, 0)
                        for d in ventana_7)

            # Score congreso: combinar gaceta + SIL
            if total_gaceta_ventana > 0:
                ratio_gaceta = min(docs_relevantes / max(total_gaceta_ventana * 0.1, 1), 1.0)
                score_gaceta = ratio_gaceta * 100
            else:
                score_gaceta = 0
            score_sil = min(sil_7 * 15, 100)  # 15 pts por cada doc SIL
            score_congreso = min(score_gaceta * 0.6 + score_sil * 0.4, 100)

            # ── score_mananera (ventana de 3 días para recency) ──
            ventana_3 = [
                (fecha_actual - timedelta(days=i)).strftime("%Y-%m-%d")
                for i in range(3)
            ]
            menciones_3 = sum(
                mananera_por_cat_dia.get(cat_clave, {}).get(d, 0)
                for d in ventana_3
            )
            # Cada mención = 25 puntos, máximo 100
            score_mananera = min(menciones_3 * 25, 100)

            # ── score_urgencia (SIL aceleración) ──
            sil_14 = sum(sil_por_cat_dia.get(cat_clave, {}).get(d, 0)
                         for d in ventana_14)
            sil_60 = sum(sil_por_cat_dia.get(cat_clave, {}).get(d, 0)
                         for d in ventana_60)
            if sil_60 > 0:
                promedio_quincenal = sil_60 / 4
                if promedio_quincenal > 0:
                    aceleracion = sil_14 / promedio_quincenal
                    sil_score = min(aceleracion * 40, 100)
                else:
                    sil_score = 0
            else:
                sil_score = 0
            score_urgencia = sil_score

            # ── score_total con pesos REDISTRIBUIDOS ──
            score_total = (
                PESOS_GAP["congreso"] * score_congreso
                + PESOS_GAP["mananera"] * score_mananera
                + PESOS_GAP["urgencia"] * score_urgencia
            )
            score_total = min(round(score_total, 2), 100)

            # Color
            if score_total >= 70:
                color = "verde"
            elif score_total >= 40:
                color = "amarillo"
            else:
                color = "rojo"

            # Insertar o actualizar
            try:
                conn.execute("""
                    INSERT INTO scores
                        (categoria, score_total, score_media, score_trends,
                         score_congreso, score_mananera, score_urgencia,
                         color, fecha, detalle)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(categoria, fecha) DO UPDATE SET
                        score_total = excluded.score_total,
                        score_media = excluded.score_media,
                        score_trends = excluded.score_trends,
                        score_congreso = excluded.score_congreso,
                        score_mananera = excluded.score_mananera,
                        score_urgencia = excluded.score_urgencia,
                        color = excluded.color,
                        detalle = excluded.detalle
                """, (
                    cat_clave, score_total,
                    0,  # media
                    0,  # trends
                    round(score_congreso, 2),
                    round(score_mananera, 2),
                    round(score_urgencia, 2),
                    color, fecha_str, "gap_redistribuido"
                ))
                total_insertados += 1
            except Exception as e:
                logger.debug(f"  Error {cat_clave} {fecha_str}: {e}")

        conn.commit()
        logger.info(f"  {fecha_str}: {len(CATEGORIAS)} categorías calculadas")
        fecha_actual += timedelta(days=1)

    logger.info(f"\n{'='*60}")
    logger.info(f"BACKFILL SCORES DIARIO COMPLETO")
    logger.info(f"  Scores insertados/actualizados: {total_insertados}")
    logger.info(f"  Días procesados: {(hasta - desde).days + 1}")
    logger.info(f"  Categorías por día: {len(CATEGORIAS)}")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
