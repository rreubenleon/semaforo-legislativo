"""
Predictor de Autoría Legislativa - FIAT
Modelo: dado un evento mediático en categoría X, ¿qué legisladores son más
probables de presentar una iniciativa o punto de acuerdo?

Señales usadas (pesos):
  1. Historial temático (35%)     — ¿cuánto ha legislado sobre este tema?
  2. Comisión relevante (25%)     — ¿está en la comisión del tema?
  3. Contexto geográfico (10%)    — ¿el evento es de su estado?
  4. Velocidad de reacción (15%)  — ¿qué tan rápido reacciona a eventos?
  5. Actividad reciente (15%)     — ¿está activo en esta legislatura?
"""

import logging
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CATEGORIAS, DATABASE

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent

# Pesos del modelo
PESOS = {
    "historial_tematico": 0.35,
    "comision_relevante": 0.25,
    "velocidad_reaccion": 0.15,
    "actividad_reciente": 0.15,
    "contexto_geografico": 0.10,
}

# Mapeo: categoría → comisiones afines (substrings para matching)
COMISIONES_POR_CATEGORIA = {
    "seguridad_justicia": ["Justicia", "Seguridad", "Defensa", "Gobernación"],
    "economia_hacienda": ["Hacienda", "Economía", "Presupuesto", "Comercio"],
    "energia": ["Energía"],
    "salud": ["Salud"],
    "educacion": ["Educación"],
    "trabajo": ["Trabajo", "Previsión Social"],
    "electoral_politico": ["Gobernación", "Puntos Constitucionales", "Reforma Política"],
    "derechos_humanos": ["Derechos Humanos", "Igualdad", "Género"],
    "infraestructura": ["Infraestructura", "Comunicaciones", "Transportes", "Movilidad"],
    "agro_rural": ["Rural", "Agrícola", "Autosuficiencia", "Pesca", "Desarrollo Rural"],
    "relaciones_exteriores": ["Relaciones Exteriores", "América"],
    "anticorrupcion": ["Transparencia", "Anticorrupción"],
    "medio_ambiente": ["Medio Ambiente", "Cambio Climático", "Recursos Naturales"],
    "inteligencia_artificial": ["Ciencia", "Tecnología", "Innovación"],
}


def predecir_autores(categoria, estado_evento=None, top_n=10):
    """
    Predice los legisladores más probables de presentar un instrumento
    legislativo sobre la categoría dada.

    Args:
        categoria: clave de categoría (ej: 'seguridad_justicia')
        estado_evento: estado de México donde ocurrió el evento (opcional)
        top_n: número de resultados a retornar

    Returns:
        Lista de dicts con legislador, score y desglose
    """
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Verificar que la categoría existe
    if categoria not in CATEGORIAS:
        conn.close()
        return []

    # Cargar todos los legisladores con actividad
    legisladores = conn.execute("""
        SELECT id, nombre, camara, partido, estado, distrito,
               comisiones, comisiones_cargo, foto_url
        FROM legisladores
        WHERE nombre != ''
    """).fetchall()

    if not legisladores:
        conn.close()
        return []

    # Pre-calcular estadísticas globales para normalización
    max_docs_cat = conn.execute("""
        SELECT MAX(cnt) FROM (
            SELECT COUNT(*) as cnt FROM actividad_legislador
            WHERE categoria = ?
            GROUP BY legislador_id
        )
    """, (categoria,)).fetchone()[0] or 1

    max_docs_total = conn.execute("""
        SELECT MAX(cnt) FROM (
            SELECT COUNT(*) as cnt FROM actividad_legislador
            GROUP BY legislador_id
        )
    """).fetchone()[0] or 1

    # Actividad reciente (últimos 60 días) — global
    max_reciente = conn.execute("""
        SELECT MAX(cnt) FROM (
            SELECT COUNT(*) as cnt FROM actividad_legislador
            WHERE fecha_presentacion >= date('now', '-60 days')
            GROUP BY legislador_id
        )
    """).fetchone()[0] or 1

    predicciones = []

    for leg in legisladores:
        leg_id = leg["id"]
        scores = {}

        # ── 1. Historial temático (35%) ──
        docs_en_cat = conn.execute("""
            SELECT COUNT(*) FROM actividad_legislador
            WHERE legislador_id = ? AND categoria = ?
        """, (leg_id, categoria)).fetchone()[0]

        scores["historial_tematico"] = min(docs_en_cat / max_docs_cat, 1.0) * 100

        # ── 2. Comisión relevante (25%) ──
        comisiones = leg["comisiones"] or ""
        comisiones_cargo = leg["comisiones_cargo"] or ""
        comisiones_afines = COMISIONES_POR_CATEGORIA.get(categoria, [])

        comision_score = 0
        for com_afin in comisiones_afines:
            com_afin_lower = com_afin.lower()
            if com_afin_lower in comisiones.lower():
                # Verificar si tiene cargo relevante
                if any(f"{com_afin}:President" in cc for cc in comisiones_cargo.split("|")):
                    comision_score = 100  # Presidente de comisión afín
                elif any(f"{com_afin}:Secretar" in cc for cc in comisiones_cargo.split("|")):
                    comision_score = max(comision_score, 80)
                else:
                    comision_score = max(comision_score, 60)  # Integrante

        scores["comision_relevante"] = comision_score

        # ── 3. Velocidad de reacción (15%) ──
        # Promedio de tiempo entre eventos mediáticos y presentaciones
        reacciones = conn.execute("""
            SELECT dias_reaccion FROM reacciones_historicas
            WHERE legislador_id = ? AND categoria = ?
            AND dias_reaccion > 0 AND dias_reaccion <= 90
        """, (leg_id, categoria)).fetchall()

        if reacciones:
            promedio_dias = sum(r["dias_reaccion"] for r in reacciones) / len(reacciones)
            # Menos días = más rápido = mejor score
            # 3 días → 100, 14 días → 50, 30 días → 25, 90 días → 0
            scores["velocidad_reaccion"] = max(0, 100 - (promedio_dias * 100 / 90))
        else:
            # Sin datos de reacción: usar historial como proxy
            scores["velocidad_reaccion"] = min(docs_en_cat * 5, 30)

        # ── 4. Actividad reciente (15%) ──
        docs_recientes = conn.execute("""
            SELECT COUNT(*) FROM actividad_legislador
            WHERE legislador_id = ?
            AND fecha_presentacion >= date('now', '-60 days')
        """, (leg_id,)).fetchone()[0]

        scores["actividad_reciente"] = min(docs_recientes / max_reciente, 1.0) * 100

        # ── 5. Contexto geográfico (10%) ──
        geo_score = 0
        if estado_evento and leg["estado"]:
            if estado_evento.lower() in leg["estado"].lower():
                geo_score = 100
            # Bonus parcial si el legislador es de un estado vecino
            # (simplificado: solo match exacto por ahora)
        scores["contexto_geografico"] = geo_score

        # ── Score final ponderado ──
        score_total = sum(
            scores[k] * PESOS[k]
            for k in PESOS
        )

        # Solo incluir si tiene alguna relevancia
        if score_total > 0:
            predicciones.append({
                "legislador_id": leg_id,
                "nombre": leg["nombre"],
                "camara": leg["camara"],
                "partido": leg["partido"],
                "estado": leg["estado"],
                "foto_url": leg["foto_url"],
                "score_total": round(score_total, 2),
                "desglose": {k: round(v, 1) for k, v in scores.items()},
                "docs_en_categoria": docs_en_cat,
                "docs_recientes": docs_recientes,
                "comisiones_afines": [
                    c for c in comisiones_afines
                    if c.lower() in comisiones.lower()
                ],
            })

    conn.close()

    # Ordenar por score descendente
    predicciones.sort(key=lambda x: x["score_total"], reverse=True)
    return predicciones[:top_n]


def calcular_reacciones_historicas():
    """
    Calcula las reacciones históricas: para cada legislador y categoría,
    mide cuántos días pasaron entre un pico mediático y su siguiente
    presentación legislativa.

    Usa la tabla de articulos (picos mediáticos) y actividad_legislador
    (presentaciones) para calcular el lag.
    """
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Limpiar reacciones anteriores
    conn.execute("DELETE FROM reacciones_historicas")

    # Obtener artículos con categoría y fecha para detectar picos mediáticos
    # El campo 'categorias' puede tener múltiples categorías separadas por coma
    # Las fechas pueden venir como "2026-02-13" o "2026-02-13 07:36:23" o con T
    articulos = conn.execute("""
        SELECT categorias, fecha
        FROM articulos
        WHERE categorias != '' AND categorias IS NOT NULL
          AND fecha != '' AND fecha IS NOT NULL
    """).fetchall()

    # Expandir y contar por (categoría, fecha normalizada a YYYY-MM-DD)
    conteos = defaultdict(lambda: defaultdict(int))
    for art in articulos:
        cats = art["categorias"].split(",")
        # Normalizar fecha a YYYY-MM-DD
        fecha_raw = art["fecha"].replace("T", " ")[:10]
        if len(fecha_raw) < 10:
            continue
        for cat in cats:
            cat = cat.strip()
            if cat:
                conteos[cat][fecha_raw] += 1

    # Filtrar picos (días con ≥2 artículos en una categoría)
    # Umbral bajo porque tenemos pocos días de datos por ahora
    picos_por_cat = defaultdict(list)
    for cat, fechas in conteos.items():
        for fecha, n in sorted(fechas.items()):
            if n >= 2:
                picos_por_cat[cat].append({
                    "fecha": fecha,
                    "intensidad": n,
                })

    # Para cada legislador con actividad
    legisladores_activos = conn.execute("""
        SELECT DISTINCT legislador_id, categoria
        FROM actividad_legislador
        WHERE legislador_id IS NOT NULL AND categoria != ''
    """).fetchall()

    total_reacciones = 0

    for leg_cat in legisladores_activos:
        leg_id = leg_cat["legislador_id"]
        cat = leg_cat["categoria"]

        if cat not in picos_por_cat:
            continue

        # Obtener presentaciones de este legislador en esta categoría
        presentaciones = conn.execute("""
            SELECT fecha_presentacion
            FROM actividad_legislador
            WHERE legislador_id = ? AND categoria = ?
            AND fecha_presentacion IS NOT NULL AND fecha_presentacion != ''
            ORDER BY fecha_presentacion
        """, (leg_id, cat)).fetchall()

        if not presentaciones:
            continue

        fechas_pres = [p["fecha_presentacion"] for p in presentaciones]

        # Para cada pico mediático, buscar la presentación más cercana posterior
        for pico in picos_por_cat[cat]:
            fecha_pico = pico["fecha"]

            for fecha_pres in fechas_pres:
                if fecha_pres >= fecha_pico:
                    try:
                        d_pico = datetime.strptime(fecha_pico, "%Y-%m-%d")
                        d_pres = datetime.strptime(fecha_pres, "%Y-%m-%d")
                        dias = (d_pres - d_pico).days
                    except ValueError:
                        continue

                    if 0 <= dias <= 90:  # Máximo 90 días de ventana
                        conn.execute("""
                            INSERT INTO reacciones_historicas
                                (legislador_id, categoria, evento_fecha,
                                 evento_descripcion, presentacion_fecha,
                                 dias_reaccion, tipo_instrumento, score_media_evento)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            leg_id, cat, fecha_pico,
                            f"Pico mediático: {pico['intensidad']} artículos",
                            fecha_pres, dias, "",
                            min(pico["intensidad"] * 10, 100),
                        ))
                        total_reacciones += 1
                    break  # Solo la primera presentación posterior

    conn.commit()
    conn.close()

    logger.info(f"Reacciones históricas calculadas: {total_reacciones}")
    return total_reacciones


def obtener_ranking_global(top_n=20):
    """
    Ranking de los legisladores más activos en todas las categorías.
    Útil para el dashboard general.
    """
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    ranking = conn.execute("""
        SELECT l.id, l.nombre, l.partido, l.camara, l.estado, l.foto_url,
               COUNT(a.id) as total_instrumentos,
               COUNT(DISTINCT a.categoria) as categorias_activas,
               MAX(a.fecha_presentacion) as ultima_actividad
        FROM legisladores l
        INNER JOIN actividad_legislador a ON a.legislador_id = l.id
        WHERE a.fecha_presentacion >= date('now', '-180 days')
        GROUP BY l.id
        ORDER BY total_instrumentos DESC
        LIMIT ?
    """, (top_n,)).fetchall()

    conn.close()
    return [dict(r) for r in ranking]


def obtener_predicciones_para_dashboard():
    """
    Genera predicciones para todas las categorías activas.
    Retorna dict: {categoria: [top 5 legisladores probables]}
    """
    resultado = {}

    for cat_clave in CATEGORIAS:
        predicciones = predecir_autores(cat_clave, top_n=5)
        if predicciones:
            resultado[cat_clave] = predicciones

    return resultado


def obtener_estadisticas_autoria():
    """Estadísticas generales del módulo de autoría."""
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    stats = {}

    stats["total_legisladores"] = conn.execute(
        "SELECT COUNT(*) FROM legisladores"
    ).fetchone()[0]

    stats["con_actividad"] = conn.execute("""
        SELECT COUNT(DISTINCT legislador_id)
        FROM actividad_legislador
        WHERE legislador_id IS NOT NULL
    """).fetchone()[0]

    stats["total_actividad"] = conn.execute(
        "SELECT COUNT(*) FROM actividad_legislador"
    ).fetchone()[0]

    stats["reacciones_historicas"] = conn.execute(
        "SELECT COUNT(*) FROM reacciones_historicas"
    ).fetchone()[0]

    # Top partidos por actividad
    stats["por_partido"] = [dict(r) for r in conn.execute("""
        SELECT l.partido, COUNT(a.id) as total
        FROM legisladores l
        INNER JOIN actividad_legislador a ON a.legislador_id = l.id
        WHERE l.partido != ''
        GROUP BY l.partido
        ORDER BY total DESC
        LIMIT 10
    """).fetchall()]

    conn.close()
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    print("=== Predictor de Autoría Legislativa ===\n")

    # Test: predecir autores para cada categoría
    for cat in ["seguridad_justicia", "economia_hacienda", "salud", "energia"]:
        print(f"\n{'='*60}")
        print(f"Categoría: {CATEGORIAS[cat]['nombre']}")
        print(f"{'='*60}")

        predicciones = predecir_autores(cat, top_n=5)
        for i, p in enumerate(predicciones, 1):
            print(f"\n  {i}. {p['nombre']} ({p['partido']}) — {p['camara']}")
            print(f"     Estado: {p['estado']}")
            print(f"     Score: {p['score_total']:.1f}")
            print(f"     Docs en categoría: {p['docs_en_categoria']}")
            print(f"     Desglose: {p['desglose']}")
            if p['comisiones_afines']:
                print(f"     Comisiones afines: {', '.join(p['comisiones_afines'])}")
