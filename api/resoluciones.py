"""
Resoluciones — Tracking de precisión predictiva del Semáforo Legislativo.

Compara predicciones semanales del semáforo contra actividad legislativa real
registrada en el SIL. Genera un % de acierto por semana y por categoría.

Lógica:
  - Para cada categoría, cada semana:
    1. score_promedio >= 50 → predecimos actividad "alta"
    2. docs_reales > promedio_historico → hubo actividad real "alta"
    3. Si ambas coinciden → acierto; si no → sobreestimación o subestimación
"""

import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CATEGORIAS, SCORING, DATABASE

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent


def init_db_resoluciones():
    """Crea tabla de resoluciones si no existe."""
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS resoluciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            semana TEXT NOT NULL,
            categoria TEXT NOT NULL,
            score_promedio REAL,
            color_prediccion TEXT,
            docs_reales INTEGER,
            promedio_historico REAL,
            acierto INTEGER,
            tipo_resultado TEXT,
            fecha_calculo TEXT NOT NULL,
            UNIQUE(semana, categoria)
        )
    """)
    conn.commit()
    return conn


def _lunes_de_semana(fecha):
    """Retorna el lunes de la semana de `fecha`."""
    dias_desde_lunes = fecha.weekday()  # 0=lunes
    return fecha - timedelta(days=dias_desde_lunes)


def _semana_iso(fecha):
    """Retorna string de semana ISO: '2026-W08'."""
    iso = fecha.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _rango_semana(lunes):
    """Retorna (lunes_str, domingo_str) para una semana."""
    domingo = lunes + timedelta(days=6)
    return lunes.strftime("%Y-%m-%d"), domingo.strftime("%Y-%m-%d")


def _asignar_color(score):
    """Asigna color basado en umbrales."""
    umbrales = SCORING.get("umbrales", {"verde": 70, "amarillo": 40})
    if score >= umbrales["verde"]:
        return "verde"
    elif score >= umbrales["amarillo"]:
        return "amarillo"
    return "rojo"


def _calcular_promedios_historicos(conn):
    """
    Calcula el promedio semanal de docs SIL por categoría sobre todo el historial.
    Retorna dict: {categoria: promedio_semanal}
    """
    promedios = {}
    for cat_clave in CATEGORIAS:
        rows = conn.execute("""
            SELECT strftime('%Y-%W', fecha_presentacion) as semana, COUNT(*) as n
            FROM sil_documentos
            WHERE categoria = ?
              AND fecha_presentacion IS NOT NULL
              AND fecha_presentacion != ''
            GROUP BY semana
        """, (cat_clave,)).fetchall()

        if rows:
            total_docs = sum(r[1] for r in rows)
            n_semanas = len(rows)
            promedios[cat_clave] = total_docs / n_semanas
        else:
            promedios[cat_clave] = 0.0

    return promedios


def calcular_resoluciones_semanales():
    """
    Calcula resoluciones para todas las semanas completas con datos de scores.
    Compara predicción (score) vs realidad (docs SIL) por categoría.

    Retorna número de semanas procesadas.
    """
    conn = init_db_resoluciones()
    conn.row_factory = sqlite3.Row

    # 1. Encontrar rango de fechas con scores
    rango = conn.execute("""
        SELECT MIN(fecha) as primera, MAX(fecha) as ultima
        FROM scores
        WHERE fecha IS NOT NULL
    """).fetchone()

    if not rango or not rango["primera"]:
        logger.info("No hay scores para calcular resoluciones")
        conn.close()
        return 0

    fecha_primera = datetime.strptime(rango["primera"], "%Y-%m-%d")
    fecha_ultima = datetime.strptime(rango["ultima"], "%Y-%m-%d")

    # 2. Calcular promedios históricos (baseline)
    promedios = _calcular_promedios_historicos(conn)

    # 3. Iterar semanas completas
    lunes_inicio = _lunes_de_semana(fecha_primera)
    lunes_actual = lunes_inicio
    hoy = datetime.now()
    semanas_procesadas = 0

    while lunes_actual + timedelta(days=6) <= hoy:
        fecha_lunes, fecha_domingo = _rango_semana(lunes_actual)
        semana_str = _semana_iso(lunes_actual)

        # Verificar que hay scores esa semana
        n_scores = conn.execute("""
            SELECT COUNT(DISTINCT fecha) FROM scores
            WHERE fecha BETWEEN ? AND ?
        """, (fecha_lunes, fecha_domingo)).fetchone()[0]

        if n_scores == 0:
            lunes_actual += timedelta(days=7)
            continue

        # Calcular resolución para cada categoría
        for cat_clave in CATEGORIAS:
            # Score promedio esa semana
            row_score = conn.execute("""
                SELECT AVG(score_total) as avg_score
                FROM scores
                WHERE categoria = ? AND fecha BETWEEN ? AND ?
            """, (cat_clave, fecha_lunes, fecha_domingo)).fetchone()

            score_prom = row_score["avg_score"] if row_score["avg_score"] else 0.0
            color = _asignar_color(score_prom)

            # Docs reales esa semana
            row_docs = conn.execute("""
                SELECT COUNT(*) as n
                FROM sil_documentos
                WHERE categoria = ?
                  AND fecha_presentacion BETWEEN ? AND ?
                  AND fecha_presentacion IS NOT NULL
                  AND fecha_presentacion != ''
            """, (cat_clave, fecha_lunes, fecha_domingo)).fetchone()

            docs_reales = row_docs["n"] if row_docs else 0
            prom_hist = promedios.get(cat_clave, 0.0)

            # Determinar acierto
            prediccion_alta = score_prom >= 50
            actividad_alta = docs_reales > prom_hist

            if prediccion_alta and actividad_alta:
                acierto, tipo = 1, "acierto"
            elif not prediccion_alta and not actividad_alta:
                acierto, tipo = 1, "acierto"
            elif prediccion_alta and not actividad_alta:
                acierto, tipo = 0, "sobreestimacion"
            else:
                acierto, tipo = 0, "subestimacion"

            # Guardar
            conn.execute("""
                INSERT OR REPLACE INTO resoluciones
                    (semana, categoria, score_promedio, color_prediccion,
                     docs_reales, promedio_historico, acierto, tipo_resultado,
                     fecha_calculo)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                semana_str, cat_clave, round(score_prom, 2), color,
                docs_reales, round(prom_hist, 2), acierto, tipo,
                datetime.now().isoformat(),
            ))

        conn.commit()
        semanas_procesadas += 1
        lunes_actual += timedelta(days=7)

    conn.close()
    logger.info(f"Resoluciones calculadas: {semanas_procesadas} semanas")
    return semanas_procesadas


def obtener_resoluciones(semanas=12):
    """
    Retorna datos de resoluciones para el dashboard.
    """
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Semanas disponibles (más recientes primero, luego invertir)
    semanas_rows = conn.execute("""
        SELECT DISTINCT semana FROM resoluciones
        ORDER BY semana DESC LIMIT ?
    """, (semanas,)).fetchall()

    result = {"accuracy_global": 0, "semanas": []}

    total_aciertos = 0
    total_evaluados = 0

    # Invertir para orden cronológico
    for sem_row in reversed(semanas_rows):
        semana = sem_row["semana"]
        rows = conn.execute("""
            SELECT * FROM resoluciones
            WHERE semana = ?
            ORDER BY score_promedio DESC
        """, (semana,)).fetchall()

        detalle = []
        aciertos_sem = 0

        for r in rows:
            cat_config = CATEGORIAS.get(r["categoria"], {})
            detalle.append({
                "categoria": r["categoria"],
                "nombre": cat_config.get("nombre", r["categoria"]),
                "score_promedio": r["score_promedio"],
                "color": r["color_prediccion"],
                "docs_reales": r["docs_reales"],
                "promedio_historico": r["promedio_historico"],
                "acierto": bool(r["acierto"]),
                "tipo": r["tipo_resultado"],
            })
            if r["acierto"]:
                aciertos_sem += 1

        total_cats = len(rows)
        accuracy = (aciertos_sem / total_cats * 100) if total_cats > 0 else 0
        total_aciertos += aciertos_sem
        total_evaluados += total_cats

        # Calcular fecha inicio de la semana ISO
        try:
            year_str, week_str = semana.split("-W")
            year = int(year_str)
            week = int(week_str)
            # Lunes de esa semana ISO
            fecha_inicio = datetime.strptime(f"{year} {week} 1", "%G %V %u").strftime("%Y-%m-%d")
        except Exception:
            fecha_inicio = ""

        result["semanas"].append({
            "semana": semana,
            "fecha_inicio": fecha_inicio,
            "accuracy": round(accuracy, 1),
            "aciertos": aciertos_sem,
            "total": total_cats,
            "detalle": detalle,
        })

    result["accuracy_global"] = round(
        (total_aciertos / total_evaluados * 100) if total_evaluados > 0 else 0, 1
    )

    conn.close()
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    n = calcular_resoluciones_semanales()
    print(f"\nSemanas procesadas: {n}")
    data = obtener_resoluciones()
    print(f"Accuracy global: {data['accuracy_global']}%")
    for s in data["semanas"]:
        print(f"  {s['semana']}: {s['accuracy']}% ({s['aciertos']}/{s['total']})")
