"""
Barrido completo del SIL usando queries anchas sin tope de resultados.

Una query genérica como 'proposición' devuelve el universo LXVI completo
(>18k filas, ~11k IDs únicos). Con dos o tres queries anchas deduplicadas
capturamos el 99%+ del catálogo oficial sin necesidad de paginar por fecha.

Fase 1 (este script): ingesta en bloque con tipo inferido por heurística
de título, sin pedir detalle individual. Rápido: 3-4 queries al SIL.

Fase 2 (posterior, vía enriquecer_fechas_sil): recorre los registros sin
fecha_presentacion y pide la ficha individual con rate limit. Se puede
correr en background o por lotes.

Uso:
    python3 scripts/barrido_sil_completo.py           # real
    python3 scripts/barrido_sil_completo.py --dry-run # solo reporta
"""

import logging
import os
import sqlite3
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection, close as close_db
from scrapers.sil import _buscar_ids
from scripts.reconstruir_tipos_sil import clasificar_por_titulo

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("barrido_sil")

# Queries anchas. Cada una captura miles de IDs únicos. La unión asegura
# cobertura. Las cuatro primeras dieron 89% tras el primer barrido; "adiciona",
# "expide" y "exhorta" fueron añadidas para cerrar la brecha contra numeralia.
QUERIES_ANCHAS = [
    "proposición",  # 18k+ filas, 11k IDs únicos
    "iniciativa",   # 9k filas
    "reforma",      # 8k filas
    "decreto",      # 2k filas
    "adiciona",     # captura "Que adiciona..." sin prefijo "reforma"
    "expide",       # captura "Que expide la Ley..."
    "exhorta",      # captura proposiciones "por el/la que exhorta..."
]


def main(dry_run: bool = False):
    conn = get_connection()
    cur = conn.cursor()

    # Cargar IDs existentes
    existentes = set()
    for r in cur.execute("SELECT seguimiento_id, asunto_id FROM sil_documentos"):
        existentes.add((str(r[0]), str(r[1])))
    logger.info(f"IDs ya en BD: {len(existentes)}")

    universo = {}  # (seg, asu) -> dict

    for q in QUERIES_ANCHAS:
        inicio = time.time()
        try:
            rs = _buscar_ids(q, max_resultados=None)
        except Exception as e:
            logger.warning(f"Query '{q}' falló: {e}")
            continue
        dur = time.time() - inicio

        antes = len(universo)
        for r in rs:
            key = (r["seguimiento_id"], r["asunto_id"])
            if key not in universo:
                universo[key] = r
        nuevos_query = len(universo) - antes
        logger.info(
            f"'{q}': {len(rs)} filas en {dur:.1f}s, "
            f"+{nuevos_query} IDs al universo (total={len(universo)})"
        )
        time.sleep(2)

    # Filtrar los que ya están en BD
    por_insertar = {k: v for k, v in universo.items() if k not in existentes}
    logger.info(f"Universo recolectado: {len(universo)}. Nuevos vs BD: {len(por_insertar)}")

    # Conteo preview por heurística
    stats = {"Iniciativa": 0, "Proposición con punto de acuerdo": 0, "Otro": 0, "sin_match": 0}
    for r in por_insertar.values():
        tipo = clasificar_por_titulo(r.get("titulo", ""))
        if not tipo:
            stats["sin_match"] += 1
        elif tipo.startswith("Otro"):
            stats["Otro"] += 1
        else:
            stats[tipo] += 1
    logger.info("Preview de nuevos por heurística:")
    for k, v in stats.items():
        logger.info(f"  {k}: {v}")

    if dry_run:
        logger.info("(dry-run: sin escribir)")
        return

    # Insertar en bloque sin detalle. La fecha, cámara, presentador y
    # partido quedan vacíos; se llenarán luego con enriquecer_fechas_sil().
    insertados = 0
    colisiones = 0
    ahora = datetime.now().isoformat()
    for (seg, asu), r in por_insertar.items():
        tipo = clasificar_por_titulo(r.get("titulo", "")) or r.get("tipo_badge", "")
        try:
            cur.execute(
                """
                INSERT INTO sil_documentos
                    (seguimiento_id, asunto_id, tipo, titulo, sinopsis,
                     camara, fecha_presentacion, legislatura, periodo,
                     estatus, partido, comision, categoria, fecha_scraping)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    seg, asu, tipo, r.get("titulo", "")[:500], r.get("sinopsis", "")[:500],
                    "", "", "LXVI", "",
                    "", "", "", "", ahora,
                ),
            )
            insertados += 1
        except sqlite3.IntegrityError:
            colisiones += 1
        except Exception as e:
            logger.warning(f"insert {seg}/{asu}: {e}")

    conn.commit()
    logger.info(f"Insertados: {insertados}. Colisiones: {colisiones}")

    # Reporte final
    total_ini = cur.execute(
        "SELECT COUNT(*) FROM sil_documentos WHERE tipo='Iniciativa'"
    ).fetchone()[0]
    total_prop = cur.execute(
        "SELECT COUNT(*) FROM sil_documentos WHERE tipo='Proposición con punto de acuerdo'"
    ).fetchone()[0]
    logger.info(f"Total iniciativas en BD post-barrido: {total_ini}")
    logger.info(f"Total proposiciones en BD post-barrido: {total_prop}")

    close_db()


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    main(dry_run=dry)
