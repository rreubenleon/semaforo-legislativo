"""
Backfill de mañaneras para un rango custom de fechas.

Reutiliza la lógica del scraper mananera.py pero permite especificar
desde/hasta en vez de "últimos N días".

Uso:
    BACKFILL_DESDE=2026-03-12 BACKFILL_HASTA=2026-04-08 python backfill_mananeras.py

Recupera versiones estenográficas de gob.mx (las mantiene históricas).
"""
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from scrapers.mananera import (
    CONF_URL_TEMPLATE,
    NUM_A_MES,
    _fetch_robust,
    buscar_menciones_por_categoria,
    extraer_bloques_csp,
    init_db,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def descubrir_conferencias_rango(desde: datetime, hasta: datetime):
    """Descubre conferencias en el rango [desde, hasta] inclusive."""
    conferencias = []
    fecha = desde
    while fecha <= hasta:
        if fecha.weekday() in (5, 6):
            fecha += timedelta(days=1)
            continue

        mes_str = NUM_A_MES.get(fecha.month, "enero")
        url = CONF_URL_TEMPLATE.format(
            dia=fecha.day, mes=mes_str, anio=fecha.year
        )

        html = _fetch_robust(url, timeout=25)
        if html:
            titulo = (
                f"Versión estenográfica. Conferencia de prensa de la presidenta "
                f"Claudia Sheinbaum Pardo del {fecha.day} de {mes_str} de {fecha.year}"
            )
            conferencias.append({
                "url": url,
                "titulo": titulo,
                "fecha_str": fecha.strftime("%Y-%m-%d"),
                "_html": html,
            })
            logger.info(f"  ✓ {fecha.strftime('%Y-%m-%d')} encontrada")
        else:
            logger.info(f"  ✗ {fecha.strftime('%Y-%m-%d')} no disponible")

        fecha += timedelta(days=1)
        time.sleep(2.0)  # Rate limiting
    return conferencias


def main():
    import sqlite3

    desde_str = os.environ.get("BACKFILL_DESDE", "2026-03-12")
    hasta_str = os.environ.get("BACKFILL_HASTA", "2026-04-08")

    desde = datetime.strptime(desde_str, "%Y-%m-%d")
    hasta = datetime.strptime(hasta_str, "%Y-%m-%d")

    logger.info(f"=== Backfill mañaneras: {desde_str} → {hasta_str} ===")

    conn = init_db()

    logger.info("Descubriendo conferencias...")
    conferencias = descubrir_conferencias_rango(desde, hasta)
    logger.info(f"Total conferencias encontradas: {len(conferencias)}")

    if not conferencias:
        logger.warning("No se encontraron conferencias en el rango")
        return

    total_menciones = 0
    for conf in conferencias:
        fecha_str = conf["fecha_str"]
        logger.info(f"Procesando {fecha_str}...")

        html = conf["_html"]
        bloques = extraer_bloques_csp(html)
        if not bloques:
            logger.debug(f"  Sin bloques CSP")
            continue

        chars = sum(len(b) for b in bloques)
        logger.info(f"  {len(bloques)} bloques CSP ({chars} chars)")

        menciones = buscar_menciones_por_categoria(bloques)

        for cat_clave, fragmento in menciones.items():
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO mananera
                        (fecha, categoria, fragmento, url, fecha_scraping)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    fecha_str,
                    cat_clave,
                    fragmento,
                    conf["url"],
                    datetime.now().isoformat() + "_backfill",
                ))
                total_menciones += 1
            except (sqlite3.IntegrityError, ValueError):
                pass

        conn.commit()
        logger.info(f"  → {len(menciones)} categorías mencionadas")

    logger.info(f"\n{'='*60}")
    logger.info(f"BACKFILL MAÑANERAS COMPLETO")
    logger.info(f"  Conferencias procesadas: {len(conferencias)}")
    logger.info(f"  Menciones nuevas: {total_menciones}")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
