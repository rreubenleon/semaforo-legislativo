"""Refresh local de la BD para experimentos, GRATIS.

Incluye: medios (RSS+HTML), gaceta (Diputados, Senado, Permanente),
mañanera, síntesis legislativa, SIL, clasificación por keyword.

NO incluye (cuesta dinero o está prohibido):
  - Twitter API (~$36/mes)
  - Google Trends vía SerpAPI
  - Haiku (PROHIBIDO, blindado borrando ANTHROPIC_API_KEY al inicio)

Se ejecuta manual o por launchd (sábados 15:00 local).
No deploya, no commitea, no sincroniza a Turso/D1.
"""
from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

# Blindaje #1: ningún proceso hijo puede llamar a Haiku.
os.environ.pop("ANTHROPIC_API_KEY", None)
os.environ.pop("FIAT_CLAUDE_API_KEY", None)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("refresh_gratis")


def run_step(name, fn):
    log.info("=" * 60)
    log.info(name)
    log.info("=" * 60)
    t0 = time.time()
    try:
        fn()
    except Exception as e:
        log.warning(f"{name} falló (no crítico): {e}")
    log.info(f"{name} → {time.time() - t0:.1f}s")


from main import (
    paso_1_scraping_medios,
    paso_2_scraping_gaceta,
    paso_2a_scraping_gaceta_senado,
    paso_2a2_scraping_gaceta_permanente,
    paso_2b_scraping_mananera,
    paso_2c_scraping_sintesis,
    paso_2e_scraping_camara_monitoreo,
    paso_4_clasificacion_nlp,
)
from scrapers.sil import scrape_sil_completo

log.info("INICIO refresh local gratis (sin Twitter, sin Trends, sin Haiku)")

run_step("Medios (RSS + HTML)", lambda: paso_1_scraping_medios())
run_step("Gaceta Diputados", lambda: paso_2_scraping_gaceta(dias=21))
run_step("Gaceta Senado", lambda: paso_2a_scraping_gaceta_senado(dias=21))
run_step("Gaceta Permanente", lambda: paso_2a2_scraping_gaceta_permanente())
run_step("Mañanera", lambda: paso_2b_scraping_mananera())
run_step("Síntesis legislativa", lambda: paso_2c_scraping_sintesis())
run_step("Cámara monitoreo", lambda: paso_2e_scraping_camara_monitoreo())
run_step("SIL (incremental)", lambda: scrape_sil_completo(fecha_desde="2024-09-01", detalle_max=500))
run_step("Clasificación NLP (keyword, $0)", lambda: paso_4_clasificacion_nlp())


def chequeo_frescura():
    """Grita si alguna fuente quedó más atrás de lo esperado. Antes el
    refresh fallaba en silencio (gaceta atascada 16 días sin avisar)."""
    from datetime import datetime, timedelta
    from db import get_connection
    conn = get_connection()
    hoy = datetime.now().date()
    mes = hoy.month
    en_receso = mes in (6, 7, 8) or (mes == 1)  # Pleno no sesiona

    # (tabla, columna, umbral_dias, salta_en_receso)
    checks = [
        ("articulos", "fecha", 3, False),
        ("mananera", "fecha", 4, False),
        ("sil_documentos", "fecha_presentacion", 4, False),
        ("gaceta", "fecha", 7, False),  # Diputados publica aun cerca del receso
    ]
    log.info("=" * 60)
    log.info("CHEQUEO DE FRESCURA")
    log.info("=" * 60)
    problemas = 0
    for tabla, col, umbral, salta in checks:
        try:
            maxf = conn.execute(f"SELECT MAX({col}) FROM {tabla}").fetchone()[0]
            if not maxf:
                log.warning(f"⚠ {tabla}: VACÍA")
                problemas += 1
                continue
            edad = (hoy - datetime.strptime(maxf[:10], "%Y-%m-%d").date()).days
            if edad > umbral and not (salta and en_receso):
                log.warning(f"🔴 {tabla}: ATRASADA {edad}d (máx {maxf[:10]}, umbral {umbral}d)")
                problemas += 1
            else:
                log.info(f"✅ {tabla}: {maxf[:10]} ({edad}d)")
        except Exception as e:
            log.warning(f"⚠ {tabla}: error chequeo {e}")
            problemas += 1
    if problemas:
        log.warning(f"🔴 {problemas} fuente(s) con problema de frescura — REVISAR")
    else:
        log.info("✅ Todas las fuentes frescas")
    return problemas


chequeo_frescura()
log.info("REFRESH LOCAL TERMINADO")
