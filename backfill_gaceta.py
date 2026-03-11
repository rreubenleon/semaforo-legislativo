"""
Backfill de Gaceta Parlamentaria — Diputados y Senado
Carga datos históricos desde el inicio de la Legislatura LXVI (Sep 1, 2024).

Uso:
  python backfill_gaceta.py diputados   # Solo Diputados (Sep 2024 → Feb 2026)
  python backfill_gaceta.py senado      # Solo Senado (Oct → Nov 2025)
  python backfill_gaceta.py ambos       # Ambos secuencialmente

Requiere:
  TURSO_DATABASE_URL y TURSO_AUTH_TOKEN en el entorno
  SEMAFORO_DB_MODE=remote (o turso)
"""

import os
import sys
import time
import logging
import warnings
from datetime import datetime, timedelta
from pathlib import Path

# Suprimir warnings de SSL
warnings.filterwarnings("ignore")

# Setup path
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

# Configurar logging con timestamps y progreso claro
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def backfill_diputados():
    """
    Backfill de Gaceta de Diputados: Sep 1, 2024 → Feb 9, 2026.
    Usa el scraper existente mes por mes para mostrar progreso.
    """
    from scrapers.gaceta import init_db, construir_url_gaceta, fetch_page
    from scrapers.gaceta import descubrir_anexos, descubrir_subpaginas
    from scrapers.gaceta import extraer_docs_de_subpagina, extraer_docs_de_comunicaciones
    from scrapers.gaceta import _insertar_documentos
    from db import get_connection, sync as sync_db

    conn = init_db()

    # Verificar cuántos docs ya tenemos
    count_antes = conn.execute(
        "SELECT COUNT(*) FROM gaceta WHERE camara = 'Diputados' OR camara IS NULL"
    ).fetchone()[0]
    logger.info(f"Docs de Diputados antes del backfill: {count_antes}")

    # Rango configurable — por defecto Sep 1, 2024 → Feb 9, 2026
    fecha_inicio_str = os.environ.get("BACKFILL_DESDE", "2024-09-01")
    fecha_fin_str = os.environ.get("BACKFILL_HASTA", "2026-02-09")
    fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d")
    fecha_fin = datetime.strptime(fecha_fin_str, "%Y-%m-%d")

    total_dias = (fecha_fin - fecha_inicio).days
    total_nuevos = 0
    total_requests = 0
    dias_con_gaceta = 0
    inicio_total = time.time()

    # Procesar mes por mes para mostrar progreso
    fecha_actual = fecha_inicio
    while fecha_actual <= fecha_fin:
        # Determinar rango del mes
        year = fecha_actual.year
        month = fecha_actual.month

        # Primer y último día del mes (dentro de nuestro rango)
        if year == fecha_inicio.year and month == fecha_inicio.month:
            primer_dia = fecha_inicio
        else:
            primer_dia = datetime(year, month, 1)

        if year == fecha_fin.year and month == fecha_fin.month:
            ultimo_dia = fecha_fin
        else:
            # Último día del mes
            if month == 12:
                ultimo_dia = datetime(year + 1, 1, 1) - timedelta(days=1)
            else:
                ultimo_dia = datetime(year, month + 1, 1) - timedelta(days=1)

        dias_mes = (ultimo_dia - primer_dia).days + 1
        nuevos_mes = 0

        logger.info(f"{'='*50}")
        logger.info(f"MES: {year}-{month:02d} ({dias_mes} días)")
        logger.info(f"{'='*50}")

        for i in range(dias_mes):
            fecha = primer_dia + timedelta(days=i)
            fecha_str = fecha.strftime("%Y-%m-%d")
            fecha_url = fecha.strftime("%Y%m%d")

            # Descargar página principal del día
            url_principal = construir_url_gaceta(fecha)
            html_principal = fetch_page(url_principal)
            total_requests += 1

            if not html_principal:
                continue

            dias_con_gaceta += 1

            # Descubrir anexos
            anexos = descubrir_anexos(html_principal, fecha_url)

            for anexo in anexos:
                if anexo["es_pdf"] or anexo["tipo"] == "agendas":
                    continue

                if anexo["tipo"] in ("iniciativas_index", "proposiciones_index"):
                    tipo_doc = "iniciativa" if "iniciativas" in anexo["tipo"] else "proposicion"

                    html_index = fetch_page(anexo["url"])
                    total_requests += 1
                    time.sleep(0.3)

                    if not html_index:
                        continue

                    subpaginas = descubrir_subpaginas(html_index, fecha_url, anexo["tipo"])

                    for sub in subpaginas:
                        html_sub = fetch_page(sub["url_html"])
                        total_requests += 1
                        time.sleep(0.3)

                        if not html_sub:
                            continue

                        docs = extraer_docs_de_subpagina(
                            html_sub, sub["url_html"], tipo_doc,
                            sub["partido"], fecha_str,
                        )

                        if sub.get("url_pdf"):
                            for doc in docs:
                                if not doc["url_pdf"]:
                                    doc["url_pdf"] = sub["url_pdf"]

                        n = _insertar_documentos(conn, docs)
                        nuevos_mes += n
                        total_nuevos += n

                elif anexo["tipo"] in ("iniciativas_subpagina", "proposiciones_subpagina"):
                    tipo_doc = "iniciativa" if "iniciativas" in anexo["tipo"] else "proposicion"

                    html_sub = fetch_page(anexo["url"])
                    total_requests += 1
                    time.sleep(0.3)

                    if not html_sub:
                        continue

                    docs = extraer_docs_de_subpagina(
                        html_sub, anexo["url"], tipo_doc, "", fecha_str,
                    )

                    n = _insertar_documentos(conn, docs)
                    nuevos_mes += n
                    total_nuevos += n

                elif anexo["tipo"] == "comunicaciones":
                    html_com = fetch_page(anexo["url"])
                    total_requests += 1
                    time.sleep(0.3)

                    if not html_com:
                        continue

                    docs = extraer_docs_de_comunicaciones(html_com, anexo["url"], fecha_str)

                    n = _insertar_documentos(conn, docs)
                    nuevos_mes += n
                    total_nuevos += n

            # Log progreso cada día con gaceta
            elapsed = time.time() - inicio_total
            logger.info(f"  {fecha_str}: +{nuevos_mes} nuevos (total: {total_nuevos}, {elapsed:.0f}s)")

        # Sync después de cada mes
        try:
            sync_db()
        except Exception:
            pass

        logger.info(f"Mes {year}-{month:02d}: {nuevos_mes} docs nuevos")

        # Avanzar al siguiente mes
        if month == 12:
            fecha_actual = datetime(year + 1, 1, 1)
        else:
            fecha_actual = datetime(year, month + 1, 1)

    duracion = time.time() - inicio_total
    count_despues = conn.execute(
        "SELECT COUNT(*) FROM gaceta WHERE camara = 'Diputados' OR camara IS NULL"
    ).fetchone()[0]

    logger.info(f"\n{'='*60}")
    logger.info(f"BACKFILL DIPUTADOS COMPLETO")
    logger.info(f"  Docs nuevos: {total_nuevos}")
    logger.info(f"  Docs antes: {count_antes} → después: {count_despues}")
    logger.info(f"  Días con gaceta: {dias_con_gaceta}/{total_dias}")
    logger.info(f"  Requests HTTP: {total_requests}")
    logger.info(f"  Duración: {duracion/60:.1f} min")
    logger.info(f"{'='*60}")

    return total_nuevos


def backfill_senado():
    """
    Backfill de Gaceta del Senado: Oct 1 → Nov 26, 2025.
    (Ya tenemos Nov 27, 2025 → presente)
    """
    from scrapers.gaceta_senado import (
        init_db, descubrir_gacetas_mes, fetch_page,
        parsear_gaceta_diaria, _insertar_documentos,
    )
    from db import sync as sync_db

    conn = init_db()

    count_antes = conn.execute(
        "SELECT COUNT(*) FROM gaceta WHERE camara = 'Senado'"
    ).fetchone()[0]
    logger.info(f"Docs de Senado antes del backfill: {count_antes}")

    total_nuevos = 0
    total_requests = 0
    inicio = time.time()

    # Meses a scrapear: Oct y Nov 2025
    meses = [(2025, 10), (2025, 11)]

    for year, month in meses:
        logger.info(f"\n{'='*50}")
        logger.info(f"SENADO MES: {year}-{month:02d}")
        logger.info(f"{'='*50}")

        gacetas = descubrir_gacetas_mes(year, month)
        total_requests += 1

        logger.info(f"Gacetas disponibles en {year}-{month:02d}: {len(gacetas)}")

        for g in sorted(gacetas, key=lambda x: x["fecha"]):
            # Solo hasta Nov 26 (ya tenemos Nov 27+)
            if g["fecha"] >= "2025-11-27":
                logger.info(f"  {g['fecha']}: ya cubierto, saltando")
                continue

            logger.info(f"  Scrapeando {g['fecha']}...")
            html = fetch_page(g["url"])
            total_requests += 1
            time.sleep(1.0)  # Rate limit de Incapsula

            if not html:
                logger.info(f"    No se pudo descargar")
                continue

            docs = parsear_gaceta_diaria(html, g["fecha"])

            if docs:
                n = _insertar_documentos(conn, docs)
                total_nuevos += n
                logger.info(f"    {len(docs)} docs encontrados, {n} nuevos")
            else:
                logger.info(f"    0 documentos")

        # Sync después de cada mes
        try:
            sync_db()
        except Exception:
            pass

    duracion = time.time() - inicio
    count_despues = conn.execute(
        "SELECT COUNT(*) FROM gaceta WHERE camara = 'Senado'"
    ).fetchone()[0]

    logger.info(f"\n{'='*60}")
    logger.info(f"BACKFILL SENADO COMPLETO")
    logger.info(f"  Docs nuevos: {total_nuevos}")
    logger.info(f"  Docs antes: {count_antes} → después: {count_despues}")
    logger.info(f"  Requests HTTP: {total_requests}")
    logger.info(f"  Duración: {duracion/60:.1f} min")
    logger.info(f"{'='*60}")

    return total_nuevos


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python backfill_gaceta.py [diputados|senado|ambos]")
        sys.exit(1)

    modo = sys.argv[1].lower()

    if modo == "diputados":
        backfill_diputados()
    elif modo == "senado":
        backfill_senado()
    elif modo == "ambos":
        backfill_diputados()
        backfill_senado()
    else:
        print(f"Modo desconocido: {modo}. Usa: diputados, senado, ambos")
        sys.exit(1)
