"""
Scraper de asuntos turnados por comisión — SITL Diputados.

Fuente: sitl.diputados.gob.mx/LXVI_leg/cuadro_asuntos_por_comisionlxvi.php?comt={ID}
Cada comisión tiene un resumen con columnas:
  Documento | Turnadas | Aprobadas | Desechadas | Atendidas | Retiradas | Pendientes
Separado por tipo de turno: Comisión Única, Comisiones Unidas, Opinión.

Devuelve un dict {nombre_comision: {turnadas, aprobadas, desechadas, pendientes, ...}}
para enriquecer la pestaña de Comisiones de FIAT.

Uso:
    from scrapers.comisiones_sitl import scrape_comisiones_diputados
    data = scrape_comisiones_diputados()
"""
from __future__ import annotations

import logging
import re
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("comisiones_sitl")

BASE_URL = "https://sitl.diputados.gob.mx/LXVI_leg"
SUMMARY_URL = f"{BASE_URL}/cuadro_asuntos_por_comisionlxvi.php"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

# IDs válidos de comisiones (1-57, excluyendo 58-70 que son especiales/basura)
COMISION_IDS = list(range(1, 58))


def _parse_int(text: str) -> int:
    """Extrae número entero de un texto, 0 si no hay."""
    if not text:
        return 0
    digits = re.sub(r"[^\d]", "", text.strip())
    return int(digits) if digits else 0


def _scrape_comision(comt_id: int) -> Optional[dict]:
    """
    Scrapea la página de resumen de una comisión.
    Devuelve dict con nombre y conteos agregados, o None si falla.
    """
    try:
        resp = requests.get(
            SUMMARY_URL, params={"comt": comt_id},
            headers=HEADERS, timeout=20
        )
        resp.raise_for_status()
        resp.encoding = "utf-8"
    except Exception as e:
        logger.warning(f"  Error HTTP comt={comt_id}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Extraer nombre de comisión
    td_nombre = soup.find("td", class_="Estilo61enex")
    if not td_nombre:
        return None
    nombre_match = re.search(
        r"Asuntos turnados a la comisi[oó]n de:\s*(.+)",
        td_nombre.get_text(strip=True), re.IGNORECASE
    )
    if not nombre_match:
        return None
    nombre = nombre_match.group(1).strip()

    # Parsear todas las filas de datos
    # Columnas: Documento | Turnadas | Aprobadas | Desechadas | Atendidas | Retiradas | Pendientes
    totales = {
        "turnadas": 0, "aprobadas": 0, "desechadas": 0,
        "atendidas": 0, "retiradas": 0, "pendientes": 0,
        "iniciativas_turnadas": 0, "iniciativas_aprobadas": 0, "iniciativas_pendientes": 0,
        "proposiciones_turnadas": 0, "proposiciones_aprobadas": 0, "proposiciones_pendientes": 0,
        "minutas_turnadas": 0, "minutas_aprobadas": 0, "minutas_pendientes": 0,
    }

    # Buscar links con conteos (son los <a> con href a iniciativaslxvi.php o proposicioneslxvi.php)
    links = soup.find_all("a", class_="estilolinks")
    for link in links:
        href = link.get("href", "")
        count = _parse_int(link.get_text())
        if count == 0:
            continue

        # Determinar tipo de documento
        if "iniciativas" in href:
            doc_type = "iniciativas"
        elif "proposiciones" in href:
            doc_type = "proposiciones"
        elif "minutas" in href:
            doc_type = "minutas"
        else:
            continue

        # Determinar estatus
        edot_match = re.search(r"edot=(\w)", href)
        if not edot_match:
            continue
        edot = edot_match.group(1)
        estatus_map = {"T": "turnadas", "A": "aprobadas", "D": "desechadas",
                       "B": "retiradas", "P": "pendientes"}
        estatus = estatus_map.get(edot)
        if not estatus:
            continue

        # Acumular en totales generales
        totales[estatus] += count
        # Acumular por tipo de documento
        key = f"{doc_type}_{estatus}"
        if key in totales:
            totales[key] += count

    # Calcular tasa de resolución
    total_turnadas = totales["turnadas"]
    total_resueltas = totales["aprobadas"] + totales["desechadas"] + totales["atendidas"]
    tasa_resolucion = round(total_resueltas / total_turnadas * 100, 1) if total_turnadas > 0 else 0.0
    tasa_aprobacion = round(totales["aprobadas"] / total_turnadas * 100, 1) if total_turnadas > 0 else 0.0

    return {
        "comt_id": comt_id,
        "nombre": nombre,
        **totales,
        "tasa_resolucion": tasa_resolucion,
        "tasa_aprobacion": tasa_aprobacion,
    }


def scrape_comisiones_diputados(delay: float = 1.5) -> dict[str, dict]:
    """
    Scrapea todas las comisiones de Diputados (IDs 1-57).
    Devuelve dict keyed por nombre de comisión normalizado.
    Delay entre requests para no saturar el servidor.
    """
    logger.info(f"Scraping SITL Diputados: {len(COMISION_IDS)} comisiones…")
    resultado = {}
    errores = 0

    for i, comt_id in enumerate(COMISION_IDS, 1):
        data = _scrape_comision(comt_id)
        if data:
            resultado[data["nombre"]] = data
            if i <= 3 or i % 10 == 0:
                logger.info(
                    f"  [{i:>2}/{len(COMISION_IDS)}] {data['nombre'][:40]}: "
                    f"{data['turnadas']}T / {data['aprobadas']}A / {data['pendientes']}P "
                    f"(tasa aprobación: {data['tasa_aprobacion']}%)"
                )
        else:
            errores += 1

        if i < len(COMISION_IDS):
            time.sleep(delay)

    logger.info(f"SITL Diputados: {len(resultado)} comisiones scrapeadas, {errores} errores")
    return resultado


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    data = scrape_comisiones_diputados(delay=1.0)
    print(f"\n{'='*60}")
    print(f"Total comisiones: {len(data)}")
    print(f"{'='*60}")
    for nombre, d in sorted(data.items(), key=lambda x: x[1]["turnadas"], reverse=True)[:10]:
        print(f"  {nombre[:45]:<45} T:{d['turnadas']:>4} A:{d['aprobadas']:>3} P:{d['pendientes']:>3} → {d['tasa_aprobacion']:.1f}%")
