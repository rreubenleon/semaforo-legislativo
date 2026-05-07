"""
Scraper de la Gaceta Parlamentaria de la Comisión Permanente.

Fuente oficial: senado.gob.mx/66/gaceta_comision_permanente/

Durante el receso (mayo-agosto y diciembre-enero) la Comisión Permanente
es la que sesiona. Esta Gaceta publica los actos legislativos del receso:
- Iniciativas y proposiciones presentadas
- Oficios del Ejecutivo (Sheinbaum)
- Comunicados de organismos
- Actas de sesión

Inserta en sil_documentos con camara='Comisión Permanente' y
legislatura='LXVI'. Idempotente — INSERT OR IGNORE por seguimiento_id.

Uso:
    python -c "from scrapers.gaceta_permanente import scrape; scrape()"
"""
from __future__ import annotations

import logging
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from db import get_connection

logger = logging.getLogger(__name__)

BASE = "https://www.senado.gob.mx"
LISTADO_URL = f"{BASE}/66/gaceta_comision_permanente/"
DOC_URL = f"{BASE}/66/gaceta_comision_permanente/documento/{{id}}"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 Chrome/120 Safari/537.36"
HEADERS = {"User-Agent": UA}

# Mapeo de patrones de título a tipo SIL
TIPO_PATRONES = [
    (r"^Iniciativa", "Iniciativa"),
    (r"^Proposici[oó]n", "Proposición con punto de acuerdo"),
    (r"^Punto\s+de\s+Acuerdo|punto\s+de\s+acuerdo", "Proposición con punto de acuerdo"),
    (r"^Acuerdo\s+Parlamentario", "Acuerdo Parlamentario"),
    (r"^Acuerdo", "Acuerdo Parlamentario"),
    (r"^Dictamen", "Dictamen"),
    (r"^Oficio", "Comunicado"),
    (r"^Comunicaci", "Comunicado"),
    (r"^ACTA", "Acta"),
    (r"^Orden\s+del\s+D[íi]a", "Orden del Día"),
    (r"^De\s+l[oa]s?\s+(senadoras|senadores|legislador|diputado)", "Iniciativa"),
    (r"^Del\s+Sen|^De\s+la\s+Sen", "Iniciativa"),
]

TIPO_GRUPO_MAP = {
    "Iniciativa": "Iniciativa",
    "Proposición con punto de acuerdo": "Proposición con PA",
    "Acuerdo Parlamentario": "Acuerdo Parlamentario",
    "Dictamen": "Dictamen",
    "Comunicado": "Comunicado",
    "Acta": "Comunicado",
    "Orden del Día": "Comunicado",
}


def clasificar_tipo(titulo: str) -> tuple[str, str]:
    """Devuelve (tipo, tipo_grupo) inferido del título."""
    if not titulo:
        return "Otro", "Otro"
    for patron, tipo in TIPO_PATRONES:
        if re.match(patron, titulo, flags=re.IGNORECASE):
            return tipo, TIPO_GRUPO_MAP.get(tipo, "Otro")
    return "Otro", "Otro"


def obtener_listado() -> list[dict]:
    """Descarga la página principal y extrae los documentos enlazados."""
    r = requests.get(LISTADO_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    docs = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        m = re.search(r"/gaceta_comision_permanente/documento/(\d+)", href)
        if not m:
            continue
        doc_id = m.group(1)
        if doc_id in seen:
            continue
        seen.add(doc_id)
        titulo = a.get_text(strip=True)
        if not titulo or len(titulo) < 5:
            continue
        docs.append({
            "doc_id": doc_id,
            "titulo": titulo[:500],
            "url": href if href.startswith("http") else f"{BASE}{href}",
        })
    return docs


def parsear_fecha_seccion(soup: BeautifulSoup, doc_id: str) -> str:
    """Extrae fecha de publicación buscando 'Fecha:' o headers de día cerca del doc."""
    # Buscar en la página principal: a menudo agrupa docs por día con header tipo
    # "Miércoles 29 de abril de 2026"
    # Por ahora: usar fecha de instalación (29-abr-2026) como default si no se encuentra
    return ""


def fecha_default_para_id(doc_id: int) -> str:
    """
    Heurística: ID 158927 es del Acta de Instalación (29-abr-2026).
    IDs cercanos son del mismo día. Sin metadata exacta, usar 29-abr-2026
    como fecha default para todos los docs LXVI 2do receso 2do año.
    Esto se afina cuando se scrape el detalle de cada doc.
    """
    return "2026-04-29"


def scrape(solo_count: bool = False) -> dict:
    """
    Descarga el listado de la Gaceta Permanente e inserta cada doc en
    sil_documentos. Idempotente.
    """
    logger.info("Scrape Gaceta Permanente LXVI")
    docs = obtener_listado()
    logger.info(f"  Documentos encontrados: {len(docs)}")

    if solo_count:
        return {"encontrados": len(docs), "insertados": 0, "saltados": 0}

    conn = get_connection()
    ahora = datetime.now().isoformat()
    insertados = 0
    saltados = 0
    distribucion_tipo = {}

    for doc in docs:
        seg_id = f"PERM_{doc['doc_id']}"
        asu_id = seg_id
        tipo, tipo_grupo = clasificar_tipo(doc["titulo"])
        distribucion_tipo[tipo] = distribucion_tipo.get(tipo, 0) + 1
        fecha = fecha_default_para_id(int(doc["doc_id"]))
        try:
            cur = conn.execute("""
                INSERT OR IGNORE INTO sil_documentos
                  (seguimiento_id, asunto_id, tipo, titulo, sinopsis, camara,
                   fecha_presentacion, legislatura, periodo, estatus, partido,
                   comision, categoria, fecha_scraping, presentador,
                   tipo_presentador, tipo_grupo, clasificacion)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                seg_id, asu_id, tipo,
                doc["titulo"][:500],
                "",
                "Comisión Permanente",
                fecha, "LXVI", "2do Receso",
                "", "", "", "",
                ahora, "", "",
                tipo_grupo,
                "legislativo_sustantivo" if tipo_grupo in ("Iniciativa", "Proposición con PA") else "",
            ))
            if cur.rowcount > 0:
                insertados += 1
            else:
                saltados += 1
        except Exception as e:
            logger.warning(f"Error insertando doc {doc['doc_id']}: {e}")

    conn.commit()
    logger.info(f"  Insertados: {insertados}, saltados (ya en BD): {saltados}")
    logger.info(f"  Distribución por tipo: {distribucion_tipo}")
    return {"encontrados": len(docs), "insertados": insertados, "saltados": saltados, "tipos": distribucion_tipo}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    res = scrape()
    print(f"\nResultado: {res}")
