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

    # 1. Tipos explícitos al inicio del título
    explicitos = [
        (r"^Iniciativa", "Iniciativa"),
        (r"^Proposici[oó]n", "Proposición con punto de acuerdo"),
        (r"^Punto\s+de\s+Acuerdo", "Proposición con punto de acuerdo"),
        (r"^Acuerdo\s+Parlamentario", "Acuerdo Parlamentario"),
        (r"^Dictamen", "Dictamen"),
        (r"^Oficio|^Comunicaci", "Comunicado"),
        (r"^ACTA", "Acta"),
        (r"^Orden\s+del\s+D[íi]a", "Orden del Día"),
    ]
    for patron, tipo in explicitos:
        if re.match(patron, titulo, flags=re.IGNORECASE):
            return tipo, TIPO_GRUPO_MAP.get(tipo, "Otro")

    # 2. Arranca con un PROMOVENTE (Sen./Dip./diputado/senador/etc.).
    # Esto cubre los casos de iniciativas y proposiciones presentadas
    # durante la Permanente que arrancan con quién las propuso.
    es_promovente = bool(re.match(
        r"^Del?\s+(?:Sen|Dip)\.\s+|"
        r"^De\s+la\s+(?:Sen|Dip)\.\s+|"
        r"^Del?\s+(?:senador|diputad|legislador)|"
        r"^De\s+la\s+(?:senadora|diputad|legislador)|"
        r"^De\s+l[oa]s?\s+(?:senador|diputad|legislador|senadora|diputada)",
        titulo, flags=re.IGNORECASE
    ))
    if es_promovente:
        # Distinguir por el contenido del título
        if re.search(r"con\s+punto\s+de\s+acuerdo|exhorta\s+a", titulo, flags=re.IGNORECASE):
            return "Proposición con punto de acuerdo", "Proposición con PA"
        if re.search(
            r"con\s+proyecto\s+de\s+decreto|"
            r"que\s+(?:reforma|adiciona|expide|abroga|deroga|modifica)|"
            r"se\s+(?:reforma|adiciona|expide|abroga|deroga|modifica)|"
            r"declara\s+el\s+\d|adicion[aóa]",
            titulo, flags=re.IGNORECASE
        ):
            return "Iniciativa", "Iniciativa"
        # Si no aplica ni proyecto de decreto ni punto de acuerdo,
        # probable comunicación de un legislador (reincorporación, etc.)
        return "Comunicado", "Comunicado"

    return "Otro", "Otro"


def extraer_presentador(titulo: str) -> str:
    """Extrae el AUTOR primario del título de un doc de la Gaceta Permanente.
    El título trae quién presenta, ej.:
      'Del Dip. Mario Alberto López Hernández, del Grupo Parlamentario...'
      'De la Sen. Paloma Sánchez Ramos, del Grupo Parlamentario...'
      'Del senador Alberto Anaya Gutiérrez y de las senadoras ...'
    Devuelve 'Sen. Nombre' / 'Dip. Nombre' (formato del resto del SIL) o ''.

    OJO: solo guardamos `presentador` (metadato). NO marcamos
    tipo_presentador='legislador' a propósito — así poblar_actividad_desde_sil
    NO mete estos docs a actividad_legislador (evita contaminar conteos como
    l3p). La atribución a un legislador se hace en el export de "últimas
    propuestas", con DEDUP contra el SIL numérico.
    """
    if not titulo:
        return ""
    m = re.match(
        r"^De(?:\s+la|l)?\s+"
        r"(Sen\.|Dip\.|senador(?:a)?|diputad[oa]|legislador(?:a)?)\s+"
        r"(.+?)"
        r"(?:,|\s+del?\s+Grupo|\s+y\s+de\s+l|\s+a\s+nombre|\s+con\s+(?:proyecto|punto)|\s+integrante)",
        titulo, flags=re.IGNORECASE)
    if not m:
        return ""
    rol = m.group(1).lower()
    nombre = m.group(2).strip().rstrip(",").strip()
    if len(nombre) < 4:
        return ""
    prefijo = "Sen." if rol.startswith(("sen", "senador")) else "Dip."
    return f"{prefijo} {nombre}"


MESES_ES = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}


def _parsear_fecha_es(s: str) -> str:
    """'Miércoles 06 de mayo de 2026 / Gaceta: …' → '2026-05-06'."""
    if not s:
        return ""
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", s.lower())
    if not m:
        return ""
    mes = MESES_ES.get(m.group(2), "")
    if not mes:
        return ""
    return f"{m.group(3)}-{mes}-{m.group(1).zfill(2)}"


def obtener_listado() -> list[dict]:
    """
    Descarga la página principal y extrae los documentos con su FECHA REAL
    de sesión (no la fecha de instalación por defecto).

    La página agrupa docs por sesión con headers tipo
      "Miércoles 06 de mayo de 2026 / Gaceta: LXVI/2SPR-4"
    Cada doc bajo ese header pertenece a esa sesión.

    Approach:
      1. WAF Imperva: usar Session + retry hasta obtener HTML real (~200KB).
      2. Recorrer el árbol DOM en orden, trackear el header de fecha más
         reciente, asociar cada link <a href=".../documento/N"> a esa fecha.
    """
    sess = requests.Session()
    sess.headers.update(HEADERS)
    html = ""
    for attempt in range(4):
        r = sess.get(LISTADO_URL, timeout=30)
        if len(r.text) > 10000:
            html = r.text
            break
        time.sleep(2 + attempt)
    if not html:
        logger.warning(f"Listado Permanente vacío tras retries (WAF?)")
        return []

    soup = BeautifulSoup(html, "html.parser")

    # Caminar el HTML en orden, manteniendo última fecha vista.
    docs = []
    seen = set()
    fecha_actual = ""
    re_fecha = re.compile(
        r"(?:Lunes|Martes|Mi[ée]rcoles|Jueves|Viernes|S[áa]bado|Domingo)\s+\d{1,2}\s+de\s+\w+\s+de\s+\d{4}",
        re.IGNORECASE,
    )

    # Iterar sobre descendientes del body en orden. Solo actualizar
    # `fecha_actual` cuando el texto del header SE INICIA con un día de
    # la semana (ej "Miércoles 06 de mayo de 2026 / Gaceta..."). Ignorar
    # textos como "ACTA DE LA SESIÓN DE INSTALACIÓN DEL MIÉRCOLES 29 DE
    # ABRIL DE 2026" que mencionan fechas pero no son headers de sesión.
    re_fecha_header = re.compile(
        r"^(?:Lunes|Martes|Mi[ée]rcoles|Jueves|Viernes|S[áa]bado|Domingo)\s+\d{1,2}\s+de\s+\w+\s+de\s+\d{4}",
        re.IGNORECASE,
    )

    body = soup.body or soup
    for elem in body.descendants:
        # Headers con fecha — solo si el texto INICIA con día de semana
        if hasattr(elem, "name") and elem.name in ("h1", "h2", "h3", "h4", "h5", "strong", "b"):
            txt = elem.get_text(" ", strip=True)
            if re_fecha_header.match(txt):
                f_iso = _parsear_fecha_es(txt)
                if f_iso:
                    fecha_actual = f_iso
            continue
        # Si es link a doc
        if hasattr(elem, "name") and elem.name == "a":
            href = elem.get("href", "")
            m = re.search(r"/gaceta_comision_permanente/documento/(\d+)", href)
            if not m:
                continue
            doc_id = m.group(1)
            if doc_id in seen:
                continue
            seen.add(doc_id)
            titulo = elem.get_text(strip=True)
            if not titulo or len(titulo) < 5:
                continue
            docs.append({
                "doc_id": doc_id,
                "titulo": titulo[:500],
                "url": href if href.startswith("http") else f"{BASE}{href}",
                "fecha": fecha_actual,  # fecha REAL de la sesión
            })
    return docs


def fecha_default_para_id(doc_id: int) -> str:
    """Fallback solo si scrape no encontró la fecha real de la sesión."""
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
        # Usar fecha REAL extraída del header de la sesión.
        # Fallback solo si el scraper no encontró el header (raro).
        fecha = doc.get("fecha") or fecha_default_para_id(int(doc["doc_id"]))
        clasificacion = "legislativo_sustantivo" if tipo_grupo in ("Iniciativa", "Proposición con PA") else ""
        # Extraer el promovente del título (metadato) + url del doc. NO se
        # marca tipo_presentador='legislador' (ver extraer_presentador).
        presentador = extraer_presentador(doc["titulo"])
        url = doc.get("url", "")
        try:
            cur = conn.execute("""
                INSERT INTO sil_documentos
                  (seguimiento_id, asunto_id, tipo, titulo, sinopsis, camara,
                   fecha_presentacion, legislatura, periodo, estatus, partido,
                   comision, categoria, fecha_scraping, presentador,
                   tipo_presentador, tipo_grupo, clasificacion, url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(seguimiento_id, asunto_id) DO UPDATE SET
                    tipo=excluded.tipo,
                    tipo_grupo=excluded.tipo_grupo,
                    clasificacion=excluded.clasificacion,
                    titulo=excluded.titulo,
                    fecha_presentacion=excluded.fecha_presentacion,
                    presentador=excluded.presentador,
                    url=excluded.url
            """, (
                seg_id, asu_id, tipo,
                doc["titulo"][:500],
                "",
                "Comisión Permanente",
                fecha, "LXVI", "2do Receso",
                "", "", "", "",
                ahora, presentador, "",
                tipo_grupo,
                clasificacion, url,
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
