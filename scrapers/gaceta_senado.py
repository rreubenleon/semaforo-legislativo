"""
Scraper de la Gaceta del Senado de la República
Extrae: iniciativas, proposiciones, dictámenes, minutas, comunicaciones

Arquitectura:
  1. Obtener calendario → descubrir gacetas disponibles por fecha
  2. Página diaria de gaceta → SUMARIO con secciones (anchors)
  3. Cada sección contiene documentos con:
     - Título (link a /documento/{id})
     - Senador(es) autores (link a /senador/{id}, img alt)
     - Turno/resolución (texto posterior al título)

URL patterns:
  Calendario:  /66/gaceta_del_senado (carga mes actual con links por día)
  Día:         /66/gaceta_del_senado/YYYY_MM_DD/{gaceta_id}
  Documento:   /66/gaceta_del_senado/documento/{doc_id}
  Senador:     /66/senador/{senador_id}
"""

import re
import time
import logging
import sqlite3
import warnings
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

# Suprimir warnings de SSL (el sitio del Senado tiene cert inconsistente)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection

logger = logging.getLogger(__name__)

BASE_URL = "https://www.senado.gob.mx"
LEGISLATURA = "66"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
}

# Secciones que contienen documentos legislativos relevantes
SECCIONES_RELEVANTES = {
    "Iniciativas": "iniciativa",
    "Dictámenes a Discusión y Votación": "dictamen",
    "Proposiciones": "proposicion",
    "Poder Ejecutivo Federal": "iniciativa",
    "Congresos de los Estados": "comunicacion",
    "Comunicaciones de Comisiones": "comunicacion",
    "Agenda Política": "proposicion",
    "Secretaría de Gobernación": "comunicacion",
    "Respuestas a Acuerdos promovidos por Senadoras y Senadores": "comunicacion",
    "Dirección General de Asuntos Jurídicos del Senado de la República": "comunicacion",
    "Publicaciones": "comunicacion",
}

# Regex para extraer partido del título
RE_PARTIDO = re.compile(
    r"[Gg]rupo\s+[Pp]arlamentario\s+(?:del?\s+)?(.+?)(?:\.|,\s|y\s+de\s|$)",
    re.IGNORECASE,
)

NORMALIZACION_PARTIDO = {
    "morena": "Morena",
    "acción nacional": "PAN",
    "pan": "PAN",
    "revolucionario institucional": "PRI",
    "pri": "PRI",
    "verde ecologista": "PVEM",
    "pvem": "PVEM",
    "del trabajo": "PT",
    "pt": "PT",
    "movimiento ciudadano": "MC",
    "mc": "MC",
    "prd": "PRD",
}

# ─────────────────────────────────────────────
# SESSION
# ─────────────────────────────────────────────

_session = None

def _get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(HEADERS)
    return _session


def fetch_page(url, timeout=30):
    """Descarga una página con manejo de errores."""
    try:
        resp = _get_session().get(url, timeout=timeout, verify=False)
        if resp.status_code == 200 and len(resp.text) > 500:
            return resp.text
        logger.debug(f"Respuesta no válida para {url}: status={resp.status_code}, len={len(resp.text)}")
        return None
    except requests.RequestException as e:
        logger.debug(f"Error descargando {url}: {e}")
        return None


# ─────────────────────────────────────────────
# BASE DE DATOS
# ─────────────────────────────────────────────

def init_db():
    """Crea/migra la tabla gaceta para soportar datos del Senado."""
    conn = get_connection()

    # Migración: agregar columna 'camara' si no existe
    try:
        conn.execute("ALTER TABLE gaceta ADD COLUMN camara TEXT DEFAULT 'Diputados'")
        conn.commit()
        logger.info("Columna 'camara' agregada a tabla gaceta")
    except (sqlite3.OperationalError, ValueError):
        pass  # Ya existe

    return conn


# ─────────────────────────────────────────────
# DESCUBRIR GACETAS DISPONIBLES
# ─────────────────────────────────────────────

def descubrir_gacetas_mes(year, month):
    """
    Obtiene las gacetas disponibles para un mes dado usando el endpoint AJAX.
    Retorna lista de dicts: [{fecha: "2026-03-04", url: "https://...3787"}, ...]
    """
    ajax_url = f"{BASE_URL}/{LEGISLATURA}/app/gaceta/functions/calendarioMes.php"
    try:
        resp = _get_session().get(
            ajax_url,
            params={"action": "ajax", "anio": year, "mes": month, "dia": 1},
            timeout=30,
            verify=False,
        )
        if resp.status_code != 200 or len(resp.text) < 100:
            return []
    except requests.RequestException as e:
        logger.debug(f"Error obteniendo calendario {year}/{month}: {e}")
        return []

    gacetas = []
    for match in re.finditer(r"gaceta_del_senado/(\d{4})_(\d{2})_(\d{2})/(\d+)", resp.text):
        y, m, d, gid = match.groups()
        fecha = f"{y}-{m}-{d}"
        full_url = f"{BASE_URL}/{LEGISLATURA}/gaceta_del_senado/{y}_{m}_{d}/{gid}"
        if not any(g["fecha"] == fecha for g in gacetas):
            gacetas.append({"fecha": fecha, "url": full_url, "gaceta_id": gid})

    return gacetas


# ─────────────────────────────────────────────
# PARSEAR PÁGINA DIARIA DE GACETA
# ─────────────────────────────────────────────

def parsear_gaceta_diaria(html, fecha):
    """
    Parsea una página diaria de la Gaceta del Senado.
    Extrae documentos agrupados por sección.
    Retorna lista de dicts con campos compatibles con tabla gaceta.
    """
    soup = BeautifulSoup(html, "html.parser")
    documentos = []

    # 1. Construir mapa de secciones desde el SUMARIO
    #    El SUMARIO tiene links tipo href="#416" con texto "Iniciativas"
    seccion_map = {}  # anchor_id → section_name
    for a in soup.find_all("a", href=lambda h: h and h.startswith("#") and len(h) > 1):
        anchor_id = a["href"][1:]
        text = a.get_text(strip=True)
        if text and len(text) > 3 and anchor_id.isdigit():
            # Preferir el nombre de sección, no el conteo
            if text not in seccion_map.values() or not text.isdigit():
                seccion_map[anchor_id] = text

    # Limpiar: remover entradas que son solo números (conteos)
    seccion_map = {k: v for k, v in seccion_map.items() if not v.isdigit()}

    # 2. Encontrar anchors de sección en el contenido
    #    <a name="416"></a> marca el inicio de la sección "Iniciativas"
    named_anchors = soup.find_all("a", attrs={"name": True})
    anchor_order = []
    for a in named_anchors:
        name = a.get("name", "")
        if name in seccion_map:
            anchor_order.append(name)

    # 3. Para cada sección, encontrar documentos entre este anchor y el siguiente
    for i, anchor_id in enumerate(anchor_order):
        seccion_nombre = seccion_map.get(anchor_id, "Otro")
        tipo_doc = SECCIONES_RELEVANTES.get(seccion_nombre)

        if tipo_doc is None:
            # Sección no relevante (Orden del Día, Acta, Efemérides)
            continue

        # Encontrar el anchor element
        anchor_el = soup.find("a", attrs={"name": anchor_id})
        if not anchor_el:
            continue

        # Recopilar elementos hasta el siguiente anchor de sección
        next_anchor_id = anchor_order[i + 1] if i + 1 < len(anchor_order) else None

        # Buscar links a documentos dentro de esta sección
        docs_en_seccion = _extraer_docs_seccion(soup, anchor_id, next_anchor_id, seccion_nombre, tipo_doc, fecha)
        documentos.extend(docs_en_seccion)

    # 4. Fallback: si no encontramos secciones (páginas simples/intermedias),
    #    extraer todos los documentos directamente
    if not documentos:
        documentos = _extraer_docs_sin_sumario(soup, fecha)

    return documentos


def _extraer_docs_sin_sumario(soup, fecha):
    """
    Fallback para páginas sin SUMARIO (gacetas intermedias, días sin sesión).
    Extrae todos los documentos directamente de los links.
    """
    docs = []
    seen_ids = set()

    for a in soup.find_all("a", href=re.compile(r"gaceta_del_senado/documento/\d+")):
        match = re.search(r"documento/(\d+)", a["href"])
        if not match:
            continue

        doc_id = match.group(1)
        if doc_id in seen_ids:
            continue

        titulo = a.get_text(strip=True)
        if not titulo or len(titulo) < 10:
            continue

        seen_ids.add(doc_id)

        # Intentar clasificar por contenido del título
        tipo_doc = _clasificar_tipo_por_titulo(titulo)
        autor = _extraer_autor_senado(soup, doc_id, titulo)
        partido = _extraer_partido(titulo)
        turno = _extraer_turno(a)
        comision = _extraer_comision_turno(turno) or _extraer_comision_titulo(titulo)

        doc_url = f"{BASE_URL}/{LEGISLATURA}/gaceta_del_senado/documento/{doc_id}"

        docs.append({
            "fecha": fecha,
            "tipo": tipo_doc,
            "titulo": _limpiar(titulo)[:500],
            "autor": autor or "No identificado",
            "comision": comision or "No especificada",
            "resumen": _limpiar(turno or titulo)[:1000],
            "url": doc_url,
            "url_pdf": "",
            "numero_doc": doc_id,
            "seccion": "General",
            "camara": "Senado",
        })

    return docs


def _clasificar_tipo_por_titulo(titulo):
    """Clasifica el tipo de documento por palabras clave en el título."""
    titulo_lower = titulo.lower()
    if re.search(r"iniciativa|que\s+reforma|que\s+adiciona|que\s+expide|proyecto\s+de\s+decreto", titulo_lower):
        return "iniciativa"
    if re.search(r"proposici[oó]n|punto\s+de\s+acuerdo|con\s+punto\s+de\s+acuerdo", titulo_lower):
        return "proposicion"
    if re.search(r"dictam[eé]n", titulo_lower):
        return "dictamen"
    if re.search(r"minuta", titulo_lower):
        return "minuta"
    if re.search(r"convocatoria|reuni[oó]n|citatorio", titulo_lower):
        return "comunicacion"
    return "comunicacion"


def _extraer_docs_seccion(soup, anchor_id, next_anchor_id, seccion_nombre, tipo_doc, fecha):
    """
    Extrae documentos de una sección delimitada por anchors.
    """
    docs = []
    html_str = str(soup)

    # Encontrar la posición del anchor de inicio y fin
    start_pattern = f'name="{anchor_id}"'
    start_idx = html_str.find(start_pattern)
    if start_idx < 0:
        start_pattern = f"name='{anchor_id}'"
        start_idx = html_str.find(start_pattern)
    if start_idx < 0:
        return docs

    if next_anchor_id:
        end_pattern = f'name="{next_anchor_id}"'
        end_idx = html_str.find(end_pattern, start_idx)
        if end_idx < 0:
            end_pattern = f"name='{next_anchor_id}'"
            end_idx = html_str.find(end_pattern, start_idx)
        if end_idx < 0:
            end_idx = len(html_str)
    else:
        end_idx = len(html_str)

    section_html = html_str[start_idx:end_idx]
    section_soup = BeautifulSoup(section_html, "html.parser")

    # Encontrar links a documentos
    seen_ids = set()
    for a in section_soup.find_all("a", href=re.compile(r"gaceta_del_senado/documento/\d+")):
        match = re.search(r"documento/(\d+)", a["href"])
        if not match:
            continue

        doc_id = match.group(1)
        if doc_id in seen_ids:
            continue

        titulo = a.get_text(strip=True)
        if not titulo or len(titulo) < 10:
            continue

        seen_ids.add(doc_id)

        # Extraer autor(es) de la(s) imagen(es) de senador(es) cercanas
        autor = _extraer_autor_senado(section_soup, doc_id, titulo)

        # Extraer partido del título
        partido = _extraer_partido(titulo)

        # Extraer turno/resolución (texto después del link del título)
        turno = _extraer_turno(a)

        # Extraer comisión del turno
        comision = _extraer_comision_turno(turno) or _extraer_comision_titulo(titulo)

        doc_url = f"{BASE_URL}/{LEGISLATURA}/gaceta_del_senado/documento/{doc_id}"

        docs.append({
            "fecha": fecha,
            "tipo": tipo_doc,
            "titulo": _limpiar(titulo)[:500],
            "autor": autor or "No identificado",
            "comision": comision or "No especificada",
            "resumen": _limpiar(turno or titulo)[:1000],
            "url": doc_url,
            "url_pdf": "",
            "numero_doc": doc_id,
            "seccion": seccion_nombre,
            "camara": "Senado",
        })

    return docs


# ─────────────────────────────────────────────
# FUNCIONES DE EXTRACCIÓN
# ─────────────────────────────────────────────

def _limpiar(texto):
    """Limpia whitespace excesivo."""
    return " ".join(texto.split()) if texto else ""


def _extraer_autor_senado(soup, doc_id, titulo):
    """
    Extrae el autor del documento.
    Busca imágenes de senadores (<img alt="Sen. Nombre">) cerca del link del documento.
    Fallback: parsear el título.
    """
    # Buscar imágenes de senadores cerca del link al documento
    doc_link = soup.find("a", href=re.compile(rf"documento/{doc_id}"))
    if not doc_link:
        return _extraer_autor_de_titulo(titulo)

    # Buscar en el contenedor padre (tabla que envuelve el documento)
    parent = doc_link.parent
    for _ in range(5):
        if parent is None:
            break
        imgs = parent.find_all("img", alt=re.compile(r"Sen\.", re.IGNORECASE))
        if imgs:
            autores = []
            for img in imgs:
                nombre = img.get("alt", "").strip()
                nombre = re.sub(r"^Sen\.\s*", "", nombre, flags=re.IGNORECASE).strip()
                if nombre:
                    autores.append(nombre)
            if autores:
                return "; ".join(autores[:3])  # Max 3 autores
        parent = parent.parent

    return _extraer_autor_de_titulo(titulo)


def _extraer_autor_de_titulo(titulo):
    """Extrae autor del texto del título."""
    # Patrón: "Del Sen. Nombre Apellido, del Grupo Parlamentario..."
    match = re.search(
        r"(?:Del?|De\s+la?)\s+Sen(?:ador[a]?)?\.?\s+(.+?)(?:,\s*del\s+[Gg]rupo|,\s*con\s+|,\s*que\s+|\.\s)",
        titulo, re.IGNORECASE,
    )
    if match:
        return match.group(1).strip()[:200]

    # Patrón: "De las senadoras y de los senadores del Grupo Parlamentario de MORENA"
    match = re.search(
        r"(?:senadoras?\s+y\s+)?(?:de\s+)?(?:los|las)\s+senador[ae]?s?\s+del\s+[Gg]rupo",
        titulo, re.IGNORECASE,
    )
    if match:
        return "Grupo Parlamentario"

    return "No identificado"


def _extraer_partido(titulo):
    """Extrae partido del título."""
    match = RE_PARTIDO.search(titulo)
    if match:
        partido_raw = match.group(1).strip().lower()
        for clave, nombre in NORMALIZACION_PARTIDO.items():
            if clave in partido_raw:
                return nombre
        return match.group(1).strip()[:30]
    return ""


def _extraer_turno(link_element):
    """
    Extrae el turno/resolución que sigue al link del título.
    Busca el siguiente hermano de texto o elemento con contenido.
    """
    # El turno suele estar en un <p> o texto después del link
    next_sib = link_element.next_sibling
    while next_sib:
        if hasattr(next_sib, "get_text"):
            text = next_sib.get_text(strip=True)
            if text and len(text) > 5:
                return text[:500]
        elif isinstance(next_sib, str) and next_sib.strip():
            return next_sib.strip()[:500]
        next_sib = next_sib.next_sibling

    # Fallback: buscar en el padre
    parent = link_element.parent
    if parent:
        # Buscar el siguiente hermano del padre
        next_parent_sib = parent.next_sibling
        while next_parent_sib:
            if hasattr(next_parent_sib, "get_text"):
                text = next_parent_sib.get_text(strip=True)
                if text and len(text) > 5:
                    return text[:500]
            next_parent_sib = next_parent_sib.next_sibling

    return ""


def _extraer_comision_turno(turno):
    """Extrae comisión del texto de turno."""
    if not turno:
        return ""

    # Patrón: "Se dio turno directo a las Comisiones Unidas de X y de Y"
    match = re.search(
        r"(?:turno|turn[oóa])\s+(?:directo\s+)?a\s+(?:las?\s+)?[Cc]omisi[oó]n(?:es)?(?:\s+[Uu]nidas)?\s+(?:de\s+)?(.+?)(?:\.|;|$)",
        turno, re.IGNORECASE,
    )
    if match:
        return match.group(1).strip()[:300]

    # Patrón: "Se remitió a las Comisiones de X"
    match = re.search(
        r"(?:remiti[oó]|comunicó)\s+a\s+(?:las?\s+)?[Cc]omisi[oó]n(?:es)?\s+(?:de\s+)?(.+?)(?:\.|;|$)",
        turno, re.IGNORECASE,
    )
    if match:
        return match.group(1).strip()[:300]

    return ""


def _extraer_comision_titulo(titulo):
    """Extrae comisión mencionada en el título."""
    match = re.search(
        r"[Cc]omisi[oó]n(?:es)?\s+(?:[Uu]nidas\s+)?(?:de\s+)?(.+?)(?:\.|,\s+el\s+|,\s+con\s+|$)",
        titulo, re.IGNORECASE,
    )
    if match:
        return match.group(1).strip()[:300]
    return ""


# ─────────────────────────────────────────────
# INSERCIÓN EN BD
# ─────────────────────────────────────────────

def _insertar_documentos(conn, documentos):
    """Inserta documentos en la BD, deduplicando por (fecha + numero_doc + camara)."""
    nuevos = 0
    for doc in documentos:
        # Deduplicar por doc_id (numero_doc) del Senado + cámara
        existe = conn.execute(
            "SELECT 1 FROM gaceta WHERE numero_doc = ? AND camara = 'Senado'",
            (doc["numero_doc"],)
        ).fetchone()

        if existe:
            continue

        try:
            conn.execute("""
                INSERT INTO gaceta (fecha, tipo, titulo, autor, comision, resumen,
                                    url, url_pdf, numero_doc, fecha_scraping, camara)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                doc["fecha"],
                doc["tipo"],
                doc["titulo"],
                doc["autor"],
                doc["comision"],
                doc["resumen"],
                doc["url"],
                doc["url_pdf"],
                doc["numero_doc"],
                datetime.now().isoformat(),
                "Senado",
            ))
            nuevos += 1
            logger.info(f"    [{doc['tipo']}] {doc['titulo'][:80]}...")
        except (sqlite3.IntegrityError, ValueError):
            pass

    if nuevos > 0:
        conn.commit()

    return nuevos


# ─────────────────────────────────────────────
# SCRAPING PRINCIPAL
# ─────────────────────────────────────────────

def scrape_gaceta_senado(dias=14):
    """
    Scrapea la Gaceta del Senado para los últimos N días.
    Descubre gacetas disponibles y extrae documentos de cada una.
    """
    conn = init_db()
    todos_documentos = []
    total_requests = 0

    # 1. Descubrir gacetas disponibles (mes actual y anterior si necesario)
    hoy = datetime.now()
    meses_a_revisar = set()
    for i in range(dias):
        fecha = hoy - timedelta(days=i)
        meses_a_revisar.add((fecha.year, fecha.month))

    gacetas_disponibles = {}
    for year, month in meses_a_revisar:
        # Obtener gacetas del mes
        gacetas_mes = _descubrir_gacetas_para_mes(year, month)
        total_requests += 1
        for g in gacetas_mes:
            gacetas_disponibles[g["fecha"]] = g

    logger.info(f"Gacetas del Senado disponibles: {len(gacetas_disponibles)} en {len(meses_a_revisar)} mes(es)")

    # 2. Filtrar solo las del rango de días solicitado
    fecha_limite = (hoy - timedelta(days=dias)).strftime("%Y-%m-%d")
    gacetas_rango = {f: g for f, g in gacetas_disponibles.items() if f >= fecha_limite}

    logger.info(f"Gacetas a procesar (últimos {dias} días): {len(gacetas_rango)}")

    # 3. Procesar cada gaceta
    for fecha in sorted(gacetas_rango.keys(), reverse=True):
        gaceta = gacetas_rango[fecha]
        logger.info(f"Scrapeando Gaceta del Senado para {fecha}")

        html = fetch_page(gaceta["url"])
        total_requests += 1
        time.sleep(1.0)  # Respetar rate limit de Incapsula

        if not html:
            logger.debug(f"  No se pudo descargar la gaceta para {fecha}")
            continue

        documentos = parsear_gaceta_diaria(html, fecha)

        if documentos:
            nuevos = _insertar_documentos(conn, documentos)
            todos_documentos.extend(documentos[:nuevos] if nuevos > 0 else [])
            logger.info(f"  {fecha}: {len(documentos)} docs encontrados, {nuevos} nuevos")
        else:
            logger.debug(f"  {fecha}: 0 documentos extraídos")

    logger.info(
        f"Gaceta Senado completo: {len(todos_documentos)} documentos nuevos "
        f"({total_requests} requests)"
    )
    return todos_documentos


def _descubrir_gacetas_para_mes(year, month):
    """Descubre gacetas disponibles para un mes usando el endpoint AJAX."""
    return descubrir_gacetas_mes(year, month)


# ─────────────────────────────────────────────
# FUNCIONES DE CONSULTA
# ─────────────────────────────────────────────

def obtener_docs_senado(dias=30, tipo=None):
    """Obtiene documentos del Senado de los últimos N días."""
    conn = get_connection()
    import sqlite3
    conn.row_factory = sqlite3.Row

    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    if tipo:
        rows = conn.execute("""
            SELECT * FROM gaceta
            WHERE camara = 'Senado' AND fecha >= ? AND tipo = ?
            ORDER BY fecha DESC
        """, (fecha_limite, tipo)).fetchall()
    else:
        rows = conn.execute("""
            SELECT * FROM gaceta
            WHERE camara = 'Senado' AND fecha >= ?
            ORDER BY fecha DESC
        """, (fecha_limite,)).fetchall()

    return [dict(r) for r in rows]


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore")
    logging.basicConfig(level=logging.INFO)

    print("=== Scraper Gaceta del Senado ===\n")
    docs = scrape_gaceta_senado(dias=14)
    print(f"\nDocumentos nuevos: {len(docs)}")

    # Agrupar por tipo
    por_tipo = {}
    for d in docs:
        t = d.get("tipo", "otro")
        por_tipo[t] = por_tipo.get(t, 0) + 1

    print(f"\nPor tipo:")
    for tipo, count in sorted(por_tipo.items()):
        print(f"  {tipo}: {count}")

    print(f"\nÚltimos 10:")
    for d in docs[:10]:
        print(f"  [{d['tipo']}] {d['titulo'][:100]}")
        print(f"    Autor: {d.get('autor', 'N/A')}")
        print(f"    Comisión: {d.get('comision', 'N/A')}")
        print()
