"""
Scraper de la Gaceta Parlamentaria - Cámara de Diputados
Extrae: iniciativas, proposiciones, dictámenes, minutas, decretos

Arquitectura de 3 niveles:
  Nivel 1: Página principal del día (20260218.html) → descubre Anexos
  Nivel 2: Página índice de Anexo (-II.html, -III.html) → descubre sub-páginas
  Nivel 3: Sub-página de contenido (-II-1.html) → extrae documentos del TOC

Estructura real del sitio:
  - Anexo I    → Comunicaciones oficiales, iniciativas de congresos estatales
  - Anexo II   → Iniciativas (sub-páginas por partido: -II-1 a -II-N)
  - Anexo III  → Proposiciones con punto de acuerdo (-III-1 a -III-N)
  - Anexo IV+  → Dictámenes, informes, convenios (muchos en PDF)
  - Anexo S/O  → Agendas de sesión (ignoradas)
"""

import re
import time
import hashlib
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import GACETA, DATABASE

logger = logging.getLogger(__name__)

BASE_URL = GACETA["base_url"]
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

MESES_URL = {
    1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
    7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic",
}

# Legislatura actual (LXVI = 66)
LEGISLATURA = "66"

# Patrones para clasificar tipo de documento
TIPO_PATTERNS = {
    "iniciativa": re.compile(
        r"iniciativa|que\s+reforma|que\s+adiciona|que\s+expide|que\s+abroga|que\s+deroga",
        re.IGNORECASE,
    ),
    "proposicion": re.compile(
        r"proposici[oó]n|punto\s+de\s+acuerdo|con\s+punto\s+de\s+acuerdo",
        re.IGNORECASE,
    ),
    "dictamen": re.compile(r"dictam[eé]n", re.IGNORECASE),
    "minuta": re.compile(r"minuta", re.IGNORECASE),
    "decreto": re.compile(r"decreto", re.IGNORECASE),
}

# Patrón para extraer autor
RE_AUTOR = re.compile(
    r"(?:suscrita?|presentada?|que\s+presenta|a\s+cargo\s+de[l]?)\s+"
    r"(?:por\s+)?(?:el|la|los|las)?\s*"
    r"(?:C\.\s*)?(?:diputad[oa]s?|senador[ae]?s?)?\s*"
    r"(.+?)(?:,\s*(?:del|de\s+los|e\s+integrantes|y\s+suscrita)|del\s+[Gg]rupo|\.\s)",
    re.IGNORECASE,
)

# Patrón para extraer grupo parlamentario
RE_PARTIDO = re.compile(
    r"[Gg]rupo[s]?\s+[Pp]arlamentario[s]?\s+(?:del?\s+)?(.+?)(?:\.|,\s|y\s+de\s|respectivamente|$)",
    re.IGNORECASE,
)

# Patrón para extraer comisión de turno
RE_COMISION = re.compile(
    r"(?:se\s+turn[oóa]|turnada?\s+a)\s+(?:la|las)?\s*[Cc]omisi[oó]n(?:es)?\s+(?:de\s+)?(.+?)(?:\.|,\s+con|\s+para)",
    re.IGNORECASE,
)


# ─────────────────────────────────────────────
# BASE DE DATOS
# ─────────────────────────────────────────────

def init_db():
    """Crea la tabla de gaceta si no existe, con migración de columnas nuevas."""
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gaceta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha TEXT NOT NULL,
            tipo TEXT NOT NULL,
            titulo TEXT NOT NULL,
            autor TEXT,
            comision TEXT,
            resumen TEXT,
            url TEXT,
            url_pdf TEXT DEFAULT '',
            numero_doc TEXT DEFAULT '',
            fecha_scraping TEXT NOT NULL
        )
    """)
    # Migración: agregar columnas nuevas a DBs existentes
    for col, default in [("url_pdf", "''"), ("numero_doc", "''")]:
        try:
            conn.execute(f"ALTER TABLE gaceta ADD COLUMN {col} TEXT DEFAULT {default}")
        except sqlite3.OperationalError:
            pass  # Ya existe
    conn.commit()
    return conn


def _titulo_hash(titulo):
    """Hash de los primeros 200 chars del título para deduplicación."""
    return hashlib.md5(titulo[:200].lower().strip().encode()).hexdigest()


# ─────────────────────────────────────────────
# DESCARGA DE PÁGINAS
# ─────────────────────────────────────────────

def construir_url_gaceta(fecha):
    """
    Construye la URL de la Gaceta para una fecha dada.
    Formato: /Gaceta/66/2026/feb/20260213.html
    """
    mes_str = MESES_URL.get(fecha.month, "ene")
    fecha_str = fecha.strftime("%Y%m%d")
    return f"{BASE_URL}/Gaceta/{LEGISLATURA}/{fecha.year}/{mes_str}/{fecha_str}.html"


def fetch_page(url):
    """Descarga una página con manejo de errores. Retorna None si falla."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30, verify=False)
        resp.encoding = "iso-8859-1"
        if resp.status_code == 200 and len(resp.text) > 500:
            return resp.text
        return None
    except requests.RequestException as e:
        logger.debug(f"Error descargando {url}: {e}")
        return None


# ─────────────────────────────────────────────
# NIVEL 1: DESCUBRIR ANEXOS
# ─────────────────────────────────────────────

def descubrir_anexos(html, fecha_str):
    """
    Parsea la página principal de un día y descubre todos los Anexos disponibles.
    Retorna lista de dicts con {sufijo, url, tipo, es_pdf}.

    Ejemplo para 20260218:
      - {sufijo: '-II', url: '.../20260218-II.html', tipo: 'iniciativas_index', es_pdf: False}
      - {sufijo: '-III', url: '.../20260218-III.html', tipo: 'proposiciones_index', es_pdf: False}
      - {sufijo: '-IV', url: '.../20260218-IV.pdf', tipo: 'otros', es_pdf: True}
    """
    soup = BeautifulSoup(html, "html.parser")
    anexos = []
    vistos = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]

        # Buscar links que contengan la fecha del día
        if fecha_str not in href:
            continue

        # Extraer el sufijo después de la fecha (ej: -II, -III, -I-1, -S, -O)
        match = re.search(rf'{fecha_str}(-[^.]+)\.(html|pdf)', href)
        if not match:
            continue

        sufijo = match.group(1)  # ej: -II, -III, -I-1, -S, -IV
        extension = match.group(2)

        # Deduplicar
        if sufijo in vistos:
            continue
        vistos.add(sufijo)

        # Construir URL completa
        if href.startswith("http"):
            url = href
        elif href.startswith("/"):
            url = f"{BASE_URL}{href}"
        else:
            url = f"{BASE_URL}/{href}"

        es_pdf = extension == "pdf"

        # Clasificar tipo de anexo
        tipo = _clasificar_anexo(sufijo)

        anexos.append({
            "sufijo": sufijo,
            "url": url,
            "tipo": tipo,
            "es_pdf": es_pdf,
        })

    logger.info(f"  Anexos descubiertos: {len(anexos)} ({', '.join(a['sufijo'] for a in anexos)})")
    return anexos


def _clasificar_anexo(sufijo):
    """Clasifica un anexo por su sufijo."""
    sufijo_upper = sufijo.upper()

    # Anexo II (con o sin sub-número) → Iniciativas
    if sufijo_upper.startswith("-II"):
        if re.match(r'-II-\d+$', sufijo, re.IGNORECASE):
            return "iniciativas_subpagina"
        return "iniciativas_index"

    # Anexo III → Proposiciones
    if sufijo_upper.startswith("-III"):
        if re.match(r'-III-\d+$', sufijo, re.IGNORECASE):
            return "proposiciones_subpagina"
        return "proposiciones_index"

    # Anexo I → Comunicaciones oficiales
    if sufijo_upper.startswith("-I") and not sufijo_upper.startswith("-II") and not sufijo_upper.startswith("-III"):
        return "comunicaciones"

    # Agendas de sesión
    if sufijo_upper in ("-S", "-O", "-OV"):
        return "agendas"

    # Todo lo demás (IV, V, VI, VII)
    return "otros"


# ─────────────────────────────────────────────
# NIVEL 2: DESCUBRIR SUB-PÁGINAS
# ─────────────────────────────────────────────

def descubrir_subpaginas(html, fecha_str, tipo_anexo):
    """
    Parsea una página índice de Anexo (ej: -II.html) y descubre sub-páginas.
    Retorna lista de dicts con {url_html, url_pdf, partido}.
    """
    soup = BeautifulSoup(html, "html.parser")
    subpaginas = []
    pdfs = {}  # mapeo sufijo → url_pdf

    for a in soup.find_all("a", href=True):
        href = a["href"]
        texto = a.get_text(strip=True)

        if fecha_str not in href:
            continue

        # Buscar sub-páginas HTML
        match_html = re.search(rf'{fecha_str}(-(?:II|III)-\d+)\.(html)', href, re.IGNORECASE)
        if match_html:
            sufijo = match_html.group(1)
            if href.startswith("http"):
                url = href
            elif href.startswith("/"):
                url = f"{BASE_URL}{href}"
            else:
                url = f"{BASE_URL}/{href}"

            # Intentar extraer partido del texto del link
            partido = _extraer_partido_de_texto(texto)

            subpaginas.append({
                "sufijo": sufijo,
                "url_html": url,
                "url_pdf": "",
                "partido": partido,
            })

        # Buscar PDFs asociados
        match_pdf = re.search(rf'{fecha_str}(-(?:II|III)-\d+(?:-\d+)?)\.(pdf)', href, re.IGNORECASE)
        if match_pdf:
            sufijo_pdf = match_pdf.group(1)
            if href.startswith("http"):
                pdf_url = href
            elif href.startswith("/"):
                pdf_url = f"{BASE_URL}{href}"
            else:
                pdf_url = f"{BASE_URL}/{href}"

            # El sufijo del PDF tiene un trailing -1 (ej: -II-1-1.pdf para la sub-página -II-1)
            # Normalizar para emparejar con la sub-página
            sufijo_base = re.sub(r'-\d+$', '', sufijo_pdf)
            pdfs[sufijo_base] = pdf_url

    # Emparejar PDFs con sub-páginas
    for sub in subpaginas:
        if sub["sufijo"] in pdfs:
            sub["url_pdf"] = pdfs[sub["sufijo"]]

    # Deduplicar por sufijo
    vistos = set()
    resultado = []
    for sub in subpaginas:
        if sub["sufijo"] not in vistos:
            vistos.add(sub["sufijo"])
            resultado.append(sub)

    logger.info(f"    Sub-páginas: {len(resultado)} ({', '.join(s['sufijo'] for s in resultado)})")
    return resultado


def _extraer_partido_de_texto(texto):
    """Extrae el nombre del partido/grupo parlamentario del texto de un link."""
    texto_lower = texto.lower()

    partidos = {
        "morena": "Morena",
        "pan": "PAN",
        "pri": "PRI",
        "pvem": "PVEM",
        "verde ecologista": "PVEM",
        "pt": "PT",
        "trabajo": "PT",
        "movimiento ciudadano": "MC",
        "mc": "MC",
        "prd": "PRD",
    }

    for clave, nombre in partidos.items():
        if clave in texto_lower:
            return nombre

    return ""


# ─────────────────────────────────────────────
# NIVEL 3: EXTRAER DOCUMENTOS DE SUB-PÁGINA
# ─────────────────────────────────────────────

def extraer_docs_de_subpagina(html, url_subpagina, tipo_doc, partido_default, fecha_str):
    """
    Parsea una sub-página de contenido y extrae documentos del TOC.

    Estructura típica:
      <ul>
        <li><a href="#Iniciativa1">Que reforma... suscrita por diputado X del GP de Morena</a></li>
        <li><a href="#Iniciativa2">Que adiciona... presentada por diputada Y del GP del PAN</a></li>
      </ul>

    Para Proposiciones:
      <ul>
        <li><a href="#Proposicion1">Con punto de acuerdo por el que...</a></li>
      </ul>
    """
    soup = BeautifulSoup(html, "html.parser")
    documentos = []

    # Estrategia 1: TOC con <li><a href="#Iniciativa/Proposicion N">
    for li in soup.find_all("li"):
        a = li.find("a", href=True)
        if not a:
            continue

        href = a.get("href", "")
        titulo = a.get_text(strip=True)

        # Solo links internos al TOC (#IniciativaN o #ProposicionN)
        if not href.startswith("#"):
            continue

        # Filtrar títulos muy cortos o no-legislativos
        if not titulo or len(titulo) < 20:
            continue

        # Extraer número del anchor (#Iniciativa3 → 3)
        num_match = re.search(r'(\d+)$', href)
        numero_doc = num_match.group(1) if num_match else ""

        # Extraer autor
        autor = _extraer_autor(titulo)

        # Extraer partido
        partido = _extraer_partido_de_titulo(titulo) or partido_default

        # Extraer comisión
        comision = _extraer_comision(titulo)

        # Clasificar tipo del documento por su contenido
        tipo_detectado = _clasificar_tipo(titulo) or tipo_doc

        documentos.append({
            "tipo": tipo_detectado,
            "titulo": titulo[:500],
            "autor": autor,
            "partido": partido,
            "comision": comision,
            "resumen": titulo[:1000],
            "fecha": fecha_str,
            "numero_doc": numero_doc,
            "url": f"{url_subpagina}{href}",
            "url_pdf": "",
        })

    # Estrategia 2 (fallback): Buscar <h2 id="IniciativaN"> si no hay TOC
    if not documentos:
        for h2 in soup.find_all("h2", id=True):
            h2_id = h2.get("id", "")
            if not re.match(r'(Iniciativa|Proposicion|Dictamen)\d+', h2_id, re.IGNORECASE):
                continue

            titulo = h2.get_text(strip=True)
            if not titulo or len(titulo) < 20:
                continue

            num_match = re.search(r'(\d+)$', h2_id)
            numero_doc = num_match.group(1) if num_match else ""

            autor = _extraer_autor(titulo)
            partido = _extraer_partido_de_titulo(titulo) or partido_default
            comision = _extraer_comision(titulo)
            tipo_detectado = _clasificar_tipo(titulo) or tipo_doc

            documentos.append({
                "tipo": tipo_detectado,
                "titulo": titulo[:500],
                "autor": autor,
                "partido": partido,
                "comision": comision,
                "resumen": titulo[:1000],
                "fecha": fecha_str,
                "numero_doc": numero_doc,
                "url": f"{url_subpagina}#{h2_id}",
                "url_pdf": "",
            })

    return documentos


def extraer_docs_de_comunicaciones(html, url_pagina, fecha_str):
    """
    Extrae documentos legislativos del Anexo I (comunicaciones oficiales).
    Aquí pueden aparecer iniciativas de congresos estatales y comunicaciones
    que contienen documentos legislativos.
    """
    soup = BeautifulSoup(html, "html.parser")
    documentos = []

    # Buscar en el TOC links que sean legislativos
    for li in soup.find_all("li"):
        a = li.find("a", href=True)
        if not a:
            continue

        href = a.get("href", "")
        titulo = a.get_text(strip=True)

        if not href.startswith("#") or not titulo or len(titulo) < 20:
            continue

        # Solo incluir si tiene contenido legislativo
        tipo = _clasificar_tipo(titulo)
        if not tipo:
            continue

        num_match = re.search(r'(\d+)$', href)
        numero_doc = num_match.group(1) if num_match else ""

        documentos.append({
            "tipo": tipo,
            "titulo": titulo[:500],
            "autor": _extraer_autor(titulo),
            "partido": _extraer_partido_de_titulo(titulo),
            "comision": _extraer_comision(titulo),
            "resumen": titulo[:1000],
            "fecha": fecha_str,
            "numero_doc": numero_doc,
            "url": f"{url_pagina}{href}",
            "url_pdf": "",
        })

    return documentos


# ─────────────────────────────────────────────
# FUNCIONES DE EXTRACCIÓN
# ─────────────────────────────────────────────

def _clasificar_tipo(texto):
    """Clasifica el tipo de documento por su texto."""
    for tipo, patron in TIPO_PATTERNS.items():
        if patron.search(texto):
            return tipo
    return None


def _extraer_autor(texto):
    """Extrae autor de un bloque de texto."""
    match = RE_AUTOR.search(texto)
    if match:
        autor = match.group(1).strip()
        # Limpiar sufijos comunes
        autor = re.sub(r',?\s*del\s+Grupo.*$', '', autor, flags=re.IGNORECASE)
        autor = re.sub(r',?\s*e\s+integrantes.*$', '', autor, flags=re.IGNORECASE)
        return autor[:200]
    return "No identificado"


def _extraer_partido_de_titulo(titulo):
    """Extrae partido del título de un documento."""
    match = RE_PARTIDO.search(titulo)
    if match:
        partido_raw = match.group(1).strip()
        # Normalizar nombres de partidos
        normalizacion = {
            "morena": "Morena",
            "del pan": "PAN",
            "pan": "PAN",
            "acción nacional": "PAN",
            "pri": "PRI",
            "revolucionario institucional": "PRI",
            "pvem": "PVEM",
            "verde ecologista": "PVEM",
            "pt": "PT",
            "del trabajo": "PT",
            "movimiento ciudadano": "MC",
            "prd": "PRD",
        }
        partido_lower = partido_raw.lower()
        for clave, nombre in normalizacion.items():
            if clave in partido_lower:
                return nombre
        return partido_raw[:30]
    return ""


def _extraer_comision(texto):
    """Extrae comisión de turno de un bloque de texto."""
    match = RE_COMISION.search(texto)
    if match:
        return match.group(1).strip()[:300]
    return ""


def _extraer_fecha_de_texto(texto):
    """Extrae fecha en formato YYYY-MM-DD del texto de la Gaceta."""
    meses = {
        "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
        "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
        "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
    }
    match = re.search(
        r"(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|"
        r"septiembre|octubre|noviembre|diciembre)\s+de\s+(\d{4})",
        texto, re.IGNORECASE,
    )
    if match:
        dia = match.group(1).zfill(2)
        mes = meses.get(match.group(2).lower(), "01")
        anio = match.group(3)
        return f"{anio}-{mes}-{dia}"
    return None


# ─────────────────────────────────────────────
# SCRAPING PRINCIPAL
# ─────────────────────────────────────────────

def scrape_gaceta_rango(dias=7):
    """
    Scrapea la Gaceta Parlamentaria para un rango de días.
    Usa arquitectura de 3 niveles: página principal → índice de anexo → sub-páginas.
    """
    conn = init_db()
    todos_documentos = []
    total_requests = 0

    for i in range(dias):
        fecha = datetime.now() - timedelta(days=i)
        fecha_str = fecha.strftime("%Y-%m-%d")
        fecha_url = fecha.strftime("%Y%m%d")

        logger.info(f"Scrapeando Gaceta para {fecha_str}")

        # NIVEL 1: Descargar página principal del día
        url_principal = construir_url_gaceta(fecha)
        html_principal = fetch_page(url_principal)
        total_requests += 1

        if not html_principal:
            logger.debug(f"  Gaceta no disponible para {fecha_str}")
            continue

        # Descubrir anexos disponibles
        anexos = descubrir_anexos(html_principal, fecha_url)

        for anexo in anexos:
            # Ignorar PDFs y agendas de sesión
            if anexo["es_pdf"] or anexo["tipo"] == "agendas":
                continue

            # NIVEL 2: Procesar índices de Iniciativas y Proposiciones
            if anexo["tipo"] in ("iniciativas_index", "proposiciones_index"):
                tipo_doc = "iniciativa" if "iniciativas" in anexo["tipo"] else "proposicion"

                html_index = fetch_page(anexo["url"])
                total_requests += 1
                time.sleep(0.3)

                if not html_index:
                    continue

                subpaginas = descubrir_subpaginas(html_index, fecha_url, anexo["tipo"])

                # NIVEL 3: Extraer documentos de cada sub-página
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

                    # Asignar PDF del índice a todos los docs de esta sub-página
                    if sub.get("url_pdf"):
                        for doc in docs:
                            if not doc["url_pdf"]:
                                doc["url_pdf"] = sub["url_pdf"]

                    # Insertar en DB
                    nuevos = _insertar_documentos(conn, docs)
                    todos_documentos.extend(docs[:nuevos] if nuevos > 0 else [])

            # Procesar sub-páginas directas (ej: -II-1 en la página principal)
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

                nuevos = _insertar_documentos(conn, docs)
                todos_documentos.extend(docs[:nuevos] if nuevos > 0 else [])

            # Procesar comunicaciones (Anexo I) — pueden tener iniciativas estatales
            elif anexo["tipo"] == "comunicaciones":
                html_com = fetch_page(anexo["url"])
                total_requests += 1
                time.sleep(0.3)

                if not html_com:
                    continue

                docs = extraer_docs_de_comunicaciones(html_com, anexo["url"], fecha_str)

                nuevos = _insertar_documentos(conn, docs)
                todos_documentos.extend(docs[:nuevos] if nuevos > 0 else [])

    conn.close()
    logger.info(f"Scraping completo: {len(todos_documentos)} documentos nuevos ({total_requests} requests)")
    return todos_documentos


def _insertar_documentos(conn, documentos):
    """Inserta documentos en la BD, deduplicando por (fecha + titulo_hash)."""
    nuevos = 0
    for doc in documentos:
        t_hash = _titulo_hash(doc["titulo"])

        # Verificar si ya existe
        existe = conn.execute(
            "SELECT 1 FROM gaceta WHERE fecha = ? AND tipo = ? AND titulo LIKE ?",
            (doc["fecha"], doc["tipo"], doc["titulo"][:100] + "%")
        ).fetchone()

        if existe:
            continue

        registro = {
            "fecha": doc["fecha"],
            "tipo": doc["tipo"],
            "titulo": doc["titulo"],
            "autor": doc.get("autor", "No identificado"),
            "comision": doc.get("comision") or "No especificada",
            "resumen": doc.get("resumen", doc["titulo"]),
            "url": doc.get("url", ""),
            "url_pdf": doc.get("url_pdf", ""),
            "numero_doc": doc.get("numero_doc", ""),
            "fecha_scraping": datetime.now().isoformat(),
        }

        try:
            conn.execute("""
                INSERT INTO gaceta (fecha, tipo, titulo, autor, comision, resumen, url, url_pdf, numero_doc, fecha_scraping)
                VALUES (:fecha, :tipo, :titulo, :autor, :comision, :resumen, :url, :url_pdf, :numero_doc, :fecha_scraping)
            """, registro)
            conn.commit()
            nuevos += 1
            logger.info(f"    [{doc['tipo']}] {doc['titulo'][:80]}...")
        except sqlite3.IntegrityError:
            pass

    return nuevos


# ─────────────────────────────────────────────
# FUNCIONES DE CONSULTA (sin cambios)
# ─────────────────────────────────────────────

def buscar_por_categoria(keyword, dias=30):
    """Busca documentos en la BD que coincidan con un keyword."""
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    resultados = conn.execute("""
        SELECT * FROM gaceta
        WHERE fecha >= ?
          AND (titulo LIKE ? OR resumen LIKE ? OR comision LIKE ?)
        ORDER BY fecha DESC
    """, (fecha_limite, f"%{keyword}%", f"%{keyword}%", f"%{keyword}%")).fetchall()

    conn.close()
    return [dict(r) for r in resultados]


def contar_actividad_por_fecha(dias=30):
    """Retorna conteo de documentos por fecha para análisis temporal."""
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))

    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    rows = conn.execute("""
        SELECT fecha, COUNT(*) as total
        FROM gaceta
        WHERE fecha >= ?
        GROUP BY fecha
        ORDER BY fecha
    """, (fecha_limite,)).fetchall()

    conn.close()
    return {row[0]: row[1] for row in rows}


def obtener_score_congreso(categoria_keywords, dias=7):
    """Calcula un score 0-100 de actividad legislativa para una categoría."""
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))

    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    total_docs = conn.execute(
        "SELECT COUNT(*) FROM gaceta WHERE fecha >= ?", (fecha_limite,)
    ).fetchone()[0]

    docs_relevantes = 0
    for kw in categoria_keywords:
        count = conn.execute("""
            SELECT COUNT(*) FROM gaceta
            WHERE fecha >= ?
              AND (titulo LIKE ? OR resumen LIKE ? OR comision LIKE ?)
        """, (fecha_limite, f"%{kw}%", f"%{kw}%", f"%{kw}%")).fetchone()[0]
        docs_relevantes += count

    conn.close()

    if total_docs == 0:
        return 0

    ratio = min(docs_relevantes / max(total_docs * 0.1, 1), 1.0)
    return min(round(ratio * 100, 2), 100)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import warnings
    warnings.filterwarnings("ignore")
    print("=== Scraper Gaceta Parlamentaria (v2 - 3 niveles) ===\n")
    docs = scrape_gaceta_rango(dias=3)
    print(f"\nDocumentos encontrados: {len(docs)}")

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
        print(f"    URL: {d.get('url', 'N/A')[:80]}")
        print()
