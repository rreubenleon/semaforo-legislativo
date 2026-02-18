"""
Scraper de la Gaceta Parlamentaria - Cámara de Diputados
Extrae: iniciativas, puntos de acuerdo, dictámenes, proposiciones
Datos: fecha, autor, comisión, tipo, resumen del texto

Estructura real del sitio:
- Frameset principal en gaceta.diputados.gob.mx
- Contenido diario en /Gaceta/66/YYYY/mes/YYYYMMDD.html
- Documentos legislativos inline en <p> tags numerados
- Secciones marcadas con <a class="Seccion">
- Items del índice con <a class="Indice">
"""

import re
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
    "iniciativa": re.compile(r"iniciativa\s+con\s+proyecto", re.IGNORECASE),
    "punto_de_acuerdo": re.compile(r"punto\s+de\s+acuerdo|proposici[oó]n\s+con\s+punto", re.IGNORECASE),
    "dictamen": re.compile(r"dictam[eé]n", re.IGNORECASE),
    "minuta": re.compile(r"minuta", re.IGNORECASE),
}

# Patrón para extraer autor (Suscrita por... / Presentada por...)
RE_AUTOR = re.compile(
    r"(?:suscrita?|presentada?|que\s+presenta)\s+por\s+(?:el|la|los|las)?\s*"
    r"(?:C\.\s*)?(?:diputad[oa]|senador[a]?)?\s*(.+?)(?:,\s*del\s+[Gg]rupo|,\s*e\s+integrantes|\.\s)",
    re.IGNORECASE,
)

# Patrón para extraer comisión de turno
RE_COMISION = re.compile(
    r"(?:se\s+turn[oóa]|turnada?\s+a)\s+(?:la|las)?\s*[Cc]omisi[oó]n(?:es)?\s+(?:de\s+)?(.+?)(?:\.|,\s+con|\s+para)",
    re.IGNORECASE,
)


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


def construir_url_gaceta(fecha):
    """
    Construye la URL de la Gaceta para una fecha dada.
    Formato: /Gaceta/66/2026/feb/20260213.html
    """
    mes_str = MESES_URL.get(fecha.month, "ene")
    fecha_str = fecha.strftime("%Y%m%d")
    return f"{BASE_URL}/Gaceta/{LEGISLATURA}/{fecha.year}/{mes_str}/{fecha_str}.html"


def obtener_gaceta_del_dia(fecha):
    """Descarga el HTML de la Gaceta de un día específico."""
    url = construir_url_gaceta(fecha)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30, verify=False)
        resp.encoding = "iso-8859-1"
        if resp.status_code == 200 and len(resp.text) > 500:
            return resp.text, url
        logger.debug(f"Gaceta no disponible para {fecha.strftime('%Y-%m-%d')} ({resp.status_code})")
    except requests.RequestException as e:
        logger.warning(f"Error accediendo Gaceta {fecha.strftime('%Y-%m-%d')}: {e}")
    return None, None


def extraer_documentos_legislativos(html, url_gaceta, fecha):
    """
    Parsea el HTML completo de una Gaceta diaria.
    Extrae documentos legislativos del cuerpo (no solo del índice).

    Los documentos aparecen como párrafos numerados:
      <span class="Negritas">1. </span> Iniciativa con proyecto de decreto...
      Suscrita por la diputada Fulana, del Grupo Parlamentario de...
    """
    soup = BeautifulSoup(html, "html.parser")
    documentos = []
    fecha_str = fecha.strftime("%Y-%m-%d")

    # Extraer fecha del título de la Gaceta si disponible
    titulo_gaceta = soup.find("div", id="NGaceta")
    if titulo_gaceta:
        fecha_extraida = _extraer_fecha_de_texto(titulo_gaceta.get_text())
        if fecha_extraida:
            fecha_str = fecha_extraida

    # Estrategia 1: Buscar párrafos numerados con contenido legislativo
    parrafos = soup.find_all("p")
    buffer_doc = None

    for p in parrafos:
        texto = p.get_text(strip=True)
        if not texto or len(texto) < 20:
            continue

        # Detectar inicio de documento legislativo (numerado)
        tiene_negrita = p.find("span", class_="Negritas")
        es_inicio_numerado = tiene_negrita and re.match(r"^\d+\.\s*", texto)

        # Clasificar tipo
        tipo = _clasificar_tipo(texto)

        if es_inicio_numerado and tipo:
            # Guardar documento previo si existe
            if buffer_doc:
                documentos.append(buffer_doc)

            # Extraer número real del documento del texto (ej: "2." → "2")
            num_match = re.match(r"^(\d+)\.\s*", texto)
            numero_doc = num_match.group(1) if num_match else str(len(documentos) + 1)

            buffer_doc = {
                "tipo": tipo,
                "titulo": texto[:500],
                "autor": _extraer_autor_inline(texto),
                "comision": "",
                "resumen": texto[:1000],
                "fecha": fecha_str,
                "numero_doc": numero_doc,
                "url": url_gaceta,
                "url_pdf": "",
            }
        elif buffer_doc:
            # Acumular texto al documento actual (autor, comisión, etc)
            buffer_doc["resumen"] = (buffer_doc["resumen"] + " " + texto)[:1000]

            # Intentar extraer autor si no se encontró
            if buffer_doc["autor"] == "No identificado":
                autor = _extraer_autor_inline(texto)
                if autor != "No identificado":
                    buffer_doc["autor"] = autor

            # Intentar extraer comisión
            if not buffer_doc["comision"]:
                comision = _extraer_comision_inline(texto)
                if comision:
                    buffer_doc["comision"] = comision

            # Detectar que salimos del bloque (nuevo tipo de sección, otro numbered item sin tipo legislativo)
            if tiene_negrita and re.match(r"^\d+\.\s*", texto) and not tipo:
                documentos.append(buffer_doc)
                buffer_doc = None

    # Guardar último documento
    if buffer_doc:
        documentos.append(buffer_doc)

    # Estrategia 2: Items del índice que son legislativos
    items_indice = soup.find_all("a", class_="Indice")
    for item in items_indice:
        texto = item.get_text(strip=True)
        tipo = _clasificar_tipo(texto)
        if tipo:
            # Verificar que no sea duplicado
            titulo_corto = texto[:100]
            ya_existe = any(titulo_corto in d["titulo"] for d in documentos)
            if not ya_existe:
                # Extraer número del documento si existe en el texto
                idx_match = re.match(r"^(\d+)\.\s*", texto)
                idx_num = idx_match.group(1) if idx_match else ""

                # Obtener href real del índice
                href = item.get("href", "")
                url_doc = f"{url_gaceta}{href}" if href else url_gaceta

                # Verificar si href apunta a un PDF
                url_pdf = ""
                if href and (".pdf" in href.lower() or "/PDF/" in href):
                    url_pdf = url_doc

                documentos.append({
                    "tipo": tipo,
                    "titulo": texto[:500],
                    "autor": _extraer_autor_inline(texto),
                    "comision": _extraer_comision_inline(texto),
                    "resumen": texto[:1000],
                    "fecha": fecha_str,
                    "numero_doc": idx_num,
                    "url": url_doc,
                    "url_pdf": url_pdf,
                })

    # Buscar links PDF en la página para asignarlos a documentos sin url_pdf
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if ".pdf" in href.lower() or "/PDF/" in href:
            # Normalizar URL
            if href.startswith("/"):
                pdf_url = f"{BASE_URL}{href}"
            elif not href.startswith("http"):
                pdf_url = f"{BASE_URL}/{href}"
            else:
                pdf_url = href
            pdf_text = a_tag.get_text(strip=True)[:200].lower()

            # Intentar emparejar con documentos sin PDF
            for doc in documentos:
                if doc.get("url_pdf"):
                    continue  # Ya tiene PDF
                doc_num = doc.get("numero_doc", "")
                doc_titulo = doc["titulo"].lower()[:100]
                # Match por número de documento en el texto del link
                if doc_num and re.search(rf"\b{doc_num}\b", pdf_text):
                    doc["url_pdf"] = pdf_url
                    break
                # Match por similitud de título
                if len(doc_titulo) > 20 and doc_titulo[:40] in pdf_text:
                    doc["url_pdf"] = pdf_url
                    break

    logger.info(f"  {fecha_str}: {len(documentos)} documentos legislativos encontrados")
    return documentos


def _clasificar_tipo(texto):
    """Clasifica el tipo de documento por su texto."""
    for tipo, patron in TIPO_PATTERNS.items():
        if patron.search(texto):
            return tipo
    return None


def _extraer_autor_inline(texto):
    """Extrae autor de un bloque de texto inline."""
    match = RE_AUTOR.search(texto)
    if match:
        return match.group(1).strip()[:200]
    return "No identificado"


def _extraer_comision_inline(texto):
    """Extrae comisión de turno de un bloque de texto inline."""
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
        r"(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+(\d{4})",
        texto, re.IGNORECASE,
    )
    if match:
        dia = match.group(1).zfill(2)
        mes = meses.get(match.group(2).lower(), "01")
        anio = match.group(3)
        return f"{anio}-{mes}-{dia}"
    return None


def scrape_gaceta_rango(dias=7):
    """
    Scrapea la Gaceta Parlamentaria para un rango de días.
    Retorna lista de documentos encontrados.
    """
    conn = init_db()
    todos_documentos = []

    for i in range(dias):
        fecha = datetime.now() - timedelta(days=i)
        logger.info(f"Scrapeando Gaceta para {fecha.strftime('%Y-%m-%d')}")

        html, url_gaceta = obtener_gaceta_del_dia(fecha)
        if html is None:
            continue

        documentos = extraer_documentos_legislativos(html, url_gaceta, fecha)

        for doc in documentos:
            # Verificar si ya existe en la BD (por fecha + tipo + numero_doc)
            numero_doc = doc.get("numero_doc", "")
            existe = conn.execute(
                "SELECT 1 FROM gaceta WHERE fecha = ? AND tipo = ? AND numero_doc = ?",
                (doc["fecha"], doc["tipo"], numero_doc)
            ).fetchone()
            if existe:
                continue

            registro = {
                "fecha": doc["fecha"],
                "tipo": doc["tipo"],
                "titulo": doc["titulo"],
                "autor": doc["autor"],
                "comision": doc["comision"] or "No especificada",
                "resumen": doc["resumen"],
                "url": doc["url"],
                "url_pdf": doc.get("url_pdf", ""),
                "numero_doc": numero_doc,
                "fecha_scraping": datetime.now().isoformat(),
            }

            try:
                conn.execute("""
                    INSERT INTO gaceta (fecha, tipo, titulo, autor, comision, resumen, url, url_pdf, numero_doc, fecha_scraping)
                    VALUES (:fecha, :tipo, :titulo, :autor, :comision, :resumen, :url, :url_pdf, :numero_doc, :fecha_scraping)
                """, registro)
                conn.commit()
                todos_documentos.append(registro)
                logger.info(f"    [{doc['tipo']}] #{numero_doc} {doc['titulo'][:80]}...")
            except sqlite3.IntegrityError:
                pass

    conn.close()
    logger.info(f"Total documentos nuevos: {len(todos_documentos)}")
    return todos_documentos


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
    print("=== Scraper Gaceta Parlamentaria ===")
    docs = scrape_gaceta_rango(dias=3)
    print(f"\nDocumentos encontrados: {len(docs)}")
    for d in docs[:10]:
        print(f"  [{d['tipo']}] {d['titulo'][:100]}")
        print(f"    Autor: {d['autor']}")
        print(f"    Comisión: {d['comision']}")
        print()
