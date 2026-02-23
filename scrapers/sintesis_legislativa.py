"""
Scraper de Síntesis Legislativa (Cámara de Diputados y Senado)
Descarga el resumen ejecutivo diario, extrae temas, clasifica por categoría.
NO aparece como fuente en el dashboard — solo boost-ea el score de Congreso.
"""

import logging
import re
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path

import requests

from config import CATEGORIAS, DATABASE

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Fuentes de síntesis
# ─────────────────────────────────────────────
SINTESIS_FUENTES = {
    "diputados": {
        "nombre": "Cámara de Diputados",
        "url": "https://comunicacion.diputados.gob.mx/sintesis/notas/whats/resumenejecutivo.pdf",
    },
    # Senado tiene WAF (Incapsula) — se intenta pero no se falla si no responde
    # "senado": {
    #     "nombre": "Senado de la República",
    #     "url": "https://comunicacionsocial.senado.gob.mx/...",
    # },
}

# ─────────────────────────────────────────────
# Extracción de texto del PDF
# ─────────────────────────────────────────────


def _descargar_pdf(url, timeout=30):
    """Descarga un PDF y retorna la ruta temporal."""
    try:
        resp = requests.get(url, timeout=timeout, headers={
            "User-Agent": "Mozilla/5.0 (compatible; FIAT-Bot/1.0)"
        })
        resp.raise_for_status()
        if "pdf" not in resp.headers.get("Content-Type", "").lower():
            logger.warning(f"Respuesta no es PDF: {resp.headers.get('Content-Type')}")
            return None
        tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        tmp.write(resp.content)
        tmp.close()
        return tmp.name
    except Exception as e:
        logger.warning(f"Error descargando PDF {url}: {e}")
        return None


def _extraer_texto_pdf(pdf_path):
    """Extrae texto de un PDF usando pdfplumber."""
    try:
        import pdfplumber
    except ImportError:
        logger.error("pdfplumber no instalado — pip install pdfplumber")
        return None, None

    try:
        pdf = pdfplumber.open(pdf_path)
    except Exception as e:
        logger.warning(f"Error abriendo PDF: {e}")
        return None, None

    # Extraer fecha de la página 2 (formato: "lunes, 23 de febrero de 2026")
    fecha = None
    if len(pdf.pages) >= 2:
        texto_p2 = pdf.pages[1].extract_text() or ""
        fecha = _parsear_fecha_sintesis(texto_p2.strip())

    # Extraer texto de todas las páginas con contenido (skip portada y fecha)
    paginas_texto = []
    for i, page in enumerate(pdf.pages):
        if i < 2:  # Skip portada y página de fecha
            continue
        text = page.extract_text()
        if text and len(text.strip()) > 50:
            paginas_texto.append(text.strip())

    pdf.close()
    texto_completo = "\n\n".join(paginas_texto)
    return fecha, texto_completo


MESES_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def _parsear_fecha_sintesis(texto):
    """Parsea 'lunes, 23 de febrero de 2026' → '2026-02-23'."""
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", texto)
    if not m:
        return None
    dia, mes_str, anio = m.group(1), m.group(2).lower(), m.group(3)
    mes = MESES_ES.get(mes_str)
    if not mes:
        return None
    return f"{anio}-{mes:02d}-{int(dia):02d}"


# ─────────────────────────────────────────────
# Clasificación por categoría legislativa
# ─────────────────────────────────────────────


def _clasificar_sintesis(texto):
    """
    Clasifica el texto de la síntesis por categoría legislativa.
    Usa keyword matching directo (igual que el clasificador NLP).
    Retorna dict {categoria_clave: {"menciones": int, "relevancia": float, "extractos": [str]}}
    """
    resultados = {}
    # Dividir en bloques por párrafos (cada nota suele ser un párrafo)
    bloques = re.split(r"\n{2,}|\n(?=[A-ZÁÉÍÓÚÑ]{3,})", texto)

    for cat_clave, cat_config in CATEGORIAS.items():
        keywords = cat_config["keywords"]
        menciones = 0
        extractos = []

        for bloque in bloques:
            bloque_lower = bloque.lower()
            kw_encontrados = 0
            for kw in keywords:
                if kw.lower() in bloque_lower:
                    kw_encontrados += 1

            if kw_encontrados >= 1:
                menciones += 1
                # Guardar extracto corto (primeros 200 chars)
                extracto = bloque.strip()[:200]
                if extracto and extracto not in extractos:
                    extractos.append(extracto)

        if menciones > 0:
            # Relevancia: más keywords encontrados = más relevante
            # Normalizar: 1 mención = 0.3, 3+ = 0.7, 6+ = 1.0
            relevancia = min(menciones / 6.0, 1.0) * 0.7 + 0.3
            resultados[cat_clave] = {
                "menciones": menciones,
                "relevancia": round(min(relevancia, 1.0), 3),
                "extractos": extractos[:5],  # Max 5 extractos
            }

    return resultados


# ─────────────────────────────────────────────
# Base de datos
# ─────────────────────────────────────────────


def _init_tabla(conn):
    """Crea la tabla sintesis_legislativa si no existe."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sintesis_legislativa (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha TEXT NOT NULL,
            fuente TEXT NOT NULL,
            categoria TEXT NOT NULL,
            menciones INTEGER DEFAULT 0,
            relevancia REAL DEFAULT 0,
            extracto TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fecha, fuente, categoria)
        )
    """)
    conn.commit()


def _guardar_resultados(conn, fecha, fuente, clasificacion):
    """Guarda los resultados de clasificación en la DB."""
    insertados = 0
    for cat_clave, datos in clasificacion.items():
        extracto = " | ".join(datos["extractos"][:3])
        try:
            conn.execute("""
                INSERT OR REPLACE INTO sintesis_legislativa
                (fecha, fuente, categoria, menciones, relevancia, extracto)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                fecha, fuente, cat_clave,
                datos["menciones"], datos["relevancia"],
                extracto[:1000],
            ))
            insertados += 1
        except Exception as e:
            logger.warning(f"Error guardando síntesis {cat_clave}: {e}")
    conn.commit()
    return insertados


# ─────────────────────────────────────────────
# Score: boost para Congreso
# ─────────────────────────────────────────────


def obtener_boost_sintesis(categoria_clave, dias=3):
    """
    Calcula un boost 0-30 para el score de Congreso basado en la
    síntesis legislativa. Si la categoría aparece mencionada en la
    síntesis reciente, le da un empujón al score de Congreso.

    Lógica:
    - Revisa los últimos 3 días de síntesis
    - Cada día con menciones contribuye puntos según relevancia
    - Boost máximo: 30 puntos (se suma al score_congreso antes del cap)
    - Sin menciones → 0 (no afecta)
    """
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT fecha, menciones, relevancia FROM sintesis_legislativa
            WHERE categoria = ?
            AND fecha >= date('now', ? || ' days')
            ORDER BY fecha DESC
        """, (categoria_clave, f"-{dias}")).fetchall()
        conn.close()
    except sqlite3.OperationalError:
        return 0.0

    if not rows:
        return 0.0

    hoy = datetime.now()
    boost = 0.0
    for row in rows:
        try:
            fecha = datetime.strptime(row["fecha"], "%Y-%m-%d")
        except (ValueError, TypeError):
            continue
        dias_atras = (hoy - fecha).days
        # Decay rápido: síntesis de hoy = 1.0, ayer = 0.6, antier = 0.35
        peso_temporal = 2.0 ** (-dias_atras / 1.5)
        boost += 15.0 * row["relevancia"] * peso_temporal

    return min(round(boost, 2), 30.0)


# ─────────────────────────────────────────────
# Función principal
# ─────────────────────────────────────────────


def scrape_sintesis_legislativa():
    """
    Descarga la síntesis legislativa del día, clasifica por categoría,
    guarda en DB. Retorna resumen de lo procesado.
    """
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    _init_tabla(conn)

    resultado = {"fuentes_procesadas": 0, "categorias_detectadas": 0}

    for fuente_key, fuente_config in SINTESIS_FUENTES.items():
        url = fuente_config["url"]
        nombre = fuente_config["nombre"]
        logger.info(f"Descargando síntesis: {nombre}")

        # Descargar PDF
        pdf_path = _descargar_pdf(url)
        if not pdf_path:
            logger.warning(f"No se pudo descargar síntesis de {nombre}")
            continue

        # Extraer texto
        fecha, texto = _extraer_texto_pdf(pdf_path)
        if not texto:
            logger.warning(f"No se pudo extraer texto de {nombre}")
            # Limpiar archivo temporal
            try:
                Path(pdf_path).unlink()
            except OSError:
                pass
            continue

        if not fecha:
            fecha = datetime.now().strftime("%Y-%m-%d")
            logger.info(f"Fecha no detectada, usando hoy: {fecha}")

        # Verificar si ya procesamos esta fecha/fuente
        existing = conn.execute(
            "SELECT COUNT(*) FROM sintesis_legislativa WHERE fecha=? AND fuente=?",
            (fecha, fuente_key)
        ).fetchone()[0]

        if existing > 0:
            logger.info(f"Síntesis {nombre} ({fecha}) ya procesada ({existing} categorías)")
            try:
                Path(pdf_path).unlink()
            except OSError:
                pass
            resultado["fuentes_procesadas"] += 1
            continue

        # Clasificar
        clasificacion = _clasificar_sintesis(texto)
        logger.info(
            f"Síntesis {nombre} ({fecha}): "
            f"{len(clasificacion)} categorías detectadas en {len(texto)} chars"
        )

        # Guardar
        insertados = _guardar_resultados(conn, fecha, fuente_key, clasificacion)
        resultado["fuentes_procesadas"] += 1
        resultado["categorias_detectadas"] += insertados

        # Limpiar archivo temporal
        try:
            Path(pdf_path).unlink()
        except OSError:
            pass

    conn.close()
    return resultado


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    resultado = scrape_sintesis_legislativa()
    print(f"\nResultado: {resultado}")

    # Mostrar boost por categoría
    print("\n=== Boost por categoría ===")
    for cat_key in sorted(CATEGORIAS.keys()):
        boost = obtener_boost_sintesis(cat_key)
        if boost > 0:
            print(f"  {CATEGORIAS[cat_key]['nombre']}: +{boost}")
