"""
Monitoreo de atención de las Cámaras (Senado + Diputados).

Dos fuentes oficiales que son SECRET SAUCE de FIAT:

1. comunicacionsocial.senado.gob.mx/informacion/comunicados
   Comunicados propios del Senado, publicados por la propia Coordinación
   de Comunicación Social. Señal directa de qué tema está empujando la
   cámara HOY (vs. PDF de síntesis que es el corte del día anterior).

2. comunicacionsocial.diputados.gob.mx/index.php/monitoreos/regional
   Monitoreo curado por la Cámara de notas de medios LOCALES/REGIONALES
   sobre diputados. Cada ítem incluye medio + estado + titular + URL.
   Esto nos da un heat map de atención regional legislador-por-legislador
   que no sale en medios nacionales ni en Twitter. Valor altísimo para
   el Radar de Legisladores.

Patrón arquitectónico — IMPORTANTE:
  No aparece como fuente en el dashboard ni en el desglose de Media.
  Funciona como BOOST INVISIBLE al score_congreso, igual que
  sintesis_legislativa.py.

Dos modos de uso:
  - Cabeza (pipeline 4h, <90s total): últimas 2-3 páginas.
  - Backfill profundo (script independiente): toda la historia disponible.

El backfill se corre UNA SOLA VEZ desde scripts/backfill_camara_monitoreo.py.
Jamás dentro del pipeline productivo; tardaría horas y tronaría el job.
"""

import logging
import re
import sqlite3
import time
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from db import get_connection
from nlp.clasificador import clasificar_texto

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# URLs y parámetros
# ─────────────────────────────────────────────

URL_SENADO_BASE = "https://comunicacionsocial.senado.gob.mx"
URL_SENADO_COMUNICADOS = f"{URL_SENADO_BASE}/informacion/comunicados"

URL_DIPUTADOS_REGIONAL = (
    "https://comunicacionsocial.diputados.gob.mx/index.php/monitoreos/regional"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
}

# Delay entre páginas — el sitio del Senado tiene Incapsula y Diputados
# también se pone sensible ante bursts. 2.5s es la marca que no nos tira 403.
DELAY_ENTRE_PAGINAS = 2.5

# Parseador de fechas en español ("Abr 13, 2026")
MESES_ABREV_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}

# ─────────────────────────────────────────────
# Base de datos
# ─────────────────────────────────────────────


def init_db(conn=None):
    """Crea la tabla atencion_camara si no existe."""
    close_local = False
    if conn is None:
        conn = get_connection()
        close_local = True
    conn.execute("""
        CREATE TABLE IF NOT EXISTS atencion_camara (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fuente TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            fecha TEXT,
            titular TEXT NOT NULL,
            resumen TEXT,
            medio_origen TEXT,
            estado TEXT,
            categoria_match TEXT,
            relevancia REAL DEFAULT 0,
            legisladores_mencionados TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_atencion_camara_fecha "
        "ON atencion_camara(fecha)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_atencion_camara_categoria "
        "ON atencion_camara(categoria_match)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_atencion_camara_fuente_fecha "
        "ON atencion_camara(fuente, fecha DESC)"
    )
    conn.commit()
    if close_local:
        conn.close()


# ─────────────────────────────────────────────
# Parsers
# ─────────────────────────────────────────────


def _parsear_fecha_diputados(txt):
    """'Abr 13, 2026' → '2026-04-13'. Devuelve None si no casa."""
    if not txt:
        return None
    m = re.search(r"([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})", txt.strip())
    if not m:
        return None
    mes = MESES_ABREV_ES.get(m.group(1).lower()[:3])
    if not mes:
        return None
    return f"{m.group(3)}-{mes:02d}-{int(m.group(2)):02d}"


def _parse_diputados_regional(html):
    """
    Parsea una página de monitoreo regional de Diputados.

    Cada ítem vive en un `<div class="media media-none--lg ...">` que
    contiene fecha, estado, medio, titular + link, y resumen.

    Retorna lista de dicts con keys: url, fecha, titular, resumen, medio_origen, estado.
    """
    soup = BeautifulSoup(html, "html.parser")
    items = []

    # Las tarjetas de nota están dentro del col principal (col-lg-8).
    # Hay otros `.media` en el sidebar pero sin el mismo layout.
    contenedor = soup.select_one("div.col-lg-8 > div.row") or soup

    for card in contenedor.select("div.media.media-none--lg"):
        a = card.select_one("h2.title-semibold-dark a[href]")
        if not a:
            continue
        url = a.get("href", "").strip()
        titular = a.get_text(" ", strip=True)
        if not url or not titular:
            continue

        # Fecha
        fecha_li = card.select_one("div.post-date-dark li")
        fecha_txt = fecha_li.get_text(" ", strip=True) if fecha_li else ""
        fecha = _parsear_fecha_diputados(fecha_txt)

        # Estado y medio (ambos son `.topic-box-sm`; orden: ESTADO luego MEDIO)
        etiquetas = [
            tag.get_text(" ", strip=True)
            for tag in card.select("div.topic-box-sm")
        ]
        estado = etiquetas[0] if len(etiquetas) >= 1 else None
        medio = etiquetas[1] if len(etiquetas) >= 2 else None

        # Resumen corto
        p = card.select_one("div.media-body > p")
        resumen = p.get_text(" ", strip=True)[:1500] if p else ""

        items.append({
            "url": url,
            "fecha": fecha,
            "titular": titular[:500],
            "resumen": resumen,
            "medio_origen": (medio or "").title() if medio else None,
            "estado": (estado or "").title() if estado else None,
        })

    return items


def _parse_senado_comunicados(html):
    """
    Parsea la lista de comunicados del Senado (home o listing).

    Cada comunicado es un `<h3 class="uk-h2">` con un `<a class="uk-link-reset"
    href="/informacion/comunicados/NNN-slug">TITULAR</a>`. No trae fecha
    estructurada en el listing; se aproxima a la fecha de scraping.

    Retorna lista de dicts con keys: url, fecha, titular, resumen.
    """
    soup = BeautifulSoup(html, "html.parser")
    items = []
    vistos = set()

    for h3 in soup.select("h3.uk-h2 a.uk-link-reset[href*='/informacion/comunicados/']"):
        href = h3.get("href", "").strip()
        if not href or href in vistos:
            continue
        vistos.add(href)
        titular = h3.get_text(" ", strip=True)
        if not titular:
            continue

        url_abs = urljoin(URL_SENADO_BASE, href)

        # El listing no expone fecha por ítem; usar fecha de hoy.
        # (El backfill profundo debería entrar al detalle de cada comunicado
        # para extraer fecha real — no lo hacemos en cabeza por presupuesto.)
        fecha = datetime.now().strftime("%Y-%m-%d")

        items.append({
            "url": url_abs,
            "fecha": fecha,
            "titular": titular[:500],
            "resumen": "",
            "medio_origen": None,
            "estado": None,
        })

    return items


# ─────────────────────────────────────────────
# Detección fuzzy de legisladores mencionados
# ─────────────────────────────────────────────


def _bigramas_nombre(nombre):
    """
    Extrae bigramas de tokens consecutivos del nombre como patrones de match.

    La DB de `legisladores` tiene los nombres en formatos mixtos:
    "Apellido1 Apellido2 Nombre" (estilo SIL) y también "Nombre1 Nombre2
    Apellido1 Apellido2" (estilo Diputados). Los bigramas capturan cualquier
    par consecutivo de tokens largos — independiente del orden — y son lo
    suficientemente específicos (10+ chars) para minimizar falsos positivos.

    Reglas:
    - Tokens deben ser alfa y >= 4 chars (skip "de", "la", "Ana").
    - Bigramas resultantes deben tener >= 10 chars total.
    - Cada bigrama se reusa para hacer substring-match case-insensitive.
    """
    tokens = [t for t in re.split(r"\s+", nombre.strip()) if t]
    # Preposiciones/artículos entre apellidos compuestos ("de la Luz Rivas")
    PREP = {"de", "la", "las", "los", "del", "y", "e", "san", "santa"}
    # Mantenemos tokens relevantes pero colapsamos preposiciones intermedias.
    relevantes = [
        t for t in tokens
        if len(t) >= 4 and t.isalpha() and t.lower() not in PREP
    ]
    bigramas = []
    for i in range(len(relevantes) - 1):
        bg = f"{relevantes[i]} {relevantes[i+1]}"
        if len(bg) >= 10:
            bigramas.append(bg)
    return bigramas


def _apellidos_legisladores(conn):
    """
    Carga la lista de (bigramas, nombre_completo) para match fuzzy.
    Si la tabla no existe o está vacía, devuelve [].
    """
    try:
        rows = conn.execute(
            "SELECT nombre FROM legisladores WHERE nombre IS NOT NULL"
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    pares = []
    for (nombre,) in rows:
        if not nombre:
            continue
        bigramas = _bigramas_nombre(nombre)
        if not bigramas:
            continue
        pares.append((bigramas, nombre))
    return pares


def _match_legisladores(texto, pares):
    """
    Busca legisladores en el texto mediante bigrama-match case-insensitive.
    Señal fuerte: dos tokens consecutivos del nombre en el texto. No hay
    match por apellido suelto — los apellidos genéricos (López, García)
    generan demasiados FPs.

    Devuelve hasta 3 nombres completos únicos (más de 3 es ruido).
    """
    if not pares or not texto:
        return []
    texto_low = texto.lower()
    encontrados = []
    vistos = set()
    for bigramas, nombre in pares:
        if nombre in vistos:
            continue
        for bg in bigramas:
            if bg.lower() in texto_low:
                encontrados.append(nombre)
                vistos.add(nombre)
                break
        if len(encontrados) >= 3:
            break
    return encontrados


# ─────────────────────────────────────────────
# HTTP con cortesía
# ─────────────────────────────────────────────


def _get(url, session, timeout=30):
    """GET con retry suave y detección de WAF."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=timeout, verify=True)
    except requests.RequestException as e:
        logger.warning(f"Error HTTP {url}: {e}")
        return None
    if resp.status_code == 403:
        logger.warning(f"403 (WAF) en {url} — abortando esta fuente para este run")
        return None
    if resp.status_code >= 500:
        logger.warning(f"{resp.status_code} en {url} — abortando esta fuente")
        return None
    if resp.status_code != 200:
        logger.info(f"{resp.status_code} en {url} — skip")
        return None
    return resp.text


# ─────────────────────────────────────────────
# Guardar + clasificar
# ─────────────────────────────────────────────


def _guardar_items(conn, fuente, items, pares_legisladores):
    """
    Clasifica y persiste items en atencion_camara.
    Dedup por URL (UNIQUE constraint). Retorna (nuevos, duplicados).
    """
    nuevos = 0
    duplicados = 0
    for item in items:
        # Clasificar por categoría legislativa
        cats = clasificar_texto(item["titular"], item["resumen"])
        categoria = None
        relevancia = 0.0
        if cats:
            # cats es dict {cat: score}; tomar la de score más alto
            categoria, relevancia = max(cats.items(), key=lambda kv: kv[1])

        # Fuzzy match de legisladores
        texto_match = f"{item['titular']} {item['resumen']}"
        leg = _match_legisladores(texto_match, pares_legisladores)
        leg_json = " | ".join(leg) if leg else None

        try:
            conn.execute("""
                INSERT INTO atencion_camara
                (fuente, url, fecha, titular, resumen,
                 medio_origen, estado, categoria_match, relevancia,
                 legisladores_mencionados)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                fuente, item["url"], item["fecha"], item["titular"],
                item["resumen"], item["medio_origen"], item["estado"],
                categoria, round(float(relevancia), 3), leg_json,
            ))
            nuevos += 1
        except sqlite3.IntegrityError:
            duplicados += 1
        except Exception as e:
            logger.warning(f"Error guardando item {item['url']}: {e}")
    conn.commit()
    return nuevos, duplicados


# ─────────────────────────────────────────────
# Entry points: cabeza (pipeline 4h)
# ─────────────────────────────────────────────


def scrape_senado_head(max_paginas=1):
    """
    Cabeza del Senado: home + opcionalmente primera página del listing.
    max_paginas=1 en pipeline productivo (~1-2s). El backfill usa más.
    """
    conn = get_connection()
    init_db(conn)
    pares = _apellidos_legisladores(conn)
    session = requests.Session()

    total_nuevos = 0
    total_dup = 0

    # El home ya publica los 10 comunicados más recientes.
    urls = [URL_SENADO_BASE + "/"]
    # Listing paginado para runs con max_paginas > 1
    for p in range(1, max_paginas):
        urls.append(f"{URL_SENADO_COMUNICADOS}?start={p * 10}")

    for i, url in enumerate(urls):
        if i > 0:
            time.sleep(DELAY_ENTRE_PAGINAS)
        html = _get(url, session)
        if not html:
            continue
        items = _parse_senado_comunicados(html)
        if not items:
            logger.info(f"Senado {url}: 0 items parseados")
            continue
        nuevos, dup = _guardar_items(conn, "senado_comunicados", items, pares)
        total_nuevos += nuevos
        total_dup += dup

    return {"nuevos": total_nuevos, "duplicados": total_dup}


def scrape_diputados_regional_head(max_paginas=3):
    """
    Cabeza del monitoreo regional de Diputados.
    max_paginas=3 en pipeline productivo (~30 items/página × 3 = 90 items,
    ~8s con delay de 2.5s). El backfill usa hasta 4380 páginas.
    """
    conn = get_connection()
    init_db(conn)
    pares = _apellidos_legisladores(conn)
    session = requests.Session()

    total_nuevos = 0
    total_dup = 0

    for p in range(1, max_paginas + 1):
        if p > 1:
            time.sleep(DELAY_ENTRE_PAGINAS)
        url = f"{URL_DIPUTADOS_REGIONAL}?p={p}"
        html = _get(url, session)
        if not html:
            break  # Si se cae una página, mejor cortar a pegar fuerte al WAF.
        items = _parse_diputados_regional(html)
        if not items:
            logger.info(f"Diputados regional p={p}: 0 items parseados")
            continue
        nuevos, dup = _guardar_items(conn, "diputados_regional", items, pares)
        total_nuevos += nuevos
        total_dup += dup
        # Si toda la página son duplicados, ya nos alcanzamos con lo guardado.
        if dup == len(items) and nuevos == 0:
            logger.info(f"Diputados regional p={p}: todo duplicado, cortamos")
            break

    return {"nuevos": total_nuevos, "duplicados": total_dup}


def scrape_camara_monitoreo():
    """
    Entry point del pipeline de 4h. Corre ambas fuentes en modo cabeza.
    Cualquier falla individual no tumba la función (los logs quedan).
    """
    resumen = {"senado": {"nuevos": 0, "duplicados": 0},
               "diputados_regional": {"nuevos": 0, "duplicados": 0}}

    try:
        resumen["senado"] = scrape_senado_head(max_paginas=1)
    except Exception as e:
        logger.warning(f"Senado monitoreo falló (no crítico): {e}")

    try:
        resumen["diputados_regional"] = scrape_diputados_regional_head(max_paginas=3)
    except Exception as e:
        logger.warning(f"Diputados regional falló (no crítico): {e}")

    return resumen


# ─────────────────────────────────────────────
# Boost invisible al score_congreso
# ─────────────────────────────────────────────


def obtener_boost_atencion_camara(categoria_clave, dias=7):
    """
    Boost 0-25 al score_congreso de una categoría, basado en qué tanto
    el monitoreo regional + comunicados del Senado han empujado el tema
    en los últimos `dias`.

    Lógica:
    - Cuenta hits únicos por URL en atencion_camara para la categoría.
    - Decay exponencial de 3 días (hits de hoy pesan más que los de hace una semana).
    - Cada hit contribuye hasta ~2 pts; max boost 25.
    - No aparece en el desglose del dashboard (secret sauce).
    """
    try:
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT fecha, relevancia FROM atencion_camara
            WHERE categoria_match = ?
              AND fecha IS NOT NULL
              AND fecha >= date('now', ?)
        """, (categoria_clave, f"-{dias} days")).fetchall()
    except (sqlite3.OperationalError, ValueError):
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
        dias_atras = max((hoy - fecha).days, 0)
        peso_temporal = 2.0 ** (-dias_atras / 3.0)
        # Relevancia viene del clasificador (0.0 si no clasificó). Fallback
        # mínimo para no anular el hit cuando la relevancia fue borderline.
        rel = max(float(row["relevancia"] or 0.0), 0.3)
        boost += 2.0 * rel * peso_temporal

    return min(round(boost, 2), 25.0)


# ─────────────────────────────────────────────
# CLI para pruebas locales
# ─────────────────────────────────────────────


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    print("→ Cabeza: Senado (1 página) + Diputados regional (3 páginas)")
    resumen = scrape_camara_monitoreo()
    print(f"\nResultado: {resumen}")

    print("\n=== Boost atención Cámara por categoría (últimos 7 días) ===")
    from config import CATEGORIAS
    for cat_key in sorted(CATEGORIAS.keys()):
        boost = obtener_boost_atencion_camara(cat_key)
        if boost > 0:
            print(f"  {CATEGORIAS[cat_key]['nombre']}: +{boost}")
