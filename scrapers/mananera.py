"""
Scraper de Conferencias Matutinas - Presidenta Claudia Sheinbaum Pardo
Extrae versiones estenográficas de gob.mx y busca menciones por categoría.

Fuente: https://www.gob.mx/presidencia/es/archivo/articulos
Ventana: Últimos 14 días
"""

import re
import time
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import subprocess

try:
    import cloudscraper
    _scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "desktop": True}
    )
except ImportError:
    import requests
    _scraper = requests.Session()

from bs4 import BeautifulSoup

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CATEGORIAS, DATABASE

logger = logging.getLogger(__name__)

BASE_URL = "https://www.gob.mx"

# Patrón de URL para conferencias matutinas (predecible por fecha)
CONF_URL_TEMPLATE = (
    "https://www.gob.mx/presidencia/es/articulos/"
    "version-estenografica-conferencia-de-prensa-de-la-presidenta-"
    "claudia-sheinbaum-pardo-del-{dia}-de-{mes}-de-{anio}"
)


def _fetch_robust(url, timeout=30, max_retries=2):
    """
    Descarga una URL con fallback: cloudscraper → curl subprocess.
    Maneja el WAF de gob.mx que puede devolver "Challenge Validation".
    """
    # Intento 1: cloudscraper / requests
    for attempt in range(max_retries):
        try:
            resp = _scraper.get(url, timeout=timeout)
            if resp.status_code == 200 and len(resp.text) > 5000:
                return resp.text
            if "Challenge" in resp.text[:300]:
                logger.debug(f"    WAF challenge en intento {attempt+1}")
                time.sleep(3)
                continue
            if resp.status_code == 404:
                return None
        except Exception:
            pass
        time.sleep(2)

    # Intento 2: curl como subprocess (a veces pasa WAFs)
    try:
        result = subprocess.run(
            [
                "curl", "-sL",
                "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "-H", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "-H", "Accept-Language: es-MX,es;q=0.9",
                "--max-time", str(timeout),
                url,
            ],
            capture_output=True, text=True, timeout=timeout + 5,
        )
        if result.returncode == 0 and len(result.stdout) > 5000:
            return result.stdout
    except Exception as e:
        logger.debug(f"    curl fallback falló: {e}")

    return None

# Regex para detectar cuando habla la Presidenta
RE_CSP_LABEL = re.compile(
    r"PRESIDENTA\s+(?:DE\s+(?:LA\s+REPÚBLICA|LOS\s+ESTADOS\s+UNIDOS\s+MEXICANOS|MÉXICO))?"
    r"[,:]?\s*(?:CLAUDIA\s+SHEINBAUM\s+PARDO)?[:\s]",
    re.IGNORECASE,
)

# Regex para detectar cuando habla otro ponente (marca fin de bloque CSP)
RE_OTHER_SPEAKER = re.compile(
    r"^(?:SECRETARI[OA]|MINISTR[OA]|PRESIDENTE\s+(?:DE\s+LA\s+MESA)|"
    r"DIPUTAD[OA]|SENADOR[A]?|PERIODISTA|PREGUNTA|INTERLOCUTOR[A]?|"
    r"DIRECTORA?|SUBSECRETARI[OA]|COMISIONAD[OA]|COORDINADOR[A]?|"
    r"GENERAL|ALMIRANTE|GOBERNADOR[A]?|JEFE|VOCERO)\b",
    re.IGNORECASE,
)

# Meses en español: nombre → número (para parsear URLs)
MESES_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}
# Diccionario inverso: número → nombre (para construir URLs)
NUM_A_MES = {v: k for k, v in MESES_ES.items()}


# ─────────────────────────────────────────────
# BASE DE DATOS
# ─────────────────────────────────────────────

def init_db():
    """Crea la tabla de mananera si no existe."""
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mananera (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha TEXT NOT NULL,
            categoria TEXT NOT NULL,
            fragmento TEXT NOT NULL,
            url TEXT NOT NULL,
            fecha_scraping TEXT NOT NULL,
            UNIQUE(fecha, categoria, url)
        )
    """)
    conn.commit()
    return conn


# ─────────────────────────────────────────────
# DESCUBRIMIENTO DE CONFERENCIAS
# ─────────────────────────────────────────────

def descubrir_conferencias(dias=14):
    """
    Construye URLs de conferencias matutinas para los últimos N días.

    Las conferencias siguen un patrón de URL predecible:
    .../version-estenografica-conferencia-de-prensa-...-del-DD-de-MES-de-YYYY

    Verifica existencia con HEAD/GET y filtra fines de semana donde no hay conferencia.
    Usa cloudscraper para evitar el WAF de gob.mx.
    """
    conferencias = []

    for i in range(dias):
        fecha = datetime.now() - timedelta(days=i)

        # Saltar sábados y domingos (no hay conferencia)
        if fecha.weekday() in (5, 6):
            continue

        mes_str = NUM_A_MES.get(fecha.month, "enero")
        url = CONF_URL_TEMPLATE.format(
            dia=fecha.day,
            mes=mes_str,
            anio=fecha.year,
        )

        # Verificar que la URL existe
        html = _fetch_robust(url, timeout=20)
        if html:
            titulo = (
                f"Versión estenográfica. Conferencia de prensa de la presidenta "
                f"Claudia Sheinbaum Pardo del {fecha.day} de {mes_str} de {fecha.year}"
            )
            conferencias.append({
                "url": url,
                "titulo": titulo,
                "fecha_str": fecha.strftime("%Y-%m-%d"),
                "_html": html,  # Cache para no re-descargar
            })
            logger.info(f"    Encontrada: {fecha.strftime('%Y-%m-%d')}")
        else:
            logger.debug(f"    No disponible: {fecha.strftime('%Y-%m-%d')}")

        time.sleep(2.0)  # Rate limiting generoso para gob.mx

    logger.info(f"  Conferencias descubiertas: {len(conferencias)}")
    return conferencias


def _extraer_fecha_de_url(url):
    """Extrae fecha de una URL con patrón del-DD-de-MES-de-YYYY."""
    match = re.search(
        r"del?-(\d{1,2})-de-(\w+)-de-(\d{4})",
        url, re.IGNORECASE,
    )
    if match:
        dia = int(match.group(1))
        mes_str = match.group(2).lower()
        anio = int(match.group(3))
        mes = MESES_ES.get(mes_str)
        if mes:
            try:
                return datetime(anio, mes, dia)
            except ValueError:
                pass
    return None


def _extraer_fecha_de_titulo(titulo):
    """Extrae fecha del título de una conferencia."""
    match = re.search(
        r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})",
        titulo, re.IGNORECASE,
    )
    if match:
        dia = int(match.group(1))
        mes_str = match.group(2).lower()
        anio = int(match.group(3))
        mes = MESES_ES.get(mes_str)
        if mes:
            try:
                return datetime(anio, mes, dia)
            except ValueError:
                pass
    return None


# ─────────────────────────────────────────────
# EXTRACCIÓN DE BLOQUES CSP
# ─────────────────────────────────────────────

def extraer_bloques_csp(html):
    """
    Parsea el HTML de una versión estenográfica y extrae los bloques
    de texto donde habla la Presidenta CSP.

    Las versiones estenográficas usan <strong> para marcar los ponentes.
    Estrategia:
    1. Buscar <strong> con patrón CSP → inicio de bloque
    2. Acumular texto hasta encontrar otro <strong> con otro ponente
    """
    soup = BeautifulSoup(html, "html.parser")

    # Buscar el contenido principal
    contenido = soup.find("div", class_=re.compile(r"article|content|entry|body", re.IGNORECASE))
    if not contenido:
        contenido = soup.find("main") or soup.body or soup

    bloques_csp = []
    bloque_actual = []
    en_bloque_csp = False

    # Iterar sobre todos los elementos hijos
    for elem in contenido.descendants:
        if elem.name in ("strong", "b"):
            texto_strong = elem.get_text(strip=True)

            if RE_CSP_LABEL.search(texto_strong):
                # Inicio de bloque CSP
                if en_bloque_csp and bloque_actual:
                    # Guardar bloque anterior
                    bloques_csp.append(" ".join(bloque_actual))
                bloque_actual = []
                en_bloque_csp = True

            elif en_bloque_csp and RE_OTHER_SPEAKER.search(texto_strong):
                # Otro ponente habla → fin del bloque CSP
                if bloque_actual:
                    bloques_csp.append(" ".join(bloque_actual))
                    bloque_actual = []
                en_bloque_csp = False

        elif en_bloque_csp and elem.name is None:
            # Es un nodo de texto
            texto = elem.strip() if isinstance(elem, str) else ""
            if texto and len(texto) > 3:
                bloque_actual.append(texto)

    # Guardar último bloque si quedó abierto
    if en_bloque_csp and bloque_actual:
        bloques_csp.append(" ".join(bloque_actual))

    # Limpiar etiquetas de ponente al inicio de cada bloque
    bloques_limpios = []
    for bloque in bloques_csp:
        bloque = RE_CSP_LABEL.sub("", bloque).strip()
        if bloque:
            bloques_limpios.append(bloque)
    bloques_csp = bloques_limpios

    # Fallback: si no encontramos bloques con <strong>, intentar por texto plano
    if not bloques_csp:
        bloques_csp = _extraer_bloques_texto_plano(contenido.get_text())

    logger.debug(f"    Bloques CSP extraídos: {len(bloques_csp)}")
    return bloques_csp


def _extraer_bloques_texto_plano(texto):
    """
    Fallback: extrae bloques CSP del texto plano cuando no hay markup.
    Busca el patrón de nombre completo como delimitador.
    """
    bloques = []
    partes = re.split(
        r"(?:PRESIDENTA\s+(?:DE\s+(?:LA\s+REPÚBLICA|MÉXICO|LOS\s+ESTADOS))?[,:]?\s*"
        r"(?:CLAUDIA\s+SHEINBAUM\s+PARDO)?[:\s])",
        texto, flags=re.IGNORECASE,
    )

    for i, parte in enumerate(partes):
        if i == 0:
            continue  # Antes del primer bloque CSP

        # Cortar en el siguiente ponente
        match_otro = RE_OTHER_SPEAKER.search(parte)
        if match_otro:
            bloque = parte[:match_otro.start()].strip()
        else:
            bloque = parte.strip()

        if bloque and len(bloque) > 50:
            bloques.append(bloque)

    return bloques


# ─────────────────────────────────────────────
# CLASIFICACIÓN POR CATEGORÍA
# ─────────────────────────────────────────────

def buscar_menciones_por_categoria(bloques):
    """
    Para cada categoría legislativa, busca menciones en los bloques CSP.
    Retorna dict {categoria_clave: fragmento_texto} con el mejor match.
    """
    menciones = {}

    for cat_clave, cat_config in CATEGORIAS.items():
        keywords = cat_config["keywords"]
        mejor_fragmento = None
        mejor_score = 0

        for bloque in bloques:
            fragmento, score = _extraer_fragmento(bloque, keywords)
            if score > mejor_score:
                mejor_score = score
                mejor_fragmento = fragmento

        if mejor_fragmento and mejor_score >= 2:
            # Mínimo 2 keyword hits para considerar relevante
            menciones[cat_clave] = mejor_fragmento
            logger.debug(f"    [{cat_clave}] score={mejor_score}: {mejor_fragmento[:80]}...")

    return menciones


def _extraer_fragmento(bloque, keywords):
    """
    Divide un bloque en oraciones, puntúa por keyword hits,
    y retorna la mejor oración + contexto (~300-400 chars).
    """
    # Dividir en oraciones
    oraciones = re.split(r'(?<=[.!?])\s+', bloque)
    if not oraciones:
        return None, 0

    # Puntuar cada oración
    scores = []
    for i, oracion in enumerate(oraciones):
        oracion_lower = oracion.lower()
        hits = sum(1 for kw in keywords if kw.lower() in oracion_lower)
        scores.append((i, hits, oracion))

    # Encontrar la oración con más hits
    scores.sort(key=lambda x: x[1], reverse=True)
    if scores[0][1] == 0:
        return None, 0

    mejor_idx = scores[0][0]
    mejor_hits = scores[0][1]

    # Construir fragmento: oración anterior + oración principal + oración siguiente
    inicio = max(0, mejor_idx - 1)
    fin = min(len(oraciones), mejor_idx + 2)

    fragmento_oraciones = oraciones[inicio:fin]
    fragmento = " ".join(fragmento_oraciones)

    # Truncar a ~400 chars si es necesario
    if len(fragmento) > 450:
        # Intentar cortar en punto
        corte = fragmento[:450].rfind(".")
        if corte > 200:
            fragmento = fragmento[:corte + 1]
        else:
            fragmento = fragmento[:400] + "..."

    # Limpiar espacios extras
    fragmento = re.sub(r'\s+', ' ', fragmento).strip()

    return fragmento, mejor_hits


# ─────────────────────────────────────────────
# SCORING: señal mañanera para fórmula FIAT
# ─────────────────────────────────────────────

def obtener_score_mananera(categoria_clave, dias=14):
    """
    Calcula un score 0-100 que mide la intensidad con la que
    la Presidenta ha hablado de este tema en sus conferencias.

    Lógica:
    - Cada mención en los últimos 14 días suma puntos.
    - Menciones más recientes pesan más (decay exponencial).
    - 1 mención reciente ≈ 40 pts, 3+ menciones ≈ 80-100 pts.
    - Sin menciones → 0 (la Presidenta no ha tocado el tema).

    Esto funciona como señal predictiva: cuando CSP habla de un
    tema, es muy probable que el Congreso actúe en días/semanas.
    """
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        rows = conn.execute("""
            SELECT fecha, fragmento FROM mananera
            WHERE categoria = ?
            AND fecha >= date('now', ? || ' days')
            ORDER BY fecha DESC
        """, (categoria_clave, f"-{dias}")).fetchall()

        conn.close()
    except sqlite3.OperationalError:
        return 0.0  # Tabla no existe aún

    if not rows:
        return 0.0

    # Scoring con decay por antigüedad
    hoy = datetime.now()
    score = 0.0

    for row in rows:
        try:
            fecha_mencion = datetime.strptime(row["fecha"], "%Y-%m-%d")
        except (ValueError, TypeError):
            continue

        dias_atras = (hoy - fecha_mencion).days

        # Decay exponencial: mención de hoy = 1.0, de hace 7 días = 0.5, de hace 14 = 0.25
        peso_temporal = 2.0 ** (-dias_atras / 7.0)

        # Bonus por longitud del fragmento (mención sustantiva vs mención tangencial)
        frag_len = len(row["fragmento"]) if row["fragmento"] else 0
        peso_sustancia = min(frag_len / 300.0, 1.0)  # Fragmentos >300 chars = peso completo

        score += 35.0 * peso_temporal * peso_sustancia

    # Tope en 100
    return min(round(score, 2), 100.0)


# ─────────────────────────────────────────────
# ORQUESTADOR PRINCIPAL
# ─────────────────────────────────────────────

def scrape_mananeras(dias=14):
    """
    Pipeline completo:
    1. Descubrir conferencias de los últimos N días
    2. Descargar cada versión estenográfica
    3. Extraer bloques donde habla CSP
    4. Buscar menciones por categoría
    5. Guardar en BD
    """
    conn = init_db()
    total_menciones = 0

    logger.info(f"Scraping conferencias matutinas (últimos {dias} días)")

    # 1. Descubrir conferencias
    conferencias = descubrir_conferencias(dias=dias)

    if not conferencias:
        logger.info("  No se encontraron conferencias matutinas")
        conn.close()
        return {"conferencias": 0, "menciones": 0}

    for conf in conferencias:
        url = conf["url"]
        fecha_str = conf["fecha_str"]
        logger.info(f"  Procesando: {conf['titulo'][:80]}...")

        # 2. Usar HTML cacheado (descargado durante descubrimiento) o re-descargar
        html = conf.get("_html")
        if not html:
            html = _fetch_robust(url, timeout=30)

        if not html or len(html) < 5000:
            logger.debug(f"    Contenido insuficiente para {fecha_str}")
            continue

        # 3. Extraer bloques CSP
        bloques = extraer_bloques_csp(html)
        if not bloques:
            logger.debug(f"    Sin bloques CSP detectados")
            continue

        logger.info(f"    {len(bloques)} bloques CSP, {sum(len(b) for b in bloques)} chars totales")

        # 4. Buscar menciones por categoría
        menciones = buscar_menciones_por_categoria(bloques)

        # 5. Guardar en BD
        for cat_clave, fragmento in menciones.items():
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO mananera
                        (fecha, categoria, fragmento, url, fecha_scraping)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    fecha_str,
                    cat_clave,
                    fragmento,
                    url,
                    datetime.now().isoformat(),
                ))
                total_menciones += 1
            except sqlite3.IntegrityError:
                pass

        conn.commit()
        time.sleep(1.5)  # Rate limiting entre conferencias

    conn.close()

    logger.info(f"Mañaneras: {len(conferencias)} conferencias, {total_menciones} menciones nuevas")
    return {
        "conferencias": len(conferencias),
        "menciones": total_menciones,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=== Scraper de Conferencias Matutinas CSP ===\n")

    resultado = scrape_mananeras(dias=14)
    print(f"\nResultado: {resultado}")

    # Mostrar menciones guardadas
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT fecha, categoria, fragmento, url
        FROM mananera
        ORDER BY fecha DESC
    """).fetchall()

    if rows:
        print(f"\n--- {len(rows)} menciones encontradas ---\n")
        for r in rows:
            cat_nombre = CATEGORIAS.get(r["categoria"], {}).get("nombre", r["categoria"])
            print(f"  [{r['fecha']}] {cat_nombre}")
            print(f"    «{r['fragmento'][:150]}...»")
            print(f"    {r['url'][:80]}")
            print()
    else:
        print("\nNo se encontraron menciones de CSP en las conferencias.")

    conn.close()
