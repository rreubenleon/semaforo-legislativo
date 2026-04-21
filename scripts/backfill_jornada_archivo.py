"""
Backfill histórico de La Jornada usando su archivo navegable por fecha.

Estructura del archivo:
    https://www.jornada.com.mx/{YYYY}/{MM}/{DD}/{seccion}
    → listado de notas con <a class="cabeza" href="seccion/XXXnYpol">Título</a>

Secciones incluidas: politica, economia, capital, estados, opinion, mundo.
Solo guarda headline + URL + fecha (suficiente para benchmark de señal).
NO descarga el body de cada nota (sería rate-limit masivo).

Uso:
    python scripts/backfill_jornada_archivo.py --desde 2024-08-15 --hasta 2024-10-15
    python scripts/backfill_jornada_archivo.py --caso reforma_judicial_2024
"""
import argparse
import hashlib
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
DB_PATH = ROOT / "semaforo.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

SECCIONES = ["politica", "economia", "capital", "estados", "opinion", "mundo"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}

RE_CABEZA = re.compile(
    r'<a class="cabeza"\s+href="([^"]+)"[^>]*>([^<]+)</a>',
    re.IGNORECASE | re.DOTALL,
)

# Casos preseteados (ventanas)
CASOS_VENTANAS = {
    "reforma_judicial_2024": ("2024-08-15", "2024-10-15"),
    "ley_economia_circular_2025": ("2025-10-01", "2026-01-10"),
    "ley_general_aguas_2025": ("2025-06-01", "2025-12-31"),
    "pensiones_cfe_pemex_2025": ("2025-01-01", "2025-12-31"),
}


def scrape_seccion_fecha(session: requests.Session, fecha: datetime, seccion: str) -> list[dict]:
    """Descarga una sección de La Jornada para una fecha dada, devuelve notas."""
    url = f"https://www.jornada.com.mx/{fecha.year}/{fecha.month:02d}/{fecha.day:02d}/{seccion}"
    try:
        r = session.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 404:
            return []
        if r.status_code != 200:
            logger.warning(f"  {url} → HTTP {r.status_code}")
            return []
    except requests.RequestException as e:
        logger.warning(f"  {url} → error: {e}")
        return []

    html = r.text
    notas = []
    seen = set()
    for href, titulo in RE_CABEZA.findall(html):
        # href suele ser relativo a la fecha, ej. "politica/003n1pol"
        url_nota = href
        if not url_nota.startswith("http"):
            url_nota = f"https://www.jornada.com.mx/{fecha.year}/{fecha.month:02d}/{fecha.day:02d}/{href.lstrip('/')}"
        titulo_limpio = re.sub(r"\s+", " ", titulo).strip()
        if not titulo_limpio or len(titulo_limpio) < 15:
            continue
        if url_nota in seen:
            continue
        seen.add(url_nota)
        notas.append({
            "titulo": titulo_limpio,
            "url": url_nota,
            "fecha": fecha.strftime("%Y-%m-%d"),
            "seccion": seccion,
        })
    return notas


def guardar_notas(conn: sqlite3.Connection, notas: list[dict]) -> int:
    """Inserta notas en tabla articulos si no existen (por hash)."""
    insertadas = 0
    for n in notas:
        # Hash único por (fuente, titulo, fecha) para evitar duplicados
        key = f"la_jornada|{n['titulo']}|{n['fecha']}".encode("utf-8")
        h = hashlib.md5(key).hexdigest()
        try:
            conn.execute(
                """
                INSERT INTO articulos (hash, fuente, titulo, fecha, url, resumen, categorias, peso_fuente, fecha_scraping)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    h, "la_jornada", n["titulo"], n["fecha"] + " 12:00:00", n["url"],
                    "", "", 1.1,  # peso_fuente 1.1 como RSS actual
                    datetime.now().isoformat(),
                ),
            )
            insertadas += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return insertadas


def backfill_rango(desde_str: str, hasta_str: str, delay: float = 0.35) -> dict:
    desde = datetime.strptime(desde_str, "%Y-%m-%d")
    hasta = datetime.strptime(hasta_str, "%Y-%m-%d")
    conn = sqlite3.connect(str(DB_PATH))
    session = requests.Session()
    total_notas = 0
    total_insertadas = 0
    fechas_procesadas = 0
    fechas_sin_data = 0

    f = desde
    while f <= hasta:
        notas_fecha = []
        for seccion in SECCIONES:
            notas = scrape_seccion_fecha(session, f, seccion)
            notas_fecha.extend(notas)
            time.sleep(delay)
        insertadas = guardar_notas(conn, notas_fecha)
        total_notas += len(notas_fecha)
        total_insertadas += insertadas
        fechas_procesadas += 1
        if not notas_fecha:
            fechas_sin_data += 1
        if fechas_procesadas % 7 == 0 or fechas_procesadas == 1:
            logger.info(
                f"  {f.strftime('%Y-%m-%d')} · {len(notas_fecha)} notas "
                f"({insertadas} nuevas) · acumulado: {total_insertadas} nuevas / {total_notas} totales"
            )
        f += timedelta(days=1)

    conn.close()
    return {
        "total_notas": total_notas,
        "insertadas": total_insertadas,
        "fechas_procesadas": fechas_procesadas,
        "fechas_sin_data": fechas_sin_data,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--desde", help="YYYY-MM-DD")
    parser.add_argument("--hasta", help="YYYY-MM-DD")
    parser.add_argument("--caso", choices=list(CASOS_VENTANAS.keys()))
    parser.add_argument("--todos-casos", action="store_true",
                        help="Correr todos los casos preseteados secuencialmente")
    parser.add_argument("--delay", type=float, default=0.35,
                        help="Pausa entre requests (segundos, default 0.35)")
    args = parser.parse_args()

    if args.todos_casos:
        for nombre, (d, h) in CASOS_VENTANAS.items():
            logger.info(f"═══ Backfill: {nombre} ({d} → {h}) ═══")
            res = backfill_rango(d, h, args.delay)
            logger.info(f"  Resultado: {res}")
        return

    if args.caso:
        desde, hasta = CASOS_VENTANAS[args.caso]
    else:
        if not args.desde or not args.hasta:
            parser.error("Requiere --desde/--hasta o --caso o --todos-casos")
        desde, hasta = args.desde, args.hasta

    logger.info(f"Backfill La Jornada {desde} → {hasta}")
    logger.info(f"Secciones: {SECCIONES}")
    res = backfill_rango(desde, hasta, args.delay)
    logger.info("═" * 50)
    logger.info(f"Fechas procesadas: {res['fechas_procesadas']}")
    logger.info(f"Fechas sin data: {res['fechas_sin_data']}")
    logger.info(f"Notas totales encontradas: {res['total_notas']}")
    logger.info(f"Notas nuevas insertadas: {res['insertadas']}")


if __name__ == "__main__":
    main()
