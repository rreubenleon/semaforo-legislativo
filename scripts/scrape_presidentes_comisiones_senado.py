"""
Scraper específico de PRESIDENTES de comisiones del Senado.

Por qué: el refresh por perfil (refresh_cargos_senadores.py) es
bloqueado por Incapsula después de ~15 requests aun con cloudscraper.
Iterar por COMISIÓN (68 páginas) y solo extraer al Presidente de cada
una es más eficiente y resuelve directamente lo que el dashboard
necesita.

Para cada comisión Senado del catálogo:
  - GET https://www.senado.gob.mx/66/comisiones/ordinarias/{id}
  - Localizar el card de Presidencia (h3 PRESIDENTE/PRESIDENTA)
  - Extraer sitl_id del senador
  - UPDATE legisladores.comisiones_cargo agregando "Comisión X:Presidente"

Sin tocar otros cargos del senador (concatenamos al string existente).
Usa cloudscraper + delay 6s + warmup entre cada 10 requests.

Uso:
  python scripts/scrape_presidentes_comisiones_senado.py
  python scripts/scrape_presidentes_comisiones_senado.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import cloudscraper
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
sys.path.insert(0, str(ROOT))

from scrapers.comisiones_senado_asuntos import CATALOGO_LXVI

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

BASE = "https://www.senado.gob.mx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "es-MX,es;q=0.9",
}


def hacer_scraper():
    s = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False}
    )
    s.headers.update(HEADERS)
    return s


def warmup(sess):
    try:
        sess.get(f"{BASE}/", timeout=30)
        time.sleep(2)
        return True
    except Exception as e:
        logger.warning(f"warmup failed: {e}")
        return False


def extraer_presidente(html, id_com):
    """Devuelve sitl_id del presidente (str) o None.

    Patrón: la página tiene varios <img src="/66/images/senadores/.../{sitl}-..."/>
    seguido de h3 con el cargo. El primer match cuyo h3 siguiente sea
    'PRESIDENTE' o 'PRESIDENTA' es el presidente."""
    soup = BeautifulSoup(html, "html.parser")
    for img in soup.find_all("img", src=re.compile(r"/senadores/\d+/(\d+)-")):
        m = re.search(r"/(\d+)-", img["src"])
        if not m:
            continue
        sitl_id = m.group(1)
        # Buscar h3/h4 después del img
        siguiente = img.find_next(["h3", "h4"])
        if not siguiente:
            continue
        txt = siguiente.get_text(strip=True).upper()
        if txt.startswith("PRESIDEN"):
            return sitl_id
    return None


def actualizar_cargo(conn, sitl_id, nombre_comision, dry_run=False):
    """Agrega 'Nombre:Presidente' al campo comisiones_cargo del senador.
    Si ya tiene esa comisión con otro rol, la sobreescribe a Presidente."""
    row = conn.execute(
        "SELECT id, nombre, comisiones_cargo FROM legisladores WHERE sitl_id = ? AND camara LIKE '%enado%'",
        (sitl_id,)
    ).fetchone()
    if not row:
        return None
    leg_id, nombre, cargo_actual = row
    cargo_actual = cargo_actual or ""
    partes = [p for p in cargo_actual.split("|") if p]
    # Quitar versión previa de esta comisión si existe
    partes = [p for p in partes if not p.startswith(f"{nombre_comision}:")]
    # Agregar como presidente
    partes.append(f"{nombre_comision}:Presidente")
    nuevo_cargo = "|".join(partes)
    com_lista = ", ".join(p.split(":", 1)[0] for p in partes)
    if dry_run:
        return (leg_id, nombre, nuevo_cargo)
    conn.execute(
        "UPDATE legisladores SET comisiones_cargo = ?, comisiones = ? WHERE id = ?",
        (nuevo_cargo, com_lista, leg_id)
    )
    conn.commit()
    return (leg_id, nombre, nuevo_cargo)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--delay", type=float, default=6.0)
    args = p.parse_args()

    conn = sqlite3.connect(str(DB))
    sess = hacer_scraper()
    if not warmup(sess):
        logger.error("Warmup falló")
        return 1

    encontrados = 0
    no_encontrados = []

    for i, (id_com, nombre) in enumerate(sorted(CATALOGO_LXVI.items()), 1):
        url = f"{BASE}/66/comisiones/ordinarias/{id_com}"
        try:
            r = sess.get(url, timeout=30)
        except Exception as e:
            logger.warning(f"  [{id_com}] HTTP error: {e}")
            no_encontrados.append((id_com, nombre, f"http_err:{e}"))
            continue

        if r.status_code != 200 or len(r.text) < 5000 or "<TITLE>Loading" in r.text:
            logger.warning(f"  [{id_com}] {nombre[:35]:35} → BLOCKED ({r.status_code}, {len(r.text)}b)")
            no_encontrados.append((id_com, nombre, "blocked"))
            time.sleep(args.delay * 2)
            continue

        sitl_id = extraer_presidente(r.text, id_com)
        if not sitl_id:
            logger.warning(f"  [{id_com}] {nombre[:35]:35} → sin presidente identificable")
            no_encontrados.append((id_com, nombre, "no_president"))
            time.sleep(args.delay)
            continue

        result = actualizar_cargo(conn, sitl_id, nombre, dry_run=args.dry_run)
        if result:
            leg_id, leg_nombre, cargo_str = result
            logger.info(f"  [{id_com:3}/{i:2}] {nombre[:38]:38} → {leg_nombre}")
            encontrados += 1
        else:
            logger.warning(f"  [{id_com}] sitl={sitl_id} no encontrado en BD")
            no_encontrados.append((id_com, nombre, f"no_match_sitl_{sitl_id}"))

        # Re-warmup cada 10 requests para no quemar la sesión
        if i % 10 == 0 and i < len(CATALOGO_LXVI):
            time.sleep(args.delay * 2)
            warmup(sess)
        else:
            time.sleep(args.delay)

    print(f"\n=== RESUMEN ===")
    print(f"Presidentes identificados y actualizados: {encontrados}/{len(CATALOGO_LXVI)}")
    if no_encontrados:
        print(f"\nSin éxito ({len(no_encontrados)}):")
        for id_c, n, motivo in no_encontrados[:10]:
            print(f"  [{id_c}] {n} → {motivo}")


if __name__ == "__main__":
    sys.exit(main() or 0)
