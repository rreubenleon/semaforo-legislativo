"""
Scrape estatus de instrumentos por comisión del Senado LXVI.

OBJETIVO:
  Las páginas de senado.gob.mx/66/senador/{id} muestran instrumentos
  por senador pero SIN estatus (solo el turno inicial). Por otro lado,
  las páginas por comisión sí muestran "Estatus: Aprobado / Desechado /
  Publicado en DOF / Turnado a Comisiones / etc." con enlace a la
  Gaceta (`documento/{ID}`) que podemos cruzar con `senador_instrumento.
  seguimiento_id = 'SEN_{ID}'`.

ESTRATEGIA:
  Para cada (comision_id, tipo_asunto) en CATALOGO_LXVI × {iniciativas,
  proposiciones, dictamenes}: scrape la página y extrae paneles con:
    - gaceta_doc_id (de href 'gaceta_del_senado/documento/{ID}')
    - estatus textual
    - título (para validación)

  Resultado en tabla nueva `estatus_senado_panel` (BD local) con
  (gaceta_doc_id, estatus, comision_id, tipo_asunto, titulo).

  Después, otro paso hace UPDATE sil_documentos.estatus para cada
  fila donde seguimiento_id = 'SEN_{gaceta_doc_id}'.

CONSIDERACIONES:
  - Incapsula: sesión con warmup + sleep ~6-8s entre requests
  - 77 comisiones × 3 tipos = ~230 requests = ~25-30 min con 8s delay
  - Checkpoint cada comisión para reanudar si falla

Uso:
  python3 scripts/scrape_estatus_comisiones_senado.py
  python3 scripts/scrape_estatus_comisiones_senado.py --solo-id 414
"""
import argparse
import json
import logging
import re
import sqlite3
import sys
import time
from pathlib import Path

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers.comisiones_senado_asuntos import (
    CATALOGO_LXVI,
    _slug_referer,
    _make_session,
    _warmup_session,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


BASE = "https://www.senado.gob.mx"
FRAME_URL = f"{BASE}/64/frames/asuntosComisiones.php"
COMISIONES_HOME = "https://comisiones.senado.gob.mx"

TIPOS = ["iniciativas", "proposiciones", "dictamenes"]

# Regex para extraer paneles. El HTML usa patrones repetidos por panel.
RE_ESTATUS = re.compile(r"Estatus:\s*</strong>\s*([^<]+)", re.IGNORECASE)
RE_DOC_GACETA = re.compile(r"gaceta_del_senado/documento/(\d+)")
RE_LOADING = re.compile(r"<title>Loading</title>", re.IGNORECASE)
RE_TOTAL = re.compile(r"Número de Registros encontrados:\s*<strong>(\d+)</strong>")


def ensure_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS estatus_senado_panel (
            gaceta_doc_id INTEGER NOT NULL,
            comision_id INTEGER NOT NULL,
            tipo_asunto TEXT NOT NULL,
            estatus TEXT NOT NULL,
            titulo TEXT,
            fecha_scraping TEXT,
            PRIMARY KEY (gaceta_doc_id, comision_id, tipo_asunto)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_estatus_doc ON estatus_senado_panel(gaceta_doc_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_estatus_estatus ON estatus_senado_panel(estatus)")
    conn.commit()


def parsear_paneles(html: str) -> list[dict]:
    """
    Cada panel del HTML contiene en orden:
      ... Estatus: </strong>{ESTATUS} ... documento/{ID} ...
    Extraemos pares (estatus, doc_id) en orden de aparición.
    """
    paneles = []
    # Estrategia: split por "Estatus: </strong>" para obtener segmentos
    # cada uno terminando antes del siguiente Estatus. Dentro de cada
    # segmento, extraer el primer doc_id de gaceta_del_senado.
    segments = re.split(r"Estatus:\s*</strong>\s*", html)
    for seg in segments[1:]:  # primer trozo es preámbulo, sin estatus
        m_est = re.match(r"([^<]+)", seg)
        if not m_est:
            continue
        estatus = m_est.group(1).strip()
        m_doc = RE_DOC_GACETA.search(seg)
        if not m_doc:
            continue
        doc_id = int(m_doc.group(1))
        # Título: primer texto entre <p>...</p> después del estatus
        m_tit = re.search(r"<p[^>]*>([^<]{20,400})</p>", seg)
        titulo = (m_tit.group(1) if m_tit else "")[:300]
        paneles.append({
            "gaceta_doc_id": doc_id,
            "estatus": estatus[:120],
            "titulo": titulo.strip(),
        })
    return paneles


def fetch_comision_tipo(sess, id_com: int, tipo: str, slug: str,
                       reintentos: int = 3):
    sess.headers["Referer"] = f"{COMISIONES_HOME}/{slug}/{tipo}"
    url = f"{FRAME_URL}?idComision={id_com}&tipoAsunto={tipo}"
    for intento in range(reintentos):
        try:
            r = sess.get(url, timeout=30, verify=False)
            if r.status_code != 200:
                logger.warning(f"  [{id_com}/{tipo}] HTTP {r.status_code}")
                time.sleep(10)
                continue
            if RE_LOADING.search(r.text):
                logger.warning(f"  [{id_com}/{tipo}] Incapsula stub, reintento {intento+1}")
                time.sleep(20)
                _warmup_session(sess)
                continue
            return r.text
        except Exception as e:
            logger.warning(f"  [{id_com}/{tipo}] error: {e}")
            time.sleep(5)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--solo-id", type=int, default=None)
    ap.add_argument("--delay", type=float, default=7.0)
    args = ap.parse_args()

    from db import get_connection
    conn = get_connection()
    ensure_table(conn)

    catalogo = CATALOGO_LXVI
    if args.solo_id:
        catalogo = {args.solo_id: CATALOGO_LXVI.get(args.solo_id, f"comision_{args.solo_id}")}

    sess = _make_session()
    if not _warmup_session(sess):
        logger.error("Warmup falló. Aborto.")
        return 1
    time.sleep(args.delay)

    total_paneles = 0
    total_insertados = 0
    ahora = time.strftime("%Y-%m-%d %H:%M:%S")

    for i, (id_com, nombre) in enumerate(sorted(catalogo.items()), 1):
        slug = _slug_referer(nombre)
        logger.info(f"[{i:3}/{len(catalogo)}] comision {id_com} {nombre[:35]}")
        for tipo in TIPOS:
            html = fetch_comision_tipo(sess, id_com, tipo, slug)
            time.sleep(args.delay)
            if not html:
                logger.warning(f"  [{id_com}/{tipo}] sin HTML")
                continue
            paneles = parsear_paneles(html)
            total_paneles += len(paneles)
            n_ins = 0
            for p in paneles:
                try:
                    conn.execute("""
                        INSERT INTO estatus_senado_panel
                            (gaceta_doc_id, comision_id, tipo_asunto, estatus, titulo, fecha_scraping)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(gaceta_doc_id, comision_id, tipo_asunto) DO UPDATE SET
                            estatus = excluded.estatus,
                            titulo = excluded.titulo,
                            fecha_scraping = excluded.fecha_scraping
                    """, (p["gaceta_doc_id"], id_com, tipo, p["estatus"], p["titulo"], ahora))
                    n_ins += 1
                except Exception as e:
                    logger.debug(f"  Error insertar: {e}")
            conn.commit()
            total_insertados += n_ins
            logger.info(f"  [{id_com}/{tipo}] {len(paneles)} paneles, {n_ins} guardados")

    logger.info(f"\nTotal: {total_paneles} paneles parseados, {total_insertados} insertados/actualizados")

    # Estadísticas
    estatus_counts = conn.execute(
        "SELECT estatus, COUNT(*) FROM estatus_senado_panel GROUP BY estatus ORDER BY COUNT(*) DESC"
    ).fetchall()
    logger.info("\nDistribución de estatus en BD:")
    for est, n in estatus_counts:
        logger.info(f"  {n:5d}× {est}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
