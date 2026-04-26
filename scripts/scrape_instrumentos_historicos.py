"""
Scrape de instrumentos LXIV y LXV de los 218 legisladores reelectos en LXVI.
Fuente: SIL Gobernación Búsqueda Avanzada.

Flujo (descifrado abr 25 2026):
  1. Resolver el ID interno de cada legislador en cada (legislatura, cámara)
     vía pp_ListLegislador.php (búsqueda por nombre, ID en `incluir(ID,...)`)
  2. POST a ProcesoBusquedaAvanzada.php con
       URL: ?SID=&REFERENCIA={ID}&PELFILLEF=PL
       body: LEGISLATURA={64|65}, TASUNTO_AR[]={11=Iniciativa | 19=PropPA},
             OPC_SELECCIONADAS={ID}, PRESENTADOR=L, CAMARA_PRESENTADOR[]={1,2,5},
             PAGINAS=100
     → server genera Serial+Reg, redirige a ResultadosBusquedaAvanzada
  3. Paginar y parsear la tabla de resultados (12 columnas).

Output: tabla NUEVA `sil_documentos_historicos`. NO toca `sil_documentos`
(LXVI activo) ni el pipeline. Idempotente.

Uso:
    .venv/bin/python scripts/scrape_instrumentos_historicos.py --resolver-ids
        # Solo paso 1: mapea nombres → IDs SIL para los 218 reelectos
        # Cachea en eval/reelectos/ids_sil.json
    .venv/bin/python scripts/scrape_instrumentos_historicos.py --diagnostic
        # 5 legisladores, no escribe BD
    .venv/bin/python scripts/scrape_instrumentos_historicos.py
        # Producción: scrape completo + escribe BD
"""
import argparse
import json
import logging
import re
import sqlite3
import sys
import time
import unicodedata
from collections import Counter
from pathlib import Path

import requests
import urllib3
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from db import get_connection
from utils.matcher import (
    normalizar_nombre as _normalizar,
    build_bd_index,
    encontrar_legislador_id as _encontrar_legislador_id_shared,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

BASE = "https://sil.gobernacion.gob.mx"
URL_AVANZADA_ROOT = f"{BASE}/Busquedas/Avanzada/"
URL_PP_LISTLEG = f"{BASE}/Busquedas/Avanzada/PopsPresentador/pp_ListLegislador.php"
URL_PROCESO = f"{BASE}/Busquedas/Avanzada/ProcesoBusquedaAvanzada.php"
URL_RESULTADOS = f"{BASE}/Busquedas/Avanzada/ResultadosBusquedaAvanzada.php"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}

DELAY = 0.6
PAGINAS = 100  # resultados por página
TASUNTO_INICIATIVA = "11"
TASUNTO_PROPOSICION = "19"

LEG_NUM_TO_ROMAN = {64: "LXIV", 65: "LXV"}
ROMAN_TO_LEG_NUM = {"LXIV": 64, "LXV": 65}

OUT_DIR = ROOT / "eval" / "reelectos"
IDS_CACHE = OUT_DIR / "ids_sil.json"


# _normalizar: importado desde utils.matcher (fuente única de verdad)


# ──────────────────────────────────────────────────────────────────────
# Schema BD
# ──────────────────────────────────────────────────────────────────────
def asegurar_schema(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sil_documentos_historicos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            legislador_id INTEGER NOT NULL,
            sil_referencia TEXT,                  -- ID interno SIL del legislador
            tipo_asunto TEXT,                     -- 'Iniciativa' o 'Proposición con punto de acuerdo'
            denominacion TEXT,                    -- título completo
            sub_clasificacion TEXT,
            camara TEXT,
            fecha_presentacion TEXT,              -- formato dd/mm/yyyy → normalizar a yyyy-mm-dd
            presentador TEXT,
            partido TEXT,
            legislatura TEXT,                     -- 'LXIV' | 'LXV'
            turnado_a TEXT,
            estatus TEXT,
            tema TEXT,
            url_detalle TEXT,
            fecha_scraping TEXT,
            FOREIGN KEY (legislador_id) REFERENCES legisladores(id),
            UNIQUE(legislador_id, denominacion, fecha_presentacion, tipo_asunto)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hist_leg ON sil_documentos_historicos(legislador_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hist_legis ON sil_documentos_historicos(legislatura)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hist_fecha ON sil_documentos_historicos(fecha_presentacion)")
    conn.commit()


# ──────────────────────────────────────────────────────────────────────
# PASO 1: Resolver IDs SIL via pp_ListLegislador
# ──────────────────────────────────────────────────────────────────────
def get_session_sid() -> tuple:
    """Cargar BusquedaAvanzada para obtener SID + cookies."""
    sess = requests.Session()
    sess.headers.update(HEADERS)
    resp = sess.get(URL_AVANZADA_ROOT, timeout=30, verify=False)
    m = re.search(r"SID=([a-f0-9]+)", resp.text)
    return sess, m.group(1) if m else ""


def resolver_id_sil(sess, sid: str, nombre: str, legislatura: int) -> dict:
    """
    Busca el ID SIL de un legislador en una legislatura específica.
    Devuelve dict {id, nombre_completo, camara, partido} o {}.

    NOTA: el endpoint pp_ListLegislador del SIL falla cuando recibe
    caracteres con acento o ñ. Mandamos el nombre SIN acentos
    (normalizado), lo que sí encuentra resultados. Esta es la corrección
    del bug que dejó 46 reelectos sin ID en el primer scrape.
    """
    query_normalizado = _normalizar(nombre)  # 'Añorve' → 'anorve'
    resp = sess.post(
        f"{URL_PP_LISTLEG}?SID={sid}&Camara=&Legislatura={legislatura}",
        data={"SELECCIONADO": "-123456", "CVE_PALABRAS": query_normalizado},
        timeout=30, verify=False,
    )
    resp.encoding = "latin-1"

    # Buscar incluir('ID', 'Texto'); donde Texto incluye [LXIV] o [LXV]
    legis_roman = LEG_NUM_TO_ROMAN[legislatura]
    nombre_norm = _normalizar(nombre)
    nombre_tokens = set(nombre_norm.split())

    matches = []
    for m in re.finditer(r"incluir\('(\d+)',\s*'([^']+)'\)", resp.text):
        sil_id = m.group(1)
        texto = m.group(2)
        # Si el texto incluye prefijo [LEGIS], filtrar; si no (cuando filtramos
        # por Legislatura en URL), el server omite el prefijo redundante.
        if "[" in texto and f"[{legis_roman}]" not in texto:
            continue
        # Verificar match por tokens (mín 3 comunes)
        texto_norm = _normalizar(re.sub(r"\([^)]*\)|\[[^\]]*\]", "", texto))
        texto_tokens = set(texto_norm.split())
        comunes = nombre_tokens & texto_tokens
        if len(comunes) >= 3:
            matches.append({
                "id": sil_id,
                "texto": texto,
                "comunes": len(comunes),
            })

    if not matches:
        return {}
    # Mejor match = más tokens en común
    mejor = max(matches, key=lambda x: x["comunes"])
    # Extraer cámara, partido del texto: "Sen. Nombre (PARTIDO)" o "Dip. ..."
    texto = mejor["texto"]
    camara = "Senado" if texto.startswith("Sen") else "Diputados"
    partido_m = re.search(r"\(([^)]+)\)", texto)
    partido = partido_m.group(1) if partido_m else ""
    return {
        "sil_id": mejor["id"],
        "nombre_completo": texto,
        "camara": camara,
        "partido": partido,
    }


def resolver_todos_ids(reelectos: list) -> dict:
    """Para cada reelecto, resolver su ID en LXIV y/o LXV.
    Cachea en disco. Devuelve {nombre_norm: {LXIV: {...}, LXV: {...}}}."""
    if IDS_CACHE.exists():
        cache = json.loads(IDS_CACHE.read_text())
        logger.info(f"IDs cacheados: {len(cache)} legisladores")
    else:
        cache = {}

    sess, sid = get_session_sid()
    logger.info(f"SID: {sid}")

    for r in reelectos:
        nn = r["nombre_normalizado"]
        if nn in cache and "LXIV" in cache[nn] and "LXV" in cache[nn]:
            continue

        cache.setdefault(nn, {"nombre": r["nombre"], "previas_legs": r.get("previas_legislaturas", [])})

        # Resolver para LXIV y LXV solo si tuvo actividad ahí
        for legis_num, legis_roman in [(64, "LXIV"), (65, "LXV")]:
            if legis_roman in cache[nn] and cache[nn][legis_roman]:
                continue
            if legis_roman not in r.get("previas_legislaturas", []):
                cache[nn][legis_roman] = None  # no aplica
                continue
            try:
                info = resolver_id_sil(sess, sid, r["nombre"], legis_num)
                cache[nn][legis_roman] = info if info else None
            except Exception as e:
                logger.warning(f"  Error resolviendo {r['nombre']} {legis_roman}: {e}")
                cache[nn][legis_roman] = None
            time.sleep(DELAY)

    IDS_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2))
    return cache


# ──────────────────────────────────────────────────────────────────────
# PASO 2: Scrape instrumentos via búsqueda avanzada
# ──────────────────────────────────────────────────────────────────────
def _normalizar_fecha(fecha: str) -> str:
    """dd/mm/yyyy → yyyy-mm-dd. Si no matchea, devuelve original."""
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", fecha.strip())
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return fecha


def _parsear_resultados(html: str, legis_roman: str) -> list[dict]:
    """Parsea una página de ResultadosBusquedaAvanzada."""
    soup = BeautifulSoup(html, "html.parser")
    big = max(soup.find_all("table"), key=lambda t: len(t.find_all("tr")), default=None)
    if not big:
        return []

    out = []
    for tr in big.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) != 12:
            continue
        n = cells[0].get_text(strip=True)
        if not n.isdigit():
            continue  # header u otra fila no de datos

        # Link al detalle (si existe)
        link = cells[2].find("a", href=True)
        url_detalle = link.get("href") if link else ""

        out.append({
            "tipo_asunto": cells[1].get_text(strip=True),
            "denominacion": cells[2].get_text(" ", strip=True),
            "sub_clasificacion": cells[3].get_text(strip=True),
            "camara": cells[4].get_text(strip=True),
            "fecha_presentacion": _normalizar_fecha(cells[5].get_text(strip=True)),
            "presentador": cells[6].get_text(strip=True),
            "partido": cells[7].get_text(strip=True),
            "legislatura": cells[8].get_text(strip=True) or legis_roman,
            "turnado_a": cells[9].get_text(" ", strip=True),
            "estatus": cells[10].get_text(strip=True),
            "tema": cells[11].get_text(strip=True),
            "url_detalle": url_detalle,
        })
    return out


def scrape_legislador_legis(sess, ref_id: str, legis_num: int, tasunto: str) -> list[dict]:
    """Hace POST + paginación para un (legislador, legislatura, tipo_asunto).
    Devuelve lista de instrumentos."""
    legis_roman = LEG_NUM_TO_ROMAN[legis_num]

    # POST inicial
    data = [
        ("LEGISLATURA", str(legis_num)),
        ("TASUNTO_AR[]", tasunto),
        ("OPCION_TEXTO", "2"),
        ("VALOR_TEXTO", ""),
        ("PRESENTADOR", "L"),
        ("OPC_SELECCIONADAS", ref_id),
        ("CAMARA_PRESENTADOR[]", "1"),
        ("CAMARA_PRESENTADOR[]", "2"),
        ("CAMARA_PRESENTADOR[]", "5"),
        ("RESULTADO", "9"),
        ("PAGINAS", str(PAGINAS)),
    ]
    try:
        resp = sess.post(
            f"{URL_PROCESO}?SID=&REFERENCIA={ref_id}&PELFILLEF=PL",
            data=data, timeout=60, verify=False,
        )
        resp.encoding = "latin-1"
    except requests.RequestException as e:
        logger.warning(f"POST falló para {ref_id}/{legis_roman}/{tasunto}: {e}")
        return []

    if "No hay resultados" in resp.text:
        return []

    # Extraer Serial + Reg
    m_serial = re.search(r"Serial=([a-f0-9]+)", resp.text)
    m_reg = re.search(r"Reg=(\d+)", resp.text)
    if not m_serial or not m_reg:
        return []
    serial = m_serial.group(1)
    reg = int(m_reg.group(1))

    # Parsear primera página (ya viene en resp)
    instrumentos = _parsear_resultados(resp.text, legis_roman)

    # Paginar si hay más
    total_paginas = (reg + PAGINAS - 1) // PAGINAS
    if total_paginas > 1:
        m_sid2 = re.search(r"SID=([a-f0-9]+)", resp.text)
        sid2 = m_sid2.group(1) if m_sid2 else ""
        for pg in range(2, total_paginas + 1):
            time.sleep(DELAY)
            try:
                r2 = sess.get(URL_RESULTADOS, params={
                    "SID": sid2, "Origen": "BA", "Serial": serial,
                    "Reg": reg, "Paginas": PAGINAS, "pagina": pg,
                }, timeout=60, verify=False)
                r2.encoding = "latin-1"
                instrumentos.extend(_parsear_resultados(r2.text, legis_roman))
            except requests.RequestException as e:
                logger.warning(f"  Página {pg} falló: {e}")
                break

    return instrumentos


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--resolver-ids", action="store_true",
                   help="Solo paso 1: resolver IDs SIL para los 218 reelectos")
    p.add_argument("--diagnostic", action="store_true",
                   help="5 legisladores, no escribe BD")
    p.add_argument("--max-legisladores", type=int, default=None)
    args = p.parse_args()

    # Cargar reelectos del diagnóstico
    diag_path = OUT_DIR / "diagnostico_unificado.json"
    if not diag_path.exists():
        logger.error(f"No existe {diag_path}")
        return
    diag = json.loads(diag_path.read_text())
    reelectos = diag["reelectos"]

    if args.diagnostic or args.max_legisladores:
        n = args.max_legisladores or 5
        reelectos = reelectos[:n]
        logger.info(f"Limitando a {len(reelectos)} legisladores")

    # PASO 1: Resolver IDs
    print(f"\n  ═══ PASO 1: Resolver IDs SIL ═══\n")
    ids = resolver_todos_ids(reelectos)

    # Estadísticas
    encontrados_lxiv = sum(1 for v in ids.values() if v.get("LXIV"))
    encontrados_lxv = sum(1 for v in ids.values() if v.get("LXV"))
    print(f"\n  Resueltos: LXIV={encontrados_lxiv}, LXV={encontrados_lxv}")

    if args.resolver_ids:
        print(f"  ✓ Cache: {IDS_CACHE.relative_to(ROOT)}")
        return

    # PASO 2: Scrape
    print(f"\n  ═══ PASO 2: Scrape de instrumentos ═══\n")
    write_db = not args.diagnostic

    conn = get_connection()
    if write_db:
        asegurar_schema(conn)

    sess, _ = get_session_sid()
    total = 0
    by_legis = Counter()
    by_tipo = Counter()
    inserts = 0
    sin_resultados = 0
    t0 = time.time()

    # Iterar por (legislador, legislatura)
    items = []
    for r in reelectos:
        nn = r["nombre_normalizado"]
        for legis_roman in r.get("previas_legislaturas", []):
            if legis_roman not in ROMAN_TO_LEG_NUM:
                continue
            info = ids.get(nn, {}).get(legis_roman)
            if not info:
                continue
            items.append((r, legis_roman, info))

    # legislador_id usando utils.matcher (fuente única de verdad).
    # Esto resuelve el bug del primer scrape donde 6,444 instrumentos
    # se descartaron por matcher exact que fallaba con Diputados.
    bd_idx = build_bd_index(conn)
    encontrar_id = lambda nn, cam: _encontrar_legislador_id_shared(nn, cam, bd_idx)

    print(f"  {len(items)} pares (legislador × legislatura) a procesar")
    print(f"  Tiempo estimado: ~{len(items) * 2 * DELAY * 1.5 / 60:.1f} min")
    print()

    # CACHE durable de scrape: si algo falla en BD, no perdemos el scrape.
    scrape_cache_path = OUT_DIR / "scrape_historicos_cache.json"
    scrape_cache = {}
    if scrape_cache_path.exists():
        scrape_cache = json.loads(scrape_cache_path.read_text())
        logger.info(f"Cache de scrape encontrada: {len(scrape_cache)} pares")

    sin_match_bd = 0
    for i, (r, legis_roman, info) in enumerate(items):
        legis_num = ROMAN_TO_LEG_NUM[legis_roman]
        ref_id = info["sil_id"]
        cache_key = f"{ref_id}_{legis_num}"

        # Iniciativas + Proposiciones
        for tasunto, label in [(TASUNTO_INICIATIVA, "Init"), (TASUNTO_PROPOSICION, "Prop")]:
            tasunto_key = f"{cache_key}_{tasunto}"
            if tasunto_key in scrape_cache:
                instrs = scrape_cache[tasunto_key]
            else:
                try:
                    instrs = scrape_legislador_legis(sess, ref_id, legis_num, tasunto)
                except Exception as e:
                    logger.warning(f"Error: {e}")
                    instrs = []
                scrape_cache[tasunto_key] = instrs
                time.sleep(DELAY)

            n = len(instrs)
            total += n
            by_legis[legis_roman] += n
            by_tipo[label] += n
            if n == 0:
                sin_resultados += 1

            if write_db and instrs:
                lid = encontrar_id(r["nombre_normalizado"], r["camara_actual"])
                if not lid:
                    sin_match_bd += 1
                    if sin_match_bd <= 5:
                        logger.warning(f"  Sin match BD: {r['nombre']} ({r['camara_actual']})")
                    continue
                for ins in instrs:
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO sil_documentos_historicos
                            (legislador_id, sil_referencia, tipo_asunto, denominacion,
                             sub_clasificacion, camara, fecha_presentacion, presentador,
                             partido, legislatura, turnado_a, estatus, tema, url_detalle,
                             fecha_scraping)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                        """, (
                            lid, ref_id, ins["tipo_asunto"], ins["denominacion"],
                            ins["sub_clasificacion"], ins["camara"], ins["fecha_presentacion"],
                            ins["presentador"], ins["partido"], ins["legislatura"],
                            ins["turnado_a"], ins["estatus"], ins["tema"], ins["url_detalle"],
                        ))
                        inserts += 1
                    except sqlite3.Error as e:
                        logger.warning(f"Insert err: {e}")

        # Persistir cache cada 20 pares
        if (i + 1) % 20 == 0:
            scrape_cache_path.write_text(json.dumps(scrape_cache, ensure_ascii=False))
            if write_db:
                conn.commit()

        elapsed = time.time() - t0
        rate = (i + 1) / elapsed if elapsed > 0 else 0
        eta = (len(items) - i - 1) / rate if rate > 0 else 0
        if (i + 1) % 5 == 0 or args.diagnostic:
            logger.info(
                f"  {i+1}/{len(items)} · {r['nombre'][:35]:<35} {legis_roman} "
                f"→ total={total} · {rate:.1f}/s · ETA {eta/60:.1f}m"
            )

    # Cache final
    scrape_cache_path.write_text(json.dumps(scrape_cache, ensure_ascii=False))
    if write_db:
        conn.commit()

    # Reporte
    duracion = (time.time() - t0) / 60
    print()
    print(f"  ═══ Resultados ═══")
    print(f"  Pares procesados: {len(items)}")
    print(f"  Total instrumentos: {total:,}")
    print(f"  Por legislatura: {dict(by_legis)}")
    print(f"  Por tipo: {dict(by_tipo)}")
    print(f"  Pares sin resultados: {sin_resultados}")
    print(f"  Duración: {duracion:.1f} min")
    if write_db:
        print(f"  Inserts: {inserts:,}")


if __name__ == "__main__":
    main()
