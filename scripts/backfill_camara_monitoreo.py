"""
Backfill profundo del monitoreo de Cámaras (Senado + Diputados regional).

Corre UNA SOLA VEZ, manualmente, fuera del pipeline de 4h. Baja toda la
historia disponible (hasta 4,380 páginas en Diputados regional) con delay
agresivo y progreso persistente, de modo que si se cae o lo matas puedes
reanudarlo sin duplicar trabajo — el `UNIQUE(url)` de `atencion_camara`
desduplica solo.

**No poner esto dentro del pipeline productivo.** A 2.5s por página, 4,380
páginas son ~3 horas de scrape. GitHub Actions mata jobs a las 6h. Correrlo
local en iMac con el venv activado o como GH Action one-shot.

Uso:
    python3 scripts/backfill_camara_monitoreo.py --fuente diputados --max-paginas 4380
    python3 scripts/backfill_camara_monitoreo.py --fuente senado --max-paginas 500
    python3 scripts/backfill_camara_monitoreo.py --fuente ambas

Opciones:
    --desde-pagina N      Reanudar desde página N (útil si se cayó a la mitad).
    --delay SEGUNDOS      Delay entre páginas. Default 2.5.
    --progreso CADA_N     Log cada N páginas. Default 25.
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from urllib.parse import urljoin

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection
from scrapers.camara_monitoreo import (
    HEADERS,
    URL_DIPUTADOS_REGIONAL,
    URL_SENADO_BASE,
    URL_SENADO_COMUNICADOS,
    _apellidos_legisladores,
    _guardar_items,
    _parse_diputados_regional,
    _parse_senado_comunicados,
    init_db,
)

logger = logging.getLogger("backfill_camara")


def _fetch(url, session, delay, timeout=30):
    """GET con detección de WAF. Si 403/5xx devuelve None y sigue."""
    time.sleep(delay)
    try:
        r = session.get(url, headers=HEADERS, timeout=timeout)
    except requests.RequestException as e:
        logger.warning(f"RequestException {url}: {e}")
        return None
    if r.status_code == 403:
        # WAF — esperar más y reintentar una vez
        logger.warning(f"403 en {url}, esperando 30s y reintentando")
        time.sleep(30)
        try:
            r = session.get(url, headers=HEADERS, timeout=timeout)
        except requests.RequestException:
            return None
        if r.status_code != 200:
            return None
    if r.status_code != 200:
        return None
    return r.text


def backfill_diputados_regional(
    desde_pagina=1,
    max_paginas=4380,
    delay=2.5,
    progreso_cada=25,
    desde_fecha=None,
):
    """
    Baja el monitoreo regional de Diputados desde `desde_pagina`.

    Si `desde_fecha` está seteada (YYYY-MM-DD), el backfill se detiene en
    cuanto encuentra una página donde TODOS los items son anteriores a esa
    fecha. Los items anteriores al tope no se guardan. La página se publica
    en orden cronológico descendente (más reciente primero), así que el
    corte es natural.
    """
    conn = get_connection()
    init_db(conn)
    pares = _apellidos_legisladores(conn)
    session = requests.Session()

    t0 = time.time()
    total_nuevos = 0
    total_dup = 0
    paginas_sin_items = 0
    paginas_bajo_corte = 0

    for p in range(desde_pagina, max_paginas + 1):
        url = f"{URL_DIPUTADOS_REGIONAL}?p={p}"
        html = _fetch(url, session, delay)
        if html is None:
            logger.warning(f"Página {p} falló, continuando")
            continue
        items = _parse_diputados_regional(html)
        if not items:
            paginas_sin_items += 1
            # Si caen 3 páginas seguidas sin items asumimos fin del histórico
            if paginas_sin_items >= 3:
                logger.info(
                    f"3 páginas consecutivas vacías en p={p}, asumimos fin del "
                    f"histórico. Cortamos backfill."
                )
                break
            continue
        paginas_sin_items = 0

        # Filtro por fecha: dropea items anteriores al tope
        if desde_fecha:
            items_filtrados = [
                it for it in items
                if not it.get("fecha") or it["fecha"] >= desde_fecha
            ]
            items_descartados = len(items) - len(items_filtrados)
            if items_filtrados and items_descartados == 0:
                paginas_bajo_corte = 0
            elif not items_filtrados:
                # Página entera bajo el corte — dos seguidas y paramos.
                paginas_bajo_corte += 1
                if paginas_bajo_corte >= 2:
                    logger.info(
                        f"p={p}: 2 páginas consecutivas bajo {desde_fecha}. "
                        f"Cortamos backfill."
                    )
                    break
                continue
            else:
                # Mezcla: guardamos lo que queda arriba del corte y seguimos,
                # porque la siguiente página podría traer items "tardíos"
                # (a veces las fechas no son 100% monótonas).
                paginas_bajo_corte = 0
            items = items_filtrados

        nuevos, dup = _guardar_items(conn, "diputados_regional", items, pares)
        total_nuevos += nuevos
        total_dup += dup

        if p % progreso_cada == 0:
            elapsed = time.time() - t0
            paginas_procesadas = p - desde_pagina + 1
            tasa = paginas_procesadas / elapsed if elapsed else 0
            restantes = max_paginas - p
            eta_min = (restantes / tasa) / 60 if tasa else 0
            logger.info(
                f"p={p}/{max_paginas} | nuevos={total_nuevos} dup={total_dup} | "
                f"{tasa:.2f} pág/s | ETA {eta_min:.0f}m"
            )

    elapsed = time.time() - t0
    logger.info(
        f"Diputados regional backfill: {total_nuevos} nuevos, {total_dup} "
        f"duplicados en {elapsed / 60:.1f} min"
    )
    return {"nuevos": total_nuevos, "duplicados": total_dup}


def backfill_senado(
    desde_pagina=1, max_paginas=500, delay=2.5, progreso_cada=10
):
    """
    Baja el listing de comunicados del Senado por bloques de 10.
    El listing usa `?start=N` (Joomla); cada página muestra ~10 comunicados.
    """
    conn = get_connection()
    init_db(conn)
    pares = _apellidos_legisladores(conn)
    session = requests.Session()

    t0 = time.time()
    total_nuevos = 0
    total_dup = 0
    paginas_sin_items = 0

    # La home ya trae los más recientes; con --desde-pagina=1 arrancamos ahí.
    for p in range(desde_pagina, max_paginas + 1):
        start = (p - 1) * 10
        url = (
            f"{URL_SENADO_BASE}/"
            if start == 0
            else f"{URL_SENADO_COMUNICADOS}?start={start}"
        )
        html = _fetch(url, session, delay)
        if html is None:
            logger.warning(f"Senado p={p} (start={start}) falló, continuando")
            continue
        items = _parse_senado_comunicados(html)
        if not items:
            paginas_sin_items += 1
            if paginas_sin_items >= 3:
                logger.info(
                    f"3 páginas consecutivas vacías en p={p}, asumimos fin. Cortamos."
                )
                break
            continue
        paginas_sin_items = 0

        nuevos, dup = _guardar_items(conn, "senado_comunicados", items, pares)
        total_nuevos += nuevos
        total_dup += dup

        if p % progreso_cada == 0:
            elapsed = time.time() - t0
            logger.info(
                f"Senado p={p}/{max_paginas} | nuevos={total_nuevos} dup={total_dup} "
                f"| {elapsed / 60:.1f} min"
            )

    elapsed = time.time() - t0
    logger.info(
        f"Senado backfill: {total_nuevos} nuevos, {total_dup} duplicados "
        f"en {elapsed / 60:.1f} min"
    )
    return {"nuevos": total_nuevos, "duplicados": total_dup}


def main():
    ap = argparse.ArgumentParser(
        description="Backfill profundo del monitoreo de Cámaras."
    )
    ap.add_argument(
        "--fuente",
        choices=("senado", "diputados", "ambas"),
        default="diputados",
        help="Fuente a backfillear. Default: diputados (el más rico).",
    )
    ap.add_argument("--desde-pagina", type=int, default=1)
    ap.add_argument(
        "--max-paginas",
        type=int,
        default=4380,
        help="Tope duro de páginas a bajar. Default 4380 (todo el histórico "
        "disponible en Diputados regional).",
    )
    ap.add_argument("--delay", type=float, default=2.5)
    ap.add_argument("--progreso-cada", type=int, default=25)
    ap.add_argument(
        "--desde-fecha",
        type=str,
        default=None,
        help="Tope inferior de fecha (YYYY-MM-DD). Items anteriores se ignoran "
        "y el backfill corta cuando dos páginas consecutivas quedan bajo el "
        "tope. Solo aplica a Diputados regional.",
    )
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    inicio = datetime.now()
    logger.info(
        f"Backfill arrancó {inicio.strftime('%Y-%m-%d %H:%M:%S')} | "
        f"fuente={args.fuente} desde={args.desde_pagina} max={args.max_paginas}"
    )

    resultado = {}
    if args.fuente in ("diputados", "ambas"):
        resultado["diputados"] = backfill_diputados_regional(
            desde_pagina=args.desde_pagina,
            max_paginas=args.max_paginas,
            delay=args.delay,
            progreso_cada=args.progreso_cada,
            desde_fecha=args.desde_fecha,
        )
    if args.fuente in ("senado", "ambas"):
        resultado["senado"] = backfill_senado(
            desde_pagina=args.desde_pagina if args.fuente == "senado" else 1,
            max_paginas=args.max_paginas if args.fuente == "senado" else 500,
            delay=args.delay,
            progreso_cada=max(args.progreso_cada // 2, 5),
        )

    fin = datetime.now()
    duracion = (fin - inicio).total_seconds() / 60
    logger.info(f"Backfill terminado en {duracion:.1f} min. Resumen: {resultado}")


if __name__ == "__main__":
    main()
