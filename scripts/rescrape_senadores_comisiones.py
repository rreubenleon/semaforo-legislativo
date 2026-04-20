"""
Re-scrape de SOLO comisiones+cargo de los 128 senadores.

Motivación: el parser viejo de _scrape_detalle_senador capturaba el menú
del sitio del Senado en lugar del bloque real de comisiones, dejando a casi
todos los senadores con basura tipo "Ordinarias:Integrante|...|Histórico
LXIV y LXV...". El parser nuevo (commit que arregla #SenadoComisiones)
ancla en <h4 class="comisiones-lista-titulo"> y respeta los rótulos de
cargo (Presidente, Secretario, Integrante).

Este script NO INSERTA nuevos legisladores — solo UPDATE de los campos
`comisiones` y `comisiones_cargo` para los registros que ya existen. Así
no toca partido/estado/foto ni reescribe nombres.

Uso:
    .venv/bin/python scripts/rescrape_senadores_comisiones.py
    .venv/bin/python scripts/rescrape_senadores_comisiones.py --delay 1.0
    .venv/bin/python scripts/rescrape_senadores_comisiones.py --dry-run --limit 5
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# Permitir import desde la raíz del repo
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import get_connection
from scrapers.legisladores import _scrape_detalle_senador, _scrape_listado_senadores

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", type=float, default=0.6,
                        help="Pausa entre requests (segundos). Default 0.6")
    parser.add_argument("--limit", type=int, default=0,
                        help="Procesar solo los primeros N. Default 0=todos")
    parser.add_argument("--dry-run", action="store_true",
                        help="No escribir a DB, solo mostrar lo que se cambiaría")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    logger.info("Trayendo listado de senadores…")
    listado = _scrape_listado_senadores()
    if not listado:
        logger.error("Listado vacío. Abortando.")
        return 1
    logger.info(f"Listado: {len(listado)} senadores")

    if args.limit > 0:
        listado = listado[: args.limit]
        logger.info(f"Limitando a {len(listado)} (--limit)")

    conn = get_connection()

    actualizados = 0
    sin_match_db = 0
    sin_comisiones = 0
    errores = 0

    for i, sen in enumerate(listado, 1):
        try:
            detalle = _scrape_detalle_senador(sen["url"], sen["senador_id"])
        except Exception as e:
            logger.warning(f"  [{i}/{len(listado)}] {sen['nombre']}: error scrape: {e}")
            errores += 1
            time.sleep(args.delay)
            continue

        if not detalle:
            logger.warning(f"  [{i}/{len(listado)}] {sen['nombre']}: sin detalle")
            errores += 1
            time.sleep(args.delay)
            continue

        comisiones = "|".join(detalle["comisiones"]) if detalle["comisiones"] else ""
        comisiones_cargo = (
            "|".join(detalle["comisiones_cargo"]) if detalle["comisiones_cargo"] else ""
        )

        if not detalle["comisiones"]:
            logger.info(f"  [{i}/{len(listado)}] {sen['nombre']}: 0 comisiones extraídas")
            sin_comisiones += 1

        # Match por sitl_id (estable) o nombre normalizado (fallback)
        row = conn.execute(
            """
            SELECT id FROM legisladores
            WHERE camara LIKE '%enad%' AND sitl_id = ?
            LIMIT 1
            """,
            (sen["senador_id"],),
        ).fetchone()
        if not row:
            row = conn.execute(
                """
                SELECT id FROM legisladores
                WHERE camara LIKE '%enad%' AND nombre = ?
                LIMIT 1
                """,
                (sen["nombre"],),
            ).fetchone()
        if not row:
            logger.warning(
                f"  [{i}/{len(listado)}] {sen['nombre']} (id_senado={sen['senador_id']}): "
                f"no existe en DB — saltando"
            )
            sin_match_db += 1
            time.sleep(args.delay)
            continue

        leg_id = row[0] if isinstance(row, tuple) else row["id"]

        if args.dry_run:
            logger.info(
                f"  [{i}/{len(listado)}] {sen['nombre']} → "
                f"{len(detalle['comisiones'])} comisiones (dry-run, no guardo)"
            )
        else:
            conn.execute(
                """
                UPDATE legisladores
                SET comisiones = ?, comisiones_cargo = ?
                WHERE id = ?
                """,
                (comisiones, comisiones_cargo, leg_id),
            )
            actualizados += 1
            if i % 10 == 0 or i == len(listado):
                logger.info(
                    f"  [{i}/{len(listado)}] {sen['nombre']} → "
                    f"{len(detalle['comisiones'])} comisiones · "
                    f"actualizados acumulados: {actualizados}"
                )

        time.sleep(args.delay)

    if not args.dry_run:
        conn.commit()

    logger.info("=" * 60)
    logger.info(f"Procesados:        {len(listado)}")
    logger.info(f"Actualizados:      {actualizados}")
    logger.info(f"Sin match en DB:   {sin_match_db}")
    logger.info(f"Sin comisiones:    {sin_comisiones}")
    logger.info(f"Errores scrape:    {errores}")
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
