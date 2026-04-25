"""
Re-fetch de la ficha SIL para instrumentos con tipo='Asunto' o vacío,
para corregir el bug del scraper que pone 'Asunto' como fallback genérico
cuando no llamó _obtener_detalle().

Uso:
    python scripts/refresh_tipos_sil.py            # corre en producción
    python scripts/refresh_tipos_sil.py --limite 50  # prueba con 50
"""
import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from db import get_connection
from scrapers.sil import _obtener_detalle

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limite", type=int, default=10000)
    parser.add_argument("--delay", type=float, default=0.4, help="segundos entre requests")
    args = parser.parse_args()

    conn = get_connection()
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT id, seguimiento_id, asunto_id, tipo, titulo
        FROM sil_documentos
        WHERE fecha_presentacion >= '2024-09-01'
          AND (tipo IN ('Asunto', '') OR tipo IS NULL)
        ORDER BY fecha_presentacion DESC
        LIMIT ?
    """, (args.limite,)).fetchall()
    logger.info(f"Instrumentos a re-fetch: {len(rows):,}")

    actualizados = 0
    errores = 0
    sin_cambio = 0
    cambios_por_tipo = {}
    t0 = time.time()

    for i, r in enumerate(rows):
        try:
            d = _obtener_detalle(r["seguimiento_id"], r["asunto_id"])
        except Exception as e:
            errores += 1
            continue

        if not d or not d.get("tipo"):
            errores += 1
            continue

        nuevo_tipo = d["tipo"]
        if nuevo_tipo == r["tipo"]:
            sin_cambio += 1
        else:
            conn.execute(
                "UPDATE sil_documentos SET tipo = ? WHERE id = ?",
                (nuevo_tipo, r["id"])
            )
            actualizados += 1
            cambios_por_tipo[nuevo_tipo] = cambios_por_tipo.get(nuevo_tipo, 0) + 1

        if (i + 1) % 50 == 0:
            conn.commit()
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            eta = (len(rows) - i - 1) / rate
            logger.info(
                f"  {i+1}/{len(rows)} · actualizados={actualizados} errores={errores} "
                f"sin_cambio={sin_cambio} · {rate:.1f}/s · ETA {eta/60:.1f}min"
            )

        time.sleep(args.delay)

    conn.commit()

    print()
    print(f"  ═══ Re-fetch SIL completo ═══")
    print(f"  Procesados:     {len(rows):,}")
    print(f"  Actualizados:   {actualizados:,}  ({100*actualizados/len(rows):.1f}%)")
    print(f"  Sin cambio:     {sin_cambio:,}")
    print(f"  Errores:        {errores:,}")
    print(f"  Duración:       {(time.time()-t0)/60:.1f} min")
    print()
    print(f"  Nuevos tipos asignados:")
    for t, n in sorted(cambios_por_tipo.items(), key=lambda x: -x[1]):
        print(f"    {t:<40} {n:>5}")


if __name__ == "__main__":
    main()
