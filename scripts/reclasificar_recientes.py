"""
Reclasifica artículos recientes que quedaron mal categorizados con el
clasificador viejo (pre-fixes de abril 2026). Usa Haiku 4.5 para casos
ambiguos y el keyword classifier actualizado para el resto.

Flujo:
  1. Selecciona artículos de los últimos N días con categorías ya
     asignadas (por el clasificador viejo)
  2. Los pasa por el clasificador actual (keyword + Haiku)
  3. Actualiza la columna `categorias` en la tabla articulos
  4. Reporta cuántos cambiaron, cuántos quedaron sin clasificar, etc.

Uso:
    python scripts/reclasificar_recientes.py --dias 7
    python scripts/reclasificar_recientes.py --dias 30 --dry-run
"""
import argparse
import logging
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from db import get_connection
from nlp.clasificador import clasificar_y_etiquetar, _haiku_disponible

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dias", type=int, default=7, help="Antigüedad máxima de artículos a reclasificar")
    parser.add_argument("--dry-run", action="store_true", help="No escribe cambios")
    parser.add_argument("--limite", type=int, default=10000, help="Máx artículos por corrida")
    args = parser.parse_args()

    if not _haiku_disponible():
        logger.warning("Haiku no disponible (falta SDK o ANTHROPIC_API_KEY) — se usa keyword only")

    conn = get_connection()
    conn.row_factory = sqlite3.Row

    desde = (datetime.now() - timedelta(days=args.dias)).strftime("%Y-%m-%d")
    logger.info(f"Reclasificando artículos desde {desde} (últimos {args.dias} días)")

    rows = conn.execute("""
        SELECT id, titulo, resumen, fecha, fuente, categorias
        FROM articulos
        WHERE fecha >= ?
          AND titulo IS NOT NULL AND LENGTH(titulo) > 15
        ORDER BY fecha DESC
        LIMIT ?
    """, (desde, args.limite)).fetchall()
    logger.info(f"Artículos candidatos: {len(rows)}")

    cambios = 0
    despejados = 0  # quedaron sin clasificar (antes tenían cat)
    se_clasificaron = 0  # no tenían → ahora sí
    sin_cambio = 0
    t0 = time.time()

    for i, r in enumerate(rows):
        cat_vieja = r["categorias"] or ""
        try:
            cat_nueva = clasificar_y_etiquetar(dict(r), conn=conn)
        except Exception as e:
            logger.warning("Error clasificando id=%s: %s", r["id"], e)
            continue

        cat_nueva = cat_nueva or ""

        if cat_vieja == cat_nueva:
            sin_cambio += 1
        elif cat_vieja and not cat_nueva:
            despejados += 1
            cambios += 1
        elif not cat_vieja and cat_nueva:
            se_clasificaron += 1
            cambios += 1
        else:
            cambios += 1

        if not args.dry_run and cat_vieja != cat_nueva:
            conn.execute("UPDATE articulos SET categorias = ? WHERE id = ?",
                         (cat_nueva, r["id"]))

        if (i + 1) % 50 == 0:
            logger.info(f"  {i+1}/{len(rows)} procesados · {cambios} cambios · {time.time()-t0:.1f}s")

    if not args.dry_run:
        conn.commit()

    dur = time.time() - t0
    print()
    print(f"  ═══ Reclasificación completa ═══")
    print(f"  Procesados:          {len(rows)}")
    print(f"  Sin cambio:          {sin_cambio}")
    print(f"  Categoría cambió:    {cambios - despejados - se_clasificaron}")
    print(f"  Se despejaron:       {despejados}  (antes: cat → ahora: ninguna)")
    print(f"  Se clasificaron:     {se_clasificaron}  (antes: ninguna → ahora: cat)")
    print(f"  Duración:            {dur:.1f}s")
    if args.dry_run:
        print(f"\n  (dry-run: no se escribió nada)")
    else:
        print(f"\n  ✓ Commits aplicados a articulos.categorias")


if __name__ == "__main__":
    main()
