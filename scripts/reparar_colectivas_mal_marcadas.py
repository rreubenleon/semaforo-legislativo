"""
Repara instrumentos COLECTIVOS que están marcados como PERSONALES.

QUÉ PASABA
  `poblar_actividad_desde_sil` guardaba `co_firmantes = presentador` sólo
  cuando el parser lograba separar varios autores. Si el bloque colectivo no
  se podía partir (p. ej. "de Legisladoras y Legisladores De senadoras y
  senadores Alejandro Moreno Cárdenas, Manuel Añorve…"), devolvía UN autor y
  la fila quedaba con co_firmantes='' → contada como trabajo INDIVIDUAL.

  Medido el 19-jul-2026: 3,064 filas de Senado y 539 de Diputados. Es la raíz
  del caso Camarillo (may-2026) y de que el número de control de Beatriz
  Mojica pasara de 40 a 61 iniciativas personales.

QUÉ HACE
  Marca esas filas como colectivas (co_firmantes = el texto del presentador),
  usando la MISMA función que ahora usa el pipeline: `_es_bloque_colectivo`.
  No borra ni inserta nada — sólo corrige la etiqueta de autoría.

Uso:
    python3 scripts/reparar_colectivas_mal_marcadas.py --dry-run
    python3 scripts/reparar_colectivas_mal_marcadas.py
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection
from scrapers.legisladores import _es_bloque_colectivo


def reparar(dry_run: bool = False) -> int:
    """Recoloca en bancada las colectivas mal marcadas. Devuelve cuántas.

    Llamable desde el pipeline (main.py paso 3e.1). Idempotente.
    """
    conn = get_connection()
    filas = conn.execute("""
        SELECT a.id, a.nombre_presentador, l.camara
        FROM actividad_legislador a
        JOIN legisladores l ON l.id = a.legislador_id
        WHERE a.co_firmantes IS NULL OR a.co_firmantes = ''
    """).fetchall()

    corregir, por_camara = [], {}
    for rid, presentador, camara in filas:
        if _es_bloque_colectivo(presentador or ""):
            corregir.append((presentador, rid))
            por_camara[camara] = por_camara.get(camara, 0) + 1

    if dry_run or not corregir:
        return len(corregir)

    conn.executemany(
        "UPDATE actividad_legislador SET co_firmantes = ? WHERE id = ?", corregir
    )
    conn.commit()
    return len(corregir)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    n = reparar(dry_run=args.dry_run)
    print(("DRY-RUN · " if args.dry_run else "") + f"colectivas mal marcadas: {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
