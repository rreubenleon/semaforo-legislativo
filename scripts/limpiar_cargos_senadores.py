"""
Limpia entradas basura en `legisladores.comisiones_cargo` (Senado).

Bug: el scraper original `backfill_comisiones_senado.py` (y mi primer
intento del refresh) parsearon mal el HTML del Senado y metieron items
del menú lateral como falsas comisiones (Diario de los Debates,
Documentos de Apoyo Parlamentario, Ordinarias, Especiales, etc.).

Solución: filtrar `comisiones_cargo` dejando SOLO entradas cuya
comisión esté en CATALOGO_LXVI (las 68 comisiones ordinarias del
Senado). Lo que no esté ahí o esté en una blacklist conocida, fuera.

Si después del filtrado un senador queda con cargo vacío, se marca
para re-scrape vía refresh_cargos_senadores.py.

Uso:
  python scripts/limpiar_cargos_senadores.py
  python scripts/limpiar_cargos_senadores.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
sys.path.insert(0, str(ROOT))

from scrapers.comisiones_senado_asuntos import CATALOGO_LXVI

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

CATALOGO_NORM = {(c or "").strip().lower(): c for c in CATALOGO_LXVI.values()}

# Blacklist explícita por si el catálogo no cubre todo. Coincidencia parcial.
BLACKLIST_TOKENS = (
    "diario de los debates", "documentos de apoyo", "expedientes legislativos",
    "ordinarias", "especiales", "calendario de reuniones",
    "memoria de labores", "histórico de la lxiv", "órganos técnicos",
    "instituto belisario", "centro de estudios", "contraloría interna",
    "centro de capacitación", "unidad técnica", "información parlamentaria",
    "asistencias", "consultoría jurídica", "iniciativas", "proposiciones",
    "excitativas", "comunicaciones", "semblanza", "trabajos legislativos",
    "votaciones", "intervenciones", "informe de comisiones",
    "directorio", "buscador", "calendario",
)


def es_comision_real(nombre: str) -> bool:
    n = (nombre or "").strip().lower()
    if not n or len(n) < 4:
        return False
    # Está en catálogo (match exacto o partial-startswith para evitar typos)
    if n in CATALOGO_NORM:
        return True
    # Match parcial: el nombre del catálogo puede aparecer dentro
    for k in CATALOGO_NORM:
        if k.startswith(n) or n.startswith(k):
            return True
    # Si está en blacklist, fuera
    for token in BLACKLIST_TOKENS:
        if token in n:
            return False
    # Si no está en catálogo Y no en blacklist, ser conservador y descartar.
    # (Mejor falsos negativos que basura visible.)
    return False


def limpiar(cargo_raw: str) -> tuple[str, list[str]]:
    """Devuelve (cargo_limpio, lista_descartados)."""
    if not cargo_raw:
        return "", []
    items = []
    descartados = []
    for parte in cargo_raw.split("|"):
        if ":" not in parte:
            continue
        com, rol = parte.split(":", 1)
        com = com.strip()
        rol = rol.strip()
        if es_comision_real(com):
            # Normalizar nombre al canónico del catálogo si hay match exacto
            canon = CATALOGO_NORM.get(com.lower(), com)
            items.append(f"{canon}:{rol}")
        else:
            descartados.append(parte)
    return "|".join(items), descartados


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    conn = sqlite3.connect(str(DB))
    rows = conn.execute("""
        SELECT id, nombre, comisiones_cargo
          FROM legisladores
         WHERE camara LIKE '%enado%'
           AND comisiones_cargo IS NOT NULL AND comisiones_cargo <> ''
    """).fetchall()
    logger.info(f"A revisar: {len(rows)} senadores con cargo")

    n_limpios, n_quedaron_vacios, n_intactos = 0, 0, 0
    para_re_scrape = []

    for leg_id, nombre, cargo in rows:
        nuevo, descartados = limpiar(cargo)
        if not descartados:
            n_intactos += 1
            continue
        n_limpios += 1
        if not nuevo:
            n_quedaron_vacios += 1
            para_re_scrape.append((leg_id, nombre))
        if args.dry_run:
            logger.info(f"  {nombre[:40]}: -{len(descartados)} basura, queda '{nuevo[:80]}'")
            continue
        # Construir lista plana de comisiones para campo `comisiones`
        com_lista = ", ".join(p.split(":", 1)[0] for p in nuevo.split("|") if p)
        conn.execute("""
            UPDATE legisladores
               SET comisiones_cargo = ?, comisiones = ?
             WHERE id = ?
        """, (nuevo, com_lista, leg_id))

    if not args.dry_run:
        conn.commit()

    print(f"\n=== RESUMEN ===")
    print(f"  Intactos:           {n_intactos}")
    print(f"  Limpiados:          {n_limpios}")
    print(f"  Quedaron vacíos:    {n_quedaron_vacios}  → necesitan re-scrape")
    if para_re_scrape:
        print(f"\n  Para re-scrape (--limit {len(para_re_scrape)}):")
        for leg_id, n in para_re_scrape[:10]:
            print(f"    id={leg_id} {n}")


if __name__ == "__main__":
    main()
