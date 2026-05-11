"""
Diagnóstico del inflado de Camarillo (y otros senadores) en actividad_legislador.

Reportado: Camarillo aparece con 161 ini + 763 prop en FIAT, pero el
Senado oficial (https://www.senado.gob.mx/66/senador/1565) muestra
73 ini + 140 prop. Inflación de ~4×.

Este script audita la BD viva:
  1. Cuenta filas en actividad_legislador para Camarillo (por tipo)
  2. Verifica duplicados por (sil_documento_id, legislador_id)
  3. Verifica si está matcheando docs que no son suyos (vía nombre_presentador)
  4. Compara con senador_instrumento (otra fuente del mismo dato)
  5. Reporta agregado por nombre exacto en nombre_presentador

Uso:
  python3 scripts/diagnostico_camarillo.py
"""
import logging
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def diagnostico(conn, leg_id: int, nombre_aprox: str):
    conn.row_factory = sqlite3.Row

    leg = conn.execute(
        "SELECT id, nombre, camara, partido FROM legisladores WHERE id = ?",
        (leg_id,),
    ).fetchone()
    if not leg:
        logger.error(f"No existe legislador id={leg_id}")
        return
    logger.info(f"Legislador {leg_id}: {leg['nombre']} ({leg['camara']}, {leg['partido']})")
    print()

    # ── 1. actividad_legislador conteos ──
    print("=" * 70)
    print("1. actividad_legislador (lo que radar_pipeline.py usa para l3p)")
    print("=" * 70)
    total = conn.execute(
        "SELECT COUNT(*) FROM actividad_legislador WHERE legislador_id = ?",
        (leg_id,),
    ).fetchone()[0]
    unicos = conn.execute(
        "SELECT COUNT(DISTINCT sil_documento_id) FROM actividad_legislador "
        "WHERE legislador_id = ?",
        (leg_id,),
    ).fetchone()[0]
    print(f"  Filas totales: {total}")
    print(f"  sil_documento_id únicos: {unicos}")
    print(f"  Ratio duplicación: {total/unicos if unicos else 0:.2f}x")
    print()

    print("  Por tipo_instrumento:")
    for r in conn.execute(
        "SELECT tipo_instrumento, COUNT(*) as filas, "
        "COUNT(DISTINCT sil_documento_id) as unicos "
        "FROM actividad_legislador WHERE legislador_id = ? "
        "GROUP BY tipo_instrumento ORDER BY filas DESC",
        (leg_id,),
    ).fetchall():
        print(f"    {r['tipo_instrumento'] or '<NULL>':35s}: "
              f"{r['filas']:5d} filas, {r['unicos']:5d} únicos")
    print()

    # ── 2. Duplicados por sil_documento_id ──
    print("=" * 70)
    print("2. Duplicados por sil_documento_id (mismo doc, múltiples filas)")
    print("=" * 70)
    dups = conn.execute(
        "SELECT sil_documento_id, COUNT(*) as n FROM actividad_legislador "
        "WHERE legislador_id = ? GROUP BY sil_documento_id "
        "HAVING n > 1 ORDER BY n DESC LIMIT 10",
        (leg_id,),
    ).fetchall()
    if not dups:
        print("  Sin duplicados ✓")
    else:
        print(f"  {len(dups)} sil_documento_id duplicados (top 10):")
        for r in dups:
            doc = conn.execute(
                "SELECT titulo, tipo FROM sil_documentos WHERE id = ?",
                (r["sil_documento_id"],),
            ).fetchone()
            titulo = (doc["titulo"][:60] if doc else "?") + "…"
            print(f"    doc_id={r['sil_documento_id']} ({r['n']}x) {titulo}")
    print()

    # ── 3. Nombres distintos como presentador ──
    print("=" * 70)
    print("3. Cómo aparece su nombre en nombre_presentador")
    print("=" * 70)
    nombres = conn.execute(
        "SELECT nombre_presentador, COUNT(*) as n FROM actividad_legislador "
        "WHERE legislador_id = ? GROUP BY nombre_presentador "
        "ORDER BY n DESC LIMIT 15",
        (leg_id,),
    ).fetchall()
    for r in nombres:
        print(f"    {r['n']:4d}× «{r['nombre_presentador']}»")
    print()

    # ── 4. senador_instrumento (fuente alternativa, scraper Senado directo) ──
    print("=" * 70)
    print("4. senador_instrumento (scraper senado.gob.mx directo)")
    print("=" * 70)
    si_total = conn.execute(
        "SELECT COUNT(*) FROM senador_instrumento "
        "WHERE senador_nombre LIKE ?",
        (f"%{nombre_aprox}%",),
    ).fetchone()[0]
    si_indv = conn.execute(
        "SELECT COUNT(*) FROM senador_instrumento "
        "WHERE senador_nombre LIKE ? AND es_individual_perfil = 1",
        (f"%{nombre_aprox}%",),
    ).fetchone()[0]
    print(f"  Total: {si_total}")
    print(f"  Individuales: {si_indv}")
    print(f"  Por tipo:")
    for r in conn.execute(
        "SELECT tipo_instrumento, COUNT(*) as n FROM senador_instrumento "
        "WHERE senador_nombre LIKE ? GROUP BY tipo_instrumento ORDER BY n DESC",
        (f"%{nombre_aprox}%",),
    ).fetchall():
        print(f"    {r['tipo_instrumento'] or '<NULL>':35s}: {r['n']}")
    print()

    # ── 5. ¿Cuántos otros legisladores podrían estar matcheando los mismos docs? ──
    print("=" * 70)
    print("5. Match cruzado: cuántos legisladores comparten estos docs")
    print("=" * 70)
    cruzado = conn.execute(
        "SELECT a2.legislador_id, l2.nombre, COUNT(DISTINCT a2.sil_documento_id) as n "
        "FROM actividad_legislador a "
        "JOIN actividad_legislador a2 ON a2.sil_documento_id = a.sil_documento_id "
        "  AND a2.legislador_id != a.legislador_id "
        "JOIN legisladores l2 ON l2.id = a2.legislador_id "
        "WHERE a.legislador_id = ? "
        "GROUP BY a2.legislador_id ORDER BY n DESC LIMIT 5",
        (leg_id,),
    ).fetchall()
    if not cruzado:
        print("  Camarillo no comparte docs con otros legisladores")
    else:
        print("  Top 5 legisladores que comparten docs con Camarillo:")
        for r in cruzado:
            print(f"    {r['n']:4d} docs ← {r['nombre']} (id={r['legislador_id']})")
    print()


def main():
    from db import get_connection

    conn = get_connection()
    diagnostico(conn, leg_id=559, nombre_aprox="Camarillo")


if __name__ == "__main__":
    main()
