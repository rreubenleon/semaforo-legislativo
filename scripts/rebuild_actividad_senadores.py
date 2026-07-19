"""
Re-popular `actividad_legislador` para senadores desde `senador_instrumento`.

CONTEXTO:
  Camarillo reportado con 174 ini + 780 prop en FIAT, pero senado.gob.mx
  muestra 73 ini + 140 prop. Diagnóstico (scripts/diagnostico_camarillo.py)
  reveló que actividad_legislador tiene 862 filas con presentador en
  formato crudo "de Ciudadanos Legisladores De senadoras y senadores...",
  donde el parser de a89482e falló y le adjudicó a Camarillo solo
  (en vez de a TODOS los firmantes del bloque colectivo).

  Como `senador_instrumento` (scraper directo senado.gob.mx) ya tiene
  los datos correctos (202 instrumentos coincidentes con 73+140=213
  oficiales), reconstruimos actividad_legislador desde ahí.

ALCANCE:
  - Solo toca filas con legislador_id de senadores (camara='Senado')
  - Diputados no se tocan (su scraper SITL funciona aparte)

ESTRATEGIA:
  1. BORRAR todas las filas de actividad_legislador donde legislador_id
     pertenece a un senador.
  2. Para cada fila de senador_instrumento:
     - Mapear senador_nombre → legisladores.id (vía utils/matcher)
     - JOIN con sil_documentos para obtener tipo/categoría/fecha/título
     - INSERT en actividad_legislador con:
       * co_firmantes = '' si es_individual_perfil=1 (cuenta como individual)
       * co_firmantes = 'colectivo (N firmantes)' si es_individual_perfil=0
         (queda fuera del filtro de individuales en _tasa_lxvi)

  3. Después: hay que correr radar_pipeline.py --skip-scraper para
     recalcular l3p_* y sync D1.

Uso:
  python3 scripts/rebuild_actividad_senadores.py
  python3 scripts/rebuild_actividad_senadores.py --dry-run
"""
import argparse
import logging
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.matcher import (
    build_bd_index,
    encontrar_legislador_id,
    normalizar_nombre,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def rebuild_senadores(dry_run: bool = False):
    """Reconstruye actividad_legislador de senadores desde senador_instrumento.

    Llamable desde el pipeline (main.py paso 3e.1). Antes era one-off y el paso
    de poblado desde SIL lo revertía en cada corrida.
    Devuelve dict con {insertados, senadores, borradas}.
    """
    from db import get_connection

    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # ── 1. Snapshot del estado actual (para reportar diff) ──
    snapshot_pre = dict(
        conn.execute("""
            SELECT l.id, COUNT(a.id) as n
            FROM legisladores l
            LEFT JOIN actividad_legislador a ON a.legislador_id = l.id
            WHERE l.camara = 'Senado'
            GROUP BY l.id
        """).fetchall()
    )
    logger.info(
        f"Pre-rebuild: {sum(snapshot_pre.values())} filas totales para "
        f"{len(snapshot_pre)} senadores"
    )

    # ── 2. Build index de legisladores para matching ──
    bd_idx = build_bd_index(conn)
    senador_ids = {c[0] for c in bd_idx if c[3] in ("Senado", "Cámara de Senadores")}
    logger.info(f"Index BD: {len(bd_idx)} legisladores, {len(senador_ids)} senadores")

    # ── 3. Leer senador_instrumento joineado con sil_documentos ──
    rows = conn.execute("""
        SELECT
            si.seguimiento_id,
            si.asunto_id,
            si.senador_id_senado,
            si.senador_nombre,
            si.senador_partido,
            si.es_individual_perfil,
            si.n_firmantes_perfil,
            si.tipo_instrumento AS tipo_si,
            sd.id AS sil_doc_id,
            sd.tipo AS tipo_sil,
            sd.titulo,
            sd.categoria,
            sd.fecha_presentacion,
            sd.comision,
            sd.estatus
        FROM senador_instrumento si
        LEFT JOIN sil_documentos sd
          ON sd.seguimiento_id = si.seguimiento_id
         AND sd.asunto_id = si.asunto_id
    """).fetchall()
    logger.info(f"senador_instrumento: {len(rows)} filas")

    # ── 4. Mapear senador_nombre → legislador_id (con cache) ──
    nombre_to_id = {}
    sin_match = set()
    sin_sil_doc = 0

    nuevas_filas = []
    for r in rows:
        if r["sil_doc_id"] is None:
            sin_sil_doc += 1
            continue

        nombre = r["senador_nombre"]
        if nombre not in nombre_to_id:
            n_norm = normalizar_nombre(nombre)
            leg_id = encontrar_legislador_id(n_norm, "Senado", bd_idx)
            nombre_to_id[nombre] = leg_id

        leg_id = nombre_to_id[nombre]
        if leg_id is None:
            sin_match.add(nombre)
            continue

        if r["es_individual_perfil"]:
            co_firmantes = ""
        else:
            co_firmantes = f"colectivo ({r['n_firmantes_perfil']} firmantes)"

        nuevas_filas.append({
            "legislador_id": leg_id,
            "nombre_presentador": nombre,
            "sil_documento_id": r["sil_doc_id"],
            "tipo_instrumento": r["tipo_sil"] or r["tipo_si"],
            "categoria": r["categoria"] or "",
            "fecha_presentacion": r["fecha_presentacion"] or "",
            "titulo": r["titulo"] or "",
            "comision_turno": r["comision"] or "",
            "estatus": r["estatus"] or "",
            "co_firmantes": co_firmantes,
        })

    logger.info(f"Filas a insertar: {len(nuevas_filas)}")
    logger.info(f"Sin sil_doc match: {sin_sil_doc}")
    logger.info(f"Sin match legislador: {len(sin_match)} nombres únicos")
    if sin_match:
        logger.warning(f"  Primeros 5: {list(sin_match)[:5]}")

    # ── 5. Resumen por senador (para reportar diff) ──
    por_senador = defaultdict(int)
    for f in nuevas_filas:
        por_senador[f["legislador_id"]] += 1

    logger.info("\nTop 10 deltas (pre → post):")
    deltas = []
    for leg_id, n_post in por_senador.items():
        n_pre = snapshot_pre.get(leg_id, 0)
        deltas.append((leg_id, n_pre, n_post, n_post - n_pre))
    deltas.sort(key=lambda x: abs(x[3]), reverse=True)
    for leg_id, n_pre, n_post, d in deltas[:10]:
        nombre = conn.execute(
            "SELECT nombre FROM legisladores WHERE id = ?", (leg_id,)
        ).fetchone()["nombre"]
        logger.info(f"  {nombre[:35]:35s} {n_pre:5d} → {n_post:5d} ({d:+d})")

    if dry_run:
        logger.info("\nDRY-RUN: no se modifica nada")
        return 0

    # ── 6. DELETE filas viejas — SOLO de senadores que SÍ podemos reconstruir ──
    # Ojo (18-jul-2026): borrar los 149 senadores del índice y reinsertar solo
    # los que están en `senador_instrumento` (112) dejaba a 38 senadores CON
    # actividad real en CERO. Antes eso se tapaba porque el paso de poblado
    # desde SIL los rellenaba en la siguiente corrida; ahora que ese paso omite
    # senadores, quedarían vacíos de forma permanente. Por eso el DELETE se
    # limita a los ids efectivamente cubiertos por la fuente oficial.
    ids_reconstruibles = {f["legislador_id"] for f in nuevas_filas
                          if f.get("legislador_id") is not None}
    sin_cobertura = senador_ids - ids_reconstruibles
    if sin_cobertura:
        logger.warning(
            f"{len(sin_cobertura)} senadores sin cobertura en senador_instrumento: "
            f"se dejan INTACTOS (no se borran ni se reconstruyen). "
            f"Sus conteos siguen viniendo del SIL y NO están validados contra "
            f"senado.gob.mx."
        )
    if ids_reconstruibles:
        placeholders = ",".join("?" * len(ids_reconstruibles))
        n_borradas = conn.execute(
            f"DELETE FROM actividad_legislador "
            f"WHERE legislador_id IN ({placeholders})",
            list(ids_reconstruibles),
        ).rowcount
    else:
        n_borradas = 0
    logger.info(f"DELETE: {n_borradas} filas viejas eliminadas "
                f"({len(ids_reconstruibles)} senadores reconstruidos)")

    # ── 7. INSERT filas nuevas ──
    conn.executemany("""
        INSERT INTO actividad_legislador
            (legislador_id, nombre_presentador, sil_documento_id,
             tipo_instrumento, categoria, fecha_presentacion,
             titulo, comision_turno, estatus, co_firmantes)
        VALUES (:legislador_id, :nombre_presentador, :sil_documento_id,
                :tipo_instrumento, :categoria, :fecha_presentacion,
                :titulo, :comision_turno, :estatus, :co_firmantes)
    """, nuevas_filas)

    conn.commit()
    logger.info(f"INSERT: {len(nuevas_filas)} filas nuevas insertadas")

    # ── 8. Verificación final ──
    snapshot_post = dict(
        conn.execute("""
            SELECT l.id, COUNT(a.id) as n
            FROM legisladores l
            LEFT JOIN actividad_legislador a ON a.legislador_id = l.id
            WHERE l.camara = 'Senado'
            GROUP BY l.id
        """).fetchall()
    )
    logger.info(
        f"Post-rebuild: {sum(snapshot_post.values())} filas totales para "
        f"{len(snapshot_post)} senadores"
    )

    return {
        "sin_cobertura": len(sin_cobertura),
        "insertados": len(nuevas_filas),
        "senadores": len(snapshot_post),
        "total_filas": sum(snapshot_post.values()),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    rebuild_senadores(dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
