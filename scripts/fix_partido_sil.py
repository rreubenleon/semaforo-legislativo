"""
Repara el campo partido en sil_documentos cruzando contra la tabla
legisladores (que tiene los partidos correctos del SIL Gob).

Bug original: el scraper oficial del Senado asignaba "PAN" a todos los
opositores y "MORENA" a todos los oficialistas, sin distinguir
PRI / PVEM / PT / MC.

Estrategia:
  1. Por cada doc LXVI con presentador, extraer el primer firmante.
  2. Usar utils.matcher.encontrar_legislador_id (matcher robusto, igual al
     que usamos en cargar_reelectos.py / cargar_permanente_lxvi.py).
  3. Si encuentra match ÚNICO → reemplazar partido y reformatear presentador.
  4. Si no encuentra match → dejar como está.

Idempotente.

Uso:
    python scripts/fix_partido_sil.py
    python scripts/fix_partido_sil.py --dry-run
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

from utils.matcher import build_bd_index, encontrar_legislador_id, normalizar_nombre

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def norm_partido(p):
    p = (p or "").strip()
    if p.lower() == "morena":
        return "MORENA"
    return p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    # Cargar índice del matcher central
    bd_idx = build_bd_index(conn)

    # Para cada legislador_id, obtener partido autoritativo
    leg_partido = {}
    for r in conn.execute(
        "SELECT id, partido FROM legisladores WHERE partido IS NOT NULL"
    ).fetchall():
        leg_partido[r["id"]] = norm_partido(r["partido"])

    docs = conn.execute("""
        SELECT id, presentador, partido, camara, seguimiento_id
        FROM sil_documentos
        WHERE legislatura = 'LXVI'
          AND presentador IS NOT NULL AND presentador != ''
    """).fetchall()
    logger.info(f"Docs LXVI con presentador: {len(docs)}")

    cambios = 0
    sin_match = 0
    confirma = 0
    actualizaciones = []
    cambios_por_partido = {}

    for doc in docs:
        pres = doc["presentador"]
        camara = doc["camara"]

        # Extraer primer firmante: 'Sen. NOMBRE (PARTIDO)' o 'Dip. NOMBRE (PARTIDO)'
        m_first = re.match(r"^(Sen|Dip)\.\s+([^()]+?)\s*\(([^)]+)\)", pres)
        if not m_first:
            sin_match += 1
            continue

        rol_pre = m_first.group(1)  # Sen | Dip
        nombre_pres = m_first.group(2).strip()
        partido_actual = norm_partido(m_first.group(3).strip())

        # Determinar cámara correcta para el matcher
        camara_match = "Senado" if rol_pre == "Sen" else "Diputados"

        nombre_n = normalizar_nombre(nombre_pres)
        leg_id = encontrar_legislador_id(nombre_n, camara_match, bd_idx)

        if not leg_id:
            sin_match += 1
            continue

        partido_correcto = leg_partido.get(leg_id)
        if not partido_correcto:
            sin_match += 1
            continue

        if partido_correcto == partido_actual:
            confirma += 1
            continue

        # Reformatear el primer firmante con el partido correcto.
        # Mantener resto de co-firmantes intacto (otros bugs separados).
        nuevo_pres = re.sub(
            r"^(Sen|Dip)\.\s+([^()]+?)\s*\(([^)]+)\)",
            lambda m: f"{m.group(1)}. {m.group(2).strip()} ({partido_correcto})",
            pres,
            count=1,
        )

        actualizaciones.append((doc["id"], partido_correcto, nuevo_pres,
                                partido_actual, nombre_pres))
        cambios += 1
        key = f"{partido_actual} → {partido_correcto}"
        cambios_por_partido[key] = cambios_por_partido.get(key, 0) + 1

    print()
    print("=" * 60)
    print("== Fix partido en sil_documentos (matcher central) ==")
    print("=" * 60)
    print(f"  Docs procesados:                {len(docs)}")
    print(f"  Partido confirmado (ya OK):     {confirma}")
    print(f"  Sin match en matcher:           {sin_match}")
    print(f"  Cambios a aplicar:              {cambios}")
    print()
    print("  Cambios por transición:")
    for k, n in sorted(cambios_por_partido.items(), key=lambda x: -x[1]):
        print(f"    {k:30} {n}")

    if cambios > 0 and not args.dry_run:
        for doc_id, partido, pres, _, _ in actualizaciones:
            conn.execute(
                "UPDATE sil_documentos SET partido = ?, presentador = ? WHERE id = ?",
                (partido, pres, doc_id),
            )
        conn.commit()
        print(f"\n  ✅ {cambios} filas actualizadas en BD")

    if args.dry_run:
        print("\n  *** DRY RUN — no se escribió la BD ***")
        print("\nMuestra primeros 10 cambios:")
        for doc_id, partido, pres, partido_old, nombre in actualizaciones[:10]:
            print(f"  {nombre}: {partido_old} → {partido}")

    print()
    print("Distribución partido post (esperada):")
    for r in conn.execute("""
        SELECT partido, COUNT(*) FROM sil_documentos
        WHERE legislatura='LXVI' AND presentador IS NOT NULL
          AND tipo_grupo IN ('Iniciativa', 'Proposición con Punto de Acuerdo')
        GROUP BY partido ORDER BY 2 DESC
    """).fetchall():
        print(f"  {(r[0] or '(vacío)'):12} {r[1]:>5}")


if __name__ == "__main__":
    main()
