"""
Cruce SIL ↔ Gaceta por match difuso de título.

Problema: SIL y Gaceta guardan el mismo asunto legislativo con títulos
levemente distintos. SIL guarda el cuerpo ("Que reforma la Ley X...").
Gaceta guarda con envoltura ("Iniciativa que reforma la Ley X...").
El match literal no funciona.

Solución: normalización agresiva.
  1. Minúsculas, colapso de espacios, quitar puntuación final.
  2. Quitar prefijos de rol en Gaceta: 'iniciativa', 'proposicion con
     punto de acuerdo', etc.
  3. Tomar primeros 150 chars del resultado como clave de match.

Genera un reporte con:
  - Gaceta (inic+prop) en ventana dada: total
  - Cuántos hacen match con algún registro SIL del mismo tipo
  - Cuántos NO hacen match (candidatos a aportar al universo)

Opcional: con --insertar crea registros en sil_documentos a partir de los
de Gaceta sin match, usando ids sintéticos (seguimiento_id='G<id>',
asunto_id='G<id>'), para cerrar el backbone.
"""

import os
import re
import sys
import unicodedata
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection, close as close_db

PREFIJOS_GACETA = (
    "iniciativa con proyecto de decreto",
    "proposición con punto de acuerdo",
    "proposicion con punto de acuerdo",
    "con proyecto de decreto",
    "proyecto de decreto",
    "iniciativa",
    "proposición",
    "proposicion",
    "dictamen",
    "minuta",
)


def _quitar_acentos(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def _clave_match(titulo: str) -> str:
    if not titulo:
        return ""
    t = _quitar_acentos(titulo.strip().lower())
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    for p in PREFIJOS_GACETA:
        pn = _quitar_acentos(p)
        if t.startswith(pn):
            t = t[len(pn):].strip()
            break
    # Algunas gacetas dicen "que reforma..." y SIL también. Ambos quedan igual.
    return t[:150]


def main(
    fecha_desde: str = "2024-09-01",
    fecha_hasta: str = "2026-04-10",
    insertar: bool = False,
):
    conn = get_connection()
    cur = conn.cursor()

    # Construir índice de claves SIL por tipo
    print(f"Ventana: {fecha_desde} a {fecha_hasta}")
    print("\nIndexando SIL (iniciativas + proposiciones)...")
    sil_keys = {"Iniciativa": set(), "Proposición con punto de acuerdo": set()}
    rows = cur.execute("""
        SELECT tipo, titulo FROM sil_documentos
        WHERE tipo IN ('Iniciativa','Proposición con punto de acuerdo')
          AND COALESCE(es_duplicado_cross_camara,0) = 0
    """).fetchall()
    for tipo, titulo in rows:
        k = _clave_match(titulo)
        if k:
            sil_keys[tipo].add(k)
    print(f"  Iniciativas SIL: {len(sil_keys['Iniciativa'])} claves")
    print(f"  Proposiciones SIL: {len(sil_keys['Proposición con punto de acuerdo'])} claves")

    # Ahora recorrer Gaceta de la ventana y medir match
    print("\nCruzando contra Gaceta...")
    mapping = {
        "iniciativa": "Iniciativa",
        "proposicion": "Proposición con punto de acuerdo",
    }

    stats = {"match_inic": 0, "miss_inic": 0, "match_prop": 0, "miss_prop": 0}
    misses = []

    rows = cur.execute("""
        SELECT id, tipo, titulo, fecha, camara FROM gaceta
        WHERE tipo IN ('iniciativa','proposicion')
          AND fecha >= ? AND fecha <= ?
    """, (fecha_desde, fecha_hasta)).fetchall()

    for gid, tipo, titulo, fecha, camara in rows:
        sil_tipo = mapping.get(tipo)
        if not sil_tipo:
            continue
        k = _clave_match(titulo)
        if not k:
            continue
        if k in sil_keys[sil_tipo]:
            if sil_tipo == "Iniciativa":
                stats["match_inic"] += 1
            else:
                stats["match_prop"] += 1
        else:
            if sil_tipo == "Iniciativa":
                stats["miss_inic"] += 1
            else:
                stats["miss_prop"] += 1
            misses.append((gid, sil_tipo, titulo, fecha, camara))

    total_gac_inic = stats["match_inic"] + stats["miss_inic"]
    total_gac_prop = stats["match_prop"] + stats["miss_prop"]
    pct_i = 100.0 * stats["match_inic"] / total_gac_inic if total_gac_inic else 0
    pct_p = 100.0 * stats["match_prop"] / total_gac_prop if total_gac_prop else 0

    print(f"\n=== Cruce SIL ↔ Gaceta ({fecha_desde} → {fecha_hasta}) ===")
    print(f"Gaceta iniciativas: {total_gac_inic}")
    print(f"  match en SIL: {stats['match_inic']} ({pct_i:.1f}%)")
    print(f"  sin match:   {stats['miss_inic']}")
    print(f"Gaceta proposiciones: {total_gac_prop}")
    print(f"  match en SIL: {stats['match_prop']} ({pct_p:.1f}%)")
    print(f"  sin match:   {stats['miss_prop']}")

    print(f"\nAporte neto de Gaceta al universo SIL: {len(misses)}")

    if misses:
        print("\nMuestra de misses:")
        for row in misses[:5]:
            print(f"  [{row[1]}] {row[3]} | {row[2][:80]}")

    if insertar and misses:
        print("\nInsertando misses como registros SIL sintéticos...")
        ahora = datetime.now().isoformat()
        insertados = 0
        for gid, sil_tipo, titulo, fecha, camara in misses:
            seg_id = f"G{gid}"
            asu_id = f"G{gid}"
            try:
                cur.execute(
                    """
                    INSERT INTO sil_documentos
                        (seguimiento_id, asunto_id, tipo, titulo, sinopsis,
                         camara, fecha_presentacion, legislatura, periodo,
                         estatus, partido, comision, categoria, fecha_scraping)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (seg_id, asu_id, sil_tipo, titulo[:500], "",
                     camara or "", fecha, "LXVI", "",
                     "", "", "", "", ahora),
                )
                insertados += 1
            except Exception:
                pass
        conn.commit()
        print(f"Insertados: {insertados}")

    close_db()


if __name__ == "__main__":
    insertar = "--insertar" in sys.argv
    main(insertar=insertar)
