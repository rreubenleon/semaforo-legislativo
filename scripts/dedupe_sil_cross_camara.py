"""
Marca duplicados reales del SIL usando `seguimiento_id`.

Contexto. Un primer intento usó título normalizado y colapsó miles de
iniciativas distintas que comparten encabezado genérico ("Que reforma y
adiciona diversas disposiciones de la Ley General de Salud", etc.). Ese
criterio sobrededuplicaba: el universo resultante quedaba 10% por debajo del
oficial de Numeralia.

Criterio correcto. En el SIL, `seguimiento_id` agrupa todas las etapas
procesales del mismo asunto, incluida la migración entre cámaras cuando se
convierte en minuta. Dos registros con el mismo `seguimiento_id` son el mismo
expediente. Se conserva el de `fecha_presentacion` más antigua y se marca el
resto como `es_duplicado_cross_camara=1`.

La columna se mantiene por compatibilidad con código consumidor, aunque la
heurística ya no depende del cruce de cámara. Agrega columna si no existe.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection, close as close_db


def main(dry_run: bool = False):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(sil_documentos)")
    cols = {r[1] for r in cur.fetchall()}
    if "es_duplicado_cross_camara" not in cols:
        if dry_run:
            print("(dry-run: agregaría columna es_duplicado_cross_camara)")
        else:
            cur.execute(
                "ALTER TABLE sil_documentos "
                "ADD COLUMN es_duplicado_cross_camara INTEGER DEFAULT 0"
            )
            conn.commit()
            print("Columna es_duplicado_cross_camara creada.")

    # Resetear marcas previas: este script es idempotente y reemplaza cualquier
    # corrida anterior (incluida la versión vieja por título).
    if not dry_run:
        cur.execute(
            "UPDATE sil_documentos SET es_duplicado_cross_camara = 0 "
            "WHERE tipo IN ('Iniciativa','Proposición con punto de acuerdo')"
        )

    rows = cur.execute("""
        SELECT id, seguimiento_id, asunto_id, fecha_presentacion, tipo
        FROM sil_documentos
        WHERE tipo IN ('Iniciativa','Proposición con punto de acuerdo')
          AND seguimiento_id IS NOT NULL AND seguimiento_id != ''
        ORDER BY
          CASE WHEN fecha_presentacion IS NULL OR fecha_presentacion = ''
               THEN 1 ELSE 0 END,
          fecha_presentacion ASC,
          id ASC
    """).fetchall()

    print(f"Filas candidatas: {len(rows)}")

    visto_por_tipo = {
        "Iniciativa": {},
        "Proposición con punto de acuerdo": {},
    }
    duplicados = []

    for rid, seg, aid, fecha, tipo in rows:
        tabla = visto_por_tipo.setdefault(tipo, {})
        if seg not in tabla:
            tabla[seg] = rid
        else:
            duplicados.append(rid)

    print(f"Duplicados por seguimiento_id detectados: {len(duplicados)}")
    for t, tabla in visto_por_tipo.items():
        print(f"  {t}: {len(tabla)} expedientes únicos")

    if dry_run:
        print("\n(dry-run: sin escribir en DB)")
        return

    cur.executemany(
        "UPDATE sil_documentos SET es_duplicado_cross_camara = 1 WHERE id = ?",
        [(rid,) for rid in duplicados],
    )
    conn.commit()
    print(f"\nMarcadas {len(duplicados)} filas como duplicadas.")

    # Conteo final útil para FIAT
    print("\n=== Universo FIAT post-dedupe (backbone al 27-mar-2026) ===")
    for tipo_filtro, etiqueta in [
        ("Iniciativa", "Iniciativas"),
        ("Proposición con punto de acuerdo", "Proposiciones"),
    ]:
        n = cur.execute(
            """SELECT COUNT(*) FROM sil_documentos
               WHERE tipo = ?
                 AND fecha_presentacion >= '2024-09-01'
                 AND fecha_presentacion <= '2026-03-27'
                 AND COALESCE(es_duplicado_cross_camara,0) = 0""",
            (tipo_filtro,),
        ).fetchone()[0]
        print(f"  {etiqueta}: {n}")

    close_db()


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    main(dry_run=dry)
