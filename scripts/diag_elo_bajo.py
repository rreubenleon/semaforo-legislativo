"""
Diagnóstico: por qué calcular_elos solo procesa 147 legisladores.

Verifica:
  1. Cuántas filas hay en actividad_legislador con co_firmantes vacío
  2. Por cámara
  3. Cuántas tienen estatus para clasificación
  4. Cuántos legisladores únicos quedan al aplicar todos los filtros
"""
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main():
    from db import get_connection
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    FECHA_INICIO = "2024-09-01"

    print("=" * 70)
    print("Diagnóstico ELO bajo")
    print("=" * 70)

    # 1. Filas totales en actividad_legislador
    n_total = conn.execute("SELECT COUNT(*) FROM actividad_legislador").fetchone()[0]
    print(f"\n1. Total filas en actividad_legislador: {n_total:,}")

    # 2. Con legislador_id válido
    n_legid = conn.execute(
        "SELECT COUNT(*) FROM actividad_legislador WHERE legislador_id IS NOT NULL"
    ).fetchone()[0]
    print(f"2. Con legislador_id válido: {n_legid:,}")

    # 3. Con fecha >= LXVI
    n_fecha = conn.execute(
        "SELECT COUNT(*) FROM actividad_legislador "
        "WHERE legislador_id IS NOT NULL AND fecha_presentacion >= ?",
        (FECHA_INICIO,)
    ).fetchone()[0]
    print(f"3. + fecha >= {FECHA_INICIO}: {n_fecha:,}")

    # 4. Con co_firmantes vacío (individuales)
    n_indiv = conn.execute(
        "SELECT COUNT(*) FROM actividad_legislador "
        "WHERE legislador_id IS NOT NULL AND fecha_presentacion >= ? "
        "AND (co_firmantes IS NULL OR co_firmantes = '')",
        (FECHA_INICIO,)
    ).fetchone()[0]
    print(f"4. + individuales (co_firmantes IS NULL/'' ): {n_indiv:,}")

    # 5. Por cámara — cuántos legisladores únicos
    print("\n5. Legisladores únicos con ≥1 acto individual LXVI:")
    for r in conn.execute("""
        SELECT l.camara, COUNT(DISTINCT al.legislador_id) as n_leg, COUNT(*) as n_filas
        FROM actividad_legislador al
        JOIN legisladores l ON l.id = al.legislador_id
        WHERE al.fecha_presentacion >= ?
          AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
        GROUP BY l.camara
    """, (FECHA_INICIO,)):
        print(f"   {r['camara']:25s}: {r['n_leg']} legisladores, {r['n_filas']:,} filas")

    # 6. + JOIN sil_documentos + clasificacion legislativo_sustantivo o NULL
    n_join = conn.execute("""
        SELECT COUNT(*) FROM actividad_legislador al
        JOIN sil_documentos sd ON sd.id = al.sil_documento_id
        WHERE al.legislador_id IS NOT NULL
          AND sd.fecha_presentacion >= ?
          AND (sd.clasificacion = 'legislativo_sustantivo' OR sd.clasificacion IS NULL)
          AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
    """, (FECHA_INICIO,)).fetchone()[0]
    print(f"\n6. + JOIN sil_documentos + clasificacion sustantivo: {n_join:,}")

    # 7. Cuántos legisladores únicos sobreviven
    print("\n7. Legisladores únicos después de TODOS los filtros (matches ELO criteria):")
    rows = conn.execute("""
        SELECT l.camara, COUNT(DISTINCT al.legislador_id) as n_leg
        FROM actividad_legislador al
        JOIN legisladores l ON l.id = al.legislador_id
        JOIN sil_documentos sd ON sd.id = al.sil_documento_id
        WHERE al.legislador_id IS NOT NULL
          AND sd.fecha_presentacion >= ?
          AND (sd.clasificacion = 'legislativo_sustantivo' OR sd.clasificacion IS NULL)
          AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
        GROUP BY l.camara
    """, (FECHA_INICIO,)).fetchall()
    total_leg = 0
    for r in rows:
        print(f"   {r['camara']:25s}: {r['n_leg']} legisladores")
        total_leg += r['n_leg']
    print(f"   TOTAL: {total_leg}")

    # 8. Distribución por estatus de docs en filas individuales LXVI
    print("\n8. Distribución de estatus (filas individuales LXVI):")
    rows = conn.execute("""
        SELECT
          CASE
            WHEN sd.estatus LIKE '%Aprobado%' OR sd.estatus LIKE '%DOF%' OR sd.estatus LIKE '%Resuelt%' THEN 'Aprobado'
            WHEN sd.estatus LIKE 'Desechado%' OR sd.estatus LIKE 'Concluid%' THEN 'Desechado'
            WHEN sd.estatus LIKE 'Pendiente%' THEN 'Pendiente'
            WHEN sd.estatus IS NULL OR sd.estatus = '' THEN 'SinEstado'
            ELSE 'Otro'
          END as bucket,
          COUNT(*) as n
        FROM actividad_legislador al
        JOIN sil_documentos sd ON sd.id = al.sil_documento_id
        WHERE al.legislador_id IS NOT NULL
          AND sd.fecha_presentacion >= ?
          AND (sd.clasificacion = 'legislativo_sustantivo' OR sd.clasificacion IS NULL)
          AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
        GROUP BY bucket
    """, (FECHA_INICIO,)).fetchall()
    for r in rows:
        print(f"   {r['bucket']:15s}: {r['n']:,}")


if __name__ == "__main__":
    main()
