"""
Diagnóstico: por qué los conteos por estado están en 0 después del rebuild.

Verifica para Camarillo (id=559):
  1. ¿Cuántas filas hay en actividad_legislador?
  2. ¿Qué fecha_presentacion tienen (rango, vacíos)?
  3. ¿Qué tipo_instrumento tienen?
  4. ¿Qué co_firmantes tienen (NULL vs '' vs 'colectivo...')?
  5. ¿Qué estatus en sil_documentos joineadas?
  6. Replica la query de paso_conteos_estado con prints
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

    LEG_ID = 559
    FECHA_INICIO = "2024-09-01"

    print("=" * 70)
    print(f"Diagnóstico Camarillo (id={LEG_ID}), FECHA_INICIO_LXVI={FECHA_INICIO}")
    print("=" * 70)

    # 1. Total filas
    n = conn.execute(
        "SELECT COUNT(*) FROM actividad_legislador WHERE legislador_id = ?",
        (LEG_ID,)
    ).fetchone()[0]
    print(f"\n1. Total filas en actividad_legislador: {n}")

    # 2. Fecha range
    fechas = conn.execute(
        "SELECT MIN(fecha_presentacion), MAX(fecha_presentacion), "
        "SUM(CASE WHEN fecha_presentacion IS NULL OR fecha_presentacion = '' THEN 1 ELSE 0 END) "
        "FROM actividad_legislador WHERE legislador_id = ?",
        (LEG_ID,)
    ).fetchone()
    print(f"\n2. Fechas: min={fechas[0]} max={fechas[1]} vacías={fechas[2]}")

    # 3. Tipo_instrumento distribution
    print("\n3. Tipos:")
    for r in conn.execute(
        "SELECT tipo_instrumento, COUNT(*) FROM actividad_legislador "
        "WHERE legislador_id = ? GROUP BY tipo_instrumento", (LEG_ID,)
    ).fetchall():
        print(f"   «{r[0]}»: {r[1]}")

    # 4. Co_firmantes distribution
    print("\n4. Co_firmantes:")
    for r in conn.execute(
        "SELECT CASE WHEN co_firmantes IS NULL THEN 'NULL' "
        "             WHEN co_firmantes = '' THEN 'EMPTY' "
        "             ELSE 'NON_EMPTY' END as cat, "
        "COUNT(*) FROM actividad_legislador WHERE legislador_id = ? GROUP BY cat",
        (LEG_ID,)
    ).fetchall():
        print(f"   {r[0]}: {r[1]}")

    # 5. Filas con fecha >= FECHA_INICIO_LXVI
    n_post = conn.execute(
        "SELECT COUNT(*) FROM actividad_legislador "
        "WHERE legislador_id = ? AND fecha_presentacion >= ?",
        (LEG_ID, FECHA_INICIO)
    ).fetchone()[0]
    print(f"\n5. Filas con fecha >= {FECHA_INICIO}: {n_post}")

    # 6. Estatus en sil_documentos joineadas (TOP)
    print("\n6. Estatus en sil_documentos (top 5):")
    for r in conn.execute(
        "SELECT sd.estatus, COUNT(*) FROM actividad_legislador al "
        "JOIN sil_documentos sd ON al.sil_documento_id = sd.id "
        "WHERE al.legislador_id = ? GROUP BY sd.estatus "
        "ORDER BY COUNT(*) DESC LIMIT 5",
        (LEG_ID,)
    ).fetchall():
        estatus = r[0] or "<NULL>"
        print(f"   ({r[1]:4d}) {estatus[:80]}")

    # 7. Replica paso_conteos_estado query
    print("\n7. Conteos por estado (replica query):")
    r = conn.execute("""
        SELECT
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%iniciativ%'
                    AND sd.estatus LIKE '%Aprobado%'
                    AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
                   THEN 1 ELSE 0 END) AS ini_aprob_ind,
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%iniciativ%'
                    AND sd.estatus LIKE 'Pendiente%'
                    AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
                   THEN 1 ELSE 0 END) AS ini_pend_ind,
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%iniciativ%'
                    AND (sd.estatus LIKE 'Desechado%' OR sd.estatus LIKE 'Retirad%')
                    AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
                   THEN 1 ELSE 0 END) AS ini_desech_ind
        FROM actividad_legislador al
        JOIN sil_documentos sd ON al.sil_documento_id = sd.id
        WHERE al.legislador_id = ?
          AND al.fecha_presentacion >= ?
    """, (LEG_ID, FECHA_INICIO)).fetchone()
    print(f"   ini_aprob_ind={r[0]} ini_pend_ind={r[1]} ini_desech_ind={r[2]}")

    # 8. Repeat sin filtro de fecha
    r = conn.execute("""
        SELECT
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%iniciativ%'
                    AND sd.estatus LIKE 'Pendiente%'
                   THEN 1 ELSE 0 END) AS ini_pend
        FROM actividad_legislador al
        JOIN sil_documentos sd ON al.sil_documento_id = sd.id
        WHERE al.legislador_id = ?
    """, (LEG_ID,)).fetchone()
    print(f"\n8. SIN filtro fecha (ini Pendiente): {r[0]}")


if __name__ == "__main__":
    main()
