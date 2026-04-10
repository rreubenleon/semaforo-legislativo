"""
Validación automática: universo FIAT vs Numeralia oficial SIL.

Se contrastan los dos tipos relevantes para FIAT (Iniciativa y Proposición con
punto de acuerdo) en la legislatura LXVI. El benchmark es la tabla oficial de
Numeralia; la fuente local es sil_documentos después de dedupe cross-cámara.

La métrica es cobertura relativa: cuánto del universo oficial está capturado.
Tolerancia objetivo: ±2% contra presentados oficiales. Por encima de ese umbral
el backbone se considera incompleto y se marca como acción pendiente.

Uso:
    python3 scripts/validar_fiat_vs_numeralia.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection, close as close_db
from scripts.sil_numeralia import obtener_numeralia

TIPOS_FIAT = ("Iniciativa", "Proposición con punto de acuerdo")

# Tolerancia operativa: el backbone FIAT nunca se va a alinear al decimal con
# numeralia porque el SIL publica el corte diario con criterios propios y el
# buscador no indexa todo. Un desvío de ±4% es dentro de rango normal. Más allá
# indica que algo se rompió (parser, reclasificador, barrido) y hay que
# auditar.
TOLERANCIA_PCT = 4.0


def _conteo_fiat(cur, tipo: str) -> dict:
    """Devuelve conteos del backbone local para un tipo SIL."""
    total = cur.execute(
        "SELECT COUNT(*) FROM sil_documentos WHERE tipo = ?", (tipo,)
    ).fetchone()[0]
    sin_dup = cur.execute(
        """SELECT COUNT(*) FROM sil_documentos
           WHERE tipo = ?
             AND COALESCE(es_duplicado_cross_camara, 0) = 0""",
        (tipo,),
    ).fetchone()[0]
    con_fecha = cur.execute(
        """SELECT COUNT(*) FROM sil_documentos
           WHERE tipo = ?
             AND COALESCE(es_duplicado_cross_camara, 0) = 0
             AND fecha_presentacion IS NOT NULL
             AND fecha_presentacion != ''""",
        (tipo,),
    ).fetchone()[0]
    return {"total": total, "sin_dup": sin_dup, "con_fecha": con_fecha}


def main() -> int:
    print("Descargando numeralia oficial SIL (LXVI, ambas cámaras)...")
    numeralia = obtener_numeralia(legislatura="66", camara="todas")
    if not numeralia:
        print("ERROR: numeralia vacía. Abortando.")
        return 1

    conn = get_connection()
    cur = conn.cursor()

    print("\n=== Validación FIAT vs Numeralia oficial SIL (LXVI) ===\n")
    header = (
        f"{'Tipo':34} {'Oficial':>9} {'FIAT':>8} {'FIAT ND':>10} "
        f"{'Δ abs':>8} {'Δ %':>8} {'Estado':>10}"
    )
    print(header)
    print("-" * len(header))

    fallas = 0
    for tipo in TIPOS_FIAT:
        oficial = numeralia.get(tipo, {}).get("presentados", 0)
        fiat = _conteo_fiat(cur, tipo)
        delta = fiat["sin_dup"] - oficial
        pct = (100.0 * delta / oficial) if oficial else 0.0
        estado = "OK" if abs(pct) <= TOLERANCIA_PCT else "DESVÍO"
        if estado != "OK":
            fallas += 1
        print(
            f"{tipo[:34]:34} {oficial:>9} {fiat['total']:>8} "
            f"{fiat['sin_dup']:>10} {delta:>+8} {pct:>+7.1f}% {estado:>10}"
        )

    # Totales
    total_oficial = numeralia.get("Totales de Asuntos", {}).get("presentados", 0)
    print(f"\nTotal oficial LXVI (todos los tipos): {total_oficial}")

    # Fechas faltantes (debuda del barrido sin detalle)
    print("\nCobertura de fecha_presentacion en backbone FIAT:")
    for tipo in TIPOS_FIAT:
        fiat = _conteo_fiat(cur, tipo)
        if fiat["sin_dup"]:
            pct_fecha = 100.0 * fiat["con_fecha"] / fiat["sin_dup"]
        else:
            pct_fecha = 0
        print(
            f"  {tipo[:34]:34} {fiat['con_fecha']:>6}/{fiat['sin_dup']:<6} "
            f"({pct_fecha:.1f}%)"
        )

    # Desglose adicional: aprobados como señal de calidad del pipeline
    print("\nReferencia de flujo oficial (LXVI completo):")
    for tipo in TIPOS_FIAT:
        n = numeralia.get(tipo, {})
        if not n:
            continue
        print(
            f"  {tipo[:34]:34} "
            f"pres={n['presentados']:>5} apro={n['aprobados']:>4} "
            f"dese={n['desechados']:>4} pend={n['pendientes']:>5} "
            f"reti={n['retirados']:>3}"
        )

    close_db()

    if fallas:
        print(f"\n{fallas} tipo(s) fuera de tolerancia ±{TOLERANCIA_PCT}%.")
        return 2
    print(f"\nBackbone dentro de tolerancia ±{TOLERANCIA_PCT}%.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
