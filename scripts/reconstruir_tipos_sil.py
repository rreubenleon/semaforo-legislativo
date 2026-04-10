"""
Reconstruye el campo `tipo` de sil_documentos a partir de patrones de título.

Causa raíz: en runs pasados del scraper, `detalle_max` topaba a 100 documentos
por corrida. El resto caía al fallback `tipo_badge='Asunto'` del listado de
búsqueda y nunca recuperaba su tipo real. Este script cubre la deuda usando
heurística de prefijo de título, que en la práctica tiene precisión >99% para
iniciativas y proposiciones.

Categorías destino (solo las relevantes para FIAT):
  - Iniciativa
  - Proposición con punto de acuerdo

Todo lo demás se etiqueta `Otro` con subtipo informativo en `tipo`. El
scoring del FIAT no lee esas filas, pero quedan disponibles para auditoría.

No sobreescribe tipos ya correctos (Iniciativa, Proposición con punto de
acuerdo, Dictamen*, Minuta, Acuerdo parlamentario, Instrumento internacional).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection, close as close_db

TIPOS_PROTEGIDOS = {
    "Iniciativa",
    "Iniciativa de inicio en cámara de diputados",
    "Proposición con punto de acuerdo",
    "Dictamen a discusión",
    "Dictamen de primera lectura / declaratoria de publicidad",
    "Dictamen en sentido negativo",
    "Minuta",
    "Acuerdo parlamentario",
    "Instrumento internacional",
}

PREFIJOS_INICIATIVA = (
    "que reforma",
    "que adiciona",
    "que deroga",
    "que expide",
    "que abroga",
    "que modifica",
    "que crea",
    "que reordena",
    "con proyecto de decreto",
    "con proyecto de ley",
    "de decreto por el que se inscribe",
    "de decreto para inscribir",
    "que establece las características de una moneda",
    "iniciativa",
    "proyecto de decreto",
)

PREFIJOS_PROPOSICION = (
    "por el que exhorta",
    "por la que exhorta",
    "por el que se exhorta",
    "por la que se exhorta",
    "por lo que exhorta",
    "por el que solicita",
    "por la que solicita",
    "por el que se solicita",
    "por la que se solicita",
    "con punto de acuerdo",
    "proposición",
    "relativo a",
    "relativa a",
    "relativo al",
    "relativa al",
)

# Patrones que NO son asuntos presentados (quedan como Otro para auditoría)
PREFIJOS_OTRO = {
    "comunicación": (
        "con la que remite",
        "con el que remite",
        "con la que informa",
        "con el que informa",
        "con la cual remite",
        "con la cual informa",
        "mediante la cual remite",
        "con el que se remite",
        "con la que se remite",
        "por el que remite",
    ),
    "respuesta_punto_acuerdo": (
        "al punto de acuerdo",
        "a los puntos de acuerdo",
    ),
    "retiro": (
        "con el que solicita retiro",
        "con la que solicita retiro",
        "con el que solicita el retiro",
        "con la que solicita el retiro",
    ),
    "dictamen_negativo": (
        "por el que se desecha",
        "por el que desecha",
        "por la que se desecha",
    ),
    "integracion_comisiones": (
        "por el que se modifica la integración",
        "por los que se modifica la integración",
        "por el que se designa",
        "por el que se nombra",
    ),
    "intervencion_parlamentaria": (
        "para referirse",
        "para guardar un minuto de silencio",
        "intervención de",
    ),
}


def clasificar_por_titulo(titulo: str) -> str:
    """
    Devuelve el tipo inferido. Categorías retornadas:
      - Iniciativa
      - Proposición con punto de acuerdo
      - Otro:<subtipo>
      - '' (si no hay match)
    """
    if not titulo:
        return ""
    t = titulo.strip().lower()

    for p in PREFIJOS_INICIATIVA:
        if t.startswith(p):
            return "Iniciativa"

    for p in PREFIJOS_PROPOSICION:
        if t.startswith(p):
            return "Proposición con punto de acuerdo"

    for sub, prefijos in PREFIJOS_OTRO.items():
        for p in prefijos:
            if t.startswith(p):
                return f"Otro:{sub}"

    return ""


def main(dry_run: bool = False):
    conn = get_connection()
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT id, tipo, titulo
        FROM sil_documentos
        WHERE (tipo='Asunto' OR tipo='' OR tipo IS NULL)
    """).fetchall()

    print(f"Filas candidatas a reclasificar: {len(rows)}")

    stats = {
        "Iniciativa": 0,
        "Proposición con punto de acuerdo": 0,
        "Otro": 0,
        "sin_match": 0,
    }

    updates = []
    for row in rows:
        rid, tipo_actual, titulo = row[0], row[1], row[2]
        nuevo = clasificar_por_titulo(titulo or "")
        if not nuevo:
            stats["sin_match"] += 1
            continue

        if nuevo.startswith("Otro:"):
            stats["Otro"] += 1
        else:
            stats[nuevo] += 1

        updates.append((nuevo, rid))

    print("\nDistribución post-heurística:")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    if dry_run:
        print("\n(dry-run: sin escribir en DB)")
        return

    cur.executemany(
        "UPDATE sil_documentos SET tipo = ? WHERE id = ?",
        updates,
    )
    conn.commit()
    print(f"\nActualizadas {len(updates)} filas.")

    # Verificación final por distinct asunto_id con tipología útil
    print("\n=== Conteo post-limpieza (LXVI, backbone al 27-mar-2026) ===")
    for tipo_filtro, etiqueta in [
        ("Iniciativa", "Iniciativas"),
        ("Proposición con punto de acuerdo", "Proposiciones"),
    ]:
        n = cur.execute(
            """SELECT COUNT(DISTINCT asunto_id) FROM sil_documentos
               WHERE tipo = ?
                 AND fecha_presentacion >= '2024-09-01'
                 AND fecha_presentacion <= '2026-03-27'""",
            (tipo_filtro,),
        ).fetchone()[0]
        print(f"  {etiqueta}: {n}")

    close_db()


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    main(dry_run=dry)
