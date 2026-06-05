"""Asigna comisión de trabajo a los documentos de la Comisión Permanente.

Problema: el scraper de gaceta_permanente.py inserta comision='' (vacío).
Por eso el dashboard solo muestra las comisiones de recesos viejos
(que sí venían del SIL) y NO las del receso actual.

En la Permanente cada asunto se turna a una de 3 comisiones de trabajo
según su MATERIA. En el 2do receso (may-ago):
  Primera  → Asuntos Políticos e Internacionales
  Segunda  → Asuntos Sociales
  Tercera  → Asuntos Económicos

Inferimos la comisión por la categoría FIAT del documento (clasificador
keyword, $0). Mapeo categoría→comisión abajo. Idempotente: solo toca
docs de la Permanente con comision vacía.

Uso: python3 scripts/enriquecer_comision_permanente.py [--dry-run]
"""
from __future__ import annotations
import argparse, sys, os
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.pop("ANTHROPIC_API_KEY", None)  # blindaje Haiku
from db import get_connection
from nlp.clasificador import clasificar_texto

# Mapeo categoría FIAT → comisión de trabajo (2do receso may-ago)
CAT_A_COMISION = {
    # Primera — Asuntos Políticos e Internacionales
    "electoral_politico": "Primera", "relaciones_exteriores": "Primera",
    "seguridad_justicia": "Primera", "anticorrupcion": "Primera",
    "derechos_humanos": "Primera", "administracion": "Primera",
    # Segunda — Asuntos Sociales
    "salud": "Segunda", "educacion": "Segunda", "trabajo": "Segunda",
    "igualdad_genero": "Segunda", "politica_social": "Segunda",
    "medio_ambiente": "Segunda", "medios_comunicacion": "Segunda",
    # Tercera — Asuntos Económicos
    "economia_hacienda": "Tercera", "energia": "Tercera",
    "infraestructura": "Tercera", "agro_rural": "Tercera",
    "turismo": "Tercera", "inteligencia_artificial": "Tercera",
}
# El sufijo de fechas es OBLIGATORIO: _obtener_permanente_stats agrupa
# recesos extrayendo "(DD/MM/YYYY-DD/MM/YYYY)" del nombre y descarta los
# que no lo tengan. 2do receso 2do año = 01/05/2026 - 31/08/2026.
SUFIJO = " (01/05/2026-31/08/2026) de Comisión Permanente"
NOMBRES = {
    "Primera": "Primera Comisión de Trabajo: Asuntos Políticos e Internacionales" + SUFIJO,
    "Segunda": "Segunda Comisión de Trabajo: Asuntos Sociales" + SUFIJO,
    "Tercera": "Tercera Comisión de Trabajo: Asuntos Económicos" + SUFIJO,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    conn = get_connection()

    rows = conn.execute("""
        SELECT id, titulo, sinopsis, categoria FROM sil_documentos
        WHERE camara='Comisión Permanente'
          AND (comision IS NULL OR comision='')
          AND fecha_presentacion >= '2026-05-01'
    """).fetchall()
    print(f"Docs Permanente sin comisión (receso actual): {len(rows)}")

    asignados = {"Primera": 0, "Segunda": 0, "Tercera": 0, "sin_match": 0}
    updates = []
    for doc_id, titulo, sinopsis, cat_raw in rows:
        # Categoría: usar la ya guardada, o clasificar al vuelo
        cat = None
        if cat_raw:
            cat = cat_raw.split(":")[0].split(",")[0].strip()
        if not cat or cat not in CAT_A_COMISION:
            res = clasificar_texto(titulo or "", sinopsis or "", None)
            if res:
                cat = max(res.items(), key=lambda x: x[1])[0]
        comision_orden = CAT_A_COMISION.get(cat)
        if not comision_orden:
            asignados["sin_match"] += 1
            continue
        asignados[comision_orden] += 1
        updates.append((NOMBRES[comision_orden], doc_id))

    print("Distribución inferida:")
    for k, v in asignados.items():
        print(f"  {k}: {v}")

    if args.dry_run:
        print("[DRY-RUN] no se escribe")
        return

    for nombre, doc_id in updates:
        conn.execute("UPDATE sil_documentos SET comision=? WHERE id=?", (nombre, doc_id))
    conn.commit()
    print(f"\n✅ {len(updates)} docs actualizados con comisión de trabajo")


if __name__ == "__main__":
    main()
