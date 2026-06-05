"""Re-parsea el campo `estatus` del SIL, que viene con 3 sub-campos
pegados sin separador, a columnas limpias.

El SIL guarda el "último estatus" como bloque:
  "<ESTADO><SITUACION><FECHA>"
  ej: "Pendiente en comisión(es) de origenPendiente09/04/2025"
      → estado="Pendiente en comisión(es) de origen"
        situacion="Pendiente"  fecha="09/04/2025"

Crea columnas estatus_estado, estatus_situacion, estatus_fecha y un
estatus_canon (Aprobado/Pendiente/Desechado/Retirada) para medir tasa
de aprobación real. NO re-scrapea — re-parsea lo que ya está en BD.

NO Haiku. Idempotente.

Uso: python3 scripts/reparsear_estatus_sil.py [--dry-run]
"""
from __future__ import annotations
import argparse, re, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from db import get_connection

SITUACIONES = ("Pendiente", "Aprobado", "Desechado", "Resuelto",
               "Retirada", "Concluido", "Precluido")


def parse_estatus(e):
    if not e:
        return "", "", ""
    m = re.search(r"(\d{2}/\d{2}/\d{4})\s*$", e)
    fecha = m.group(1) if m else ""
    resto = e[:m.start()] if m else e
    sm = re.search(r"(" + "|".join(SITUACIONES) + r")\s*$", resto)
    sit = sm.group(1) if sm else ""
    estado = (resto[:sm.start()] if sm else resto).strip()
    return estado, sit, fecha


def canon(estado, sit):
    """Estado canónico para tasa de aprobación."""
    t = (estado + " " + sit).lower()
    if "publicado en dof" in t or "aprobado" in t or "turnado al ejecutivo" in t or "resuelto" in t:
        return "Aprobado"
    if "desechado" in t or "precluido" in t or "concluido" in t:
        return "Desechado"
    if "retirada" in t or "retirado" in t:
        return "Retirada"
    if "pendiente" in t or "turnado" in t or "lectura" in t or "presentado" in t:
        return "Pendiente"
    return "Otro" if estado else "SinEstado"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    conn = get_connection()

    # Migración idempotente
    for col in ("estatus_estado", "estatus_situacion", "estatus_fecha", "estatus_canon"):
        try:
            conn.execute(f"ALTER TABLE sil_documentos ADD COLUMN {col} TEXT DEFAULT ''")
        except Exception:
            pass
    conn.commit()

    rows = conn.execute(
        "SELECT id, estatus FROM sil_documentos WHERE estatus IS NOT NULL AND estatus != ''"
    ).fetchall()
    print(f"Filas con estatus a re-parsear: {len(rows)}")

    from collections import Counter
    canon_dist = Counter()
    updates = []
    for doc_id, est in rows:
        estado, sit, fecha = parse_estatus(est)
        cn = canon(estado, sit)
        canon_dist[cn] += 1
        updates.append((estado, sit, fecha, cn, doc_id))

    print("Distribución canónica:")
    for k, v in canon_dist.most_common():
        print(f"  {k:12} {v:>6} ({100*v/len(rows):.0f}%)")
    aprob = canon_dist["Aprobado"]
    resueltos = aprob + canon_dist["Desechado"] + canon_dist["Retirada"]
    if resueltos:
        print(f"\nTasa de aprobación (aprob / resueltos): {100*aprob/resueltos:.1f}%")

    if args.dry_run:
        print("[DRY-RUN] no se escribe")
        return

    for estado, sit, fecha, cn, doc_id in updates:
        conn.execute(
            "UPDATE sil_documentos SET estatus_estado=?, estatus_situacion=?, "
            "estatus_fecha=?, estatus_canon=? WHERE id=?",
            (estado, sit, fecha, cn, doc_id))
    conn.commit()
    print(f"\n✅ {len(updates)} filas re-parseadas (columnas estatus_estado/situacion/fecha/canon)")


if __name__ == "__main__":
    main()
