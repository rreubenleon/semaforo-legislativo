"""Refresco COMPLETO del estatus de TODOS los documentos SIL ya en BD.

PROBLEMA QUE RESUELVE (hallazgo 7 jun 2026):
  El scraper SIL solo pide detalle de IDs NUEVOS. Los ~17K documentos ya
  guardados quedan congelados con el estatus del día que se scrapearon.
  Una iniciativa puede pasar Pendiente→Aprobada→Desechada con el tiempo y
  FIAT se queda en el primer estado. Partido está 100% correcto; el estatus
  se DESFASA. Esto re-visita cada expediente y actualiza estatus + comisión.

  Mismo UPDATE que el ON CONFLICT del scraper (estatus + columnas parseadas
  + comisión). NO toca partido (ya está al 100%, no arriesgar).

COSTO: $0 (solo HTTP scraping al SIL). NO Haiku — blindado.
TIEMPO: ~2-3h (rate limit). Resumible: si se corta, vuelve a correr y
  retoma desde el último id procesado (checkpoint en archivo de estado).

Uso:
  python3 scripts/refresh_estatus_completo.py            # corre/resume
  python3 scripts/refresh_estatus_completo.py --reset    # ignora checkpoint
  python3 scripts/refresh_estatus_completo.py --limit 50 # prueba corta
"""
from __future__ import annotations

# Blindaje #1: ningún proceso hijo puede llamar a Haiku.
import os
os.environ.pop("ANTHROPIC_API_KEY", None)
os.environ.pop("FIAT_CLAUDE_API_KEY", None)

import argparse
import json
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from db import get_connection
from scrapers.sil import _obtener_detalle

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("refresh_estatus")

CHECKPOINT = ROOT / "scripts" / ".refresh_estatus_checkpoint.json"
# seg entre fichas; SIL no bloquea pero conviene ser amable. Configurable
# por env para que en CI (límite de 6h por job) corra más rápido sin
# perder el ritmo seguro local. Default local 0.7; en CI usamos ~0.4.
SLEEP = float(os.environ.get("REFRESH_SLEEP", "0.7"))


def cargar_checkpoint():
    if CHECKPOINT.exists():
        try:
            return json.loads(CHECKPOINT.read_text())
        except Exception:
            pass
    return {"last_id": 0, "procesados": 0, "cambiados": 0, "sin_detalle": 0}


def guardar_checkpoint(state):
    CHECKPOINT.write_text(json.dumps(state))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="ignora checkpoint")
    ap.add_argument("--limit", type=int, default=0, help="máx docs (0=todos)")
    args = ap.parse_args()

    conn = get_connection()
    state = {"last_id": 0, "procesados": 0, "cambiados": 0, "sin_detalle": 0}
    if not args.reset:
        state = cargar_checkpoint()
        if state["last_id"]:
            log.info(f"Reanudando desde id>{state['last_id']} "
                     f"(ya procesados {state['procesados']})")

    q = ("SELECT id, seguimiento_id, asunto_id, estatus_canon "
         "FROM sil_documentos WHERE id > ? "
         "AND seguimiento_id NOT LIKE 'PERM_%' "
         "ORDER BY id")
    rows = conn.execute(q, (state["last_id"],)).fetchall()
    if args.limit:
        rows = rows[:args.limit]

    total = len(rows)
    log.info(f"Documentos a refrescar en esta corrida: {total}")
    if not total:
        log.info("Nada que hacer. ¿Ya terminó? Usa --reset para empezar de cero.")
        return

    t0 = time.time()
    for i, (doc_id, seg, asu, canon_old) in enumerate(rows, 1):
        det = _obtener_detalle(str(seg), str(asu))
        if det:
            estatus_raw = (det.get("estatus", "") or "").strip()
            # GUARD CRÍTICO: el SIL reorganizó fichas viejas y dejó de
            # exponer "Último estatus" (solo "Último Trámite"). Para esas,
            # _obtener_detalle devuelve estatus="" → SinEstado. NUNCA
            # sobrescribir un estatus real previo con vacío: eso borraría
            # datos válidos y haría FIAT peor. Solo actualizamos cuando el
            # SIL nos da un estatus REAL (no vacío). Esto sí captura las
            # progresiones reales (Pendiente→Desechado), que vienen con
            # estatus no vacío.
            if not estatus_raw:
                state["sin_detalle"] += 1
                # Comisión sí se puede refrescar aunque falte estatus.
                com = (det.get("comision", "") or "").strip()
                if com:
                    conn.execute(
                        "UPDATE sil_documentos SET comision=? WHERE id=?",
                        (com, doc_id))
            else:
                canon_new = det.get("estatus_canon", "")
                if canon_new != canon_old:
                    state["cambiados"] += 1
                    log.info(f"  CAMBIO id={doc_id} seg={seg}: "
                             f"{canon_old} → {canon_new}")
                conn.execute(
                    "UPDATE sil_documentos SET estatus=?, estatus_estado=?, "
                    "estatus_situacion=?, estatus_fecha=?, estatus_canon=?, "
                    "comision=? WHERE id=?",
                    (estatus_raw, det.get("estatus_estado", ""),
                     det.get("estatus_situacion", ""), det.get("estatus_fecha", ""),
                     det.get("estatus_canon", ""), det.get("comision", ""), doc_id))
        else:
            state["sin_detalle"] += 1

        state["last_id"] = doc_id
        state["procesados"] += 1

        if i % 50 == 0:
            conn.commit()
            guardar_checkpoint(state)
            ritmo = i / (time.time() - t0)
            falta = (total - i) / ritmo if ritmo else 0
            log.info(f"  {i}/{total} ({100*i/total:.0f}%) | "
                     f"cambios={state['cambiados']} sin_detalle={state['sin_detalle']} | "
                     f"ETA {falta/60:.0f} min")
        time.sleep(SLEEP)

    conn.commit()
    guardar_checkpoint(state)
    log.info("=" * 60)
    log.info(f"TERMINADO. Procesados {state['procesados']}, "
             f"cambios de estatus {state['cambiados']}, "
             f"sin_detalle {state['sin_detalle']}")
    log.info("Siguiente: recalcular tasa aprobación / por_estado, "
             "re-exportar data.json, sync D1.")


if __name__ == "__main__":
    main()
