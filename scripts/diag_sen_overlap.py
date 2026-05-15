"""
Diagnóstico: cuántos SEN_* sin estatus tienen su doc_id en estatus_senado_panel.
Si overlap es alto → estos docs SÍ están en comisiones pero match falló.
Si overlap es bajo → los docs no llegaron a comisión (sin estatus es correcto).
"""
import sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection

conn = get_connection()
conn.row_factory = sqlite3.Row

panel_ids = set(r[0] for r in conn.execute("SELECT DISTINCT gaceta_doc_id FROM estatus_senado_panel"))
print(f"gaceta_doc_id distintos en panel: {len(panel_ids)}")

sin_est = conn.execute("""
    SELECT seguimiento_id FROM sil_documentos
    WHERE seguimiento_id LIKE 'SEN_%' AND (estatus IS NULL OR estatus='')
""").fetchall()
print(f"SEN_* sin estatus: {len(sin_est)}")

# Extraer N de SEN_N y ver si está en panel_ids
overlap = 0
no_overlap_ids = []
for r in sin_est:
    seg = r[0]
    try:
        n = int(seg.replace("SEN_", ""))
        if n in panel_ids:
            overlap += 1
        else:
            no_overlap_ids.append(n)
    except ValueError:
        pass

print(f"Overlap (existen en panel): {overlap}")
print(f"NO overlap (no llegaron a comisión): {len(no_overlap_ids)}")
print(f"Sample sin overlap: {no_overlap_ids[:5]}")

# Para los que SÍ tienen overlap, ¿qué dice el panel?
if overlap > 0:
    sample_overlap_ids = []
    for r in sin_est[:100]:
        try:
            n = int(r[0].replace("SEN_", ""))
            if n in panel_ids:
                sample_overlap_ids.append(n)
        except:
            pass
    if sample_overlap_ids:
        sample = sample_overlap_ids[:3]
        for sid in sample:
            estatus = conn.execute(
                "SELECT estatus FROM estatus_senado_panel WHERE gaceta_doc_id = ? LIMIT 1",
                (sid,)
            ).fetchone()
            print(f"  SEN_{sid}: panel estatus = {estatus[0] if estatus else 'N/A'}")
