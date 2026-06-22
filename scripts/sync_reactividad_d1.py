"""
Sincroniza la reactividad (eval/reactividad.json) a D1 → legisladores_stats.
Agrega las columnas si no existen. Corre EN WORKFLOW (necesita CF token).

Columnas: reactividad (0-100), ppa_reactivas, ppa_total. La ficha las lee del
Worker /radar. NO usa Haiku.
"""
from __future__ import annotations
import json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from scripts.radar_pipeline import ejecutar_sql_d1

DATA = ROOT / "eval" / "reactividad.json"


def main() -> int:
    if not DATA.exists():
        print(f"Falta {DATA} (corre reactividad_pipeline.py primero)."); return 1
    data = json.loads(DATA.read_text())

    # Crear columnas si no existen (idempotente).
    for col in ("reactividad INTEGER", "ppa_reactivas INTEGER", "ppa_total INTEGER"):
        try:
            ejecutar_sql_d1(f"ALTER TABLE legisladores_stats ADD COLUMN {col} DEFAULT NULL;")
        except Exception:
            pass  # ya existe

    sqls = []
    for lid, v in data.items():
        if v.get("reactividad") is None:
            continue
        sqls.append(
            "UPDATE legisladores_stats SET "
            f"reactividad = {int(v['reactividad'])}, "
            f"ppa_reactivas = {int(v['reacciono'])}, "
            f"ppa_total = {int(v['ppa'])} "
            f"WHERE legislador_id = {int(lid)};"
        )
    print(f"Updates: {len(sqls)}")
    for i in range(0, len(sqls), 100):
        ejecutar_sql_d1("\n".join(sqls[i:i + 100]))
        print(f"  batch {i // 100 + 1} → D1")
    print("Reactividad en D1.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
