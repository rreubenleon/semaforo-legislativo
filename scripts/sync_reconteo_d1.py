"""
Sincroniza los conteos VERIFICADOS del SIL (eval/reconteo_sil.json) a D1,
sobrescribiendo los l3p_* inflados de legisladores_stats.

Corre EN WORKFLOW (necesita CLOUDFLARE_API_TOKEN; no hay token local).
Es el fix inmediato para que la página deje de inflar. NO usa Haiku.

Conteo = SIL como promovente (individual). Por ahora pone _col (grupo) en 0
hasta que se agregue el reconteo de grupo (fast-follow). Así la página muestra
el número individual-correcto y verificable, no la suma contaminada.

Uso (en workflow):
    python scripts/sync_reconteo_d1.py
    python scripts/sync_reconteo_d1.py --dry-run
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from scripts.radar_pipeline import ejecutar_sql_d1

DATA = ROOT / "eval" / "reconteo_sil.json"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not DATA.exists():
        print(f"No existe {DATA}. Corre recontar_instrumentos_sil.py primero.")
        return 1

    data = json.loads(DATA.read_text())
    sqls, saltados = [], 0
    for lid, v in data.items():
        ini, prop = v.get("ini"), v.get("prop")
        if ini is None or prop is None:
            saltados += 1
            continue
        # Conteo SIL verificado → individual. Grupo (_col) a 0 por ahora.
        sqls.append(
            "UPDATE legisladores_stats SET "
            f"promedio_l3p_iniciativas = {float(ini):.1f}, "
            f"promedio_l3p_proposiciones = {float(prop):.1f}, "
            "promedio_l3p_iniciativas_col = 0.0, "
            "promedio_l3p_proposiciones_col = 0.0 "
            f"WHERE legislador_id = {int(lid)};"
        )

    print(f"Updates a aplicar: {len(sqls)} | sin dato SIL (saltados): {saltados}")
    if args.dry_run:
        print("\n".join(sqls[:5]))
        return 0

    for i in range(0, len(sqls), 100):
        ejecutar_sql_d1("\n".join(sqls[i:i + 100]))
        print(f"  batch {i // 100 + 1} ({len(sqls[i:i+100])}) → D1")
    print("Listo. Conteos del SIL en D1.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
