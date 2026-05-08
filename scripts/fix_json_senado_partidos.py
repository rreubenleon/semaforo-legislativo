"""
Repara dashboard/senadores_lxvi_oficial.json:

  1. Partidos correctos: cruza nombre del senador con tabla legisladores
     (que tiene partidos del SIL Gob: MORENA, PAN, PRI, PVEM, PT, MC).
     El JSON original solo tenía MORENA o PAN — bug del scraper.

  2. URL canónica: el JSON apunta a /64/ (legislatura legacy). LXVI usa
     /66/. Lo reemplazamos: '/64/gaceta_del_senado/' → '/66/gaceta_del_senado/'

Idempotente.

Uso:
    python scripts/fix_json_senado_partidos.py
    python scripts/fix_json_senado_partidos.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
JSON_PATH = ROOT / "dashboard" / "senadores_lxvi_oficial.json"
sys.path.insert(0, str(ROOT))

from utils.matcher import build_bd_index, encontrar_legislador_id, normalizar_nombre

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def norm_partido(p):
    p = (p or "").strip()
    if p.lower() == "morena":
        return "MORENA"
    return p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not JSON_PATH.exists():
        logger.error(f"No existe {JSON_PATH}")
        sys.exit(1)

    data = json.loads(JSON_PATH.read_text())
    senadores = data.get("senadores", [])
    instrumentos = data.get("instrumentos", [])
    logger.info(f"Cargado: {len(senadores)} senadores, {len(instrumentos)} instrumentos")

    # Cargar matcher con BD legisladores
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    bd_idx = build_bd_index(conn)
    leg_partido = {}
    for r in conn.execute(
        "SELECT id, partido FROM legisladores WHERE camara IN ('Cámara de Senadores','Senado') AND partido IS NOT NULL"
    ).fetchall():
        leg_partido[r["id"]] = norm_partido(r["partido"])

    # 1. Fix partido por senador (lookup table)
    senador_id_a_partido_correcto = {}
    cambios_partido = []
    for s in senadores:
        nombre_n = normalizar_nombre(s["nombre"])
        leg_id = encontrar_legislador_id(nombre_n, "Senado", bd_idx)
        if not leg_id:
            continue
        partido_correcto = leg_partido.get(leg_id)
        if not partido_correcto:
            continue
        partido_actual = norm_partido(s.get("partido", ""))
        senador_id_a_partido_correcto[s["id"]] = partido_correcto
        if partido_actual != partido_correcto:
            cambios_partido.append((s["nombre"], partido_actual, partido_correcto))
            s["partido"] = partido_correcto

    logger.info(f"Senadores con partido corregido: {len(cambios_partido)}")
    print()
    print("Cambios partido por senador (top 20):")
    by_transition = {}
    for nombre, old, new in cambios_partido:
        key = f"{old or '(vacío)'} → {new}"
        by_transition.setdefault(key, []).append(nombre)
    for k, lst in sorted(by_transition.items(), key=lambda x: -len(x[1])):
        print(f"  {k:25} {len(lst)} senadores")
        for n in lst[:3]:
            print(f"      · {n}")

    # 2. Aplicar partido correcto a instrumentos
    cambios_inst = 0
    cambios_url = 0
    for inst in instrumentos:
        sid = inst.get("senador_id")
        partido_correcto = senador_id_a_partido_correcto.get(sid)
        if partido_correcto and inst.get("senador_partido") != partido_correcto:
            inst["senador_partido"] = partido_correcto
            cambios_inst += 1
        # Fix URL: /64/ → /66/
        url = inst.get("enlace_gaceta", "")
        if url and "/64/gaceta_del_senado/" in url:
            inst["enlace_gaceta"] = url.replace("/64/gaceta_del_senado/", "/66/gaceta_del_senado/")
            # Forzar https
            if inst["enlace_gaceta"].startswith("http://"):
                inst["enlace_gaceta"] = inst["enlace_gaceta"].replace("http://", "https://", 1)
            cambios_url += 1

    logger.info(f"Instrumentos con partido actualizado: {cambios_inst}")
    logger.info(f"Instrumentos con URL actualizada (/64/ → /66/): {cambios_url}")

    # Distribución final
    print()
    print("Distribución final (senadores):")
    from collections import Counter
    c = Counter(s.get("partido", "") for s in senadores)
    for p, n in c.most_common():
        print(f"  {p:10} {n:>4}")

    if not args.dry_run:
        JSON_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2))
        print(f"\n✅ Escrito: {JSON_PATH}")
    else:
        print("\n*** DRY RUN — no se escribió el JSON ***")


if __name__ == "__main__":
    main()
