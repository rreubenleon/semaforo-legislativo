"""
Auditoría externa: comparar conteos de FIAT contra fuentes públicas.

Hoy compara las cifras de iniciativas individuales del Senado en LXVI
contra la nota de Leticia Robles (Excélsior, 4-may-2026), que reporta
el universo completo del SIL Senado / SIL Gobernación al cierre del
2do periodo ordinario.

El objetivo es atrapar regresiones en cobertura: si el delta promedio
contra Robles supera ~10%, asumir que el SIL scraper se quedó atrás
otra vez y lanzar alerta.

Uso:
    python scripts/auditoria_externa.py
    python scripts/auditoria_externa.py --umbral 0.10
    python scripts/auditoria_externa.py --json   # salida machine-readable

Exit codes:
    0  Cobertura OK (delta promedio < umbral)
    1  Cobertura insuficiente (alerta)
    2  Error de ejecución
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
sys.path.insert(0, str(ROOT))


# Cifras del artículo de Leticia Robles (Excélsior, 4-may-2026):
# https://www.excelsior.com.mx/nacional/prian-le-gana-a-la-4t-en-el-top-10
# Top 10 senadores LXVI por iniciativas individuales presentadas y
# turnadas a comisiones durante los primeros dos años ordinarios.
TOP_10_ROBLES = [
    # (nombre_completo_segun_sil, iniciativas_individuales, firmadas_o_adheridas)
    ("Pablo Guillermo Angulo Briceño",   137, 85),
    ("Karen Michel González Márquez",    116, 49),
    ("María del Rocío Corona Nakamura",  109, 84),
    ("Olga Patricia Sosa Ruíz",           79,  8),
    ("Enrique Vargas del Villar",         78, 128),
    ("Saúl Monreal Ávila",                66, 13),
    ("María Martina Kantún Can",          55, 29),
    ("Juan Antonio Martín del Campo",     48, 51),
    ("Virgilio Mendoza Amezcua",          22, 104),
    ("Miguel Ángel Riquelme",             24, 76),
]


def _columnas_disponibles(conn: sqlite3.Connection, tabla: str) -> set:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({tabla})").fetchall()}


def contar_iniciativas_senador(conn: sqlite3.Connection, nombre: str) -> dict:
    """
    Devuelve {'individuales': int, 'firmadas_con_otros': int, 'total': int}
    para un senador en LXVI.

    'individuales' = iniciativas donde el senador aparece como ÚNICO firmante
    'firmadas_con_otros' = iniciativas donde aparece junto a otros nombres

    Compatible con cache CI que puede no tener tipo_grupo o tipo_presentador
    (caches viejos pre-clasificación).
    """
    cols = _columnas_disponibles(conn, "sil_documentos")
    # Filtro de tipo: tipo_grupo si existe, sino tipo (con LIKE para variantes)
    if "tipo_grupo" in cols:
        filtro_tipo = "tipo_grupo = 'Iniciativa'"
    else:
        filtro_tipo = "tipo LIKE 'Iniciativa%'"
    # Filtro de presentador: tipo_presentador si existe
    filtro_pres = "AND tipo_presentador = 'legislador'" if "tipo_presentador" in cols else ""

    base = f"""
        SELECT presentador
        FROM sil_documentos
        WHERE legislatura = 'LXVI'
          AND {filtro_tipo}
          AND camara = 'Cámara de Senadores'
          {filtro_pres}
          AND presentador LIKE ?
    """
    rows = conn.execute(base, (f"%{nombre}%",)).fetchall()
    individuales = 0
    firmadas = 0
    for (pres,) in rows:
        # Contar cuántos firmantes hay en el campo
        n_firmantes = pres.count("Sen.")
        if n_firmantes <= 1:
            individuales += 1
        else:
            firmadas += 1
    return {
        "individuales": individuales,
        "firmadas_con_otros": firmadas,
        "total": individuales + firmadas,
    }


def auditar(umbral_alerta: float = 0.10) -> dict:
    if not DB.exists():
        return {"error": f"BD no encontrada: {DB}"}

    conn = sqlite3.connect(str(DB))
    filas = []
    deltas_pct = []

    for nombre, robles_ind, robles_firmadas in TOP_10_ROBLES:
        fiat = contar_iniciativas_senador(conn, nombre)
        # Comparar SOLO individuales (fácil de aislar) y total
        delta_ind = (fiat["individuales"] - robles_ind) / robles_ind if robles_ind else 0
        deltas_pct.append(abs(delta_ind))
        filas.append({
            "senador": nombre,
            "robles_individuales": robles_ind,
            "fiat_individuales": fiat["individuales"],
            "robles_firmadas": robles_firmadas,
            "fiat_firmadas": fiat["firmadas_con_otros"],
            "delta_pct_ind": round(delta_ind * 100, 1),
            "alerta": abs(delta_ind) > umbral_alerta,
        })

    delta_promedio = sum(deltas_pct) / len(deltas_pct)
    veredicto = "OK" if delta_promedio <= umbral_alerta else "ALERTA"

    return {
        "fuente_externa": "Leticia Robles, Excélsior, 4-may-2026 (top 10 senadores LXVI)",
        "umbral_alerta_pct": round(umbral_alerta * 100, 1),
        "delta_promedio_pct": round(delta_promedio * 100, 1),
        "veredicto": veredicto,
        "filas": filas,
    }


def imprimir_tabla(resultado: dict):
    print(f"\n{'='*88}")
    print(f"AUDITORÍA EXTERNA — {resultado['fuente_externa']}")
    print(f"{'='*88}")
    print(f"{'Senador':<38} {'Robles ind':>10} {'FIAT ind':>10} {'Δ%':>8}  Alerta")
    print(f"{'-'*88}")
    for f in resultado["filas"]:
        marker = " ⚠" if f["alerta"] else ""
        print(f"{f['senador']:<38} {f['robles_individuales']:>10} "
              f"{f['fiat_individuales']:>10} {f['delta_pct_ind']:>7}%{marker}")
    print(f"{'-'*88}")
    print(f"Δ promedio: {resultado['delta_promedio_pct']}%   Umbral: {resultado['umbral_alerta_pct']}%   "
          f"Veredicto: {resultado['veredicto']}")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--umbral", type=float, default=0.10,
                    help="Delta% promedio máximo aceptable (default: 0.10 = 10%%)")
    ap.add_argument("--json", action="store_true", help="Salida JSON machine-readable")
    args = ap.parse_args()

    res = auditar(umbral_alerta=args.umbral)

    if "error" in res:
        print(f"ERROR: {res['error']}", file=sys.stderr)
        sys.exit(2)

    if args.json:
        print(json.dumps(res, ensure_ascii=False, indent=2))
    else:
        imprimir_tabla(res)

    sys.exit(0 if res["veredicto"] == "OK" else 1)


if __name__ == "__main__":
    main()
