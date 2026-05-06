"""
Imprime tabla resumen del scrape oficial Senado LXVI.
Compara contra los 10 senadores que Leticia Robles publicó en Excélsior.
"""
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
JSON_PATH = ROOT / "dashboard" / "senadores_lxvi_oficial.json"

ROBLES_TOP10 = [
    ("Pablo Guillermo Angulo Briceño", 137, 85),
    ("Karen Michel González Márquez", 116, 49),
    ("María del Rocío Corona Nakamura", 109, 84),
    ("Olga Patricia Sosa Ruíz", 79, 8),
    ("Enrique Vargas del Villar", 78, 128),
    ("Saúl Monreal Ávila", 66, 13),
    ("María Martina Kantún Can", 55, 29),
    ("Juan Antonio Martín del Campo", 48, 51),
    ("Virgilio Mendoza Amezcua", 22, 104),
    ("Miguel Ángel Riquelme", 24, 76),
]


def main():
    if not JSON_PATH.exists():
        print(f"ERROR: no existe {JSON_PATH}")
        sys.exit(1)
    d = json.loads(JSON_PATH.read_text())
    print(f"Senadores procesados: {len(d['senadores'])}")
    print(f"Instrumentos totales: {len(d['instrumentos'])}")
    print(f"Por tipo: {dict(Counter(i['tipo'] for i in d['instrumentos']))}")
    print()

    stats = defaultdict(lambda: {"ini": 0, "prop": 0, "ind_ini": 0})
    for i in d["instrumentos"]:
        k = (i["senador_nombre"], i["senador_partido"])
        if i["tipo"] == "iniciativa":
            stats[k]["ini"] += 1
            if i["es_individual"]:
                stats[k]["ind_ini"] += 1
        else:
            stats[k]["prop"] += 1

    top = sorted(stats.items(), key=lambda x: x[1]["ini"], reverse=True)[:15]
    print("Top 15 LXVI por iniciativas totales:")
    print(f"{'Senador':<42s} {'Partido':<8s} {'Ini':>4s} {'IndCalc':>8s} {'Prop':>5s}")
    for (nom, par), v in top:
        print(f"{nom:<42s} {par:<8s} {v['ini']:>4d} {v['ind_ini']:>8d} {v['prop']:>5d}")

    print()
    print("Cotejo contra Robles (Excélsior, 4-may-2026):")
    print(f"{'Senador':<42s} {'Robles tot':>11s} {'FIAT tot':>9s} {'Δ%':>7s}")
    # Stopwords típicos de nombres mexicanos que no son apellidos distintivos
    NO_DISTINTIVOS = {"miguel", "ángel", "angel", "maría", "maria", "del",
                      "de", "la", "los", "las", "y", "san", "santa", "san"}
    for nombre, r_ind, r_firm in ROBLES_TOP10:
        # Match basado en APELLIDOS distintivos (no nombres de pila)
        # Tomamos los tokens del nombre Robles que NO son nombres comunes
        toks_r = [t for t in nombre.lower().split() if t not in NO_DISTINTIVOS and len(t) > 2]
        match = None
        mejor_score = 0
        for (nom, par), v in stats.items():
            toks_f = [t for t in nom.lower().split() if t not in NO_DISTINTIVOS and len(t) > 2]
            comunes = set(toks_r) & set(toks_f)
            # Requerir al menos 2 apellidos distintivos en común,
            # y que el ÚLTIMO token de Robles esté en el FIAT
            # Requisito: al menos 1 apellido distintivo en común. Si Robles
            # tiene 2+ apellidos, exigir 2 comunes; si solo 1, basta con eso.
            min_req = 2 if len(toks_r) >= 2 else 1
            if len(comunes) >= min_req:
                if len(comunes) > mejor_score:
                    mejor_score = len(comunes)
                    match = (nom, par, v)
        if match:
            nom, par, v = match
            r_total = r_ind + r_firm
            f_total = v["ini"]
            delta = (f_total - r_total) / r_total * 100 if r_total else 0
            marker = "✓" if abs(delta) <= 5 else "⚠"
            print(f"{nombre:<42s} {r_total:>11d} {f_total:>9d} {delta:>6.1f}% {marker}")
        else:
            print(f"{nombre:<42s} {r_ind+r_firm:>11d} {'NO MATCH':>9s}")


if __name__ == "__main__":
    main()
