"""Genera pesos de scoring POR CATEGORÍA (cada tema = mercado propio).

Lee data/responsividad_subcat.json (lift media→instrumento por sub-cat,
validado en 2 ventanas) y deriva, por categoría padre, un peso de media
ajustado a qué tan reactivo es el tema. Renormaliza los otros 6
componentes para que el total siga sumando 1.0 (método A).

Salida: data/pesos_por_categoria.json — auditable, regenerable.
Re-correr cuando se actualice responsividad_subcat.json.
"""
import sys, json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from config import CATEGORIAS, SCORING

PESOS_BASE = SCORING["pesos"]            # global de hoy
MEDIA_BASE = PESOS_BASE["media"]         # 0.20
MEDIA_MIN, MEDIA_MAX = 0.10, 0.35        # cap de seguridad

resp_path = ROOT / "data" / "responsividad_subcat.json"
responsividad = json.loads(resp_path.read_text(encoding="utf-8"))["subcats"]


def factor_categoria(cat):
    """Promedio ponderado por #keywords de los multiplicadores de sub-cats.
    Sub-cats no-confiables aportan 1.0 (neutro)."""
    subs = CATEGORIAS[cat].get("subcategorias", {})
    if not isinstance(subs, dict) or not subs:
        return 1.0
    num = den = 0
    for sk, sc in subs.items():
        kws = sc.get("keywords", []) if isinstance(sc, dict) else []
        if not kws:
            continue
        mult = responsividad.get(f"{cat}/{sk}", {}).get("multiplicador_media", 1.0)
        peso = len(kws)
        num += mult * peso
        den += peso
    return (num / den) if den else 1.0


pesos_por_cat = {}
for cat in CATEGORIAS:
    factor = factor_categoria(cat)
    nueva_media = min(MEDIA_MAX, max(MEDIA_MIN, round(MEDIA_BASE * factor, 4)))
    # Renormalizar los otros 6 para que el total = 1.0 (método A)
    otros = {k: v for k, v in PESOS_BASE.items() if k != "media"}
    suma_otros = sum(otros.values())            # 0.80
    escala = (1.0 - nueva_media) / suma_otros
    pesos = {"media": round(nueva_media, 4)}
    for k, v in otros.items():
        pesos[k] = round(v * escala, 4)
    # Ajuste fino para que sume exactamente 1.0 (acumular redondeo en congreso)
    diff = round(1.0 - sum(pesos.values()), 4)
    pesos["congreso"] = round(pesos["congreso"] + diff, 4)
    pesos_por_cat[cat] = {"factor_media": round(factor, 3), "pesos": pesos}

# Guardar
out = ROOT / "data" / "pesos_por_categoria.json"
out.write_text(json.dumps({
    "fecha": "2026-05-29",
    "metodo": "media ajustada por responsividad (lift 2-ventanas), renormalización A",
    "pesos_base_global": PESOS_BASE,
    "categorias": pesos_por_cat,
}, indent=2, ensure_ascii=False), encoding="utf-8")

# Reporte
print(f"{'categoría':24} | factor | media | trends cong  mañ   urg   dom   leg  | suma")
print("-" * 92)
for cat, d in pesos_por_cat.items():
    p = d["pesos"]
    s = sum(p.values())
    print(f"{cat:24} | {d['factor_media']:>6.2f} | {p['media']:.3f} | "
          f"{p['trends']:.3f} {p['congreso']:.3f} {p['mananera']:.3f} "
          f"{p['urgencia']:.3f} {p['dominancia']:.3f} {p['legisladores']:.3f} | {s:.3f}")
print("-" * 92)
print(f"\n✅ Guardado: {out}")
print(f"Categorías con media ajustada: "
      f"{sum(1 for d in pesos_por_cat.values() if abs(d['pesos']['media']-MEDIA_BASE)>0.005)}/{len(pesos_por_cat)}")
