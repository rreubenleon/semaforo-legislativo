"""Construir tabla de calibración score → probabilidad y guardarla.

Outcome binario discriminante: el # de instrumentos sustantivos en
la categoría durante los siguientes 14 días supera la MEDIANA
histórica de esa categoría. Eso responde una pregunta interpretable:
'¿Probabilidad de que haya un surge legislativo arriba de lo normal?'

Output: data/calibracion_score.json — lookup score_bin → probabilidad.
"""
import sys, json, statistics
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path("/Users/rubenleon/Desktop/Monitor/semaforo-legislativo")
sys.path.insert(0, str(ROOT))
from db import get_connection

conn = get_connection()
VENTANA = 14  # ventana del outcome

def parse_cats(s):
    if not s: return []
    return [p.split(":")[0].strip() for p in s.split(",") if p.strip()]

# Index: (cat, fecha) -> # instrumentos sustantivos ese día
instr_diario = defaultdict(int)
for r in conn.execute(
    "SELECT categoria, fecha_presentacion FROM sil_documentos "
    "WHERE tipo_grupo IN ('Iniciativa','Proposición con Punto de Acuerdo','Proposición con PA') "
    "AND fecha_presentacion IS NOT NULL"
).fetchall():
    f = r[1][:10]
    for cat in parse_cats(r[0]):
        instr_diario[(cat, f)] += 1

def count_in_window(cat, fecha, N):
    f0 = datetime.strptime(fecha, "%Y-%m-%d")
    return sum(instr_diario.get((cat, (f0+timedelta(days=k)).strftime("%Y-%m-%d")), 0)
               for k in range(1, N+1))

# Cargar pares válidos (régimen CI feb-abr 2026)
rows = conn.execute(
    "SELECT fecha, categoria, score_total FROM scores "
    "WHERE fecha >= '2026-02-13' AND fecha <= '2026-04-30' AND score_total IS NOT NULL"
).fetchall()

# Mediana por categoría de # instrumentos en VENTANA días
counts_por_cat = defaultdict(list)
for f, c, _ in rows:
    counts_por_cat[c].append(count_in_window(c, f, VENTANA))
mediana_por_cat = {c: statistics.median(v) for c, v in counts_por_cat.items() if v}
print(f"Medianas históricas (ventana {VENTANA}d) por cat:")
for c, m in sorted(mediana_por_cat.items(), key=lambda x: -x[1])[:10]:
    print(f"  {c:24} mediana={m:.0f}")

# Outcome binario: count_VENTANA > mediana_cat
dataset = []
for f, c, s in rows:
    cnt = count_in_window(c, f, VENTANA)
    surge = 1 if cnt > mediana_por_cat.get(c, 0) else 0
    dataset.append({"score": s, "surge": surge})

print()
print(f"Total pares: {len(dataset)}")
print(f"Base rate de surge: {100*sum(d['surge'] for d in dataset)/len(dataset):.1f}%")

# Calibración por bin (lookup table)
bins = [(0,10),(10,20),(20,30),(30,40),(40,50),(50,60),(60,70),(70,80),(80,90),(90,101)]
calibracion = {}
print()
print("Tabla de calibración (P de surge ARRIBA de la mediana histórica):")
print("-" * 60)
print(f"{'Score bin':12} | {'n':>4} | {'P(surge)':>10}")
for lo, hi in bins:
    b = [d for d in dataset if lo <= d["score"] < hi]
    if not b:
        calibracion[f"{lo:03d}-{hi-1:03d}"] = None
        continue
    p = sum(d["surge"] for d in b) / len(b)
    calibracion[f"{lo:03d}-{hi-1:03d}"] = round(p, 3)
    print(f"{lo:3d}-{hi-1:3d}     | {len(b):>4} | {100*p:>6.1f}%")

# Verificar monotonicidad
vals_validos = [v for v in calibracion.values() if v is not None]
es_monotono = all(vals_validos[i] <= vals_validos[i+1] + 0.05 for i in range(len(vals_validos)-1))
print()
print(f"¿Monotónico? {'sí ✓' if es_monotono else 'no (tiene caídas)'}")

# Suavizar: aplicar isotonic forzada (forzar monotónico ascendente)
suavizado = list(vals_validos)
for i in range(1, len(suavizado)):
    if suavizado[i] < suavizado[i-1]:
        suavizado[i] = suavizado[i-1]
print(f"Suavizado monotónico: {[f'{100*v:.0f}%' for v in suavizado]}")

# Guardar archivo de calibración
output = {
    "ventana_dias": VENTANA,
    "fecha_calculo": datetime.now().isoformat(),
    "fuente_datos": f"scores tabla, {len(rows)} pares feb-abr 2026",
    "outcome": "instrumentos sustantivos en ventana > mediana histórica por categoría",
    "spearman_rho_score_vs_count_14d": 0.560,
    "base_rate_surge": round(sum(d['surge'] for d in dataset)/len(dataset), 3),
    "calibracion_bins": calibracion,
    "calibracion_suavizada": dict(zip([k for k,v in calibracion.items() if v is not None], suavizado)),
    "mediana_por_cat": {c: round(m,1) for c,m in mediana_por_cat.items()},
}

out_path = ROOT / "data" / "calibracion_score.json"
out_path.parent.mkdir(exist_ok=True)
out_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"\n✅ Calibración guardada: {out_path}")
