"""Entrena el predictor de probabilidad por categoría (period-aware).

Outcome: surge_14d = (iniciativas+proposiciones con PA en la cat en
los siguientes 14d) > mediana histórica de esa cat en ese periodo.
Binary 0/1.

Modelo ORDINARIO (regresión logística per-cat):
  P(surge | señales) = sigmoid(b0 + b1·media + b2·trends + b3·congreso
                             + b4·mañanera + b5·urgencia + b6·dominancia
                             + b7·legisladores)
  Entrenado con feb-abr 2026 (régimen CI válido).

Modelo RECESO (solo baseline):
  P(surge) = mediana_empírica de surge en receso (histórico SIL).
  No tenemos señales válidas en receso (trends/mañanera no existían
  pre-feb-2026; media solo La Jornada), así que NO entrenamos
  coeficientes. Solo baseline.

Salida: data/predictor_probabilidad.json — coeficientes per-cat per-periodo.
Re-correr cuando crezca la historia (recomendado mensual).
"""
from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from scipy.optimize import minimize

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from db import get_connection

VENTANA = 14
SCORES_REGIMEN_DESDE = "2026-02-13"
TRAIN_HASTA = "2026-03-30"
TEST_DESDE = "2026-03-31"
TEST_HASTA = "2026-04-30"

OUTPUT = ROOT / "data" / "predictor_probabilidad.json"


def es_receso(d_str: str) -> bool:
    d = datetime.strptime(d_str[:10], "%Y-%m-%d")
    m, day = d.month, d.day
    if m in (5, 6, 7, 8):
        return True
    if m == 1:
        return True
    if m == 12 and day >= 16:
        return True
    return False


def parse_cats(s):
    return [p.split(":")[0].strip() for p in (s or "").split(",") if p.strip()]


def sigmoid(z):
    return 1 / (1 + np.exp(-np.clip(z, -50, 50)))


def neg_log_likelihood(beta, X, y, lam=0.5):
    z = X @ beta
    p = sigmoid(z)
    eps = 1e-12
    ll = np.sum(y * np.log(p + eps) + (1 - y) * np.log(1 - p + eps))
    reg = lam * np.sum(beta[1:] ** 2)  # ridge, no penaliza intercept
    return -ll + reg


def main():
    conn = get_connection()

    # Migración idempotente: la columna score_legisladores nació en
    # commit ee228e2 pero la BD local puede no tenerla si paso_5_scoring
    # no ha corrido aquí.
    try:
        conn.execute("ALTER TABLE scores ADD COLUMN score_legisladores REAL DEFAULT 0")
        conn.commit()
    except Exception:
        pass

    # ── 1. Outcomes: instrumentos sustantivos por (cat, fecha) ──
    instr = defaultdict(int)
    for r in conn.execute(
        "SELECT categoria, fecha_presentacion FROM sil_documentos "
        "WHERE tipo_grupo IN ('Iniciativa','Proposición con Punto de Acuerdo','Proposición con PA') "
        "AND fecha_presentacion >= '2024-09-01' AND fecha_presentacion IS NOT NULL"
    ).fetchall():
        f = r[1][:10]
        for c in parse_cats(r[0]):
            instr[(c, f)] += 1

    def count_14d(c, f):
        d0 = datetime.strptime(f, "%Y-%m-%d")
        return sum(instr.get((c, (d0 + timedelta(days=k)).strftime("%Y-%m-%d")), 0)
                   for k in range(1, VENTANA + 1))

    # ── 2. Mediana per-cat per-periodo (histórico SIL completo) ──
    mediana_por_cat_periodo = {}  # (cat, "ordinario"|"receso") -> mediana
    fechas_todas = []
    d0 = datetime.strptime("2024-09-01", "%Y-%m-%d")
    d_fin = datetime.strptime("2026-05-01", "%Y-%m-%d")
    while d0 <= d_fin:
        fechas_todas.append(d0.strftime("%Y-%m-%d"))
        d0 += timedelta(days=1)

    cats = sorted({c for c, _ in instr.keys()})
    for cat in cats:
        for periodo in ("ordinario", "receso"):
            counts = [count_14d(cat, f) for f in fechas_todas
                      if (es_receso(f) if periodo == "receso" else not es_receso(f))]
            mediana_por_cat_periodo[(cat, periodo)] = (
                statistics.median(counts) if counts else 0
            )

    # ── 3. Baseline receso por cat (no necesita modelo) ──
    baseline_receso = {}
    for cat in cats:
        days = [f for f in fechas_todas if es_receso(f)]
        med = mediana_por_cat_periodo.get((cat, "receso"), 0)
        surges = sum(1 for f in days if count_14d(cat, f) > med)
        baseline_receso[cat] = round(surges / len(days), 4) if days else 0

    # ── 4. Modelo ORDINARIO per cat (feb-abr 2026, signals completas) ──
    rows = conn.execute(f"""
        SELECT fecha, categoria,
               score_media, score_trends, score_congreso,
               score_mananera, score_urgencia, score_dominancia,
               score_legisladores
        FROM scores
        WHERE fecha >= '{SCORES_REGIMEN_DESDE}' AND fecha <= '{TEST_HASTA}'
        AND score_total IS NOT NULL
    """).fetchall()

    feature_names = ["media", "trends", "congreso", "mananera",
                     "urgencia", "dominancia", "legisladores"]

    modelos = {}
    metricas_train = {}
    metricas_test = {}
    for cat in cats:
        # Solo rows ordinario (feb-abr todas son ordinario, pero por correctness)
        rows_cat = [(r[0], r[2:]) for r in rows if r[1] == cat and not es_receso(r[0])]
        if len(rows_cat) < 20:
            continue
        # Train/test split temporal
        train = [(f, feats) for f, feats in rows_cat if f <= TRAIN_HASTA]
        test = [(f, feats) for f, feats in rows_cat if f >= TEST_DESDE]
        if len(train) < 15 or len(test) < 5:
            continue
        med_ord = mediana_por_cat_periodo.get((cat, "ordinario"), 0)
        # Construir X, y
        def build(rows_list):
            X = []
            y = []
            for f, feats in rows_list:
                # Normaliza signals a 0-1
                feats_norm = [(float(v) if v is not None else 0) / 100.0 for v in feats]
                X.append([1.0] + feats_norm)
                cnt = count_14d(cat, f)
                y.append(1 if cnt > med_ord else 0)
            return np.array(X), np.array(y)

        X_tr, y_tr = build(train)
        X_te, y_te = build(test)

        if y_tr.mean() in (0, 1):
            continue  # No hay variación

        beta0 = np.zeros(X_tr.shape[1])
        res = minimize(neg_log_likelihood, beta0, args=(X_tr, y_tr),
                       method="L-BFGS-B")
        beta = res.x

        # Métricas
        p_tr = sigmoid(X_tr @ beta)
        p_te = sigmoid(X_te @ beta)
        acc_tr = float(np.mean((p_tr > 0.5) == y_tr))
        acc_te = float(np.mean((p_te > 0.5) == y_te)) if len(y_te) else None
        # Baseline ordinario (en X.mean features)
        b0_only = sigmoid(beta[0])  # con todas features=0 → "tema sin señales"
        baseline_mean = sigmoid(beta @ X_tr.mean(axis=0))  # tema en día promedio

        modelos[cat] = {
            "ordinario": {
                "coefs": {"intercept": float(beta[0]),
                          **{n: float(beta[i + 1]) for i, n in enumerate(feature_names)}},
                "baseline_sin_senales": float(b0_only),
                "baseline_dia_promedio": float(baseline_mean),
                "mediana_outcome_14d": float(med_ord),
                "n_train": len(train),
                "n_test": len(test),
                "accuracy_train": acc_tr,
                "accuracy_test_walk_forward": acc_te,
            },
            "receso": {
                "baseline_solo": baseline_receso[cat],
                "mediana_outcome_14d": float(mediana_por_cat_periodo.get((cat, "receso"), 0)),
                "nota": "Sin coeficientes — no hay señales válidas en receso pre-2026.",
            },
        }
        metricas_train[cat] = acc_tr
        metricas_test[cat] = acc_te

    # ── 5. Reporte ──
    print("=" * 80)
    print("PREDICTOR DE PROBABILIDAD — entrenado, walk-forward validado")
    print("=" * 80)
    print(f"{'Categoría':24} | n_tr | n_te | Train | Test (walk-fwd) | Receso baseline")
    print("-" * 80)
    for cat in sorted(modelos.keys()):
        m = modelos[cat]["ordinario"]
        r = modelos[cat]["receso"]
        acc_te_str = f"{100*m['accuracy_test_walk_forward']:>5.0f}%" if m['accuracy_test_walk_forward'] else "  N/A"
        print(f"{cat:24} | {m['n_train']:>4} | {m['n_test']:>4} | "
              f"{100*m['accuracy_train']:>4.0f}% | {acc_te_str:>14} | "
              f"{100*r['baseline_solo']:>4.0f}%")
    print("-" * 80)
    print(f"\nPromedio accuracy:")
    print(f"  Train (in-sample, optimista): "
          f"{100*statistics.mean(metricas_train.values()):.1f}%")
    valid_test = [v for v in metricas_test.values() if v is not None]
    if valid_test:
        print(f"  Test (walk-forward, honesto): "
              f"{100*statistics.mean(valid_test):.1f}%")
    print(f"\n  Baseline receso promedio: {100*statistics.mean(baseline_receso.values()):.1f}%")

    # ── 6. Guardar artefacto ──
    OUTPUT.parent.mkdir(exist_ok=True)
    artefacto = {
        "fecha_entrenamiento": datetime.now().isoformat(),
        "ventana_outcome_dias": VENTANA,
        "outcome": "iniciativas+proposiciones con PA > mediana histórica per cat per periodo",
        "regimen_signals": f"{SCORES_REGIMEN_DESDE} → {TEST_HASTA}",
        "train_period": f"≤ {TRAIN_HASTA}",
        "test_period": f"{TEST_DESDE} → {TEST_HASTA}",
        "features": feature_names,
        "modelos": modelos,
    }
    OUTPUT.write_text(json.dumps(artefacto, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n✅ Coeficientes guardados: {OUTPUT}")


if __name__ == "__main__":
    main()
