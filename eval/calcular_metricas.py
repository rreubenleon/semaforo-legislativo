"""
Calcula precision/recall/F1 del clasificador FIAT contra el eval set etiquetado.

Métricas reportadas:
  1. Binario (relevancia legislativa):
       - Positivo del modelo = clasificador devolvió >= 1 categoría
       - Positivo del oráculo = gold_relevante == 1
  2. Multi-label por categoría:
       - Para cada una de las 18 categorías, P/R/F1
       - Macro-promedio (no ponderado) y micro-promedio
  3. Top-1 accuracy (cuando es relevante):
       - ¿La categoría top del modelo está en las gold?
  4. Errores destacados:
       - Falsos positivos (modelo dice relevante, gold dice no)
       - Falsos negativos (modelo dice no relevante, gold dice sí)
       - Categorías mal asignadas (set diff entre modelo y gold)

Uso:
    python eval/calcular_metricas.py [--csv salida.csv]
"""

import argparse
import csv
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import CATEGORIAS
from db import get_connection

logger = logging.getLogger(__name__)

CATEGORIAS_LISTA = list(CATEGORIAS.keys())


def parsear_pred(pred_str):
    """'seguridad_justicia:0.53,electoral_politico:0.41' -> set(['seguridad_justicia', 'electoral_politico'])"""
    if not pred_str:
        return set()
    cats = set()
    for item in pred_str.split(","):
        if ":" in item:
            cats.add(item.split(":")[0])
        elif item.strip():
            cats.add(item.strip())
    return cats


def parsear_gold(gold_str):
    if not gold_str:
        return set()
    return set(c for c in gold_str.split(",") if c)


def primera_categoria(pred_str):
    """Devuelve la categoría con mayor score (la primera en el string ordenado)."""
    if not pred_str:
        return None
    primera = pred_str.split(",")[0]
    return primera.split(":")[0] if ":" in primera else primera


def prf(tp, fp, fn):
    p = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    r = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f = 2 * p * r / (p + r) if (p + r) > 0 else 0.0
    return p, r, f


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", help="Exportar tabla por categoría a CSV")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    conn = get_connection()

    rows = conn.execute(
        """
        SELECT id, fuente_tabla, fuente_id, titulo, pred_categorias,
               gold_relevante, gold_categorias
          FROM eval_set
         WHERE gold_relevante IS NOT NULL
        """
    ).fetchall()

    if not rows:
        logger.error("No hay filas etiquetadas en eval_set. Etiqueta primero con etiquetar_web.py")
        return

    total = len(rows)
    logger.info(f"\n{'='*60}")
    logger.info(f"  EVAL SET — {total} documentos etiquetados")
    logger.info(f"{'='*60}\n")

    # ── 1. Binario: relevancia legislativa ──────────────────────────
    bin_tp = bin_fp = bin_tn = bin_fn = 0
    for r in rows:
        pred_cats = parsear_pred(r[4])
        modelo_pos = len(pred_cats) > 0
        gold_pos = r[5] == 1
        if modelo_pos and gold_pos:
            bin_tp += 1
        elif modelo_pos and not gold_pos:
            bin_fp += 1
        elif not modelo_pos and gold_pos:
            bin_fn += 1
        else:
            bin_tn += 1

    p, rec, f1 = prf(bin_tp, bin_fp, bin_fn)
    accuracy = (bin_tp + bin_tn) / total

    logger.info("1) RELEVANCIA LEGISLATIVA (binario)")
    logger.info("─" * 60)
    logger.info(f"   Confusion matrix:")
    logger.info(f"                  modelo SÍ    modelo NO")
    logger.info(f"   gold SÍ          {bin_tp:4d}         {bin_fn:4d}")
    logger.info(f"   gold NO          {bin_fp:4d}         {bin_tn:4d}")
    logger.info(f"")
    logger.info(f"   Precision: {p:.3f}   ({bin_tp}/{bin_tp+bin_fp})")
    logger.info(f"   Recall:    {rec:.3f}   ({bin_tp}/{bin_tp+bin_fn})")
    logger.info(f"   F1:        {f1:.3f}")
    logger.info(f"   Accuracy:  {accuracy:.3f}\n")

    # ── 2. Multi-label por categoría ────────────────────────────────
    por_cat = {c: {"tp": 0, "fp": 0, "fn": 0} for c in CATEGORIAS_LISTA}

    for r in rows:
        pred_cats = parsear_pred(r[4])
        gold_cats = parsear_gold(r[6])

        for cat in CATEGORIAS_LISTA:
            in_pred = cat in pred_cats
            in_gold = cat in gold_cats
            if in_pred and in_gold:
                por_cat[cat]["tp"] += 1
            elif in_pred and not in_gold:
                por_cat[cat]["fp"] += 1
            elif not in_pred and in_gold:
                por_cat[cat]["fn"] += 1

    logger.info("2) MULTI-LABEL POR CATEGORÍA")
    logger.info("─" * 60)
    logger.info(f"   {'categoria':22s} {'TP':>4} {'FP':>4} {'FN':>4}  {'P':>6} {'R':>6} {'F1':>6}")
    logger.info(f"   {'-'*22} {'-'*4} {'-'*4} {'-'*4}  {'-'*6} {'-'*6} {'-'*6}")

    macro_p = macro_r = macro_f = 0.0
    sum_tp = sum_fp = sum_fn = 0
    cats_con_gold = 0
    rows_csv = []

    for cat in CATEGORIAS_LISTA:
        tp, fp, fn = por_cat[cat]["tp"], por_cat[cat]["fp"], por_cat[cat]["fn"]
        if tp + fp + fn == 0:
            continue  # categoría sin ninguna instancia, no aporta
        cats_con_gold += 1
        p_c, r_c, f_c = prf(tp, fp, fn)
        macro_p += p_c
        macro_r += r_c
        macro_f += f_c
        sum_tp += tp
        sum_fp += fp
        sum_fn += fn
        logger.info(f"   {cat:22s} {tp:>4} {fp:>4} {fn:>4}  {p_c:>6.3f} {r_c:>6.3f} {f_c:>6.3f}")
        rows_csv.append([cat, tp, fp, fn, round(p_c, 3), round(r_c, 3), round(f_c, 3)])

    if cats_con_gold > 0:
        macro_p /= cats_con_gold
        macro_r /= cats_con_gold
        macro_f /= cats_con_gold
    micro_p, micro_r, micro_f = prf(sum_tp, sum_fp, sum_fn)

    logger.info(f"   {'-'*22} {'-'*4} {'-'*4} {'-'*4}  {'-'*6} {'-'*6} {'-'*6}")
    logger.info(f"   {'MACRO avg':22s} {'':>4} {'':>4} {'':>4}  {macro_p:>6.3f} {macro_r:>6.3f} {macro_f:>6.3f}")
    logger.info(f"   {'MICRO avg':22s} {sum_tp:>4} {sum_fp:>4} {sum_fn:>4}  {micro_p:>6.3f} {micro_r:>6.3f} {micro_f:>6.3f}")
    logger.info("")

    # ── 3. Top-1 accuracy (sobre los relevantes) ───────────────────
    top1_aciertos = 0
    top1_evaluables = 0
    for r in rows:
        if r[5] != 1:
            continue
        gold_cats = parsear_gold(r[6])
        if not gold_cats:
            continue
        top = primera_categoria(r[4])
        top1_evaluables += 1
        if top and top in gold_cats:
            top1_aciertos += 1

    if top1_evaluables > 0:
        logger.info("3) TOP-1 (sobre documentos relevantes con gold categoría)")
        logger.info("─" * 60)
        logger.info(f"   Top-1 accuracy: {top1_aciertos/top1_evaluables:.3f}  ({top1_aciertos}/{top1_evaluables})")
        logger.info("")

    # ── 4. Errores destacados ─────────────────────────────────────
    fps = []
    fns = []
    cat_errors = []
    for r in rows:
        pred_cats = parsear_pred(r[4])
        gold_cats = parsear_gold(r[6])
        modelo_pos = len(pred_cats) > 0
        gold_pos = r[5] == 1

        if modelo_pos and not gold_pos:
            fps.append(r)
        elif not modelo_pos and gold_pos:
            fns.append(r)
        elif modelo_pos and gold_pos and pred_cats != gold_cats:
            cat_errors.append((r, pred_cats, gold_cats))

    def _mostrar(label, items, fmt):
        if not items:
            return
        logger.info(f"4) {label} ({len(items)})")
        logger.info("─" * 60)
        for item in items[:10]:
            logger.info(fmt(item))
        if len(items) > 10:
            logger.info(f"   … +{len(items) - 10} más")
        logger.info("")

    _mostrar(
        "FALSOS POSITIVOS (modelo dice relevante, tú dijiste NO)",
        fps,
        lambda r: f"   [{r[1]}#{r[2]}] {r[3][:90]}\n     pred: {r[4]}",
    )
    _mostrar(
        "FALSOS NEGATIVOS (tú dijiste relevante, modelo dijo NO)",
        fns,
        lambda r: f"   [{r[1]}#{r[2]}] {r[3][:90]}\n     gold: {r[6]}",
    )
    _mostrar(
        "CATEGORÍAS MAL ASIGNADAS (relevante pero categoría diferente)",
        cat_errors,
        lambda x: f"   [{x[0][1]}#{x[0][2]}] {x[0][3][:90]}\n     pred: {sorted(x[1])}\n     gold: {sorted(x[2])}",
    )

    # ── 5. CSV opcional ──────────────────────────────────────────
    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["categoria", "tp", "fp", "fn", "precision", "recall", "f1"])
            w.writerows(rows_csv)
            w.writerow([])
            w.writerow(["macro_avg", "", "", "", round(macro_p, 3), round(macro_r, 3), round(macro_f, 3)])
            w.writerow(["micro_avg", sum_tp, sum_fp, sum_fn, round(micro_p, 3), round(micro_r, 3), round(micro_f, 3)])
        logger.info(f"📄 CSV exportado a {args.csv}")


if __name__ == "__main__":
    main()
