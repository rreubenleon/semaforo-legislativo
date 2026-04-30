"""
Análisis histórico de recesos legislativos LXVI.

Pregunta que responde: ¿qué pasa durante el receso? Volumen, tipos,
tasas de aprobación, categorías más activas, comparación con periodos
ordinarios. Genera reporte para calibrar el modo "receso" del pipeline.

Salida: eval/recesos/analisis_historico.md

Uso:
    python scripts/analisis_recesos_historico.py
"""
from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
OUT_DIR = ROOT / "eval" / "recesos"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def main():
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT seguimiento_id, asunto_id, tipo_grupo, tipo, titulo, camara,
               estatus, presentador, partido, comision, categoria,
               fecha_presentacion, periodo, legislatura
          FROM sil_documentos
         WHERE legislatura = 'LXVI'
           AND clasificacion = 'legislativa'
           AND fecha_presentacion IS NOT NULL AND fecha_presentacion != ''
           AND periodo IS NOT NULL AND periodo != ''
    """).fetchall()

    # Agrupar por periodo
    por_periodo = defaultdict(list)
    for r in rows:
        por_periodo[r['periodo']].append(r)

    out = []
    out.append(f"# Análisis Histórico de Recesos — LXVI Legislatura\n")
    out.append(f"_Generado el {datetime.now().strftime('%Y-%m-%d %H:%M')} desde "
               f"{len(rows):,} instrumentos legislativos clasificados._\n")

    # ── 1. Volumen por periodo ────────────────────────────────────────────
    out.append("## 1. Volumen por periodo\n")
    out.append("| Periodo | Total | Iniciativas | Proposiciones PA | Dictámenes | Minutas |")
    out.append("|---|---:|---:|---:|---:|---:|")
    periodos_orden = sorted(por_periodo.keys(), key=lambda p:
                            (0 if 'Ordinario' in p else 1, p))
    for p in periodos_orden:
        items = por_periodo[p]
        ini = sum(1 for it in items if it['tipo_grupo'] == 'Iniciativa')
        prop = sum(1 for it in items if it['tipo_grupo'] == 'Proposición con Punto de Acuerdo')
        dic = sum(1 for it in items if it['tipo_grupo'] == 'Dictamen')
        mini = sum(1 for it in items if it['tipo_grupo'] == 'Minuta')
        out.append(f"| {p} | {len(items):,} | {ini:,} | {prop:,} | {dic:,} | {mini:,} |")
    out.append("")

    # ── 2. Comparativo Receso vs Ordinario ────────────────────────────────
    rec_items = [it for p in por_periodo for it in por_periodo[p] if 'Receso' in p]
    ord_items = [it for p in por_periodo for it in por_periodo[p] if 'Ordinario' in p]

    def stats(items, label):
        ini = sum(1 for it in items if it['tipo_grupo'] == 'Iniciativa')
        prop = sum(1 for it in items if it['tipo_grupo'] == 'Proposición con Punto de Acuerdo')
        dic = sum(1 for it in items if it['tipo_grupo'] == 'Dictamen')
        aprob = sum(1 for it in items
                    if any(k in (it['estatus'] or '').lower() for k in ('aprob', 'resuelto')))
        return {'label': label, 'total': len(items), 'ini': ini, 'prop': prop,
                'dic': dic, 'aprob': aprob,
                'tasa_aprob': round(aprob / len(items) * 100, 1) if items else 0}

    s_rec = stats(rec_items, "Recesos")
    s_ord = stats(ord_items, "Ordinarios")

    out.append("## 2. Receso vs Ordinario (LXVI completo)\n")
    out.append("| Métrica | Receso | Ordinario | Δ |")
    out.append("|---|---:|---:|---:|")
    out.append(f"| Total instrumentos | {s_rec['total']:,} | {s_ord['total']:,} | {s_rec['total']/max(s_ord['total'],1)*100:.0f}% del ordinario |")
    out.append(f"| Iniciativas | {s_rec['ini']:,} | {s_ord['ini']:,} | {s_rec['ini']/max(s_ord['ini'],1)*100:.0f}% |")
    out.append(f"| Proposiciones PA | {s_rec['prop']:,} | {s_ord['prop']:,} | {s_rec['prop']/max(s_ord['prop'],1)*100:.0f}% |")
    out.append(f"| Dictámenes | {s_rec['dic']:,} | {s_ord['dic']:,} | {s_rec['dic']/max(s_ord['dic'],1)*100:.0f}% |")
    out.append(f"| Aprobados | {s_rec['aprob']:,} | {s_ord['aprob']:,} | — |")
    out.append(f"| Tasa de aprobación | **{s_rec['tasa_aprob']}%** | **{s_ord['tasa_aprob']}%** | — |")
    out.append("")

    # ── 3. Distribución por categoría FIAT ───────────────────────────────
    out.append("## 3. Categorías más activas en RECESO\n")
    out.append("¿Qué temas SÍ se mueven cuando no hay sesiones ordinarias?\n")
    out.append("| Categoría | En Receso | En Ordinario | Ratio (Rec/Ord) | Interpretación |")
    out.append("|---|---:|---:|---:|---|")

    cat_rec = Counter((it['categoria'] or '').split(':')[0].strip()
                      for it in rec_items if it['categoria'])
    cat_ord = Counter((it['categoria'] or '').split(':')[0].strip()
                      for it in ord_items if it['categoria'])

    # Top categorías por volumen en receso
    todas_cats = set(cat_rec.keys()) | set(cat_ord.keys())
    rows_cats = []
    for cat in todas_cats:
        if not cat:
            continue
        rec_n = cat_rec.get(cat, 0)
        ord_n = cat_ord.get(cat, 0)
        if rec_n == 0:
            continue
        # Ratio normalizado: receso ~13% del total → categoría con ratio >0.13 es sobrerrepresentada
        ratio = rec_n / max(ord_n, 1)
        # Interpretación cualitativa
        if ratio > 0.25:
            interp = "Muy reactiva en receso"
        elif ratio > 0.15:
            interp = "Reactiva moderada"
        elif ratio > 0.05:
            interp = "Baja reactividad"
        else:
            interp = "Inactiva en receso"
        rows_cats.append((cat, rec_n, ord_n, ratio, interp))
    rows_cats.sort(key=lambda x: -x[3])  # por ratio descendente
    for cat, rec_n, ord_n, ratio, interp in rows_cats[:15]:
        out.append(f"| {cat} | {rec_n} | {ord_n} | {ratio:.2f} | {interp} |")
    out.append("")
    out.append("_Ratio = receso / ordinario por categoría. Más alto = tema más reactivo a coyuntura "
               "(no necesita sesión ordinaria para aparecer)._\n")

    # ── 4. Estatus en receso: ¿qué les pasa? ─────────────────────────────
    out.append("## 4. ¿Qué pasa con los instrumentos presentados en receso?\n")
    estatus_rec = Counter()
    for it in rec_items:
        est = (it['estatus'] or '').strip()
        # Simplificar estatus
        est_low = est.lower()
        if 'aprob' in est_low or 'resuelto' in est_low: cat_est = 'Aprobado/Resuelto'
        elif 'desech' in est_low: cat_est = 'Desechado'
        elif 'retir' in est_low: cat_est = 'Retirado'
        elif 'pendiente' in est_low: cat_est = 'Pendiente'
        elif 'turnado' in est_low: cat_est = 'Turnado (sin resolver)'
        else: cat_est = 'Otro'
        estatus_rec[cat_est] += 1
    total_rec = sum(estatus_rec.values())
    out.append("| Estatus | N | % |")
    out.append("|---|---:|---:|")
    for est, n in estatus_rec.most_common():
        out.append(f"| {est} | {n:,} | {n/max(total_rec,1)*100:.1f}% |")
    out.append("")

    # ── 5. Tipo dominante por periodo ─────────────────────────────────────
    out.append("## 5. Composición por tipo de instrumento\n")
    out.append("¿Cómo cambia el mix de tipos entre receso y ordinario?\n")
    out.append("| Tipo | Receso | Ordinario |")
    out.append("|---|---:|---:|")
    tipos = ['Iniciativa', 'Proposición con Punto de Acuerdo', 'Dictamen', 'Minuta',
             'Acuerdo Parlamentario', 'Comunicado']
    for t in tipos:
        r_n = sum(1 for it in rec_items if it['tipo_grupo'] == t)
        o_n = sum(1 for it in ord_items if it['tipo_grupo'] == t)
        r_pct = r_n / max(s_rec['total'], 1) * 100
        o_pct = o_n / max(s_ord['total'], 1) * 100
        out.append(f"| {t} | {r_n:,} ({r_pct:.0f}%) | {o_n:,} ({o_pct:.0f}%) |")
    out.append("")

    # ── 6. Cámaras durante receso ─────────────────────────────────────────
    out.append("## 6. Cámara que más actúa en receso\n")
    cam_rec = Counter((it['camara'] or '?').replace('Cámara de ', '') for it in rec_items)
    out.append("| Cámara | N | % |")
    out.append("|---|---:|---:|")
    for cam, n in cam_rec.most_common():
        out.append(f"| {cam} | {n:,} | {n/max(s_rec['total'],1)*100:.1f}% |")
    out.append("")
    out.append("_Nota: durante receso, la Comisión Permanente puede sesionar en cualquiera de las dos cámaras "
               "según el receso. Esta tabla incluye todo lo presentado durante esos meses._\n")

    # ── 7. Conclusiones para calibración ──────────────────────────────────
    out.append("## 7. Implicaciones para el modelo FIAT\n")
    pct_volumen = s_rec['total'] / (s_rec['total'] + s_ord['total']) * 100
    pct_aprob_rec = s_rec['tasa_aprob']
    pct_aprob_ord = s_ord['tasa_aprob']
    cats_reactivas = [c for c, _, _, ratio, _ in rows_cats if ratio > 0.20]
    cats_muertas = [c for c, _, _, ratio, _ in rows_cats if ratio < 0.05]

    out.append(f"1. **Volumen en receso:** {pct_volumen:.0f}% del volumen total LXVI ocurre en receso. "
               f"El score `Congreso` baseline DEBE recalibrarse por categoría.")
    out.append(f"2. **Tasa de aprobación:** receso {pct_aprob_rec}% vs ordinario {pct_aprob_ord}%. "
               f"{'Receso APROBA MÁS' if pct_aprob_rec > pct_aprob_ord else 'Receso aproba menos' if pct_aprob_rec < pct_aprob_ord else 'Similares'}.")
    if cats_reactivas:
        out.append(f"3. **Categorías reactivas en receso** (las que sí se mueven sin sesión): "
                   f"{', '.join(cats_reactivas[:5])}. Para estas categorías el modo receso debe seguir "
                   f"alimentando `Congreso` normal.")
    if cats_muertas:
        out.append(f"4. **Categorías inactivas en receso**: {', '.join(cats_muertas[:5])}. Para estas, "
                   f"el `Congreso` debe **estructuralmente bajar** durante mayo-agosto y diciembre-enero "
                   f"sin que cuente como señal negativa.")
    out.append(f"5. **Predicción de actividad**: la métrica `iniciativas_proy_15d` carece de sentido en receso. "
               f"Sugerir reemplazarla por `prob_aparicion_permanente_15d` durante el modo receso.")
    out.append("")

    # ── 8. Anexo: top instrumentos aprobados en receso ────────────────────
    out.append("## 8. Anexo: muestras de aprobaciones en receso\n")
    aprobados = [it for it in rec_items
                 if any(k in (it['estatus'] or '').lower() for k in ('aprob', 'resuelto'))]
    out.append(f"Total aprobados en recesos LXVI: {len(aprobados):,}\n")
    out.append("| Periodo | Tipo | Cámara | Categoría | Título (truncado) |")
    out.append("|---|---|---|---|---|")
    for it in aprobados[:15]:
        cat = (it['categoria'] or '').split(':')[0].strip()
        cam = (it['camara'] or '').replace('Cámara de ', '')[:14]
        tit = (it['titulo'] or '')[:80]
        out.append(f"| {it['periodo']} | {it['tipo_grupo']} | {cam} | {cat} | {tit} |")
    out.append("")

    # Guardar
    out_path = OUT_DIR / "analisis_historico.md"
    out_path.write_text("\n".join(out))
    print(f"✓ Reporte generado: {out_path}")
    print(f"  {len(out)} líneas, {sum(len(l) for l in out)//1024} KB")


if __name__ == "__main__":
    main()
