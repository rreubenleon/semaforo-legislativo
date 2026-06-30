"""Computa la serie histórica de scores 2024-09 → presente usando la hemeroteca
MULTI-FUENTE (La Jornada + Proceso) para media/dominancia, y gaceta/SIL (que sí
tenemos históricos) para congreso/urgencia. trends/mañanera/legisladores = 0
(no existían). Escribe a tabla AISLADA `scores_historico` — NO toca scores ni
articulos ni nada de producción. NO reentrena nada.

Reusa las fórmulas de producción (backfill_scores._calcular_*) para que media y
congreso salgan en la misma escala 0-100. La media histórica es 2-fuente (es lo
que hay); el modelo decidirá después cómo reconciliar escalas.
"""
import json, sqlite3, math
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from config import CATEGORIAS, SCORING, obtener_keywords_categoria
from backfill_scores import _calcular_score_media, _calcular_score_congreso, _calcular_score_urgencia

DB = ROOT / "semaforo.db"
LJ = json.loads((ROOT / "data" / "jornada_hemeroteca.json").read_text())
PR = json.loads((ROOT / "data" / "proceso_hemeroteca.json").read_text())
PESOS_CAT = json.loads((ROOT / "data" / "pesos_por_categoria.json").read_text())["categorias"]
PG = SCORING["pesos"]
INICIO, FIN = date(2024, 9, 1), date(2026, 6, 28)
NCAT = len(CATEGORIAS)


def _dominancia(n_media, n_cong):
    """Réplica simple de la dominancia: media alta + congreso alto = tema caliente.
    Divisores p95-style (mismos órdenes que producción: ~media/8, ~congreso/3)."""
    m = min(n_media / 8.0, 1.0)
    c = min(n_cong / 3.0, 1.0)
    return round(min((0.6 * m + 0.4 * c) * 100, 100), 2)


def main():
    # articulos_por_dia desde hemeroteca (La Jornada titulares + Proceso slugs)
    art = defaultdict(list)
    for d, secs in LJ.items():
        for _s, tits in secs.items():
            for t in tits:
                art[d].append({"titulo": t.lower(), "resumen": "", "peso": 1.0, "fuente": "la_jornada"})
    for d, slugs in PR.items():
        for s in slugs:
            art[d].append({"titulo": s.replace("-", " ").lower(), "resumen": "", "peso": 1.0, "fuente": "proceso"})

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    gac = defaultdict(list)
    for g in conn.execute("SELECT titulo,resumen,comision,fecha FROM gaceta WHERE fecha>='2024-08-01'"):
        gac[str(g["fecha"])[:10]].append({"titulo": (g["titulo"] or "").lower(),
                                          "resumen": (g["resumen"] or "").lower(),
                                          "comision": (g["comision"] or "").lower()})
    sil = defaultdict(lambda: defaultdict(int))
    for s in conn.execute("SELECT categoria,fecha_presentacion FROM sil_documentos WHERE fecha_presentacion>='2024-07-01'"):
        if s["categoria"]:
            sil[s["categoria"]][str(s["fecha_presentacion"])[:10]] += 1

    conn.execute("""CREATE TABLE IF NOT EXISTS scores_historico (
        categoria TEXT, fecha TEXT, score_total REAL, score_media REAL,
        score_trends REAL, score_congreso REAL, score_mananera REAL,
        score_urgencia REAL, score_dominancia REAL, score_legisladores REAL,
        PRIMARY KEY(categoria,fecha))""")
    conn.execute("DELETE FROM scores_historico")

    fechas = []
    d = INICIO
    while d <= FIN:
        fechas.append(d.isoformat()); d += timedelta(days=1)

    n = 0
    for i, f in enumerate(fechas):
        d0 = date.fromisoformat(f)
        vent7 = [(d0 - timedelta(days=k)).isoformat() for k in range(7)]
        dias14 = [(d0 - timedelta(days=k)).isoformat() for k in range(14)]
        dias60 = [(d0 - timedelta(days=k)).isoformat() for k in range(60)]
        total_peso = sum(a["peso"] for dd in vent7 for a in art.get(dd, []))
        total_gac = sum(len(gac.get(dd, [])) for dd in vent7)
        for cat in CATEGORIAS:
            kws = [k.lower() for k in obtener_keywords_categoria(cat)]
            me = _calcular_score_media(art, vent7, kws, total_peso, NCAT) if total_peso else 0.0
            co = _calcular_score_congreso(gac, vent7, kws, total_gac) if total_gac else 0.0
            ur = _calcular_score_urgencia(sil, cat, dias14, dias60, me, 1.0)
            n_media = sum(1 for dd in vent7 for a in art.get(dd, [])
                          if any(k in a["titulo"] for k in kws))
            n_cong = sum(1 for dd in vent7 for g in gac.get(dd, [])
                         if any(k in g["titulo"] or k in g["resumen"] or k in g["comision"] for k in kws))
            do = _dominancia(n_media, n_cong)
            p = PESOS_CAT.get(cat, {}).get("pesos", PG)
            tot = min(p["media"]*me + p["congreso"]*co + p["urgencia"]*ur
                      + p["dominancia"]*do, 100)  # trends/mananera/legisladores = 0
            conn.execute("INSERT OR REPLACE INTO scores_historico VALUES (?,?,?,?,?,?,?,?,?,?)",
                         (cat, f, round(tot, 2), round(me, 2), 0.0, round(co, 2), 0.0,
                          round(ur, 2), do, 0.0))
            n += 1
        if i % 60 == 0:
            conn.commit(); print(f"  {i}/{len(fechas)} ({f})", flush=True)
    conn.commit()
    r = conn.execute("SELECT COUNT(*),MIN(fecha),MAX(fecha) FROM scores_historico").fetchone()
    nz = conn.execute("SELECT COUNT(*) FROM scores_historico WHERE score_media>0").fetchone()[0]
    print(f"Listo: {r[0]} filas ({r[1]}→{r[2]}), con media>0: {nz} → tabla scores_historico", flush=True)


if __name__ == "__main__":
    main()
