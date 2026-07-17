"""Tendencia de temas en el Congreso — DATOS OBSERVADOS, sin modelo.
Conteo SEMANAL de iniciativas + puntos de acuerdo por tema (la clasificación
FIAT de sil_documentos), desde febrero-2026 a la fecha. Dedupe por contenido
auditado (una pieza = un conteo; las filas por firmante inflaban).
La semana EN CURSO (incompleta) se excluye — un corte a media semana pinta
un desplome falso al final. Salida: dashboard/tendencia_temas.json.
Decisión del usuario (17-jul): la Tendencia del dashboard muestra esto —
frecuencia real de temas — y sobre ella se lee el score.
"""
import json
import re
import sqlite3
import sys
import unicodedata
import datetime as dt
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from config import CATEGORIAS

DB = ROOT / "semaforo.db"
SALIDA = ROOT / "dashboard" / "tendencia_temas.json"
DESDE = "2026-02-01"


def na(s):
    return "".join(c for c in unicodedata.normalize("NFD", (s or "").lower())
                   if unicodedata.category(c) != "Mn")


_PRE_AUTOR = re.compile(
    r"^.{0,600}?(con\s+(?:punto de acuerdo|proyecto de decreto|proposici[oó]n)\s+"
    r"|por el que\s+|que\s+(?=(?:reforma|adiciona|expide|deroga|modifica)))",
    re.I | re.S)


def clave_contenido(t):
    t = na(t).strip()
    m = _PRE_AUTOR.search(t)
    if m and m.start(1) > 30:
        t = t[m.start(1):]
    return re.sub(r"\s+", " ", t)[:160]


def lunes(iso):
    d = dt.date.fromisoformat(iso)
    return (d - dt.timedelta(days=d.weekday())).isoformat()


def main():
    con = sqlite3.connect(str(DB))
    crudos = con.execute(
        """SELECT titulo, substr(fecha_presentacion,1,10), COALESCE(categoria,''),
                  lower(COALESCE(NULLIF(tipo_grupo,''), tipo))
           FROM sil_documentos
           WHERE fecha_presentacion >= ? AND titulo IS NOT NULL
             AND es_duplicado_cross_camara IS NOT 1
             AND (lower(COALESCE(NULLIF(tipo_grupo,''), tipo)) LIKE '%iniciativa%'
                  OR COALESCE(NULLIF(tipo_grupo,''), tipo) LIKE '%PA%'
                  OR lower(COALESCE(NULLIF(tipo_grupo,''), tipo)) LIKE '%punto de acuerdo%')""",
        (DESDE,)).fetchall()
    vistos, filas = set(), []
    for t, f, cat, tg in crudos:
        k = (clave_contenido(t), f)
        if k in vistos:
            continue
        vistos.add(k)
        filas.append((f, cat, "iniciativa" if "iniciativa" in tg else "pa"))
    n_dedup = len(crudos) - len(filas)

    hoy = max(f for f, _, _ in filas)
    # última semana COMPLETA: la del lunes anterior al lunes de `hoy`
    # (si hoy es domingo la semana está completa; criterio conservador: la
    # semana de `hoy` se incluye solo si hoy es >= sábado)
    hd = dt.date.fromisoformat(hoy)
    corte = lunes(hoy) if hd.weekday() >= 5 else (dt.date.fromisoformat(lunes(hoy)) - dt.timedelta(days=7)).isoformat()

    w0 = lunes(DESDE)
    semanas = []
    w = dt.date.fromisoformat(w0)
    while w.isoformat() <= corte:
        semanas.append(w.isoformat())
        w += dt.timedelta(days=7)
    idx = {s: i for i, s in enumerate(semanas)}

    series = {}
    sin_cat = 0
    for f, cat, tipo in filas:
        wk = lunes(f)
        if wk not in idx:
            continue
        if not cat:
            sin_cat += 1
            continue
        if cat not in series:
            series[cat] = {"ini": [0] * len(semanas), "pa": [0] * len(semanas)}
        series[cat]["ini" if tipo == "iniciativa" else "pa"][idx[wk]] += 1

    # serie DIARIA (suma móvil de 7 días, paso diario): la textura "técnica"
    # de la gráfica — misma unidad instrumentos/semana, ~165 puntos por tema
    d_ini = dt.date.fromisoformat(DESDE)
    d_fin = dt.date.fromisoformat(hoy)
    dias_diario = [(d_ini + dt.timedelta(days=i)).isoformat()
                   for i in range((d_fin - d_ini).days + 1)]
    por_dia = defaultdict(lambda: defaultdict(int))
    for f, cat, tipo in filas:
        if cat:
            por_dia[cat][f] += 1

    # cobertura de MEDIOS por tema (categoría primaria del clasificador FIAT),
    # misma unidad de textura: suma móvil 7d de notas, paso diario. Va al
    # FONDO de la gráfica para leer disonancia/uniformidad medios↔Congreso.
    # Días sin captura (caída del scraper: 12-mar→8-abr-2026, arranque de feb)
    # van como null — el fondo no se dibuja ahí en vez de fingir silencio.
    medios_dia = defaultdict(lambda: defaultdict(int))
    vol_dia = defaultdict(int)
    for cats, f in con.execute(
            """SELECT categorias, substr(fecha,1,10) FROM articulos
               WHERE fecha >= ?""", (DESDE,)):
        vol_dia[f] += 1
        cat_p = (cats or "").split(",")[0].split(":")[0].strip()
        if cat_p:
            medios_dia[cat_p][f] += 1
    UMBRAL_7D = 1500  # una semana normal captura ~9K notas; una caída, ~0

    out = {}
    for cat, s in series.items():
        total = [a + b for a, b in zip(s["ini"], s["pa"])]
        if sum(total) < 10:
            continue
        nombre = CATEGORIAS.get(cat, {}).get("nombre", cat)
        u4 = total[-4:]
        p4 = total[-8:-4] if len(total) >= 8 else total[:4]
        m_u, m_p = sum(u4) / max(1, len(u4)), sum(p4) / max(1, len(p4))
        tendencia = "sube" if m_u > m_p * 1.15 else ("baja" if m_u < m_p * 0.85 else "estable")
        pd = por_dia[cat]
        md = medios_dia.get(cat, {})
        diario, medios = [], []
        for d in dias_diario:
            dd = dt.date.fromisoformat(d)
            ult7 = [(dd - dt.timedelta(days=j)).isoformat() for j in range(7)]
            diario.append(sum(pd.get(x, 0) for x in ult7))
            con_captura = sum(vol_dia.get(x, 0) for x in ult7) >= UMBRAL_7D
            medios.append(sum(md.get(x, 0) for x in ult7) if con_captura else None)
        out[cat] = {"nombre": nombre, "total": total, "ini": s["ini"], "pa": s["pa"],
                    "suma": sum(total), "prom4": round(m_u, 1),
                    "tendencia": tendencia, "diario": diario, "medios": medios}

    SALIDA.write_text(json.dumps({
        "generado": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "desde": DESDE, "hasta": hoy, "ultima_semana_completa": corte,
        "semanas": semanas, "dias": dias_diario, "dedupe_filas": n_dedup, "sin_categoria": sin_cat,
        "nota": "Conteo semanal observado de iniciativas + puntos de acuerdo "
                "por tema (clasificación FIAT), dedupe por contenido; semana "
                "en curso excluida.",
        "series": out,
    }, ensure_ascii=False))
    print(f"✅ tendencia_temas: {len(out)} temas · {len(semanas)} semanas "
          f"({w0} → {corte}) · dedupe {n_dedup} · sin categoría {sin_cat} · hasta {hoy}")


if __name__ == "__main__":
    main()
