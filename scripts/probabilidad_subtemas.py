"""Probabilidad por subtema: P(se presenta ≥1 INICIATIVA del subtema en los
próximos 14 días) — probabilidad EMPÍRICA CONDICIONADA, calibrada y vigilada.

Método (backtest 15-jul-2026: skill +19% vs referencia, walk-forward):
  contexto = (periodo ordinario vs receso) × (momentum 14d en terciles propios)
  P cruda   = frecuencia histórica del evento en ese contexto, ponderada por
              recencia (vida media 90 días) + suavizado bayesiano a la base
  P final   = capa de recalibración (bins de 5 pts contra lo ocurrido)

Clasificación automática (guardas anti-credibilidad):
  vivo         → 10% ≤ base < 90% y ≥25 iniciativas: publica probabilidad
  siempre_activo → base ≥90%: se etiqueta así, SIN número (decir 95% siempre
                   es vender aire — regla del usuario 15-jul)
  monitoreando → <25 iniciativas o calibración degradada: sin número

Salida: dashboard/probabilidad_subtemas.json (gráfica Tendencia del dashboard).
Corre en CI (probabilidad.yml). $0: CPU pura, ~5s los 88 subtemas.
Taxonomía: config.CATEGORIAS[*]["subcategorias"] — la existente, nada nuevo.
"""
import json
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
SALIDA = ROOT / "dashboard" / "probabilidad_subtemas.json"
H = 14          # horizonte del evento (días)
HL = 90.0       # vida media de la ponderación por recencia (días)
K_SUAVE = 8     # fuerza del suavizado bayesiano hacia la base del subtema
MIN_INIS = 25   # mínimo de iniciativas históricas para publicar probabilidad
CAL_TOL = 15.0  # si |predicho-ocurrido| mediano por bin supera esto → degradar


def na(s):
    return "".join(c for c in unicodedata.normalize("NFD", (s or "").lower())
                   if unicodedata.category(c) != "Mn")


def en_periodo(iso):
    m, day = int(iso[5:7]), int(iso[8:10])
    return 1 if (m in (2, 3, 4) or m in (9, 10, 11) or (m == 12 and day <= 15)) else 0


def subtemas():
    out = {}
    for cat, v in CATEGORIAS.items():
        for key, sv in (v.get("subcategorias") or {}).items():
            kws = sv.get("keywords", []) if isinstance(sv, dict) else list(sv)
            nombre = sv.get("nombre", key) if isinstance(sv, dict) else key
            out[f"{cat}::{key}"] = {"nombre": nombre, "cat": cat,
                                    "kw": [na(k) for k in kws]}
    return out


def main():
    con = sqlite3.connect(str(DB))
    rows = [(na(t), f) for t, f in con.execute(
        """SELECT titulo, substr(fecha_presentacion,1,10) FROM sil_documentos
           WHERE fecha_presentacion>='2024-09-01' AND titulo IS NOT NULL
             AND lower(COALESCE(NULLIF(tipo_grupo,''),tipo)) LIKE '%iniciativa%'
             AND es_duplicado_cross_camara IS NOT 1""")]
    hoy = max(f for _, f in rows)
    d0, d1 = dt.date(2024, 9, 20), dt.date.fromisoformat(hoy)
    dias = [(d0 + dt.timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]

    board = []
    preds_globales = []
    for key, s in subtemas().items():
        porf = defaultdict(int)
        tot = 0
        for t, f in rows:
            if any(k in t for k in s["kw"]):
                porf[f] += 1
                tot += 1

        serie = [porf.get(f, 0) for f in dias]
        filas = []
        for i in range(20, len(dias)):
            rec = sum(serie[i - 13:i + 1])
            fut = sum(serie[i + 1:i + 1 + H]) if i + H < len(dias) else None
            filas.append([dias[i], en_periodo(dias[i]), rec,
                          (1 if fut > 0 else 0) if fut is not None else None])
        recs = sorted(x[2] for x in filas)
        t1, t2 = recs[len(recs) // 3], recs[2 * len(recs) // 3]

        hist = defaultdict(list)
        track = []
        preds = []
        for j, x in enumerate(filas):
            mom = 0 if x[2] <= t1 else (1 if x[2] <= t2 else 2)
            obs = hist[(x[1], mom)]
            ev = sum(y * 0.5 ** ((j - di) / HL) for di, y in obs)
            tt = sum(0.5 ** ((j - di) / HL) for di, y in obs)
            allo = [o for l in hist.values() for o in l]
            bev = sum(y * 0.5 ** ((j - di) / HL) for di, y in allo)
            btot = sum(0.5 ** ((j - di) / HL) for di, y in allo)
            base_c = bev / btot if btot > 0 else 0.5
            p = (ev + K_SUAVE * base_c) / (tt + K_SUAVE) if tt >= 10 else None
            track.append((x[0], None if p is None else p))
            if p is not None and x[3] is not None:
                preds.append((x[0], p, x[3]))
            if j >= H and filas[j - H][3] is not None:
                xa = filas[j - H]
                ma = 0 if xa[2] <= t1 else (1 if xa[2] <= t2 else 2)
                hist[(xa[1], ma)].append((j - H, xa[3]))
        preds_globales += preds

        # base sin condicionar (para clasificar saturados)
        evs = [x[3] for x in filas if x[3] is not None]
        base = sum(evs) / len(evs) if evs else 0.0
        vals = [(f, v) for f, v in track if v is not None]
        board.append({"key": key, "nombre": s["nombre"], "cat": s["cat"],
                      "n_inis": tot, "base": round(base, 3),
                      "_track": vals})

    # ── capa de recalibración global (bins de 5 pts, walk-forward) ──
    preds_globales.sort()
    bins = defaultdict(lambda: [0, 0])
    pend = []
    for f, p, y in preds_globales:
        fd = dt.date.fromisoformat(f)
        while pend and pend[0][0] <= fd:
            _, bk, yy = pend.pop(0)
            bins[bk][0] += yy
            bins[bk][1] += 1
        pend.append((fd + dt.timedelta(days=H), min(19, int(p * 20)), y))
        pend.sort()
    mapa = {}
    calib = []
    for k in range(20):
        ev, tt = bins[k]
        centro = k / 20 + 0.025
        mapa[k] = (ev + 20 * centro) / (tt + 20) if tt >= 40 else None
        if tt >= 40:
            calib.append({"pred": round(centro * 100), "obs": round(ev / tt * 100, 1), "n": tt})

    def recal(p):
        m = mapa.get(min(19, int(p * 20)))
        return m if m is not None else p

    # vigilancia de calibración: desviación mediana |pred-obs| de los bins
    desv = sorted(abs(c["pred"] - c["obs"]) for c in calib)
    desv_med = desv[len(desv) // 2] if desv else 99.0
    calibracion_ok = desv_med <= CAL_TOL

    # ── clasificación + salida ──
    out_board = []
    for b in board:
        vals = b.pop("_track")
        pR = [(f, round(recal(v) * 100)) for f, v in vals]
        p_hoy = pR[-1][1] if pR else None
        p_4w = pR[-29][1] if len(pR) >= 29 else (pR[0][1] if pR else None)
        if b["base"] >= 0.90:
            clase = "siempre_activo"
        elif b["n_inis"] < MIN_INIS or len(pR) < 60 or not calibracion_ok:
            clase = "monitoreando"
        else:
            clase = "vivo"
        # spark semanal (cada 7 días, últimas ~26 semanas)
        spark = [v for _, v in pR[-182::7]] if pR else []
        out_board.append({"key": b["key"], "nombre": b["nombre"], "cat": b["cat"],
                          "clase": clase, "n_inis": b["n_inis"],
                          "p": p_hoy if clase == "vivo" else None,
                          "delta": (p_hoy - p_4w) if (clase == "vivo" and p_4w is not None) else None,
                          "spark": spark if clase == "vivo" else [],
                          "_pR": pR})

    vivos = [b for b in out_board if b["clase"] == "vivo"]
    vivos.sort(key=lambda b: -abs(b["delta"] or 0))
    movers = [b["key"] for b in vivos[:5]]
    tracks = {b["key"]: b["_pR"][-210:] for b in out_board if b["key"] in movers}
    for b in out_board:
        b.pop("_pR")

    SALIDA.write_text(json.dumps({
        "generado": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "hasta": hoy, "horizonte_dias": H,
        "definicion": "Probabilidad de que se presente al menos una iniciativa "
                      "del subtema en los próximos 14 días (frecuencia histórica "
                      "condicionada, recalibrada).",
        "calibracion": {"ok": calibracion_ok, "desviacion_mediana": desv_med,
                        "bins": calib},
        "movers": movers, "tracks": tracks, "board": out_board,
    }, ensure_ascii=False))
    nv = sum(1 for b in out_board if b["clase"] == "vivo")
    print(f"✅ {SALIDA.name}: {len(out_board)} subtemas ({nv} vivos) · "
          f"calibración {'OK' if calibracion_ok else 'DEGRADADA'} "
          f"(desv mediana {desv_med:.1f} pts) · hasta {hoy}")


if __name__ == "__main__":
    main()
