"""Probabilidad por subtema v2 — P(≥1 INICIATIVA del subtema en 14 días).
Probabilidad EMPÍRICA CONDICIONADA, calibrada, vigilada — y BLINDADA con las
9 correcciones de la auditoría metodológica del 16-jul (informe en
/tmp/auditoria_formula_metodologo.md; arnés audit_core.py):

  F1 dedupe POR CONTENIDO (una iniciativa = un conteo; las filas por firmante
     inflaban terciles: 1 iniciativa contada 15 veces).
  F2 valor rezagado → SIN número (nada de publicar el 30-jun como "hoy").
  F3 delta por FECHA real (28±3 días), no por índice con huecos.
  F4 terciles EXPANDING (solo pasado; la versión anterior usaba la serie
     completa = fuga menor pero real).
  F5 p publicada = MEDIANA de los últimos 7 días válidos (mata acantilados
     de ±40 pts por cruce de bucket); movers excluyen subtemas con saltos.
  F6 celda sin evidencia (ev=0 o masa<30) → SIN número (el 37% de IA era
     100% prior).
  F7 guardia de calibración POR SUBTEMA sobre la salida RECALIBRADA
     (>15 pts de desviación mediana → monitoreando), además de la global.
  F8 keywords de participacion_ciudadana podadas para iniciativas (los
     términos de medios metían 20-30% de falsos positivos).
  F9 rezago del scraper EXPUESTO (rezago_dias + dato_al por subtema).

Salida: dashboard/probabilidad_subtemas.json. $0, CPU pura. La sección del
dashboard solo se renderiza si hay subtemas vivos válidos (regla: no listo =
no sale). Re-adopción: arnés completo + gate Escéptico + OK del usuario.
"""
import json
import re
import sqlite3
import sys
import unicodedata
import datetime as dt
from collections import defaultdict
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from config import CATEGORIAS

DB = ROOT / "semaforo.db"
SALIDA = ROOT / "dashboard" / "probabilidad_subtemas.json"
H = 14
HL = 90.0
K_SUAVE = 8
MIN_INIS = 25
MASA_MIN = 30.0     # F6: masa ponderada mínima de la celda para emitir p
CAL_TOL = 15.0      # tolerancia de desviación mediana (global Y por subtema)
STALE_DIAS = 2      # F2: p sin dato fresco (> este rezago) no se publica
SALTO_MAX = 25      # F5: subtema con salto diario > esto no entra a movers

# F8: para INICIATIVAS, keywords precisas (las de config sirven para medios)
KW_OVERRIDE = {
    # Escéptico 17-jul: 'radiodifusión' a secas duplicaba el mismo evento
    # (reforma LFTR) con telecomunicaciones — 96% de matches compartidos.
    "medios_comunicacion::television_radio": [
        "television abierta", "television publica", "medio radiofonico",
        "concesion de television", "transmision televisiva", "senal abierta",
        "television de paga", "contenido audiovisual", "canal de television",
        "sistema publico de radiodifusion"],
    "electoral_politico::participacion_ciudadana": [
        "consulta popular", "revocacion de mandato", "referendum",
        "plebiscito", "democracia participativa", "democracia directa",
        "participacion ciudadana", "iniciativa ciudadana", "cabildo abierto",
        "presupuesto participativo"],
}


def na(s):
    return "".join(c for c in unicodedata.normalize("NFD", (s or "").lower())
                   if unicodedata.category(c) != "Mn")


_PRE_AUTOR = re.compile(
    r"^.{0,600}?(con\s+(?:punto de acuerdo|proyecto de decreto|proposici[oó]n)\s+"
    r"|por el que\s+|que\s+(?=(?:reforma|adiciona|expide|deroga|modifica)))",
    re.I | re.S)


def clave_contenido(titulo):
    """F1: clave del CONTENIDO del instrumento — quita el prefijo de autores
    (las filas por firmante solo difieren ahí) y normaliza."""
    t = na(titulo).strip()
    m = _PRE_AUTOR.search(t)
    if m and m.start(1) > 30:
        t = t[m.start(1):]
    return re.sub(r"\s+", " ", t)[:160]


def en_periodo(iso):
    m, day = int(iso[5:7]), int(iso[8:10])
    return 1 if (m in (2, 3, 4) or m in (9, 10, 11) or (m == 12 and day <= 15)) else 0


def subtemas():
    out = {}
    for cat, v in CATEGORIAS.items():
        for key, sv in (v.get("subcategorias") or {}).items():
            kws = sv.get("keywords", []) if isinstance(sv, dict) else list(sv)
            nombre = sv.get("nombre", key) if isinstance(sv, dict) else key
            k = f"{cat}::{key}"
            out[k] = {"nombre": nombre, "cat": cat,
                      "kw": [na(x) for x in KW_OVERRIDE.get(k, kws)]}
    return out


def main():
    con = sqlite3.connect(str(DB))
    crudos = [(na(t), f) for t, f in con.execute(
        """SELECT titulo, substr(fecha_presentacion,1,10) FROM sil_documentos
           WHERE fecha_presentacion>='2024-09-01' AND titulo IS NOT NULL
             AND lower(COALESCE(NULLIF(tipo_grupo,''),tipo)) LIKE '%iniciativa%'
             AND es_duplicado_cross_camara IS NOT 1""")]
    # F1: una iniciativa = un conteo (dedupe por contenido+fecha)
    vistos, rows = set(), []
    for t, f in crudos:
        k = (clave_contenido(t), f)
        if k in vistos:
            continue
        vistos.add(k)
        rows.append((t, f))
    n_dedup = len(crudos) - len(rows)

    hoy = max(f for _, f in rows)
    hoy_d = dt.date.fromisoformat(hoy)
    rezago = (dt.datetime.utcnow().date() - hoy_d).days   # F9
    d0 = dt.date(2024, 9, 20)
    dias = [(d0 + dt.timedelta(days=i)).isoformat() for i in range((hoy_d - d0).days + 1)]

    # C1 (re-auditoría): las keywords se buscan en el CONTENIDO del título
    # (tras quitar el prefijo de autoría) — "grupo parlamentario" en la
    # boilerplate de autores generaba 96% de falsos positivos.
    rows_c = [(clave_contenido(t), f) for t, f in rows]
    board, preds_globales = [], []
    for key, s in subtemas().items():
        # A2 (Escéptico 17-jul): frontera de palabra — el substring crudo
        # metía 'presa'⊂empresa (33% FP en agua) e 'ine'⊂cine/minería (39%).
        rx = re.compile(r"\b(?:" + "|".join(re.escape(k) for k in s["kw"]) + r")\b")
        porf = defaultdict(int)
        tot = 0
        for t, f in rows_c:
            if rx.search(t):
                porf[f] += 1
                tot += 1
        serie = [porf.get(f, 0) for f in dias]
        filas = []
        for i in range(20, len(dias)):
            rec = sum(serie[i - 13:i + 1])
            fut = sum(serie[i + 1:i + 1 + H]) if i + H < len(dias) else None
            filas.append([dias[i], en_periodo(dias[i]), rec,
                          (1 if fut > 0 else 0) if fut is not None else None])

        # F4: terciles EXPANDING — solo con el pasado de cada día
        recs_hist = []
        hist = defaultdict(list)
        track, preds = [], []
        for j, x in enumerate(filas):
            if len(recs_hist) >= 60:
                sr = sorted(recs_hist)
                t1, t2 = sr[len(sr) // 3], sr[2 * len(sr) // 3]
                mom = 0 if x[2] <= t1 else (1 if x[2] <= t2 else 2)
            else:
                mom = None
            p = None
            if mom is not None:
                obs = hist[(x[1], mom)]
                ev = sum(y * 0.5 ** ((j - di) / HL) for di, y in obs)
                tt = sum(0.5 ** ((j - di) / HL) for di, y in obs)
                # C2 (re-auditoría): (ev>0 ∧ tt≥15) ∨ (tt≥30). Con masa
                # suficiente, ev=0 emite p BAJA (es evidencia, no ausencia);
                # sin masa, solo si hay evidencia positiva.
                if (ev > 0 and tt >= 15) or tt >= 30:
                    allo = [o for l in hist.values() for o in l]
                    bev = sum(y * 0.5 ** ((j - di) / HL) for di, y in allo)
                    btot = sum(0.5 ** ((j - di) / HL) for di, y in allo)
                    base_c = bev / btot if btot > 0 else 0.5
                    p = (ev + K_SUAVE * base_c) / (tt + K_SUAVE)
            track.append((x[0], p))
            if p is not None and x[3] is not None:
                preds.append((x[0], p, x[3]))
            recs_hist.append(x[2])
            if j >= H and filas[j - H][3] is not None:
                xa = filas[j - H]
                if len(recs_hist) > H + 60:
                    sr = sorted(recs_hist[:j - H + 1])
                    ta, tb = sr[len(sr) // 3], sr[2 * len(sr) // 3]
                    ma = 0 if xa[2] <= ta else (1 if xa[2] <= tb else 2)
                    hist[(xa[1], ma)].append((j - H, xa[3]))
        preds_globales += [(f, key, p, y) for f, p, y in preds]

        evs = [x[3] for x in filas if x[3] is not None]
        base = sum(evs) / len(evs) if evs else 0.0
        board.append({"key": key, "nombre": s["nombre"], "cat": s["cat"],
                      "n_inis": tot, "base": round(base, 3),
                      "_track": [(f, v) for f, v in track],
                      "_preds": preds})

    # ── recalibración global walk-forward (bins de 5 pts) ──
    preds_globales.sort()
    bins = defaultdict(lambda: [0, 0])
    pend = []
    finales = []
    for f, key, p, y in preds_globales:
        fd = dt.date.fromisoformat(f)
        while pend and pend[0][0] <= fd:
            _, bk, yy = pend.pop(0)
            bins[bk][0] += yy
            bins[bk][1] += 1
        k = min(19, int(p * 20))
        ev, tt = bins[k]
        adj = (ev + 20 * p) / (tt + 20) if tt >= 40 else p
        finales.append((f, key, adj, y))
        pend.append((fd + dt.timedelta(days=H), k, y))
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

    # guardia GLOBAL sobre la salida RECALIBRADA (F7a)
    gb = defaultdict(lambda: [0, 0, 0.0])
    for _, _, adj, y in finales:
        kb = min(9, int(adj * 10))
        gb[kb][0] += y; gb[kb][1] += 1; gb[kb][2] += adj
    desvs = [abs(sp / n * 100 - ev / n * 100) for ev, n, sp in gb.values() if n >= 100]
    desv_global = median(desvs) if desvs else 99.0
    calibracion_ok = desv_global <= CAL_TOL

    # calibración POR SUBTEMA sobre salida recalibrada (F7b)
    por_sub = defaultdict(list)
    for _, key, adj, y in finales:
        por_sub[key].append((adj, y))
    desv_sub = {}
    for key, ps in por_sub.items():
        # C4a: quintiles (como antes)
        q = None
        if len(ps) >= 100:
            qb = defaultdict(lambda: [0, 0, 0.0])
            for adj, y in ps:
                kb = min(4, int(adj * 5))
                qb[kb][0] += y; qb[kb][1] += 1; qb[kb][2] += adj
            ds = [abs(sp / n * 100 - ev / n * 100) for ev, n, sp in qb.values() if n >= 25]
            q = round(median(ds), 1) if ds else None
        # C4b' (Escéptico 17-jul): ventanas independientes POR BANDAS de p
        # (la media global se cancela entre bandas: migracion daba 0.9 global
        # y 15.1 z=2.91 por bandas). Orden por FECHA, cada H días.
        indep_falla = False
        thin = ps[::H]   # ps ya viene en orden cronológico
        if len(thin) >= 10:
            # bandas = TERCILES DE LA DISTRIBUCIÓN de p (no tercios fijos:
            # con p concentrada arriba, los tercios fijos diluían la banda
            # y migracion_refugio se escapaba)
            orden = sorted(thin, key=lambda t: t[0])
            n3 = len(orden) // 3
            bandas = {0: orden[:n3], 1: orden[n3:2 * n3], 2: orden[2 * n3:]}
            for bb in bandas.values():
                if len(bb) >= 8:
                    pm = sum(a for a, _ in bb) / len(bb)
                    om = sum(y for _, y in bb) / len(bb)
                    se = (pm * (1 - pm) / len(bb)) ** 0.5 or 1e-6
                    if abs(om - pm) * 100 > CAL_TOL and abs(om - pm) / se > 1.96:
                        indep_falla = True
        desv_sub[key] = None if q is None else (99.0 if indep_falla else q)

    # ── clasificación + números publicables ──
    out_board = []
    for b in board:
        vals = [(f, recal(v)) for f, v in b.pop("_track") if v is not None]
        b.pop("_preds")
        pR = [(f, round(v * 100)) for f, v in vals]

        # F2: frescura — sin dato reciente no hay número
        fresco = bool(pR) and (hoy_d - dt.date.fromisoformat(pR[-1][0])).days <= STALE_DIAS
        # F5: p publicada = mediana de los últimos 7 días con dato
        ult7 = [v for f, v in pR if (hoy_d - dt.date.fromisoformat(f)).days <= 6]
        p_pub = round(median(ult7)) if len(ult7) >= 3 else None
        # C3 (re-auditoría): delta = mediana7 actual vs mediana7 centrada
        # en hace 28 días (un día suelto caía en huecos)
        delta = None
        if p_pub is not None:
            pasada = [v for f, v in pR
                      if 25 <= (hoy_d - dt.date.fromisoformat(f)).days <= 31]
            if len(pasada) >= 2:
                delta = p_pub - round(median(pasada))
        # F5: salto diario máximo reciente (excluye de movers a los acantilados)
        salto = 0
        rec30 = [v for f, v in pR if (hoy_d - dt.date.fromisoformat(f)).days <= 30]
        for a, bb in zip(rec30, rec30[1:]):
            salto = max(salto, abs(bb - a))

        ds = desv_sub.get(b["key"])
        if b["base"] >= 0.90:
            clase = "siempre_activo"
        elif (b["n_inis"] < MIN_INIS or not calibracion_ok or not fresco
              or p_pub is None or (ds is not None and ds > CAL_TOL)):
            clase = "monitoreando"
        else:
            clase = "vivo"
        out_board.append({"key": b["key"], "nombre": b["nombre"], "cat": b["cat"],
                          "clase": clase, "n_inis": b["n_inis"],
                          "p": p_pub if clase == "vivo" else None,
                          "delta": delta if clase == "vivo" else None,
                          "salto_max_30d": salto,
                          "desv_calib": ds,
                          "dato_al": pR[-1][0] if pR else None,   # F9
                          "spark": [v for _, v in pR[-182::7]] if clase == "vivo" else [],
                          "track21": pR[-21:] if clase == "vivo" else []})

    # A4 (Escéptico 17-jul): si el último lote de sesión está PARCIAL en la
    # BD (numeraria viva 69 vs 54 cargadas), los deltas del borde mienten →
    # se suprimen y los destacados salen por NIVEL. Heurística: conteo de la
    # última sesión < 60% de la mediana de las 6 sesiones previas.
    ses = sorted({f for _, f in rows})
    por_ses = defaultdict(int)
    for _, f in rows:
        por_ses[f] += 1
    sesiones = [f for f in ses if por_ses[f] >= 5]
    lote_parcial = False
    if len(sesiones) >= 7:
        prev = sorted(por_ses[f] for f in sesiones[-7:-1])
        med = prev[len(prev) // 2]
        lote_parcial = por_ses[sesiones[-1]] < 0.6 * med
    if lote_parcial:
        for b in out_board:
            b["delta"] = None
    # los destacados SIEMPRE son los que se MUEVEN — nunca "los más altos"
    # (eso re-apilaba todo en 90-97 arriba, el problema original). Con lote
    # parcial el movimiento se mide dentro de la propia ventana de 21 días
    # (rango del track diario), que no depende del delta suprimido.
    def rango21(b):
        vs = [v for _, v in b.get("track21", [])]
        return (max(vs) - min(vs)) if len(vs) >= 5 else 0
    cand = [b for b in out_board if b["clase"] == "vivo" and b["salto_max_30d"] <= SALTO_MAX]
    movers = [b["key"] for b in sorted(
        cand, key=lambda b: (-rango21(b) if lote_parcial else -abs(b["delta"] or 0)))][:5]

    SALIDA.write_text(json.dumps({
        "generado": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "hasta": hoy, "rezago_dias": rezago, "horizonte_dias": H,
        "dedupe_filas": n_dedup, "lote_parcial": lote_parcial,
        "definicion": "Probabilidad de que se presente al menos una iniciativa "
                      "del subtema en los próximos 14 días (frecuencia histórica "
                      "condicionada, recalibrada; mediana de 7 días).",
        "calibracion": {"ok": calibracion_ok, "desviacion_mediana": round(desv_global, 1),
                        "bins": calib},
        "movers": movers,
        "tracks": {k: next(b for b in out_board if b["key"] == k)["track21"] for k in movers},
        "board": out_board,
    }, ensure_ascii=False))
    nv = sum(1 for b in out_board if b["clase"] == "vivo")
    dg = sum(1 for b in out_board if b["clase"] == "monitoreando" and (desv_sub.get(b["key"]) or 0) > CAL_TOL)
    print(f"✅ v2: {len(out_board)} subtemas · {nv} vivos · {dg} degradados por calibración propia · "
          f"dedupe {n_dedup} filas · rezago {rezago}d · global {desv_global:.1f} pts "
          f"{'OK' if calibracion_ok else 'DEGRADADA'} · hasta {hoy}")


if __name__ == "__main__":
    main()
