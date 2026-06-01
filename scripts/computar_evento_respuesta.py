"""Pieza 2 — Evento→Respuesta por sub-categoría → D1.

Para cada sub-tema, sobre histórico sep2024→hoy:
  - Detecta picos mediáticos (z>=1.5 vs ritmo del periodo ordinario)
  - Mide P(respuesta legislativa SOBRE lo normal en 21d tras el pico)
  - Identifica qué partido responde más

Esto es la base predictiva del Trigger Desk / consola: "ante un evento
en este sub-tema, hay X% de que el Congreso responda, típicamente del
partido Y". Validado vs realidad LXVI.

Escribe tabla `evento_respuesta_subcat` a SQLite local; sync a D1 con
--sync (requiere CLOUDFLARE_API_TOKEN, solo en CI).

NO usa Haiku. NO toca front-end. Idempotente (DROP+CREATE).

Uso:
    python3 scripts/computar_evento_respuesta.py            # local solo
    python3 scripts/computar_evento_respuesta.py --sync     # + D1 (CI)
    python3 scripts/computar_evento_respuesta.py --dry-run  # no escribe
"""
from __future__ import annotations
import argparse, json, re, sys, subprocess
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from db import get_connection
from config import CATEGORIAS

INI = "2024-09-01"
Z, VENT, MIN_PICOS = 1.5, 21, 5
D1_DB = "fiat-busqueda"


def es_receso_mes(f):
    m = int(f[5:7])
    return m in (5, 6, 7, 8) or m == 1


def mk(kws):
    o = []
    for k in kws:
        kl = k.lower()
        o.append(("wb", re.compile(r"(?<![a-záéíóúñ])" + re.escape(kl) + r"(?![a-záéíóúñ])"))
                  if len(k) <= 4 else ("s", kl))
    return o


def mt(t, ms):
    for tp, m in ms:
        if tp == "s" and m in t:
            return True
        if tp == "wb" and m.search(t):
            return True
    return False


def computar(conn):
    fin = datetime.now().strftime("%Y-%m-%d")
    arts = conn.execute(
        "SELECT LOWER(titulo||' '||IFNULL(resumen,'')), substr(fecha,1,10), LOWER(IFNULL(categorias,'')) "
        "FROM articulos WHERE fecha>=? AND fecha<=?", (INI, fin)).fetchall()
    docs = conn.execute(
        "SELECT LOWER(titulo||' '||IFNULL(sinopsis,'')), substr(fecha_presentacion,1,10), partido, LOWER(IFNULL(categoria,'')) "
        "FROM sil_documentos WHERE tipo_grupo IN ('Iniciativa','Proposición con Punto de Acuerdo','Proposición con PA') "
        "AND fecha_presentacion>=?", (INI,)).fetchall()
    d0 = datetime.strptime(INI, "%Y-%m-%d"); d1 = datetime.strptime(fin, "%Y-%m-%d")
    N = (d1 - d0).days + 1
    fechas = [(d0 + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(N)]
    fidx = {f: i for i, f in enumerate(fechas)}
    ord_mask = np.array([not es_receso_mes(f) for f in fechas])

    filas = []
    for cat, cfg in CATEGORIAS.items():
        subs = cfg.get("subcategorias", {})
        if not isinstance(subs, dict):
            continue
        catl = cat.lower()
        arts_p = [(t, f) for t, f, cc in arts if catl in cc]
        docs_p = [(t, f, p) for t, f, p, cc in docs if catl in cc]
        for sk, sc in subs.items():
            kws = sc.get("keywords", []) if isinstance(sc, dict) else []
            if not kws:
                continue
            ms = mk(kws)
            media = np.zeros(N); inst = np.zeros(N); inst_part = defaultdict(list)
            for t, f in arts_p:
                if f in fidx and mt(t, ms):
                    media[fidx[f]] += 1
            for t, f, p in docs_p:
                if f in fidx and mt(t, ms):
                    inst[fidx[f]] += 1
                    inst_part[f].append(p or "?")
            win = np.array([inst[i + 1:i + 1 + VENT].sum() for i in range(N)])
            vals = media[ord_mask]
            if vals.std() == 0 or len(vals) < 20:
                continue
            mu, sd = vals.mean(), vals.std()
            normal = float(np.median(win[ord_mask]))
            pk_hit = pk_tot = 0; partidos = defaultdict(int)
            for i in range(N - VENT):
                if not ord_mask[i] or media[i] == 0 or (media[i] - mu) / sd < Z:
                    continue
                pk_tot += 1
                if win[i] > normal:
                    pk_hit += 1
                    for k in range(1, VENT + 1):
                        if i + k < N:
                            for p in inst_part.get(fechas[i + k], []):
                                partidos[p] += 1
            if pk_tot < MIN_PICOS:
                continue
            top = [(p, n) for p, n in sorted(partidos.items(), key=lambda x: -x[1]) if p != "?"][:3]
            filas.append({
                "subcat": f"{cat}/{sk}",
                "categoria": cat,
                "subcategoria": sk,
                "p_respuesta": round(pk_hit / pk_tot, 3),
                "n_picos": pk_tot,
                "ritmo_normal_21d": normal,
                "partido_1": top[0][0] if len(top) > 0 else "",
                "partido_1_n": top[0][1] if len(top) > 0 else 0,
                "partido_2": top[1][0] if len(top) > 1 else "",
                "partido_2_n": top[1][1] if len(top) > 1 else 0,
            })
    return filas


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sync", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    conn = get_connection()
    filas = computar(conn)
    print(f"Sub-temas computados: {len(filas)}")
    # validación contra referencia conocida
    ref = {f["subcat"]: f for f in filas}
    for k in ["medio_ambiente/conservacion_biodiversidad", "electoral_politico/reforma_electoral"]:
        if k in ref:
            f = ref[k]
            print(f"  CHECK {k}: p={f['p_respuesta']} n={f['n_picos']} top={f['partido_1']}({f['partido_1_n']})")

    if args.dry_run:
        print("[DRY-RUN] no se escribe")
        return

    # Escribir a SQLite local
    conn.execute("DROP TABLE IF EXISTS evento_respuesta_subcat")
    conn.execute("""CREATE TABLE evento_respuesta_subcat(
        subcat TEXT PRIMARY KEY, categoria TEXT, subcategoria TEXT,
        p_respuesta REAL, n_picos INTEGER, ritmo_normal_21d REAL,
        partido_1 TEXT, partido_1_n INTEGER, partido_2 TEXT, partido_2_n INTEGER,
        fecha_calculo TEXT)""")
    ahora = datetime.now().isoformat()
    for f in filas:
        conn.execute("INSERT INTO evento_respuesta_subcat VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                     (f["subcat"], f["categoria"], f["subcategoria"], f["p_respuesta"],
                      f["n_picos"], f["ritmo_normal_21d"], f["partido_1"], f["partido_1_n"],
                      f["partido_2"], f["partido_2_n"], ahora))
    conn.commit()
    print(f"Escritas {len(filas)} filas a SQLite local (tabla evento_respuesta_subcat)")

    if args.sync:
        # Generar SQL para D1
        sql = ["DROP TABLE IF EXISTS evento_respuesta_subcat;",
               """CREATE TABLE evento_respuesta_subcat(subcat TEXT PRIMARY KEY, categoria TEXT,
                subcategoria TEXT, p_respuesta REAL, n_picos INTEGER, ritmo_normal_21d REAL,
                partido_1 TEXT, partido_1_n INTEGER, partido_2 TEXT, partido_2_n INTEGER, fecha_calculo TEXT);"""]
        for f in filas:
            def esc(v): return "'" + str(v).replace("'", "''") + "'" if isinstance(v, str) else str(v)
            sql.append(f"INSERT INTO evento_respuesta_subcat VALUES ({esc(f['subcat'])},{esc(f['categoria'])},"
                       f"{esc(f['subcategoria'])},{f['p_respuesta']},{f['n_picos']},{f['ritmo_normal_21d']},"
                       f"{esc(f['partido_1'])},{f['partido_1_n']},{esc(f['partido_2'])},{f['partido_2_n']},{esc(ahora)});")
        sqlfile = ROOT / "worker" / "_evento_respuesta.sql"
        sqlfile.write_text("\n".join(sql), encoding="utf-8")
        r = subprocess.run(["npx", "wrangler", "d1", "execute", D1_DB, "--remote",
                            "--file", str(sqlfile)], capture_output=True, text=True, cwd=str(ROOT / "worker"))
        print("D1 sync:", "OK" if r.returncode == 0 else "ERROR " + r.stderr[:300])


if __name__ == "__main__":
    main()
