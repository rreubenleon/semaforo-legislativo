"""
Reactividad legislativa = COBERTURA DE LA AGENDA mediática.

NO es una tasa sobre lo que el legislador presentó (eso premiaba la pureza de
outputs chiquitos: 3/3 = alto aunque toque casi nada). Es PRESENCIA en la
coyuntura: de los picos mediáticos del periodo, ¿a cuántos respondió?

Agenda = todos los EVENTOS mediáticos (días con z>=1.5 vs ritmo del periodo
ordinario, días consecutivos colapsados) por subcategoría (config.CATEGORIAS).
Cobertura del legislador = eventos distintos que cubrió, donde "cubrió" = filtró
una proposición con punto de acuerdo del MISMO subtema dentro de 21d tras el
inicio del evento. También se cuentan los temas (subcats) distintos tocados.

reactividad (0-100) = eventos cubiertos escalados al LÍDER del conjunto
calculado. Mientras el padrón completo esté bloqueado por el timeout del
reconteo, el líder es la cohorte; se sube a percentil-padrón al destrabar.
Los conteos crudos (cobertura_eventos/cobertura_temas) son absolutos y no
dependen de la escala.

Fuente LIMPIA: eval/instrumentos_sil.json (con títulos). NUNCA sil_documentos.
Solo proposiciones (las iniciativas son estructurales). Salida: eval/reactividad.json.
"""
from __future__ import annotations
import json, re, sys, unicodedata
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
import sqlite3
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from config import CATEGORIAS

INSTR = ROOT / "eval" / "instrumentos_sil.json"
OUT = ROOT / "eval" / "reactividad.json"

INI = "2024-09-01"
Z = 1.5          # umbral de pico (z-score vs ritmo ordinario)
VENT = 21        # ventana de respuesta tras el inicio del evento (días)
GAP = 7          # picos a <=7d se funden en el mismo evento


def norm(s):
    s = (s or "").lower()
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def es_receso_mes(f):
    m = int(f[5:7]); return m in (5, 6, 7, 8) or m == 1


def mk(kws):
    o = []
    for k in kws:
        kl = norm(k)
        o.append(("wb", re.compile(r"(?<![a-z])" + re.escape(kl) + r"(?![a-z])")) if len(k) <= 4 else ("s", kl))
    return o


def mt(t, ms):
    for tp, m in ms:
        if tp == "s" and m in t: return True
        if tp == "wb" and m.search(t): return True
    return False


def construir_agenda(con):
    """Devuelve (agenda, fidx): agenda[subcat] = [(ini_idx, fin_idx)] de eventos."""
    fin = datetime.now().strftime("%Y-%m-%d")
    arts = con.execute(
        "SELECT LOWER(titulo||' '||IFNULL(resumen,'')), substr(fecha,1,10), LOWER(IFNULL(categorias,'')) "
        "FROM articulos WHERE fecha>=? AND fecha<=?", (INI, fin)).fetchall()
    d0 = datetime.strptime(INI, "%Y-%m-%d"); d1 = datetime.strptime(fin, "%Y-%m-%d")
    N = (d1 - d0).days + 1
    fechas = [(d0 + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(N)]
    fidx = {f: i for i, f in enumerate(fechas)}
    ord_mask = np.array([not es_receso_mes(f) for f in fechas])
    arts_norm = [(norm(t), f, cc) for t, f, cc in arts]

    submatch = {}
    for cat, cfg in CATEGORIAS.items():
        subs = cfg.get("subcategorias", {}) if isinstance(cfg, dict) else {}
        if not isinstance(subs, dict): continue
        for sk, sc in subs.items():
            kws = sc.get("keywords", []) if isinstance(sc, dict) else []
            if kws: submatch[f"{cat}/{sk}"] = (norm(cat), mk(kws))

    agenda = defaultdict(list)
    for subcat, (catl, ms) in submatch.items():
        media = np.zeros(N)
        for t, f, cc in arts_norm:
            if catl in cc and f in fidx and mt(t, ms):
                media[fidx[f]] += 1
        vals = media[ord_mask]
        if vals.std() == 0 or len(vals) < 20: continue
        mu, sd = vals.mean(), vals.std()
        dias = [i for i in range(N - VENT) if ord_mask[i] and media[i] > 0 and (media[i] - mu) / sd >= Z]
        for d in dias:
            if agenda[subcat] and d - agenda[subcat][-1][1] <= GAP:
                agenda[subcat][-1] = (agenda[subcat][-1][0], d)
            else:
                agenda[subcat].append((d, d))
    return agenda, fidx, submatch


VINCULOS = ROOT / "eval" / "vinculos_produccion.json"


def cobertura_vinculos(con, agenda, fidx, submatch):
    """Eventos cubiertos según vínculos CONFIRMADOS evento→instrumento (juez,
    eval/vinculos_produccion.json). La NOTA identifica el evento: subcat cuyo
    keyword matchea la nota + ventana del pico que contiene su fecha.

    Filtro anti-ruido (caso Cervantes, validado 3-jul): el subcat también debe
    ser coherente con el INSTRUMENTO — misma categoría padre que la categoría
    del instrumento en actividad_legislador, o keywords del subcat presentes en
    el título del instrumento. Sin esto, una palabra suelta en la nota mete
    subtemas ajenos (Sedena/'El Mencho' sumaba agua_saneamiento).

    Devuelve {lid_str: set[(subcat, ev_i)]} para unir con la cobertura keyword.
    """
    if not VINCULOS.exists():
        return {}
    extra = defaultdict(set)
    for x in json.loads(VINCULOS.read_text()).get("vinculos", []):
        rows = con.execute(
            "SELECT al.legislador_id, al.categoria FROM actividad_legislador al "
            "JOIN sil_documentos sd ON sd.id = al.sil_documento_id "
            "WHERE sd.seguimiento_id = ? AND al.legislador_id IS NOT NULL",
            (x["sil_id"],)).fetchall()
        if not rows:
            continue
        ni = fidx.get(x["nota_fecha"])
        if ni is None:
            continue
        tn = norm(x["nota_titulo"]); ti = norm(x.get("titulo", ""))
        for subcat, (catl, ms) in submatch.items():
            if not mt(tn, ms):
                continue
            padre = subcat.split("/")[0]
            eventos = [ev_i for ev_i, (s0, s1) in enumerate(agenda.get(subcat, []))
                       if s0 - 2 <= ni <= s1 + 3]
            if not eventos:
                continue
            for lid, cat_instr in rows:
                if (cat_instr or "") != padre and not mt(ti, ms):
                    continue  # anti-ruido: subcat ajeno al instrumento
                for ev_i in eventos:
                    extra[str(lid)].add((subcat, ev_i))
    return extra


def main():
    if not INSTR.exists():
        print(f"Falta {INSTR} (corre recontar_instrumentos_sil.py con títulos)."); return 1
    instr = json.loads(INSTR.read_text())
    sample = next((v for v in instr.values() if v), [])
    if sample and "tit" not in sample[0]:
        print("instrumentos_sil.json aún sin títulos — re-corre el reconteo primero."); return 1

    con = sqlite3.connect(str(ROOT / "semaforo.db"))
    agenda, fidx, submatch = construir_agenda(con)
    total_eventos = sum(len(v) for v in agenda.values())
    extra_vinc = cobertura_vinculos(con, agenda, fidx, submatch)

    crudo = {}
    for lid, recs in instr.items():
        props = [r for r in recs if r.get("t") == "prop" and r.get("tit") and r.get("f")]
        cubiertos = set(); temas = set()
        for r in props:
            t = norm(r["tit"]); pi = fidx.get(r["f"][:10])
            if pi is None: continue
            for subcat, (catl, ms) in submatch.items():
                if not mt(t, ms): continue
                for ev_i, (s0, s1) in enumerate(agenda.get(subcat, [])):
                    if s0 <= pi <= s1 + VENT:
                        cubiertos.add((subcat, ev_i)); temas.add(subcat)
        # unir eventos confirmados por vínculo (juez) — solo SUMA lo verificado
        cubiertos |= extra_vinc.get(lid, set())
        temas |= {sc for sc, _ in extra_vinc.get(lid, set())}
        crudo[lid] = {"ppa_total": len(props), "cobertura_eventos": len(cubiertos),
                      "cobertura_temas": len(temas)}

    # Escala 0-100 contra el líder (máx eventos) del conjunto calculado.
    lider = max((v["cobertura_eventos"] for v in crudo.values()), default=0) or 1
    out = {}
    for lid, v in crudo.items():
        # score si hay proposiciones O cobertura confirmada por vínculos
        # (un legislador sin PPAs pero con iniciativas vinculadas a eventos
        # confirmados SÍ tiene reactividad medible)
        medible = v["ppa_total"] or v["cobertura_eventos"]
        score = round(100 * v["cobertura_eventos"] / lider) if medible else None
        out[lid] = {**v, "reactividad": score}

    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"Agenda: {total_eventos} eventos. Líder cobertura: {lider} eventos.")
    print(f"Reactividad (cobertura) calculada: {len(out)} legisladores → {OUT}")
    for k in ("545", "592", "590", "566"):
        if k in out:
            print(f"  {k}: {out[k]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
