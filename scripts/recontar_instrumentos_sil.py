"""
Reconteo de instrumentos desde el SIL (fuente de verdad), para corregir los
conteos inflados de la página (que venían de actividad_legislador con efemérides
+ duplicados).

El SIL Búsqueda Avanzada separa bien los tipos (TASUNTO 11=Iniciativa,
19=Proposición con PA), así que un conteo por PRESENTADOR=L da el número limpio
"como promovente" — el que el usuario verifica abriendo senado.gob.mx/SIL.

Uso:
    python scripts/recontar_instrumentos_sil.py            # todos los activos LXVI
    python scripts/recontar_instrumentos_sil.py --ids 545,586   # canario
Salida: eval/reconteo_sil.json  (legislador_id -> {ini, prop})  + cache durable.

NO escribe a D1 (eso va por workflow, donde está el CF token). NO usa Haiku.
"""
from __future__ import annotations
import argparse, json, sys, time, sqlite3, warnings
from pathlib import Path
warnings.filterwarnings("ignore")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from scripts import scrape_instrumentos_historicos as H

H.LEG_NUM_TO_ROMAN[66] = "LXVI"
H.PAGINAS = 400          # una sola página para casi todos
H.DELAY = 0.4

OUT = ROOT / "eval" / "reconteo_sil.json"
OUT_REC = ROOT / "eval" / "instrumentos_sil.json"       # registro por instrumento (para ELO)
CACHE = ROOT / "eval" / "reconteo_sil_cache_v3.json"   # v3: incluye registro por comisión


def _norm(s: str) -> str:
    import unicodedata
    s = (s or "").lower()
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def _bucket(estatus: str) -> str:
    el = _norm(estatus)
    if "aprobad" in el and "no aprob" not in el:
        return "apr"
    if any(k in el for k in ["desech", "rechaz", "preclui", "retir", "no aprob", "negativ", "desestim"]):
        return "des"
    return "pen"


def resolver(sess, sid, nombre, cam):
    import re
    resp = sess.post(
        f"{H.URL_PP_LISTLEG}?SID={sid}&Camara=&Legislatura=66",
        data={"SELECCIONADO": "-123456", "CVE_PALABRAS": _norm(nombre)},
        timeout=30, verify=False,
    )
    resp.encoding = "latin-1"
    toks = set(_norm(nombre).split())
    best, bs = None, -1
    for i, t in re.findall(r"incluir\('(\d+)',\s*'([^']+)'\)", resp.text):
        tt = set(_norm(re.sub(r"\([^)]*\)|\[[^\]]*\]", "", t)).split())
        sc = len(toks & tt) + (0.4 if t.startswith(cam) else 0)
        if sc > bs:
            bs, best = sc, i
    return best


def contar(sess, ref):
    from collections import Counter
    li = H.scrape_legislador_legis(sess, ref, 66, H.TASUNTO_INICIATIVA)
    time.sleep(0.3)
    lp = H.scrape_legislador_legis(sess, ref, 66, H.TASUNTO_PROPOSICION)
    bi = Counter(_bucket(x.get("estatus", "")) for x in li)
    bp = Counter(_bucket(x.get("estatus", "")) for x in lp)
    # Registro por instrumento (para recomputar ELO desde datos limpios del SIL):
    # tipo, comisión a la que se turnó, estatus, fecha.
    recs = ([{"t": "ini", "tit": x.get("denominacion", ""), "com": x.get("turnado_a", ""), "est": x.get("estatus", ""), "f": x.get("fecha_presentacion", "")} for x in li]
            + [{"t": "prop", "tit": x.get("denominacion", ""), "com": x.get("turnado_a", ""), "est": x.get("estatus", ""), "f": x.get("fecha_presentacion", "")} for x in lp])
    return {
        "ini": len(li), "prop": len(lp),
        "ini_apr": bi["apr"], "ini_pen": bi["pen"], "ini_des": bi["des"],
        "prop_apr": bp["apr"], "prop_pen": bp["pen"], "prop_des": bp["des"],
        # agregados útiles para los widgets (Historial + Efectividad)
        "aprobados": bi["apr"] + bp["apr"],
        "pendientes": bi["pen"] + bp["pen"],
        "desechados": bi["des"] + bp["des"],
        "_recs": recs,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ids", help="csv de legislador_id (default: todos activos LXVI)")
    args = ap.parse_args()

    con = sqlite3.connect(str(ROOT / "semaforo.db"))
    con.row_factory = sqlite3.Row
    if args.ids:
        ids = [int(x) for x in args.ids.split(",")]
        q = f"SELECT id, nombre, camara FROM legisladores WHERE id IN ({','.join('?'*len(ids))})"
        rows = con.execute(q, ids).fetchall()
    else:
        rows = con.execute(
            "SELECT DISTINCT l.id, l.nombre, l.camara FROM legisladores l "
            "JOIN actividad_legislador a ON a.legislador_id = l.id"
        ).fetchall()

    cache = json.loads(CACHE.read_text()) if CACHE.exists() else {}
    sess, sid = H.get_session_sid()
    out = {}
    for i, r in enumerate(rows, 1):
        key = str(r["id"])
        if key in cache:
            out[key] = cache[key]; continue
        cam = "Dip" if (r["camara"] or "").startswith("C") and "Diput" in r["camara"] else "Sen"
        ref = resolver(sess, sid, r["nombre"], cam)
        if not ref:
            out[key] = {"ini": None, "prop": None, "nombre": r["nombre"], "error": "no_resuelto"}
        else:
            res = contar(sess, ref)
            res.update({"nombre": r["nombre"], "sil_id": ref})
            out[key] = res
        cache[key] = out[key]
        if i % 10 == 0:
            CACHE.write_text(json.dumps(cache, ensure_ascii=False))
            print(f"  {i}/{len(rows)}…", flush=True)
        time.sleep(0.3)

    CACHE.write_text(json.dumps(cache, ensure_ascii=False))
    # Separar: conteos/estatus (lean) → OUT ; registro por instrumento → OUT_REC
    recs_out = {}
    for k, v in out.items():
        if isinstance(v, dict) and "_recs" in v:
            recs_out[k] = v.pop("_recs")
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    OUT_REC.write_text(json.dumps(recs_out, ensure_ascii=False))
    print(f"Listo: {len(out)} legisladores → {OUT} ; registros → {OUT_REC}")
    for k, v in out.items():
        print(f"  {v.get('nombre','?')}: ini={v.get('ini')} prop={v.get('prop')}")


if __name__ == "__main__":
    main()
