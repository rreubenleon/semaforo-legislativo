"""
Scrape de la BANCADA (suscritas / firmadas en grupo) de cada senador, desde su
perfil oficial en senado.gob.mx. El SIL solo da "como promovente"; el perfil del
Senado da el TOTAL (promovente + suscritas). Entonces:

    bancada = total_perfil − promovente_SIL

Verificable contra lo que cualquiera ve en senado.gob.mx/66/senador/{id}.
Ej. Waldo: perfil ini=124, SIL promovente=62 → bancada=62.

Salida: eval/bancada_senado.json  { legislador_id: {ini_col, prop_col} }
NO escribe a D1 (eso va por workflow). NO usa Haiku. Solo senadores (los
diputados usan otro sitio — pendiente aparte).
"""
from __future__ import annotations
import json, re, sys, time, sqlite3, warnings
from pathlib import Path
warnings.filterwarnings("ignore")
import requests

ROOT = Path(__file__).resolve().parent.parent
RECONTEO = ROOT / "eval" / "reconteo_sil.json"
OUT = ROOT / "eval" / "bancada_senado.json"
CACHE = ROOT / "eval" / "bancada_senado_cache.json"

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"})


def perfil_totales(sitl_id):
    """Devuelve [iniciativas, proposiciones, instrumentos] del perfil, o None."""
    try:
        h = S.get(f"https://www.senado.gob.mx/66/senador/{sitl_id}", timeout=30, verify=False).text
        nums = [int(m.group(1)) for m in re.finditer(r"Resultados encontrados:</strong>\s*(\d+)", h)]
        return nums[:3] if nums else None
    except Exception:
        return None


def main():
    if not RECONTEO.exists():
        print("Falta eval/reconteo_sil.json (corre recontar_instrumentos_sil.py primero)."); return 1
    rc = json.loads(RECONTEO.read_text())
    con = sqlite3.connect(str(ROOT / "semaforo.db"))
    senadores = con.execute(
        "SELECT id, nombre, sitl_id FROM legisladores "
        "WHERE camara='Senado' AND sitl_id IS NOT NULL AND sitl_id!=''"
    ).fetchall()

    cache = json.loads(CACHE.read_text()) if CACHE.exists() else {}
    out = {}
    for i, (lid, nombre, sitl) in enumerate(senadores, 1):
        key = str(lid)
        if key in cache:
            out[key] = cache[key]; continue
        prom = rc.get(key)
        tot = perfil_totales(sitl)
        if not prom or not tot or len(tot) < 2:
            out[key] = {"ini_col": None, "prop_col": None, "nombre": nombre}
        else:
            out[key] = {
                "ini_col": max(0, tot[0] - (prom.get("ini") or 0)),
                "prop_col": max(0, tot[1] - (prom.get("prop") or 0)),
                "perfil_ini": tot[0], "perfil_prop": tot[1], "nombre": nombre,
            }
        cache[key] = out[key]
        if i % 15 == 0:
            CACHE.write_text(json.dumps(cache, ensure_ascii=False)); print(f"  {i}/{len(senadores)}…", flush=True)
        time.sleep(0.5)

    CACHE.write_text(json.dumps(cache, ensure_ascii=False))
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    ok = sum(1 for v in out.values() if v.get("ini_col") is not None)
    print(f"Listo: {len(out)} senadores, {ok} con bancada → {OUT}")
    for k in ("545", "586"):
        if k in out:
            v = out[k]; print(f"  {v['nombre']}: bancada {v.get('ini_col')} ini / {v.get('prop_col')} prop")


if __name__ == "__main__":
    sys.exit(main())
