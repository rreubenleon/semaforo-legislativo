"""
Conteos de DIPUTADOS desde el SITL (sitl.diputados.gob.mx) — el sistema oficial
de la Cámara de Diputados, verificable por cualquiera. Es la fuente de verdad
para diputados (paralelo a senado.gob.mx para senadores). NO el SIL, que para
diputados da números distintos (Raymundo: SIL 17 vs SITL 10 iniciativas).

Por diputado, sumando todos los periodos (pert):
  - INICIATIVAS: tabla resumen "Tipo de presentación" →
      solo = Iniciante (+Promovente) ; bancada = Adherente + De Grupo
  - PROPOSICIONES: lista de detalle "presentadas por el diputado" →
      solo = # de proposiciones ; bancada = 0 (el SITL no expone suscritas aquí)

Salida: eval/bancada_diputados.json
  { legislador_id: {ini_solo, ini_col, prop_solo, prop_col} }
NO escribe a D1 (va por workflow). NO usa Haiku.
"""
from __future__ import annotations
import json, sys, time, sqlite3, warnings
from pathlib import Path
warnings.filterwarnings("ignore")
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "eval" / "bancada_diputados.json"
CACHE = ROOT / "eval" / "bancada_diputados_cache.json"
BASE = "http://sitl.diputados.gob.mx/LXVI_leg"


def _perts_validos(dipt, tipo):
    """Lee del shell los periodos (pert) que el diputado realmente tiene.
    Sumar un rango ciego subcuenta/duplica; el shell da el set exacto."""
    import re
    page = "iniciativas" if tipo == "ini" else "proposiciones"
    try:
        h = S.get(f"{BASE}/{page}_diputados_xperiodonplxvi.php?dipt={dipt}", timeout=30, verify=False).text
    except Exception:
        return []
    return sorted(set(int(m) for m in re.findall(
        rf"{page}_por_pernplxvi\.php\?iddipt={dipt}&pert=(\d+)", h)))

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"})


def _ini_por_tipo(dipt, pert):
    """Tabla resumen de iniciativas: {Iniciante, Adherente, De Grupo, Promovente} → Total."""
    try:
        h = S.get(f"{BASE}/iniciativas_por_pernplxvi.php?iddipt={dipt}&pert={pert}", timeout=30, verify=False).text
    except Exception:
        return {}
    soup = BeautifulSoup(h, "html.parser")
    out = {}
    for t in soup.find_all("table"):
        rows = t.find_all("tr")
        if not rows:
            continue
        head = " ".join(c.get_text(" ", strip=True) for c in rows[0].find_all(["td", "th"]))
        if "Tipo de presentaci" not in head:
            continue
        for tr in rows:
            cells = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
            if cells and cells[0] in ("Iniciante", "Promovente", "Adherente", "De Grupo"):
                try:
                    out[cells[0]] = int(cells[1])
                except (ValueError, IndexError):
                    pass
        break
    return out


def _prop_count(dipt, pert):
    """Cuenta proposiciones (detalle 'presentadas por el diputado')."""
    try:
        h = S.get(f"{BASE}/proposiciones_por_pernplxvi.php?iddipt={dipt}&pert={pert}", timeout=30, verify=False).text
    except Exception:
        return 0
    soup = BeautifulSoup(h, "html.parser")
    n = 0
    for t in soup.find_all("table"):
        rows = t.find_all("tr")
        if rows and rows[0].get_text(" ", strip=True).startswith("PROPOSICI"):
            for tr in rows[1:]:
                cells = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
                if cells and cells[0] and cells[0][0].isdigit():
                    n += 1
    return n


def main():
    con = sqlite3.connect(str(ROOT / "semaforo.db"))
    dips = con.execute(
        "SELECT id, nombre, sitl_id FROM legisladores "
        "WHERE camara='Cámara de Diputados' AND sitl_id IS NOT NULL AND sitl_id!=''"
    ).fetchall()
    cache = json.loads(CACHE.read_text()) if CACHE.exists() else {}
    out = {}
    for i, (lid, nombre, sitl) in enumerate(dips, 1):
        key = str(lid)
        if key in cache:
            out[key] = cache[key]; continue
        ini = {"Iniciante": 0, "Promovente": 0, "Adherente": 0, "De Grupo": 0}
        for pert in _perts_validos(sitl, "ini"):
            for k, v in _ini_por_tipo(sitl, pert).items():
                ini[k] = ini.get(k, 0) + v
            time.sleep(0.2)
        prop = 0
        for pert in _perts_validos(sitl, "prop"):
            prop += _prop_count(sitl, pert)
            time.sleep(0.2)
        out[key] = {
            "ini_solo": ini["Iniciante"] + ini["Promovente"],
            "ini_col": ini["Adherente"] + ini["De Grupo"],
            "prop_solo": prop,
            "prop_col": 0,
            "nombre": nombre,
        }
        cache[key] = out[key]
        if i % 10 == 0:
            CACHE.write_text(json.dumps(cache, ensure_ascii=False)); print(f"  {i}/{len(dips)}…", flush=True)

    CACHE.write_text(json.dumps(cache, ensure_ascii=False))
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"Listo: {len(out)} diputados → {OUT}")
    for k in ("232", "79"):
        if k in out:
            v = out[k]; print(f"  {v['nombre']}: ini {v['ini_solo']}+{v['ini_col']} | prop {v['prop_solo']}")


if __name__ == "__main__":
    sys.exit(main())
