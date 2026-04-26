"""
Diagnóstico de diputados reelectos LXIV/LXV → LXVI.

Fuente: SITL Diputados (sitl.diputados.gob.mx)
  - http://sitl.diputados.gob.mx/LXIV_leg/listado_diputados_gpnp.php
  - http://sitl.diputados.gob.mx/LXV_leg/listado_diputados_gpnp.php
  - http://sitl.diputados.gob.mx/LXVI_leg/listado_diputados_gpnp.php

Output (NO escribe en BD):
  - eval/reelectos/diputados_LXIV.json
  - eval/reelectos/diputados_LXV.json
  - eval/reelectos/diputados_LXVI.json
  - eval/reelectos/cruce_diputados.json
  - eval/reelectos/reporte_diputados.md

Uso:
    python eval/reelectos/diagnostico_diputados.py
"""
import json
import re
import sys
import time
from collections import Counter
from pathlib import Path

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
from db import get_connection
from scrapers.legisladores import _normalizar_nombre

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}
OUT_DIR = ROOT / "eval" / "reelectos"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def fetch_listado(legislatura: str) -> list[dict]:
    """Devuelve [{nombre, sitl_id, estado, distrito, legislatura}, ...]."""
    url = f"http://sitl.diputados.gob.mx/{legislatura}_leg/listado_diputados_gpnp.php?tipot=TOTAL"
    resp = requests.get(url, headers=HEADERS, timeout=60, verify=False)
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    big_table = next((t for t in soup.find_all("table") if len(t.find_all("tr")) > 100), None)
    if big_table is None:
        raise RuntimeError(f"No encontré tabla grande para {legislatura}")

    out = []
    for tr in big_table.find_all("tr")[1:]:
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue
        link = cells[0].find("a", href=True)
        if not link:
            continue
        nombre_raw = link.get_text(strip=True)
        sitl_match = re.search(r"dipt=(\d+)", link.get("href", ""))
        sitl_id = sitl_match.group(1) if sitl_match else ""
        nombre = re.sub(r"^\d+\s+", "", nombre_raw).strip()
        out.append({
            "legislatura": legislatura,
            "sitl_id": sitl_id,
            "nombre": nombre,
            "nombre_normalizado": _normalizar_nombre(nombre),
            "estado": cells[1].get_text(strip=True) if len(cells) > 1 else "",
            "distrito": cells[2].get_text(strip=True) if len(cells) > 2 else "",
        })
    return out


def main():
    print("\n  ═══ Diagnóstico de diputados reelectos (sin tocar BD) ═══\n")

    # 1) Scrape los tres rosters (con caché en disco)
    rosters = {}
    for leg in ("LXIV", "LXV", "LXVI"):
        cache = OUT_DIR / f"diputados_{leg}.json"
        if cache.exists():
            rosters[leg] = json.loads(cache.read_text())
            print(f"  · {leg}: {len(rosters[leg])} (cache)")
        else:
            print(f"  · {leg}: descargando…", end=" ", flush=True)
            rosters[leg] = fetch_listado(leg)
            cache.write_text(json.dumps(rosters[leg], ensure_ascii=False, indent=2))
            print(f"{len(rosters[leg])}")
            time.sleep(0.8)

    # 2) Detectar duplicados de nombre normalizado dentro de cada legislatura
    for leg, lista in rosters.items():
        norms = [d["nombre_normalizado"] for d in lista]
        dups = {n: c for n, c in Counter(norms).items() if c > 1}
        if dups:
            print(f"  ⚠ {leg} tiene {len(dups)} nombres normalizados repetidos: {list(dups.items())[:3]}")

    # 3) Index por nombre normalizado
    idx = {leg: {d["nombre_normalizado"]: d for d in lista} for leg, lista in rosters.items()}

    # 4) Cruce: para cada LXVI, marcar LXIV/LXV
    cruce = []
    for d in rosters["LXVI"]:
        n = d["nombre_normalizado"]
        en_lxiv = idx["LXIV"].get(n)
        en_lxv = idx["LXV"].get(n)
        if not (en_lxiv or en_lxv):
            continue
        legislaturas = []
        if en_lxiv: legislaturas.append("LXIV")
        if en_lxv: legislaturas.append("LXV")
        legislaturas.append("LXVI")
        cruce.append({
            "nombre": d["nombre"],
            "nombre_normalizado": n,
            "sitl_id_lxvi": d["sitl_id"],
            "estado_lxvi": d["estado"],
            "distrito_lxvi": d["distrito"],
            "legislaturas_diputado": legislaturas,
            "marcador": f"Activo en {' y '.join(legislaturas[:-1])}" if len(legislaturas) > 1 else None,
            "lxiv": en_lxiv,
            "lxv": en_lxv,
        })

    cruce_path = OUT_DIR / "cruce_diputados.json"
    cruce_path.write_text(json.dumps(cruce, ensure_ascii=False, indent=2))

    # 5) Cruce contra BD (legisladores LXVI activos)
    conn = get_connection()
    db_lxvi = {row[0] for row in conn.execute(
        "SELECT nombre_normalizado FROM legisladores WHERE camara='Cámara de Diputados'"
    ).fetchall()}

    en_bd = sum(1 for r in cruce if r["nombre_normalizado"] in db_lxvi)
    fuera_bd = sum(1 for r in cruce if r["nombre_normalizado"] not in db_lxvi)

    # 6) Resumen
    n_lxvi = len(rosters["LXVI"])
    n_solo_lxv = sum(1 for r in cruce if "LXIV" not in r["legislaturas_diputado"] and "LXV" in r["legislaturas_diputado"])
    n_solo_lxiv = sum(1 for r in cruce if "LXIV" in r["legislaturas_diputado"] and "LXV" not in r["legislaturas_diputado"])
    n_ambas = sum(1 for r in cruce if "LXIV" in r["legislaturas_diputado"] and "LXV" in r["legislaturas_diputado"])

    print()
    print(f"  Roster SITL:")
    print(f"    LXIV:  {len(rosters['LXIV'])}")
    print(f"    LXV:   {len(rosters['LXV'])}")
    print(f"    LXVI:  {len(rosters['LXVI'])}")
    print(f"  BD legisladores LXVI Diputados: {len(db_lxvi)}")
    print()
    print(f"  Reelectos detectados: {len(cruce)} ({100*len(cruce)/n_lxvi:.1f}% de LXVI)")
    print(f"    Activos en LXIV y LXV (3 periodos):  {n_ambas}")
    print(f"    Activos en LXV (2 periodos consecutivos): {n_solo_lxv}")
    print(f"    Activos solo en LXIV (saltaron LXV):  {n_solo_lxiv}")
    print()
    print(f"  Match contra BD legisladores:")
    print(f"    Reelectos que SÍ están en BD:   {en_bd}")
    print(f"    Reelectos NO encontrados en BD: {fuera_bd}")

    # 7) Reporte markdown
    md = OUT_DIR / "reporte_diputados.md"
    lines = [
        "# Diagnóstico — Diputados reelectos LXIV/LXV → LXVI",
        "",
        f"Fuente: SITL Diputados (sitl.diputados.gob.mx)",
        f"Sin escrituras en BD. Solo diagnóstico.",
        "",
        "## Conteos",
        "",
        f"- LXIV: **{len(rosters['LXIV'])}** propietarios",
        f"- LXV:  **{len(rosters['LXV'])}** propietarios",
        f"- LXVI: **{len(rosters['LXVI'])}** propietarios",
        f"- BD `legisladores` (Diputados activos LXVI, incluye suplentes en funciones): **{len(db_lxvi)}**",
        "",
        f"## Reelectos en LXVI",
        "",
        f"- **Total reelectos**: {len(cruce)} ({100*len(cruce)/n_lxvi:.1f}% del roster LXVI)",
        f"- 3 periodos (LXIV+LXV+LXVI): **{n_ambas}**",
        f"- 2 periodos consecutivos (LXV+LXVI): **{n_solo_lxv}**",
        f"- Saltaron una legislatura (LXIV+LXVI, sin LXV): **{n_solo_lxiv}**",
        "",
        f"## Match con BD",
        "",
        f"- Reelectos que coinciden con `legisladores`: **{en_bd}**",
        f"- Reelectos NO encontrados en BD (probable propietario sustituido por suplente): **{fuera_bd}**",
        "",
        "## Top 30 reelectos con 3 periodos (LXIV+LXV+LXVI)",
        "",
        "| Nombre LXVI | Estado | Distrito | LXIV estado | LXV estado |",
        "|---|---|---|---|---|",
    ]
    treped = [r for r in cruce if "LXIV" in r["legislaturas_diputado"] and "LXV" in r["legislaturas_diputado"]]
    for r in treped[:30]:
        lines.append(
            f"| {r['nombre']} | {r['estado_lxvi']} | {r['distrito_lxvi']} | "
            f"{r['lxiv']['estado'] if r['lxiv'] else ''} | {r['lxv']['estado'] if r['lxv'] else ''} |"
        )

    lines += [
        "",
        "## Notas",
        "",
        "- El cruce solo se hace por **nombre normalizado** (lower + sin acentos + sin prefijos).",
        "- No considera cambio de cámara (Diputado→Senador). Para eso hace falta scrapear Senado LXIV/LXV (pendiente: senado.gob.mx redirige en bucle, requiere otro path).",
        "- Casos como **Alejandro Moreno** (Diputado LXV → Senador LXVI) NO aparecen aquí porque no está en el roster de Diputados LXVI.",
        "- Casos como **Rubén Moreira** (Diputado LXIV → LXV → LXVI) sí aparecen.",
        "- Caso **Manuel Añorve** (Senador 2018→presente) requiere scrape de Senado para detectarlo.",
    ]
    md.write_text("\n".join(lines))

    print(f"\n  ✓ Output:")
    print(f"    {cruce_path.relative_to(ROOT)}")
    print(f"    {md.relative_to(ROOT)}")
    print()


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()
