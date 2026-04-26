"""
Diagnóstico UNIFICADO de legisladores reelectos LXIV/LXV → LXVI.

Fuente única: SIL Gobernación Numeralia
  https://sil.gobernacion.gob.mx/Numeralia/Legisladores/resultadosNumeraliaLegisladores.php
    ?SID=&Legislatura={64|65|66}&Camara={1=Diputados, 2=Senado}

Ventajas vs SITL:
  - Misma fuente para Diputados y Senado
  - Trae partido, entidad, tipo de elección, fecha de protesta
  - Permite detectar cambios de cámara (Diputado LXIV → Senador LXVI, etc.)

Output (NO escribe en BD):
  - eval/reelectos/roster_{LEG}_{Diputados|Senado}.json
  - eval/reelectos/diagnostico_unificado.json   ← reporte completo
  - eval/reelectos/reporte_unificado.md         ← human-readable

Uso:
    .venv/bin/python eval/reelectos/diagnostico_unificado.py
"""
import json
import re
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import requests
import urllib3
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
from db import get_connection

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}
OUT_DIR = ROOT / "eval" / "reelectos"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CAMARA_MAP = {1: "Diputados", 2: "Senado"}
LEG_MAP = {64: "LXIV", 65: "LXV", 66: "LXVI"}


# ─────────────────────────────────────────────────────────────────────
# Normalización
# ─────────────────────────────────────────────────────────────────────
def _split_camelcase(s: str) -> str:
    """SIL Numeralia trae nombres pegados: 'Rocío AdrianaAbreuArtiñano'.
    Inserta espacio antes de mayúscula que sigue a minúscula/acento."""
    return re.sub(r"(?<=[a-záéíóúñ])(?=[A-ZÁÉÍÓÚÑ])", " ", s)


def _normalizar(nombre: str) -> str:
    """'Dip. José Elías Lixa Abimerhi (PAN)' → 'jose elias lixa abimerhi'."""
    nombre = _split_camelcase(nombre)
    nombre = re.sub(r"^(Dip\.|Sen\.|C\.|Diputad[oa]|Senador[a]?)\s*", "", nombre.strip(), flags=re.I)
    nombre = re.sub(r"\s*\([^)]*\)\s*", "", nombre)
    nombre = unicodedata.normalize("NFKD", nombre.lower())
    nombre = "".join(c for c in nombre if not unicodedata.combining(c))
    nombre = re.sub(r"[^a-z\s]", "", nombre)
    nombre = re.sub(r"\s+", " ", nombre).strip()
    return nombre


# ─────────────────────────────────────────────────────────────────────
# Scraper SIL Numeralia
# ─────────────────────────────────────────────────────────────────────
def fetch_roster(leg_id: int, cam_id: int) -> list[dict]:
    """Devuelve lista de propietarios + suplentes en función para un (legislatura, cámara)."""
    url = (
        "https://sil.gobernacion.gob.mx/Numeralia/Legisladores/"
        f"resultadosNumeraliaLegisladores.php?SID=&Legislatura={leg_id}&Camara={cam_id}"
    )
    resp = requests.get(url, headers=HEADERS, timeout=60, verify=False)
    # SIL sirve latin-1 con páginas pesadas
    resp.encoding = "latin-1"
    soup = BeautifulSoup(resp.text, "html.parser")

    # Tabla grande = la del listado
    big = max(soup.find_all("table"), key=lambda t: len(t.find_all("tr")), default=None)
    if big is None:
        raise RuntimeError(f"No hay tablas en {url}")

    rows = big.find_all("tr")
    if len(rows) < 50:
        raise RuntimeError(f"Tabla demasiado chica para {url}")

    out = []
    for tr in rows[2:]:  # saltar header + fila vacía de orderBy
        cells = tr.find_all("td")
        if len(cells) < 10:
            continue
        nombre_raw = cells[2].get_text(" ", strip=True)
        if not nombre_raw:
            continue
        nombre = _split_camelcase(nombre_raw).strip()
        # Quitar dobles espacios introducidos por split
        nombre = re.sub(r"\s+", " ", nombre)
        out.append({
            "legislatura": LEG_MAP[leg_id],
            "camara": CAMARA_MAP[cam_id],
            "nombre": nombre,
            "nombre_normalizado": _normalizar(nombre_raw),
            "instancia": cells[3].get_text(strip=True),
            "partido": cells[4].get_text(strip=True),
            "tipo_eleccion": cells[5].get_text(strip=True),
            "entidad": cells[6].get_text(strip=True),
            "fecha_protesta": cells[7].get_text(strip=True),
            "en_funciones": cells[8].get_text(strip=True),
            "suplente": cells[9].get_text(" ", strip=True) if len(cells) > 9 else "",
        })
    return out


# ─────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────
def main():
    print("\n  ═══ Diagnóstico unificado de legisladores reelectos ═══")
    print("  Fuente: SIL Gobernación Numeralia")
    print("  (no se escribe en BD, solo diagnóstico)\n")

    rosters = {}  # (leg_str, cam_str) -> list[dict]
    for leg_id, leg_str in LEG_MAP.items():
        for cam_id, cam_str in CAMARA_MAP.items():
            cache = OUT_DIR / f"roster_{leg_str}_{cam_str}.json"
            key = (leg_str, cam_str)
            if cache.exists():
                rosters[key] = json.loads(cache.read_text())
                print(f"  · {leg_str} {cam_str:<9}: {len(rosters[key]):>4} (cache)")
            else:
                print(f"  · {leg_str} {cam_str:<9}: descargando…", end=" ", flush=True)
                rosters[key] = fetch_roster(leg_id, cam_id)
                cache.write_text(json.dumps(rosters[key], ensure_ascii=False, indent=2))
                print(f"{len(rosters[key])}")
                time.sleep(0.6)

    # ─────────────────────────────────────────────────────────
    # Construir índice: nombre_normalizado → set de (leg, cam) en los que aparece
    # ─────────────────────────────────────────────────────────
    apariciones = defaultdict(list)  # nombre_norm → [{leg, cam, nombre, partido, ...}]
    for (leg, cam), lista in rosters.items():
        for d in lista:
            apariciones[d["nombre_normalizado"]].append(d)

    # ─────────────────────────────────────────────────────────
    # Para cada legislador LXVI (Diputados o Senado), reportar trayectoria
    # ─────────────────────────────────────────────────────────
    lxvi_dip = rosters[("LXVI", "Diputados")]
    lxvi_sen = rosters[("LXVI", "Senado")]
    lxvi_all = lxvi_dip + lxvi_sen

    reelectos = []
    for d in lxvi_all:
        nn = d["nombre_normalizado"]
        if not nn:
            continue
        trayectoria = apariciones[nn]
        legs_apariciones = {(t["legislatura"], t["camara"]) for t in trayectoria}
        # Quitar la aparición LXVI actual para ver historia previa
        previas = legs_apariciones - {(d["legislatura"], d["camara"])}
        # ¿Aparecía en LXIV o LXV en cualquier cámara?
        previas_validas = {(l, c) for (l, c) in previas if l in ("LXIV", "LXV")}
        if not previas_validas:
            continue

        # Construir marcador "Activo en LXIV y LXV" o "Activo en LXIV (Diputados) y LXV (Senado)"
        previas_legs_unicos = sorted({l for (l, _) in previas_validas},
                                     key=lambda x: ["LXIV", "LXV"].index(x))
        # Detectar si cambió de cámara
        camaras_previas = {c for (_, c) in previas_validas}
        cambio_camara = (d["camara"] not in camaras_previas) or len(camaras_previas) > 1

        if cambio_camara:
            # Marcador detallado
            partes = []
            for l in previas_legs_unicos:
                cams_de_l = sorted({c for (ll, c) in previas_validas if ll == l})
                partes.append(f"{l} ({'/'.join(cams_de_l)})")
            marcador = "Activo en " + " y ".join(partes)
        else:
            marcador = "Activo en " + " y ".join(previas_legs_unicos)

        reelectos.append({
            "nombre": d["nombre"],
            "nombre_normalizado": nn,
            "camara_actual": d["camara"],
            "partido_actual": d["partido"],
            "entidad_actual": d["entidad"],
            "tipo_eleccion_actual": d["tipo_eleccion"],
            "instancia_actual": d["instancia"],
            "previas": [
                {"leg": t["legislatura"], "camara": t["camara"], "partido": t["partido"],
                 "entidad": t["entidad"], "tipo_eleccion": t["tipo_eleccion"],
                 "fecha_protesta": t["fecha_protesta"]}
                for t in trayectoria if (t["legislatura"], t["camara"]) != (d["legislatura"], d["camara"])
                and t["legislatura"] in ("LXIV", "LXV")
            ],
            "previas_legislaturas": previas_legs_unicos,
            "cambio_camara": cambio_camara,
            "marcador": marcador,
        })

    # ─────────────────────────────────────────────────────────
    # Match contra BD
    # ─────────────────────────────────────────────────────────
    conn = get_connection()
    db_dip = {r[0] for r in conn.execute(
        "SELECT nombre_normalizado FROM legisladores WHERE camara='Cámara de Diputados'"
    ).fetchall()}
    db_sen = {r[0] for r in conn.execute(
        "SELECT nombre_normalizado FROM legisladores WHERE camara IN ('Cámara de Senadores','Senado')"
    ).fetchall()}

    en_bd = sum(
        1 for r in reelectos
        if (r["camara_actual"] == "Diputados" and r["nombre_normalizado"] in db_dip)
        or (r["camara_actual"] == "Senado" and r["nombre_normalizado"] in db_sen)
    )

    # ─────────────────────────────────────────────────────────
    # Tally
    # ─────────────────────────────────────────────────────────
    n_total = len(reelectos)
    n_3per = sum(1 for r in reelectos if set(r["previas_legislaturas"]) == {"LXIV", "LXV"})
    n_2per_consec = sum(1 for r in reelectos if r["previas_legislaturas"] == ["LXV"])
    n_salto = sum(1 for r in reelectos if r["previas_legislaturas"] == ["LXIV"])
    n_cambio = sum(1 for r in reelectos if r["cambio_camara"])

    by_partido_actual = Counter(r["partido_actual"] for r in reelectos)
    by_camara_actual = Counter(r["camara_actual"] for r in reelectos)

    # Casos famosos que mencionó el user (si los detectamos)
    famosos = ["ruben moreira", "manuel anorve", "alejandro moreno", "ivonne ortega",
               "yerico abramo", "ector jaime ramirez", "carolina viggiano"]
    detectados_famosos = []
    for r in reelectos:
        for f in famosos:
            if f in r["nombre_normalizado"]:
                detectados_famosos.append((f, r))

    # ─────────────────────────────────────────────────────────
    # Print resumen
    # ─────────────────────────────────────────────────────────
    print()
    print(f"  Roster total LXVI (titulares + suplentes en función):")
    print(f"    Diputados: {len(lxvi_dip)}")
    print(f"    Senado:    {len(lxvi_sen)}")
    print()
    print(f"  ═ REELECTOS DETECTADOS: {n_total} ═")
    print(f"    Diputados LXVI con historia previa: {by_camara_actual.get('Diputados', 0)}")
    print(f"    Senadores LXVI con historia previa: {by_camara_actual.get('Senado', 0)}")
    print()
    print(f"    3 periodos (LXIV+LXV+LXVI):  {n_3per}")
    print(f"    2 periodos consecutivos (LXV+LXVI):  {n_2per_consec}")
    print(f"    Saltaron LXV (LXIV+LXVI):  {n_salto}")
    print(f"    Cambiaron de cámara entre LXIV/LXV → LXVI:  {n_cambio}")
    print()
    print(f"  Por partido actual (top):")
    for p, n in by_partido_actual.most_common(10):
        print(f"    {p:<20} {n:>4}")
    print()
    print(f"  Match contra BD legisladores: {en_bd}/{n_total} ({100*en_bd/n_total:.0f}%)")
    print()
    if detectados_famosos:
        print(f"  Casos mencionados detectados:")
        seen = set()
        for f, r in detectados_famosos:
            if r["nombre_normalizado"] in seen:
                continue
            seen.add(r["nombre_normalizado"])
            print(f"    • {r['nombre']} → {r['marcador']} (actual: {r['camara_actual']}/{r['partido_actual']})")
    print()

    # ─────────────────────────────────────────────────────────
    # Output
    # ─────────────────────────────────────────────────────────
    diag_path = OUT_DIR / "diagnostico_unificado.json"
    diag_path.write_text(json.dumps({
        "metadata": {
            "fuente": "SIL Gobernación Numeralia",
            "url_pattern": "resultadosNumeraliaLegisladores.php?Legislatura={64|65|66}&Camara={1|2}",
            "fecha_generacion": time.strftime("%Y-%m-%d %H:%M"),
            "totales_roster": {f"{l}_{c}": len(rosters[(l, c)]) for (l, c) in rosters},
        },
        "totales": {
            "reelectos_total": n_total,
            "reelectos_diputados": by_camara_actual.get("Diputados", 0),
            "reelectos_senado": by_camara_actual.get("Senado", 0),
            "tres_periodos": n_3per,
            "dos_consecutivos": n_2per_consec,
            "saltaron_lxv": n_salto,
            "cambio_camara": n_cambio,
            "match_bd": en_bd,
        },
        "por_partido": dict(by_partido_actual),
        "reelectos": reelectos,
    }, ensure_ascii=False, indent=2))

    # Markdown
    md = OUT_DIR / "reporte_unificado.md"
    lines = [
        "# Diagnóstico unificado — Reelectos LXIV/LXV → LXVI",
        "",
        "**Fuente**: SIL Gobernación Numeralia (Diputados + Senado, mismas tres legislaturas).",
        "**No escribe en BD.** Solo reporte para decidir storage.",
        "",
        "## Roster scrapeado",
        "",
        "| Legislatura | Diputados | Senado |",
        "|---|---:|---:|",
    ]
    for l in ["LXIV", "LXV", "LXVI"]:
        lines.append(f"| {l} | {len(rosters[(l, 'Diputados')])} | {len(rosters[(l, 'Senado')])} |")
    lines += [
        "",
        "(Incluye titulares + suplentes que entraron en funciones).",
        "",
        "## Resumen de reelectos LXVI",
        "",
        f"- **Total reelectos**: {n_total}",
        f"  - Diputados LXVI con historia previa: **{by_camara_actual.get('Diputados', 0)}**",
        f"  - Senadores LXVI con historia previa: **{by_camara_actual.get('Senado', 0)}**",
        "",
        "### Por trayectoria",
        "",
        f"- **3 periodos** (LXIV + LXV + LXVI): **{n_3per}**",
        f"- **2 consecutivos** (LXV + LXVI): **{n_2per_consec}**",
        f"- **Saltaron LXV** (LXIV + LXVI): **{n_salto}**",
        f"- **Cambiaron de cámara** entre periodos: **{n_cambio}**",
        "",
        "### Por partido actual (top 10)",
        "",
        "| Partido | Reelectos |",
        "|---|---:|",
    ]
    for p, n in by_partido_actual.most_common(10):
        lines.append(f"| {p} | {n} |")

    lines += [
        "",
        f"### Match con BD `legisladores`: {en_bd}/{n_total} ({100*en_bd/n_total:.0f}%)",
        "",
        "## Casos famosos detectados",
        "",
    ]
    seen = set()
    for f, r in detectados_famosos:
        if r["nombre_normalizado"] in seen:
            continue
        seen.add(r["nombre_normalizado"])
        lines.append(f"- **{r['nombre']}** ({r['camara_actual']}/{r['partido_actual']}) → {r['marcador']}")

    lines += [
        "",
        "## Top 30 con 3 periodos",
        "",
        "| Nombre | Cámara LXVI | Partido | Entidad | Marcador |",
        "|---|---|---|---|---|",
    ]
    treped = [r for r in reelectos if set(r["previas_legislaturas"]) == {"LXIV", "LXV"}]
    for r in treped[:30]:
        lines.append(
            f"| {r['nombre']} | {r['camara_actual']} | {r['partido_actual']} | "
            f"{r['entidad_actual']} | {r['marcador']} |"
        )

    lines += [
        "",
        "## Cambios de cámara detectados",
        "",
        "| Nombre | Trayectoria → LXVI | Marcador |",
        "|---|---|---|",
    ]
    for r in reelectos:
        if r["cambio_camara"]:
            traj = " · ".join(f"{p['leg']}/{p['camara']}/{p['partido']}" for p in r["previas"])
            lines.append(f"| {r['nombre']} | {traj} → LXVI/{r['camara_actual']}/{r['partido_actual']} | {r['marcador']} |")

    lines += [
        "",
        "## Decisión pendiente: dónde almacenar",
        "",
        "Opciones discutidas:",
        "1. Columna nueva en `legisladores`: `marcador_continuidad TEXT` (string corto, ej. `Activo en LXIV y LXV`).",
        "2. Tabla nueva `legisladores_trayectoria(legislador_id, leg, camara, partido, entidad, tipo_eleccion)` — más rica, permite cambios de cámara y partido entre periodos.",
        "3. JSON anidado en columna existente.",
        "",
        "Recomendación: **opción 2** (tabla `legisladores_trayectoria`), porque conserva la trazabilidad cámara/partido sin saturar la tabla principal y deja la opción 1 (`marcador_continuidad`) como derived view rápido para UI.",
    ]
    md.write_text("\n".join(lines))

    print(f"  ✓ Output:")
    print(f"    {diag_path.relative_to(ROOT)}")
    print(f"    {md.relative_to(ROOT)}")
    print()


if __name__ == "__main__":
    main()
