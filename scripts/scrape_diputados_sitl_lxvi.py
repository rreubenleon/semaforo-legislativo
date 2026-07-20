"""
Scrape de iniciativas + proposiciones de TODOS los diputados LXVI desde SITL,
incluyendo TODOS los roles (Iniciante, Adherente, Grupo, Suscriptor).

Bug original: la versión anterior solo capturaba Iniciante/Promovente,
sub-contando ~25% de las iniciativas vinculadas (las Adherente y las
de Grupo Parlamentario). Ej. Chedraui SITL real: 20 Iniciante + 3
Adherente + 2 Grupo = 25 vinculadas; mi BD solo tenía 19.

Endpoints:
  iniciativas_por_pernplxvi.php?iddipt={ID}&pert={PERT}
  proposiciones_por_pernplxvi.php?iddipt={ID}&pert={PERT}

Cada página tiene:
  - Tabla 2 (resumen): "Iniciante 5 ... Adherente 3 ..." (totales por rol)
  - Tabla 3 (detalle): cada iniciativa con título + Iniciante: NOMBRE + Fecha

PERTS LXVI:
  1 = 1er Ord 1er Año (sept-dic 2024)
  2 = 1er Receso 1er Año (dic 2024-ene 2025)
  3 = 2do Ord 1er Año (feb-abr 2025)
  4 = 2do Receso 1er Año (may-ago 2025)
  6 = 1er Ord 2do Año (sept-dic 2025)
  8 = 2do Ord 2do Año (feb-abr 2026)

Output: dashboard/diputados_lxvi_oficial.json con instrumentos por rol.

Uso:
    python scripts/scrape_diputados_sitl_lxvi.py
    python scripts/scrape_diputados_sitl_lxvi.py --diagnostic --only-id 330
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import sys
import time
import urllib3
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

BASE = "http://sitl.diputados.gob.mx/LXVI_leg"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "es-MX,es;q=0.9",
}

PERTS = [1, 2, 3, 4, 6, 8]
DELAY = 0.25

# El caché y el checkpoint eran PERMANENTES: `fetch_html` devolvía el HTML
# guardado para siempre y el checkpoint saltaba a todo diputado ya presente en
# el JSON. Resultado: el scrape corría en verde cada miércoles sin volver a
# pedir NADA, y el espejo oficial de Diputados quedó congelado el 28-abr-2026
# (descubierto el 19-jul, ~3 meses después). Ahora el caché expira: los
# periodos cerrados se reutilizan, pero lo reciente se vuelve a bajar.
CACHE_TTL_DIAS = 6

CACHE_DIR = ROOT / "eval" / "diputados" / "cache_sitl"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
OUT_JSON = ROOT / "dashboard" / "diputados_lxvi_oficial.json"

MESES = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}


def fecha_iso(f: str) -> str:
    if not f:
        return ""
    m = re.match(r"(\d{1,2})-(\w+)-(\d{4})", f.strip())
    if not m:
        return ""
    mes = MESES.get(m.group(2).lower(), "")
    if not mes:
        return ""
    return f"{m.group(3)}-{mes}-{m.group(1).zfill(2)}"


def cargar_diputados_bd() -> list[dict]:
    db = ROOT / "semaforo.db"
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, sitl_id, nombre, partido
        FROM legisladores
        WHERE camara = 'Cámara de Diputados'
        ORDER BY id
        """
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_html(url: str, cache_path: Path, ttl_dias: float = CACHE_TTL_DIAS) -> str:
    if cache_path.exists() and cache_path.stat().st_size > 100:
        edad_dias = (time.time() - cache_path.stat().st_mtime) / 86400.0
        if edad_dias < ttl_dias:
            return cache_path.read_text(encoding="utf-8")
        logger.debug(f"  caché vencido ({edad_dias:.1f}d), re-descargando {url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=20, verify=False)
        r.encoding = "utf-8"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(r.text, encoding="utf-8")
        time.sleep(DELAY)
        return r.text
    except Exception as e:
        logger.warning(f"  fetch fail {url}: {e}")
        return ""


# Roles posibles según el HTML del SITL Diputados
# Iniciativas:   Iniciante (titular), Adherente (firmante adicional),
#                Suscriptor, De Grupo (presentada por GP, todos los
#                miembros del GP están vinculados — ej Gibrán/MC = 47)
# Proposiciones: Promovente, Proponente, Adherente, Suscriptor, De Grupo
ROLES_INICIATIVA = ["Iniciante", "Adherente", "Suscriptor", "De Grupo"]
ROLES_PROPOSICION = ["Promovente", "Proponente", "Adherente", "Suscriptor", "De Grupo"]


def parse_listado(html: str, tipo_doc: str, sitl_id_dip: str) -> list[dict]:
    """
    Parsea el HTML extrayendo TODOS los roles posibles. Cada bloque
    "Fecha de presentación: DD-Mes-YYYY" delimita un instrumento.
    """
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    tablas = soup.find_all("table")
    if not tablas:
        return []

    # Concatenar todas las tablas (excepto header del sitio)
    detalle = " ".join(t.get_text(" ", strip=True) for t in tablas[1:])
    if "Fecha de presentación" not in detalle:
        return []

    roles = ROLES_INICIATIVA if tipo_doc == "Iniciativa" else ROLES_PROPOSICION

    # Construir patrón regex que matchee cualquiera de los roles
    rol_pattern = "|".join(roles)

    items = []
    pre_blocks = re.split(r"Fecha de presentación:\s*", detalle)
    for i in range(len(pre_blocks) - 1):
        pre = pre_blocks[i]
        next_b = pre_blocks[i + 1]
        m_fecha = re.match(r"(\d{1,2}-[A-Za-zñÑáéíóú]+-\d{4})", next_b)
        if not m_fecha:
            continue
        fecha = m_fecha.group(1)
        cuerpo_post = next_b[len(fecha):].strip()

        # Buscar TODOS los roles en el bloque pre. Algunos instrumentos
        # listan "Iniciante: X" y luego "Adherente: Y" en el mismo bloque
        # → son apariciones distintas de la misma iniciativa.
        # Tomamos el ÚLTIMO rol como el rol principal del diputado actual
        # (el bloque típicamente termina con "Iniciante: NOMBRE_X" donde
        # NOMBRE_X es el legislador del listado).
        m_rol = re.findall(
            rf"({rol_pattern}):\s*([^\n]+?)(?=\s+(?:{rol_pattern}):|\s*$)",
            pre,
        )
        if not m_rol:
            continue
        # Tomar el último rol que aparece — es el del diputado del query
        rol_label, rol_value = m_rol[-1]
        rol_value = rol_value.strip()

        antes_rol = pre.split(f"{rol_label}:")[0]
        # Quitar otros roles que aparecieron antes
        for r in roles:
            antes_rol = re.sub(rf"\b{r}:\s*[^\n]+", "", antes_rol)
        m_titulo = re.match(r".*?\s*\d+\s+(?:&nbsp;)?\s*(.+)\s*$", antes_rol, re.DOTALL)
        titulo = (m_titulo.group(1).strip() if m_titulo else antes_rol.strip())
        titulo = re.sub(r"\s+", " ", titulo)[:500]

        items.append({
            "rol": rol_label,
            "rol_valor": rol_value[:200],
            "fecha": fecha_iso(fecha),
            "fecha_raw": fecha,
            "titulo": titulo,
            "sinopsis": cuerpo_post[:500],
        })
    return items


def scrape_diputado(sitl_id: str) -> list[dict]:
    items = []
    for pert in PERTS:
        for endpoint, tipo in [
            ("iniciativas_por_pernplxvi.php", "Iniciativa"),
            ("proposiciones_por_pernplxvi.php", "Proposición con Punto de Acuerdo"),
        ]:
            url = f"{BASE}/{endpoint}?iddipt={sitl_id}&pert={pert}"
            cache = CACHE_DIR / f"{sitl_id}_{pert}_{tipo[:3]}.html"
            html = fetch_html(url, cache)
            for it in parse_listado(html, tipo, sitl_id):
                it["sitl_id_dip"] = sitl_id
                it["periodo"] = pert
                it["tipo"] = tipo
                items.append(it)
    return items


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--diagnostic", action="store_true")
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--only-id", type=str, default="")
    args = p.parse_args()

    diputados = cargar_diputados_bd()
    logger.info(f"Diputados LXVI en BD: {len(diputados)}")

    if args.only_id:
        diputados = [d for d in diputados if str(d["sitl_id"]) == args.only_id]
    elif args.limit > 0:
        diputados = diputados[: args.limit]

    inicio = datetime.now()
    todos_items = []
    diputados_meta = []

    # Checkpoint: si JSON existe, cargar diputados ya hechos y skipear.
    # Crítico para resiliencia ante timeouts (60min original, 180min ahora).
    # OJO (19-jul-2026): el checkpoint es para reanudar tras un timeout DENTRO
    # de una corrida, NO para saltarse semanas. Si el JSON ya es viejo hay que
    # volver a bajar todo, o el espejo se congela (fue lo que pasó: 28-abr →
    # 19-jul en verde, sin refrescar un solo diputado).
    out_path = ROOT / "dashboard" / "diputados_lxvi_oficial.json"
    ya_hechos = set()
    _cp_vencido = False
    if out_path.exists():
        _edad_cp = (time.time() - out_path.stat().st_mtime) / 86400.0
        if _edad_cp >= CACHE_TTL_DIAS:
            _cp_vencido = True
            logger.info(
                f"Checkpoint VENCIDO ({_edad_cp:.1f} días): se ignora y se "
                f"re-scrapea a todos los diputados."
            )
    if out_path.exists() and not _cp_vencido:
        try:
            existing = json.loads(out_path.read_text())
            for d in existing.get("diputados", []):
                ya_hechos.add(str(d.get("sitl_id", "")))
            diputados_meta = existing.get("diputados", []).copy()
            todos_items = existing.get("instrumentos", []).copy()
            logger.info(f"Checkpoint: {len(ya_hechos)} diputados ya scrapeados, reanudando")
        except Exception as e:
            logger.warning(f"No se pudo cargar checkpoint: {e}")

    for i, dip in enumerate(diputados, 1):
        sitl_id = str(dip["sitl_id"]) if dip["sitl_id"] else ""
        if not sitl_id:
            continue
        if sitl_id in ya_hechos:
            continue
        items = scrape_diputado(sitl_id)
        # Stats por rol
        from collections import Counter
        roles_count = Counter((it["tipo"], it["rol"]) for it in items)
        n_ini_iniciante = roles_count.get(("Iniciativa", "Iniciante"), 0)
        n_ini_adherente = roles_count.get(("Iniciativa", "Adherente"), 0)
        n_ini_grupo = roles_count.get(("Iniciativa", "De Grupo"), 0)
        n_prop_promovente = (
            roles_count.get(("Proposición con Punto de Acuerdo", "Promovente"), 0)
            + roles_count.get(("Proposición con Punto de Acuerdo", "Proponente"), 0)
        )
        n_prop_adherente = roles_count.get(("Proposición con Punto de Acuerdo", "Adherente"), 0)
        n_prop_grupo = roles_count.get(("Proposición con Punto de Acuerdo", "De Grupo"), 0)
        diputados_meta.append({
            "sitl_id": sitl_id,
            "legislador_id": dip["id"],
            "nombre": dip["nombre"],
            "partido": dip["partido"],
            "n_iniciativas_iniciante": n_ini_iniciante,
            "n_iniciativas_adherente": n_ini_adherente,
            "n_iniciativas_grupo": n_ini_grupo,
            "n_proposiciones": n_prop_promovente,
            "n_proposiciones_adherente": n_prop_adherente,
            "n_proposiciones_grupo": n_prop_grupo,
        })
        todos_items.extend(items)
        if i % 5 == 0 or i == len(diputados):
            elapsed = (datetime.now() - inicio).total_seconds()
            rate = i / max(elapsed, 1)
            eta = (len(diputados) - i) / max(rate, 0.1)
            logger.info(
                f"  [{i:3}/{len(diputados)}] {dip['nombre'][:38]:38} "
                f"i={n_ini_iniciante:2} a={n_ini_adherente:2} g={n_ini_grupo:2} "
                f"p={n_prop_promovente:2} · ETA {eta/60:.1f}m"
            )
            # Checkpoint incremental: guardar JSON cada 25 diputados para
            # no perder trabajo si timeout / crash mid-scrape.
            try:
                out_path.parent.mkdir(parents=True, exist_ok=True)
                ckpt = {
                    "diputados": diputados_meta,
                    "instrumentos": todos_items,
                    "scraped_at": datetime.now().isoformat(timespec="seconds"),
                    "total_diputados": len(diputados_meta),
                    "total_instrumentos": len(todos_items),
                    "checkpoint": True,
                }
                out_path.write_text(json.dumps(ckpt, ensure_ascii=False))
            except Exception as e:
                logger.warning(f"  No se pudo guardar checkpoint: {e}")

    out = {
        "diputados": diputados_meta,
        "instrumentos": todos_items,
        "scraped_at": datetime.now().isoformat(timespec="seconds"),
        "total_diputados": len(diputados_meta),
        "total_instrumentos": len(todos_items),
        "total_iniciativas_iniciante": sum(1 for it in todos_items if it["tipo"] == "Iniciativa" and it["rol"] == "Iniciante"),
        "total_iniciativas_adherente": sum(1 for it in todos_items if it["tipo"] == "Iniciativa" and it["rol"] == "Adherente"),
        "total_proposiciones": sum(1 for it in todos_items if it["tipo"] != "Iniciativa"),
    }

    if not args.diagnostic:
        OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2))
        logger.info(f"\nGuardado: {OUT_JSON}")

    print()
    print("=" * 60)
    print(f"Total diputados procesados: {out['total_diputados']}")
    print(f"Total instrumentos:         {out['total_instrumentos']}")
    print(f"  · Iniciante:              {out['total_iniciativas_iniciante']}")
    print(f"  · Adherente:              {out['total_iniciativas_adherente']}")
    print(f"  · Proposiciones:          {out['total_proposiciones']}")
    print(f"Tiempo: {(datetime.now() - inicio).total_seconds() / 60:.1f} min")


if __name__ == "__main__":
    main()
