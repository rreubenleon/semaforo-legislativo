"""Reconcilia la CATEGORÍA de los asuntos de la Comisión Permanente contra el
turno OFICIAL de senado.gob.mx.

POR QUÉ EXISTE (bug reportado 30-jul-2026)
  El clasificador leía el bloque de autores del título ("De la Dip. X, del Grupo
  Parlamentario del Partido Y, con proyecto…") y mandaba a electoral_politico
  iniciativas que en realidad son de salud, niñez, economía, etc. Medido contra
  el turno oficial: de 928 asuntos de la Permanente en electoral_politico, solo
  17 eran de verdad electorales; 186 (de los que empatan turno) estaban mal.

  La señal CONFIABLE es el turno: "SE DIO TURNO A LA COMISIÓN DE X". El Senado la
  publica. Este script la usa como AUTORIDAD para fijar la categoría.

QUÉ TOCA
  Solo la columna `categoria` de sil_documentos (camara='Comisión Permanente').
  NUNCA toca `comision` (esa la usa la pestaña Comisiones para agrupar por
  Comisión de Trabajo — es otra cosa).

POLÍTICA (quirúrgica, no genera "sin categoría" gratis)
  1. Si el asunto empata un turno oficial SUSTANTIVO → esa categoría (autoridad).
  2. Si NO empata turno, pero está en electoral_politico y trae bloque de autor:
     se reclasifica el OBJETO (título sin preámbulo). Si el objeto ya no es
     electoral, se corrige (puede quedar sin categoría: era un artefacto del
     bloque de partido, es lo honesto).
  3. En cualquier otro caso NO se toca (no se degrada lo ya bien clasificado).

  Comisiones PROCESALES (Puntos Constitucionales, Estudios Legislativos, Régimen/
  Reglamentos) NO definen materia → caen al clasificador del objeto (que ya lee
  el artículo constitucional citado, art. 28 → economía, etc.).

Uso:
    python3 scripts/reclasificar_permanente_oficial.py --dry-run
    python3 scripts/reclasificar_permanente_oficial.py            # escribe a la BD
    python3 scripts/reclasificar_permanente_oficial.py --d1       # + propaga a D1
    python3 scripts/reclasificar_permanente_oficial.py --periodos CP66-2ASPR
"""
from __future__ import annotations
import argparse, html, logging, os, re, sys, unicodedata
import requests, urllib3

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from db import get_connection
from nlp.clasificador import (clasificar_texto, quitar_preambulo_autor,
                              _RE_PREAMBULO_AUTOR)
from config import comision_a_categoria

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("reclasif_permanente")

BASE = "https://www.senado.gob.mx/permanente"
SECCIONES = ("proyectosDecreto", "puntosTurnados")
PERIODOS = ("CP66-2ASPR",)  # el índice de LXVI solo expone el receso vigente
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
           "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
           "Accept-Language": "es-MX,es;q=0.9"}

# Comisiones que NO definen materia: cualquier tema pasa por ellas.
PROCESAL = ("puntos constitucionales", "estudios legislativos", "regimen",
            "régimen", "reglamentos", "practicas parlamentarias",
            "prácticas parlamentarias", "jurisdiccional")


def _clave(titulo: str) -> str:
    s = quitar_preambulo_autor(titulo or "")
    s = "".join(c for c in unicodedata.normalize("NFD", s.lower())
                if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", s)).strip()[:90]


def _comision_de_turno(txt: str) -> str:
    m = re.search(r"COMISI[ÓO]N(?:ES)?\s+(?:UNIDAS\s+)?"
                  r"(?:DIRECTO A LA COMISI[ÓO]N DE |DE )?(.+?)"
                  r"(?:\s+DE LA C[ÁA]MARA|\s+DEL SENADO|,|;|\.| CON OPINI|$)",
                  txt or "", re.I)
    return m.group(1).strip() if m else ""


def _cat_de_turno(txt: str):
    """Categoría sustantiva del turno, o None si procesal / no mapeable."""
    cn = _comision_de_turno(txt)
    if not cn or any(p in cn.lower() for p in PROCESAL):
        return None
    return comision_a_categoria(cn)


def scrape_turnos(periodos) -> dict:
    """Devuelve {clave_objeto: categoria_sustantiva} desde senado.gob.mx."""
    out = {}
    ses = requests.Session(); ses.headers.update(HEADERS)
    try:  # warmup Incapsula
        ses.get("https://www.senado.gob.mx/66/", timeout=30, verify=False)
    except Exception:
        pass
    for per in periodos:
        for sec in SECCIONES:
            url = f"{BASE}/{per}/{sec}"
            try:
                t = ses.get(url, timeout=45, verify=False).text
            except Exception as e:
                log.warning("no se pudo leer %s: %s", url, e); continue
            n = 0
            for blk in t.split("id='titulo-buscador'")[1:]:
                tm = re.search(r">\s*([^<>]{50,600})\s*<", blk)
                tu = re.search(r"SE DIO TURNO[^<]*", blk)
                if not (tm and tu):
                    continue
                cat = _cat_de_turno(html.unescape(tu.group(0)))
                if cat:
                    out[_clave(html.unescape(tm.group(1)))] = cat; n += 1
            log.info("%s/%s: %d turnos sustantivos", per, sec, n)
    return out


def _cat0(x): return (x or "").split(":")[0].split(",")[0].strip() or "sin categoría"


def reclasificar(dry_run=False, a_d1=False, periodos=PERIODOS):
    turnos = scrape_turnos(periodos)
    log.info("turnos sustantivos totales: %d", len(turnos))
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, titulo, sinopsis, categoria FROM sil_documentos "
        "WHERE camara='Comisión Permanente'").fetchall()

    updates = []            # (nueva_cat, id)
    por_turno = de_electoral = 0
    for doc_id, titulo, sinopsis, cat_raw in rows:
        old = _cat0(cat_raw)
        new = None
        cat_turno = turnos.get(_clave(titulo))
        if cat_turno:                                   # 1) autoridad del turno
            new = cat_turno
            if new != old:
                por_turno += 1
        elif old == "electoral_politico" and _RE_PREAMBULO_AUTOR.search(titulo or ""):
            sc = clasificar_texto(titulo, sinopsis or "")   # 2) de-electoralizar
            obj = max(sc, key=sc.get) if sc else "sin categoría"
            if obj != "electoral_politico":
                new = obj
                de_electoral += 1
        if new and new != old:
            updates.append((new, doc_id))

    log.info("cambios: %d (por turno: %d, sacados de electoral: %d)",
             len(updates), por_turno, de_electoral)
    if dry_run:
        from collections import Counter
        c = Counter(n for n, _ in updates)
        log.info("destino de los cambios: %s", dict(c.most_common()))
        return updates

    conn.executemany("UPDATE sil_documentos SET categoria=? WHERE id=?", updates)
    conn.commit()
    log.info("BD actualizada: %d filas", len(updates))
    if a_d1:
        _propagar_d1(conn, [i for _, i in updates])
    return updates


def _propagar_d1(conn, ids):
    """Sube las categorías corregidas a D1 (radar). Requiere CLOUDFLARE_API_TOKEN."""
    if not os.getenv("CLOUDFLARE_API_TOKEN"):
        log.warning("sin CLOUDFLARE_API_TOKEN: no se propaga a D1 (lo hará radar.yml)")
        return
    import subprocess
    filas = conn.execute(
        "SELECT id, categoria FROM sil_documentos WHERE id IN (%s)"
        % ",".join("?" * len(ids)), ids).fetchall() if ids else []
    stmts = ";\n".join(
        "UPDATE sil_documentos SET categoria='%s' WHERE id=%d"
        % ((c or "").replace("'", "''"), i) for i, c in filas)
    if not stmts:
        return
    path = os.path.join(ROOT, "_d1_reclasif.sql")
    open(path, "w").write(stmts + ";\n")
    subprocess.run(["npx", "wrangler", "d1", "execute", "fiat-busqueda",
                    "--remote", "--file", path], cwd=ROOT, check=False)
    log.info("propagadas %d filas a D1", len(filas))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--d1", action="store_true")
    ap.add_argument("--periodos", nargs="+", default=list(PERIODOS))
    a = ap.parse_args()
    reclasificar(dry_run=a.dry_run, a_d1=a.d1, periodos=tuple(a.periodos))
