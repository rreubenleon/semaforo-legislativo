"""
Turnos a comisión de la Comisión Permanente, desde la fuente OFICIAL.

POR QUÉ EXISTE
  El SIL no guarda el turno de los asuntos de la Permanente: medido el
  19-jul-2026, de 1,312 asuntos cuyo turno SÍ publica el Senado, FIAT
  conservaba 110 (se perdía el 92%). Como durante el receso la Permanente es
  lo ÚNICO que sesiona, la sección de Comisiones se veía estancada desde
  abril aunque hubiera actividad real todos los días.

FUENTE (la publica el Senado, es el registro oficial)
  https://www.senado.gob.mx/permanente/{PERIODO}/proyectosDecreto
  https://www.senado.gob.mx/permanente/{PERIODO}/puntosTurnados

  Cada asunto trae título completo, fecha de publicación y el turno textual:
    "SE DIO TURNO DIRECTO A LAS COMISIONES UNIDAS DE GOBERNACIÓN Y DE
     ESTUDIOS LEGISLATIVOS, SEGUNDA DE LA CÁMARA DE SENADORES."

CÓMO EMPAREJA
  Por el OBJETO del asunto (se descarta el bloque de autores, que varía entre
  fuentes y es larguísimo en las proposiciones de grupo). Validado el
  19-jul-2026 contra CP66-2ASPR: 1,511 de 1,525 asuntos emparejados (99.1%).

  Solo ESCRIBE `comision` donde está vacía. Nunca sobreescribe un turno ya
  registrado ni toca ningún otro campo.

Uso:
    python3 scripts/scrape_permanente_turnos.py --dry-run
    python3 scripts/scrape_permanente_turnos.py
    python3 scripts/scrape_permanente_turnos.py --periodos CP66-2ASPR
"""
from __future__ import annotations

import argparse
import html
import logging
import os
import re
import sys
import unicodedata

import requests
import urllib3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("permanente_turnos")

BASE = "https://www.senado.gob.mx/permanente"
SECCIONES = ("proyectosDecreto", "puntosTurnados")
# Solo el receso ACTUAL por defecto.
# Medido 19-jul-2026 (emparejamiento contra sil_documentos):
#   CP66-2ASPR (receso actual) → 96% de match dentro de Permanente, 99% contra
#                                toda la BD. Confiable para escribir.
#   CP66-1ASPR (receso 2025)   → 12% / 45%. Más de la mitad de ese periodo NO
#                                está capturado en FIAT; escribir turnos ahí
#                                sería forzar datos sobre un hueco de captura.
#                                Es un problema aparte (falta el scrape de ese
#                                receso), no se tapa desde aquí.
# Para procesarlo igualmente: --periodos CP66-1ASPR CP66-2ASPR
PERIODOS = ("CP66-2ASPR",)
# senado.gob.mx rechaza el UA por defecto de requests (Incapsula).
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept-Language": "es-MX,es;q=0.9",
}
MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
         "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10,
         "noviembre": 11, "diciembre": 12}


def _na(s: str) -> str:
    s = "".join(c for c in unicodedata.normalize("NFD", (s or "").lower())
                if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", s)).strip()


def clave_objeto(titulo: str) -> str:
    """Núcleo del asunto, sin el bloque de autores. Es la llave de emparejamiento."""
    t = _na(titulo)
    m = re.search(r"(con proyecto de decreto|con punto de acuerdo|que reforma"
                  r"|proposicion con punto)", t)
    return t[m.start():][:110] if m else t[-110:]


def parsear_seccion(html_txt: str) -> list[dict]:
    """Extrae {titulo, fecha, turno} de una página de la Permanente."""
    items = []
    for blk in html_txt.split("id='titulo-buscador'")[1:]:
        m = re.search(r"gaceta_comision_permanente/documento/(\d+)'[^>]*>(.*?)</a>",
                      blk, re.S)
        if not m:
            continue
        titulo = html.unescape(re.sub(r"<[^>]+>", "", m.group(2))).strip()
        f = re.search(r"Fecha de publicaci[^<]*</strong>\s*\w+\s+(\d{1,2})\s+de\s+"
                      r"(\w+)\s+de\s+(\d{4})", blk)
        fecha = (f"{f.group(3)}-{MESES.get(f.group(2).lower(), 0):02d}-"
                 f"{int(f.group(1)):02d}") if f else None
        tu = re.search(r"(SE DIO TURNO[^<]*)", blk)
        items.append({
            "doc": m.group(1),
            "titulo": re.sub(r"\s+", " ", titulo),
            "fecha": fecha,
            "turno": html.unescape(tu.group(1)).strip() if tu else None,
        })
    return items


def _titulo_es(txt: str) -> str:
    """Capitaliza respetando conectores en minúscula (Gobernación y Población)."""
    menores = {"y", "de", "del", "la", "las", "los", "el", "e", "en", "para", "a"}
    palabras = txt.lower().split()
    return " ".join(w if i and w in menores else w.capitalize()
                    for i, w in enumerate(palabras))


def cargar_catalogo(conn) -> set:
    """Nombres de comisión REALES ya presentes en la base.

    Sirve para no partir en dos una comisión cuyo propio nombre lleva "y de"
    (p. ej. "Derechos de la Niñez y de la Adolescencia"): si el nombre completo
    existe en el catálogo, se respeta entero.
    """
    cat = set()
    for q in ("SELECT DISTINCT comision FROM sil_documentos WHERE comision != ''",
              "SELECT DISTINCT comision FROM gaceta WHERE comision != ''"):
        try:
            for (c,) in conn.execute(q):
                for parte in re.split(r"\s*;\s*", c or ""):
                    parte = re.sub(r"^comisi[óo]n(es)?\s+(unidas\s+)?de\s+", "",
                                   parte.strip(), flags=re.I).strip()
                    if len(parte) > 2:
                        cat.add(_na(parte))
        except Exception:
            pass
    return cat


def comisiones_del_turno(turno: str, catalogo: set | None = None) -> str:
    """Normaliza el texto oficial del turno a la lista de comisiones.

    'SE DIO TURNO DIRECTO A LAS COMISIONES UNIDAS DE GOBERNACIÓN Y DE ESTUDIOS
     LEGISLATIVOS, SEGUNDA DE LA CÁMARA DE SENADORES.'
       → 'Gobernación y Población; Estudios Legislativos, Segunda'

    Las Primera/Segunda/Tercera Comisión son las comisiones propias de la
    Comisión Permanente y se conservan con ese nombre: son reales aunque no
    estén en el catálogo de comisiones ordinarias de las cámaras.
    """
    if not turno:
        return ""
    t = turno.strip().rstrip(".")
    t = re.sub(r"^SE DIO TURNO\s+(DIRECTO\s+)?A\s+(LAS?\s+)?", "", t, flags=re.I)
    t = re.sub(r"\s+DE\s+LA\s+C[ÁA]MARA\s+DE\s+(SENADORES|DIPUTADOS).*$", "", t, flags=re.I)
    t = re.sub(r"\s+DE\s+LA\s+COMISI[ÓO]N\s+PERMANENTE.*$", "", t, flags=re.I)
    # "…CON OPINIÓN DE LA COMISIÓN DE X" → la comisión de opinión es secundaria
    t = re.sub(r",?\s+CON\s+OPINI[ÓO]N\s+DE.*$", "", t, flags=re.I)
    t = re.sub(r"^COMISIONES\s+UNIDAS\s+DE\s+", "", t, flags=re.I)
    t = re.sub(r"^COMISI[ÓO]N\s+DE\s+", "", t, flags=re.I)
    # Si el nombre COMPLETO es una comisión real, no partirlo.
    if catalogo and _na(t) in catalogo:
        return _titulo_es(t.strip(" ,;"))
    partes = []
    for p in re.split(r"\s+Y\s+DE\s+|\s*;\s*", t, flags=re.I):
        p = p.strip(" ,;")
        # restos del separador cuando el texto trae variantes ("Y DE LA", "Y")
        p = re.sub(r"^(Y\s+DE\s+|Y\s+|DE\s+)", "", p, flags=re.I).strip(" ,;")
        if len(p) > 2:
            partes.append(_titulo_es(p))
    return "; ".join(dict.fromkeys(partes))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--periodos", nargs="*", default=list(PERIODOS))
    args = ap.parse_args()

    oficiales = []
    for periodo in args.periodos:
        for sec in SECCIONES:
            url = f"{BASE}/{periodo}/{sec}"
            try:
                r = requests.get(url, headers=HEADERS, timeout=60, verify=False)
                if r.status_code != 200:
                    logger.warning(f"  {url} → HTTP {r.status_code}")
                    continue
                its = parsear_seccion(r.text)
                logger.info(f"  {periodo}/{sec}: {len(its)} asuntos")
                oficiales.extend(its)
            except Exception as e:
                logger.warning(f"  {url} falló: {e}")

    con_turno = [o for o in oficiales if o["turno"]]
    logger.info(f"Total oficial: {len(oficiales)} asuntos · con turno: {len(con_turno)}")
    if not con_turno:
        logger.warning("Sin turnos que aplicar. Abortando sin escribir.")
        return 0

    conn = get_connection()
    filas = conn.execute(
        """SELECT id, titulo, comision FROM sil_documentos
           WHERE camara = 'Comisión Permanente'"""
    ).fetchall()
    catalogo = cargar_catalogo(conn)
    logger.info(f"Catálogo de comisiones conocidas: {len(catalogo)} nombres")
    idx: dict[str, list] = {}
    for rid, titulo, comision in filas:
        idx.setdefault(clave_objeto(titulo), []).append((rid, comision))

    updates, ya_tenian, sin_match = [], 0, 0
    for o in con_turno:
        hits = idx.get(clave_objeto(o["titulo"]))
        if not hits:
            sin_match += 1
            continue
        com = comisiones_del_turno(o["turno"], catalogo)
        if not com:
            continue
        for rid, comision_actual in hits:
            if comision_actual and comision_actual.strip():
                ya_tenian += 1
            else:
                updates.append((com, rid))

    logger.info(f"Emparejados con turno nuevo: {len(updates)} · "
                f"ya tenían comisión: {ya_tenian} · sin match: {sin_match}")
    if args.dry_run:
        for com, rid in updates[:10]:
            logger.info(f"    id={rid} → {com}")
        logger.info("DRY-RUN: no se escribe nada.")
        return 0
    if updates:
        conn.executemany(
            "UPDATE sil_documentos SET comision = ? WHERE id = ?", updates)
        conn.commit()
        logger.info(f"✅ {len(updates)} turnos escritos.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
