"""
Scraper de RESPALDO para Diputados LXVI — rellena los huecos que el
scraper primario (scrapers/perfil_sil.py, SIL Gobernación) no logró
cubrir.

Dos fuentes OFICIALES de la Cámara de Diputados (sitl.diputados.gob.mx),
ambas estáticas (sin JS), servidas en UTF-8:

  1. PARTIDO (grupo parlamentario actual LXVI)
     `listado_diputados_gpnp.php?tipot=<N>` — listado oficial por Grupo
     Parlamentario. Hay una página por grupo; cada una se AUTO-IDENTIFICA
     por el logo del partido en la cabecera de la tabla `tablegpnp`.
     Mapeo logo→partido explícito y validado (LOGO_A_PARTIDO). Si una
     página trae un logo NO reconocido, se DESCARTA entera (nunca se
     adivina el partido). La unión de todas las páginas se valida contra
     la composición oficial publicada (MORENA/PAN/PVEM/PT/PRI/MC/IND)
     y contra 500 diputados totales; si no cuadra, se ABORTA el escribir
     partido (regla dura: 0 datos malos).

  2. PERFIL (año de nacimiento, escolaridad, biografía)
     `curricula.php?dipt=<SITL_ID>` — ficha oficial del diputado. Trae
     fecha de nacimiento, ESCOLARIDAD, TRAYECTORIA POLÍTICA, EXPERIENCIA
     LEGISLATIVA, etc. El año de nacimiento se extrae con regex estricto
     + rango sanity [1900,2010]; si no se puede extraer con confianza se
     deja en blanco (NO se inventa).

La llave de unión local↔fuente es `legisladores.sitl_id` == `dipt`
(ambos provienen del mismo sistema SITL). Además se exige acuerdo de
nombre (≥2 tokens en común, normalizado sin acentos) como segunda
barrera de seguridad antes de escribir nada.

Idempotente. Soporta --dry-run y --d1 (igual patrón que
scripts/limpiar_fantasmas_sil_inferido.py: el --d1 propaga a Cloudflare
D1 vía wrangler; los IDs están alineados local↔D1).

NO toca Senado: el sitl_id de Senado no resuelve contra curricula.php
(es otro sistema, senado.gob.mx, SPA sin fuente estática keyed por ese
ID) y además Senado no tiene hueco de partido. Senado queda PENDIENTE.

Uso:
    python scripts/scrape_respaldo_diputados.py --dry-run
    python scripts/scrape_respaldo_diputados.py
    python scripts/scrape_respaldo_diputados.py --d1
"""
from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import requests
import urllib3

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
DB = ROOT / "semaforo.db"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ────────────────────────────────────────────
# Config
# ────────────────────────────────────────────
BASE = "https://sitl.diputados.gob.mx/LXVI_leg"
ROSTER_URL = BASE + "/listado_diputados_gpnp.php?tipot={tipot}"
CURRICULA_URL = BASE + "/curricula.php?dipt={dipt}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

DELAY = 1.5  # segundos entre requests (cortés con la Cámara)

# tipot a barrer: cubre los 7 grupos + colchón por si SITL renumera.
TIPOTS = list(range(1, 16))

FUENTE = "SITL-CURRICULA"

# Logo de cabecera (en images/<slug>.<ext>) → partido canónico.
# Validado 16-may-2026 contra las 7 páginas reales:
#   tipot=14 LogoMorena.jpg→MORENA · tipot=3 pan.png→PAN
#   tipot=1 pri01.png→PRI · tipot=5 logvrd.jpg→PVEM
#   tipot=4 pt.gif→PT · tipot=6 logo_movimiento_ciudadano.png→MC
#   tipot=9 ind.webp→Sin Partido
# Si aparece un slug nuevo NO listado aquí, la página se descarta entera.
LOGO_A_PARTIDO = {
    "logomorena": "MORENA",
    "morena": "MORENA",
    "pan": "PAN",
    "logopan": "PAN",
    "pri01": "PRI",
    "pri": "PRI",
    "logopri": "PRI",
    "logvrd": "PVEM",
    "pvem": "PVEM",
    "logopvem": "PVEM",
    "pt": "PT",
    "logopt": "PT",
    "logo_movimiento_ciudadano": "MC",
    "mc": "MC",
    "logomc": "MC",
    "prd": "PRD",
    "logoprd": "PRD",
    "ind": "Sin Partido",
    "logoind": "Sin Partido",
}

# Composición oficial publicada por la Cámara (Datos al 23/03/2026).
# Se usa como guard-rail: si la unión de páginas no calza EXACTO con
# esto, no se escribe ningún partido (probable cambio de estructura).
COMPOSICION_OFICIAL = {
    "MORENA": 253,
    "PAN": 70,
    "PVEM": 62,
    "PT": 49,
    "PRI": 37,
    "MC": 28,
    "Sin Partido": 1,
}
TOTAL_OFICIAL = 500

MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
    "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
    "octubre": 10, "noviembre": 11, "diciembre": 12,
}

# Etiquetas de sección de la ficha curricula que componen la biografía.
SECCIONES_BIO = [
    "ESCOLARIDAD",
    "TRAYECTORIA POLÍTICA",
    "CARGOS DE ELECCIÓN POPULAR",
    "EXPERIENCIA LEGISLATIVA",
    "ADMINISTRACIÓN PÚBLICA FEDERAL",
    "ADMINISTRACIÓN PÚBLICA ESTATAL",
    "ACTIVIDAD EMPRESARIAL",
    "ACTIVIDAD DOCENTE",
    "ACTIVIDAD ACADÉMICA",
]
# Cola de la página que no es biografía (pie institucional).
CORTE_BIO = "Secretaría General"


# ────────────────────────────────────────────
# Utilidades de texto
# ────────────────────────────────────────────
def _sin_acentos(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c))


def _tokens(s: str) -> set:
    s = _sin_acentos(s).lower()
    return {t for t in re.sub(r"[^a-z ]", " ", s).split() if len(t) >= 2}


def _get(session: requests.Session, url: str, intentos: int = 3) -> str | None:
    """GET robusto, decodifica SIEMPRE como UTF-8 (SITL Diputados sirve
    Content-Type charset=UTF-8; forzar windows-1252 corrompe acentos)."""
    for i in range(intentos):
        try:
            r = session.get(url, timeout=(6, 20), verify=False)
            if r.status_code == 200 and len(r.content) > 400:
                r.encoding = "utf-8"
                return r.text
            if r.status_code == 404:
                return None
        except requests.RequestException as e:
            logger.debug(f"  GET {url} intento {i + 1}: {e}")
        time.sleep(DELAY * (i + 1))
    return None


# ────────────────────────────────────────────
# Fuente 1: roster por grupo parlamentario → dipt→partido
# ────────────────────────────────────────────
def _partido_de_pagina(html: str) -> str | None:
    """Auto-identifica el grupo por el logo en la cabecera de tablegpnp.
    Devuelve None si no hay logo reconocido (→ descartar la página)."""
    idx = html.find("tablegpnp")
    if idx == -1:
        return None
    head = html[idx: idx + 1500]
    for m in re.finditer(
        r'<img[^>]+src="[^"]*?([\w-]+)\.(?:png|jpe?g|gif|webp)"', head, re.I
    ):
        slug = m.group(1).lower()
        if slug in LOGO_A_PARTIDO:
            return LOGO_A_PARTIDO[slug]
        # logos genéricos que NO son de partido: seguir buscando
        if slug in ("logo_lxvi", "background", "banner", "verde"):
            continue
        # slug desconocido en zona de logo → no arriesgar
        return None
    return None


def _dipt_nombre_de_pagina(html: str) -> dict[int, str]:
    out: dict[int, str] = {}
    for m in re.finditer(
        r'curricula\.php\?dipt=(\d+)"[^>]*>\s*([^<]+)</a>', html
    ):
        dipt = int(m.group(1))
        nom = re.sub(r"^\d+\s+", "", m.group(2).strip())
        out[dipt] = nom
    return out


def construir_mapa_partidos(session: requests.Session) -> tuple[dict, dict]:
    """Devuelve (dipt→partido, dipt→nombre_oficial). {} si no se valida."""
    mapa: dict[int, str] = {}
    nombres: dict[int, str] = {}
    from collections import Counter

    for tp in TIPOTS:
        html = _get(session, ROSTER_URL.format(tipot=tp))
        if not html:
            continue
        dn = _dipt_nombre_de_pagina(html)
        if not dn:
            continue  # página vacía (tipot sin grupo)
        partido = _partido_de_pagina(html)
        if not partido:
            logger.warning(
                f"  tipot={tp}: {len(dn)} diputados pero logo de grupo "
                f"NO reconocido → página DESCARTADA (no se adivina)"
            )
            time.sleep(DELAY)
            continue
        for d, nm in dn.items():
            if d in mapa and mapa[d] != partido:
                logger.error(
                    f"  CONFLICTO dipt={d}: {mapa[d]} vs {partido} "
                    f"(tipot={tp}) → mapa de partidos INVÁLIDO"
                )
                return {}, {}
            mapa[d] = partido
            nombres[d] = nm
        logger.info(f"  tipot={tp:2d} → {partido:12s} ({len(dn)} diputados)")
        time.sleep(DELAY)

    dist = Counter(mapa.values())
    if len(mapa) != TOTAL_OFICIAL or dict(dist) != COMPOSICION_OFICIAL:
        logger.error(
            "Mapa de partidos NO calza con la composición oficial.\n"
            f"  total={len(mapa)} (esperado {TOTAL_OFICIAL})\n"
            f"  dist={dict(dist)}\n  oficial={COMPOSICION_OFICIAL}\n"
            "→ NO se escribirá ningún partido (regla dura: 0 datos malos)."
        )
        return {}, {}
    logger.info(
        f"Mapa de partidos validado: {len(mapa)} diputados, "
        f"composición oficial OK {dict(dist)}"
    )
    return mapa, nombres


# ────────────────────────────────────────────
# Fuente 2: ficha curricula → perfil
# ────────────────────────────────────────────
def parsear_curricula(html: str) -> dict:
    """Extrae campos de la ficha. Cada campo es opcional: si no se puede
    extraer con confianza se devuelve vacío/None (nunca se inventa)."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style"]):
        t.decompose()

    h1 = soup.find("h1")
    nombre = ""
    if h1:
        nombre = re.sub(r"^\s*Dip\.\s*", "", h1.get_text(" ", strip=True)).strip()

    txt = soup.get_text("\n", strip=True)

    # Año de nacimiento: línea tipo "9-enero - 1966" (estricto + rango).
    anio = None
    fnac = ""
    m = re.search(
        r"(\d{1,2})\s*-\s*([A-Za-zÁÉÍÓÚáéíóúñ]+)\s*-\s*['’]?(\d{4})",
        txt,
    )
    if m:
        dd = int(m.group(1))
        mes = _sin_acentos(m.group(2)).lower()
        yyyy = int(m.group(3))
        if mes in MESES and 1 <= dd <= 31 and 1900 <= yyyy <= 2010:
            anio = yyyy
            fnac = f"{dd:02d}/{MESES[mes]:02d}/{yyyy}"

    def _campo(label: str) -> str:
        mm = re.search(
            re.escape(label) + r"\s*:?\s*\n([^\n]+)", txt, re.I
        )
        if not mm:
            return ""
        v = mm.group(1).strip()
        return v if v and v.lower() not in ("no proporcionó", "no proporciono") else ""

    principio = _campo("Principio de elección")
    entidad = _campo("Entidad")
    distrito = _campo("Distrito")
    circ = _campo("Circunscripción")
    if not distrito and circ:
        distrito = circ

    # Suplente: solo si parece un nombre propio real (≥2 palabras
    # capitalizadas) — evita guardar basura como "de:".
    suplente = ""
    ms = re.search(r"Suplente\s*:?\s*\n([^\n]+)", txt, re.I)
    if ms:
        cand = ms.group(1).strip()
        pal = cand.split()
        if (
            len(pal) >= 2
            and all(p[:1].isalpha() for p in pal)
            and ":" not in cand
            and len(cand) <= 80
        ):
            suplente = cand

    # Escolaridad (estudios): bloque tras "ESCOLARIDAD" hasta la siguiente
    # sección o el corte institucional.
    estudios = ""
    me = re.search(r"\nESCOLARIDAD\n(.+?)(?=\n[A-ZÁÉÍÓÚ ]{6,}\n|\Z)", txt, re.S)
    if me:
        bloque = me.group(1).strip()
        if bloque and bloque.lower() not in ("no proporcionó", "no proporciono"):
            estudios = re.sub(r"\s*\n\s*", " · ", bloque).strip(" ·")[:600]

    # Biografía: concatena las secciones biográficas conocidas, recortando
    # el pie institucional. Solo se conserva si tiene sustancia.
    biografia = ""
    primer_idx = None
    for sec in SECCIONES_BIO:
        i = txt.find("\n" + sec + "\n")
        if i != -1 and (primer_idx is None or i < primer_idx):
            primer_idx = i
    if primer_idx is not None:
        cola = txt[primer_idx:]
        ci = cola.find(CORTE_BIO)
        if ci != -1:
            cola = cola[:ci]
        cola = re.sub(r"\s*\n\s*", " ", cola).strip()
        cola = re.sub(r"\s{2,}", " ", cola)
        if len(cola) >= 25:
            biografia = cola[:4000]

    return {
        "nombre": nombre,
        "anio_nacimiento": anio,
        "fecha_nacimiento": fnac,
        "principio_eleccion": principio,
        "entidad": entidad,
        "distrito": distrito,
        "suplente": suplente,
        "estudios": estudios,
        "biografia": biografia,
    }


# ────────────────────────────────────────────
# Esquema (espejo de scrapers/perfil_sil.py)
# ────────────────────────────────────────────
def asegurar_tablas(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS legisladores_perfil (
            legislador_id INTEGER PRIMARY KEY,
            biografia TEXT,
            anio_nacimiento INTEGER,
            genero TEXT,
            profesion TEXT,
            estudios TEXT,
            twitter_handle TEXT,
            web_personal TEXT,
            foto_hd_url TEXT,
            wikipedia_url TEXT,
            fuente_scraping TEXT,
            fecha_scraping TEXT
        )
        """
    )
    conn.commit()


# ────────────────────────────────────────────
# Orquestador
# ────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="no escribe nada; reporta lo que haría")
    ap.add_argument("--d1", action="store_true",
                    help="además propaga partido a D1 vía wrangler "
                         "(requiere CLOUDFLARE_API_TOKEN). El perfil se "
                         "sincroniza con scripts/sync_perfil_d1.py.")
    ap.add_argument("--limit", type=int, default=0,
                    help="procesa solo los primeros N objetivos (debug)")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    asegurar_tablas(conn)

    # Objetivo: Diputados con sitl_id numérico y (partido vacío O sin
    # fila en legisladores_perfil). El sitl_id GLOB '[0-9]*' garantiza
    # que es la llave curricula (no un "SIL:nnn" del scraper SIL).
    objetivos = conn.execute(
        """
        SELECT l.id, l.nombre, l.sitl_id, l.partido,
               (SELECT 1 FROM legisladores_perfil p
                 WHERE p.legislador_id = l.id) AS tiene_perfil
        FROM legisladores l
        WHERE l.camara LIKE '%iputad%'
          AND l.sitl_id GLOB '[0-9]*'
          AND ( (l.partido IS NULL OR l.partido = '')
                OR (SELECT 1 FROM legisladores_perfil p
                     WHERE p.legislador_id = l.id) IS NULL )
        ORDER BY l.id
        """
    ).fetchall()
    if args.limit:
        objetivos = objetivos[: args.limit]

    n_falta_part = sum(1 for o in objetivos if not (o["partido"] or "").strip())
    n_falta_perf = sum(1 for o in objetivos if not o["tiene_perfil"])
    logger.info(
        f"Objetivos Diputados: {len(objetivos)} "
        f"(sin partido: {n_falta_part}, sin perfil: {n_falta_perf})"
    )
    if not objetivos:
        logger.info("Nada que hacer.")
        return 0

    session = requests.Session()
    session.headers.update(HEADERS)

    # ── Fuente 1: mapa de partidos (solo si hay quien lo necesite)
    mapa_part: dict[int, str] = {}
    nom_oficial: dict[int, str] = {}
    if n_falta_part:
        logger.info("Construyendo mapa de partidos (roster oficial)…")
        mapa_part, nom_oficial = construir_mapa_partidos(session)
        if not mapa_part:
            logger.error(
                "Mapa de partidos no disponible/ inválido. "
                "Se continúa SOLO con perfil; partido NO se tocará."
            )

    # ── Procesar cada objetivo
    part_ok: list[tuple[int, str]] = []   # (legislador_id, partido)
    part_skip: list[tuple[int, str]] = []  # (sitl_id, motivo)
    perf_ok = 0
    perf_skip = 0
    cur_cache: dict[int, dict] = {}

    for o in objetivos:
        lid = o["id"]
        sid = int(o["sitl_id"])
        nom_db = o["nombre"]

        # — Partido —
        if not (o["partido"] or "").strip() and mapa_part:
            p = mapa_part.get(sid)
            if not p:
                part_skip.append((sid, "no está en roster oficial"))
            else:
                nm = nom_oficial.get(sid, "")
                inter = len(_tokens(nom_db) & _tokens(nm))
                if inter < 2:
                    part_skip.append(
                        (sid, f"nombre no concuerda db={nom_db!r} "
                              f"roster={nm!r} ov={inter}")
                    )
                else:
                    part_ok.append((lid, p))

        # — Perfil (curricula) —
        if not o["tiene_perfil"]:
            html = _get(session, CURRICULA_URL.format(dipt=sid))
            time.sleep(DELAY)
            if not html:
                perf_skip += 1
                logger.debug(f"  sitl={sid}: curricula no accesible → SKIP")
                continue
            d = parsear_curricula(html)
            cur_cache[lid] = d
            # acuerdo de nombre como barrera de seguridad
            inter = len(_tokens(nom_db) & _tokens(d.get("nombre", "")))
            tiene_algo = d["anio_nacimiento"] or d["biografia"] or d["estudios"]
            if inter < 2 or not tiene_algo:
                perf_skip += 1
                if inter < 2:
                    logger.debug(
                        f"  sitl={sid}: nombre no concuerda "
                        f"db={nom_db!r} ficha={d.get('nombre')!r} → SKIP"
                    )
                continue
            perf_ok += 1

    # ── Reporte
    logger.info("─" * 60)
    logger.info(f"PARTIDO  → escribibles: {len(part_ok)}  "
                f"saltados: {len(part_skip)}")
    for sid, motivo in part_skip:
        logger.info(f"   SKIP partido sitl={sid}: {motivo}")
    logger.info(f"PERFIL   → escribibles: {perf_ok}  saltados: {perf_skip}")

    if args.dry_run:
        logger.info("(dry-run: no se tocó la BD)")
        for lid, p in part_ok[:10]:
            r = conn.execute(
                "SELECT nombre FROM legisladores WHERE id=?", (lid,)
            ).fetchone()
            logger.info(f"   dry partido id={lid} {r['nombre']!r} → {p}")
        return 0

    # ── Escribir partido (local)
    for lid, p in part_ok:
        conn.execute(
            "UPDATE legisladores SET partido=? WHERE id=? "
            "AND (partido IS NULL OR partido='')",
            (p, lid),
        )

    # ── Escribir perfil (local, UPSERT idempotente — espejo perfil_sil)
    now = datetime.utcnow().isoformat()
    perf_written = 0
    for lid, d in cur_cache.items():
        nom_db = conn.execute(
            "SELECT nombre FROM legisladores WHERE id=?", (lid,)
        ).fetchone()["nombre"]
        inter = len(_tokens(nom_db) & _tokens(d.get("nombre", "")))
        tiene_algo = d["anio_nacimiento"] or d["biografia"] or d["estudios"]
        if inter < 2 or not tiene_algo:
            continue
        conn.execute(
            """
            INSERT INTO legisladores_perfil (
                legislador_id, biografia, anio_nacimiento, genero,
                profesion, estudios, twitter_handle, web_personal,
                foto_hd_url, wikipedia_url, fuente_scraping, fecha_scraping
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(legislador_id) DO UPDATE SET
                biografia = excluded.biografia,
                anio_nacimiento = excluded.anio_nacimiento,
                estudios = excluded.estudios,
                fuente_scraping = excluded.fuente_scraping,
                fecha_scraping = excluded.fecha_scraping
            """,
            (
                lid,
                d["biografia"] or None,
                d["anio_nacimiento"],
                None,
                None,
                d["estudios"] or None,
                None,
                None,
                None,
                None,
                FUENTE,
                now,
            ),
        )
        perf_written += 1

    conn.commit()
    logger.info(f"BD local: partido escrito={len(part_ok)}  "
                f"perfil escrito={perf_written}")

    # ── Propagar partido a D1 (radar lee legisladores.partido de D1)
    if args.d1 and part_ok:
        sql = "\n".join(
            f"UPDATE legisladores SET partido='{p}' WHERE id={lid} "
            f"AND (partido IS NULL OR partido='');"
            for lid, p in part_ok
        )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".sql", delete=False
        ) as tmp:
            tmp.write(sql)
            tmp_path = tmp.name
        try:
            r = subprocess.run(
                ["npx", "wrangler", "d1", "execute", "fiat-busqueda",
                 "--remote", "--file", tmp_path],
                cwd=ROOT / "worker", capture_output=True, text=True,
                timeout=180,
            )
            if r.returncode != 0:
                logger.error(f"wrangler stderr: {r.stderr[:1200]}")
            else:
                logger.info(f"✓ D1: partido actualizado para "
                            f"{len(part_ok)} diputados")
        finally:
            Path(tmp_path).unlink()
        logger.info(
            "Nota: el perfil se sincroniza a D1 con "
            "scripts/sync_perfil_d1.py (no lo hace este script)."
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
