"""
RESPALDO Senado LXVI — limpieza de la fila-basura Vargas en D1 + (a
futuro) relleno de biografías de senadores que el scraper primario
(scrapers/perfil_sil.py, SIL Gobernación) no cubrió.

═══════════════════════════════════════════════════════════════════
ESTADO (verificado en vivo 17-may-2026 — LEER ANTES DE TOCAR):
═══════════════════════════════════════════════════════════════════

1) Limpieza Vargas D1  → ACTIVA y verificada localmente.
   El senador real "Enrique Vargas Del Villar" (legisladores.id LOCAL
   = 543, nombre limpio de 25 chars, PAN, Senado) está BIEN en la BD
   local. En D1 existe ADEMÁS una fila-basura cuyo `nombre` es un
   fragmento de texto de iniciativa ("Enrique Vargas del Villar, del
   Grupo P…"). Esa fila vive SOLO en D1.

   scripts/limpiar_legisladores_fantasma.py NO la atrapa: deriva los
   IDs objetivo de la BD LOCAL (que está limpia → 0 fantasmas) y hace
   `return 0` temprano antes de tocar D1 (verificado leyendo el script:
   `if not fantasmas: return 0`). Por eso este script, con --d1, borra
   de D1 las filas de `legisladores` cuyo NOMBRE matchee los patrones
   de basura (LENGTH>80 / "proyecto de decreto" / "grupo
   parlamentario, con" / "se dio turno"), consultando D1 DIRECTAMENTE.
   El id real 543 tiene nombre de 25 chars: JAMÁS matchea esos
   patrones → queda intacto. La fila-basura es un id DISTINTO de 543
   (igual que el caso documentado id=685), así que borrarla no
   produce pérdida de datos.

2) Relleno de biografías  → PENDIENTE. No hay fuente estática fiable.
   Comprobado en vivo contra las fuentes oficiales:
     · www.senado.gob.mx/66/senador/{id} y sus variantes
       (/informacion_curricular, /datosCurricular/{id}, /curricula/{id}):
       el HTML estático es un cascarón de navegación; la "Información
       Curricular" se renderiza por JavaScript → `requests` no la ve.
     · SIL Gobernación pp_PerfilLegislador.php: para los 4 senadores
       que SÍ tienen Referencia (567/584/588/596) el perfil llega SIN
       "Preparación académica" ni "Experiencia legislativa" (bio
       vacía). La búsqueda por nombre pp_ListLegislador.php devolvió 0
       refs para los 8.
   No existe fuente OFICIAL que se pueda parsear con plain `requests`
   y verificar campo a campo. Regla dura del proyecto (incidentes
   previos costaron dinero): 0 datos malos. Por eso el scrape de bio
   queda DESACTIVADO por defecto y SOLO corre con
   --enable-bio-scrape, y aun así el parser RECHAZA cualquier texto
   que no tenga marcadores biográficos reales (nació/licenciado/…) y
   que parezca el cascarón de navegación o el widget de la Gaceta.
   Mientras no haya fuente fiable, este flag no debe usarse en el
   workflow.

Idempotente. --dry-run y --d1 (mismo patrón que
scripts/scrape_respaldo_diputados.py). El perfil, si algún día se
llena, se sincroniza a D1 con scripts/sync_perfil_d1.py.

Uso:
    python scripts/scrape_respaldo_senado.py --dry-run
    python scripts/scrape_respaldo_senado.py --d1            # limpia Vargas D1
    python scripts/scrape_respaldo_senado.py --enable-bio-scrape  # NO usar aún
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

from utils.matcher import (  # noqa: E402
    build_bd_index,
    encontrar_legislador_id,
    normalizar_nombre,
)

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
BASE = "https://www.senado.gob.mx"
LISTADO_URL = f"{BASE}/66/senadores/por_grupo_parlamentario"
PERFIL_URL = f"{BASE}/66/senador/{{sid}}"

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

DELAY = 1.5  # segundos entre requests (cortés con el Senado)

FUENTE = "SENADO-GOB"

ANIO_MIN = 1930
ANIO_MAX = 2006
BIO_MIN_CHARS = 220  # una semblanza real es larga; el cascarón es corto

MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
    "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
    "octubre": 10, "noviembre": 11, "diciembre": 12,
}

# Marcadores que DEBE tener un texto para ser una biografía real.
# Sin al menos uno, el texto se descarta (no se guarda nada).
MARCADORES_BIO = (
    "nació", "nacio", "es licenciad", "es licenciada", "licenciatura",
    "es ingenier", "es abogad", "es maestr", "es doctor", "es doctora",
    "es egresad", "estudió", "estudio la", "cursó", "se graduó",
    "obtuvo el grado", "tiene estudios", "es originari",
)

# Fragmentos que delatan el cascarón de navegación / widget de Gaceta
# (NO es biografía). Si aparece alguno, se descarta el texto entero.
BASURA_BIO = (
    "sobre el senado", "qué es el senado", "que es el senado",
    "resultados encontrados", "asuntos publicados en la gaceta",
    "descargar asuntos", "mapa del sitio", "aviso de privacidad",
    "iniciar sesión", "javascript", "no proporcion",
    "sin información", "sin informacion", "404", "not found",
)

# Patrones de basura para detectar la fila Vargas (y similares) en D1.
PATRONES_BASURA_D1 = (
    "LENGTH(nombre) > 80",
    "LOWER(nombre) LIKE '%proyecto de decreto%'",
    "LOWER(nombre) LIKE '%grupo parlamentario%, con%'",
    "LOWER(nombre) LIKE '%se dio turno%'",
)


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
    """GET robusto, decodifica como UTF-8 (senado.gob.mx sirve UTF-8)."""
    for i in range(intentos):
        try:
            r = session.get(url, timeout=(6, 25), verify=False)
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
# (Futuro) Directorio oficial del Senado → {nombre_norm: sid}
# ────────────────────────────────────────────
def construir_indice_senado(session: requests.Session) -> dict[str, int]:
    """{nombre_normalizado: senado_id} desde el directorio oficial.
    {} si no se pudo descargar/parsear (entonces NO se escribe nada)."""
    html = _get(session, LISTADO_URL)
    if not html:
        logger.error("Directorio del Senado no accesible.")
        return {}
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    idx: dict[str, int] = {}
    for a in soup.find_all("a", href=True):
        m = re.search(r"/66/senador/(\d+)", a["href"])
        if not m:
            continue
        sid = int(m.group(1))
        nombre = a.get_text(" ", strip=True)
        if not nombre:
            img = a.find("img", alt=True)
            nombre = (img.get("alt") if img else "") or ""
        nombre = re.sub(r"^\s*Sen(?:\.|adora?)\s+", "", nombre, flags=re.I)
        nombre = re.sub(r"\s+", " ", nombre).strip()
        nn = normalizar_nombre(nombre)
        if nn and nn not in idx:
            idx[nn] = sid
    logger.info(f"Directorio Senado: {len(idx)} senadores con id")
    return idx


def _mejor_sid(nombre_db: str, idx: dict[str, int]) -> int | None:
    nn = normalizar_nombre(nombre_db)
    if nn in idx:
        return idx[nn]
    db_tok = _tokens(nombre_db)
    if len(db_tok) < 2:
        return None
    candidatos: list[tuple[int, int]] = []
    for k, sid in idx.items():
        comunes = len(db_tok & _tokens(k))
        if comunes >= 2:
            candidatos.append((comunes, sid))
    if not candidatos:
        return None
    candidatos.sort(reverse=True)
    if len(candidatos) == 1 or candidatos[0][0] != candidatos[1][0]:
        return candidatos[0][1]
    return None


# ────────────────────────────────────────────
# (Futuro) Parser de perfil — endurecido contra el cascarón JS
# ────────────────────────────────────────────
def parsear_perfil_senado(html: str) -> dict:
    """
    Extrae {nombre, anio_nacimiento, fecha_nacimiento, biografia}.

    REGLA DURA: la biografía SOLO se devuelve si (a) supera
    BIO_MIN_CHARS, (b) contiene ≥1 MARCADORES_BIO (nació, licenciado,
    estudió, …) y (c) NO contiene NINGÚN fragmento de BASURA_BIO
    (cascarón de navegación / widget de la Gaceta). Si no, "". Esto
    impide guardar el cascarón JS que sirve hoy senado.gob.mx.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style", "noscript"]):
        t.decompose()

    nombre = ""
    if soup.title and soup.title.string:
        nombre = re.sub(r"^\s*Sen(?:\.|adora?)\s+", "",
                        soup.title.string, flags=re.I).strip()
        nombre = re.sub(r"\s*[-|].*$", "", nombre).strip()
    if not nombre:
        h1 = soup.find(["h1", "h2"])
        if h1:
            nombre = re.sub(r"^\s*Sen(?:\.|adora?)\s+", "",
                            h1.get_text(" ", strip=True), flags=re.I).strip()

    txt = soup.get_text("\n", strip=True)
    txt_flat = re.sub(r"\s+", " ", txt)

    anio = None
    fnac = ""
    m = re.search(
        r"(\d{1,2})\s+de\s+([A-Za-zÁÉÍÓÚáéíóúñ]+)\s+de\s+(\d{4})",
        txt_flat,
    )
    if m:
        dd = int(m.group(1))
        mes = _sin_acentos(m.group(2)).lower()
        yyyy = int(m.group(3))
        ctx = txt_flat[max(0, m.start() - 60): m.start()].lower()
        if (mes in MESES and 1 <= dd <= 31
                and ANIO_MIN <= yyyy <= ANIO_MAX
                and any(k in ctx for k in ("nac", "naci"))):
            anio = yyyy
            fnac = f"{dd:02d}/{MESES[mes]:02d}/{yyyy}"

    # Candidato a biografía: bloque tras una cabecera reconocible.
    biografia = ""
    for enc in ("SEMBLANZA", "Semblanza", "BIOGRAFÍA", "Biografía",
                "Biografia", "PERFIL"):
        i = txt.find("\n" + enc)
        if i == -1:
            i = txt.find(enc)
        if i == -1:
            continue
        cola = txt[i + len(enc):]
        for corte in ("Comisiones", "Iniciativas", "Proposiciones",
                      "Mapa del sitio", "Aviso de privacidad",
                      "Derechos Reservados", "Av. Paseo de la Reforma"):
            ci = cola.find(corte)
            if ci != -1:
                cola = cola[:ci]
        cand = re.sub(r"\s*\n\s*", " ", cola).strip()
        cand = re.sub(r"\s{2,}", " ", cand).strip(" :·-")
        low = cand.lower()
        if (len(cand) >= BIO_MIN_CHARS
                and any(mk in low for mk in MARCADORES_BIO)
                and not any(b in low for b in BASURA_BIO)):
            biografia = cand[:4000]
            break

    return {
        "nombre": nombre,
        "anio_nacimiento": anio,
        "fecha_nacimiento": fnac,
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
# Limpieza Vargas en D1 (consulta D1 directamente)
# ────────────────────────────────────────────
def limpiar_basura_d1(dry_run: bool = False) -> None:
    """
    Borra de D1 las filas de `legisladores` cuyo nombre matchee los
    patrones de basura (caso Vargas: nombre = fragmento de iniciativa).
    Consulta D1 DIRECTAMENTE — no deriva IDs de la BD local. El id real
    543 tiene nombre de 25 chars: NUNCA matchea estos patrones, queda
    intacto. Defensivo: solo DELETE WHERE <patrón>, nunca por id.
    """
    where = " OR ".join(PATRONES_BASURA_D1)
    sql = "\n".join([
        f"DELETE FROM legisladores_elo WHERE legislador_id IN "
        f"(SELECT id FROM legisladores WHERE {where});",
        f"DELETE FROM legisladores_stats WHERE legislador_id IN "
        f"(SELECT id FROM legisladores WHERE {where});",
        f"DELETE FROM legisladores_hit_rate WHERE legislador_id IN "
        f"(SELECT id FROM legisladores WHERE {where});",
        f"DELETE FROM legisladores_perfil WHERE legislador_id IN "
        f"(SELECT id FROM legisladores WHERE {where});",
        f"DELETE FROM legisladores WHERE {where};",
    ])
    if dry_run:
        logger.info("(dry-run) SQL de limpieza Vargas que se ejecutaría "
                    "en D1:\n%s", sql)
        return
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
            logger.error(f"wrangler (limpieza Vargas) stderr: "
                         f"{r.stderr[:1200]}")
        else:
            logger.info("✓ D1: filas-basura de legisladores "
                        "(patrón Vargas) eliminadas si existían")
    finally:
        Path(tmp_path).unlink()


# ────────────────────────────────────────────
# Orquestador
# ────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="no escribe nada; reporta lo que haría")
    ap.add_argument("--d1", action="store_true",
                    help="limpia la fila-basura Vargas en D1 vía wrangler "
                         "(requiere CLOUDFLARE_API_TOKEN). El perfil, si "
                         "se llena, se sincroniza con sync_perfil_d1.py.")
    ap.add_argument("--enable-bio-scrape", action="store_true",
                    help="NO USAR aún: intenta scrapear bios del Senado. "
                         "No hay fuente estática fiable (la curricular es "
                         "JS). El parser rechaza el cascarón; en la "
                         "práctica no escribirá nada hasta que exista "
                         "fuente verificable.")
    ap.add_argument("--limit", type=int, default=0,
                    help="procesa solo los primeros N objetivos (debug)")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    asegurar_tablas(conn)

    # ── Parte 2 (PENDIENTE): relleno de biografías. Solo si se opta-in
    #    explícito, y aun así el parser endurecido descartará el
    #    cascarón JS → en la práctica no escribe nada hoy.
    if args.enable_bio_scrape:
        objetivos = conn.execute(
            """
            SELECT l.id, l.nombre
            FROM legisladores l
            WHERE l.camara IN ('Senado', 'Cámara de Senadores')
              AND COALESCE(LENGTH(TRIM((
                  SELECT p.biografia FROM legisladores_perfil p
                   WHERE p.legislador_id = l.id))), 0) < 25
            ORDER BY l.id
            """
        ).fetchall()
        if args.limit:
            objetivos = objetivos[: args.limit]
        logger.info(f"[bio-scrape] objetivos Senado sin bio: "
                    f"{len(objetivos)}")

        session = requests.Session()
        session.headers.update(HEADERS)
        bd_idx = build_bd_index(conn)
        resultados: list[tuple[int, dict]] = []
        skip: list[tuple[str, str]] = []

        idx_senado = construir_indice_senado(session) if objetivos else {}
        if objetivos and not idx_senado:
            logger.error("Sin directorio del Senado → no se escribe perfil.")
        for o in objetivos if idx_senado else []:
            lid, nom_db = o["id"], o["nombre"]
            # Barrera #1: coherencia con el matcher robusto.
            encontrar_legislador_id(
                normalizar_nombre(nom_db), "Senado", bd_idx
            )
            sid = _mejor_sid(nom_db, idx_senado)
            if not sid:
                skip.append((nom_db, "no resuelto en directorio Senado"))
                continue
            html = _get(session, PERFIL_URL.format(sid=sid))
            time.sleep(DELAY)
            if not html:
                skip.append((nom_db, f"senado/{sid} no accesible"))
                continue
            d = parsear_perfil_senado(html)
            # Barrera #2: acuerdo de nombre ≥2 tokens.
            inter = len(_tokens(nom_db) & _tokens(d.get("nombre", "")))
            if inter < 2:
                skip.append((nom_db, f"nombre no concuerda "
                             f"perfil={d.get('nombre')!r} (senado/{sid})"))
                continue
            if not d["biografia"] and not d["anio_nacimiento"]:
                skip.append((nom_db, f"senado/{sid}: sin bio fiable "
                             f"(cascarón JS) ni año"))
                continue
            resultados.append((lid, d))

        logger.info("─" * 60)
        logger.info(f"[bio-scrape] escribibles: {len(resultados)}  "
                    f"saltados: {len(skip)}")
        for nom, motivo in skip:
            logger.info(f"   SKIP {nom!r}: {motivo}")

        if not args.dry_run and resultados:
            now = datetime.utcnow().isoformat()
            written = 0
            for lid, d in resultados:
                prev = conn.execute(
                    "SELECT biografia, anio_nacimiento FROM "
                    "legisladores_perfil WHERE legislador_id = ?",
                    (lid,),
                ).fetchone()
                prev_bio = (prev["biografia"] if prev else "") or ""
                bio_final = d["biografia"] or None
                if len(prev_bio.strip()) >= 25 and not d["biografia"]:
                    bio_final = prev_bio
                anio_final = d["anio_nacimiento"]
                if anio_final is None and prev and prev["anio_nacimiento"]:
                    anio_final = prev["anio_nacimiento"]
                if not bio_final and anio_final is None:
                    continue
                conn.execute(
                    """
                    INSERT INTO legisladores_perfil (
                        legislador_id, biografia, anio_nacimiento, genero,
                        profesion, estudios, twitter_handle, web_personal,
                        foto_hd_url, wikipedia_url, fuente_scraping,
                        fecha_scraping
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(legislador_id) DO UPDATE SET
                        biografia = excluded.biografia,
                        anio_nacimiento = excluded.anio_nacimiento,
                        fuente_scraping = excluded.fuente_scraping,
                        fecha_scraping = excluded.fecha_scraping
                    """,
                    (lid, bio_final, anio_final, None, None, None, None,
                     None, None, None, FUENTE, now),
                )
                written += 1
            conn.commit()
            logger.info(f"[bio-scrape] BD local: perfil escrito={written}")
    else:
        logger.info(
            "Relleno de biografías DESACTIVADO (no hay fuente estática "
            "fiable; ver docstring). Use --enable-bio-scrape solo cuando "
            "exista fuente verificable. Hoy no se toca legisladores_perfil."
        )

    # ── Parte 1 (ACTIVA): limpieza Vargas en D1.
    if args.d1 or args.dry_run:
        logger.info("Limpieza fila-basura Vargas en D1 (consulta D1 "
                    "directa; id real 543 intacto)…")
        limpiar_basura_d1(dry_run=args.dry_run or not args.d1)
        if args.d1 and not args.dry_run:
            logger.info(
                "Nota: el perfil se sincroniza a D1 con "
                "scripts/sync_perfil_d1.py (no lo hace este script)."
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
