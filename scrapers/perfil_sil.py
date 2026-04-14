"""
Scraper de perfiles SIL para Radar de Legisladores.

Fuente única: sil.gobernacion.gob.mx/Librerias/pp_PerfilLegislador.php?Referencia=N
Cubre Diputados y Senadores LXVI en un solo barrido. Filtra por periodo
de legislatura que inicie el 29/08/2024 (LXVI), descarta el resto.

Tablas destino:
  - legisladores_perfil       biografía, escolaridad, contacto
  - legisladores_trayectoria  trayectoria legislativa cross-periodo

Cadencia: semanal (domingo 12 UTC). El enriquecimiento biográfico es
estático; no tiene sentido refrescarlo cada 4 horas.
"""

import logging
import re
import sqlite3
import sys
import time
import unicodedata
import warnings
from datetime import datetime
from pathlib import Path

import requests
import urllib3
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────
# Configuración
# ────────────────────────────────────────────
SIL_URL = "https://sil.gobernacion.gob.mx/Librerias/pp_PerfilLegislador.php?SID=&Referencia={ref}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
}

# Rango empírico LXVI descubierto por bisección contra el SIL.
# Ampliado abril 2026: SIL sigue dando de alta suplentes/tardíos hasta
# ~+200 sobre la cota histórica. Mantener cotejado con el skip-list:
# si el scraper ya cacheó todo el rango, ampliarlo solo añade unos
# segundos por el skip-cache. Ajustar si cambia la numeración en
# legislaturas futuras.
LXVI_REF_MIN = 9228140
LXVI_REF_MAX = 9229640
LXVI_PERIODO = "29/08/2024"
LXVI_PERIODO_FIN = "31/08/2027"

DELAY_SEG = 2.5  # rate limit conservador para Gobernación (modo secuencial)
DELAY_SEG_PARALELO = 1.0  # rate limit por worker en modo paralelo (4 workers × 1s ≈ 4 req/s global)


# ────────────────────────────────────────────
# Utilidades
# ────────────────────────────────────────────
def _normalizar_nombre(nombre: str) -> str:
    """
    Normaliza un nombre para matching:
    'Sen. Alma Carolina Viggiano Austria (PRI)' → 'alma carolina viggiano austria'
    """
    if not nombre:
        return ""
    nombre = re.sub(r"^(Dip\.|Sen\.|C\.|Diputad[oa]|Senador[a]?)\s*", "", nombre.strip())
    nombre = re.sub(r"\s*\([^)]*\)\s*", "", nombre)
    nombre = nombre.lower().strip()
    nombre = "".join(
        c for c in unicodedata.normalize("NFKD", nombre) if not unicodedata.combining(c)
    )
    nombre = re.sub(r"[^a-z\s]", "", nombre)
    nombre = re.sub(r"\s+", " ", nombre).strip()
    return nombre


class _TimeoutError(Exception):
    pass


def _alarm_handler(signum, frame):
    raise _TimeoutError("Hard timeout por signal.alarm")


def _get(session: requests.Session | None, ref: int, intentos: int = 2) -> str | None:
    """
    Fetch robusto de un perfil SIL. Devuelve texto decodificado o None.
    Usa signal.alarm como hard kill (20s) para evitar que SIL deje
    el socket abierto sin datos indefinidamente.

    NOTA: signal.alarm solo funciona en el main thread. Para uso en
    ThreadPoolExecutor usar `_get_threadsafe` en su lugar.
    """
    import signal
    import threading

    # Si no estamos en main thread, delegar al threadsafe
    if threading.current_thread() is not threading.main_thread():
        return _get_threadsafe(session, ref, intentos)

    url = SIL_URL.format(ref=ref)
    old_handler = signal.signal(signal.SIGALRM, _alarm_handler)
    for intento in range(intentos):
        try:
            signal.alarm(20)  # hard kill en 20s
            resp = (session or requests).get(
                url,
                timeout=(5, 15),
                verify=False,
                headers=HEADERS,
            )
            signal.alarm(0)  # cancelar alarma
            resp.close()
            if resp.status_code == 200 and len(resp.content) > 5000:
                resp.encoding = "windows-1252"
                return resp.text
            return None
        except _TimeoutError:
            logger.debug(f"  Hard timeout ref {ref} (intento {intento + 1})")
            if intento + 1 < intentos:
                time.sleep(DELAY_SEG)
                continue
            return None
        except requests.RequestException:
            signal.alarm(0)
            if intento + 1 < intentos:
                time.sleep(DELAY_SEG * 2)
                continue
            return None
    signal.signal(signal.SIGALRM, old_handler)
    return None


def _get_threadsafe(session: requests.Session | None, ref: int, intentos: int = 2) -> str | None:
    """
    Variante thread-safe de `_get` para uso con ThreadPoolExecutor.
    No usa signal.alarm (no funciona fuera del main thread). En su
    lugar confía en el timeout del socket de requests, que es robusto
    si se combina con HTTPAdapter con pool y retries limitados.

    Timeouts: (connect=5s, read=12s). Si SIL cuelga el pipe, requests
    levanta ReadTimeout y reintentamos con backoff.
    """
    url = SIL_URL.format(ref=ref)
    sess = session or requests.Session()
    for intento in range(intentos):
        try:
            resp = sess.get(
                url,
                timeout=(5, 12),
                verify=False,
                headers=HEADERS,
            )
            try:
                if resp.status_code == 200 and len(resp.content) > 5000:
                    resp.encoding = "windows-1252"
                    return resp.text
                return None
            finally:
                resp.close()
        except requests.RequestException:
            if intento + 1 < intentos:
                time.sleep(2.0)
                continue
            return None
    return None


def _texto_celda(td) -> str:
    return td.get_text(" ", strip=True)


# ────────────────────────────────────────────
# Parser principal
# ────────────────────────────────────────────
def parsear_perfil(html: str, ref: int) -> dict | None:
    """
    Parsea un perfil SIL completo. Devuelve dict con:
      perfil, trayectoria, comisiones, es_lxvi, camara, nombre, partido
    None si no es un perfil válido o la legislatura no es LXVI.
    """
    if not html or "Perfil" not in html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # ── Header: tabla datosL con los campos personales
    datos = {}
    for table in soup.find_all("table"):
        cls = table.get("class") or []
        if "TFtable" in cls:
            for tr in table.find_all("tr"):
                cells = tr.find_all("td")
                if len(cells) == 2:
                    key = _texto_celda(cells[0]).rstrip(":").strip()
                    val = _texto_celda(cells[1])
                    if key and val:
                        datos[key] = val

    periodo = datos.get("Periodo de la legislatura", "")
    if LXVI_PERIODO not in periodo:
        return None  # descarta legislaturas anteriores

    # ── Nombre: primer match "Senadora/Diputado <nombre>"
    nombre = None
    camara = None
    nombre_re = re.search(
        r"(Senador[a]?|Diputad[oa])\s+([A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ\.\s]{5,80}?)(?:\s*<|\s*\(|$|\s{2,})",
        html,
    )
    if nombre_re:
        titulo = nombre_re.group(1)
        nombre = nombre_re.group(2).strip().rstrip(".").strip()
        camara = "Senado" if titulo.lower().startswith("senador") else "Cámara de Diputados"

    if not nombre or len(nombre) < 5:
        return None

    # ── Foto: el img con alt "Foto del Legislador"
    foto_hd_url = ""
    img = soup.find("img", alt=re.compile("Foto del Legislador", re.IGNORECASE))
    if img and img.get("src"):
        src = img["src"]
        if src.startswith("/"):
            foto_hd_url = f"https://sil.gobernacion.gob.mx{src}"
        elif src.startswith("http"):
            foto_hd_url = src
        else:
            foto_hd_url = f"https://sil.gobernacion.gob.mx/Librerias/{src}"

    # ── Perfil biográfico
    nacimiento_str = datos.get("Nacimiento", "")
    anio_nacimiento = None
    m_anio = re.search(r"(\d{4})", nacimiento_str)
    if m_anio:
        anio_nacimiento = int(m_anio.group(1))

    biografia = " ".join(
        filter(
            None,
            [
                datos.get("Preparación académica", ""),
                datos.get("Experiencia legislativa", ""),
            ],
        )
    ).strip()

    perfil = {
        "legislador_ref": ref,
        "nombre_sil": nombre,
        "camara_sil": camara,
        "partido": datos.get("Partido", ""),
        "biografia": biografia,
        "anio_nacimiento": anio_nacimiento,
        "profesion": datos.get("Experiencia legislativa", ""),
        "estudios": datos.get("Último grado de estudios", ""),
        "preparacion": datos.get("Preparación académica", ""),
        "email_institucional": datos.get("Correo electrónico", ""),
        "telefono": datos.get("Teléfono", ""),
        "suplente": datos.get("Suplente", ""),
        "ubicacion_oficina": datos.get("Ubicación", ""),
        "principio_eleccion": datos.get("Principio de elección", ""),
        "region_eleccion": datos.get("Región de elección", ""),
        "entidad": datos.get("Entidad", ""),
        "ciudad": datos.get("Ciudad", ""),
        "fecha_nacimiento": nacimiento_str,
        "foto_hd_url": foto_hd_url,
    }

    # ── Comisiones LXVI actuales (snapshot)
    comisiones = []
    for i, table in enumerate(soup.find_all("table")):
        cls = table.get("class") or []
        if "TFtable2" not in cls:
            continue
        # verificar si la tabla anterior era "COMISIONES"
        prev = table.find_previous("table", class_="datosL2")
        if prev and "COMISI" in _texto_celda(prev).upper():
            for tr in table.find_all("tr")[1:]:
                cells = [_texto_celda(c) for c in tr.find_all("td")]
                if len(cells) >= 5 and cells[4] == "Activo":
                    comisiones.append(
                        {
                            "comision": cells[0],
                            "puesto": cells[1],
                            "fecha_inicio": cells[2],
                            "fecha_fin": cells[3],
                        }
                    )
            break

    # ── Trayectoria legislativa cross-periodo
    # El HTML del SIL fusiona secciones en tablas gigantes y no permite
    # seccionar limpio por BeautifulSoup. Diferimos el parsing detallado
    # a Fase 5 (showcase Viggiano), cuando haya valor suficiente para
    # invertir en el DOM exacto. Por ahora, solo conservamos la
    # Experiencia legislativa declarada en el header.
    trayectoria = []
    exp_leg = datos.get("Experiencia legislativa", "")
    if exp_leg:
        trayectoria.append(
            {
                "del_anio": "",
                "al_anio": "",
                "descripcion": exp_leg,
            }
        )

    return {
        "perfil": perfil,
        "comisiones": comisiones,
        "trayectoria": trayectoria,
        "es_lxvi": True,
    }


# ────────────────────────────────────────────
# Descubrimiento de IDs LXVI
# ────────────────────────────────────────────
def descubrir_refs_lxvi(
    session: requests.Session,
    ref_min: int = LXVI_REF_MIN,
    ref_max: int = LXVI_REF_MAX,
    delay: float = DELAY_SEG,
) -> list[int]:
    """
    Barrido numérico sobre el rango LXVI. Devuelve lista de refs válidas.
    Se corre solo la primera vez; después se reutiliza el cache en
    legisladores_perfil.legislador_ref.
    """
    logger.info(
        f"Barrido SIL LXVI rango {ref_min}-{ref_max} "
        f"({ref_max - ref_min} candidatos, ~{(ref_max - ref_min) * delay / 60:.0f} min)"
    )
    validas = []
    for i, ref in enumerate(range(ref_min, ref_max + 1)):
        html = _get(session, ref)
        if html and LXVI_PERIODO in html:
            validas.append(ref)
        if (i + 1) % 100 == 0:
            logger.info(f"  Progreso: {i + 1}/{ref_max - ref_min}, válidas={len(validas)}")
        time.sleep(delay)
    logger.info(f"Barrido completo: {len(validas)} refs LXVI encontradas")
    return validas


# ────────────────────────────────────────────
# Persistencia
# ────────────────────────────────────────────
def _match_legislador_id(conn, nombre_sil: str, camara_sil: str | None) -> int | None:
    """Busca el legislador_id existente por nombre normalizado."""
    nombre_norm = _normalizar_nombre(nombre_sil)
    if not nombre_norm:
        return None

    # Match exacto
    row = conn.execute(
        "SELECT id FROM legisladores WHERE nombre_normalizado = ?",
        (nombre_norm,),
    ).fetchone()
    if row:
        return row[0]

    # Fallback: match por apellidos (últimas 2 palabras)
    partes = nombre_norm.split()
    if len(partes) >= 2:
        apellidos = " ".join(partes[-2:])
        row = conn.execute(
            "SELECT id FROM legisladores WHERE nombre_normalizado LIKE ? LIMIT 1",
            (f"%{apellidos}%",),
        ).fetchone()
        if row:
            return row[0]

    return None


def guardar_perfil(conn, parsed: dict) -> bool:
    """UPSERT idempotente de perfil + trayectoria."""
    perfil = parsed["perfil"]
    ref = perfil["legislador_ref"]
    leg_id = _match_legislador_id(conn, perfil["nombre_sil"], perfil["camara_sil"])

    if not leg_id:
        logger.debug(f"  Ref {ref}: sin match en legisladores ({perfil['nombre_sil'][:40]})")
        return False

    # legisladores_perfil
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
            profesion = excluded.profesion,
            estudios = excluded.estudios,
            foto_hd_url = excluded.foto_hd_url,
            fuente_scraping = excluded.fuente_scraping,
            fecha_scraping = excluded.fecha_scraping
        """,
        (
            leg_id,
            perfil["biografia"],
            perfil["anio_nacimiento"],
            None,
            perfil["profesion"],
            f'{perfil["estudios"]} - {perfil["preparacion"]}'.strip(" -"),
            None,
            None,
            perfil["foto_hd_url"],
            None,
            f"SIL:{ref}",
            datetime.utcnow().isoformat(),
        ),
    )

    # legisladores_trayectoria: una fila por paso legislativo histórico
    for paso in parsed["trayectoria"]:
        legislatura = paso.get("descripcion", "")[:120]
        fecha_ini = paso.get("del_anio", "")
        fecha_fin = paso.get("al_anio", "")
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO legisladores_trayectoria (
                    legislador_id, legislatura, cargo, camara, partido,
                    estado, distrito, principio_eleccion, comisiones,
                    fecha_inicio, fecha_fin, fuente
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    leg_id,
                    legislatura,
                    paso.get("descripcion", ""),
                    perfil["camara_sil"],
                    perfil["partido"],
                    perfil["entidad"],
                    "",
                    perfil["principio_eleccion"],
                    "",
                    fecha_ini,
                    fecha_fin,
                    f"SIL:{ref}",
                ),
            )
        except sqlite3.IntegrityError:
            pass

    # Guardar la ref SIL en legisladores.sitl_id si no existe
    # (no pisar el sitl_id original de SITL Diputados)
    conn.execute(
        """
        UPDATE legisladores
        SET sitl_id = COALESCE(NULLIF(sitl_id, ''), ?)
        WHERE id = ?
        """,
        (f"SIL:{ref}", leg_id),
    )

    return True


# ────────────────────────────────────────────
# Orquestador
# ────────────────────────────────────────────
def scrape_perfiles_sil(
    modo: str = "incremental",
    ref_min: int = LXVI_REF_MIN,
    ref_max: int = LXVI_REF_MAX,
    delay: float = DELAY_SEG,
) -> dict:
    """
    modo:
      - "descubrimiento": barre todo el rango LXVI y carga lo nuevo
      - "incremental":    solo refs ya conocidas en legisladores_perfil
    """
    conn = get_connection()
    session = requests.Session()
    session.headers.update(HEADERS)

    # Decidir el set de refs a procesar
    if modo == "incremental":
        cur = conn.execute(
            """
            SELECT DISTINCT CAST(SUBSTR(fuente_scraping, 5) AS INTEGER)
            FROM legisladores_perfil
            WHERE fuente_scraping LIKE 'SIL:%'
            """
        ).fetchall()
        refs = [r[0] for r in cur if r[0]]
        if not refs:
            logger.info("Sin refs en cache → cambio a modo descubrimiento")
            modo = "descubrimiento"

    if modo == "descubrimiento":
        refs = descubrir_refs_lxvi(session, ref_min, ref_max, delay)

    logger.info(f"Procesando {len(refs)} refs SIL en modo '{modo}'")

    stats = {"procesadas": 0, "guardadas": 0, "sin_match": 0, "errores": 0}

    for i, ref in enumerate(refs):
        html = _get(session, ref)
        if not html:
            stats["errores"] += 1
            time.sleep(delay)
            continue

        parsed = parsear_perfil(html, ref)
        if not parsed:
            stats["errores"] += 1
            time.sleep(delay)
            continue

        try:
            ok = guardar_perfil(conn, parsed)
            if ok:
                stats["guardadas"] += 1
            else:
                stats["sin_match"] += 1
        except (sqlite3.IntegrityError, sqlite3.OperationalError) as e:
            logger.warning(f"  Ref {ref}: error SQL {e}")
            stats["errores"] += 1

        stats["procesadas"] += 1

        if (i + 1) % 25 == 0:
            conn.commit()
            logger.info(f"  Avance: {stats}")

        time.sleep(delay)

    conn.commit()
    logger.info(f"Scraper SIL perfiles terminado: {stats}")
    return stats


# ────────────────────────────────────────────
# Main
# ────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--modo",
        choices=["descubrimiento", "incremental", "test"],
        default="test",
    )
    ap.add_argument("--ref", type=int, help="Referencia SIL individual para test")
    ap.add_argument("--ref-min", type=int, default=LXVI_REF_MIN)
    ap.add_argument("--ref-max", type=int, default=LXVI_REF_MAX)
    ap.add_argument("--delay", type=float, default=DELAY_SEG)
    args = ap.parse_args()

    if args.modo == "test":
        session = requests.Session()
        session.headers.update(HEADERS)
        target = args.ref or 9228960
        html = _get(session, target)
        parsed = parsear_perfil(html, target)
        if parsed:
            import json

            print(json.dumps(parsed, indent=2, ensure_ascii=False))
        else:
            print(f"No se pudo parsear ref {target}")
    else:
        scrape_perfiles_sil(modo=args.modo, ref_min=args.ref_min, ref_max=args.ref_max, delay=args.delay)
