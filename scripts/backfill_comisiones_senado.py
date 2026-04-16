"""
Backfill de comisiones para senadores (LXVI Legislatura).

Contexto:
El scraper original (scrapers/legisladores.py :: _scrape_detalle_senador)
intentaba extraer comisiones parseando `soup.get_text()` línea a línea
después de encontrar el keyword "COMISIONES". Pero la palabra "COMISIONES"
aparece en el menú global del sitio del Senado, así que el modo
"in_comisiones" se activaba prematuramente y volcaba el menú entero
(links a "Centro de Estudios", "Instituto Belisario Domínguez",
"Contraloría Interna", etc.) hasta topar con "Informe" o "Turnado".
Resultado: 128/128 senadores con data corrupta o vacía en
`legisladores.comisiones_cargo` (hasta 288KB basura en un solo row).

Solución correcta (este script):
El sitio del Senado publica por separado la lista de comisiones en
/66/comisiones/ordinarias y /66/comisiones/especiales. Cada una tiene
página propia en /66/comisiones/{tipo}/{id} donde cada card de senador
tiene h3 con su cargo (Presidencia, Secretaría, Integrante). Invertimos:
scrapeamos las ~72 comisiones y construimos el mapeo
  sitl_id_senador -> [(comisión, cargo), ...]

Uso:
    .venv/bin/python scripts/backfill_comisiones_senado.py
    .venv/bin/python scripts/backfill_comisiones_senado.py --dry-run

Escribe directamente en semaforo.db (`legisladores.comisiones_cargo` y
`legisladores.comisiones`) para los 128 senadores LXVI. El radar_pipeline
luego empuja a D1 en el snapshot.
"""
from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
import time
from pathlib import Path

import cloudscraper
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "semaforo.db"

BASE = "https://www.senado.gob.mx"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
}

logger = logging.getLogger("backfill_comisiones_senado")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

# Rango de cargos: cargo con rank menor gana si un senador repite.
CARGO_RANK = {
    "Presidente": 0,
    "Secretaría": 1,
    "Junta Directiva": 2,
    "Integrante": 3,
}


def _normaliza_cargo(texto: str) -> str:
    t = (texto or "").strip().lower()
    if t.startswith("presiden"):
        return "Presidente"
    if t.startswith("secretar"):
        return "Secretaría"
    if t.startswith("junta"):
        return "Junta Directiva"
    # "Integrante", "Integrantes", cualquier otra cosa
    return "Integrante"


class SesionHolder:
    """
    Envoltura mutable alrededor de cloudscraper para permitir refresh
    cuando el WAF bloquea persistentemente (cookies de sesión quemadas
    después de ~50 requests).
    """

    def __init__(self) -> None:
        self.s: requests.Session = _nuevo_scraper()
        self.requests_desde_refresh = 0

    def refresh(self) -> None:
        logger.info("  → refrescando sesión cloudscraper (WAF persistente)")
        try:
            self.s.close()
        except Exception:
            pass
        time.sleep(15)
        self.s = _nuevo_scraper()
        self.requests_desde_refresh = 0


def _nuevo_scraper() -> requests.Session:
    s = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False}
    )
    s.headers.update(HEADERS)
    # Warm-up suave
    s.get(f"{BASE}/", timeout=30)
    time.sleep(2)
    s.get(
        f"{BASE}/66/comisiones",
        timeout=30,
        headers={"Referer": f"{BASE}/"},
    )
    time.sleep(2)
    return s


def crear_sesion() -> SesionHolder:
    return SesionHolder()


def descubre_comisiones(sess: "SesionHolder") -> list[tuple[str, str, str]]:
    """
    Devuelve lista de (tipo, cid, nombre).
    tipo ∈ {ordinarias, especiales}.
    """
    comisiones: list[tuple[str, str, str]] = []
    for tipo in ("ordinarias", "especiales"):
        url = f"{BASE}/66/comisiones/{tipo}"
        r = _fetch_con_reintentos(sess, url, f"{BASE}/66/comisiones")
        if r is None:
            logger.warning(f"[{tipo}] no disponible; skipping")
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        vistas: dict[str, str] = {}
        for a in soup.find_all("a", href=True):
            m = re.search(rf"/66/comisiones/{tipo}/(\d+)", a["href"])
            if not m:
                continue
            cid = m.group(1)
            nombre = a.get_text(strip=True)
            if (
                nombre
                and nombre.lower() not in ("integrantes", "integrante", "")
                and cid not in vistas
            ):
                vistas[cid] = nombre
        for cid, nom in vistas.items():
            comisiones.append((tipo, cid, nom))
        logger.info(f"  {tipo}: {len(vistas)} comisiones")
        time.sleep(2)
    return comisiones


def _fetch_con_reintentos(
    sess: "SesionHolder", url: str, referer: str, max_intentos: int = 5
) -> requests.Response | None:
    """
    Fetch con detección de Incapsula WAF. Si la respuesta es corta
    (< 3KB) probablemente es el iframe-bloqueo; espera y reintenta.
    Al 2º intento fallido consecutivo refresca la sesión cloudscraper
    (las cookies de Incapsula se queman después de ~50 requests).
    """
    for intento in range(1, max_intentos + 1):
        try:
            r = sess.s.get(url, timeout=30, headers={"Referer": referer})
        except Exception as e:
            logger.warning(f"  Excepción HTTP ({type(e).__name__}). "
                           f"Intento {intento}/{max_intentos}")
            time.sleep(10)
            if intento >= 2:
                sess.refresh()
            continue
        # Real WAF blocks son iframes de ~850 bytes. Páginas legítimas
        # pueden contener "_Incapsula_Resource" como recurso JS estático,
        # así que NO se usa como señal de bloqueo. Solo tamaño.
        if r.status_code == 200 and len(r.text) > 3000:
            sess.requests_desde_refresh += 1
            return r
        logger.warning(
            f"  WAF/error (status={r.status_code} len={len(r.text)}). "
            f"Intento {intento}/{max_intentos} — pausando…"
        )
        time.sleep(10)
        # Segundo fallo consecutivo ⇒ refrescar sesión
        if intento >= 2:
            sess.refresh()
    return None


def extrae_integrantes(
    sess: "SesionHolder", tipo: str, cid: str
) -> dict[str, str]:
    """
    Devuelve dict {senador_sitl_id -> cargo} para la comisión.
    Cada card tiene un h3 con el cargo y link /66/senador/XXXX.
    """
    url = f"{BASE}/66/comisiones/{tipo}/{cid}"
    r = _fetch_con_reintentos(sess, url, f"{BASE}/66/comisiones/{tipo}")
    if r is None:
        logger.warning(f"  {tipo}/{cid}: no se pudo obtener después de reintentos")
        return {}

    soup = BeautifulSoup(r.text, "html.parser")

    asignaciones: dict[str, str] = {}

    # Caso 1: cards estándar en div.col-sm-3 (Secretaría + Integrantes)
    # Caso 2: Presidencia en div.sectionMain u otro contenedor
    # Unificamos: por CADA link a /senador/ID, buscamos su card ancestral
    # y el h3 dentro de ese card.
    for a in soup.find_all("a", href=re.compile(r"/66/senador/\d+")):
        m = re.search(r"/senador/(\d+)", a["href"])
        if not m:
            continue
        sid = m.group(1)

        # Subir al card: primer ancestro div con h3 dentro que tenga cargo
        card = None
        node = a
        for _ in range(6):
            node = node.parent
            if node is None or node.name != "div":
                if node is None:
                    break
                continue
            h3 = node.find("h3")
            if h3 and _cargo_valido(h3.get_text(strip=True)):
                card = node
                break
        if card is None:
            cargo = "Integrante"
        else:
            cargo = _normaliza_cargo(card.find("h3").get_text(strip=True))

        prev = asignaciones.get(sid)
        if prev is None or CARGO_RANK[cargo] < CARGO_RANK[prev]:
            asignaciones[sid] = cargo

    return asignaciones


def _cargo_valido(texto: str) -> bool:
    t = (texto or "").strip().lower()
    return t.startswith(("presiden", "secretar", "integrante", "junta"))


def cargar_senadores(conn: sqlite3.Connection) -> dict[str, int]:
    """Devuelve {sitl_id -> legislador_id} de los 128 senadores LXVI."""
    out: dict[str, int] = {}
    for r in conn.execute(
        """
        SELECT id, sitl_id FROM legisladores
        WHERE camara = 'Senado'
          AND sitl_id IS NOT NULL AND sitl_id <> ''
        """
    ):
        out[str(r[1]).strip()] = int(r[0])
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--delay", type=float, default=5.0,
                    help="Segundos entre requests (evitar WAF Incapsula)")
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH)
    senadores = cargar_senadores(conn)
    logger.info(f"Senadores LXVI en BD: {len(senadores)}")

    logger.info("Creando sesión con warm-up anti-Incapsula…")
    sess = crear_sesion()

    logger.info("Descubriendo comisiones…")
    comisiones = descubre_comisiones(sess)
    logger.info(f"Total comisiones: {len(comisiones)}")

    # Mapeo inverso: {legislador_id -> [(comision_nombre, cargo), ...]}
    por_legislador: dict[int, list[tuple[str, str]]] = {}
    no_matcheados: set[str] = set()

    # Comisiones que fallaron en la primera pasada (las reintentamos
    # al final con sesión fresca).
    fallidas: list[tuple[str, str, str]] = []

    for i, (tipo, cid, nombre_com) in enumerate(comisiones, 1):
        # Refresh proactivo: cada 40 requests exitosos, renueva tokens
        # antes de que Incapsula tire la sesión.
        if sess.requests_desde_refresh >= 40:
            sess.refresh()
        logger.info(f"[{i:>2}/{len(comisiones)}] {tipo}/{cid}: {nombre_com[:60]}")
        integrantes = extrae_integrantes(sess, tipo, cid)
        if not integrantes:
            fallidas.append((tipo, cid, nombre_com))
        for sid, cargo in integrantes.items():
            leg_id = senadores.get(sid)
            if leg_id is None:
                no_matcheados.add(sid)
                continue
            por_legislador.setdefault(leg_id, []).append((nombre_com, cargo))
        time.sleep(args.delay)

    # Segunda pasada: reintenta las fallidas con sesión fresca
    if fallidas:
        logger.info(f"Reintentando {len(fallidas)} comisiones fallidas con sesión fresca…")
        sess.refresh()
        for tipo, cid, nombre_com in fallidas:
            if sess.requests_desde_refresh >= 40:
                sess.refresh()
            logger.info(f"[retry] {tipo}/{cid}: {nombre_com[:60]}")
            integrantes = extrae_integrantes(sess, tipo, cid)
            for sid, cargo in integrantes.items():
                leg_id = senadores.get(sid)
                if leg_id is None:
                    no_matcheados.add(sid)
                    continue
                por_legislador.setdefault(leg_id, []).append((nombre_com, cargo))
            time.sleep(args.delay)

    logger.info(f"Senadores con al menos una comisión: {len(por_legislador)}")
    if no_matcheados:
        logger.warning(
            f"Senate IDs sin match en legisladores.sitl_id: {len(no_matcheados)} "
            f"(muestra: {sorted(no_matcheados)[:8]})"
        )

    # Actualizar legisladores.comisiones_cargo y legisladores.comisiones
    if args.dry_run:
        logger.info("[DRY-RUN] — no se escribe a SQLite")
        muestra = list(por_legislador.items())[:3]
        for leg_id, coms in muestra:
            r = conn.execute(
                "SELECT nombre FROM legisladores WHERE id=?", (leg_id,)
            ).fetchone()
            print(f"\n{leg_id} {r[0] if r else '?'}:")
            # Orden: Presidente → Secretaría → Junta → Integrante
            coms_ord = sorted(coms, key=lambda x: CARGO_RANK.get(x[1], 9))
            for nom, cargo in coms_ord:
                print(f"  {cargo:>15}  {nom}")
        return 0

    # Escribir
    actualizados = 0
    for leg_id, coms in por_legislador.items():
        coms_ord = sorted(coms, key=lambda x: CARGO_RANK.get(x[1], 9))
        # Dedupe preservando orden: (comisión, cargo)
        visto: set[tuple[str, str]] = set()
        unicos: list[tuple[str, str]] = []
        for c in coms_ord:
            k = (c[0], c[1])
            if k in visto:
                continue
            visto.add(k)
            unicos.append(c)

        comisiones_cargo = "|".join(f"{nom}:{cargo}" for nom, cargo in unicos)
        comisiones_lista = "|".join(nom for nom, _ in unicos)

        conn.execute(
            """
            UPDATE legisladores
            SET comisiones = ?, comisiones_cargo = ?
            WHERE id = ?
            """,
            (comisiones_lista, comisiones_cargo, leg_id),
        )
        actualizados += 1

    # Senadores sin ninguna comisión detectada: limpiamos el campo corrupto
    limpiados = conn.execute(
        """
        UPDATE legisladores
        SET comisiones_cargo = '', comisiones = ''
        WHERE camara = 'Senado'
          AND id NOT IN ({})
        """.format(
            ",".join(str(lid) for lid in por_legislador.keys()) or "-1"
        )
    ).rowcount

    conn.commit()
    logger.info(
        f"Actualizados con comisiones: {actualizados} · "
        f"limpiados sin match: {limpiados}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
