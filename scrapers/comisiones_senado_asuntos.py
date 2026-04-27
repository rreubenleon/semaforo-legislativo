"""
Scraper de asuntos turnados por comisión — SENADO LXVI.

Fuente correcta (descubierta por iframe oculto en comisiones.senado.gob.mx):
    https://www.senado.gob.mx/64/frames/asuntosComisiones.php?idComision={ID}&tipoAsunto={TIPO}

Donde:
    ID    = 346..422 (catálogo construido desde /66/comisiones/ordinarias)
    TIPO  = iniciativas | proposiciones | dictamenes | minutas

Cada respuesta es HTML que contiene un panel con:
    "Número de Registros encontrados: <strong>NN</strong>"
y debajo, NN paneles, cada uno con un asunto turnado a esa comisión.

Defensas anti-bot del Senado:
    - Incapsula. Bloquea sin warm-up de sesión: devuelve "Loading" stub.
    - Solución: Session() con cookies persistentes + Referer apropiado +
      sleep generoso (8s) entre requests.

Uso:
    from scrapers.comisiones_senado_asuntos import scrape_senado_asuntos
    data = scrape_senado_asuntos()
    # data = {(idComision, nombre): {'iniciativas': N, 'proposiciones': N,
    #                                'dictamenes': N, 'minutas': N, 'turnadas': sum}}

CLI:
    .venv/bin/python -m scrapers.comisiones_senado_asuntos
    .venv/bin/python -m scrapers.comisiones_senado_asuntos --solo-id 414
    .venv/bin/python -m scrapers.comisiones_senado_asuntos --output /tmp/senado_asuntos.json
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import time
import urllib3
from pathlib import Path
from typing import Optional

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger("comisiones_senado_asuntos")

BASE = "https://www.senado.gob.mx"
FRAME_URL = f"{BASE}/64/frames/asuntosComisiones.php"
COMISIONES_HOME = "https://comisiones.senado.gob.mx"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
}

# Catálogo descubierto desde /66/comisiones/ordinarias
# (id, nombre canónico). Si Senado agrega nuevas comisiones, basta
# refrescar este catálogo con build_catalogo() y commit.
CATALOGO_LXVI = {
    346: "Puntos Constitucionales",
    347: "Estudios Legislativos",
    348: "Administración",
    349: "Agricultura",
    350: "Anticorrupción y Transparencia",
    351: "Asuntos de la Frontera Norte",
    352: "Asuntos de la Frontera Sur",
    353: "Asuntos Migratorios",
    354: "Bienestar",
    355: "Ciencia, Humanidades, Tecnología e Innovación",
    356: "Comunicaciones y Transportes",
    357: "Cultura",
    358: "Defensa de los Consumidores",
    359: "Defensa Nacional",
    360: "Deporte",
    361: "Derechos de la Niñez y de la Adolescencia",
    362: "Derechos Digitales",
    363: "Derechos Humanos",
    364: "Desarrollo Municipal",
    365: "Desarrollo Regional",
    366: "Desarrollo Rural",
    367: "Desarrollo Urbano y Ordenamiento Territorial",
    368: "Economía",
    369: "Educación",
    370: "Energía",
    371: "Estudios Legislativos, Primera",
    372: "Estudios Legislativos, Segunda",
    373: "Federalismo",
    374: "Ganadería",
    375: "Gobernación",
    376: "Hacienda y Crédito Público",
    377: "Jurisdiccional",
    378: "Justicia",
    379: "Juventud",
    380: "Marina",
    381: "Medio Ambiente, Recursos Naturales y Cambio Climático",
    382: "Minería",
    383: "Organismos Internacionales",
    384: "Para la Igualdad de Género",
    385: "Participación Ciudadana",
    386: "Pesca y Acuacultura",
    387: "Pueblos Indígenas y Afromexicanos",
    388: "Puertos e Infraestructura Marítima",
    389: "Radio, Televisión y Cinematografía",
    390: "Recursos Hídricos e Infraestructura Hidráulica",
    391: "Reforma Agraria",
    392: "Reglamentos y Prácticas Parlamentarias",
    393: "Relaciones Exteriores",
    394: "Relaciones Exteriores, África",
    395: "Relaciones Exteriores, América del Norte",
    396: "Relaciones Exteriores, América Latina y el Caribe",
    397: "Relaciones Exteriores, Asia-Pacífico",
    398: "Relaciones Exteriores, Europa",
    399: "Salud",
    400: "Seguridad Pública",
    401: "Seguridad Social",
    402: "Trabajo y Previsión Social",
    403: "Turismo",
    405: "Zonas Metropolitanas y Movilidad",
    406: "Medalla Belisario Domínguez",
    407: "Análisis, Seguimiento y Evaluación sobre la aplicación y desarrollo de la Inteligencia Artificial en México",
    411: "Reordenamiento Urbano y Vivienda",
    412: "Seguimiento a la Implementación y Revisión del T-MEC",
    414: "Guardia Nacional",
    415: "Infraestructura Ferroviaria",
    416: "Fomento Económico y al Emprendimiento",
    421: "Para el Seguimiento a la Implementación de la Agenda 2030",
    422: "Ciberseguridad",
}

# Slugs probables (para Referer realista; no es estricto)
def _slug_referer(nombre: str) -> str:
    s = nombre.lower()
    for old, new in [("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),("ñ","n")]:
        s = s.replace(old, new)
    s = re.sub(r"[^a-z0-9 ]", "", s)
    return s.replace(" ", "_")[:40]

TIPOS_ASUNTO = ("iniciativas", "proposiciones", "dictamenes", "minutas")

# Regex para parsear la respuesta del frame
RE_TOTAL = re.compile(r'encontrados:\s*<strong>(\d+)</strong>', re.IGNORECASE)
RE_LOADING = re.compile(r'<TITLE>Loading</TITLE>', re.IGNORECASE)


def _make_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(HEADERS)
    return sess


def _warmup_session(sess: requests.Session) -> bool:
    """Visita home de comisiones para que Incapsula deje cookies."""
    try:
        r = sess.get(f"{COMISIONES_HOME}/agricultura/iniciativas",
                     timeout=30, verify=False)
        return r.status_code == 200 and "Loading" not in r.text[:200]
    except Exception as e:
        logger.warning(f"warmup falló: {e}")
        return False


def _fetch_count(sess: requests.Session, id_com: int, tipo: str,
                 nombre_referer: str = "agricultura",
                 reintentos: int = 3) -> Optional[int]:
    """
    Consulta el frame y extrae el número de registros encontrados.
    Retorna None si Incapsula bloquea o si la respuesta no es parseable.
    """
    sess.headers["Referer"] = f"{COMISIONES_HOME}/{nombre_referer}/{tipo}"
    url = f"{FRAME_URL}?idComision={id_com}&tipoAsunto={tipo}"
    for intento in range(reintentos):
        try:
            r = sess.get(url, timeout=30, verify=False)
            if r.status_code != 200:
                logger.warning(f"  [{id_com}/{tipo}] HTTP {r.status_code}")
                time.sleep(10)
                continue
            if RE_LOADING.search(r.text):
                logger.warning(f"  [{id_com}/{tipo}] Incapsula 'Loading' stub, reintento {intento+1}")
                time.sleep(20)
                _warmup_session(sess)
                continue
            m = RE_TOTAL.search(r.text)
            if m:
                return int(m.group(1))
            logger.warning(f"  [{id_com}/{tipo}] sin match de 'encontrados'")
            return 0  # tabla vacía
        except Exception as e:
            logger.warning(f"  [{id_com}/{tipo}] error: {e}")
            time.sleep(5)
    return None


def scrape_senado_asuntos(catalogo: dict = None,
                          delay: float = 8.0,
                          checkpoint: Optional[Path] = None) -> dict:
    """
    Scrapea TODAS las comisiones del catálogo, todos los tipos.

    Args:
        catalogo: dict {id: nombre}. Default: CATALOGO_LXVI.
        delay: segundos entre cada request (recomendado >= 6).
        checkpoint: si se da, guarda resultados parciales en cada comisión.

    Returns:
        dict {(id, nombre): {iniciativas, proposiciones, dictamenes, minutas, turnadas, scrape_ts}}
    """
    catalogo = catalogo or CATALOGO_LXVI
    sess = _make_session()

    if not _warmup_session(sess):
        logger.error("Warmup falló — Incapsula activo. Aborto.")
        return {}
    time.sleep(delay)

    out = {}
    if checkpoint and checkpoint.exists():
        try:
            out = {tuple(int(x) if x.isdigit() else x for x in k.split("|", 1)): v
                   for k, v in json.loads(checkpoint.read_text()).items()}
            logger.info(f"Reanudo desde checkpoint con {len(out)} comisiones ya hechas")
        except Exception:
            out = {}

    total = len(catalogo)
    for i, (id_com, nombre) in enumerate(sorted(catalogo.items()), 1):
        if (id_com, nombre) in out:
            logger.info(f"[{i:3}/{total}] {id_com} {nombre[:40]} → cached, skip")
            continue
        logger.info(f"[{i:3}/{total}] {id_com} {nombre[:40]}")
        slug = _slug_referer(nombre)
        counts = {}
        for tipo in TIPOS_ASUNTO:
            n = _fetch_count(sess, id_com, tipo, nombre_referer=slug)
            counts[tipo] = n if n is not None else 0
            time.sleep(delay)
        counts["turnadas"] = sum(counts.values())
        counts["scrape_ts"] = time.strftime("%Y-%m-%d %H:%M:%S")
        out[(id_com, nombre)] = counts
        logger.info(
            f"           ini={counts['iniciativas']} prop={counts['proposiciones']} "
            f"dict={counts['dictamenes']} min={counts['minutas']} → total={counts['turnadas']}"
        )
        if checkpoint:
            # Serializar tuplas como "id|nombre"
            ser = {f"{k[0]}|{k[1]}": v for k, v in out.items()}
            checkpoint.write_text(json.dumps(ser, ensure_ascii=False, indent=2))

    return out


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    p = argparse.ArgumentParser()
    p.add_argument("--solo-id", type=int, default=None,
                   help="Scrapea solo una comisión (debug)")
    p.add_argument("--delay", type=float, default=8.0)
    p.add_argument("--output", type=str, default="/tmp/senado_asuntos.json")
    p.add_argument("--checkpoint", type=str, default="/tmp/senado_asuntos_ckpt.json")
    args = p.parse_args()

    catalogo = CATALOGO_LXVI
    if args.solo_id:
        catalogo = {args.solo_id: CATALOGO_LXVI.get(args.solo_id, f"comision_{args.solo_id}")}

    data = scrape_senado_asuntos(catalogo, delay=args.delay,
                                 checkpoint=Path(args.checkpoint))

    # Serializar a JSON
    out_path = Path(args.output)
    ser = {f"{k[0]}|{k[1]}": v for k, v in data.items()}
    out_path.write_text(json.dumps(ser, ensure_ascii=False, indent=2))
    logger.info(f"\nGuardado en {out_path} ({len(data)} comisiones)")

    # Resumen
    total_ini = sum(v["iniciativas"] for v in data.values())
    total_prop = sum(v["proposiciones"] for v in data.values())
    total_dict = sum(v["dictamenes"] for v in data.values())
    total_min = sum(v["minutas"] for v in data.values())
    print(f"\n=== RESUMEN ===")
    print(f"Total iniciativas LXVI:    {total_ini}")
    print(f"Total proposiciones LXVI:  {total_prop}")
    print(f"Total dictámenes LXVI:     {total_dict}")
    print(f"Total minutas LXVI:        {total_min}")
    print(f"GRAN TOTAL:                {total_ini + total_prop + total_dict + total_min}")


if __name__ == "__main__":
    main()
