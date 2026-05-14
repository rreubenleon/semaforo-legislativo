"""
Scrape de iniciativaslxvi.php por TODAS las 53 comisiones LXVI Diputados
para construir mapping (titulo, fecha) → init_id (URL pública del SITL).

Fuente:
  https://sitl.diputados.gob.mx/LXVI_leg/iniciativaslxvi.php?comt={ID_COM}&tipo_turnot=1&edot=T

Cada fila en esa página tiene:
  - link href="dictameneslxvi_ld.php?init={N}"
  - título técnico de la iniciativa
  - fecha de presentación
  - presentador

Output:
  dashboard/diputados_urls_oficial.json con:
    {
      "instrumentos": [
        {"init_id": "1737", "titulo": "...", "fecha": "2025-09-23",
         "presentador": "...", "comision": 36, "url": "..."}
      ]
    }

Y también update sil_documentos.url donde matchee por (titulo, fecha,
apellido_presentador).

Idempotente.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import sys
import time
import unicodedata
from pathlib import Path

from datetime import datetime

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
OUT_JSON = ROOT / "dashboard" / "diputados_urls_oficial.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

BASE = "https://sitl.diputados.gob.mx/LXVI_leg"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "es-MX,es;q=0.9",
}
DELAY = 0.4


def normalizar(s):
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s.lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^\x00-\x7f]", "", s)
    return s


def core_titulo(s):
    n = normalizar(s)
    for p in [r"^proyecto\s+de\s+decreto\s+", r"^iniciativa\s+(?:con\s+proyecto\s+de\s+decreto\s+)?",
              r"^proposicion\s+con\s+punto\s+de\s+acuerdo\s*,?\s*"]:
        n = re.sub(p, "", n).strip()
    n = re.sub(r"(\d+)\s*[o°º]\.?", r"\1", n)
    n = re.sub(r"\s+", " ", n).strip(" .,;:")
    return n


def fecha_iso(f):
    """'17-Febrero-2026' o '17/02/2026' → '2026-02-17'."""
    MESES = {
        "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
        "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
        "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
    }
    if not f:
        return ""
    m = re.match(r"(\d{1,2})-(\w+)-(\d{4})", f.strip())
    if m:
        mes = MESES.get(m.group(2).lower(), "")
        if mes:
            return f"{m.group(3)}-{mes}-{m.group(1).zfill(2)}"
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", f.strip())
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    return ""


CACHE_DIR_URL = ROOT / "eval" / "diputados" / "cache_urls"
CACHE_DIR_URL.mkdir(parents=True, exist_ok=True)
CACHE_DIR_DICT = ROOT / "eval" / "diputados" / "cache_dictamen"
CACHE_DIR_DICT.mkdir(parents=True, exist_ok=True)


def fetch_url_dictamen(init_id: str) -> str:
    """
    Fetcha página dictameneslxvi_ld.php?init=N para obtener URL del PDF
    del DICTAMEN aprobado en la Gaceta Parlamentaria. Esta URL es DISTINTA
    a la URL de la iniciativa original.

      - URL iniciativa: gaceta.diputados.gob.mx/Gaceta/.../#IniciativaN
        → texto de la iniciativa presentada
      - URL dictamen:   gaceta.diputados.gob.mx/PDF/.../YYYYMMDD-V.pdf#page=N
        → texto del dictamen aprobado por la comisión

    Cachea por init_id.
    """
    cache_path = CACHE_DIR_DICT / f"{init_id}.html"
    if cache_path.exists() and cache_path.stat().st_size > 100:
        html = cache_path.read_text(encoding="utf-8")
    else:
        url = f"{BASE}/dictameneslxvi_ld.php?tipot=&pert=0&init={init_id}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15, verify=False)
            r.encoding = "utf-8"
        except Exception:
            return ""
        if r.status_code != 200:
            return ""
        html = r.text
        cache_path.write_text(html, encoding="utf-8")
        time.sleep(DELAY)
    # Buscar URL del PDF del dictamen
    m = re.search(
        r'https?://gaceta\.diputados\.gob\.mx/PDF/[^\s"\'>]+\.pdf(?:#page=\d+)?',
        html,
    )
    return m.group(0) if m else ""


def normalizar_comision_dip(nombre: str) -> str:
    """
    Normaliza el nombre de comisión Diputados al formato canónico que ya
    usa la BD: con prefijo "Comisión de ".

    El scraper anterior (scrapers/comisiones_sitl.py) inserta con prefijo.
    Mi scrape_urls_diputados.py inserta sin prefijo → causaba duplicación
    en comisiones_stats al hacer GROUP BY.

    Ejemplos:
      "Cambio Climático y Sostenibilidad" → "Comisión de Cambio Climático y Sostenibilidad"
      "Comisión de Justicia" (ya con prefijo) → mismo
      "Comisión Jurisdiccional" (sin "de") → mismo
    """
    if not nombre:
        return ""
    n = nombre.strip()
    # Si ya empieza con "Comisión", no tocar
    if n.startswith("Comisión "):
        return n
    return f"Comisión de {n}"


def scrape_comision(comt: int, endpoint: str = "iniciativaslxvi.php",
                    tipo_doc: str = "iniciativa", tipo_turnot: int = 1,
                    edot: str = "T") -> list[dict]:
    """
    Scrape `iniciativaslxvi.php` o `proposicioneslxvi.php` para una
    combinación específica (comisión × tipo_turnot × estado).

    Args:
      comt: ID de comisión LXVI (1-60)
      endpoint: 'iniciativaslxvi.php' o 'proposicioneslxvi.php'
      tipo_doc: 'iniciativa' o 'proposicion'
      tipo_turnot: 1=Única, 2=Unidas
      edot: T=Turnadas, P=Pendientes, A=Aprobadas, B=Bajadas/Desechadas

    Cachea HTML por combo en eval/diputados/cache_urls/ para que
    re-corridas sean instantáneas.
    """
    url = f"{BASE}/{endpoint}?comt={comt}&tipo_turnot={tipo_turnot}&edot={edot}"
    cache_path = CACHE_DIR_URL / f"{comt}_{tipo_doc[:3]}_t{tipo_turnot}_{edot}.html"

    if cache_path.exists() and cache_path.stat().st_size > 100:
        html = cache_path.read_text(encoding="utf-8")
    else:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30, verify=False)
            r.encoding = "utf-8"
        except Exception as e:
            logger.warning(f"  comt={comt} {tipo_doc[:3]} t{tipo_turnot} {edot} fail: {e}")
            return []
        if r.status_code != 200:
            return []
        html = r.text
        cache_path.write_text(html, encoding="utf-8")

    # Mapeo edot → estado humano
    edot_to_estado = {"T": "Turnada", "P": "Pendiente", "A": "Aprobada", "B": "Desechada/Retirada"}
    estado_default = edot_to_estado.get(edot, "Turnada")

    items = []
    r_text = html
    class FakeR:
        def __init__(self, t): self.text = t
    r = FakeR(html)

    # ── Parser de Pendientes y otros edot sin init= ──
    # El HTML tiene bloques con estructura: número + título + Proponente
    # + fechas + comisión. Para iniciativas: "Proyecto de decreto...".
    # Para proposiciones: "Por el que..." / "Por la que..." (no usa
    # "Proposición con punto"). Usamos "Proponente:" como ancla.
    if edot in ("P", "B"):
        # Limpiar HTML
        text_clean = re.sub(r"<script.*?</script>", " ", html, flags=re.DOTALL)
        text_clean = re.sub(r"<style.*?</style>", " ", text_clean, flags=re.DOTALL)
        text_clean = re.sub(r"<[^>]+>", " ", text_clean)
        text_clean = re.sub(r"&nbsp;", " ", text_clean)
        text_clean = re.sub(r"\s+", " ", text_clean)

        # Cada bloque: ANCLA "Proponente:" — antes está el título,
        # después está el resto (Publicación en Gaceta, Fecha, Comisión).
        # Splitear por "Proponente:" da N+1 piezas.
        partes = re.split(r"\bProponente:\s*", text_clean)
        for idx in range(len(partes) - 1):
            antes = partes[idx]      # contiene el título al final
            despues = partes[idx + 1]  # empieza con el proponente

            # Proponente: hasta el primer "(...)"
            m_prop = re.match(r"([^()]+\([^)]+\))", despues)
            if not m_prop:
                continue
            proponente = m_prop.group(1).strip()

            # Después del proponente: fechas, comisión, estado
            resto = despues[m_prop.end():]

            # Publicación en Gaceta y Fecha de presentación
            m_fpub = re.search(
                r"Publicaci[óo]n\s+en\s+Gaceta:\s*(\d{1,2}-[A-Za-zñÑáéíóú]+-\d{4})",
                resto,
            )
            fecha_pub_iso = fecha_iso(m_fpub.group(1)) if m_fpub else ""

            m_fpres = re.search(
                r"Fecha\s+de\s+presentaci[óo]n:\s*(\d{1,2}-[A-Za-zñÑáéíóú]+-\d{4})",
                resto,
            )
            fecha_pres_iso = fecha_iso(m_fpres.group(1)) if m_fpres else ""

            # Comisión: "- {COMISIÓN}" después de las fechas
            m_com = re.search(
                r"-\s+([A-ZÁÉÍÓÚÑa-záéíóúñ,\s]{6,150}?)\s+(?:-->|Único\.|Primero\.|La\s+Cámara|La\s+Comisión)",
                resto,
            )
            comision = m_com.group(1).strip() if m_com else ""

            # Título: REFACTOR 13-may. El bloque "antes" tiene metadata
            # mezclada con el título real:
            #   "- {COMISIÓN} --> [sinopsis]. Pendiente Publicación en
            #    Gaceta: {FECHA} {NUM_ORDEN} {TÍTULO REAL}"
            # Estrategia: limpiar metadata y aislar el título real.
            antes_clean = antes.strip()

            # Cortar metadata: "Pendiente Publicación en Gaceta: FECHA NUM"
            # → quedarse con lo que está DESPUÉS de esos campos.
            # FECHA = "DD-Mes-YYYY", NUM_ORDEN = 1-3 dígitos.
            m_after_meta = re.search(
                r"Pendiente\s+Publicaci[óo]n\s+en\s+Gaceta:\s*\d{1,2}-[A-Za-zñÑáéíóú]+-\d{4}\s+\d+\s+(.+)$",
                antes_clean,
            )
            if m_after_meta:
                titulo = m_after_meta.group(1).strip()
            else:
                # Sin metadata "Pendiente..." (algunos casos): tomar después del
                # ÚLTIMO patrón "{NUM} {Mayúscula}" en últimos 500 chars.
                m_num = re.search(
                    r"\b\d+\s+([A-ZÁÉÍÓÚ][^\d].+)$",
                    antes_clean[-500:],
                )
                titulo = m_num.group(1).strip() if m_num else ""

            # Limpieza adicional: quitar prefijo "- {COMISIÓN} --> [sinopsis]"
            # si aún quedó (regex de arriba puede haber fallado).
            titulo = re.sub(r"^[-•]\s*[A-ZÁÉÍÓÚÑa-záéíóúñ,\s]+-->\s*[^.]+\.\s*", "", titulo)

            titulo = re.sub(r"\s+", " ", titulo)[:500]
            if len(titulo) < 30:
                continue

            # Validar: el título debe empezar con un patrón legislativo
            # esperado. Si no, descartar (probablemente parseo falló).
            if not re.match(
                r"^(Proyecto\s+de\s+decreto|Iniciativa|Por\s+(?:el|la|los|las)\s+que|"
                r"Que\s+(?:reforma|adiciona|expide|deroga|exhorta)|Con\s+punto\s+de\s+acuerdo|"
                r"Que\s+expide)",
                titulo,
                re.IGNORECASE,
            ):
                continue

            # ID sintético
            import hashlib
            seed = f"{titulo[:80]}|{fecha_pres_iso or fecha_pub_iso}|{tipo_doc}"
            sid = hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]
            prefix = "PEND" if edot == "P" else "DESC"
            init_id = f"{prefix}_{sid}"

            estado_val = "Pendiente" if edot == "P" else "Desechada/Retirada"

            items.append({
                "init_id": init_id,
                "titulo": titulo,
                "tipo_doc": tipo_doc,
                "comision_dictaminadora": comision,
                "proponente": proponente,
                "fecha_presentacion": fecha_pres_iso,
                "fecha_dictamen": "",
                "fecha_gaceta": fecha_pub_iso,
                "estado": estado_val,
                "url": f"{BASE}/cuadro_asuntos_por_comisionlxvi.php?comt={comt}",
                "url_gaceta": "",
                "url_sitl": f"{BASE}/cuadro_asuntos_por_comisionlxvi.php?comt={comt}",
                "comt": comt,
                "tipo_turnot": tipo_turnot,
                "edot": edot,
            })
        return items
    init_positions = [(m.start(), m.group(1)) for m in re.finditer(r"init=(\d+)", r.text)]
    seen_ids = set()
    for pos, init_id in init_positions:
        if init_id in seen_ids:
            continue
        seen_ids.add(init_id)
        chunk = r.text[max(0, pos - 4000): pos + 3000]
        # URL gaceta cerca
        m_gac = re.search(r"https?://gaceta\.diputados\.gob\.mx/Gaceta/[^\s\"'>]+", chunk)
        url_gaceta = m_gac.group(0) if m_gac else ""
        # Título
        titulos_chunk = re.findall(
            r'class="Estilo7\d"[^>]*>(?:<[^>]+>)*([^<]{40,500})',
            chunk
        )
        titulo = ""
        for t in titulos_chunk:
            t_clean = re.sub(r"\s+", " ", t).strip()
            if len(t_clean) > 40 and not re.match(r"^[\d/\s\-A-Za-z]{0,30}$", t_clean[:30]):
                titulo = t_clean
                break
        if not titulo:
            for m in re.finditer(r">([^<]{60,400})<", chunk):
                t_clean = re.sub(r"\s+", " ", m.group(1)).strip()
                if len(t_clean) > 60 and not t_clean.startswith("Publicación"):
                    titulo = t_clean
                    break
        if not titulo:
            continue

        # Texto plano del bloque para extraer estado y fechas
        chunk_plain = re.sub(r"<[^>]+>", " ", chunk)
        chunk_plain = re.sub(r"\s+", " ", chunk_plain)

        # Estado: "Aprobada con fecha DD-Mes-YYYY" / "Pendiente" / "Desechada" / "Retirada"
        estado = "Pendiente"
        fecha_dictamen = ""
        m_aprobada = re.search(
            r"Aprobada\s+(?:con\s+fecha\s+)?(\d{1,2}-[A-Za-z]+-\d{4})",
            chunk_plain,
        )
        if m_aprobada:
            estado = "Aprobada"
            fecha_dictamen = fecha_iso(m_aprobada.group(1))
        elif "Desechada" in chunk_plain:
            estado = "Desechada"
        elif "Retirada" in chunk_plain:
            estado = "Retirada"

        # Comisión dictaminadora: aparece como "Cambio Climático y Sostenibilidad -->"
        # antes del título. Y a veces es "Comisiones Unidas de X y de Y -->".
        # Patrón: "- {COMISIÓN} -->" justo antes del texto explicativo.
        m_com = re.search(r"-\s+([A-ZÁÉÍÓÚÑa-záéíóúñ,\s]{6,150}?)\s+-->", chunk_plain)
        comision = m_com.group(1).strip() if m_com else ""

        # Proponente: "Proponente: Apellido Nombre (PARTIDO)"
        m_prop = re.search(r"Proponente:\s*([^\n]+?\([^)]+\))", chunk_plain)
        proponente = m_prop.group(1).strip() if m_prop else ""

        # Fecha de presentación: "Fecha de presentación: DD-Mes-YYYY"
        m_fpres = re.search(
            r"Fecha\s+de\s+presentación:\s*(\d{1,2}-[A-Za-z]+-\d{4})",
            chunk_plain,
        )
        fecha_pres = fecha_iso(m_fpres.group(1)) if m_fpres else ""

        # Publicación en Gaceta: "Publicación en Gaceta: DD-Mes-YYYY"
        m_fgac = re.search(
            r"Publicaci[óo]n\s+en\s+Gaceta:\s*(\d{1,2}-[A-Za-z]+-\d{4})",
            chunk_plain,
        )
        fecha_gaceta = fecha_iso(m_fgac.group(1)) if m_fgac else ""

        # Si no extrajo estado del HTML, usar el default del query (edot)
        if estado == "Pendiente" and not fecha_dictamen and edot != "P":
            estado = estado_default

        url_sitl = f"{BASE}/dictameneslxvi_ld.php?tipot=&pert=0&init={init_id}"
        items.append({
            "init_id": init_id,
            "titulo": titulo[:500],
            "tipo_doc": tipo_doc,
            "comision_dictaminadora": comision,
            "proponente": proponente,
            "fecha_presentacion": fecha_pres,
            "fecha_dictamen": fecha_dictamen,
            "fecha_gaceta": fecha_gaceta,
            "estado": estado,
            "url": url_gaceta or url_sitl,
            "url_gaceta": url_gaceta,
            "url_sitl": url_sitl,
            "comt": comt,
            "tipo_turnot": tipo_turnot,
            "edot": edot,
        })
    return items


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit-comisiones", type=int, default=0,
                    help="Solo N primeras comisiones (debug)")
    ap.add_argument("--solo-mes-actual", action="store_true",
                    help="Solo scrape estados que pueden cambiar (Pendientes "
                         "+ Aprobadas/Desechadas del mes actual). Salta histórico "
                         "ya inmutable. Ahorra ~70%% del tiempo de scrape.")
    args = ap.parse_args()

    # IDs de las 53 comisiones LXVI Diputados (rango 1-60 con gaps)
    comisiones = list(range(1, 61))
    if args.limit_comisiones:
        comisiones = comisiones[: args.limit_comisiones]

    # Iteración LXVI: 2 endpoints × 2 turnot × 4 estados = 16 combos/comisión
    # Modo --solo-mes-actual: solo Pendientes (P) y Aprobadas/Desechadas
    # del mes actual. Histórico ya en BD no se re-scrape.
    combos = []
    if args.solo_mes_actual:
        # Estados que pueden cambiar:
        #   P = Pendientes (puede pasar a Aprobada/Desechada)
        #   A = Aprobadas (filtramos por fecha del mes actual al insertar)
        #   B = Desechadas/Retiradas (igual)
        edots = ["P", "A", "B"]
        logger.info("MODO --solo-mes-actual: skip histórico inmutable")
    else:
        # Modo completo (migración inicial o workflow_dispatch manual):
        edots = ["T", "P", "A", "B"]
    for endpoint, tipo_doc in [
        ("iniciativaslxvi.php", "iniciativa"),
        ("proposicioneslxvi.php", "proposicion"),
    ]:
        for tipo_turnot in [1, 2]:
            for edot in edots:
                combos.append((endpoint, tipo_doc, tipo_turnot, edot))

    logger.info(f"Scrapeando {len(comisiones)} comisiones × {len(combos)} combinaciones = "
                f"{len(comisiones) * len(combos)} reqs LXVI Diputados…")

    todos_items = []
    total_reqs = len(comisiones) * len(combos)
    done = 0
    for i, comt in enumerate(comisiones, 1):
        for endpoint, tipo_doc, tipo_turnot, edot in combos:
            items = scrape_comision(comt, endpoint=endpoint, tipo_doc=tipo_doc,
                                    tipo_turnot=tipo_turnot, edot=edot)
            if items:
                todos_items.extend(items)
            done += 1
            time.sleep(DELAY)
        if i % 5 == 0:
            logger.info(f"  [{i}/{len(comisiones)}] reqs {done}/{total_reqs} · acumulados {len(todos_items)}")

    # Dedupe por init_id. Cuando un mismo init aparece en varios edot
    # (ej. Aprobada Y Turnada), preferir el que tiene fecha_dictamen
    # (= estado más informativo).
    seen = {}
    for it in todos_items:
        existing = seen.get(it["init_id"])
        if existing is None:
            seen[it["init_id"]] = it
        else:
            # Preferir el que tiene fecha_dictamen (Aprobada > Pendiente)
            if it["fecha_dictamen"] and not existing["fecha_dictamen"]:
                seen[it["init_id"]] = it
            # Preferir el que tiene comisión específica
            elif it["comision_dictaminadora"] and not existing["comision_dictaminadora"]:
                seen[it["init_id"]] = it
    todos_items = list(seen.values())
    logger.info(f"Total instrumentos únicos: {len(todos_items)}")
    # Stats por estado
    from collections import Counter
    por_estado = Counter(it["estado"] for it in todos_items)
    logger.info(f"  Por estado: {dict(por_estado)}")
    por_tipo = Counter(it["tipo_doc"] for it in todos_items)
    logger.info(f"  Por tipo: {dict(por_tipo)}")

    # Guardar JSON
    if not args.dry_run:
        OUT_JSON.write_text(json.dumps({
            "instrumentos": todos_items,
            "total": len(todos_items),
        }, ensure_ascii=False, indent=2))
        logger.info(f"  ✅ Guardado: {OUT_JSON}")

    # Construir mapping y aplicar URL a sil_documentos
    conn = sqlite3.connect(str(DB))
    # Asegurar columna url
    cols = {r[1] for r in conn.execute("PRAGMA table_info(sil_documentos)").fetchall()}
    if "url" not in cols:
        conn.execute("ALTER TABLE sil_documentos ADD COLUMN url TEXT DEFAULT ''")

    # Migración retroactiva: agregar prefijo "Comisión de " a docs SITL_*
    # antiguos que se insertaron sin prefijo (antes del fix de
    # normalizar_comision_dip). Idempotente.
    fix_prefijo = conn.execute("""
        UPDATE gaceta
           SET comision = 'Comisión de ' || comision
         WHERE numero_doc LIKE 'SITL_%'
           AND camara='Diputados'
           AND comision NOT LIKE 'Comisión%'
           AND comision != ''
    """).rowcount
    if fix_prefijo > 0:
        logger.info(f"Migración retroactiva: {fix_prefijo} docs Diputados con prefijo 'Comisión de' agregado")
    conn.commit()

    # Index por core_titulo[:60]. La página por comisión solo tiene fecha
    # del dictamen, no de la presentación, así que el match cross-fuente
    # tiene que ser por título. Riesgo de falso positivo bajo: el título
    # técnico de una iniciativa es bastante único en LXVI.
    idx = {}
    for it in todos_items:
        key = core_titulo(it["titulo"])[:60]
        idx.setdefault(key, []).append(it)

    # Recorrer Diputados sin URL
    rows = conn.execute("""
        SELECT id, titulo, fecha_presentacion
        FROM sil_documentos
        WHERE legislatura='LXVI' AND camara='Cámara de Diputados'
          AND (url IS NULL OR url = '')
          AND tipo_grupo IN ('Iniciativa', 'Proposición con Punto de Acuerdo')
    """).fetchall()
    logger.info(f"Diputados sin URL: {len(rows)}")

    matched = 0
    for row_id, titulo, fecha in rows:
        if not titulo:
            continue
        key = core_titulo(titulo)[:60]
        cands = idx.get(key, [])
        if not cands:
            continue
        url = cands[0]["url"]
        if not args.dry_run:
            conn.execute("UPDATE sil_documentos SET url = ? WHERE id = ?", (url, row_id))
        matched += 1
    if not args.dry_run:
        conn.commit()
    logger.info(f"BD updates sil_documentos URL: {matched}/{len(rows)} ({100*matched/max(len(rows),1):.1f}%)")

    # ── Insertar/actualizar TODOS los instrumentos LXVI en tabla `gaceta` ──
    # Antes: solo Aprobadas (322). Ahora: TODOS los estados (~6,000+ esperados).
    # Cada uno con su tipo correcto:
    #   - Aprobada (con fecha_dictamen) → tipo='dictamen'
    #   - Pendiente / Turnada / Desechada → tipo='iniciativa' o 'proposicion'
    # Esto alimenta:
    #   1. Tab Comisiones — histórico mensual real de toda la legislatura
    #   2. URLs Diputados — todos los instrumentos tienen url canónica
    #      de gaceta.diputados.gob.mx
    #
    # Filtros: solo instrumentos con título Y fecha (presentación o gaceta)
    # Modo --solo-mes-actual: filtrar candidatos al mes en curso (incluyendo
    # mes anterior por seguridad, dado que dictámenes pueden tener fecha
    # ligeramente desfasada).
    cutoff_fecha = ""
    if args.solo_mes_actual:
        from datetime import datetime as _dt, timedelta as _td
        cutoff = _dt.now() - _td(days=45)  # ~mes y medio para tener margen
        cutoff_fecha = cutoff.strftime("%Y-%m-%d")
        logger.info(f"  Filtrando candidatos con fecha >= {cutoff_fecha}")

    candidatos = []
    for it in todos_items:
        if not it["titulo"]:
            continue
        # Determinar fecha relevante (la del dictamen si existe, si no fecha_pres)
        fecha = it["fecha_dictamen"] or it["fecha_presentacion"] or it["fecha_gaceta"]
        if not fecha:
            continue
        # Modo --solo-mes-actual: descartar instrumentos viejos
        if cutoff_fecha and fecha < cutoff_fecha:
            continue
        # Determinar tipo en tabla gaceta
        if it["estado"] == "Aprobada" and it["fecha_dictamen"]:
            tipo_gaceta = "dictamen"
            fecha_gaceta_target = it["fecha_dictamen"]
        else:
            tipo_gaceta = "iniciativa" if it["tipo_doc"] == "iniciativa" else "proposicion"
            fecha_gaceta_target = it["fecha_presentacion"] or fecha
        candidatos.append((it, tipo_gaceta, fecha_gaceta_target))

    logger.info(f"\nCandidatos a insertar/actualizar en gaceta: {len(candidatos)}")
    aprobadas = [c for c in candidatos if c[1] == "dictamen"]
    iniciativas = [c for c in candidatos if c[1] == "iniciativa"]
    proposiciones = [c for c in candidatos if c[1] == "proposicion"]
    logger.info(f"  · Dictámenes:    {len(aprobadas)}")
    logger.info(f"  · Iniciativas:   {len(iniciativas)}")
    logger.info(f"  · Proposiciones: {len(proposiciones)}")

    inserted_dict = 0
    if not args.dry_run and candidatos:
        ahora_iso = datetime.now().isoformat()
        # CLEANUP CRÍTICO: borrar dictámenes Diputados SITL_* viejos que
        # tienen URL sin suffix -initN. En runs anteriores el UPDATE fallaba
        # con UNIQUE constraint cuando dos init compartían mismo PDF#page.
        # Re-creamos limpio para que el INSERT con suffix -initN no choque.
        n_borrados = conn.execute("""
            DELETE FROM gaceta
            WHERE numero_doc LIKE 'SITL_%'
              AND camara='Diputados'
              AND tipo='dictamen'
        """).rowcount
        logger.info(f"  Cleanup: {n_borrados} dictámenes Diputados borrados para re-insert limpio")
        conn.commit()
        # UNIQUE en gaceta puede ser url+fecha+camara o numero_doc.
        # Usamos SELECT primero para evitar IntegrityError si ya existe.
        # Para los DICTÁMENES aprobados, fetchar URL del PDF del dictamen
        # (distinta a la URL de la iniciativa original). El user reportó:
        # "click en Último Dictamen me lleva a una iniciativa, no al
        # dictamen que se vota". El URL real del DICTAMEN está solo en
        # la página de detalle dictameneslxvi_ld.php?init=N.
        logger.info("Resolviendo URLs de DICTÁMENES (PDF Gaceta)...")
        dictamenes_resolved = 0
        for it, tipo_gaceta, _ in candidatos:
            if tipo_gaceta == "dictamen" and not it["init_id"].startswith(("PEND_", "DESC_")):
                url_pdf_dict = fetch_url_dictamen(it["init_id"])
                if url_pdf_dict:
                    it["url_pdf_dictamen"] = url_pdf_dict
                    dictamenes_resolved += 1
        logger.info(f"  URLs PDF dictamen resueltas: {dictamenes_resolved}/{len(aprobadas)}")

        for it, tipo_gaceta, fecha_target in candidatos:
            url_gaceta = it["url_gaceta"]
            url_pdf_dict = it.get("url_pdf_dictamen", "")
            # URL única por init: agregar suffix de fragment para evitar
            # UNIQUE constraint cuando varios dictámenes comparten mismo
            # PDF del día (tabla gaceta tiene UNIQUE en url). Los PDF
            # readers ignoran fragments adicionales después de #page=N.
            if tipo_gaceta == "dictamen" and url_pdf_dict:
                # PDF del dictamen aprobado (lo que el user quiere ver).
                # Si url_pdf_dict ya tiene #page=N, le agregamos &init=N
                # como query-style después del fragment para hacerla única
                # (los browsers solo usan el primer fragment).
                if "#" in url_pdf_dict:
                    url_unico = f"{url_pdf_dict}-init{it['init_id']}"
                else:
                    url_unico = f"{url_pdf_dict}#init{it['init_id']}"
            elif url_gaceta:
                url_unico = f"{url_gaceta}#init{it['init_id']}"
            else:
                url_unico = f"{it['url_sitl']}#{it['init_id']}"
            numero_doc = f"SITL_{it['init_id']}"
            existing = conn.execute(
                "SELECT id FROM gaceta WHERE numero_doc = ?",
                (numero_doc,),
            ).fetchone()
            try:
                if existing:
                    conn.execute("""
                        UPDATE gaceta SET
                          titulo = ?, resumen = ?, fecha = ?, tipo = ?,
                          comision = ?, autor = ?, camara = 'Diputados',
                          url = ?, url_pdf = ?
                        WHERE id = ?
                    """, (
                        it["titulo"][:500],
                        it["titulo"][:1000],
                        fecha_target,
                        tipo_gaceta,
                        normalizar_comision_dip(it["comision_dictaminadora"])[:200],
                        it["proponente"][:200],
                        url_unico, url_gaceta or it["url_sitl"],
                        existing[0],
                    ))
                else:
                    conn.execute("""
                        INSERT INTO gaceta
                          (titulo, resumen, fecha, tipo, comision, autor, camara,
                           url, url_pdf, numero_doc, categorias, fecha_scraping)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        it["titulo"][:500],
                        it["titulo"][:1000],
                        fecha_target,
                        tipo_gaceta,
                        normalizar_comision_dip(it["comision_dictaminadora"])[:200],
                        it["proponente"][:200],
                        "Diputados",
                        url_unico,
                        url_gaceta or it["url_sitl"],
                        numero_doc,
                        "",
                        ahora_iso,
                    ))
                inserted_dict += 1
            except Exception as e:
                logger.warning(f"  insert/update gaceta fail (init={it['init_id']}, tipo={tipo_gaceta}): {e}")
        conn.commit()
    logger.info(f"\nInstrumentos insertados/actualizados en tabla gaceta: {inserted_dict}/{len(candidatos)}")

    if args.dry_run:
        logger.info("*** DRY RUN ***")


if __name__ == "__main__":
    main()
