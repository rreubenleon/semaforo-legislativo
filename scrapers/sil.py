"""
Scraper del Sistema de Información Legislativa (SIL)
https://sil.gobernacion.gob.mx

Estrategia de dos fases:
  Fase 1 (búsqueda): Extrae título + sinopsis + IDs de la búsqueda masiva
  Fase 2 (detalle):  Obtiene fecha, partido, comisión, estatus de cada ficha

El SIL devuelve resultados en una tabla gigante (1 celda/fila con HTML rico).
La ficha de detalle tiene una tabla clave-valor con todos los metadatos.
"""

import logging
import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CATEGORIAS, DATABASE

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
SIL_SEARCH = "https://sil.gobernacion.gob.mx/Librerias/Search/search_UTF.php"
SIL_DETALLE = "http://sil.gobernacion.gob.mx/Librerias/pp_ReporteSeguimiento.php"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SemaforoLegislativo/1.0)"}

# Partidos políticos mexicanos
PARTIDOS_MEXICO = {
    "MORENA": {"nombre": "Morena", "color": "#8B1A2B"},
    "PAN":    {"nombre": "PAN", "color": "#003B7A"},
    "PRI":    {"nombre": "PRI", "color": "#C4161C"},
    "PVEM":   {"nombre": "PVEM", "color": "#00813E"},
    "PT":     {"nombre": "PT", "color": "#CC0000"},
    "MC":     {"nombre": "Movimiento Ciudadano", "color": "#FF6600"},
    "PRD":    {"nombre": "PRD", "color": "#FFD700"},
    "NA":     {"nombre": "Nueva Alianza", "color": "#00B4AA"},
    "SIN PARTIDO": {"nombre": "Sin Partido", "color": "#999999"},
}


# ────────────────────────────────────────────
# Base de datos
# ────────────────────────────────────────────
def init_db():
    """Crea tabla para documentos del SIL."""
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sil_documentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            seguimiento_id TEXT NOT NULL,
            asunto_id TEXT NOT NULL,
            tipo TEXT,
            titulo TEXT NOT NULL,
            sinopsis TEXT,
            camara TEXT,
            fecha_presentacion TEXT,
            legislatura TEXT,
            periodo TEXT,
            estatus TEXT,
            partido TEXT,
            comision TEXT,
            categoria TEXT,
            fecha_scraping TEXT NOT NULL,
            UNIQUE(seguimiento_id, asunto_id)
        )
    """)
    conn.commit()
    conn.close()


# ────────────────────────────────────────────
# Fase 1: Búsqueda masiva
# ────────────────────────────────────────────
def _buscar_ids(query, max_resultados=500):
    """
    Busca en el SIL y extrae pares (seguimiento_id, asunto_id, titulo, sinopsis)
    de la tabla de resultados.  Cada fila tiene una sola celda <td> con:
      <div class="tdcriterio"><a href="...?Seguimiento=X&Asunto=Y"><b>TÍTULO</b></a>
       | <div class="badge">TIPO</div></div>
       ... sinopsis ...
    """
    try:
        resp = requests.get(
            SIL_SEARCH,
            params={"Valor": query},
            timeout=90,
            headers=HEADERS,
        )
        resp.encoding = "latin-1"
    except requests.RequestException as e:
        logger.warning(f"SIL búsqueda falló para '{query}': {e}")
        return []

    if resp.status_code != 200:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Encontrar la tabla grande (>50 filas)
    big_table = None
    for t in soup.find_all("table"):
        if len(t.find_all("tr")) > 50:
            big_table = t
            break

    if not big_table:
        logger.info(f"SIL query '{query}': sin tabla de resultados")
        return []

    rows = big_table.find_all("tr")[1:]  # saltar header
    resultados = []

    for row in rows[:max_resultados]:
        td = row.find("td")
        if not td:
            continue

        # Extraer link con IDs
        link = td.find("a", href=True)
        if not link:
            continue

        href = link.get("href", "")
        seg_match = re.search(r'Seguimiento=(\d+)', href)
        asu_match = re.search(r'Asunto=(\d+)', href)
        if not seg_match or not asu_match:
            continue

        seg_id = seg_match.group(1)
        asu_id = asu_match.group(1)

        # Título del <b> dentro del <a>
        titulo = link.get_text(strip=True)

        # Tipo del badge
        badge = td.find("div", class_=lambda c: c and "badge" in c)
        tipo_badge = badge.get_text(strip=True) if badge else ""

        # Sinopsis: texto después del tdcriterio
        full_text = td.get_text(strip=True)
        # Quitar título y badge del texto
        sinopsis = full_text
        if titulo in sinopsis:
            sinopsis = sinopsis.split(titulo, 1)[-1]
        # Limpiar
        sinopsis = re.sub(r'^\s*\|\s*(Asunto|Seguimiento)\s*', '', sinopsis)
        sinopsis = sinopsis[:500].strip()
        if sinopsis.startswith("..."):
            sinopsis = sinopsis[3:].strip()

        resultados.append({
            "seguimiento_id": seg_id,
            "asunto_id": asu_id,
            "titulo": titulo[:500],
            "sinopsis": sinopsis[:500],
            "tipo_badge": tipo_badge,
        })

    logger.info(f"SIL query '{query}': {len(resultados)} IDs encontrados")
    return resultados


# ────────────────────────────────────────────
# Fase 2: Detalle individual
# ────────────────────────────────────────────
def _obtener_detalle(seg_id, asu_id):
    """
    Obtiene metadatos de la ficha de seguimiento:
    Cámara, Fecha, Legislatura, Periodo, Tipo, Partido, Comisión, Estatus.
    La ficha tiene una tabla de ~11 filas con pares clave-valor.
    """
    try:
        resp = requests.get(
            SIL_DETALLE,
            params={"Seguimiento": seg_id, "Asunto": asu_id},
            timeout=30,
            headers=HEADERS,
        )
        resp.encoding = "latin-1"
    except requests.RequestException:
        return None

    if resp.status_code != 200 or len(resp.text) < 3000:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Buscar la tabla de metadatos (5-15 filas, 2 columnas)
    meta = {}
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if 5 <= len(rows) <= 20:
            for row in rows:
                cells = row.find_all("td")
                if len(cells) == 2:
                    key = cells[0].get_text(strip=True).lower()
                    val = cells[1].get_text(strip=True)
                    meta[key] = val
            if "fecha de presentación" in meta or "fecha de presentacion" in meta:
                break

    if not meta:
        return None

    # Parsear fecha
    fecha_raw = meta.get("fecha de presentación", meta.get("fecha de presentacion", ""))
    fecha = _parsear_fecha(fecha_raw)

    # Tipo de instrumento
    tipo = ""
    for k in meta:
        if k in ("iniciativa", "minuta", "proposición con punto de acuerdo",
                 "acuerdo parlamentario", "dictamen", "instrumento internacional"):
            tipo = k.capitalize()
            break
        # También buscar como valor
    if not tipo:
        for k, v in meta.items():
            if "iniciativa" in k or "proposición" in k or "dictamen" in k:
                tipo = k.capitalize()
                break

    # Extraer partido del campo "Presentador"
    partido = meta.get("presentador", "").strip().upper()
    # Normalizar
    if partido:
        for p in PARTIDOS_MEXICO:
            if p in partido:
                partido = p
                break

    # Comisión del "Último Trámite"
    tramite = meta.get("último trámite", meta.get("ultimo trámite", ""))
    comision = ""
    com_match = re.search(r'Comisión\s*(?:\(es\))?\s*:\s*(.+?)(?:\s+de\s+Cámara|\.\s|$)', tramite)
    if com_match:
        comision = com_match.group(1).strip()

    return {
        "camara": meta.get("cámara origen", meta.get("camara origen", "")),
        "fecha_presentacion": fecha,
        "legislatura": meta.get("legislatura", ""),
        "periodo": meta.get("periodo de sesiones", meta.get("periodo", "")),
        "tipo": tipo or meta.get("tipo", ""),
        "partido": partido,
        "comision": comision,
        "estatus": meta.get("último estatus", meta.get("ultimo estatus", "")),
    }


def _parsear_fecha(fecha_raw):
    """Parsea fecha del SIL a YYYY-MM-DD."""
    fecha_raw = fecha_raw.strip()
    for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"]:
        try:
            return datetime.strptime(fecha_raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', fecha_raw)
    if match:
        return match.group(0)
    match2 = re.search(r'(\d{2})/(\d{2})/(\d{4})', fecha_raw)
    if match2:
        return f"{match2.group(3)}-{match2.group(2)}-{match2.group(1)}"
    return fecha_raw[:10]


# ────────────────────────────────────────────
# Clasificación
# ────────────────────────────────────────────
def _clasificar_documento(titulo, sinopsis=""):
    """Clasifica un documento del SIL en nuestras 12 categorías."""
    texto = f"{titulo} {sinopsis}".lower()
    mejores = {}

    for cat_clave, cat_config in CATEGORIAS.items():
        score = 0
        for kw in cat_config["keywords"]:
            kw_lower = kw.lower()
            if len(kw_lower) <= 4:
                if re.search(r'\b' + re.escape(kw_lower) + r'\b', texto):
                    score += 1
            else:
                if kw_lower in texto:
                    score += 1
        if score >= 1:
            mejores[cat_clave] = score

    return max(mejores, key=mejores.get) if mejores else ""


# ────────────────────────────────────────────
# Orquestador principal
# ────────────────────────────────────────────
def scrape_sil_completo(fecha_desde="2025-09-01", detalle_max=200):
    """
    Pipeline completo del SIL:
    1. Busca IDs por cada categoría (keywords top)
    2. Deduplica
    3. Obtiene detalle de los primeros N documentos
    4. Clasifica y guarda en BD

    Args:
        fecha_desde: solo guardar docs con fecha >= esta
        detalle_max: max documentos a los que pedir detalle (rate limit)
    """
    init_db()
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))

    # Fase 1: recolectar IDs de todas las categorías
    todos_ids = {}  # key: (seg_id, asu_id) -> {titulo, sinopsis, tipo_badge}

    for cat_clave, cat_config in CATEGORIAS.items():
        queries = [kw for kw in cat_config["keywords"] if len(kw) >= 5][:4]

        for query in queries:
            resultados = _buscar_ids(query, max_resultados=200)
            for r in resultados:
                key = (r["seguimiento_id"], r["asunto_id"])
                if key not in todos_ids:
                    todos_ids[key] = r
            time.sleep(2)  # respetar servidor

    logger.info(f"SIL Fase 1: {len(todos_ids)} documentos únicos recolectados")

    # Fase 2: filtrar los que ya están en BD y obtener detalle
    nuevos = 0
    existentes = 0
    sin_detalle = 0
    detalles_obtenidos = 0

    for (seg_id, asu_id), info in todos_ids.items():
        # Verificar si ya existe
        row = conn.execute(
            "SELECT id FROM sil_documentos WHERE seguimiento_id=? AND asunto_id=?",
            (seg_id, asu_id)
        ).fetchone()
        if row:
            existentes += 1
            continue

        # Obtener detalle (con rate limit)
        detalle = None
        if detalles_obtenidos < detalle_max:
            detalle = _obtener_detalle(seg_id, asu_id)
            detalles_obtenidos += 1
            time.sleep(0.8)

        if detalle:
            fecha = detalle["fecha_presentacion"]
            if fecha and fecha < fecha_desde:
                continue

            categoria = _clasificar_documento(info["titulo"], info["sinopsis"])

            try:
                conn.execute("""
                    INSERT INTO sil_documentos
                        (seguimiento_id, asunto_id, tipo, titulo, sinopsis,
                         camara, fecha_presentacion, legislatura, periodo,
                         estatus, partido, comision, categoria, fecha_scraping)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    seg_id, asu_id,
                    detalle["tipo"], info["titulo"], info["sinopsis"],
                    detalle["camara"], fecha,
                    detalle["legislatura"], detalle["periodo"],
                    detalle["estatus"], detalle["partido"],
                    detalle["comision"], categoria,
                    datetime.now().isoformat(),
                ))
                nuevos += 1
            except sqlite3.IntegrityError:
                existentes += 1
        else:
            # Guardar sin detalle (solo título y sinopsis de búsqueda)
            categoria = _clasificar_documento(info["titulo"], info["sinopsis"])
            try:
                conn.execute("""
                    INSERT INTO sil_documentos
                        (seguimiento_id, asunto_id, tipo, titulo, sinopsis,
                         camara, fecha_presentacion, legislatura, periodo,
                         estatus, partido, comision, categoria, fecha_scraping)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    seg_id, asu_id,
                    info["tipo_badge"], info["titulo"], info["sinopsis"],
                    "", "", "", "", "", "", "", categoria,
                    datetime.now().isoformat(),
                ))
                sin_detalle += 1
                nuevos += 1
            except sqlite3.IntegrityError:
                existentes += 1

    conn.commit()
    conn.close()

    logger.info(
        f"SIL completo: {nuevos} nuevos ({detalles_obtenidos} con detalle, "
        f"{sin_detalle} sin detalle), {existentes} existentes"
    )

    return {
        "nuevos": nuevos,
        "con_detalle": detalles_obtenidos,
        "existentes": existentes,
        "total_ids": len(todos_ids),
    }


# ────────────────────────────────────────────
# Funciones para el dashboard y análisis
# ────────────────────────────────────────────
def contar_actividad_sil_por_fecha(categoria=None, dias=60):
    """Serie temporal: {fecha: count} de docs del SIL."""
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))

    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    if categoria:
        rows = conn.execute("""
            SELECT fecha_presentacion, COUNT(*) as n
            FROM sil_documentos
            WHERE categoria = ? AND fecha_presentacion >= ?
              AND fecha_presentacion != ''
            GROUP BY fecha_presentacion
        """, (categoria, fecha_limite)).fetchall()
    else:
        rows = conn.execute("""
            SELECT fecha_presentacion, COUNT(*) as n
            FROM sil_documentos
            WHERE fecha_presentacion >= ? AND fecha_presentacion != ''
            GROUP BY fecha_presentacion
        """, (fecha_limite,)).fetchall()

    conn.close()
    return {r[0]: r[1] for r in rows}


def obtener_serie_temporal_sil(categoria=None, dias=30):
    """Lista de {fecha, count} para los últimos N días (para gráficas)."""
    actividad = contar_actividad_sil_por_fecha(categoria, dias)
    hoy = datetime.now()
    return [
        {"fecha": (hoy - timedelta(days=i)).strftime("%Y-%m-%d"),
         "count": actividad.get((hoy - timedelta(days=i)).strftime("%Y-%m-%d"), 0)}
        for i in range(dias - 1, -1, -1)
    ]


def obtener_stats_por_partido(dias=180):
    """
    Estadísticas de instrumentos legislativos por partido.
    Retorna dict para el dashboard.
    """
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    # Total por partido
    rows_total = conn.execute("""
        SELECT partido, COUNT(*) as total
        FROM sil_documentos
        WHERE fecha_presentacion >= ? AND partido != '' AND partido IS NOT NULL
        GROUP BY partido ORDER BY total DESC
    """, (fecha_limite,)).fetchall()

    # Por partido y categoría
    rows_cat = conn.execute("""
        SELECT partido, categoria, COUNT(*) as n
        FROM sil_documentos
        WHERE fecha_presentacion >= ? AND partido != '' AND categoria != ''
        GROUP BY partido, categoria ORDER BY partido, n DESC
    """, (fecha_limite,)).fetchall()

    conn.close()

    partidos = {}
    for row in rows_total:
        p = row[0]
        meta = PARTIDOS_MEXICO.get(p, {"nombre": p, "color": "#666"})
        partidos[p] = {
            "nombre": meta["nombre"],
            "color": meta["color"],
            "total": row[1],
            "por_categoria": {},
            "top_categoria": None,
        }

    for row in rows_cat:
        p, cat, n = row
        if p in partidos:
            cat_nombre = CATEGORIAS.get(cat, {}).get("nombre", cat)
            partidos[p]["por_categoria"][cat] = {"nombre": cat_nombre, "count": n}
            if not partidos[p]["top_categoria"]:
                partidos[p]["top_categoria"] = {"categoria": cat, "nombre": cat_nombre, "count": n}

    return partidos


def obtener_conteo_sil():
    """Retorna conteo total de docs en SIL para estadísticas."""
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    total = conn.execute("SELECT COUNT(*) FROM sil_documentos").fetchone()[0]
    con_partido = conn.execute(
        "SELECT COUNT(*) FROM sil_documentos WHERE partido != '' AND partido IS NOT NULL"
    ).fetchone()[0]
    con_fecha = conn.execute(
        "SELECT COUNT(*) FROM sil_documentos WHERE fecha_presentacion != '' AND fecha_presentacion IS NOT NULL"
    ).fetchone()[0]
    conn.close()
    return {"total": total, "con_partido": con_partido, "con_fecha": con_fecha}


def enriquecer_fechas_sil(limite=200):
    """
    Busca documentos SIL sin fecha_presentacion y consulta el detalle
    para obtenerla. Útil para completar los ~5000 'Asuntos' sin fecha.

    Corre en lotes pequeños para no saturar el SIL.
    Solo funciona si sil.gobernacion.gob.mx responde (puede fallar por SSL).
    """
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))

    # Obtener docs sin fecha
    rows = conn.execute("""
        SELECT id, seguimiento_id, asunto_id, titulo FROM sil_documentos
        WHERE (fecha_presentacion = '' OR fecha_presentacion IS NULL)
        LIMIT ?
    """, (limite,)).fetchall()

    if not rows:
        logger.info("SIL enriquecimiento: todos los documentos ya tienen fecha")
        conn.close()
        return {"procesados": 0, "enriquecidos": 0, "fallidos": 0}

    logger.info(f"SIL enriquecimiento: procesando {len(rows)} documentos sin fecha")

    enriquecidos = 0
    fallidos = 0

    for row in rows:
        doc_id, seg_id, asu_id, titulo = row

        try:
            detalle = _obtener_detalle(seg_id, asu_id)
        except Exception:
            detalle = None

        if detalle and detalle.get("fecha_presentacion"):
            conn.execute("""
                UPDATE sil_documentos
                SET fecha_presentacion = ?,
                    legislatura = COALESCE(NULLIF(legislatura, ''), ?),
                    periodo = COALESCE(NULLIF(periodo, ''), ?),
                    partido = COALESCE(NULLIF(partido, ''), ?),
                    comision = COALESCE(NULLIF(comision, ''), ?),
                    estatus = COALESCE(NULLIF(estatus, ''), ?)
                WHERE id = ?
            """, (
                detalle["fecha_presentacion"],
                detalle.get("legislatura", ""),
                detalle.get("periodo", ""),
                detalle.get("partido", ""),
                detalle.get("comision", ""),
                detalle.get("estatus", ""),
                doc_id,
            ))
            enriquecidos += 1
        else:
            fallidos += 1

        time.sleep(0.5)

        # Commit cada 50
        if (enriquecidos + fallidos) % 50 == 0:
            conn.commit()
            logger.info(
                f"SIL enriquecimiento: {enriquecidos} enriquecidos, "
                f"{fallidos} sin fecha de {enriquecidos + fallidos}"
            )

    conn.commit()
    conn.close()

    logger.info(
        f"SIL enriquecimiento completado: {enriquecidos}/{len(rows)} enriquecidos"
    )
    return {
        "procesados": len(rows),
        "enriquecidos": enriquecidos,
        "fallidos": fallidos,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    print("=== SIL Scraper - Test rápido ===\n")

    # Test búsqueda
    ids = _buscar_ids("reforma energética")
    print(f"Resultados para 'reforma energética': {len(ids)}")
    for r in ids[:3]:
        print(f"  Seg={r['seguimiento_id']} Asu={r['asunto_id']}")
        print(f"  {r['titulo'][:80]}")
        print(f"  Badge: {r['tipo_badge']}")
        print()

    # Test detalle
    if ids:
        print("--- Obteniendo detalle del primer resultado ---")
        det = _obtener_detalle(ids[0]["seguimiento_id"], ids[0]["asunto_id"])
        if det:
            for k, v in det.items():
                print(f"  {k}: {v[:80] if isinstance(v, str) else v}")
