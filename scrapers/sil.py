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

# Claves válidas de partidos (para normalización)
PARTIDOS_VALIDOS = set(PARTIDOS_MEXICO.keys())


def normalizar_partido(texto_presentador):
    """
    Normaliza el campo 'Presentador' del SIL al partido real.
    Ej: 'Dip. Juan Pérez (PAN)' → 'PAN'
        'EJECUTIVO FEDERAL' → 'EJECUTIVO FEDERAL' (no es partido, se guarda como tipo)
        'GOBIERNO DE HIDALGO' → '' (no es legislador federal)
    Retorna (partido, tipo_presentador):
      - partido: clave de PARTIDOS_MEXICO o ''
      - tipo_presentador: 'legislador', 'ejecutivo', 'gobierno_estatal',
                          'congreso_estatal', 'organismo', 'comision', 'otro'
    """
    if not texto_presentador:
        return "", "otro"

    texto = texto_presentador.strip().upper()

    # Ordenar partidos de mayor a menor longitud para evitar matches parciales
    # Ej: "MORENA" debe matchear antes que "NA"
    import re as _re
    partidos_ordenados = sorted(PARTIDOS_VALIDOS, key=len, reverse=True)

    # 1. Buscar partido explícito entre paréntesis: "Dip. Nombre (PAN)"
    match_partido = _re.search(r'\(([^)]+)\)', texto)
    if match_partido:
        contenido = match_partido.group(1).strip()
        for p in partidos_ordenados:
            if p in contenido:
                return p, "legislador"
        # Partido independiente
        if "IND" in contenido:
            return "SIN PARTIDO", "legislador"

    # 2. Buscar partido en el texto directo
    for p in partidos_ordenados:
        if p == texto or texto.startswith(f"{p} ") or texto.endswith(f" {p}"):
            return p, "legislador"

    # 3. Clasificar por tipo de presentador no-partido
    if any(x in texto for x in ["EJECUTIVO FEDERAL", "PRESIDENCIA", "PRESIDENTE"]):
        return "", "ejecutivo"
    if any(x in texto for x in ["GOBIERNO DE ", "GOBERNADOR", "GOBERNADORA"]):
        return "", "gobierno_estatal"
    if any(x in texto for x in ["CONGRESO DE ", "LEGISLATURA"]):
        return "", "congreso_estatal"
    if any(x in texto for x in ["CÁMARA DE", "CAMARA DE", "MESA DIRECTIVA",
                                  "JUCOPO", "COMISIÓN PERMANENTE"]):
        return "", "comision"
    if any(x in texto for x in ["SECRETARÍA", "SECRETARIA", "COMISIÓN FEDERAL",
                                  "BANCO DE", "SERVICIO DE", "PROCURADURÍA",
                                  "CENTROS DE"]):
        return "", "organismo"

    # 4. Último recurso: si contiene "DIP." o "SEN." es legislador
    if "DIP." in texto or "SEN." in texto:
        # Intentar extraer partido del nombre
        for p in PARTIDOS_VALIDOS:
            if p in texto:
                return p, "legislador"
        return "", "legislador"

    return "", "otro"


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
            presentador TEXT DEFAULT '',
            tipo_presentador TEXT DEFAULT '',
            fecha_scraping TEXT NOT NULL,
            UNIQUE(seguimiento_id, asunto_id)
        )
    """)
    # Agregar columnas nuevas si no existen (migración)
    for col, default in [("presentador", "''"), ("tipo_presentador", "''")]:
        try:
            conn.execute(f"ALTER TABLE sil_documentos ADD COLUMN {col} TEXT DEFAULT {default}")
        except sqlite3.OperationalError:
            pass  # Ya existe
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
            verify=False,
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

    # Extraer presentador raw y normalizar partido
    presentador_raw = meta.get("presentador", "").strip()
    partido, tipo_presentador = normalizar_partido(presentador_raw)

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
        "presentador": presentador_raw,
        "tipo_presentador": tipo_presentador,
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
            time.sleep(1.5)  # respetar servidor

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
                         estatus, partido, comision, categoria,
                         presentador, tipo_presentador, fecha_scraping)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    seg_id, asu_id,
                    detalle["tipo"], info["titulo"], info["sinopsis"],
                    detalle["camara"], fecha,
                    detalle["legislatura"], detalle["periodo"],
                    detalle["estatus"], detalle["partido"],
                    detalle["comision"], categoria,
                    detalle.get("presentador", ""),
                    detalle.get("tipo_presentador", ""),
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
    Estadísticas de instrumentos legislativos por partido político real.
    Solo incluye partidos de PARTIDOS_MEXICO (excluye ejecutivo, gobiernos estatales, etc.).
    Retorna dict para el dashboard.
    """
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    # Construir filtro de partidos válidos
    placeholders = ",".join(["?"] * len(PARTIDOS_VALIDOS))

    # Total por partido (solo partidos reales)
    rows_total = conn.execute(f"""
        SELECT partido, COUNT(*) as total
        FROM sil_documentos
        WHERE fecha_presentacion >= ? AND partido IN ({placeholders})
        GROUP BY partido ORDER BY total DESC
    """, (fecha_limite, *PARTIDOS_VALIDOS)).fetchall()

    # Por partido y categoría
    rows_cat = conn.execute(f"""
        SELECT partido, categoria, COUNT(*) as n
        FROM sil_documentos
        WHERE fecha_presentacion >= ? AND partido IN ({placeholders})
          AND categoria != '' AND categoria IS NOT NULL
        GROUP BY partido, categoria ORDER BY partido, n DESC
    """, (fecha_limite, *PARTIDOS_VALIDOS)).fetchall()

    # Por partido y tipo de instrumento
    rows_tipo = conn.execute(f"""
        SELECT partido, tipo, COUNT(*) as n
        FROM sil_documentos
        WHERE fecha_presentacion >= ? AND partido IN ({placeholders})
          AND tipo != '' AND tipo IS NOT NULL
        GROUP BY partido, tipo ORDER BY partido, n DESC
    """, (fecha_limite, *PARTIDOS_VALIDOS)).fetchall()

    # Serie temporal por partido (últimos 6 meses, por semana)
    rows_temporal = conn.execute(f"""
        SELECT partido,
               strftime('%Y-W%W', fecha_presentacion) as semana,
               COUNT(*) as n
        FROM sil_documentos
        WHERE fecha_presentacion >= ? AND partido IN ({placeholders})
        GROUP BY partido, semana
        ORDER BY partido, semana
    """, (fecha_limite, *PARTIDOS_VALIDOS)).fetchall()

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
            "por_tipo": {},
            "serie_semanal": {},
            "top_categoria": None,
        }

    for row in rows_cat:
        p, cat, n = row
        if p in partidos:
            cat_nombre = CATEGORIAS.get(cat, {}).get("nombre", cat)
            partidos[p]["por_categoria"][cat] = {"nombre": cat_nombre, "count": n}
            if not partidos[p]["top_categoria"]:
                partidos[p]["top_categoria"] = {"categoria": cat, "nombre": cat_nombre, "count": n}

    for row in rows_tipo:
        p, tipo, n = row
        if p in partidos and tipo:
            partidos[p]["por_tipo"][tipo] = n

    for row in rows_temporal:
        p, semana, n = row
        if p in partidos:
            partidos[p]["serie_semanal"][semana] = n

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
    para obtenerla. También extrae presentador, partido normalizado,
    cámara, comisión, etc.

    Corre en lotes configurables. Usa verify=False para SSL expirado.
    """
    init_db()  # Asegurar columnas nuevas existen
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
            # Re-clasificar categoría si estaba vacía
            cat_actual = conn.execute(
                "SELECT categoria FROM sil_documentos WHERE id = ?", (doc_id,)
            ).fetchone()[0]
            if not cat_actual:
                cat_actual = _clasificar_documento(titulo)

            conn.execute("""
                UPDATE sil_documentos
                SET fecha_presentacion = ?,
                    camara = COALESCE(NULLIF(camara, ''), ?),
                    legislatura = COALESCE(NULLIF(legislatura, ''), ?),
                    periodo = COALESCE(NULLIF(periodo, ''), ?),
                    partido = ?,
                    comision = COALESCE(NULLIF(comision, ''), ?),
                    estatus = COALESCE(NULLIF(estatus, ''), ?),
                    tipo = COALESCE(NULLIF(tipo, ''), ?),
                    presentador = ?,
                    tipo_presentador = ?,
                    categoria = COALESCE(NULLIF(categoria, ''), ?)
                WHERE id = ?
            """, (
                detalle["fecha_presentacion"],
                detalle.get("camara", ""),
                detalle.get("legislatura", ""),
                detalle.get("periodo", ""),
                detalle.get("partido", ""),
                detalle.get("comision", ""),
                detalle.get("estatus", ""),
                detalle.get("tipo", ""),
                detalle.get("presentador", ""),
                detalle.get("tipo_presentador", ""),
                cat_actual,
                doc_id,
            ))
            enriquecidos += 1
        else:
            fallidos += 1

        time.sleep(0.3)

        # Commit cada 100
        if (enriquecidos + fallidos) % 100 == 0:
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


def normalizar_partidos_existentes():
    """
    Re-normaliza el campo partido para todos los documentos SIL
    que tienen un partido que no es uno de los partidos válidos.
    También puebla presentador y tipo_presentador si están vacíos.
    Solo trabaja con datos locales, NO hace HTTP.
    """
    init_db()  # Asegurar columnas presentador/tipo_presentador existen
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))

    # Docs con partido que no es de PARTIDOS_VALIDOS y no está vacío
    rows = conn.execute("""
        SELECT id, partido, presentador FROM sil_documentos
        WHERE partido != '' AND partido IS NOT NULL
    """).fetchall()

    actualizados = 0
    for doc_id, partido_actual, presentador_actual in rows:
        if partido_actual in PARTIDOS_VALIDOS:
            # Partido ya es válido, solo asegurar tipo_presentador esté seteado
            if not presentador_actual:
                conn.execute("""
                    UPDATE sil_documentos
                    SET tipo_presentador = COALESCE(NULLIF(tipo_presentador, ''), 'legislador')
                    WHERE id = ?
                """, (doc_id,))
                actualizados += 1
            continue

        # El partido_actual podría ser el presentador raw (era el viejo comportamiento)
        texto_para_normalizar = presentador_actual or partido_actual
        nuevo_partido, tipo_pres = normalizar_partido(texto_para_normalizar)

        conn.execute("""
            UPDATE sil_documentos
            SET partido = ?,
                presentador = COALESCE(NULLIF(presentador, ''), ?),
                tipo_presentador = COALESCE(NULLIF(tipo_presentador, ''), ?)
            WHERE id = ?
        """, (nuevo_partido, partido_actual, tipo_pres, doc_id))
        actualizados += 1

    conn.commit()
    conn.close()

    logger.info(f"Partidos normalizados: {actualizados} documentos actualizados")
    return actualizados


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
