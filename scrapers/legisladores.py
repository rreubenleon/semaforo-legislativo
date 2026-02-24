"""
Scraper de Directorio de Legisladores
Fuentes:
  - Cámara de Diputados: sitl.diputados.gob.mx/LXVI_leg/
  - Senado: senado.gob.mx/66/senadores

Extrae: nombre, partido, estado, distrito, comisiones (con cargo),
        principio de elección, email, suplente, foto_url
"""

import logging
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DATABASE

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SemaforoLegislativo/1.0)"}

# ────────────────────────────────────────────
# Base de datos
# ────────────────────────────────────────────
def init_db():
    """Crea tablas para legisladores y su historial."""
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))

    conn.execute("""
        CREATE TABLE IF NOT EXISTS legisladores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL,
            nombre_normalizado TEXT NOT NULL,
            camara TEXT NOT NULL,
            partido TEXT,
            estado TEXT,
            distrito TEXT,
            principio_eleccion TEXT,
            comisiones TEXT,
            comisiones_cargo TEXT,
            email TEXT,
            suplente TEXT,
            foto_url TEXT,
            sitl_id TEXT,
            legislatura TEXT DEFAULT 'LXVI',
            fecha_scraping TEXT NOT NULL,
            UNIQUE(nombre_normalizado, camara)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS actividad_legislador (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            legislador_id INTEGER,
            nombre_presentador TEXT NOT NULL,
            sil_documento_id INTEGER,
            tipo_instrumento TEXT,
            categoria TEXT,
            fecha_presentacion TEXT,
            titulo TEXT,
            comision_turno TEXT,
            estatus TEXT,
            co_firmantes TEXT,
            FOREIGN KEY (legislador_id) REFERENCES legisladores(id),
            FOREIGN KEY (sil_documento_id) REFERENCES sil_documentos(id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS reacciones_historicas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            legislador_id INTEGER,
            categoria TEXT NOT NULL,
            evento_fecha TEXT,
            evento_descripcion TEXT,
            presentacion_fecha TEXT,
            dias_reaccion INTEGER,
            tipo_instrumento TEXT,
            score_media_evento REAL,
            FOREIGN KEY (legislador_id) REFERENCES legisladores(id)
        )
    """)

    # Índices para queries rápidos
    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS idx_actividad_legislador ON actividad_legislador(legislador_id)",
        "CREATE INDEX IF NOT EXISTS idx_actividad_categoria ON actividad_legislador(categoria)",
        "CREATE INDEX IF NOT EXISTS idx_actividad_fecha ON actividad_legislador(fecha_presentacion)",
        "CREATE INDEX IF NOT EXISTS idx_reacciones_legislador ON reacciones_historicas(legislador_id)",
        "CREATE INDEX IF NOT EXISTS idx_reacciones_categoria ON reacciones_historicas(categoria)",
        "CREATE INDEX IF NOT EXISTS idx_legisladores_partido ON legisladores(partido)",
        "CREATE INDEX IF NOT EXISTS idx_legisladores_estado ON legisladores(estado)",
    ]:
        try:
            conn.execute(idx_sql)
        except sqlite3.OperationalError:
            pass

    conn.commit()
    return conn


def _normalizar_nombre(nombre):
    """
    Normaliza un nombre para matching:
    'Dip. José Elías Lixa Abimerhi (PAN)' → 'jose elias lixa abimerhi'
    """
    # Quitar prefijos
    nombre = re.sub(r'^(Dip\.|Sen\.|C\.|Diputad[oa]|Senador[a]?)\s*', '', nombre.strip())
    # Quitar partido entre paréntesis
    nombre = re.sub(r'\s*\([^)]*\)\s*', '', nombre)
    # Normalizar acentos y minúsculas
    nombre = nombre.lower().strip()
    # Remover acentos
    reemplazos = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'ñ': 'n', 'ü': 'u',
    }
    for orig, remp in reemplazos.items():
        nombre = nombre.replace(orig, remp)
    # Quitar caracteres especiales
    nombre = re.sub(r'[^a-z\s]', '', nombre)
    # Normalizar espacios
    nombre = re.sub(r'\s+', ' ', nombre).strip()
    return nombre


# ────────────────────────────────────────────
# Scraper: Cámara de Diputados
# ────────────────────────────────────────────
def _scrape_listado_diputados():
    """
    Obtiene lista de todos los diputados del SITL.
    URL: sitl.diputados.gob.mx/LXVI_leg/listado_diputados_gpnp.php?tipot=TOTAL
    Retorna lista de {nombre, estado, distrito, sitl_id}
    """
    url = "http://sitl.diputados.gob.mx/LXVI_leg/listado_diputados_gpnp.php?tipot=TOTAL"
    try:
        resp = requests.get(url, timeout=60, headers=HEADERS, verify=False)
        resp.encoding = "utf-8"
    except requests.RequestException as e:
        logger.error(f"Error accediendo listado de diputados: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Buscar tabla grande (>100 filas)
    big_table = None
    for t in soup.find_all("table"):
        if len(t.find_all("tr")) > 100:
            big_table = t
            break

    if not big_table:
        logger.error("No se encontró tabla de diputados")
        return []

    diputados = []
    for tr in big_table.find_all("tr")[1:]:  # saltar header
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue

        # Celda 0: nombre con link
        link = cells[0].find("a", href=True)
        if not link:
            continue

        nombre_raw = link.get_text(strip=True)
        href = link.get("href", "")

        # Extraer sitl_id del link: curricula.php?dipt=391
        sitl_match = re.search(r'dipt=(\d+)', href)
        sitl_id = sitl_match.group(1) if sitl_match else ""

        # Quitar número del inicio: "1 Abreu Artiñano Rocío Adriana"
        nombre = re.sub(r'^\d+\s+', '', nombre_raw).strip()

        # Celda 1: estado
        estado = cells[1].get_text(strip=True) if len(cells) > 1 else ""

        # Celda 2: distrito/circunscripción
        distrito = cells[2].get_text(strip=True) if len(cells) > 2 else ""

        diputados.append({
            "nombre": nombre,
            "estado": estado,
            "distrito": distrito,
            "sitl_id": sitl_id,
        })

    logger.info(f"Listado diputados: {len(diputados)} encontrados")
    return diputados


def _scrape_detalle_diputado(sitl_id):
    """
    Obtiene detalle de un diputado individual del SITL.
    URL: sitl.diputados.gob.mx/LXVI_leg/curricula.php?dipt=XXX
    Retorna: partido, principio_eleccion, comisiones, email, suplente, foto_url
    """
    url = f"http://sitl.diputados.gob.mx/LXVI_leg/curricula.php?dipt={sitl_id}"
    try:
        resp = requests.get(url, timeout=30, headers=HEADERS, verify=False)
        resp.encoding = "utf-8"
    except requests.RequestException:
        return None

    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    text_lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]

    detalle = {
        "partido": "",
        "principio_eleccion": "",
        "comisiones": [],
        "comisiones_cargo": [],
        "email": "",
        "suplente": "",
        "foto_url": "",
    }

    # Partido: extraer de la imagen con class "header-gp"
    # El archivo se llama images/morena.webp, images/pan.webp, etc.
    PARTIDO_IMG_MAP = {
        "morena": "MORENA", "pan": "PAN", "pri": "PRI",
        "pvem": "PVEM", "pt": "PT", "mc": "MC", "prd": "PRD",
        "na": "NA", "sin_partido": "SIN PARTIDO",
    }
    gp_img = soup.find("img", class_="header-gp")
    if gp_img:
        src = gp_img.get("src", "").lower()
        for key, partido_val in PARTIDO_IMG_MAP.items():
            if key in src:
                detalle["partido"] = partido_val
                break

    # Foto del legislador
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if "fotos_lxvi" in src.lower() or "foto" in src.lower():
            if src.startswith("./"):
                src = f"http://sitl.diputados.gob.mx/LXVI_leg/{src[2:]}"
            elif src.startswith("/"):
                src = f"http://sitl.diputados.gob.mx{src}"
            detalle["foto_url"] = src
            break

    # Parsear líneas de texto
    for i, line in enumerate(text_lines):
        line_lower = line.lower()

        if "principio de elección:" in line_lower:
            if i + 1 < len(text_lines):
                detalle["principio_eleccion"] = text_lines[i + 1].strip()

        elif "suplente:" in line_lower:
            if i + 1 < len(text_lines):
                detalle["suplente"] = text_lines[i + 1].strip()

        elif "@diputados.gob.mx" in line:
            detalle["email"] = line.strip()

    # Extraer comisiones (aparecen después de "COMISIONES A LAS QUE PERTENECE")
    in_comisiones = False
    for i, line in enumerate(text_lines):
        if "COMISIONES" in line and "PERTENECE" in text_lines[i + 1] if i + 1 < len(text_lines) else False:
            in_comisiones = True
            continue

        if in_comisiones:
            if line == "|":
                continue
            if line in ("GRUPO DE AMISTAD", "Secretaría General"):
                break

            # Detectar cargo entre paréntesis
            cargo_match = re.search(r'\(([^)]+)\)', line)
            comision_nombre = re.sub(r'\s*\([^)]*\)\s*', '', line).strip()

            if comision_nombre and comision_nombre not in ("ORDINARIA", "ESPECIAL"):
                cargo = cargo_match.group(1) if cargo_match else "Integrante"
                detalle["comisiones"].append(comision_nombre)
                detalle["comisiones_cargo"].append(f"{comision_nombre}:{cargo}")

    return detalle


# ────────────────────────────────────────────
# Scraper: Senado de la República
# ────────────────────────────────────────────
def _scrape_listado_senadores():
    """
    Obtiene lista de todos los senadores del sitio del Senado.
    Usa la página de grupo parlamentario que contiene los 128 (senadores + senadoras).
    URL: senado.gob.mx/66/senadores/por_grupo_parlamentario
    """
    # La página principal solo tiene 64, necesitamos la de grupo parlamentario
    # que tiene los 128 completos
    urls_a_probar = [
        "https://www.senado.gob.mx/66/senadores/por_grupo_parlamentario",
        "https://www.senado.gob.mx/66/senadores",
    ]

    soup = None
    for url in urls_a_probar:
        try:
            resp = requests.get(url, timeout=60, headers=HEADERS, verify=False)
            resp.encoding = "utf-8"
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                # Contar IDs únicos
                ids_test = set()
                for link in soup.find_all("a", href=True):
                    m = re.search(r'/senador/(\d+)', link.get("href", ""))
                    if m:
                        ids_test.add(m.group(1))
                if len(ids_test) >= 100:  # Si tiene >100, es la buena
                    logger.info(f"Usando {url} ({len(ids_test)} senadores)")
                    break
        except requests.RequestException:
            continue

    if not soup:
        logger.error("No se pudo acceder al listado de senadores")
        return []

    # Si no encontramos 100+ en una sola página, combinar senadores + senadoras
    senadores = []
    seen = set()

    # Primero intentar la página actual
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        text = link.get_text(strip=True)

        if re.search(r'/66/senador/\d+', href) and text:
            senador_id = re.search(r'/senador/(\d+)', href).group(1)
            if senador_id in seen:
                continue

            # Limpiar nombre (puede venir como "Sen. Nombre" o solo "Nombre")
            nombre = re.sub(r'^Sen\.\s*', '', text).strip()
            if not nombre or len(nombre) < 3:
                continue

            seen.add(senador_id)
            senadores.append({
                "nombre": nombre,
                "senador_id": senador_id,
                "url": href if href.startswith("http") else f"https://www.senado.gob.mx{href}",
            })

    # Si aún no tenemos 100+, complementar con las subpáginas
    if len(senadores) < 100:
        for sub in ["senadores", "senadoras"]:
            sub_url = f"https://www.senado.gob.mx/66/senadores/{sub}"
            try:
                resp = requests.get(sub_url, timeout=30, headers=HEADERS, verify=False)
                resp.encoding = "utf-8"
                sub_soup = BeautifulSoup(resp.text, "html.parser")

                for link in sub_soup.find_all("a", href=True):
                    href = link.get("href", "")
                    text = link.get_text(strip=True)
                    if re.search(r'/66/senador/\d+', href) and text:
                        senador_id = re.search(r'/senador/(\d+)', href).group(1)
                        if senador_id in seen:
                            continue
                        nombre = re.sub(r'^Sen\.\s*', '', text).strip()
                        if not nombre or len(nombre) < 3:
                            continue
                        seen.add(senador_id)
                        senadores.append({
                            "nombre": nombre,
                            "senador_id": senador_id,
                            "url": href if href.startswith("http") else f"https://www.senado.gob.mx{href}",
                        })
            except requests.RequestException:
                continue

    logger.info(f"Listado senadores: {len(senadores)} encontrados")
    return senadores


def _scrape_detalle_senador(senador_url, senador_id):
    """
    Obtiene detalle de un senador individual.
    """
    try:
        resp = requests.get(senador_url, timeout=30, headers=HEADERS, verify=False)
        resp.encoding = "utf-8"
    except requests.RequestException:
        return None

    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    text_lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]

    detalle = {
        "partido": "",
        "estado": "",
        "principio_eleccion": "",
        "comisiones": [],
        "comisiones_cargo": [],
        "foto_url": "",
    }

    # Buscar partido (Grupo Parlamentario)
    for i, line in enumerate(text_lines):
        if "Grupo Parlamentario" in line:
            if "Morena" in line:
                detalle["partido"] = "MORENA"
            elif "Acción Nacional" in line or "PAN" in line:
                detalle["partido"] = "PAN"
            elif "Revolucionario Institucional" in line or "PRI" in line:
                detalle["partido"] = "PRI"
            elif "Verde" in line or "PVEM" in line:
                detalle["partido"] = "PVEM"
            elif "Trabajo" in line or "PT" in line:
                detalle["partido"] = "PT"
            elif "Movimiento Ciudadano" in line or "MC" in line:
                detalle["partido"] = "MC"
            if detalle["partido"]:
                break

    # Buscar estado y principio de elección
    for i, line in enumerate(text_lines):
        if "Electo por el Principio de" in line or "Electa por el Principio de" in line:
            detalle["principio_eleccion"] = line.strip()
        if "Entidad:" in line:
            detalle["estado"] = line.replace("Entidad:", "").strip()

    # Buscar comisiones
    in_comisiones = False
    for line in text_lines:
        if "COMISIONES" in line.upper():
            in_comisiones = True
            continue
        if in_comisiones:
            if line.startswith("Informe") or line.startswith("Turnado"):
                break
            # Detectar cargo
            cargo_match = re.search(r'\(([^)]*(?:President|Secretari|Integrante)[^)]*)\)', line, re.IGNORECASE)
            comision_nombre = re.sub(r'\s*\([^)]*\)\s*', '', line).strip()
            if comision_nombre and len(comision_nombre) > 3 and comision_nombre not in ("|", "ORDINARIA", "ESPECIAL"):
                cargo = cargo_match.group(1) if cargo_match else "Integrante"
                detalle["comisiones"].append(comision_nombre)
                detalle["comisiones_cargo"].append(f"{comision_nombre}:{cargo}")

    return detalle


# ────────────────────────────────────────────
# Orquestadores
# ────────────────────────────────────────────
def scrape_diputados(max_detalle=500):
    """Scrapea el directorio completo de la Cámara de Diputados."""
    conn = init_db()

    listado = _scrape_listado_diputados()
    nuevos = 0

    for i, dip in enumerate(listado):
        nombre_norm = _normalizar_nombre(dip["nombre"])

        # Verificar si ya existe
        existe = conn.execute(
            "SELECT id FROM legisladores WHERE nombre_normalizado = ? AND camara = ?",
            (nombre_norm, "Cámara de Diputados")
        ).fetchone()
        if existe:
            continue

        # Obtener detalle
        detalle = None
        if dip["sitl_id"] and i < max_detalle:
            detalle = _scrape_detalle_diputado(dip["sitl_id"])
            time.sleep(0.5)

        partido = detalle["partido"] if detalle else ""
        principio = detalle["principio_eleccion"] if detalle else ""
        comisiones = "|".join(detalle["comisiones"]) if detalle else ""
        comisiones_cargo = "|".join(detalle["comisiones_cargo"]) if detalle else ""
        email = detalle["email"] if detalle else ""
        suplente = detalle["suplente"] if detalle else ""
        foto_url = detalle["foto_url"] if detalle else ""

        try:
            conn.execute("""
                INSERT INTO legisladores
                    (nombre, nombre_normalizado, camara, partido, estado, distrito,
                     principio_eleccion, comisiones, comisiones_cargo, email,
                     suplente, foto_url, sitl_id, legislatura, fecha_scraping)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                dip["nombre"], nombre_norm, "Cámara de Diputados",
                partido, dip["estado"], dip["distrito"],
                principio, comisiones, comisiones_cargo,
                email, suplente, foto_url, dip["sitl_id"],
                "LXVI", datetime.now().isoformat(),
            ))
            nuevos += 1
            if nuevos % 50 == 0:
                logger.info(f"  Diputados procesados: {nuevos}...")
                conn.commit()
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()

    logger.info(f"Diputados: {nuevos} nuevos registrados de {len(listado)} en listado")
    return {"nuevos": nuevos, "total_listado": len(listado)}


def scrape_senadores(max_detalle=150):
    """Scrapea el directorio completo del Senado."""
    conn = init_db()

    listado = _scrape_listado_senadores()
    nuevos = 0

    for i, sen in enumerate(listado):
        nombre_norm = _normalizar_nombre(sen["nombre"])

        existe = conn.execute(
            "SELECT id FROM legisladores WHERE nombre_normalizado = ? AND camara = ?",
            (nombre_norm, "Senado")
        ).fetchone()
        if existe:
            continue

        detalle = None
        if i < max_detalle:
            detalle = _scrape_detalle_senador(sen["url"], sen["senador_id"])
            time.sleep(0.5)

        partido = detalle["partido"] if detalle else ""
        estado = detalle["estado"] if detalle else ""
        principio = detalle["principio_eleccion"] if detalle else ""
        comisiones = "|".join(detalle["comisiones"]) if detalle else ""
        comisiones_cargo = "|".join(detalle["comisiones_cargo"]) if detalle else ""

        try:
            conn.execute("""
                INSERT INTO legisladores
                    (nombre, nombre_normalizado, camara, partido, estado, distrito,
                     principio_eleccion, comisiones, comisiones_cargo, email,
                     suplente, foto_url, sitl_id, legislatura, fecha_scraping)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sen["nombre"], nombre_norm, "Senado",
                partido, estado, "",
                principio, comisiones, comisiones_cargo,
                "", "", "", sen["senador_id"],
                "LXVI", datetime.now().isoformat(),
            ))
            nuevos += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()

    logger.info(f"Senadores: {nuevos} nuevos registrados de {len(listado)} en listado")
    return {"nuevos": nuevos, "total_listado": len(listado)}


# ────────────────────────────────────────────
# Poblar actividad_legislador desde SIL
# ────────────────────────────────────────────
def poblar_actividad_desde_sil():
    """
    Vincula los documentos del SIL existentes con legisladores.
    Usa el campo 'presentador' ya almacenado en sil_documentos (sin HTTP extra).
    Si no existe presentador, hace fallback al campo partido.
    """
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Cargar legisladores indexados por nombre normalizado
    legisladores = {}
    for row in conn.execute("SELECT id, nombre, nombre_normalizado FROM legisladores"):
        legisladores[row["nombre_normalizado"]] = row["id"]

    if not legisladores:
        logger.warning("No hay legisladores en la BD. Ejecuta scrape_diputados/senadores primero.")
        conn.close()
        return {"vinculados": 0, "sin_match": 0}

    # Procesar documentos SIL que aún no están en actividad_legislador
    # Solo procesar docs con presentador, fecha y categoría
    docs = conn.execute("""
        SELECT s.id, s.seguimiento_id, s.asunto_id, s.tipo, s.titulo,
               s.categoria, s.fecha_presentacion, s.comision, s.estatus,
               s.partido, s.presentador, s.tipo_presentador
        FROM sil_documentos s
        LEFT JOIN actividad_legislador a ON a.sil_documento_id = s.id
        WHERE a.id IS NULL
          AND s.tipo_presentador = 'legislador'
          AND s.presentador != '' AND s.presentador IS NOT NULL
          AND s.fecha_presentacion != '' AND s.fecha_presentacion IS NOT NULL
    """).fetchall()

    logger.info(f"Procesando {len(docs)} documentos SIL (legisladores) para vincular...")

    vinculados = 0
    sin_match = 0

    for doc in docs:
        presentador = doc["presentador"]

        # Parsear múltiples autores del campo presentador
        autores = _parsear_presentadores(presentador)

        for autor_nombre in autores:
            nombre_norm = _normalizar_nombre(autor_nombre)
            legislador_id = _buscar_legislador(nombre_norm, legisladores)

            try:
                conn.execute("""
                    INSERT INTO actividad_legislador
                        (legislador_id, nombre_presentador, sil_documento_id,
                         tipo_instrumento, categoria, fecha_presentacion,
                         titulo, comision_turno, estatus, co_firmantes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    legislador_id,
                    autor_nombre,
                    doc["id"],
                    doc["tipo"],
                    doc["categoria"],
                    doc["fecha_presentacion"],
                    doc["titulo"],
                    doc["comision"],
                    doc["estatus"],
                    presentador if len(autores) > 1 else "",
                ))
                vinculados += 1
            except sqlite3.IntegrityError:
                pass

        if not autores:
            sin_match += 1

        if vinculados % 200 == 0 and vinculados > 0:
            conn.commit()
            logger.info(f"  Vinculados: {vinculados}...")

    conn.commit()
    conn.close()

    logger.info(f"Actividad legislador: {vinculados} vínculos creados, {sin_match} sin match")
    return {"vinculados": vinculados, "sin_match": sin_match}


def _obtener_presentador_sil(seg_id, asu_id):
    """Obtiene el campo 'Presentador' de una ficha del SIL."""
    SIL_DETALLE = "http://sil.gobernacion.gob.mx/Librerias/pp_ReporteSeguimiento.php"
    try:
        resp = requests.get(
            SIL_DETALLE,
            params={"Seguimiento": seg_id, "Asunto": asu_id},
            timeout=20,
            headers=HEADERS,
        )
        resp.encoding = "latin-1"
    except requests.RequestException:
        return None

    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if 5 <= len(rows) <= 20:
            for row in rows:
                cells = row.find_all("td")
                if len(cells) == 2:
                    key = cells[0].get_text(strip=True).lower()
                    if key == "presentador":
                        return cells[1].get_text(strip=True)

    return None


def _parsear_presentadores(texto):
    """
    Parsea el campo Presentador del SIL que puede tener múltiples autores:
    'Dip. Francisco Pelayo Covarrubias (PAN)Dip. José Elías Lixa Abimerhi (PAN)'
    Retorna lista de nombres limpios.
    """
    if not texto:
        return []

    # Separar por "Dip." o "Sen."
    partes = re.split(r'(?=Dip\.|Sen\.)', texto)
    nombres = []

    for parte in partes:
        parte = parte.strip()
        if not parte:
            continue
        # Quitar prefijo y partido
        nombre = re.sub(r'^(Dip\.|Sen\.)\s*', '', parte)
        nombre = re.sub(r'\s*\([^)]*\)\s*$', '', nombre).strip()
        if nombre and len(nombre) > 3:
            nombres.append(nombre)

    return nombres


def _buscar_legislador(nombre_norm, legisladores_dict):
    """
    Busca un legislador por nombre normalizado.
    Intenta match exacto primero, luego parcial por apellidos.
    """
    # Match exacto
    if nombre_norm in legisladores_dict:
        return legisladores_dict[nombre_norm]

    # Match parcial: buscar por apellidos (últimas 2 palabras)
    partes = nombre_norm.split()
    if len(partes) >= 2:
        apellidos = " ".join(partes[-2:])
        for key, leg_id in legisladores_dict.items():
            if apellidos in key:
                return leg_id

    return None  # Sin match → legislador_id queda NULL


# ────────────────────────────────────────────
# Funciones de consulta
# ────────────────────────────────────────────
def obtener_legisladores(partido=None, estado=None, camara=None):
    """Retorna lista de legisladores con filtros opcionales."""
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    query = "SELECT * FROM legisladores WHERE 1=1"
    params = []

    if partido:
        query += " AND partido = ?"
        params.append(partido)
    if estado:
        query += " AND estado = ?"
        params.append(estado)
    if camara:
        query += " AND camara = ?"
        params.append(camara)

    rows = conn.execute(query + " ORDER BY nombre", params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def obtener_perfil_legislador(legislador_id):
    """Retorna perfil completo de un legislador con su actividad."""
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    leg = conn.execute("SELECT * FROM legisladores WHERE id = ?", (legislador_id,)).fetchone()
    if not leg:
        conn.close()
        return None

    perfil = dict(leg)

    # Actividad reciente
    perfil["actividad"] = [dict(r) for r in conn.execute("""
        SELECT * FROM actividad_legislador
        WHERE legislador_id = ?
        ORDER BY fecha_presentacion DESC
        LIMIT 50
    """, (legislador_id,)).fetchall()]

    # Estadísticas
    stats = conn.execute("""
        SELECT categoria, COUNT(*) as total,
               COUNT(DISTINCT tipo_instrumento) as tipos
        FROM actividad_legislador
        WHERE legislador_id = ?
        GROUP BY categoria
        ORDER BY total DESC
    """, (legislador_id,)).fetchall()
    perfil["stats_por_categoria"] = [dict(r) for r in stats]

    # Total instrumentos
    total = conn.execute(
        "SELECT COUNT(*) FROM actividad_legislador WHERE legislador_id = ?",
        (legislador_id,)
    ).fetchone()[0]
    perfil["total_instrumentos"] = total

    conn.close()
    return perfil


def obtener_stats_legisladores():
    """Estadísticas generales de la tabla de legisladores."""
    db_path = ROOT / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    stats = {
        "total": conn.execute("SELECT COUNT(*) FROM legisladores").fetchone()[0],
        "por_camara": {},
        "por_partido": {},
        "con_comisiones": conn.execute(
            "SELECT COUNT(*) FROM legisladores WHERE comisiones != '' AND comisiones IS NOT NULL"
        ).fetchone()[0],
    }

    for row in conn.execute("SELECT camara, COUNT(*) as n FROM legisladores GROUP BY camara"):
        stats["por_camara"][row["camara"]] = row["n"]

    for row in conn.execute("SELECT partido, COUNT(*) as n FROM legisladores WHERE partido != '' GROUP BY partido ORDER BY n DESC"):
        stats["por_partido"][row["partido"]] = row["n"]

    conn.close()
    return stats


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    print("=== Scraper de Legisladores ===\n")

    print("1. Scrapeando Cámara de Diputados...")
    resultado_dip = scrape_diputados()
    print(f"   Resultado: {resultado_dip}\n")

    print("2. Scrapeando Senado...")
    resultado_sen = scrape_senadores()
    print(f"   Resultado: {resultado_sen}\n")

    stats = obtener_stats_legisladores()
    print(f"3. Estadísticas: {stats}")
