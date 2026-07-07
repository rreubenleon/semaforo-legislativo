"""
Pipeline del Radar de Legisladores.

Corre en workflow SEPARADO del pipeline principal de FIAT. Nunca toca
`main.py` ni hace commits al cache de `semaforo.db`. Solo lee las
tablas que alimenta el pipeline principal y escribe los resultados
directamente a Cloudflare D1.

Flujo:
  1. Abrir semaforo.db en modo read-only (copia local del cache).
  2. Leer universo de legisladores, actividad y reacciones.
  3. Ejecutar scraper SIL incremental (solo refs nuevas o expiradas).
  4. Calcular hit_rate por legislador y categoría dominante.
  5. Calcular matchup grade por legislador.
  6. Proyectar iniciativas y proposiciones forward 15 días.
  7. Generar INSERT OR REPLACE a D1 en batch vía wrangler.

Cadencia sugerida:
  - Descubrimiento de refs SIL: domingo 12 UTC (1 vez por semana).
  - Recalcular stats agregados: diario a las 13 UTC.
"""

import argparse
import json
import logging
import re
import os
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────
# Lectura de la DB principal en modo read-only
# ────────────────────────────────────────────
def abrir_db_readonly(db_path: Path) -> sqlite3.Connection:
    """Abre semaforo.db en modo RO sin bloquear el pipeline principal."""
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


# ────────────────────────────────────────────
# Emisión de SQL a D1 via wrangler
# ────────────────────────────────────────────
D1_DB = "fiat-busqueda"


def ejecutar_sql_d1(sql: str, remote: bool = True) -> dict:
    """Ejecuta SQL contra D1 vía wrangler. Reintenta si D1 está ocupado con otra
    importación (colisión entre workflows concurrentes — p.ej. radar.yml vs el
    reconteo semanal escribiendo a D1 a la vez)."""
    import time as _time
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".sql", delete=False, encoding="utf-8"
    ) as f:
        f.write(sql)
        sql_path = f.name

    cmd = [
        "npx", "wrangler", "d1", "execute", D1_DB,
        "--remote" if remote else "--local", "--file", sql_path, "--json",
    ]
    try:
        for intento in range(5):
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode == 0:
                return {"ok": True, "stdout_size": len(result.stdout)}
            salida = (result.stderr or "") + (result.stdout or "")
            # Error REAL de SQL → fallar de inmediato (reintentar enmascararía
            # un bug nuestro). Todo lo demás (D1 ocupado, "internal error"
            # code 7500 de la API de Cloudflare, timeouts de red) es
            # transitorio → reintentar con espera creciente.
            permanente = ("SQLITE_" in salida or "syntax" in salida.lower()
                          or "no such table" in salida.lower()
                          or "no such column" in salida.lower())
            if not permanente and intento < 4:
                espera = 30 * (intento + 1)
                logger.warning(
                    f"D1 falló (transitorio); reintento "
                    f"{intento + 1}/4 en {espera}s: {salida[:200]}")
                _time.sleep(espera)
                continue
            logger.error(f"wrangler stderr: {result.stderr[:800]}")
            logger.error(f"wrangler stdout: {result.stdout[:800]}")
            raise RuntimeError("wrangler d1 execute failed")
    finally:
        os.unlink(sql_path)


def _sql_escape(s: str) -> str:
    """Escapa un string para SQL literal."""
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


# ────────────────────────────────────────────
# Paso 0: Snapshot de legisladores a D1
# ────────────────────────────────────────────
def paso_snapshot_legisladores(db_ro: sqlite3.Connection) -> dict:
    """
    Empuja el universo de legisladores (id, nombre, camara, partido…)
    desde semaforo.db (RO) a D1. Se corre al inicio del pipeline Radar
    para garantizar que el foreign key de legisladores_perfil resuelva.

    Usa DELETE + INSERT en batch. El pipeline principal NO toca esta
    tabla en D1, así que solo el Radar la mantiene.
    """
    rows = db_ro.execute(
        """
        SELECT id, nombre, nombre_normalizado, camara,
               COALESCE(partido, '')  AS partido,
               COALESCE(estado, '')   AS estado,
               COALESCE(distrito, '') AS distrito,
               COALESCE(foto_url, '') AS foto_url,
               COALESCE(legislatura, 'LXVI') AS legislatura,
               COALESCE(comisiones_cargo, '') AS comisiones_cargo,
               COALESCE(origen, '')   AS origen
        FROM legisladores
        """
    ).fetchall()

    # Limpieza de comisiones_cargo: el scraper de Diputados concatena el CV
    # completo (formación, trayectoria, legislaturas pasadas, cargos públicos)
    # dentro de comisiones_cargo. Como el frontend pinta CUALQUIER "X:Y" como
    # chip de comisión, hay que filtrar la basura aquí. Senado ya está limpio.
    import unicodedata as _ud
    _INVIS = {0x00AD, 0x200B, 0x200C, 0x200D, 0xFEFF}
    _CARGOS_OK = {'presidente', 'presidenta', 'secretario', 'secretaria',
                  'integrante', 'vocal'}
    # Substrings de CV/trayectoria (NO comisiones). Nada de 'municipal' suelto:
    # "Desarrollo Municipal" es comisión real. Los cargos de carrera traen
    # cargo roto (':a', ':o') y caen por el filtro de cargo.
    _JUNK_SUB = (
        'maestria', 'licenciatura', 'doctorado', 'bachillerato', 'preparatoria',
        'posgrado', 'diplomado', 'especialidad en', 'no proporcion', 'legislatura',
        'presidencia de la republica', 'gobierno del', 'oficialia mayor',
        'oficial mayor', 'director', 'coordinador', 'subsecretari', 'jefe de',
        'jefa de', 'secretario tecnico', 'secretario nacional', 'secretaria del',
        'titular de', 'delegad', 'regidor', 'presidente municipal',
        'presidencia municipal', 'ayuntamiento', 'gobernador', 'centro de',
        'instituto', 'contralor', 'unidad t', 'informaci', 'consultor',
        'asistencias', 'calendario', 'memoria de labores', 'organos t',
        'capacitaci', 'ordinarias:', 'especiales:', 'a las que pertenece',
        'iniciativa', 'proposicion', 'municipio de', 'comite directivo',
        'comision nacional', 'comisario',
    )
    _JUNK_EXACT = {'derecho', 'de derecho', 'medicina', 'ingenieria',
                   'arquitectura', 'contaduria', 'psicologia', 'enfermeria',
                   'no proporciono', 'independiente', 'presidenta', 'consejera'}

    def _na(s: str) -> str:
        s = s.translate({c: None for c in _INVIS})
        return ''.join(c for c in _ud.normalize('NFD', s.lower())
                       if _ud.category(c) not in ('Mn', 'Cf')).strip()

    def _partes_validas(raw: str):
        """Itera (parte_limpia, nombre_normalizado) con cargo válido."""
        for p in (raw or '').split('|'):
            p = ''.join(ch for ch in p if ord(ch) not in _INVIS).strip()
            if not p or ':' not in p:
                continue
            nom, cargo = p.rsplit(':', 1)
            if _na(cargo) in _CARGOS_OK:
                yield p, _na(nom)

    # Frecuencia global de nombres de comisión (cargo válido): una comisión
    # real aparece en muchos legisladores; la basura rara del CV (años,
    # municipios, escuelas, cargos partidistas) aparece 1-2 veces. Piso = 3.
    from collections import Counter as _Counter
    _freq = _Counter()
    for _r in rows:
        for _p, _nn in _partes_validas(_r['comisiones_cargo']):
            _freq[_nn] += 1

    def _limpia_comisiones(raw: str) -> str:
        buenas, seen = [], set()
        for p, nn in _partes_validas(raw):
            if not any(c.isalpha() for c in nn):       # años / numéricos
                continue
            if nn in _JUNK_EXACT or any(j in nn for j in _JUNK_SUB):
                continue                                # CV / trayectoria / formación
            if _freq[nn] < 3:                           # ruido raro no-comisión
                continue
            if p not in seen:                           # dedup
                seen.add(p)
                buenas.append(p)
        return '|'.join(buenas)[:800]

    # No asignar estado a senadores INFERIDOS (origen='sil_inferido'): no son
    # ocupantes oficiales de banca y inflaban el mapa de representación (p.ej.
    # Nuevo León salía con 6 senadores en vez de 3). Conservan el resto de su
    # data; solo no cuentan como titulares de un estado en el mapa.
    def _estado_oficial(r):
        if r['camara'] == 'Senado' and r['origen'] == 'sil_inferido':
            return ''
        return r['estado']
    rows = [
        {**dict(r), 'comisiones_cargo': _limpia_comisiones(r['comisiones_cargo']),
         'estado': _estado_oficial(r)}
        for r in rows
    ]

    # Overrides manuales: comisiones que el scraper no puede obtener (p.ej.
    # senadores de licencia cuya adscripción real no está en los rosters
    # vigentes de senado.gob.mx). Se re-aplican en cada run, así el
    # INSERT OR REPLACE de abajo nunca los pierde.
    overrides_path = ROOT / "eval" / "comisiones_overrides.json"
    if overrides_path.exists():
        ov = json.loads(overrides_path.read_text())
        ov = {int(k): v for k, v in ov.items() if not k.startswith("_")}
        n_ov = 0
        for r in rows:
            if r['id'] in ov:
                r['comisiones_cargo'] = ov[r['id']]
                n_ov += 1
        if n_ov:
            logger.info(f"Overrides de comisiones aplicados: {n_ov}")

    logger.info(f"Snapshot de {len(rows)} legisladores → D1")

    # INSERT OR REPLACE para no romper FK de legisladores_perfil.
    # UPSERT que SOLO toca sus propias columnas. OJO: antes era INSERT OR
    # REPLACE, que en SQLite BORRA la fila y la re-inserta → todas las columnas
    # no listadas (n_ini_adherente/n_ini_de_grupo/n_prop_* de los conteos por
    # rol) se reseteaban a 0 EN CADA CORRIDA. Así murió en silencio el desglose
    # Ind/Grupo de Diputados (detectado en auditoría 7-jul).
    sqls = []
    for r in rows:
        sqls.append(
            "INSERT INTO legisladores "
            "(id, nombre, nombre_normalizado, camara, partido, estado, "
            "distrito, foto_url, legislatura, comisiones_cargo) VALUES ("
            f"{r['id']}, "
            f"{_sql_escape(r['nombre'])}, "
            f"{_sql_escape(r['nombre_normalizado'])}, "
            f"{_sql_escape(r['camara'])}, "
            f"{_sql_escape(r['partido'])}, "
            f"{_sql_escape(r['estado'])}, "
            f"{_sql_escape(r['distrito'])}, "
            f"{_sql_escape(r['foto_url'])}, "
            f"{_sql_escape(r['legislatura'])}, "
            f"{_sql_escape(r['comisiones_cargo'])}"
            ") ON CONFLICT(id) DO UPDATE SET "
            "nombre=excluded.nombre, "
            "nombre_normalizado=excluded.nombre_normalizado, "
            "camara=excluded.camara, "
            "partido=excluded.partido, "
            "estado=excluded.estado, "
            "distrito=excluded.distrito, "
            "foto_url=excluded.foto_url, "
            "legislatura=excluded.legislatura, "
            "comisiones_cargo=excluded.comisiones_cargo;"
        )

    # Batching: con comisiones_cargo incluido (puede ser 1-2KB por fila),
    # D1 rechaza payloads grandes con SQLITE_TOOBIG. Chunk pequeño.
    BATCH = 25
    for i in range(0, len(sqls), BATCH):
        ejecutar_sql_d1("\n".join(sqls[i:i + BATCH]))
    logger.info(f"Snapshot empujado: {len(rows)} filas (en {(len(sqls) + BATCH - 1) // BATCH} batches)")
    return {"snapshot_size": len(rows)}


# ────────────────────────────────────────────
# Paso 1: Scraper SIL incremental
# ────────────────────────────────────────────
SKIP_REFS_CACHE = ROOT / ".radar_skip_refs.json"


def _cargar_skip_refs() -> dict:
    """
    Lee el cache local de refs que ya sabemos que no aportan (sin match
    de legislador o HTML inválido). Estas refs NO cambian entre corridas
    del scraper — reprocesarlas es puro desperdicio de requests.

    Formato:
      {"sin_match": [ref, ...], "invalidas": [ref, ...], "ts": "..."}
    """
    if not SKIP_REFS_CACHE.exists():
        return {"sin_match": [], "invalidas": []}
    try:
        return json.loads(SKIP_REFS_CACHE.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"skip-refs cache corrupto, ignoro: {e}")
        return {"sin_match": [], "invalidas": []}


def _guardar_skip_refs(data: dict) -> None:
    data["ts"] = datetime.utcnow().isoformat()
    SKIP_REFS_CACHE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def paso_scraper_sil(
    db_ro: sqlite3.Connection,
    max_refs: int | None = None,
    ref_min: int | None = None,
    workers: int = 4,
    delay_por_worker: float = 1.0,
    flush_cada: int = 100,
    ignorar_skip_cache: bool = False,
) -> dict:
    """
    Corre el scraper SIL. Usa semaforo.db RO solo para matching de nombres.
    Escribe los perfiles capturados directamente a D1.

    Optimizaciones vs. versión secuencial:
      - ThreadPoolExecutor con `workers` threads (default 4).
      - Delay de `delay_por_worker`s por worker (no global). Con 4 workers
        y 1s eso da ~4 req/s globales, que SIL aguanta.
      - Skip-list persistente en `.radar_skip_refs.json`: refs que ya
        sabemos que no matchean legislador o que están vacías NO se
        reprocesan en runs futuros. Esto es ~70% de las refs del
        rango alto (suplentes / nombres raros).
      - Flush a D1 cada `flush_cada` (default 100) para amortizar el
        overhead de `wrangler`.
      - HTTP connection pool compartido (más keepalives).

    `max_refs` limita cuántas se procesan en este run. None = todas.
    """
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    import requests
    from requests.adapters import HTTPAdapter

    from scrapers.perfil_sil import (
        HEADERS,
        LXVI_REF_MAX,
        LXVI_REF_MIN,
        _get_threadsafe,
        _normalizar_nombre,
        parsear_perfil,
    )

    # Cargar índice local de nombres → legislador_id (para matching)
    # Dos índices:
    #   nombres_idx: exact match de nombre_normalizado (rápido)
    #   bigramas_idx: bigrama consecutivo → list[legislador_id]
    #     robusto contra reordenamiento Nombre↔Apellido, que pasa
    #     constante entre SIL, SITL, Senado y Cámara.
    nombres_idx = {}
    bigramas_idx: dict[str, list[int]] = {}
    PREP_STOP = {"de", "la", "las", "los", "del", "y", "e", "san", "santa", "da", "di"}

    def _bigramas_de(nombre_norm: str) -> list[str]:
        """Bigramas consecutivos de tokens ≥4 chars alfabéticos, skip preposiciones."""
        toks = [
            t for t in nombre_norm.split()
            if len(t) >= 4 and t.isalpha() and t not in PREP_STOP
        ]
        out = []
        for i in range(len(toks) - 1):
            bg = f"{toks[i]} {toks[i+1]}"
            if len(bg) >= 10:
                out.append(bg)
        return out

    # Índice adicional por token distintivo (apellidos/nombres raros).
    # Si un token ≥5 chars aparece en EXACTAMENTE UN legislador, es
    # prácticamente patognomónico — con verlo en el nombre SIL basta.
    token_count: dict[str, int] = {}
    token_to_leg: dict[str, int] = {}

    for row in db_ro.execute("SELECT id, nombre, nombre_normalizado FROM legisladores"):
        leg_id = row["id"]
        # Colapsar dobles espacios en el normalizado (hay datos sucios en
        # la tabla `legisladores` tipo "Díaz  Luis Armando" con 2 spaces)
        norm = " ".join((row["nombre_normalizado"] or "").split())
        if not norm:
            continue
        nombres_idx[norm] = leg_id
        for bg in _bigramas_de(norm):
            bigramas_idx.setdefault(bg, []).append(leg_id)
        # Tokens largos para matching por unicidad
        for t in norm.split():
            if len(t) >= 5 and t.isalpha() and t not in PREP_STOP:
                token_count[t] = token_count.get(t, 0) + 1
                token_to_leg[t] = leg_id  # guardamos el último; solo sirve si count==1

    tokens_unicos = {t: token_to_leg[t] for t, c in token_count.items() if c == 1}

    logger.info(
        f"Índice de matching: {len(nombres_idx)} legisladores, "
        f"{len(bigramas_idx)} bigramas únicos, "
        f"{len(tokens_unicos)} tokens patognomónicos"
    )

    # Leer refs ya presentes en D1 para modo incremental
    refs_en_d1 = _leer_refs_en_d1()
    refs_conocidas = set(refs_en_d1)
    logger.info(f"Refs ya cacheadas en D1: {len(refs_conocidas)}")

    # Cargar skip-list persistente
    skip_cache = {"sin_match": [], "invalidas": []}
    skip_set = set()
    if not ignorar_skip_cache:
        skip_cache = _cargar_skip_refs()
        skip_set = set(skip_cache.get("sin_match", [])) | set(skip_cache.get("invalidas", []))
        logger.info(
            f"Skip-list local: {len(skip_cache.get('sin_match', []))} sin_match + "
            f"{len(skip_cache.get('invalidas', []))} inválidas = {len(skip_set)} total"
        )

    # Estrategia:
    #   - Si D1 tiene <300 refs, hacemos descubrimiento del rango completo.
    #   - Si ya hay ≥300, solo refrescamos las conocidas (más cualquier
    #     ref nueva en el rango que no esté aún en D1).
    #   - Si --ref-min fue pasado, forzar descubrimiento (backfill manual).
    necesita_descubrimiento = len(refs_conocidas) < 300 or ref_min is not None

    if necesita_descubrimiento:
        # Descontar las refs que ya están en D1 y las del skip-list
        rango_inicio = max(LXVI_REF_MIN, ref_min) if ref_min else LXVI_REF_MIN
        rango_completo = set(range(rango_inicio, LXVI_REF_MAX + 1))
        refs_pendientes = sorted(rango_completo - refs_conocidas - skip_set)
        refs_a_procesar = refs_pendientes
        logger.info(
            f"Modo DESCUBRIMIENTO: {len(refs_pendientes)} refs pendientes "
            f"(rango {rango_inicio}-{LXVI_REF_MAX}, ya en D1: {len(refs_conocidas)}, "
            f"en skip: {len(skip_set)})"
        )
    else:
        refs_a_procesar = sorted(refs_conocidas)
        logger.info(f"Modo INCREMENTAL: refresh de {len(refs_a_procesar)} refs")

    if max_refs:
        refs_a_procesar = refs_a_procesar[:max_refs]
        logger.info(f"Truncado a {max_refs} refs por --max-refs")

    if not refs_a_procesar:
        logger.info("No hay refs que procesar. Fin del paso.")
        return {"procesadas": 0, "guardadas": 0, "sin_match": 0, "invalidas": 0}

    # HTTP session con connection pool para todos los workers
    session = requests.Session()
    session.headers.update(HEADERS)
    adapter = HTTPAdapter(pool_connections=workers * 2, pool_maxsize=workers * 2, max_retries=0)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    stats = {"procesadas": 0, "guardadas": 0, "sin_match": 0, "invalidas": 0}
    batch_sql: list[str] = []
    nuevos_sin_match: list[int] = []
    nuevos_invalidas: list[int] = []
    lock = threading.Lock()
    # Semáforo para rate-limit suave entre workers
    rate_lock = threading.Lock()
    ultimo_hit = [0.0]

    def _match_local(nombre_sil: str) -> int | None:
        """
        Match de un nombre SIL contra la tabla legisladores.
        Escalera de estrategias, de rápido a fuzzy:
          1. Match exacto por nombre_normalizado.
          2. Intersección de bigramas consecutivos. Si hay un solo
             candidato con ≥1 bigrama compartido (o el de mayor score
             si hay empate), es match.
          3. Token patognomónico: si el nombre SIL contiene un token
             ≥5 chars que es único en la tabla de legisladores, es match
             directo (ej. "Viggiano", "Téllez", "Samaniego").
          4. Fallback: últimos dos apellidos como substring.
        """
        norm = _normalizar_nombre(nombre_sil)
        if not norm:
            return None
        norm = " ".join(norm.split())  # colapsa dobles espacios
        # (1) exacto
        if norm in nombres_idx:
            return nombres_idx[norm]
        # (2) bigramas
        bgs = _bigramas_de(norm)
        if bgs:
            scores: dict[int, int] = {}
            for bg in bgs:
                for leg_id in bigramas_idx.get(bg, []):
                    scores[leg_id] = scores.get(leg_id, 0) + 1
            if scores:
                best_id, best_score = max(scores.items(), key=lambda kv: kv[1])
                if best_score >= 2:
                    return best_id
                if best_score == 1:
                    tied = [lid for lid, s in scores.items() if s == 1]
                    if len(tied) == 1:
                        return best_id
        # (3) token patognomónico
        for t in norm.split():
            if len(t) >= 5 and t.isalpha() and t not in PREP_STOP:
                if t in tokens_unicos:
                    return tokens_unicos[t]
        # (4) fallback por substring de apellidos
        partes = norm.split()
        if len(partes) >= 2:
            apellidos = " ".join(partes[-2:])
            for key, leg_id in nombres_idx.items():
                if apellidos in key:
                    return leg_id
        return None

    def _respetar_rate_limit():
        """Espaciado global suave entre requests. Con 4 workers y 0.25s
        de gap mínimo entre hits consecutivos obtenemos ~4 req/s."""
        gap_min = delay_por_worker / max(workers, 1)
        with rate_lock:
            dt = time.time() - ultimo_hit[0]
            if dt < gap_min:
                time.sleep(gap_min - dt)
            ultimo_hit[0] = time.time()

    def _procesar_ref(ref: int) -> tuple[int, str, dict | None]:
        """
        Fetch + parse + match de una ref. Retorna (ref, estado, payload)
        donde estado ∈ {'guardada', 'sin_match', 'invalida'}.
        """
        _respetar_rate_limit()
        html = _get_threadsafe(session, ref)
        if not html:
            return (ref, "invalida", None)
        parsed = parsear_perfil(html, ref)
        if not parsed:
            return (ref, "invalida", None)
        perfil = parsed["perfil"]
        leg_id = _match_local(perfil["nombre_sil"])
        if not leg_id:
            return (ref, "sin_match", None)
        return (ref, "guardada", {"leg_id": leg_id, "perfil": perfil})

    logger.info(
        f"Lanzando ThreadPoolExecutor con {workers} workers. "
        f"Delay efectivo ~{delay_por_worker/workers:.2f}s entre requests globales. "
        f"Refs a procesar: {len(refs_a_procesar)}"
    )
    sys.stdout.flush()

    t0 = time.time()
    completadas = 0
    total = len(refs_a_procesar)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_procesar_ref, ref): ref for ref in refs_a_procesar}
        for fut in as_completed(futures):
            try:
                ref, estado, payload = fut.result()
            except Exception as e:
                logger.warning(f"  Future explotó: {e}")
                ref = futures[fut]
                estado = "invalida"
                payload = None

            completadas += 1
            with lock:
                stats["procesadas"] += 1
                if estado == "guardada" and payload:
                    stats["guardadas"] += 1
                    perfil = payload["perfil"]
                    leg_id = payload["leg_id"]
                    estudios = f'{perfil["estudios"]} - {perfil["preparacion"]}'.strip(" -")
                    sql_row = (
                        "INSERT INTO legisladores_perfil "
                        "(legislador_id, biografia, anio_nacimiento, profesion, estudios, "
                        "foto_hd_url, fuente_scraping, fecha_scraping) "
                        f"VALUES ({leg_id}, {_sql_escape(perfil['biografia'])}, "
                        f"{perfil['anio_nacimiento'] or 'NULL'}, "
                        f"{_sql_escape(perfil['profesion'])}, "
                        f"{_sql_escape(estudios)}, "
                        f"{_sql_escape(perfil['foto_hd_url'])}, "
                        f"{_sql_escape(f'SIL:{ref}')}, "
                        f"{_sql_escape(datetime.utcnow().isoformat())}) "
                        "ON CONFLICT(legislador_id) DO UPDATE SET "
                        "biografia=excluded.biografia, "
                        "anio_nacimiento=excluded.anio_nacimiento, "
                        "profesion=excluded.profesion, "
                        "estudios=excluded.estudios, "
                        "foto_hd_url=excluded.foto_hd_url, "
                        "fuente_scraping=excluded.fuente_scraping, "
                        "fecha_scraping=excluded.fecha_scraping;"
                    )
                    batch_sql.append(sql_row)
                elif estado == "sin_match":
                    stats["sin_match"] += 1
                    nuevos_sin_match.append(ref)
                else:
                    stats["invalidas"] += 1
                    nuevos_invalidas.append(ref)

                # Flush a D1 cada flush_cada guardadas
                if len(batch_sql) >= flush_cada:
                    to_flush = batch_sql[:]
                    batch_sql.clear()
                else:
                    to_flush = None

            if to_flush:
                ejecutar_sql_d1("\n".join(to_flush))
                elapsed = time.time() - t0
                rate = completadas / elapsed if elapsed else 0
                eta = (total - completadas) / rate if rate else 0
                logger.info(
                    f"  Flush a D1 ({len(to_flush)} filas). "
                    f"Avance {completadas}/{total} ({rate:.1f} req/s, ETA {eta/60:.1f}m). "
                    f"stats={stats}"
                )
                sys.stdout.flush()

            # Progress log cada 50 completadas
            if completadas % 50 == 0:
                elapsed = time.time() - t0
                rate = completadas / elapsed if elapsed else 0
                eta = (total - completadas) / rate if rate else 0
                logger.info(
                    f"  Progreso {completadas}/{total} "
                    f"({rate:.1f} req/s, ETA {eta/60:.1f}m) stats={stats}"
                )
                sys.stdout.flush()

    # Flush final
    if batch_sql:
        ejecutar_sql_d1("\n".join(batch_sql))

    # Persistir skip-list actualizada
    if not ignorar_skip_cache:
        skip_cache["sin_match"] = sorted(set(skip_cache.get("sin_match", [])) | set(nuevos_sin_match))
        skip_cache["invalidas"] = sorted(set(skip_cache.get("invalidas", [])) | set(nuevos_invalidas))
        _guardar_skip_refs(skip_cache)
        logger.info(
            f"Skip-list actualizada: +{len(nuevos_sin_match)} sin_match, "
            f"+{len(nuevos_invalidas)} inválidas. "
            f"Total ahora: {len(skip_cache['sin_match'])}+{len(skip_cache['invalidas'])}"
        )

    elapsed = time.time() - t0
    logger.info(
        f"Scraper SIL terminado en {elapsed/60:.1f}m "
        f"({stats['procesadas']/elapsed:.1f} req/s): {stats}"
    )
    return stats


def _leer_refs_en_d1() -> list[int]:
    """Lee las refs SIL ya presentes en D1."""
    try:
        cmd = [
            "npx",
            "wrangler",
            "d1",
            "execute",
            D1_DB,
            "--remote",
            "--command",
            "SELECT fuente_scraping FROM legisladores_perfil "
            "WHERE fuente_scraping LIKE 'SIL:%';",
            "--json",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            return []
        data = json.loads(result.stdout)
        refs = []
        for entry in data:
            for row in entry.get("results", []):
                src = row.get("fuente_scraping", "")
                if src.startswith("SIL:"):
                    try:
                        refs.append(int(src[4:]))
                    except ValueError:
                        pass
        return refs
    except Exception as e:
        logger.warning(f"No se pudo leer refs en D1: {e}")
        return []


# ────────────────────────────────────────────
# Paso 2: Hit rate por legislador (Fase 2)
# ────────────────────────────────────────────
# Constantes de la metodología (pico, ventanas, ancla LXVI) movidas a
# config.py — UNA SOLA FUENTE DE VERDAD compartida con
# api/predictor_autoria.py (predicción de autoría usa el mismo hit rate).
from config import (  # noqa: E402
    FECHA_INICIO_LXVI,
    PICO_SCORE_MEDIA_MIN,
    HITRATE_VENTANA_PICOS,
    HITRATE_VENTANA_DIAS,
)


def paso_hit_rate(db_ro: sqlite3.Connection) -> dict:
    """
    Hit rate **recalibrado** (Fase 2.5).

    Antes: leía `reacciones_historicas`, que ya está pre-filtrada a casos
    donde el legislador reaccionó → hit_rate saturaba ≥0.89. Sesgo grave.

    Ahora: cruza directamente contra `scores` (picos mediáticos reales,
    hayan respondido o no) y `actividad_legislador` (presentación ≤ ventana).

    Fórmula:
      - Para cada categoría: extraer los últimos N picos donde
        score_media ≥ PICO_SCORE_MEDIA_MIN.
      - Para cada legislador con categoría dominante C:
        respondio = # picos en C donde el legislador presentó un
          instrumento de categoría C entre [pico_fecha, pico_fecha+7d]
        total = # picos evaluados
        hit_rate = respondio / total

    Escritura: `legisladores_hit_rate` + `legisladores_stats` en D1.
    """
    logger.info("Cálculo de hit rate por legislador (recalibrado)…")

    # 1) Categoría dominante por legislador.
    #    Mínimo 3 actos en la categoría, ponderado por tipo de instrumento:
    #    iniciativa = 2x, resto = 1x. Las iniciativas reflejan el área de
    #    trabajo sustantivo mejor que las proposiciones, que suelen ser
    #    exhortos genéricos (ej. Chedraui: 5 iniciativas de medio ambiente
    #    vs 8 proposiciones mal clasificadas como electoral_politico).
    #    Tie-break determinístico: score ponderado → count real → nombre asc.
    cats_por_leg: dict[int, str] = {}
    raw_counts: dict[int, dict[str, dict[str, float]]] = {}
    # OPCIÓN B (decisión usuario 12-may): para CATEGORÍA DOMINANTE incluir
    # también firmas colectivas, pero con peso reducido (0.3). Esto da
    # cobertura universal: senadores PRI/PVEM que firman casi todo con
    # bancada también reciben categoría dominante basada en lo que su
    # bloque empuja. Hit Rate y todo lo demás siguen calculándose contra
    # esta categoría enriquecida.
    # Pesos: iniciativa individual=2.0, prop individual=1.0,
    #        iniciativa colectiva=0.6, prop colectiva=0.3.
    for row in db_ro.execute(
        """
        SELECT legislador_id, categoria, tipo_instrumento,
               CASE WHEN co_firmantes IS NULL OR co_firmantes = ''
                    THEN 1 ELSE 0 END as es_indiv,
               COUNT(*) as n
        FROM actividad_legislador
        WHERE legislador_id IS NOT NULL
          AND categoria IS NOT NULL AND categoria <> ''
          AND fecha_presentacion >= ?
        GROUP BY legislador_id, categoria, tipo_instrumento, es_indiv
        """,
        (FECHA_INICIO_LXVI,),
    ):
        leg_id = row["legislador_id"]
        cat = row["categoria"]
        tipo_norm = (row["tipo_instrumento"] or "").lower()
        es_iniciativa = "iniciativa" in tipo_norm
        es_indiv = bool(row["es_indiv"])
        if es_indiv:
            peso = 2.0 if es_iniciativa else 1.0
        else:
            peso = 0.6 if es_iniciativa else 0.3
        n = int(row["n"] or 0)
        leg_bucket = raw_counts.setdefault(leg_id, {})
        cat_bucket = leg_bucket.setdefault(cat, {"count": 0.0, "score": 0.0})
        cat_bucket["count"] += n
        cat_bucket["score"] += n * peso

    for leg_id, cats in raw_counts.items():
        # Threshold reducido de ≥3 a ≥1 (decisión 12-may con Opción B).
        # Antes excluía a quienes firman poco. Ahora cualquier legislador
        # con al menos 1 acto en una categoría puede tener categoría
        # dominante. Tie-break por score ponderado mantiene calidad
        # (iniciativa individual pesa 2.0, colectiva 0.6).
        candidates = {c: v for c, v in cats.items() if v["count"] >= 1}
        if not candidates:
            continue
        best_cat = sorted(
            candidates.items(),
            key=lambda kv: (-kv[1]["score"], -kv[1]["count"], kv[0]),
        )[0][0]
        cats_por_leg[leg_id] = best_cat

    logger.info(
        f"  Legisladores con categoría dominante (≥3 actos, iniciativa 2x): "
        f"{len(cats_por_leg)}"
    )

    # 2) Picos REALES por categoría: z>=1.5 del conteo diario de notas de la
    #    categoría vs su ritmo ordinario (mismo detector estadístico que
    #    reactividad), días consecutivos (gap<=7) colapsados a UN evento cuya
    #    fecha es el día de inicio. ANTES: score_media>=55 sobre el score
    #    SATURADO → 50-52% de TODOS los días eran "pico" (auditoría 6-jul) y
    #    el hit rate medía "actividad reciente en la categoría", no respuesta
    #    a picos mediáticos como promete el tooltip.
    from datetime import date as _date, timedelta as _td
    _d0 = datetime.strptime(FECHA_INICIO_LXVI, "%Y-%m-%d").date()
    _hoy = datetime.utcnow().date()
    _NDIAS = (_hoy - _d0).days + 1
    _fidx = {(_d0 + _td(days=_i)).isoformat(): _i for _i in range(_NDIAS)}
    _series: dict[str, list[float]] = {}
    for _row in db_ro.execute(
        "SELECT substr(fecha,1,10) AS f, categorias FROM articulos "
        "WHERE fecha >= ? AND categorias IS NOT NULL AND categorias != ''",
        (FECHA_INICIO_LXVI,),
    ):
        _i = _fidx.get(_row["f"])
        if _i is None:
            continue
        for _tok in (_row["categorias"] or "").split(","):
            _c = _tok.split(":")[0].strip()
            if _c:
                _series.setdefault(_c, [0.0] * _NDIAS)[_i] += 1
    # z-score sobre el SHARE del día (proporción de la cobertura total), NO
    # sobre conteos crudos: el scraper tiene huecos y ráfagas de recuperación
    # (ej. 5-8 abr ~10-50 notas/día y el 9-abr 1,470) que disparaban "evento"
    # en TODAS las categorías a la vez. El share es inmune al volumen.
    _tot = [0.0] * _NDIAS
    for _arr in _series.values():
        for _i in range(_NDIAS):
            _tot[_i] += _arr[_i]
    _MIN_TOTAL_DIA = 100  # share inestable en días de captura rala
    # ritmo ordinario = días no-receso (mismos meses que excluye reactividad)
    _ord_idx = [_i for _i in range(_NDIAS)
                if (_d0 + _td(days=_i)).month not in (1, 5, 6, 7, 8)
                and _tot[_i] >= _MIN_TOTAL_DIA]
    picos_por_cat: dict[str, list[str]] = {}  # {cat: [fecha_inicio_evento] desc}
    for _c, _arr in _series.items():
        _shares = {_i: _arr[_i] / _tot[_i] for _i in range(_NDIAS)
                   if _tot[_i] >= _MIN_TOTAL_DIA}
        _vals = [_shares[_i] for _i in _ord_idx if _i in _shares]
        if len(_vals) < 20:
            continue
        _mu = sum(_vals) / len(_vals)
        _sd = (sum((_v - _mu) ** 2 for _v in _vals) / len(_vals)) ** 0.5
        if _sd == 0:
            continue
        _dias = [_i for _i in sorted(_shares)
                 if _arr[_i] > 0 and (_shares[_i] - _mu) / _sd >= 1.5]
        _evs: list[list[int]] = []
        for _i in _dias:
            if _evs and _i - _evs[-1][1] <= 7:
                _evs[-1][1] = _i
            else:
                _evs.append([_i, _i])
        picos_por_cat[_c] = [(_d0 + _td(days=_s)).isoformat()
                             for _s, _ in reversed(_evs)]

    logger.info(
        f"  Picos REALES (z≥1.5, eventos colapsados) en "
        f"{len(picos_por_cat)} categorías · total={sum(len(v) for v in picos_por_cat.values())}"
    )

    # 3) Pre-cargar set de (legislador_id, categoria, fecha) de actividad
    #    para lookup O(1) sin hacer 10 queries por legislador.
    actividad_set: set[tuple[int, str, str]] = set()
    # Opción B: incluir colectivas. Si el legislador (o su bancada con
    # él dentro) presentó algo en el rango del pico, cuenta como "respondió".
    # Esto da cobertura universal para Hit Rate.
    for row in db_ro.execute(
        """
        SELECT legislador_id, categoria, fecha_presentacion
        FROM actividad_legislador
        WHERE legislador_id IS NOT NULL
          AND categoria IS NOT NULL AND categoria <> ''
          AND fecha_presentacion IS NOT NULL AND fecha_presentacion <> ''
          AND fecha_presentacion >= ?
        """,
        (FECHA_INICIO_LXVI,),
    ):
        actividad_set.add(
            (row["legislador_id"], row["categoria"], row["fecha_presentacion"])
        )
    logger.info(f"  Actos de actividad indexados: {len(actividad_set)}")

    # 3b) Vínculos CONFIRMADOS evento→instrumento (juez; eval/vinculos_
    #     produccion.json). Test adicional de "respondió": el legislador tiene
    #     un vínculo cuya NOTA cae en la ventana del pico Y es del TEMA del
    #     pico (keywords de la categoría — sin esto, coincidencias de pura
    #     fecha inflaban 0%→95%, validado 3-jul). Solo SUMA hits verificados;
    #     corrige subconteo por misclasificación (ej. exhorto Abud → "trabajo").
    notas_vinc: dict[int, list[tuple[str, str]]] = {}
    _cat_matchers: dict[str, list] = {}
    _ruta_vinc = ROOT / "eval" / "vinculos_produccion.json"
    if _ruta_vinc.exists():
        from scripts.reactividad_pipeline import norm as _rnorm, mk as _rmk, mt as _rmt
        from config import CATEGORIAS as _CATS
        for _cat, _cfg in _CATS.items():
            _kws = list(_cfg.get("keywords", [])) if isinstance(_cfg, dict) else []
            _subs = _cfg.get("subcategorias", {}) if isinstance(_cfg, dict) else {}
            if isinstance(_subs, dict):
                for _sc in _subs.values():
                    if isinstance(_sc, dict):
                        _kws += _sc.get("keywords", [])
            if _kws:
                _cat_matchers[_cat] = _rmk(_kws)
        for _x in json.loads(_ruta_vinc.read_text()).get("vinculos", []):
            for _row in db_ro.execute(
                "SELECT al.legislador_id AS lid FROM actividad_legislador al "
                "JOIN sil_documentos sd ON sd.id = al.sil_documento_id "
                "WHERE sd.seguimiento_id = ? AND al.legislador_id IS NOT NULL",
                (_x["sil_id"],),
            ):
                notas_vinc.setdefault(int(_row["lid"]), []).append(
                    (_x["nota_fecha"], _rnorm(_x["nota_titulo"])))
        logger.info(
            f"  Vínculos confirmados: {sum(len(v) for v in notas_vinc.values())} "
            f"notas · {len(notas_vinc)} legisladores")

    # 4) Calcular hit rate por legislador
    batch_sql: list[str] = []
    ahora = datetime.utcnow().isoformat()
    stats_rows: list[tuple[int, str, float]] = []
    calculados = 0
    distro_hits = {"0": 0, "<25": 0, "<50": 0, "<75": 0, ">=75": 0}

    from datetime import timedelta
    for leg_id, categoria in cats_por_leg.items():
        picos = picos_por_cat.get(categoria, [])[:HITRATE_VENTANA_PICOS]
        if not picos:
            continue
        total = len(picos)
        respondio = 0
        _ms_cat = _cat_matchers.get(categoria)
        _notas_leg = notas_vinc.get(leg_id, [])
        for pico_str in picos:
            try:
                d0 = datetime.strptime(pico_str, "%Y-%m-%d").date()
            except Exception:
                continue
            # Ventana bidireccional [d0 - ventana/2, d0 + ventana/2].
            # El legislador a veces actúa ANTES del pico (puede causarlo).
            reacciono = False
            mitad = HITRATE_VENTANA_DIAS // 2
            ventana_fechas = set()
            for delta in range(-mitad, HITRATE_VENTANA_DIAS - mitad + 1):
                d = (d0 + timedelta(days=delta)).isoformat()
                ventana_fechas.add(d)
                if (leg_id, categoria, d) in actividad_set:
                    reacciono = True
                    break
            # Test por vínculo confirmado: nota en ventana Y del tema del pico
            if not reacciono and _ms_cat and _notas_leg:
                for _nf, _nt in _notas_leg:
                    if _nf in ventana_fechas and _rmt(_nt, _ms_cat):
                        reacciono = True
                        break
            if reacciono:
                respondio += 1

        hit_rate = respondio / total if total else 0.0

        # Telemetría de distribución
        pct = hit_rate * 100
        if pct == 0:
            distro_hits["0"] += 1
        elif pct < 25:
            distro_hits["<25"] += 1
        elif pct < 50:
            distro_hits["<50"] += 1
        elif pct < 75:
            distro_hits["<75"] += 1
        else:
            distro_hits[">=75"] += 1

        batch_sql.append(
            "INSERT INTO legisladores_hit_rate "
            "(legislador_id, categoria, ventana_picos, ventana_dias, "
            "respondio, total_oportunidades, fecha_calculo) VALUES ("
            f"{leg_id}, {_sql_escape(categoria)}, "
            f"{HITRATE_VENTANA_PICOS}, {HITRATE_VENTANA_DIAS}, "
            f"{respondio}, {total}, {_sql_escape(ahora)}) "
            "ON CONFLICT(legislador_id, categoria, ventana_picos, ventana_dias) "
            "DO UPDATE SET respondio=excluded.respondio, "
            "total_oportunidades=excluded.total_oportunidades, "
            "fecha_calculo=excluded.fecha_calculo;"
        )
        stats_rows.append((leg_id, categoria, hit_rate))
        calculados += 1

        if len(batch_sql) >= 200:
            ejecutar_sql_d1("\n".join(batch_sql))
            batch_sql = []

    if batch_sql:
        ejecutar_sql_d1("\n".join(batch_sql))

    # 5) Actualizar legisladores_stats (categoria_dominante + prob)
    if stats_rows:
        stats_sqls = []
        for leg_id, cat, hr in stats_rows:
            stats_sqls.append(
                "INSERT INTO legisladores_stats "
                "(legislador_id, fecha_calculo, categoria_dominante, prob_reaccion_dominante) "
                f"VALUES ({leg_id}, {_sql_escape(ahora)}, {_sql_escape(cat)}, {hr:.4f}) "
                "ON CONFLICT(legislador_id) DO UPDATE SET "
                "fecha_calculo=excluded.fecha_calculo, "
                "categoria_dominante=excluded.categoria_dominante, "
                "prob_reaccion_dominante=excluded.prob_reaccion_dominante;"
            )
        for i in range(0, len(stats_sqls), 200):
            ejecutar_sql_d1("\n".join(stats_sqls[i:i + 200]))

    logger.info(
        f"Hit rate: {calculados} legisladores → D1. "
        f"Distribución: {distro_hits}"
    )
    return {"calculados": calculados, "distribucion": distro_hits}


# ────────────────────────────────────────────
# Paso 3: Matchup grade (tasa de dictamen de comisión, percentil relativo)
# ────────────────────────────────────────────
MATCHUP_MIN_DOCS_COM = 10   # mínimo de docs en comisión para calcular tasa
MATCHUP_MIN_DOCS_LEG = 2    # mínimo de docs del legislador en su comisión dominante


def _norm_turno_matchup(s: str) -> str:
    """Normaliza un comision_turno: quita el sufijo de cámara y el boilerplate
    de co-comisión de las 'comisiones unidas' del Senado, para que variantes de
    la misma comisión se agreguen juntas y puedan calificarse. NO adivina la
    comisión primaria (no parte en comas ambiguas)."""
    if not s:
        return s
    t = re.sub(r"\s+de la C[aá]mara de (Senadores|Diputados).*$", "", s.strip(), flags=re.I)
    t = re.sub(r"[,;]?\s*y?\s*de (Estudios Legislativos|Reglamentos y Pr[aá]cticas Parlamentarias)[,;]?\s*(Primera|Segunda)?\s*$", "", t, flags=re.I)
    return t.strip().rstrip(",;").strip()


def paso_matchup_grade(db_ro: sqlite3.Connection) -> dict:
    """
    TRACCIÓN del LEGISLADOR (propia, NO la de su comisión).

    Mide qué % de SUS instrumentos individuales sustantivos han avanzado
    (dictaminados = aprobados + desechados + retiradas) frente a los que siguen
    en pendiente. Es una nota PROPIA de cada legislador; la calificación de la
    comisión es algo aparte (vive en la pestaña Comisiones).

    Asignación (entre legisladores con ≥3 instrumentos propios):
      - Los que SÍ mueven cosas (tasa>0) se rankean en 4 bandas parejas:
        A=Alta tracción (top 25%) · B=Buena · C=Media · D=Baja.
      - Los que NO han movido nada (tasa=0) → F = Sin tracción suficiente.
      - <3 instrumentos propios → sin nota (NULL): muestra muy chica.

    matchup_comision_target guarda su comisión dominante solo como contexto.
    """
    logger.info("Cálculo de tracción por legislador (propia)…")
    MIN_INSTR = 3

    # 1) Tasa de dictamen de los instrumentos PROPIOS (individuales, sustantivos)
    rows = db_ro.execute(
        """
        SELECT al.legislador_id,
               SUM(CASE WHEN sd.estatus LIKE '%Aprobado%' OR sd.estatus LIKE 'Desechado%'
                         OR sd.estatus LIKE 'Rechazado%' OR sd.estatus LIKE 'Retirada%'
                        THEN 1 ELSE 0 END) AS decididos,
               COUNT(*) AS total
        FROM actividad_legislador al
        JOIN sil_documentos sd ON al.sil_documento_id = sd.id
        WHERE al.legislador_id IS NOT NULL
          AND al.fecha_presentacion >= ?
          AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
          AND (LOWER(al.tipo_instrumento) LIKE '%iniciativ%' OR LOWER(al.tipo_instrumento) LIKE '%proposici%')
        GROUP BY al.legislador_id
        """,
        (FECHA_INICIO_LXVI,),
    ).fetchall()

    tasa_leg: dict[int, float] = {}
    movers, zeros = [], []
    for r in rows:
        if (r["total"] or 0) < MIN_INSTR:
            continue
        tasa = (r["decididos"] or 0) / r["total"]
        tasa_leg[r["legislador_id"]] = tasa
        (movers if tasa > 0 else zeros).append((r["legislador_id"], tasa))

    # 2) Ranking ENTRE LEGISLADORES (no comisiones): movers en 4 bandas parejas;
    #    los que no mueven nada → F. Distribución pareja, nota propia.
    movers.sort(key=lambda x: -x[1])
    m = len(movers)
    grade_leg: dict[int, str] = {}
    for idx, (lid, _) in enumerate(movers):
        pct = idx / m if m else 0
        grade_leg[lid] = "A" if pct < 0.25 else "B" if pct < 0.50 else "C" if pct < 0.75 else "D"
    for lid, _ in zeros:
        grade_leg[lid] = "F"

    # 3) Comisión dominante (solo contexto de display)
    leg_rows = db_ro.execute(
        """
        SELECT al.legislador_id, al.comision_turno, COUNT(*) AS total
        FROM actividad_legislador al
        WHERE al.legislador_id IS NOT NULL
          AND al.comision_turno IS NOT NULL AND al.comision_turno <> ''
          AND al.fecha_presentacion >= ?
          AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
        GROUP BY al.legislador_id, al.comision_turno
        """,
        (FECHA_INICIO_LXVI,),
    ).fetchall()
    com_dom: dict[int, tuple] = {}
    for r in leg_rows:
        lid = r["legislador_id"]
        if lid not in com_dom or r["total"] > com_dom[lid][1]:
            com_dom[lid] = (_norm_turno_matchup(r["comision_turno"]), r["total"])

    # 4) Escribir a D1
    ahora = datetime.utcnow().isoformat()
    stats_sqls: list[str] = []
    distro = {"A": 0, "B": 0, "C": 0, "D": 0, "F": 0, "NULL": 0}
    todos = set(grade_leg) | set(com_dom)
    for lid in todos:
        grade = grade_leg.get(lid)
        distro[grade if grade else "NULL"] += 1
        comision = com_dom.get(lid, (None, 0))[0]
        tasa_val = f"{tasa_leg[lid]:.4f}" if lid in tasa_leg else "NULL"
        stats_sqls.append(
            "INSERT INTO legisladores_stats "
            "(legislador_id, fecha_calculo, matchup_grade, "
            "matchup_comision_target, matchup_tasa_dictamen) VALUES ("
            f"{lid}, {_sql_escape(ahora)}, "
            f"{_sql_escape(grade) if grade else 'NULL'}, "
            f"{_sql_escape(comision) if comision else 'NULL'}, {tasa_val}) "
            "ON CONFLICT(legislador_id) DO UPDATE SET "
            "fecha_calculo=excluded.fecha_calculo, "
            "matchup_grade=excluded.matchup_grade, "
            "matchup_comision_target=excluded.matchup_comision_target, "
            "matchup_tasa_dictamen=excluded.matchup_tasa_dictamen;"
        )

    for i in range(0, len(stats_sqls), 200):
        ejecutar_sql_d1("\n".join(stats_sqls[i:i + 200]))

    logger.info(f"Tracción por legislador: {len(grade_leg)} con nota de {len(todos)}. Distribución: {distro}")
    return {"calculados": len(todos), "distribucion": distro}


# ────────────────────────────────────────────
# Paso 4: Proyecciones forward 15 días (Fase 2)
# ────────────────────────────────────────────
VENTANA_PROYECCION_DIAS = 15
VENTANA_BASELINE_DIAS = 270  # L3P ≈ 9 meses


def paso_proyecciones(db_ro: sqlite3.Connection) -> dict:
    """
    Proyección forward 15 días por legislador.

    Metodología simple (Fase 2):
      baseline_diario = actividad(últimos 270d) / 270
      factor_reciente = actividad(últimos 30d) / 30 / baseline_diario
                        (capped en [0.3, 3.0] para evitar ruido)
      proyeccion_15d  = baseline_diario * factor_reciente * 15

    Se calcula por separado para iniciativas y proposiciones
    (tipo_instrumento como filtro). También se guarda el promedio L3P
    bruto para la narrativa.

    Destino: `legisladores_stats` (iniciativas_proy_15d,
    proposiciones_proy_15d, promedio_l3p_*).
    """
    logger.info("Cálculo de proyecciones forward 15d…")

    # Migration: añadir columnas para conteos colectivos (idempotente).
    # Worker D1 las expondrá si existen, sino devuelve null.
    try:
        ejecutar_sql_d1(
            "ALTER TABLE legisladores_stats ADD COLUMN promedio_l3p_iniciativas_col REAL DEFAULT 0.0;\n"
            "ALTER TABLE legisladores_stats ADD COLUMN promedio_l3p_proposiciones_col REAL DEFAULT 0.0;"
        )
        logger.info("  Columnas col agregadas a legisladores_stats")
    except Exception as e:
        # Ignorar si ya existen (segundo run en adelante)
        logger.debug(f"  Migration cols col: {e}")

    # Patrones LIKE (case-insensitive) para tipo_instrumento.
    # Usamos LIKE en vez de IN porque el SIL devuelve strings completos
    # como "Proposición con punto de acuerdo", no solo "proposicion".
    # Chedraui tiene 16 proposiciones en SIL que el tuple anterior
    # ("proposicion", "Proposicion", "proposición", "Proposición") nunca
    # matcheaba. Con LIKE '%proposici%' sí.
    TIPOS_INICIATIVA = ("%iniciativ%",)
    TIPOS_PROPOSICION = ("%proposici%",)

    # Días transcurridos desde inicio LXVI (baseline real de la legislatura)
    fila = db_ro.execute(
        "SELECT CAST(julianday('now') - julianday(?) AS INTEGER) as d",
        (FECHA_INICIO_LXVI,),
    ).fetchone()
    DIAS_LXVI = max(int(fila["d"] or 0), 1)
    logger.info(f"  Baseline LXVI: {DIAS_LXVI} días desde {FECHA_INICIO_LXVI}")

    # Instrumentos REACTIVOS confirmados (vínculo evento→instrumento, juez):
    # se proyectan con su promedio de largo plazo, NO con el ritmo reciente —
    # una ráfaga de 6 exhortos por una explosión no es un ritmo nuevo y antes
    # inflaba el factor_reciente. Set de sil_documento_id para exclusión.
    _react_ids: set[int] = set()
    _vinc_path = ROOT / "eval" / "vinculos_produccion.json"
    if _vinc_path.exists():
        _sids = [v["sil_id"] for v in json.loads(_vinc_path.read_text()).get("vinculos", [])]
        for _i in range(0, len(_sids), 500):
            _chunk = _sids[_i:_i + 500]
            _q = ",".join("?" * len(_chunk))
            for _r in db_ro.execute(
                    f"SELECT id FROM sil_documentos WHERE seguimiento_id IN ({_q})", _chunk):
                _react_ids.add(int(_r["id"]))
        logger.info(f"  Instrumentos reactivos confirmados (proyección suavizada): {len(_react_ids)}")
    _react_csv = ",".join(str(i) for i in sorted(_react_ids)) or "0"

    def _tasa_lxvi(leg_id: int, tipos: tuple, solo_individual: bool = True,
                   reactivos: str = "ambos") -> float:
        """Actividad del tipo desde inicio LXVI / días_lxvi.

        Por defecto (solo_individual=True): solo cuenta actos donde el
        legislador es promovente único. Las firmas con bancada se excluyen
        del ranking de eficiencia personal porque son acción colectiva
        del partido. Si solo_individual=False, cuenta todas las actividades.
        reactivos: 'ambos' (todo) | 'sin' (excluye reactivos) | 'solo' (solo reactivos).
        """
        like_clause = " OR ".join(["LOWER(tipo_instrumento) LIKE ?"] * len(tipos))
        filtro_ind = " AND (co_firmantes IS NULL OR co_firmantes = '')" if solo_individual else ""
        filtro_react = ""
        if reactivos == "sin":
            filtro_react = f" AND (sil_documento_id IS NULL OR sil_documento_id NOT IN ({_react_csv}))"
        elif reactivos == "solo":
            filtro_react = f" AND sil_documento_id IN ({_react_csv})"
        row = db_ro.execute(
            f"""
            SELECT COUNT(*) as n FROM actividad_legislador
            WHERE legislador_id = ?
              AND ({like_clause})
              AND fecha_presentacion >= ?{filtro_ind}{filtro_react}
            """,
            (leg_id, *tipos, FECHA_INICIO_LXVI),
        ).fetchone()
        return (row["n"] or 0) / DIAS_LXVI

    def _tasa_reciente(leg_id: int, tipos: tuple, dias: int, solo_individual: bool = True,
                       reactivos: str = "ambos") -> float:
        like_clause = " OR ".join(["LOWER(tipo_instrumento) LIKE ?"] * len(tipos))
        filtro_ind = " AND (co_firmantes IS NULL OR co_firmantes = '')" if solo_individual else ""
        filtro_react = ""
        if reactivos == "sin":
            filtro_react = f" AND (sil_documento_id IS NULL OR sil_documento_id NOT IN ({_react_csv}))"
        elif reactivos == "solo":
            filtro_react = f" AND sil_documento_id IN ({_react_csv})"
        row = db_ro.execute(
            f"""
            SELECT COUNT(*) as n FROM actividad_legislador
            WHERE legislador_id = ?
              AND ({like_clause})
              AND fecha_presentacion >= date('now', '-{dias} days'){filtro_ind}{filtro_react}
            """,
            (leg_id, *tipos),
        ).fetchone()
        return (row["n"] or 0) / dias

    # Set de legisladores con actividad durante la LXVI
    leg_activos = [
        row["legislador_id"] for row in db_ro.execute(
            """
            SELECT DISTINCT legislador_id FROM actividad_legislador
            WHERE legislador_id IS NOT NULL
              AND fecha_presentacion >= ?
            """,
            (FECHA_INICIO_LXVI,),
        )
    ]

    logger.info(f"  Legisladores activos en LXVI: {len(leg_activos)}")

    ahora = datetime.utcnow().isoformat()
    batch_sql: list[str] = []
    calculados = 0

    # ── FUENTE DE VERDAD del conteo: reconteo del SIL ──
    # Los conteos desde actividad_legislador estaban inflados (efemérides contadas
    # como proposiciones + duplicados de scrape: Waldo daba 187/122 vs 62/48 real).
    # recontar_instrumentos_sil.py trae el conteo limpio "como promovente",
    # verificable contra senado.gob.mx/SIL. Si existe, MANDA sobre el cálculo local.
    import json as _json, os as _os
    _reconteo = {}
    _rc_path = _os.path.join(
        _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))),
        "eval", "reconteo_sil.json")
    try:
        if _os.path.exists(_rc_path):
            _reconteo = _json.loads(open(_rc_path, encoding="utf-8").read())
            logger.info(f"  Reconteo SIL cargado: {len(_reconteo)} legisladores (fuente de verdad del conteo)")
    except Exception as e:
        logger.warning(f"  No se pudo cargar reconteo SIL: {e}")

    # Bancada (suscritas/en grupo) de senadores, desde el perfil del Senado.
    # total_perfil − promovente_SIL. Ver scrape_bancada_senado.py.
    _bancada = {}
    _bc_path = _os.path.join(
        _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))),
        "eval", "bancada_senado.json")
    try:
        if _os.path.exists(_bc_path):
            _bancada = _json.loads(open(_bc_path, encoding="utf-8").read())
            logger.info(f"  Bancada Senado cargada: {len(_bancada)} senadores")
    except Exception as e:
        logger.warning(f"  No se pudo cargar bancada Senado: {e}")

    # Bancada de diputados desde el SITL (sitl.diputados.gob.mx, su sistema
    # oficial). Iniciativas: Adherente + De Grupo. Ver scrape_bancada_diputados.py.
    _bancada_dip = {}
    _bcd_path = _os.path.join(
        _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))),
        "eval", "bancada_diputados.json")
    try:
        if _os.path.exists(_bcd_path):
            _bancada_dip = _json.loads(open(_bcd_path, encoding="utf-8").read())
            logger.info(f"  Bancada Diputados cargada: {len(_bancada_dip)} diputados")
    except Exception as e:
        logger.warning(f"  No se pudo cargar bancada Diputados: {e}")

    for leg_id in leg_activos:
        # Tasas individuales (default) — ranking de eficiencia personal
        base_ini = _tasa_lxvi(leg_id, TIPOS_INICIATIVA)
        base_prop = _tasa_lxvi(leg_id, TIPOS_PROPOSICION)
        # Tasas TOTALES (incluye colectivas) — para mostrar como dato adicional
        base_ini_total = _tasa_lxvi(leg_id, TIPOS_INICIATIVA, solo_individual=False)
        base_prop_total = _tasa_lxvi(leg_id, TIPOS_PROPOSICION, solo_individual=False)
        rec_ini = _tasa_reciente(leg_id, TIPOS_INICIATIVA, 30)
        rec_prop = _tasa_reciente(leg_id, TIPOS_PROPOSICION, 30)

        def _factor(rec: float, base: float) -> float:
            if base <= 0:
                return 1.0 if rec > 0 else 0.0
            f = rec / base
            return max(0.3, min(3.0, f))

        # Proyección anti-ráfaga: lo ESTRUCTURAL se proyecta con su ritmo
        # reciente (factor como siempre); lo REACTIVO confirmado se proyecta
        # con su promedio de largo plazo (una ráfaga por un evento no es un
        # ritmo nuevo). Sin reactivos, es idéntica a la fórmula anterior.
        def _proy(leg: int, tipos: tuple) -> float:
            base_estr = _tasa_lxvi(leg, tipos, reactivos="sin")
            rec_estr = _tasa_reciente(leg, tipos, 30, reactivos="sin")
            base_react = _tasa_lxvi(leg, tipos, reactivos="solo")
            return (base_estr * _factor(rec_estr, base_estr) + base_react) \
                * VENTANA_PROYECCION_DIAS

        proy_ini = _proy(leg_id, TIPOS_INICIATIVA)
        proy_prop = _proy(leg_id, TIPOS_PROPOSICION)

        if proy_ini == 0 and proy_prop == 0 and base_ini == 0 and base_prop == 0 \
                and base_ini_total == 0 and base_prop_total == 0:
            continue

        # L3P individuales (lo que evalúa al legislador en lo personal)
        prom_l3p_ini = base_ini * DIAS_LXVI
        prom_l3p_prop = base_prop * DIAS_LXVI
        # L3P totales (referencia: ind + colectivas)
        prom_l3p_ini_total = base_ini_total * DIAS_LXVI
        prom_l3p_prop_total = base_prop_total * DIAS_LXVI
        # Colectivas = total - individuales
        prom_l3p_ini_col = max(0.0, prom_l3p_ini_total - prom_l3p_ini)
        prom_l3p_prop_col = max(0.0, prom_l3p_prop_total - prom_l3p_prop)

        # Override con el conteo VERIFICADO del SIL (como promovente). El grupo
        # limpio aún no se jala del SIL → 0 por ahora (fast-follow). Esto evita
        # la re-inflación: el pipeline ya no usa el conteo contaminado.
        _rc = _reconteo.get(str(leg_id))
        if _rc and _rc.get("ini") is not None and _rc.get("prop") is not None:
            prom_l3p_ini = float(_rc["ini"])
            prom_l3p_prop = float(_rc["prop"])
            # Bancada real: senadores ← perfil del Senado; diputados ← SITL.
            # Si no hay dato, queda 0 — no inventamos.
            _bc = _bancada.get(str(leg_id)) or _bancada_dip.get(str(leg_id))
            if _bc and _bc.get("ini_col") is not None:
                prom_l3p_ini_col = float(_bc["ini_col"])
                prom_l3p_prop_col = float(_bc.get("prop_col") or 0)
            else:
                prom_l3p_ini_col = 0.0
                prom_l3p_prop_col = 0.0

        batch_sql.append(
            "INSERT INTO legisladores_stats "
            "(legislador_id, fecha_calculo, iniciativas_proy_15d, "
            "proposiciones_proy_15d, promedio_l3p_iniciativas, "
            "promedio_l3p_proposiciones, "
            "promedio_l3p_iniciativas_col, promedio_l3p_proposiciones_col) VALUES ("
            f"{leg_id}, {_sql_escape(ahora)}, "
            f"{proy_ini:.2f}, {proy_prop:.2f}, "
            f"{prom_l3p_ini:.1f}, {prom_l3p_prop:.1f}, "
            f"{prom_l3p_ini_col:.1f}, {prom_l3p_prop_col:.1f}) "
            "ON CONFLICT(legislador_id) DO UPDATE SET "
            "fecha_calculo=excluded.fecha_calculo, "
            "iniciativas_proy_15d=excluded.iniciativas_proy_15d, "
            "proposiciones_proy_15d=excluded.proposiciones_proy_15d, "
            "promedio_l3p_iniciativas=excluded.promedio_l3p_iniciativas, "
            "promedio_l3p_proposiciones=excluded.promedio_l3p_proposiciones, "
            "promedio_l3p_iniciativas_col=excluded.promedio_l3p_iniciativas_col, "
            "promedio_l3p_proposiciones_col=excluded.promedio_l3p_proposiciones_col;"
        )
        calculados += 1

        if len(batch_sql) >= 200:
            ejecutar_sql_d1("\n".join(batch_sql))
            batch_sql = []

    if batch_sql:
        ejecutar_sql_d1("\n".join(batch_sql))

    logger.info(f"Proyecciones 15d: {calculados} legisladores → D1")
    return {"calculados": calculados}


# ────────────────────────────────────────────
# Paso 5: Conteos por estado (Aprobada/Pendiente/Desechada)
# ────────────────────────────────────────────
def paso_conteos_estado(db_ro: sqlite3.Connection) -> dict:
    """
    Cuenta instrumentos por (tipo, estado, individual/colectivo) por
    legislador en LXVI. Resultado: 12 columnas en legisladores_stats que
    el frontend usará para mostrar desglose claro:

      n_ini_aprob_ind, n_ini_aprob_col      — iniciativas aprobadas
      n_ini_pend_ind,  n_ini_pend_col       — iniciativas pendientes
      n_ini_desech_ind,n_ini_desech_col     — iniciativas desechadas/retiradas
      n_prop_aprob_ind, n_prop_aprob_col    — proposiciones aprobadas
      n_prop_pend_ind,  n_prop_pend_col     — proposiciones pendientes
      n_prop_desech_ind,n_prop_desech_col   — proposiciones desechadas/retiradas

    Individual = co_firmantes IS NULL OR co_firmantes = ''
    Colectivo  = co_firmantes != ''
    """
    logger.info("Cálculo de conteos por estado…")

    # Migration: añadir columnas (idempotente).
    cols_nuevas = [
        "n_ini_aprob_ind", "n_ini_aprob_col",
        "n_ini_pend_ind", "n_ini_pend_col",
        "n_ini_desech_ind", "n_ini_desech_col",
        "n_prop_aprob_ind", "n_prop_aprob_col",
        "n_prop_pend_ind", "n_prop_pend_col",
        "n_prop_desech_ind", "n_prop_desech_col",
    ]
    for col in cols_nuevas:
        try:
            ejecutar_sql_d1(
                f"ALTER TABLE legisladores_stats ADD COLUMN {col} INTEGER DEFAULT 0;"
            )
        except Exception:
            pass  # ya existe

    # Query consolidada: una fila por legislador con los 12 conteos
    rows = db_ro.execute(
        """
        SELECT al.legislador_id,
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%iniciativ%'
                    AND sd.estatus LIKE '%Aprobado%'
                    AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
                   THEN 1 ELSE 0 END) AS n_ini_aprob_ind,
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%iniciativ%'
                    AND sd.estatus LIKE '%Aprobado%'
                    AND al.co_firmantes != ''
                   THEN 1 ELSE 0 END) AS n_ini_aprob_col,
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%iniciativ%'
                    AND sd.estatus LIKE 'Pendiente%'
                    AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
                   THEN 1 ELSE 0 END) AS n_ini_pend_ind,
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%iniciativ%'
                    AND sd.estatus LIKE 'Pendiente%'
                    AND al.co_firmantes != ''
                   THEN 1 ELSE 0 END) AS n_ini_pend_col,
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%iniciativ%'
                    AND (sd.estatus LIKE 'Desechado%' OR sd.estatus LIKE 'Retirad%')
                    AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
                   THEN 1 ELSE 0 END) AS n_ini_desech_ind,
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%iniciativ%'
                    AND (sd.estatus LIKE 'Desechado%' OR sd.estatus LIKE 'Retirad%')
                    AND al.co_firmantes != ''
                   THEN 1 ELSE 0 END) AS n_ini_desech_col,
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%proposici%'
                    AND sd.estatus LIKE '%Aprobado%'
                    AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
                   THEN 1 ELSE 0 END) AS n_prop_aprob_ind,
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%proposici%'
                    AND sd.estatus LIKE '%Aprobado%'
                    AND al.co_firmantes != ''
                   THEN 1 ELSE 0 END) AS n_prop_aprob_col,
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%proposici%'
                    AND sd.estatus LIKE 'Pendiente%'
                    AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
                   THEN 1 ELSE 0 END) AS n_prop_pend_ind,
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%proposici%'
                    AND sd.estatus LIKE 'Pendiente%'
                    AND al.co_firmantes != ''
                   THEN 1 ELSE 0 END) AS n_prop_pend_col,
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%proposici%'
                    AND (sd.estatus LIKE 'Desechado%' OR sd.estatus LIKE 'Retirad%')
                    AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
                   THEN 1 ELSE 0 END) AS n_prop_desech_ind,
          SUM(CASE WHEN LOWER(al.tipo_instrumento) LIKE '%proposici%'
                    AND (sd.estatus LIKE 'Desechado%' OR sd.estatus LIKE 'Retirad%')
                    AND al.co_firmantes != ''
                   THEN 1 ELSE 0 END) AS n_prop_desech_col
        FROM actividad_legislador al
        JOIN sil_documentos sd ON al.sil_documento_id = sd.id
        WHERE al.legislador_id IS NOT NULL
          AND al.fecha_presentacion >= ?
        GROUP BY al.legislador_id
        """,
        (FECHA_INICIO_LXVI,),
    ).fetchall()

    ahora = datetime.utcnow().isoformat()
    batch_sql: list[str] = []
    calculados = 0
    # Override n_*_pend_ind desde el reconteo del SIL (fuente de verdad).
    # actividad_legislador infla pendientes (efemérides + duplicados); el SIL
    # da el pendiente real "como promovente". Alimenta el bloque Historial LXVI.
    import json as _json2, os as _os2
    _rc2 = {}
    _rcp2 = _os2.path.join(_os2.path.dirname(_os2.path.dirname(_os2.path.abspath(__file__))), "eval", "reconteo_sil.json")
    if _os2.path.exists(_rcp2):
        try:
            _rc2 = _json2.loads(open(_rcp2, encoding="utf-8").read())
        except Exception:
            _rc2 = {}

    def _valpend(r, c):
        rc = _rc2.get(str(r["legislador_id"]))
        if rc:
            if c == "n_ini_pend_ind" and rc.get("ini_pen") is not None:
                return rc["ini_pen"]
            if c == "n_prop_pend_ind" and rc.get("prop_pen") is not None:
                return rc["prop_pen"]
        return r[c] or 0

    for r in rows:
        valores = ", ".join(str(_valpend(r, c)) for c in cols_nuevas)
        cols_str = ", ".join(cols_nuevas)
        sets = ", ".join(f"{c}=excluded.{c}" for c in cols_nuevas)
        batch_sql.append(
            f"INSERT INTO legisladores_stats (legislador_id, fecha_calculo, {cols_str}) "
            f"VALUES ({r['legislador_id']}, {_sql_escape(ahora)}, {valores}) "
            f"ON CONFLICT(legislador_id) DO UPDATE SET "
            f"fecha_calculo=excluded.fecha_calculo, {sets};"
        )
        calculados += 1
        if len(batch_sql) >= 200:
            ejecutar_sql_d1("\n".join(batch_sql))
            batch_sql = []
    if batch_sql:
        ejecutar_sql_d1("\n".join(batch_sql))

    logger.info(f"Conteos por estado: {calculados} legisladores → D1")
    return {"calculados": calculados}


# ────────────────────────────────────────────
# Orquestador
# ────────────────────────────────────────────
def main(
    db_path: Path | None = None,
    skip_scraper: bool = False,
    max_refs: int | None = None,
    ref_min: int | None = None,
    workers: int = 4,
    delay_por_worker: float = 1.0,
    flush_cada: int = 100,
    ignorar_skip_cache: bool = False,
) -> int:
    log_file = ROOT / "radar_backfill.log"
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(log_file), mode="a", encoding="utf-8"),
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
    )
    # Forzar flush en cada write del FileHandler
    for h in handlers:
        h.flush = h.stream.flush if hasattr(h, "stream") else h.flush
    db_path = db_path or ROOT / "semaforo.db"

    if not db_path.exists():
        logger.error(f"No se encontró {db_path}")
        return 1

    logger.info(f"Radar pipeline iniciado. Leyendo {db_path} en modo RO")
    db_ro = abrir_db_readonly(db_path)

    try:
        paso_snapshot_legisladores(db_ro)

        if not skip_scraper:
            paso_scraper_sil(
                db_ro,
                max_refs=max_refs,
                ref_min=ref_min,
                workers=workers,
                delay_por_worker=delay_por_worker,
                flush_cada=flush_cada,
                ignorar_skip_cache=ignorar_skip_cache,
            )
        else:
            logger.info("Scraper SIL OMITIDO por --skip-scraper")

        paso_hit_rate(db_ro)
        paso_matchup_grade(db_ro)
        paso_proyecciones(db_ro)
        paso_conteos_estado(db_ro)
    finally:
        db_ro.close()

    logger.info("Radar pipeline terminado")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, default=None)
    ap.add_argument("--skip-scraper", action="store_true")
    ap.add_argument("--max-refs", type=int, default=None)
    ap.add_argument(
        "--ref-min",
        type=int,
        default=None,
        help="Ref mínima para descubrimiento (salta refs anteriores)",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Threads concurrentes para fetch SIL. Default 4.",
    )
    ap.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay por worker (segundos). Default 1.0 → con 4 workers ≈4 req/s globales.",
    )
    ap.add_argument(
        "--flush-cada",
        type=int,
        default=100,
        help="Batch size para flush a D1 vía wrangler. Default 100.",
    )
    ap.add_argument(
        "--ignorar-skip-cache",
        action="store_true",
        help="Ignorar el skip-list local (.radar_skip_refs.json). Útil si cambió el normalizador de nombres.",
    )
    args = ap.parse_args()
    sys.exit(
        main(
            db_path=args.db,
            skip_scraper=args.skip_scraper,
            max_refs=args.max_refs,
            ref_min=args.ref_min,
            workers=args.workers,
            delay_por_worker=args.delay,
            flush_cada=args.flush_cada,
            ignorar_skip_cache=args.ignorar_skip_cache,
        )
    )
