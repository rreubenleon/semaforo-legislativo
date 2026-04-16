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
    """Ejecuta SQL contra D1 vía wrangler. Devuelve metadata."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".sql", delete=False, encoding="utf-8"
    ) as f:
        f.write(sql)
        sql_path = f.name

    try:
        cmd = [
            "npx",
            "wrangler",
            "d1",
            "execute",
            D1_DB,
            "--remote" if remote else "--local",
            "--file",
            sql_path,
            "--json",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.error(f"wrangler stderr: {result.stderr[:800]}")
            logger.error(f"wrangler stdout: {result.stdout[:800]}")
            raise RuntimeError("wrangler d1 execute failed")
        return {"ok": True, "stdout_size": len(result.stdout)}
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
               COALESCE(comisiones_cargo, '') AS comisiones_cargo
        FROM legisladores
        """
    ).fetchall()

    # Limpieza de comisiones_cargo: el scraper de Diputados todavía mete
    # entries espurias ("A LAS QUE PERTENECE", centros, institutos, etc.).
    # Senado ya está limpio (backfill_comisiones_senado.py). Este filtro
    # actúa como safety net para ambas cámaras.
    def _limpia_comisiones(raw: str) -> str:
        if not raw:
            return ''
        partes = raw.split('|')
        ruido = (
            'centro de', 'instituto', 'contralor', 'unidad t',
            'informaci', 'consultor', 'asistencias', 'calendario',
            'memoria de labores', 'histórico de la lxiv', 'historico de la lxiv',
            'órganos t', 'organos t', 'capacitaci', 'ordinarias:',
            'especiales:', 'a las que pertenece',
        )
        buenas = []
        for p in partes:
            p_low = p.lower().strip()
            if not p_low or any(ruido_k in p_low for ruido_k in ruido):
                continue
            buenas.append(p.strip())
        resultado = '|'.join(buenas)
        return resultado[:800]

    rows = [
        {**dict(r), 'comisiones_cargo': _limpia_comisiones(r['comisiones_cargo'])}
        for r in rows
    ]

    logger.info(f"Snapshot de {len(rows)} legisladores → D1")

    # INSERT OR REPLACE para no romper FK de legisladores_perfil.
    # Si un row ya existe, lo actualiza; si no, lo crea. Nunca borra.
    sqls = []
    for r in rows:
        sqls.append(
            "INSERT OR REPLACE INTO legisladores "
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
            ");"
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
# Umbral para considerar un día como "pico mediático" en una categoría.
# Empírico: sobre ~18 meses de `scores` diarios, score_media ≥ 55 cae en
# ~top 15-20% de los días — se alinea con la noción de "pico" del Radar.
# Inicio de la LXVI Legislatura — toda la muestra del Radar se ancla aquí
FECHA_INICIO_LXVI = "2024-09-01"

PICO_SCORE_MEDIA_MIN = 55.0

# Ventanas del hit rate
HITRATE_VENTANA_PICOS = 20   # últimos N picos por categoría (LXVI completa)
HITRATE_VENTANA_DIAS = 14     # días para considerar que el legislador respondió


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
    for row in db_ro.execute(
        """
        SELECT legislador_id, categoria, tipo_instrumento, COUNT(*) as n
        FROM actividad_legislador
        WHERE legislador_id IS NOT NULL
          AND categoria IS NOT NULL AND categoria <> ''
          AND fecha_presentacion >= ?
        GROUP BY legislador_id, categoria, tipo_instrumento
        """,
        (FECHA_INICIO_LXVI,),
    ):
        leg_id = row["legislador_id"]
        cat = row["categoria"]
        tipo_norm = (row["tipo_instrumento"] or "").lower()
        peso = 2.0 if "iniciativa" in tipo_norm else 1.0
        n = int(row["n"] or 0)
        leg_bucket = raw_counts.setdefault(leg_id, {})
        cat_bucket = leg_bucket.setdefault(cat, {"count": 0.0, "score": 0.0})
        cat_bucket["count"] += n
        cat_bucket["score"] += n * peso

    for leg_id, cats in raw_counts.items():
        candidates = {c: v for c, v in cats.items() if v["count"] >= 3}
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

    # 2) Picos por categoría desde `scores` (fuente de verdad, sin sesgo).
    #    Ventana amplia para tener historia suficiente, luego cada legislador
    #    usa sus últimos N picos.
    picos_por_cat: dict[str, list[str]] = {}  # {cat: [fecha_pico, ...] desc}
    for row in db_ro.execute(
        """
        SELECT categoria, fecha
        FROM scores
        WHERE score_media >= ?
          AND fecha >= ?
        ORDER BY categoria, fecha DESC
        """,
        (PICO_SCORE_MEDIA_MIN, FECHA_INICIO_LXVI),
    ):
        picos_por_cat.setdefault(row["categoria"], []).append(row["fecha"])

    logger.info(
        f"  Picos (score_media≥{PICO_SCORE_MEDIA_MIN}) en "
        f"{len(picos_por_cat)} categorías · total={sum(len(v) for v in picos_por_cat.values())}"
    )

    # 3) Pre-cargar set de (legislador_id, categoria, fecha) de actividad
    #    para lookup O(1) sin hacer 10 queries por legislador.
    actividad_set: set[tuple[int, str, str]] = set()
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
        for pico_str in picos:
            try:
                d0 = datetime.strptime(pico_str, "%Y-%m-%d").date()
            except Exception:
                continue
            # Ventana bidireccional [d0 - ventana/2, d0 + ventana/2].
            # El legislador a veces actúa ANTES del pico (puede causarlo).
            reacciono = False
            mitad = HITRATE_VENTANA_DIAS // 2
            for delta in range(-mitad, HITRATE_VENTANA_DIAS - mitad + 1):
                d = (d0 + timedelta(days=delta)).isoformat()
                if (leg_id, categoria, d) in actividad_set:
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
# Paso 3: Matchup grade (stub, Fase 2 posterior)
# ────────────────────────────────────────────
MATCHUP_MIN_DECIDIDOS = 5  # mínimo de dictámenes para asignar grade


def paso_matchup_grade(db_ro: sqlite3.Connection) -> dict:
    """
    Matchup grade A–F por legislador frente a su comisión dictaminadora
    más frecuente. Cruza `actividad_legislador` con `sil_documentos` para
    calcular la tasa de aprobación personal en esa comisión.

    Fórmula:
      tasa_aprobacion = aprobados / (aprobados + desechados + retiradas)
      grade = A/B/C/D/F según percentiles absolutos:
        A: ≥ 85%
        B: 70–85%
        C: 55–70%
        D: 40–55%
        F: < 40%
      Pendientes se excluyen del denominador.
      Mínimo 5 decididos, si no → NULL.

    Escribe matchup_grade / matchup_comision_target / matchup_tasa_dictamen
    en legisladores_stats.
    """
    logger.info("Cálculo de matchup grade por legislador…")

    # Por cada legislador, encontrar comisión de turno dominante y
    # tasa de aprobación ahí. Hacemos esto en SQL con JOIN para no
    # iterar N veces.
    rows = db_ro.execute(
        """
        SELECT al.legislador_id,
               al.comision_turno,
               SUM(CASE WHEN sd.estatus LIKE '%Aprobado%' THEN 1 ELSE 0 END) AS aprobados,
               SUM(CASE WHEN sd.estatus LIKE 'Desechado%' OR sd.estatus LIKE 'Retirada%' THEN 1 ELSE 0 END) AS rechazados,
               COUNT(*) AS total
        FROM actividad_legislador al
        JOIN sil_documentos sd ON al.sil_documento_id = sd.id
        WHERE al.legislador_id IS NOT NULL
          AND al.comision_turno IS NOT NULL
          AND al.comision_turno <> ''
          AND al.fecha_presentacion >= ?
        GROUP BY al.legislador_id, al.comision_turno
        HAVING total >= 3
        """,
        (FECHA_INICIO_LXVI,),
    ).fetchall()

    # Para cada legislador, tomar la comisión con más documentos
    por_leg: dict[int, dict] = {}
    for r in rows:
        leg_id = r["legislador_id"]
        tot = r["total"]
        prev = por_leg.get(leg_id)
        if prev is None or tot > prev["total"]:
            por_leg[leg_id] = dict(r)

    logger.info(f"  Legisladores con comisión dominante (≥3 docs): {len(por_leg)}")

    def _grade(tasa: float) -> str:
        if tasa >= 0.85: return "A"
        if tasa >= 0.70: return "B"
        if tasa >= 0.55: return "C"
        if tasa >= 0.40: return "D"
        return "F"

    ahora = datetime.utcnow().isoformat()
    stats_sqls: list[str] = []
    distro = {"A": 0, "B": 0, "C": 0, "D": 0, "F": 0, "NULL": 0}
    calculados = 0

    for leg_id, r in por_leg.items():
        aprobados = r["aprobados"] or 0
        rechazados = r["rechazados"] or 0
        decididos = aprobados + rechazados
        comision = r["comision_turno"]

        if decididos >= MATCHUP_MIN_DECIDIDOS:
            tasa = aprobados / decididos
            grade = _grade(tasa)
            tasa_val = f"{tasa:.4f}"
        else:
            tasa = None
            grade = None
            tasa_val = "NULL"

        distro[grade if grade else "NULL"] += 1

        # Upsert parcial — no pisamos categoria_dominante / prob / proy si ya están.
        stats_sqls.append(
            "INSERT INTO legisladores_stats "
            "(legislador_id, fecha_calculo, matchup_grade, "
            "matchup_comision_target, matchup_tasa_dictamen) VALUES ("
            f"{leg_id}, {_sql_escape(ahora)}, "
            f"{_sql_escape(grade) if grade else 'NULL'}, "
            f"{_sql_escape(comision)}, {tasa_val}) "
            "ON CONFLICT(legislador_id) DO UPDATE SET "
            "fecha_calculo=excluded.fecha_calculo, "
            "matchup_grade=excluded.matchup_grade, "
            "matchup_comision_target=excluded.matchup_comision_target, "
            "matchup_tasa_dictamen=excluded.matchup_tasa_dictamen;"
        )
        calculados += 1

    for i in range(0, len(stats_sqls), 200):
        ejecutar_sql_d1("\n".join(stats_sqls[i:i + 200]))

    logger.info(f"Matchup grade: {calculados} legisladores → D1. Distribución: {distro}")
    return {"calculados": calculados, "distribucion": distro}


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

    def _tasa_lxvi(leg_id: int, tipos: tuple) -> float:
        """Actividad del tipo desde inicio LXVI / días_lxvi."""
        like_clause = " OR ".join(["LOWER(tipo_instrumento) LIKE ?"] * len(tipos))
        row = db_ro.execute(
            f"""
            SELECT COUNT(*) as n FROM actividad_legislador
            WHERE legislador_id = ?
              AND ({like_clause})
              AND fecha_presentacion >= ?
            """,
            (leg_id, *tipos, FECHA_INICIO_LXVI),
        ).fetchone()
        return (row["n"] or 0) / DIAS_LXVI

    def _tasa_reciente(leg_id: int, tipos: tuple, dias: int) -> float:
        like_clause = " OR ".join(["LOWER(tipo_instrumento) LIKE ?"] * len(tipos))
        row = db_ro.execute(
            f"""
            SELECT COUNT(*) as n FROM actividad_legislador
            WHERE legislador_id = ?
              AND ({like_clause})
              AND fecha_presentacion >= date('now', '-{dias} days')
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

    for leg_id in leg_activos:
        base_ini = _tasa_lxvi(leg_id, TIPOS_INICIATIVA)
        base_prop = _tasa_lxvi(leg_id, TIPOS_PROPOSICION)
        rec_ini = _tasa_reciente(leg_id, TIPOS_INICIATIVA, 30)
        rec_prop = _tasa_reciente(leg_id, TIPOS_PROPOSICION, 30)

        def _factor(rec: float, base: float) -> float:
            if base <= 0:
                return 1.0 if rec > 0 else 0.0
            f = rec / base
            return max(0.3, min(3.0, f))

        proy_ini = base_ini * _factor(rec_ini, base_ini) * VENTANA_PROYECCION_DIAS
        proy_prop = base_prop * _factor(rec_prop, base_prop) * VENTANA_PROYECCION_DIAS

        if proy_ini == 0 and proy_prop == 0 and base_ini == 0 and base_prop == 0:
            continue

        # L3P absoluto: total de actividades durante la LXVI
        prom_l3p_ini = base_ini * DIAS_LXVI  # = count
        prom_l3p_prop = base_prop * DIAS_LXVI

        batch_sql.append(
            "INSERT INTO legisladores_stats "
            "(legislador_id, fecha_calculo, iniciativas_proy_15d, "
            "proposiciones_proy_15d, promedio_l3p_iniciativas, "
            "promedio_l3p_proposiciones) VALUES ("
            f"{leg_id}, {_sql_escape(ahora)}, "
            f"{proy_ini:.2f}, {proy_prop:.2f}, "
            f"{prom_l3p_ini:.1f}, {prom_l3p_prop:.1f}) "
            "ON CONFLICT(legislador_id) DO UPDATE SET "
            "fecha_calculo=excluded.fecha_calculo, "
            "iniciativas_proy_15d=excluded.iniciativas_proy_15d, "
            "proposiciones_proy_15d=excluded.proposiciones_proy_15d, "
            "promedio_l3p_iniciativas=excluded.promedio_l3p_iniciativas, "
            "promedio_l3p_proposiciones=excluded.promedio_l3p_proposiciones;"
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
