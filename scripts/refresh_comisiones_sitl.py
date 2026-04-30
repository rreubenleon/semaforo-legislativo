"""
Pipeline de Comisiones → D1 (separado del pipeline principal).

Maneja DOS tipos de datos con cadencias distintas:

  1. Datos de Gaceta (composición, dictamen, histórico, score)
     Fuente: SQLite local (semaforo.db)
     Cadencia: 1x por semana (miércoles)
     Comando: --paso gaceta

  2. Datos de SITL Diputados (turnadas, aprobadas, pendientes, tasa)
     Fuente: sitl.diputados.gob.mx (scraping externo)
     Cadencia: escalonado L/M
     Comando: --paso sitl --batch lunes|martes

Todo se escribe a la tabla `comisiones_stats` en Cloudflare D1.
UNIQUE(nombre, camara) → siempre UPSERT, nunca crece.
Tamaño máximo estimado: ~150 KB (87 comisiones × ~1.7 KB c/u).

Uso:
    # Paso 1: Datos de Gaceta → D1 (miércoles)
    python scripts/refresh_comisiones_sitl.py --paso gaceta

    # Paso 2: SITL batch lunes (IDs 5-29)
    python scripts/refresh_comisiones_sitl.py --paso sitl --batch lunes

    # Paso 2: SITL batch martes (IDs 30-57)
    python scripts/refresh_comisiones_sitl.py --paso sitl --batch martes

    # Backfill inicial: todo de una vez
    python scripts/refresh_comisiones_sitl.py --paso todo

    # Dry-run
    python scripts/refresh_comisiones_sitl.py --paso gaceta --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logger = logging.getLogger("refresh_comisiones")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

D1_DB = "fiat-busqueda"
WORKER_DIR = str(ROOT / "worker")
DB_PATH = ROOT / "semaforo.db"

# SITL batches
BATCH_LUNES = list(range(5, 30))     # 25 comisiones
BATCH_MARTES = list(range(30, 58))   # 28 comisiones
BATCH_TODAS = list(range(5, 58))     # 53 comisiones


# ────────────────────────────────────────────
# Utilidades D1
# ────────────────────────────────────────────
def _sql_escape(s):
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def ejecutar_sql_d1(sql, remote=True):
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".sql", delete=False, encoding="utf-8"
    ) as f:
        f.write(sql)
        sql_path = f.name
    try:
        cmd = [
            "npx", "wrangler", "d1", "execute", D1_DB,
            "--remote" if remote else "--local",
            "--file", sql_path,
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, cwd=WORKER_DIR, timeout=120
        )
        if result.returncode != 0:
            logger.error(f"wrangler error: {result.stderr[:500]}")
            raise RuntimeError(result.stderr[:500])
        return result.stdout
    finally:
        os.unlink(sql_path)


# ────────────────────────────────────────────
# Paso GACETA: Lee SQLite local → D1
# ────────────────────────────────────────────
def _invertir_nombre(nombre):
    """
    Invierte un nombre de formato "Apellido1 Apellido2 Nombre(s)" (SITL)
    al formato natural "Nombre(s) Apellido1 Apellido2".

    Heurística para nombres mexicanos almacenados como apellidos-primero:
      - 1-2 palabras: no se puede invertir con certeza, se deja igual.
      - 3 palabras:  último es nombre → "Nombre Ap1 Ap2"
      - 4+ palabras: últimos 2 son nombre(s) → "Nombre1 Nombre2 Ap1 Ap2"

    Ejemplos:
      "Chedraui Peralta Alejandra"         → "Alejandra Chedraui Peralta"
      "González Hernández Juan Antonio"    → "Juan Antonio González Hernández"
      "Lixa Abimerhi José Elías"           → "José Elías Lixa Abimerhi"
      "García López María de los Ángeles"  → "María de los Ángeles García López"
        (4+ partes → últimas 2 no es perfecto aquí, pero aceptable)
    """
    if not nombre:
        return nombre
    partes = nombre.strip().split()
    if len(partes) <= 2:
        return nombre  # No se puede invertir con certeza
    if len(partes) == 3:
        # Último es nombre: "Ap1 Ap2 Nombre"
        return f"{partes[2]} {partes[0]} {partes[1]}"
    # 4+ partes: últimos 2 son nombre(s): "Ap1 Ap2 Nombre1 Nombre2"
    apellidos = " ".join(partes[:2])
    nombres = " ".join(partes[2:])
    return f"{nombres} {apellidos}"


def _normalizar_cargo(cargo_raw):
    import unicodedata
    cargo = unicodedata.normalize("NFKD", cargo_raw).strip()
    cargo = cargo.replace("\xad", "").replace("\u00ad", "")
    cl = cargo.lower()
    if "president" in cl:
        return "Presidente"
    if "secretar" in cl:
        return "Secretario"
    return "Integrante"


def paso_gaceta(dry_run=False):
    """Lee datos de Gaceta + legisladores desde SQLite → D1."""
    from config import (
        comision_a_categoria, CATEGORIAS, normalizar_comision_senado,
        COMISIONES_SENADO,
    )

    if not DB_PATH.exists():
        logger.error(f"DB no encontrada: {DB_PATH}")
        return {"ok": 0, "errores": 1}

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    ahora = datetime.utcnow().isoformat()

    # ── 1. Actividad por comisión (LXVI completa) ──
    logger.info("Leyendo actividad de Gaceta…")
    rows = conn.execute("""
        SELECT comision, camara, tipo, COUNT(*) as total,
               MAX(fecha) as ultima_fecha
        FROM gaceta
        WHERE comision IS NOT NULL AND comision != '' AND comision != 'No especificada'
          AND fecha >= '2024-09-01'
        GROUP BY comision, camara, tipo
    """).fetchall()

    comisiones = {}
    for r in rows:
        nombre_raw = r["comision"]
        camara = r["camara"] or "Diputados"
        if camara == "Senado":
            nombre = normalizar_comision_senado(nombre_raw)
            if not nombre:
                continue
        else:
            nombre = nombre_raw
        key = f"{nombre}|{camara}"
        if key not in comisiones:
            cat = comision_a_categoria(nombre)
            comisiones[key] = {
                "nombre": nombre, "camara": camara,
                "categoria": cat,
                "categoria_nombre": CATEGORIAS[cat]["nombre"] if cat and cat in CATEGORIAS else None,
                "tipos": {}, "total_docs": 0, "ultima_actividad": "",
            }
        c = comisiones[key]
        tipo = r["tipo"] or "otro"
        c["tipos"][tipo] = c["tipos"].get(tipo, 0) + r["total"]
        c["total_docs"] += r["total"]
        if r["ultima_fecha"] and r["ultima_fecha"] > c["ultima_actividad"]:
            c["ultima_actividad"] = r["ultima_fecha"]

    # ── Completar con TODAS las comisiones oficiales del Senado, incluso las
    # que no tienen actividad en Gaceta (espíritu de transparencia: FIAT
    # muestra TODO lo que existe, no sólo las que trabajan). Las inactivas
    # entran con counts=0, total_docs=0, score_actividad=0.
    for nombre_oficial in COMISIONES_SENADO:
        key = f"{nombre_oficial}|Senado"
        if key in comisiones:
            continue
        cat = comision_a_categoria(nombre_oficial)
        comisiones[key] = {
            "nombre": nombre_oficial, "camara": "Senado",
            "categoria": cat,
            "categoria_nombre": CATEGORIAS[cat]["nombre"] if cat and cat in CATEGORIAS else None,
            "tipos": {}, "total_docs": 0, "ultima_actividad": "",
        }

    # Score
    resultado = list(comisiones.values())
    if resultado:
        max_docs = max(c["total_docs"] for c in resultado)
        for c in resultado:
            sv = (c["total_docs"] / max_docs * 60) if max_docs > 0 else 0
            bonus = 0
            if "dictamen" in c["tipos"]:
                bonus += 20
            if "iniciativa" in c["tipos"]:
                bonus += 10
            if "proposicion" in c["tipos"]:
                bonus += 10
            c["score_actividad"] = min(round(sv + bonus, 1), 100)

    logger.info(f"  {len(resultado)} comisiones con actividad en Gaceta")

    # ── 2. Composición ──
    logger.info("Leyendo composición de legisladores…")
    legs = conn.execute("""
        SELECT nombre, partido, camara, comisiones_cargo
        FROM legisladores
        WHERE comisiones_cargo IS NOT NULL AND comisiones_cargo != ''
    """).fetchall()

    composicion_map = {}
    for r in legs:
        camara_raw = r["camara"] or ""
        camara = "Senado" if "senad" in camara_raw.lower() else "Diputados"
        for entry in (r["comisiones_cargo"] or "").split("|"):
            entry = entry.strip()
            if not entry or "A LAS QUE PERTENECE" in entry.upper():
                continue
            parts = entry.rsplit(":", 1)
            com_name = parts[0].strip() if len(parts) == 2 else entry
            cargo_raw = parts[1].strip() if len(parts) == 2 else "Integrante"
            cargo = _normalizar_cargo(cargo_raw)

            if camara == "Senado":
                com_norm = normalizar_comision_senado(com_name)
                if not com_norm:
                    continue
                com_name = com_norm

            # Key para match con gaceta (Diputados tiene "Comisión de" prefix)
            keys_to_add = [f"{com_name}|{camara}"]
            if camara == "Diputados":
                keys_to_add.append(f"Comisión de {com_name}|{camara}")

            # Solo Diputados tiene nombres invertidos ("Apellido Apellido Nombre")
            # Senado ya guarda en orden natural ("Nombre Apellido Apellido")
            nombre_mostrar = _invertir_nombre(r["nombre"]) if camara == "Diputados" else r["nombre"]
            miembro = {"nombre": nombre_mostrar, "cargo": cargo, "partido": r["partido"] or "?"}
            for k in keys_to_add:
                composicion_map.setdefault(k, []).append(miembro)

    # Ordenar
    cargo_orden = {"Presidente": 0, "Secretario": 1, "Integrante": 2}
    for k in composicion_map:
        composicion_map[k].sort(key=lambda m: (cargo_orden.get(m["cargo"], 9), m["nombre"]))

    # ── 3. Último dictamen (fecha + URL) ──
    # Query ordenada DESC para que la primera fila por (comision_norm, camara)
    # sea el dictamen más reciente, incluyendo su URL para linkear en UI.
    logger.info("Leyendo último dictamen…")
    dict_rows = conn.execute("""
        SELECT comision, camara, fecha, url, url_pdf
        FROM gaceta
        WHERE tipo='dictamen' AND comision IS NOT NULL AND comision != ''
        ORDER BY fecha DESC, id DESC
    """).fetchall()
    ultimo_dict = {}       # key -> fecha
    ultimo_dict_url = {}   # key -> url
    for r in dict_rows:
        camara = r["camara"] or "Diputados"
        nombre = r["comision"]
        if camara == "Senado":
            nombre = normalizar_comision_senado(nombre)
            if not nombre:
                continue
        key = f"{nombre}|{camara}"
        fecha = r["fecha"]
        if key not in ultimo_dict or (fecha and fecha > ultimo_dict[key]):
            ultimo_dict[key] = fecha
            ultimo_dict_url[key] = r["url_pdf"] or r["url"] or ""

    # ── 4. Histórico mensual ──
    # Nota: el GROUP BY SQL agrupa por nombre RAW de comisión. Para Senado,
    # múltiples variantes del nombre colapsan al mismo nombre canónico tras
    # normalizar, así que hay que re-agregar por (key, mes) sumando counts
    # en Python. De lo contrario el histórico muestra el mismo mes repetido
    # con counts distintos y el frontend renderiza vacío/rotos.
    logger.info("Leyendo histórico mensual…")
    hist_rows = conn.execute("""
        SELECT comision, camara, strftime('%Y-%m', fecha) as mes, COUNT(*) as n
        FROM gaceta
        WHERE fecha >= '2024-09-01' AND comision IS NOT NULL AND comision != ''
        GROUP BY comision, camara, mes ORDER BY comision, mes
    """).fetchall()
    historico_agg = {}  # key -> {mes: count}
    for r in hist_rows:
        camara = r["camara"] or "Diputados"
        nombre = r["comision"]
        if camara == "Senado":
            nombre = normalizar_comision_senado(nombre)
            if not nombre:
                continue
        key = f"{nombre}|{camara}"
        mes = r["mes"]
        bucket = historico_agg.setdefault(key, {})
        bucket[mes] = bucket.get(mes, 0) + r["n"]

    # Serializar como lista ordenada ascendente por mes
    historico_map = {
        key: [{"mes": m, "count": c} for m, c in sorted(bucket.items())]
        for key, bucket in historico_agg.items()
    }

    conn.close()

    # ── Enriquecer resultado ──
    hoy_str = datetime.now().strftime("%Y-%m-%d")
    for c in resultado:
        key = f"{c['nombre']}|{c['camara']}"
        # Composición
        comp = composicion_map.get(key, [])
        c["composicion"] = comp[:30]
        c["total_integrantes"] = len(comp)
        # Último dictamen (+ URL para linkear en UI)
        fd = ultimo_dict.get(key)
        c["ultimo_dictamen"] = fd
        c["ultimo_dictamen_url"] = ultimo_dict_url.get(key) or None
        if fd:
            try:
                c["dias_sin_dictamen"] = (datetime.strptime(hoy_str, "%Y-%m-%d") - datetime.strptime(fd, "%Y-%m-%d")).days
            except Exception:
                c["dias_sin_dictamen"] = None
        else:
            c["dias_sin_dictamen"] = None
        # Histórico
        c["historico_mensual"] = historico_map.get(key, [])

    logger.info(f"  Con composición: {sum(1 for c in resultado if c['total_integrantes'] > 0)}")
    logger.info(f"  Con último dictamen: {sum(1 for c in resultado if c['ultimo_dictamen'])}")

    if dry_run:
        logger.info("[DRY-RUN] No se escribe a D1")
        for c in sorted(resultado, key=lambda x: x["score_actividad"], reverse=True)[:5]:
            logger.info(f"  {c['nombre'][:35]} ({c['camara'][:3]}) score={c['score_actividad']} int={c['total_integrantes']}")
        return {"ok": len(resultado), "errores": 0}

    # ── Escribir a D1 ──
    sqls = []
    for c in resultado:
        comp_json = json.dumps(c["composicion"], ensure_ascii=False) if c["composicion"] else None
        hist_json = json.dumps(c["historico_mensual"], ensure_ascii=False) if c["historico_mensual"] else None
        sqls.append(
            f"INSERT INTO comisiones_stats "
            f"(nombre, camara, categoria, categoria_nombre, "
            f"docs_dictamen, docs_iniciativa, docs_proposicion, docs_comunicacion, docs_otro, "
            f"total_docs, score_actividad, ultima_actividad, "
            f"ultimo_dictamen, ultimo_dictamen_url, dias_sin_dictamen, "
            f"composicion, total_integrantes, historico_mensual, actualizado_gaceta) "
            f"VALUES ("
            f"{_sql_escape(c['nombre'])}, {_sql_escape(c['camara'])}, "
            f"{_sql_escape(c.get('categoria'))}, {_sql_escape(c.get('categoria_nombre'))}, "
            f"{c['tipos'].get('dictamen', 0)}, {c['tipos'].get('iniciativa', 0)}, "
            f"{c['tipos'].get('proposicion', 0)}, {c['tipos'].get('comunicacion', 0)}, "
            f"{sum(v for k, v in c['tipos'].items() if k not in ('dictamen','iniciativa','proposicion','comunicacion'))}, "
            f"{c['total_docs']}, {c['score_actividad']}, {_sql_escape(c['ultima_actividad'])}, "
            f"{_sql_escape(c['ultimo_dictamen'])}, {_sql_escape(c.get('ultimo_dictamen_url'))}, "
            f"{c['dias_sin_dictamen'] if c['dias_sin_dictamen'] is not None else 'NULL'}, "
            f"{_sql_escape(comp_json)}, {c['total_integrantes']}, "
            f"{_sql_escape(hist_json)}, {_sql_escape(ahora)}) "
            f"ON CONFLICT(nombre, camara) DO UPDATE SET "
            f"categoria=excluded.categoria, categoria_nombre=excluded.categoria_nombre, "
            f"docs_dictamen=excluded.docs_dictamen, docs_iniciativa=excluded.docs_iniciativa, "
            f"docs_proposicion=excluded.docs_proposicion, docs_comunicacion=excluded.docs_comunicacion, "
            f"docs_otro=excluded.docs_otro, total_docs=excluded.total_docs, "
            f"score_actividad=excluded.score_actividad, ultima_actividad=excluded.ultima_actividad, "
            f"ultimo_dictamen=excluded.ultimo_dictamen, ultimo_dictamen_url=excluded.ultimo_dictamen_url, "
            f"dias_sin_dictamen=excluded.dias_sin_dictamen, "
            f"composicion=excluded.composicion, total_integrantes=excluded.total_integrantes, "
            f"historico_mensual=excluded.historico_mensual, actualizado_gaceta=excluded.actualizado_gaceta;"
        )

    # Batches de 20 (D1 tiene límite de tamaño por request)
    batch_size = 20
    total_written = 0
    for i in range(0, len(sqls), batch_size):
        batch = sqls[i:i + batch_size]
        ejecutar_sql_d1("\n".join(batch))
        total_written += len(batch)
        logger.info(f"  Batch {i // batch_size + 1}: {len(batch)} filas a D1")

    logger.info(f"Gaceta → D1: {total_written} comisiones escritas")
    return {"ok": total_written, "errores": 0}


# ────────────────────────────────────────────
# Paso SITL: Scrape externo → D1
# ────────────────────────────────────────────
def paso_sitl(batch_ids, dry_run=False, delay=1.5):
    """Scrapea SITL Diputados y actualiza columnas sitl_* en D1."""
    from scrapers.comisiones_sitl import _scrape_comision

    resultados = []
    errores = 0
    ahora = datetime.utcnow().isoformat()

    logger.info(f"Scraping SITL: {len(batch_ids)} comisiones (IDs {batch_ids[0]}-{batch_ids[-1]})…")

    for i, comt_id in enumerate(batch_ids, 1):
        data = _scrape_comision(comt_id)
        if data:
            resultados.append(data)
            if i <= 3 or i % 10 == 0:
                logger.info(
                    f"  [{i:>2}/{len(batch_ids)}] {data['nombre'][:40]}: "
                    f"{data['turnadas']}T / {data['aprobadas']}A / {data['pendientes']}P"
                )
        else:
            errores += 1
        if i < len(batch_ids):
            time.sleep(delay)

    logger.info(f"SITL scraping: {len(resultados)} OK, {errores} errores")

    if dry_run:
        logger.info("[DRY-RUN]")
        for d in resultados[:5]:
            logger.info(f"  {d['nombre'][:40]}: {d['turnadas']}T → {d['tasa_aprobacion']}%")
        return {"ok": len(resultados), "errores": errores}

    if not resultados:
        return {"ok": 0, "errores": errores}

    # UPDATE solo las columnas SITL (no tocar gaceta/composición)
    # Gaceta usa "Comisión de X", SITL usa "X" → intentar ambos nombres
    sitl_set_clause = (
        "sitl_turnadas={turnadas}, sitl_aprobadas={aprobadas}, "
        "sitl_desechadas={desechadas}, sitl_pendientes={pendientes}, "
        "sitl_retiradas={retiradas}, "
        "sitl_tasa_aprobacion={tasa_aprobacion}, "
        "sitl_tasa_resolucion={tasa_resolucion}, "
        "sitl_ini_turnadas={ini_turnadas}, "
        "sitl_ini_aprobadas={ini_aprobadas}, "
        "sitl_ini_pendientes={ini_pendientes}, "
        "sitl_prop_turnadas={prop_turnadas}, "
        "sitl_prop_aprobadas={prop_aprobadas}, "
        "sitl_prop_pendientes={prop_pendientes}, "
        "sitl_desglose={desglose}, "
        "comt_id={comt_id}, actualizado_sitl={ahora}"
    )
    sqls = []
    for d in resultados:
        vals = {
            "turnadas": d['turnadas'], "aprobadas": d['aprobadas'],
            "desechadas": d['desechadas'], "pendientes": d['pendientes'],
            "retiradas": d['retiradas'],
            "tasa_aprobacion": d['tasa_aprobacion'],
            "tasa_resolucion": d['tasa_resolucion'],
            "ini_turnadas": d['iniciativas_turnadas'],
            "ini_aprobadas": d['iniciativas_aprobadas'],
            "ini_pendientes": d.get('iniciativas_pendientes', 0),
            "prop_turnadas": d['proposiciones_turnadas'],
            "prop_aprobadas": d.get('proposiciones_aprobadas', 0),
            "prop_pendientes": d.get('proposiciones_pendientes', 0),
            "comt_id": d['comt_id'], "ahora": _sql_escape(ahora),
            "desglose": _sql_escape(json.dumps(d.get('desglose_turno', {}), ensure_ascii=False)),
        }
        set_clause = sitl_set_clause.format(**vals)
        nombre_corto = d['nombre']
        nombre_largo = f"Comisión de {nombre_corto}"

        # UPSERT con nombre Gaceta ("Comisión de X") — INSERT crea la fila
        # si no existe, ON CONFLICT actualiza si ya la creó paso_gaceta
        desglose_json = _sql_escape(json.dumps(d.get('desglose_turno', {}), ensure_ascii=False))
        sitl_cols = "sitl_turnadas, sitl_aprobadas, sitl_desechadas, sitl_pendientes, sitl_retiradas, sitl_tasa_aprobacion, sitl_tasa_resolucion, sitl_ini_turnadas, sitl_ini_aprobadas, sitl_ini_pendientes, sitl_prop_turnadas, sitl_prop_aprobadas, sitl_prop_pendientes, sitl_desglose"
        sqls.append(
            f"INSERT INTO comisiones_stats (nombre, camara, comt_id, {sitl_cols}, actualizado_sitl) "
            f"VALUES ({_sql_escape(nombre_largo)}, 'Diputados', {d['comt_id']}, "
            f"{d['turnadas']}, {d['aprobadas']}, {d['desechadas']}, {d['pendientes']}, {d['retiradas']}, "
            f"{d['tasa_aprobacion']}, {d['tasa_resolucion']}, "
            f"{d['iniciativas_turnadas']}, {d['iniciativas_aprobadas']}, {d.get('iniciativas_pendientes', 0)}, "
            f"{d['proposiciones_turnadas']}, {d.get('proposiciones_aprobadas', 0)}, {d.get('proposiciones_pendientes', 0)}, "
            f"{desglose_json}, "
            f"{_sql_escape(ahora)}) "
            f"ON CONFLICT(nombre, camara) DO UPDATE SET {set_clause};"
        )

    # Execute in batches
    batch_size = 40
    for i in range(0, len(sqls), batch_size):
        batch = sqls[i:i + batch_size]
        ejecutar_sql_d1("\n".join(batch))
        logger.info(f"  SITL batch {i // batch_size + 1}: {len(batch)} statements")

    logger.info(f"SITL → D1: {len(resultados)} comisiones actualizadas")
    return {"ok": len(resultados), "errores": errores}


# ────────────────────────────────────────────
# Main
# ────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Pipeline Comisiones → D1")
    ap.add_argument(
        "--paso", required=True,
        choices=["gaceta", "sitl", "todo"],
        help="gaceta=SQLite→D1, sitl=SITL scrape→D1, todo=ambos"
    )
    ap.add_argument(
        "--batch", choices=["lunes", "martes", "todas"],
        help="Batch SITL: lunes (IDs 5-29), martes (IDs 30-57), todas (backfill)"
    )
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--delay", type=float, default=1.5)
    ap.add_argument("--ignorar-modo-receso", action="store_true",
                    help="Forzar ejecución incluso en receso fuera de primera semana del mes")
    args = ap.parse_args()

    # Guard de modo receso: durante mayo-agosto (y dic-enero), las comisiones
    # ordinarias casi no sesionan. Bajamos a 1x/mes (primer lunes/martes).
    if not args.ignorar_modo_receso:
        try:
            from modo_receso import debe_correr_comisiones
            correr, motivo = debe_correr_comisiones()
            if not correr:
                logger.info(f"SKIP comisiones: {motivo}")
                return 0
            logger.info(f"OK correr comisiones: {motivo}")
        except ImportError:
            logger.warning("modo_receso.py no encontrado, corriendo sin guard")

    if args.paso == "gaceta":
        result = paso_gaceta(dry_run=args.dry_run)
    elif args.paso == "sitl":
        if not args.batch:
            logger.error("--batch requerido para paso sitl")
            return 1
        batch_map = {"lunes": BATCH_LUNES, "martes": BATCH_MARTES, "todas": BATCH_TODAS}
        result = paso_sitl(batch_map[args.batch], dry_run=args.dry_run, delay=args.delay)
    elif args.paso == "todo":
        r1 = paso_gaceta(dry_run=args.dry_run)
        r2 = paso_sitl(BATCH_TODAS, dry_run=args.dry_run, delay=args.delay)
        result = {"ok": r1["ok"] + r2["ok"], "errores": r1["errores"] + r2["errores"]}

    logger.info(f"Resultado final: {result}")
    return 0 if result["errores"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
