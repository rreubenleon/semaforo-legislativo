"""
Head-to-head: legislador × comisión.

Para cada legislador, computa stats de su track record por comisión
dictaminadora. Convierte el matchup grade (A/B/C/D/F) en una tasa cruda
interpretable: cuántos instrumentos turnó a cada comisión, cuántos
prosperaron, cuál es el baseline de esa comisión, qué diferencial logra.

Output: tabla legisladores_h2h en SQLite local, lista para sync a D1.

Estructura:
  legislador_id × comision → stats agregados + 5 instrumentos más recientes

Uso:
    python scripts/calcular_h2h_legisladores.py
    python scripts/calcular_h2h_legisladores.py --top 5
"""
import argparse
import json
import logging
import re
import sqlite3
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
sys.path.insert(0, str(ROOT))

# Reusar matchers fuzzy del ELO
sys.path.insert(0, str(Path(__file__).parent))
from calcular_elo_legisladores import (
    _construir_indice_legisladores,
    _matchear_legislador,
    extraer_legislador,
    clasificar_estatus,
    DIAS_MIN_PENDIENTE,
    DIAS_MAX_PENDIENTE,
    HOY,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

# Mínimos para que un h2h sea reportable
MIN_INSTRUMENTOS_LEG_COMISION = 3   # ≥3 instrumentos del legislador en la comisión
MIN_BASELINE_COMISION = 10           # ≥10 instrumentos totales en la comisión para tasa baseline


def parsear_estatus(estatus_raw):
    """'DesechadoDesechado30/01/2026' → ('Desechado', date(2026,1,30))
    'Resuelto / AprobadoAprobado22/10/2025' → ('Aprobado', date(2025,10,22))
    'Pendiente en comisión(es) de origenPendiente05/11/2025' → ('Pendiente', date)
    Devuelve (categoria, fecha_resolucion) o (categoria, None) si no parsea fecha.
    """
    if not estatus_raw:
        return (None, None)
    m = re.search(r"(Aprobado|Desechado|Retirada|Pendiente|Rechazado)(\d{2}/\d{2}/\d{4})\s*$", estatus_raw)
    if not m:
        # Sin fecha
        for cat in ("Aprobado", "Desechado", "Retirada", "Pendiente", "Rechazado"):
            if cat in estatus_raw:
                return (cat, None)
        return (None, None)
    cat = m.group(1)
    try:
        fecha = datetime.strptime(m.group(2), "%d/%m/%Y").date()
    except ValueError:
        fecha = None
    return (cat, fecha)


def calcular_baseline_comisiones(conn):
    """Tasa de dictamen por comisión usando TODOS los presentadores en LXVI.
    Solo cuenta instrumentos con verdict (excluye pendientes <90d).
    Retorna dict comision → {tasa, n_aprobados, n_desechados, n_pendientes_largo, n_total}."""
    rows = conn.execute("""
        SELECT comision, estatus, fecha_presentacion
        FROM sil_documentos
        WHERE fecha_presentacion >= '2024-09-01'
          AND comision IS NOT NULL AND comision != ''
          AND (clasificacion = 'legislativo_sustantivo' OR clasificacion IS NULL)
    """).fetchall()

    por_com = defaultdict(lambda: {"apr": 0, "des": 0, "pen_largo": 0, "total": 0})
    for comision, estatus, fp in rows:
        s = clasificar_estatus(estatus or "", fp or "")
        if s is None:
            continue  # excluir pendientes <90d
        por_com[comision]["total"] += 1
        if s >= 0.7:
            por_com[comision]["apr"] += 1
        elif s == 0.0:
            if "desechado" in (estatus or "").lower() or "rechazado" in (estatus or "").lower() or "retirada" in (estatus or "").lower():
                por_com[comision]["des"] += 1
            else:
                por_com[comision]["pen_largo"] += 1

    out = {}
    for com, c in por_com.items():
        if c["total"] >= MIN_BASELINE_COMISION:
            out[com] = {
                "tasa": c["apr"] / c["total"],
                "n_aprobados": c["apr"],
                "n_desechados": c["des"],
                "n_pendientes_largo": c["pen_largo"],
                "n_total": c["total"],
            }
    return out


def construir_h2h(conn):
    """Para cada (legislador, comisión), construye stats + lista de instrumentos
    recientes. Solo emite filas con ≥MIN_INSTRUMENTOS_LEG_COMISION."""
    logger.info("Calculando baselines de comisión…")
    baselines = calcular_baseline_comisiones(conn)
    logger.info(f"  {len(baselines)} comisiones con baseline (≥{MIN_BASELINE_COMISION} instrumentos)")

    logger.info("Cargando instrumentos LXVI…")
    rows = conn.execute("""
        SELECT id, presentador, partido, camara, tipo, comision, estatus,
               fecha_presentacion, titulo
        FROM sil_documentos
        WHERE fecha_presentacion >= '2024-09-01'
          AND presentador != ''
          AND comision IS NOT NULL AND comision != ''
          AND (clasificacion = 'legislativo_sustantivo' OR clasificacion IS NULL)
        ORDER BY fecha_presentacion DESC
    """).fetchall()
    logger.info(f"  {len(rows)} instrumentos con comisión")

    # Agrupar por (nombre_legislador, comision)
    por_leg_com = defaultdict(list)  # (nombre, comision) → list of {id, titulo, fecha_pres, status_raw, fecha_pres_str}
    for sid, presentador, partido_raw, camara, tipo, comision, estatus, fp, titulo in rows:
        info = extraer_legislador(presentador)
        if not info:
            continue
        nombre_leg, _ = info
        por_leg_com[(nombre_leg, comision)].append({
            "sil_id": sid,
            "titulo": titulo or "",
            "fecha_pres_str": fp or "",
            "estatus_raw": estatus or "",
            "tipo": tipo or "",
        })

    # Match nombres de legisladores
    inverted, tokens_by_id = _construir_indice_legisladores(conn)
    legislador_id_cache = {}

    def get_leg_id(nombre):
        if nombre not in legislador_id_cache:
            legislador_id_cache[nombre] = _matchear_legislador(nombre, inverted, tokens_by_id)
        return legislador_id_cache[nombre]

    # Construir filas h2h
    h2h_rows = []
    skipped = 0
    for (nombre_leg, comision), instrumentos in por_leg_com.items():
        if len(instrumentos) < MIN_INSTRUMENTOS_LEG_COMISION:
            skipped += 1
            continue
        leg_id = get_leg_id(nombre_leg)
        if not leg_id:
            skipped += 1
            continue

        # Stats
        n = len(instrumentos)
        apr = des = pen_largo = pen_corto = pen_reciente = retirados = 0
        dias_a_dictamen = []  # solo de aprobados con fecha resolución parseable

        for inst in instrumentos:
            try:
                fp_date = datetime.strptime(inst["fecha_pres_str"], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue
            cat_estatus, fecha_resol = parsear_estatus(inst["estatus_raw"])
            inst["categoria_estatus"] = cat_estatus
            inst["fecha_resol"] = fecha_resol.isoformat() if fecha_resol else None

            if cat_estatus == "Aprobado":
                apr += 1
                if fecha_resol:
                    dias_a_dictamen.append((fecha_resol - fp_date).days)
            elif cat_estatus == "Desechado" or cat_estatus == "Rechazado":
                des += 1
            elif cat_estatus == "Retirada":
                retirados += 1
            elif cat_estatus == "Pendiente":
                dias = (HOY - fp_date).days
                if dias < DIAS_MIN_PENDIENTE:
                    pen_reciente += 1
                elif dias > DIAS_MAX_PENDIENTE:
                    pen_largo += 1
                else:
                    pen_corto += 1

        # Tasa de aprobación: solo cuenta los que tienen verdict (excluye pendientes recientes)
        n_con_verdict = apr + des + pen_largo + pen_corto + retirados
        tasa_aprob = (apr / n_con_verdict) if n_con_verdict > 0 else None

        # Baseline de la comisión
        baseline = baselines.get(comision)
        tasa_com = baseline["tasa"] if baseline else None
        diferencial = (tasa_aprob - tasa_com) if (tasa_aprob is not None and tasa_com is not None) else None

        # Tiempo promedio a dictamen (mediana sobre lista de días)
        dias_prom = None
        if dias_a_dictamen:
            dias_a_dictamen.sort()
            mid = len(dias_a_dictamen) // 2
            if len(dias_a_dictamen) % 2:
                dias_prom = dias_a_dictamen[mid]
            else:
                dias_prom = (dias_a_dictamen[mid-1] + dias_a_dictamen[mid]) / 2

        # Top 5 instrumentos recientes
        recientes = sorted(instrumentos, key=lambda x: x["fecha_pres_str"], reverse=True)[:5]
        recientes_json = []
        for inst in recientes:
            try:
                fp_date = datetime.strptime(inst["fecha_pres_str"], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue
            dias_total = (HOY - fp_date).days
            dias_dict = None
            cat = inst.get("categoria_estatus")
            if cat == "Aprobado" and inst.get("fecha_resol"):
                try:
                    fr = datetime.strptime(inst["fecha_resol"], "%Y-%m-%d").date()
                    dias_dict = (fr - fp_date).days
                except (ValueError, TypeError):
                    pass
            recientes_json.append({
                "sil_id": inst["sil_id"],
                "titulo": (inst["titulo"][:200] + "…") if len(inst["titulo"]) > 200 else inst["titulo"],
                "tipo": inst["tipo"],
                "fecha_pres": inst["fecha_pres_str"],
                "estatus": cat or "—",
                "dias_a_dictamen": dias_dict,
                "dias_desde_pres": dias_total,
            })

        h2h_rows.append({
            "legislador_id": leg_id,
            "nombre_legislador": nombre_leg,
            "comision": comision,
            "n_instrumentos": n,
            "aprobados": apr,
            "desechados": des,
            "retirados": retirados,
            "pendientes_largo": pen_largo,
            "pendientes_corto": pen_corto,
            "pendientes_recientes": pen_reciente,
            "tasa_aprobacion": round(tasa_aprob, 4) if tasa_aprob is not None else None,
            "tasa_comision_lxvi": round(tasa_com, 4) if tasa_com is not None else None,
            "diferencial_pp": round(diferencial * 100, 2) if diferencial is not None else None,
            "dias_promedio_dictamen": round(dias_prom, 1) if dias_prom is not None else None,
            "instrumentos_recientes": json.dumps(recientes_json, ensure_ascii=False),
            "fecha_calculo": datetime.now().isoformat(),
        })

    logger.info(f"  H2H rows generadas: {len(h2h_rows)} (skipped: {skipped})")
    return h2h_rows


def crear_tabla(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS legisladores_h2h (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            legislador_id INTEGER NOT NULL,
            nombre_legislador TEXT,
            comision TEXT NOT NULL,
            n_instrumentos INTEGER,
            aprobados INTEGER,
            desechados INTEGER,
            retirados INTEGER,
            pendientes_largo INTEGER,
            pendientes_corto INTEGER,
            pendientes_recientes INTEGER,
            tasa_aprobacion REAL,
            tasa_comision_lxvi REAL,
            diferencial_pp REAL,
            dias_promedio_dictamen REAL,
            instrumentos_recientes TEXT,
            fecha_calculo TEXT,
            UNIQUE(legislador_id, comision)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_h2h_legislador ON legisladores_h2h(legislador_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_h2h_comision ON legisladores_h2h(comision)")


def guardar(conn, h2h_rows):
    crear_tabla(conn)
    conn.execute("DELETE FROM legisladores_h2h")
    for r in h2h_rows:
        conn.execute("""
            INSERT INTO legisladores_h2h (
                legislador_id, nombre_legislador, comision,
                n_instrumentos, aprobados, desechados, retirados,
                pendientes_largo, pendientes_corto, pendientes_recientes,
                tasa_aprobacion, tasa_comision_lxvi, diferencial_pp,
                dias_promedio_dictamen, instrumentos_recientes, fecha_calculo
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (r["legislador_id"], r["nombre_legislador"], r["comision"],
              r["n_instrumentos"], r["aprobados"], r["desechados"], r["retirados"],
              r["pendientes_largo"], r["pendientes_corto"], r["pendientes_recientes"],
              r["tasa_aprobacion"], r["tasa_comision_lxvi"], r["diferencial_pp"],
              r["dias_promedio_dictamen"], r["instrumentos_recientes"], r["fecha_calculo"]))
    conn.commit()


def imprimir_top(conn, top_n=10):
    """Top N matchups por diferencial (mejor que su comisión)."""
    rows = conn.execute(f"""
        SELECT nombre_legislador, comision, n_instrumentos, aprobados,
               tasa_aprobacion, tasa_comision_lxvi, diferencial_pp, dias_promedio_dictamen
        FROM legisladores_h2h
        WHERE diferencial_pp IS NOT NULL AND n_instrumentos >= 5
        ORDER BY diferencial_pp DESC
        LIMIT {top_n}
    """).fetchall()
    print()
    print(f"  Top {top_n} matchups (mayor diferencial vs baseline de comisión):")
    print(f"  {'LEGISLADOR':<35} {'COMISIÓN':<30} {'N':>3} {'TASA':>6} {'BASE':>6} {'Δ':>7}")
    for nombre, com, n, apr, tasa, base, dif, dias in rows:
        print(f"  {nombre[:33]:<35} {(com or '')[:28]:<30} {n:>3} "
              f"{tasa*100:>5.0f}% {base*100:>5.0f}% {dif:>+5.1f}pp")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=10)
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB))
    h2h = construir_h2h(conn)
    if not h2h:
        logger.error("No se generaron H2H rows.")
        sys.exit(1)
    guardar(conn, h2h)
    print(f"\n  ✓ Guardadas {len(h2h)} filas en legisladores_h2h")

    n_legs = conn.execute("SELECT COUNT(DISTINCT legislador_id) FROM legisladores_h2h").fetchone()[0]
    n_coms = conn.execute("SELECT COUNT(DISTINCT comision) FROM legisladores_h2h").fetchone()[0]
    print(f"  Cobertura: {n_legs} legisladores · {n_coms} comisiones únicas")

    imprimir_top(conn, args.top)


if __name__ == "__main__":
    main()
