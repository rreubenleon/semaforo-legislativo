"""
Divergence Index: KL divergence entre las 6 fuentes del score_total.

Pregunta que responde: "¿Las 6 capas que componen el score_total están
diciendo lo mismo, o están divergiendo? Y si divergen, ¿cuál es el
patrón?"

Las 6 fuentes (de tabla `scores`):
  · score_media       — cobertura en medios
  · score_trends      — búsquedas en Google
  · score_congreso    — Gaceta + SIL
  · score_mananera    — menciones de CSP
  · score_urgencia    — reactividad histórica
  · score_dominancia  — relación media-legislativo

Metodología:
  1. Para cada categoría, calcula el promedio histórico de las 6 fuentes
     en una ventana rolling de 90 días → distribución baseline (suma 1).
  2. Para el día de hoy, calcula la distribución observada (suma 1).
  3. KL(hoy ‖ baseline). Rango: 0 (idéntico) a ~1.5 (extremo).
  4. Identifica fuente más sobre-representada y más sub-representada.
  5. Aplica reglas de patrón → etiqueta humana.

Output: agrega campos `divergence_index`, `divergence_pattern` a la tabla
`scores` para la fecha más reciente. Tabla nueva opcional para histórico.

Uso:
    python scripts/calcular_divergencia.py
    python scripts/calcular_divergencia.py --fecha 2026-04-22
    python scripts/calcular_divergencia.py --dry-run
"""
import argparse
import logging
import math
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

FUENTES = [
    "score_media",
    "score_trends",
    "score_congreso",
    "score_mananera",
    "score_urgencia",
    "score_dominancia",
]
FUENTE_LABEL = {
    "score_media":      "Media",
    "score_trends":     "Trends",
    "score_congreso":   "Congreso",
    "score_mananera":   "Mañanera",
    "score_urgencia":   "Urgencia",
    "score_dominancia": "Dominancia",
}

VENTANA_BASELINE_DIAS = 90
EPSILON = 1e-9            # evitar log(0)
RATIO_ALTO = 1.5          # fuente_hoy / fuente_baseline > 1.5 → ↑
RATIO_BAJO = 0.6          # fuente_hoy / fuente_baseline < 0.6 → ↓
KL_MIN_REPORTABLE = 0.05  # bajo este umbral no etiquetamos patrón


def normalizar_distribucion(valores):
    """Convierte lista de valores ≥0 en distribución de probabilidad
    (suma 1). Aplica suavizado mínimo para evitar ceros absolutos."""
    suavizados = [max(v, EPSILON) for v in valores]
    total = sum(suavizados)
    return [v / total for v in suavizados]


def kl_divergence(p, q):
    """KL(p ‖ q) = Σ p_i · log(p_i / q_i). Retorna float."""
    return sum(pi * math.log(pi / qi) for pi, qi in zip(p, q) if pi > 0)


def detectar_patron(valores_hoy_norm, valores_base_norm, kl):
    """Reglas declarativas basadas en qué fuente está sobre-representada
    y cuál sub-representada vs el baseline. Las fuentes derivadas
    (Urgencia, Dominancia) producen patrones técnicos (rebalanceo) en
    vez de narrativos, porque no representan señal periodística directa."""
    if kl < KL_MIN_REPORTABLE:
        return ("convergencia", "Convergencia",
                "Las 6 fuentes alineadas con su distribución típica.")

    ratios = []
    for fuente, hoy, base in zip(FUENTES, valores_hoy_norm, valores_base_norm):
        r = hoy / base if base > 0 else 0
        ratios.append((fuente, r))
    ratios_sorted = sorted(ratios, key=lambda x: -x[1])
    sobre = ratios_sorted[0][0]
    sub = ratios_sorted[-1][0]

    # Patrones narrativos (señal periodística)
    if sobre == "score_media":
        if sub == "score_congreso":
            return ("presion_sin_respuesta", "Presión sin respuesta",
                    "Cobertura mediática elevada con baja respuesta legislativa.")
        if sub == "score_mananera":
            return ("cobertura_autonoma", "Cobertura mediática autónoma",
                    "Tema mediático sin tracción presidencial.")
        return ("tema_mediatico_dominante", "Tema mediático dominante",
                "La cobertura en medios concentra el peso del score.")

    if sobre == "score_congreso":
        if sub in ("score_media", "score_trends"):
            return ("movimiento_legislativo_silencioso", "Movimiento legislativo silencioso",
                    "Actividad en Congreso sin tracción mediática proporcional.")
        return ("actividad_legislativa_intensiva", "Actividad legislativa intensiva",
                "El Congreso concentra el peso del score por encima de las demás capas.")

    if sobre == "score_mananera":
        if sub in ("score_media", "score_congreso"):
            return ("agenda_presidencial_aislada", "Agenda presidencial aislada",
                    "El Ejecutivo aborda el tema sin reflejo en otras capas.")
        return ("empuje_ejecutivo", "Empuje desde el Ejecutivo",
                "La Mañanera concentra el peso del score.")

    if sobre == "score_trends":
        if sub == "score_media":
            return ("reaccion_ciudadana_solitaria", "Reacción ciudadana solitaria",
                    "Búsquedas elevadas sin amplificación mediática.")
        return ("interes_ciudadano_elevado", "Interés ciudadano elevado",
                "Las búsquedas concentran el peso del score.")

    # Cuando la fuente sobre-representada es derivada (Dominancia o Urgencia),
    # no es señal narrativa — es un reordenamiento técnico del modelo.
    if sobre in ("score_dominancia", "score_urgencia"):
        return ("rebalanceo_tecnico", "Rebalanceo del modelo",
                "Las fuentes derivadas (Urgencia/Dominancia) dominan el reparto. Sin patrón narrativo asociado.")

    return ("divergencia_atipica", "Divergencia atípica",
            "Las fuentes presentan un balance inusual sin patrón estándar.")


def cargar_baseline(conn, categoria, fecha_pivote, ventana_dias=VENTANA_BASELINE_DIAS):
    """Promedio rolling de cada fuente en los últimos `ventana_dias`
    previos a `fecha_pivote` para esa categoría. Retorna lista en el
    orden de FUENTES."""
    desde = (fecha_pivote - timedelta(days=ventana_dias)).isoformat()
    hasta = (fecha_pivote - timedelta(days=1)).isoformat()
    cols = ", ".join([f"AVG(COALESCE({f}, 0))" for f in FUENTES])
    cols += ", COUNT(*)"
    row = conn.execute(f"""
        SELECT {cols}
        FROM scores
        WHERE categoria = ? AND fecha >= ? AND fecha <= ?
    """, (categoria, desde, hasta)).fetchone()
    if not row or row[-1] < 14:
        return None  # menos de 14 días de histórico → baseline no confiable
    return list(row[:-1])


def calcular_para_fecha(conn, fecha):
    """Computa divergencia para todas las categorías en una fecha.
    Retorna lista de dicts."""
    cols = ", ".join(FUENTES)
    rows = conn.execute(f"""
        SELECT categoria, {cols}, score_total
        FROM scores
        WHERE fecha = ?
    """, (fecha.isoformat(),)).fetchall()

    out = []
    for r in rows:
        categoria = r[0]
        valores_hoy = [float(v or 0) for v in r[1:1+len(FUENTES)]]
        score_total = float(r[-1] or 0)

        # Si todos los valores son ~0, no tiene sentido calcular
        if sum(valores_hoy) < 1:
            continue

        baseline = cargar_baseline(conn, categoria, fecha)
        if baseline is None:
            continue

        p = normalizar_distribucion(valores_hoy)
        q = normalizar_distribucion(baseline)
        kl = kl_divergence(p, q)

        patron_id, patron_label, explicacion = detectar_patron(p, q, kl)

        # Identificar fuente sobre/sub representada
        ratios = [(FUENTES[i], p[i] / q[i] if q[i] > 0 else 0) for i in range(len(FUENTES))]
        ratios_sorted = sorted(ratios, key=lambda x: -x[1])
        sobre = FUENTE_LABEL[ratios_sorted[0][0]]
        sub = FUENTE_LABEL[ratios_sorted[-1][0]]

        out.append({
            "categoria": categoria,
            "fecha": fecha.isoformat(),
            "score_total": round(score_total, 2),
            "kl": round(kl, 3),
            "patron_id": patron_id,
            "patron_label": patron_label,
            "explicacion": explicacion,
            "sobre_representada": sobre,
            "sub_representada": sub,
            "valores_hoy": [round(v, 1) for v in valores_hoy],
            "valores_baseline": [round(v, 1) for v in baseline],
        })
    return out


def crear_tabla(conn):
    """Tabla histórica para fase 2 (serie temporal). Por ahora la
    poblamos pero el dashboard sólo lee la fila más reciente."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS divergencia (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            categoria TEXT NOT NULL,
            fecha TEXT NOT NULL,
            kl REAL,
            patron_id TEXT,
            patron_label TEXT,
            explicacion TEXT,
            sobre_representada TEXT,
            sub_representada TEXT,
            UNIQUE(categoria, fecha)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_div_fecha ON divergencia(fecha)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_div_categoria ON divergencia(categoria)")


def guardar(conn, divergencias):
    crear_tabla(conn)
    for d in divergencias:
        conn.execute("""
            INSERT INTO divergencia (
                categoria, fecha, kl, patron_id, patron_label,
                explicacion, sobre_representada, sub_representada
            ) VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(categoria, fecha) DO UPDATE SET
                kl=excluded.kl,
                patron_id=excluded.patron_id,
                patron_label=excluded.patron_label,
                explicacion=excluded.explicacion,
                sobre_representada=excluded.sobre_representada,
                sub_representada=excluded.sub_representada
        """, (d["categoria"], d["fecha"], d["kl"], d["patron_id"],
              d["patron_label"], d["explicacion"],
              d["sobre_representada"], d["sub_representada"]))
    conn.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fecha", help="YYYY-MM-DD (default: última fecha en scores)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB))

    if args.fecha:
        fecha = datetime.strptime(args.fecha, "%Y-%m-%d").date()
    else:
        row = conn.execute("SELECT MAX(fecha) FROM scores").fetchone()
        fecha = datetime.strptime(row[0], "%Y-%m-%d").date()

    logger.info(f"Calculando divergencia para fecha {fecha}")
    divergencias = calcular_para_fecha(conn, fecha)
    logger.info(f"Categorías procesadas: {len(divergencias)}")

    if not divergencias:
        logger.error("Sin divergencias calculadas (¿histórico insuficiente?).")
        sys.exit(1)

    print()
    print(f"  ═══ Divergencia · {fecha} ═══")
    print(f"  {'CATEGORÍA':<22} {'KL':>5} {'PATRÓN':<38} {'↑ FUENTE':<12} {'↓ FUENTE':<12}")
    for d in sorted(divergencias, key=lambda x: -x["kl"]):
        print(f"  {d['categoria'][:20]:<22} {d['kl']:>5.2f} {d['patron_label'][:36]:<38} {d['sobre_representada']:<12} {d['sub_representada']:<12}")

    if args.dry_run:
        print("\n  (dry-run: no se guardó en BD)")
        return

    guardar(conn, divergencias)
    print(f"\n  ✓ Guardadas {len(divergencias)} filas en tabla divergencia")


if __name__ == "__main__":
    main()
