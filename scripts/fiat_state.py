"""Snapshot del estado real de FIAT.

Lo que Claude debe correr ANTES de afirmar algo sobre el estado del
sistema. Pregunta + respuesta verificable, no memoria.

Cubre: linaje de fuentes, frescura por tabla, cobertura por mes,
quirks del pipeline, scores recientes, deploys recientes, overrides
activos, divergencia local vs producción.

Uso:
    python scripts/fiat_state.py
    python scripts/fiat_state.py --seccion fuentes  # solo una sección
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from db import get_connection


# Quirks del pipeline (cosas que NO se pueden inferir de la BD).
# Actualizar cuando cambien.
QUIRKS = [
    ("Pipeline cron", "Cada 4h: 00, 04, 08, 12, 16, 20 UTC"),
    ("Gaceta Senado en receso", "Solo corre los miércoles (paso_2a). Si máx-fecha = abr-21, normal."),
    ("Permanente vs Pleno", "Durante receso (1-may→31-ago, 16-dic→31-ene) el Pleno no sesiona — la Permanente sí."),
    ("Google Trends", "Pagado (SerpAPI). Solo corre 1×/día en run de 12-13 UTC."),
    ("Twitter", "Pagado (~$36/mes). PROHIBIDO escalar sin OK explícito."),
    ("Haiku 4.5", "PROHIBIDO. Memoria: feedback_haiku_prohibido.md. Solo clasificar_texto (keyword, $0)."),
    ("Scoring (post-23-may-2026)", "Snapshot intradía cada 4h + consolidación 20 UTC vía AVG → scores."),
    ("Score components", "0.20 media + 0.15 trends + 0.25 congreso + 0.10 mañanera + 0.15 urgencia + 0.15 dominancia"),
    ("Color umbrales", "Verde ≥70 · Amarillo 40-69 · Rojo <40"),
    ("Override extraordinario", "prob_extraordinario_global forzado a 100% (main.py:723) hasta arreglar scraper citatorios + filtro SIL."),
    ("Permanente excluido", "ELO + H2H ya excluyen camara='Comisión Permanente'. Predictor_autoria pendiente (necesita schema)."),
]


def seccion(titulo: str):
    print("\n" + "=" * 78)
    print(titulo)
    print("=" * 78)


def fuentes(c):
    seccion("LINAJE DE FUENTES DE MEDIOS (cuándo empezó cada scrape)")
    print(f"{'fuente':22} {'desde':12} {'hasta':12} {'n':>8}")
    print("-" * 60)
    for r in c.execute(
        "SELECT fuente, MIN(substr(fecha,1,10)), MAX(substr(fecha,1,10)), COUNT(*) "
        "FROM articulos GROUP BY fuente ORDER BY MIN(fecha)"
    ).fetchall():
        print(f"{r[0]:22} {r[1]:12} {r[2]:12} {r[3]:>8,}")
    print()
    print("⚠ Comparaciones de volumen mediático ANTES de feb-2026 NO son")
    print("  comparables — solo La Jornada estaba activa en gran parte.")


def frescura(c):
    seccion("FRESCURA DE DATOS (última fecha por tabla)")
    hoy = datetime.utcnow().date()
    tablas = [
        ("articulos", "fecha"),
        ("gaceta", "fecha"),
        ("sil_documentos", "fecha_presentacion"),
        ("trends", "fecha"),
        ("mananera", "fecha"),
        ("tweets", "fecha"),
        ("scores", "fecha"),
        ("correlaciones", "fecha_analisis"),
        ("resoluciones", "fecha_calculo"),
    ]
    print(f"{'tabla':24} {'max fecha':22} {'edad':>10}")
    print("-" * 60)
    for t, col in tablas:
        try:
            r = c.execute(f"SELECT MAX({col}), COUNT(*) FROM {t}").fetchone()
            if r[0]:
                f = r[0][:10]
                edad = (hoy - datetime.strptime(f, "%Y-%m-%d").date()).days
                print(f"{t:24} {r[0][:22]:22} {edad:>4}d  (n={r[1]:,})")
            else:
                print(f"{t:24} (vacía)")
        except Exception as e:
            print(f"{t:24} ERROR {e}")


def cobertura_mes(c):
    seccion("COBERTURA POR MES (artículos, ordenado por mes ↓, top 12)")
    print(f"{'mes':10} {'total':>7}  {'#fuentes':>9}  {'top fuente':24}")
    print("-" * 60)
    for r in c.execute(
        "SELECT substr(fecha,1,7) ym, COUNT(*) n, COUNT(DISTINCT fuente) nf "
        "FROM articulos WHERE fecha>='2024-09' GROUP BY ym ORDER BY ym DESC LIMIT 12"
    ).fetchall():
        top = c.execute(
            "SELECT fuente, COUNT(*) FROM articulos WHERE substr(fecha,1,7)=? "
            "GROUP BY fuente ORDER BY 2 DESC LIMIT 1", (r[0],)
        ).fetchone()
        print(f"{r[0]:10} {r[1]:>7,}  {r[2]:>9}  {top[0]:24}")


def scores_state(c):
    seccion("SCORES (estado del último día)")
    r = c.execute("SELECT MAX(fecha), COUNT(DISTINCT fecha) FROM scores").fetchone()
    print(f"Último día con scores: {r[0]}  (total días con scores: {r[1]})")
    rows = c.execute(
        "SELECT categoria, score_total, color FROM scores "
        "WHERE fecha = (SELECT MAX(fecha) FROM scores) ORDER BY score_total DESC"
    ).fetchall()
    if rows:
        print(f"\nDistribución del último día ({len(rows)} categorías):")
        for cat, total, color in rows[:5]:
            print(f"  {color[:8]:8} {total:5.1f}  {cat}")
        print(f"  ... ({len(rows) - 5} más)")
    # tabla scores_intradia (post-23-may)
    try:
        r2 = c.execute("SELECT MAX(fecha), COUNT(*) FROM scores_intradia").fetchone()
        if r2[0]:
            print(f"\nscores_intradia: últimos snapshots hasta {r2[0]} (n={r2[1]})")
    except Exception:
        print("\nscores_intradia: aún no existe (esperando primer pipeline post-deploy)")


def git_state():
    seccion("ESTADO GIT (local vs prod)")
    def run(cmd):
        try:
            return subprocess.check_output(cmd, cwd=ROOT, text=True, stderr=subprocess.DEVNULL).strip()
        except Exception:
            return "(?)"
    branch = run(["git", "branch", "--show-current"])
    print(f"Branch: {branch}")
    behind_ahead = run(["git", "rev-list", "--left-right", "--count", f"origin/main...{branch}"])
    if behind_ahead and "\t" in behind_ahead:
        b, a = behind_ahead.split("\t")
        print(f"Diferencia con origin/main: {b} behind, {a} ahead")
    print("\nÚltimos 5 commits:")
    print(run(["git", "log", "--oneline", "-5"]))
    diff = run(["git", "status", "--short"])
    if diff:
        print("\nArchivos modificados sin commitear:")
        print(diff)


def pipeline_recent():
    seccion("ÚLTIMAS CORRIDAS DEL PIPELINE EN CI")
    try:
        out = subprocess.check_output(
            ["gh", "run", "list", "--workflow=pipeline.yml", "--limit", "5"],
            cwd=ROOT, text=True, stderr=subprocess.DEVNULL
        ).strip()
        print(out)
    except Exception:
        print("(gh no disponible o sin auth — saltado)")


def quirks_section():
    seccion("QUIRKS Y REGLAS DURAS DEL SISTEMA (no inferibles de la BD)")
    for k, v in QUIRKS:
        print(f"  • {k}: {v}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seccion", choices=["fuentes", "frescura", "cobertura",
                                          "scores", "git", "pipeline", "quirks", "all"],
                    default="all")
    args = ap.parse_args()
    c = get_connection()
    print(f"FIAT STATE — {datetime.utcnow().isoformat(timespec='seconds')}Z")

    if args.seccion in ("all", "fuentes"): fuentes(c)
    if args.seccion in ("all", "frescura"): frescura(c)
    if args.seccion in ("all", "cobertura"): cobertura_mes(c)
    if args.seccion in ("all", "scores"): scores_state(c)
    if args.seccion in ("all", "git"): git_state()
    if args.seccion in ("all", "pipeline"): pipeline_recent()
    if args.seccion in ("all", "quirks"): quirks_section()


if __name__ == "__main__":
    main()
