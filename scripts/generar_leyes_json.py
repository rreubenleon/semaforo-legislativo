"""
Genera dashboard/leyes.json — catálogo de leyes objetivo + agregados, para la
pestaña "Leyes" (búsqueda por ley: "¿cuántas reformas a la Ley X?").

Fuente única: config.SQL_SUSTANTIVO (universo iniciativas+proposiciones) +
estatus_canon (estado). Extrae la ley del título con extraer_ley_de_titulo()
y normaliza variantes (mayúsculas/acentos) a una llave canónica.

Por ley: total, desglose por estatus, por partido, por mes, y los N
instrumentos más recientes (titulo, fecha, partido, estatus, presentador, url).

Uso:
    python scripts/generar_leyes_json.py
"""
import json
import logging
import re
import sqlite3
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
OUT = ROOT / "dashboard" / "leyes.json"
sys.path.insert(0, str(ROOT))

from api.predictor_autoria import extraer_ley_de_titulo
try:
    from config import SQL_SUSTANTIVO
except Exception:
    SQL_SUSTANTIVO = "(LOWER(tipo) LIKE '%iniciativ%' OR LOWER(tipo) LIKE '%proposici%')"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

FECHA_INICIO = "2024-09-01"
TOP_INSTRUMENTOS = 25   # instrumentos recientes por ley en el JSON (controla tamaño)
MIN_INSTRUMENTOS = 2    # no listar leyes con 1 solo instrumento (ruido)


def norm_key(s):
    s = (s or "").lower()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def estatus_bucket(estatus_canon):
    e = (estatus_canon or "").lower()
    if "aprob" in e or "public" in e:
        return "aprobado"
    if "desech" in e or "rechaz" in e:
        return "desechado"
    if "retir" in e:
        return "retirada"
    if "pend" in e:
        return "pendiente"
    return "sin_estatus"


def generar(conn):
    rows = conn.execute(f"""
        SELECT titulo, fecha_presentacion, partido, tipo,
               COALESCE(estatus_canon, '') AS estatus_canon,
               COALESCE(presentador, '') AS presentador,
               COALESCE(url, '') AS url, COALESCE(camara, '') AS camara
        FROM sil_documentos
        WHERE fecha_presentacion >= '{FECHA_INICIO}'
          AND {SQL_SUSTANTIVO}
          AND titulo IS NOT NULL AND titulo != ''
        ORDER BY fecha_presentacion DESC
    """).fetchall()
    logger.info(f"Instrumentos sustantivos: {len(rows):,}")

    leyes = {}
    def _nueva():
        return {
            "display": None, "variantes": Counter(), "n": 0,
            "por_estatus": Counter(), "por_partido": Counter(),
            "por_mes": Counter(), "instrumentos": [],
        }

    con_ley = 0
    for titulo, fp, partido, tipo, est_canon, presentador, url, camara in rows:
        ley = extraer_ley_de_titulo(titulo)
        if not ley:
            continue
        k = norm_key(ley)
        if len(k) < 6:
            continue
        con_ley += 1
        d = leyes.setdefault(k, _nueva())
        d["variantes"][ley.strip()] += 1
        d["n"] += 1
        d["por_estatus"][estatus_bucket(est_canon)] += 1
        if partido:
            d["por_partido"][partido] += 1
        if fp and len(fp) >= 7:
            d["por_mes"][fp[:7]] += 1
        if len(d["instrumentos"]) < TOP_INSTRUMENTOS:
            d["instrumentos"].append({
                "titulo": titulo[:180],
                "fecha": fp or "",
                "partido": partido or "",
                "tipo": tipo or "",
                "estatus": estatus_bucket(est_canon),
                "presentador": (presentador or "")[:120],
                "url": url or "",
                "camara": camara or "",
            })

    catalogo = []
    for k, d in leyes.items():
        if d["n"] < MIN_INSTRUMENTOS:
            continue
        catalogo.append({
            "key": k,
            "display": d["variantes"].most_common(1)[0][0],
            "n": d["n"],
            "por_estatus": dict(d["por_estatus"]),
            "por_partido": dict(d["por_partido"].most_common(12)),
            "por_mes": dict(sorted(d["por_mes"].items())),
            "instrumentos": d["instrumentos"],
        })
    catalogo.sort(key=lambda x: -x["n"])

    out = {
        "generado": None,  # lo estampa el pipeline; aquí sin Date.now
        "fecha_inicio": FECHA_INICIO,
        "total_instrumentos_con_ley": con_ley,
        "total_leyes": len(catalogo),
        "leyes": catalogo,
    }
    OUT.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    kb = OUT.stat().st_size / 1024
    logger.info(f"✓ {len(catalogo)} leyes (≥{MIN_INSTRUMENTOS} instrumentos) → {OUT} ({kb:.0f} KB)")
    logger.info(f"  Top: " + " · ".join(f"{c['display'][:28]} ({c['n']})" for c in catalogo[:5]))


def main():
    conn = sqlite3.connect(str(DB))
    generar(conn)


if __name__ == "__main__":
    main()
