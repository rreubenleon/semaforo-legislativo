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


CATALOGO_PATH = ROOT / "data" / "leyes_vigentes.txt"

_STOP = {"de", "del", "la", "las", "los", "el", "y", "en", "a", "para", "por", "con", "que", "su", "al"}


def norm_key(s):
    s = (s or "").lower()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _toks(s):
    return set(t for t in norm_key(s).split() if t not in _STOP)


def cargar_catalogo():
    """Lee data/leyes_vigentes.txt (una ley por línea, 'Canónico | alias | …').
    Devuelve lista de (display, [(key_norm, key_toks), …])."""
    cat = []
    for line in CATALOGO_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        partes = [p.strip() for p in line.split("|") if p.strip()]
        display = partes[0]
        claves = [(norm_key(p), _toks(p)) for p in partes]
        cat.append((display, claves))
    return cat


def construir_matcher(cat):
    """Mapea el nombre de ley extraído de un título → ley vigente canónica.
    None si no corresponde a ninguna ley vigente (extensión/mal codificada)."""
    exacto = {}
    for disp, claves in cat:
        for kn, _ in claves:
            exacto.setdefault(kn, disp)

    def match(ley_extraida):
        kn = norm_key(ley_extraida)
        if not kn:
            return None
        if kn in exacto:
            return exacto[kn]
        et = set(t for t in kn.split() if t not in _STOP)
        if len(et) < 2:
            return None
        # Prioridad: el nombre oficial EMPIEZA con lo extraído (truncación real).
        # Desambigua casos como "Constitución Política", que aparece DENTRO del
        # nombre de muchas leyes reglamentarias pero solo ENCABEZA la suya.
        pref = {disp for disp, claves in cat for kc, _ in claves if kc == kn or kc.startswith(kn + " ")}
        if len(pref) == 1:
            return next(iter(pref))
        # Truncación: el extractor corta en comas/"y" → el extraído suele ser un
        # PREFIJO del nombre oficial. Buscar canónicos que CONTENGAN todos los
        # tokens del extraído. Si solo uno → match limpio (caso "Ley de Caminos").
        hits = []  # (display, n_tokens_canonico)
        for disp, claves in cat:
            best = None
            for _, kt in claves:
                if kt and et <= kt:
                    if best is None or len(kt) < best:
                        best = len(kt)
            if best is not None:
                hits.append((disp, best))
        displays = {d for d, _ in hits}
        if len(displays) == 1:
            return hits[0][0]
        if len(displays) > 1:
            # ambiguo: aceptar el más específico solo si el extraído lo cubre
            # bien y es estrictamente más específico que el siguiente.
            hits.sort(key=lambda x: x[1])
            if hits[0][1] < hits[1][1] and len(et) / hits[0][1] >= 0.5:
                return hits[0][0]
            return None
        # Caso inverso: extraído con cola extra (canónico ⊆ extraído), único.
        inv = {disp for disp, claves in cat for _, kt in claves if kt and kt <= et and len(kt) >= 3}
        if len(inv) == 1:
            return next(iter(inv))
        return None

    return match


def camara_bucket(camara):
    c = (camara or "")
    if c.startswith(("Cámara de Sen", "Senado")):
        return "Senado"
    if c.startswith("Cámara de Dip") or c == "Diputados":
        return "Diputados"
    if "Permanente" in c:
        return "Permanente"
    return "Otros"


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

    cat = cargar_catalogo()
    match = construir_matcher(cat)
    logger.info(f"Catálogo de leyes vigentes: {len(cat)}")

    leyes = {}
    def _nueva():
        return {
            "display": None, "variantes": Counter(), "n": 0,
            "por_estatus": Counter(), "por_partido": Counter(),
            "por_mes": Counter(), "por_camara": Counter(), "instrumentos": [],
        }

    nuevas = {}  # propuestas para CREAR leyes nuevas ("expide" + no vigente)

    con_ley = 0       # instrumentos cuyo título nombra una ley
    con_vigente = 0   # … que mapea a una ley VIGENTE del catálogo
    for titulo, fp, partido, tipo, est_canon, presentador, url, camara in rows:
        ley = extraer_ley_de_titulo(titulo)
        if not ley:
            continue
        con_ley += 1
        canon = match(ley)
        if not canon:
            # No es vigente. Si el título "expide" una ley → es propuesta de
            # NUEVA ley (idea para crear una ley que aún no existe). Si no,
            # es reforma a algo no vigente / ruido → fuera.
            if "expid" in (titulo or "").lower():
                nk = norm_key(ley)
                if len(nk) >= 6:
                    nd = nuevas.setdefault(nk, {"display": None, "variantes": Counter(), "n": 0,
                                                "por_estatus": Counter(), "por_partido": Counter(),
                                                "instrumentos": []})
                    nd["variantes"][ley.strip()] += 1
                    nd["n"] += 1
                    nd["por_estatus"][estatus_bucket(est_canon)] += 1
                    if partido:
                        nd["por_partido"][partido] += 1
                    if len(nd["instrumentos"]) < 12:
                        nd["instrumentos"].append({
                            "titulo": titulo[:180], "fecha": fp or "", "partido": partido or "",
                            "estatus": estatus_bucket(est_canon), "presentador": (presentador or "")[:120],
                            "url": url or "", "camara": camara_bucket(camara),
                        })
            continue
        con_vigente += 1
        k = norm_key(canon)
        d = leyes.setdefault(k, _nueva())
        d["display"] = canon
        d["variantes"][canon] += 1
        d["n"] += 1
        cam = camara_bucket(camara)
        d["por_estatus"][estatus_bucket(est_canon)] += 1
        d["por_camara"][cam] += 1
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
                "camara": cam,
            })

    catalogo = []
    for k, d in leyes.items():
        if d["n"] < MIN_INSTRUMENTOS:
            continue
        catalogo.append({
            "key": k,
            "display": d["display"],
            "n": d["n"],
            "por_estatus": dict(d["por_estatus"]),
            "por_camara": dict(d["por_camara"]),
            "por_partido": dict(d["por_partido"].most_common(12)),
            "por_mes": dict(sorted(d["por_mes"].items())),
            "instrumentos": d["instrumentos"],
        })
    catalogo.sort(key=lambda x: -x["n"])

    nuevas_cat = []
    for k, d in nuevas.items():
        if d["n"] < MIN_INSTRUMENTOS:
            continue
        nuevas_cat.append({
            "key": k,
            "display": d["variantes"].most_common(1)[0][0],
            "n": d["n"],
            "por_estatus": dict(d["por_estatus"]),
            "por_partido": dict(d["por_partido"].most_common(12)),
            "instrumentos": d["instrumentos"],
        })
    nuevas_cat.sort(key=lambda x: -x["n"])
    nuevas_total = sum(d["n"] for d in nuevas.values())

    out = {
        "generado": None,  # lo estampa el pipeline; aquí sin Date.now
        "fecha_inicio": FECHA_INICIO,
        "total_instrumentos_con_ley": con_ley,
        "total_instrumentos_vigente": con_vigente,
        "total_leyes": len(catalogo),
        "leyes": catalogo,
        "nuevas_leyes": nuevas_cat,
        "nuevas_leyes_total": nuevas_total,
    }
    OUT.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    kb = OUT.stat().st_size / 1024
    logger.info(f"Con ley en título: {con_ley:,} · mapean a ley vigente: {con_vigente:,} ({round(100*con_vigente/max(1,con_ley))}%)")
    logger.info(f"✓ {len(catalogo)} leyes vigentes (≥{MIN_INSTRUMENTOS} instrumentos) → {OUT} ({kb:.0f} KB)")
    logger.info(f"  Top: " + " · ".join(f"{c['display'][:28]} ({c['n']})" for c in catalogo[:5]))


def main():
    conn = sqlite3.connect(str(DB))
    generar(conn)


if __name__ == "__main__":
    main()
