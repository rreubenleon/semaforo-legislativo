"""Lector de DOCUMENTOS OFICIALES: fecha de presentación REAL + texto íntegro.

Causa raíz del gate v3 (13-jul): 33.5% del ledger fue ingerido de vistas de
ETAPA (cuadros de asuntos por comisión de SITL, registros de turno en gaceta
del Senado) — su fecha_presentacion es la fecha del trámite, no de la
presentación. La fecha real (y el título/objeto íntegro) está EN el documento:
la ficha del SIL, el listado por comisión de SITL y la página de gaceta.

Escribe en sil_documentos (NO pisa fecha_presentacion — efectividad/radar la
usan): fecha_presentacion_real, fuente_fecha_real ∈ {sil, sitl}; y mejora
sinopsis/titulo con el texto oficial cuando pasa los guards de sanar.

Pasos ($0, particionables para CI):
  --paso universo : queries anchas a search_UTF del SIL (títulos ÍNTEGROS +
                    sinopsis + ids) → eval/_sil_universo.json.gz  (~minutos)
  --paso match    : instrumentos objetivo ↔ universo (clave-12 de contenido +
                    apellidos; guard medido) → eval/_sil_match.json + tasa
  --paso detalle  : ficha SIL por match → fecha real + texto  (~1.1s c/u)
  --paso sitl     : listados por comisión de SITL → fecha real + sinopsis
                    para las filas ingeridas de cuadros  (~53 fetches)
"""
import argparse
import gzip
import json
import os
import re
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from scripts.nucleo_titulos import key12, ctoks, apellidos
from scripts.sanar_titulos_truncados import stitch, objeto_valido, LONGS_TRUNCADAS

DB = Path(os.environ.get("SANAR_DB", ROOT / "semaforo.db"))
UNIVERSO = ROOT / "eval" / "_sil_universo.json.gz"
MATCHES = ROOT / "eval" / "_sil_match.json"

QUERIES = ["proposición", "iniciativa", "punto de acuerdo", "proyecto de decreto"]

MESES = {"enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
         "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
         "septiembre": "09", "octubre": "10", "noviembre": "11",
         "diciembre": "12"}


def ensure_cols(conn):
    for col in ("fecha_presentacion_real TEXT", "fuente_fecha_real TEXT"):
        try:
            conn.execute(f"ALTER TABLE sil_documentos ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass
    conn.commit()


def objetivo(conn, solo_sin_real=True):
    """Instrumentos a resolver: relevantes post-2024-09."""
    q = ("SELECT id, seguimiento_id, titulo, substr(fecha_presentacion,1,10), "
         "COALESCE(presentador,''), COALESCE(sinopsis,''), COALESCE(url,'') "
         "FROM sil_documentos WHERE fecha_presentacion >= '2024-09-01' "
         "AND titulo IS NOT NULL "
         "AND (tipo_grupo LIKE '%PA%' OR lower(tipo_grupo) LIKE '%iniciativa%' "
         "OR lower(tipo_grupo) LIKE '%punto de acuerdo%')")
    if solo_sin_real:
        q += " AND COALESCE(fecha_presentacion_real,'') = ''"
    return conn.execute(q).fetchall()


def paso_universo():
    from scrapers.sil import _buscar_ids
    vistos = {}
    for q in QUERIES:
        rows = _buscar_ids(q)
        for r in rows:
            vistos[(r["seguimiento_id"], r["asunto_id"])] = r
        print(f"query '{q}': {len(rows)} filas · universo acumulado: {len(vistos)}")
        time.sleep(2)
    out = list(vistos.values())
    with gzip.open(UNIVERSO, "wt", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)
    print(f"universo SIL guardado: {len(out)} asuntos → {UNIVERSO}")


def paso_match(conn):
    with gzip.open(UNIVERSO, "rt", encoding="utf-8") as f:
        uni = json.load(f)
    idx = {}
    for r in uni:
        k = key12(r["titulo"])
        if k:
            idx.setdefault(k, []).append(r)
    rows = objetivo(conn)
    print(f"objetivo: {len(rows)} instrumentos · universo: {len(uni)}")
    matches, ambiguos = {}, 0
    for rid, sid, titulo, f, pres, sin_, url in rows:
        k = key12(titulo)
        cands = idx.get(k, [])
        if not cands:
            continue
        ap = apellidos(pres, titulo)
        ntok = len(k.split())
        # guard medido: apellidos ∩ ≥2, o clave ≥8 tokens si algún lado no
        # trae autor (el universo de search no trae presentador)
        ok = [c for c in cands if ntok >= 8 or not ap]
        if not ok:
            ambiguos += 1
            continue
        # si hay varios asuntos SIL (seriales/etapas), guardar TODOS los pares
        # — el detalle decide cuál corresponde por fecha/presentador
        matches[sid] = [(c["seguimiento_id"], c["asunto_id"], c["titulo"],
                         c.get("sinopsis", "")) for c in ok[:4]]
    MATCHES.write_text(json.dumps(matches, ensure_ascii=False))
    print(f"match: {len(matches)}/{len(rows)} ({100*len(matches)/max(1,len(rows)):.1f}%) "
          f"· ambiguos descartados: {ambiguos}")
    return matches


def _mejorar_texto(conn, rid, titulo, sin_actual, texto_oficial):
    """Sinopsis/título con el texto oficial ÍNTEGRO si pasa los guards."""
    if not texto_oficial or len(texto_oficial) < 60:
        return
    if not objeto_valido(titulo, texto_oficial):
        return
    if len(texto_oficial) > len(sin_actual or ""):
        conn.execute("UPDATE sil_documentos SET sinopsis=? WHERE id=?",
                     (texto_oficial, rid))
    if len(titulo or "") in LONGS_TRUNCADAS:
        nuevo = stitch(titulo, texto_oficial)
        if nuevo:
            conn.execute("UPDATE sil_documentos SET titulo=? WHERE id=?",
                         (nuevo, rid))


def paso_detalle(conn, limite, dry, solo_vinculos=False):
    from scrapers.sil import _obtener_detalle
    matches = json.loads(MATCHES.read_text())
    rows = [r for r in objetivo(conn) if r[1] in matches]
    if solo_vinculos:
        V = json.loads((ROOT / "eval" / "vinculos_produccion.json").read_text())
        ids = {v["sil_id"] for v in V["vinculos"]}
        rows = [r for r in rows if r[1] in ids]
    rows = rows[:limite] if limite else rows
    print(f"fichas SIL a leer: {len(rows)}")
    rep = fall = 0
    for rid, sid, titulo, f, pres, sin_, url in rows:
        ap = apellidos(pres, titulo)
        mejor = None  # (fecha, texto, delta_a_fila)
        for seg, asu, tit_u, sin_u in matches[sid]:
            det = _obtener_detalle(seg, asu)
            time.sleep(1.1)
            if not det or not det.get("fecha_presentacion"):
                continue
            fd = det["fecha_presentacion"][:10]
            ap_d = apellidos(det.get("presentador", ""))
            # el asunto correcto: autor compatible; entre varios (seriales),
            # el de fecha más cercana SIN exceder la fecha de nuestra fila
            if ap and ap_d and not (ap & ap_d):
                continue
            delta = abs((int(f.replace("-", "")) - int(fd.replace("-", ""))))
            cand = (fd, (tit_u or "") + (" " + sin_u if sin_u else ""), delta)
            if mejor is None or (fd <= f) > (mejor[0] <= f) or delta < mejor[2]:
                mejor = cand
        if not mejor:
            fall += 1
            continue
        rep += 1
        if not dry:
            conn.execute("UPDATE sil_documentos SET fecha_presentacion_real=?, "
                         "fuente_fecha_real='sil' WHERE id=?", (mejor[0], rid))
            _mejorar_texto(conn, rid, titulo, sin_, mejor[1])
            if rep % 50 == 0:
                conn.commit()
                print(f"  {rep} resueltas · {fall} sin ficha")
    if not dry:
        conn.commit()
    print(f"detalle SIL: fecha real={rep} · sin resolver={fall}")


_SITL_ITEM = re.compile(
    r'class="linkTitulo">\s*([^<]{40,}?)</a>.*?'
    r'Proponente:\s*<b>(.*?)</b>.*?'
    r'Fecha de presentaci[oó]n:\s*<b>\s*(\d{1,2})-(\w+)-(\d{4})\b',
    re.S | re.I)
_SITL_SINOPSIS = re.compile(
    r'<td width="350"[^>]*><span class="Estilo71">([^<]{40,})</span>', re.I)


def paso_sitl_json(conn, dry):
    """Fechas de presentación desde el scrape per-diputado de SITL
    (dashboard/diputados_lxvi_oficial.json) para filas de cuadros que el
    scrape web por comisión no resolvió (canario tarifazo NL, 13-jul)."""
    src = ROOT / "dashboard" / "diputados_lxvi_oficial.json"
    if not src.exists():
        print("sin diputados_lxvi_oficial.json — paso omitido")
        return
    data = json.loads(src.read_text())
    idx = {}
    insts = data if isinstance(data, list) else data.get("instrumentos", [])
    if isinstance(data, dict) and not insts:
        insts = [i for v in data.values() if isinstance(v, list) for i in v]
    for inst in insts:
        if not isinstance(inst, dict):
            continue
        t, f = inst.get("titulo") or "", (inst.get("fecha") or "")[:10]
        if not t or not f:
            continue
        k = key12(t)
        if k:
            idx.setdefault(k, []).append((f, inst))
    rows = [r for r in objetivo(conn) if "cuadro_asuntos_por_comision" in r[6]]
    rep = 0
    for rid, sid, titulo, f, pres, sin_, url in rows:
        k = key12(titulo)
        cands = idx.get(k, [])
        if not cands:
            continue
        fechas = sorted({fd for fd, _ in cands})
        rep += 1
        if not dry:
            conn.execute("UPDATE sil_documentos SET fecha_presentacion_real=?, "
                         "fuente_fecha_real='sitl' WHERE id=? "
                         "AND COALESCE(fecha_presentacion_real,'')=''",
                         (fechas[0], rid))
    if not dry:
        conn.commit()
    print(f"SITL json per-diputado: fechas resueltas={rep}/{len(rows)}")


def paso_sitl(conn, dry):
    import urllib.request
    UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36"}
    # objetivo: filas ingeridas de cuadros por comisión (fecha = turno)
    rows = [r for r in objetivo(conn) if "cuadro_asuntos_por_comision" in r[6]]
    idx = {}
    for r in rows:
        k = key12(r[2])
        if k:
            idx.setdefault(k, []).append(r)
    print(f"filas de cuadros SITL a resolver: {len(rows)}")
    rep = 0
    for plantilla in ("iniciativaslxvi.php?comt={c}&tipo_turnot=1&edot=T",
                      "proposicioneslxvi.php?comt={c}&tipo_turnot=1&edot=T"):
        vacias = 0
        for c in range(1, 70):
            if vacias >= 8:
                break
            url = f"https://sitl.diputados.gob.mx/LXVI_leg/" + plantilla.format(c=c)
            try:
                req = urllib.request.Request(url, headers=UA)
                html = urllib.request.urlopen(req, timeout=40).read().decode("utf-8", "ignore")
            except Exception:
                vacias += 1
                continue
            time.sleep(0.5)
            items = list(_SITL_ITEM.finditer(html))
            if not items:
                vacias += 1
                continue
            vacias = 0
            for i, m in enumerate(items):
                tit, prop, dd, mes, aa = m.groups()
                fiso = f"{aa}-{MESES.get(mes.lower(), '01')}-{int(dd):02d}"
                # sinopsis oficial: primera celda-350 tras el título
                seg_html = html[m.end():items[i + 1].start() if i + 1 < len(items) else m.end() + 4000]
                msin = _SITL_SINOPSIS.search(seg_html)
                sin_of = (msin.group(1).strip() if msin else "")
                k = key12(tit)
                for rid, sid, titulo, f, pres, sin_, u in idx.get(k, []):
                    ap, ap_s = apellidos(pres, titulo), apellidos(prop, tit)
                    if ap and ap_s and not (ap & ap_s):
                        continue
                    rep += 1
                    if not dry:
                        conn.execute(
                            "UPDATE sil_documentos SET fecha_presentacion_real=?, "
                            "fuente_fecha_real='sitl' WHERE id=? "
                            "AND COALESCE(fecha_presentacion_real,'')=''",
                            (fiso, rid))
                        texto = tit + (". " + sin_of if sin_of else "")
                        _mejorar_texto(conn, rid, titulo, sin_, texto)
        if not dry:
            conn.commit()
    print(f"SITL: fechas reales resueltas={rep}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--paso", choices=["universo", "match", "detalle", "sitl",
                                       "sitl_json"],
                    required=True)
    ap.add_argument("--limite", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--solo-vinculos", action="store_true",
                    help="detalle solo para instrumentos del lote de vínculos")
    a = ap.parse_args()
    if a.paso == "universo":
        paso_universo()
    else:
        conn = sqlite3.connect(str(DB))
        ensure_cols(conn)
        if a.paso == "match":
            paso_match(conn)
        elif a.paso == "detalle":
            paso_detalle(conn, a.limite, a.dry_run, a.solo_vinculos)
        elif a.paso == "sitl_json":
            paso_sitl_json(conn, a.dry_run)
        else:
            paso_sitl(conn, a.dry_run)
