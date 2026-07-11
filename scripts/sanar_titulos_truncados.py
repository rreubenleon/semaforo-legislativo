"""Backfill de títulos truncados en sil_documentos (causa raíz de la cuarentena
de vínculos: caps [:200]/[:500] de ingesta cortaban el OBJETO del exhorto).

Dos pasos, ambos $0:
  --paso reconteo : cruza con eval/instrumentos_sil.json (reconteo semanal, que
                    guarda títulos ÍNTEGROS) por legislador+fecha+tipo. Match
                    único directo; con varios candidatos desempata por tokens.
  --paso gaceta   : para los sin cruce, descarga el documento de gaceta del
                    Senado (columna url) con el cliente anti-Incapsula de
                    gaceta_senado (sesión + rate limit) y extrae el objeto.

Reparación en dos columnas:
  - `sinopsis` ← objeto completo (SIEMPRE que se recupere; es lo que los
    jueces de vínculos necesitan y no rompe ninguna UI).
  - `titulo`  ← solo si el "stitch" es limpio (≥5 tokens de traslape entre el
    final del título truncado y el objeto): titulo_truncado + continuación.

Uso:
  python scripts/sanar_titulos_truncados.py --paso reconteo [--dry-run]
  python scripts/sanar_titulos_truncados.py --paso gaceta --limite 800
"""
import argparse
import json
import re
import sqlite3
import sys
import time
import unicodedata
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import os

DB = Path(os.environ.get("SANAR_DB", ROOT / "semaforo.db"))
LONGS_TRUNCADAS = (199, 200, 499, 500)


def na(s):
    s = (s or "").lower()
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def toks(s):
    return re.findall(r"[a-z0-9]+", na(s))


def stitch(trunco, objeto):
    """Une título truncado + objeto completo si hay traslape limpio (≥5 tokens).
    Devuelve el título completo reconstruido o None si no se puede con limpieza."""
    tt = toks(trunco)
    if len(tt) < 5:
        return None
    # tokens del objeto CON offsets de carácter para recuperar el texto crudo
    spans = [(m.group(0), m.start()) for m in re.finditer(r"[^\s]+", objeto)]
    to = [na(re.sub(r"[^\wáéíóúñ]", "", w)) for w, _ in spans]
    for k in range(min(12, len(tt)), 4, -1):
        cola = tt[-k:]
        for j in range(0, len(to) - k + 1):
            ventana = [re.sub(r"[^a-z0-9]", "", x) for x in to[j:j + k]]
            if ventana == cola:
                # continuación cruda del objeto después del traslape
                fin = j + k
                if fin >= len(spans):
                    return None  # el truncado ya cubre todo el objeto
                cont = objeto[spans[fin][1]:].strip()
                if not cont:
                    return None
                return (trunco.rstrip() + " " + cont).strip()
    return None


def filas_truncadas(conn, solo_sin_sinopsis=True):
    q = f"""SELECT sd.id, sd.seguimiento_id, sd.titulo,
                   substr(sd.fecha_presentacion,1,10) f, LOWER(COALESCE(sd.tipo_grupo,'')) tg,
                   COALESCE(sd.sinopsis,'') sin, COALESCE(sd.url,'') url
            FROM sil_documentos sd
            WHERE sd.fecha_presentacion >= '2024-09-01'
              AND LENGTH(sd.titulo) IN ({",".join(str(x) for x in LONGS_TRUNCADAS)})"""
    if solo_sin_sinopsis:
        # sin sinopsis O sinopsis CONTAMINADA (el scraper viejo guardaba los
        # autores + el propio título truncado — firma detectada por el
        # Escéptico 11-jul: 60% de las "sanas" eran esto)
        q += (" AND (LENGTH(COALESCE(sd.sinopsis,'')) < 40"
              " OR instr(lower(sd.sinopsis), lower(substr(sd.titulo,1,120))) > 0)")
    return conn.execute(q).fetchall()


def paso_reconteo(conn, dry):
    ins = json.loads((ROOT / "eval" / "instrumentos_sil.json").read_text())
    idx = defaultdict(list)
    for lid, recs in ins.items():
        for r in recs:
            t = r.get("tit") or ""
            f = (r.get("f") or "")[:10]
            if t and f:
                idx[(int(lid), f, r.get("t"))].append(t)
    legs = defaultdict(list)
    for r in conn.execute("""SELECT sil_documento_id, legislador_id FROM actividad_legislador
                             WHERE legislador_id IS NOT NULL"""):
        legs[r[0]].append(r[1])

    rows = filas_truncadas(conn)
    print(f"truncadas pendientes: {len(rows)}")
    rep_sin = rep_tit = 0
    ejemplos = []
    for rid, sid, titulo, f, tg, sin_, url in rows:
        tp = "ini" if "iniciativa" in tg else "prop"
        cands = []
        for lid in set(legs.get(rid, [])):
            cands.extend(idx.get((lid, f, tp), []))
        cands = list(dict.fromkeys(cands))
        objeto = None
        if len(cands) == 1:
            objeto = cands[0]
        elif len(cands) > 1:
            # desempate: mayor traslape de tokens con el final del truncado
            cola = set(toks(titulo)[-20:])
            scored = sorted(((len(cola & set(toks(c))), c) for c in cands), reverse=True)
            if scored[0][0] >= 5 and (len(scored) == 1 or scored[0][0] > scored[1][0]):
                objeto = scored[0][1]
        if not objeto or len(objeto) < 40:
            continue
        nuevo = stitch(titulo, objeto)
        rep_sin += 1
        if not dry:
            conn.execute("UPDATE sil_documentos SET sinopsis=? WHERE id=?", (objeto, rid))
            if nuevo:
                conn.execute("UPDATE sil_documentos SET titulo=? WHERE id=?", (nuevo, rid))
        if nuevo:
            rep_tit += 1
        if len(ejemplos) < 3:
            ejemplos.append((sid, titulo[-60:], (nuevo or objeto)[:120]))
    if not dry:
        conn.commit()
    print(f"reparadas vía reconteo: sinopsis={rep_sin} · titulo re-unido={rep_tit}")
    for sid, cola, res in ejemplos:
        print(f"  [{sid}] …{cola!r}\n     → {res}…")
    return rep_sin


def extraer_objeto_gaceta(html_text):
    """El objeto del asunto en la página del documento de gaceta del Senado."""
    limpio = re.sub(r"<[^>]+>", " ", html_text)
    limpio = re.sub(r"\s+", " ", limpio)
    m = re.search(
        r"((?:con\s+)?(?:punto de acuerdo|proyecto de decreto|proposici[oó]n|iniciativa)"
        r"[^|]{60,2000}?)(?:\s{2,}|Sen\.|Dip\.|SINTESIS|Síntesis|VOTACI|Documento)",
        limpio, re.I)
    return m.group(1).strip() if m else None


def paso_gaceta(conn, limite, dry):
    from scrapers.gaceta_senado import fetch_page

    def url_de(sid, url):
        if "senado.gob.mx" in url:
            return url
        # PERM_<n>/SEN_<n>: el número ES el id del documento de gaceta
        m = re.match(r"(?:PERM|SEN)_(\d+)$", sid or "")
        if m:
            return f"https://www.senado.gob.mx/66/gaceta_del_senado/documento/{m.group(1)}"
        return None

    rows = [(rid, sid, t, f, tg, sin_, url_de(sid, u))
            for rid, sid, t, f, tg, sin_, u in filas_truncadas(conn)]
    rows = [r for r in rows if r[6]][:limite]
    print(f"a scrapear (lote): {len(rows)}")
    rep = fall = 0
    for rid, sid, titulo, f, tg, sin_, url in rows:
        html_text = fetch_page(url)
        time.sleep(2.5)  # rate limit Incapsula (regla del proyecto)
        if not html_text:
            fall += 1
            continue
        objeto = extraer_objeto_gaceta(html_text)
        if not objeto or len(objeto) < 60:
            fall += 1
            continue
        nuevo = stitch(titulo, objeto)
        if not dry:
            conn.execute("UPDATE sil_documentos SET sinopsis=? WHERE id=?", (objeto, rid))
            if nuevo:
                conn.execute("UPDATE sil_documentos SET titulo=? WHERE id=?", (nuevo, rid))
        rep += 1
        if rep % 50 == 0:
            print(f"  {rep} reparadas · {fall} fallidas")
            if not dry:
                conn.commit()
    if not dry:
        conn.commit()
    print(f"gaceta: reparadas={rep} · fallidas={fall}")
    return rep


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--paso", choices=["reconteo", "gaceta"], required=True)
    ap.add_argument("--limite", type=int, default=800)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    conn = sqlite3.connect(str(DB))
    if a.paso == "reconteo":
        paso_reconteo(conn, a.dry_run)
    else:
        paso_gaceta(conn, a.limite, a.dry_run)
