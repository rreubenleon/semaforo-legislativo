"""Genera el HTML de auditoría del matcher evento↔instrumento (v2, corpus ARCHIVO+RSS).

Corrige el defecto del set anterior: las etiquetas estaban condicionadas a las notas
que mostró el matcher VIEJO (sobre corpus RSS, que perdía eventos). Aquí:
  - corpus = hemeroteca (archivo, La Jornada print + Proceso) ∪ articulos (RSS 22 fuentes)
  - candidatos = ancla-entidad ∪ top-K embedding, en ventana -21d..+3d
  - reranker = cross-encoder multilingüe local (gratis) que juzga par-a-par
  - muestra top-3 notas por caso para que el usuario juzgue "¿mismo evento?"
Salida: FIAT_auditoria_matcher_v2.html (Desktop) + eval/auditoria_v2_data.json
"""
import json
import sys
import re
import sqlite3
import time
from datetime import date
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from scripts.matcher_entidades import entidades
from scripts.matcher_evento import terms
from collections import Counter, defaultdict

CACHE = ROOT / "eval" / "_emb_cache"
CACHE.mkdir(exist_ok=True)


def _fix(s):
    """Best-effort para mojibake (utf-8 leído como latin1) en slugs/notas viejas."""
    if not s:
        return ""
    if "Ã" in s or "â€" in s or "Ä" in s:
        try:
            s = s.encode("latin1").decode("utf-8")
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass
    return re.sub(r"\s*\d{5,}\s*$", "", s).strip()  # quita ids de slug al final


def cargar_corpus():
    media = []
    LJ = json.loads((ROOT / "data" / "jornada_hemeroteca.json").read_text())
    PR = json.loads((ROOT / "data" / "proceso_hemeroteca.json").read_text())
    for d, secs in LJ.items():
        for sec in secs.values():
            for t in sec:
                media.append((d[:10], _fix(t), "La Jornada"))
    for d, slugs in PR.items():
        for s in slugs:
            media.append((d[:10], _fix(s.replace("-", " ")), "Proceso"))
    con = sqlite3.connect(str(ROOT / "semaforo.db"))
    for f, t, r in con.execute(
        "SELECT substr(fecha,1,10), titulo, fuente FROM articulos "
        "WHERE fecha>='2025-08-01' AND titulo IS NOT NULL"):
        media.append((f, _fix((t or "").strip()), (r or "medio").replace("_", " ")))
    return media


def main():
    from sentence_transformers import SentenceTransformer, CrossEncoder
    ev = json.loads((ROOT / "eval" / "matcher_eval_set.json").read_text())["casos"]
    media = cargar_corpus()
    mdate = np.array([date.fromisoformat(d).toordinal() for d, _, _ in media])
    mtxt = [t for _, t, _ in media]
    print(f"corpus combinado: {len(media)} notas (archivo + RSS)")

    emb = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
    cache = CACHE / f"emb_{len(media)}.npy"
    if cache.exists():
        M = np.load(cache)
    else:
        t0 = time.time()
        M = emb.encode(mtxt, batch_size=256, normalize_embeddings=True, show_progress_bar=True)
        np.save(cache, M)
        print(f"embed corpus en {time.time()-t0:.0f}s")

    # términos de cada nota (para intersección por entidad, sin corte df:
    # un nombre propio distintivo lo es aunque sea frecuente, p.ej. 'segalmex')
    mterms = [terms(t) for t in mtxt]
    df = Counter()
    for s in mterms:
        df.update(s)

    ce = CrossEncoder("cross-encoder/mmarco-mMiniLMv2-L12-H384-v1", max_length=256)
    Q = emb.encode([c["titulo"] for c in ev], normalize_embeddings=True)

    # etiqueta previa del usuario (para mostrarla como pista): v2 si existe, si no v1
    prev_lab = {}
    v2 = Path.home() / "Desktop" / "fiat_matcher_labels_v2.json"
    if v2.exists():
        prev_lab = {c["id"]: c.get("etiqueta") for c in json.loads(v2.read_text())["casos"]}

    casos_out = []
    for k, c in enumerate(ev):
        d0 = date.fromisoformat(c["fecha"][:10]).toordinal()
        win = np.where((mdate >= d0 - 21) & (mdate <= d0 + 3))[0]
        cand = set()
        if len(win):
            sims = M[win] @ Q[k]
            cand.update(win[np.argsort(-sims)[:30]].tolist())
        # candidatos por entidad: intersección directa sobre las notas de la ventana
        E = entidades(c["titulo"])
        if E:
            for i in win:
                if E & mterms[i]:
                    cand.add(int(i))
        cand = list(cand)
        notas = []
        if cand:
            sc = ce.predict([[c["titulo"], mtxt[i]] for i in cand])
            # score combinado: CE + bonus por compartir ENTIDAD distintiva (df<80).
            # No entrenado aún; sube notas con nombre propio compartido (GM, SEGALMEX).
            E_dist = {e for e in entidades(c["titulo"]) if 0 < df.get(e, 0) < 80}
            comb = []
            for pos, i in enumerate(cand):
                bonus = sum(1 for x in (E_dist & mterms[i]))
                comb.append((float(sc[pos]) + 2.0 * min(bonus, 2), float(sc[pos]), i))
            comb.sort(reverse=True)
            vistos = set()
            for total, ce_raw, i in comb:
                key = mtxt[i][:40].lower()
                if key in vistos:
                    continue
                vistos.add(key)
                notas.append({"fecha": media[i][0], "fuente": media[i][2],
                              "texto": mtxt[i], "ce": round(ce_raw, 2),
                              "score": round(total, 2)})
                if len(notas) >= 8:
                    break
        casos_out.append({"id": c["id"], "stratum": c.get("stratum", ""),
                          "lid": c.get("lid", ""), "fecha": c["fecha"],
                          "titulo": c["titulo"],
                          "marca_previa": prev_lab.get(c["id"], c["verdad"]),
                          "notas": notas})

    (ROOT / "eval" / "auditoria_v2_data.json").write_text(
        json.dumps(casos_out, ensure_ascii=False, indent=1))
    html = construir_html(casos_out)
    out = Path.home() / "Desktop" / "FIAT_auditoria_por_nota.html"
    out.write_text(html, encoding="utf-8")
    print(f"\n✅ {out}")
    print(f"   {len(casos_out)} casos; con nota candidata: "
          f"{sum(1 for c in casos_out if c['notas'])}")


def construir_html(casos):
    data = json.dumps(casos, ensure_ascii=False)
    return """<!doctype html><html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>FIAT · Auditoría del matcher v2</title>
<style>
:root{--verde:#16a34a;--rojo:#dc2626;--gris:#6b7280;--bg:#f8fafc;--card:#fff;--linea:#e5e7eb}
*{box-sizing:border-box}body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:0;background:var(--bg);color:#111827;line-height:1.5}
header{position:sticky;top:0;background:#0f172a;color:#fff;padding:14px 20px;z-index:10;box-shadow:0 2px 8px rgba(0,0,0,.15)}
header h1{margin:0;font-size:16px}header .prog{font-size:13px;color:#cbd5e1;margin-top:4px}
.wrap{max-width:900px;margin:0 auto;padding:20px}
.card{background:var(--card);border:1px solid var(--linea);border-radius:12px;padding:18px;margin-bottom:16px;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.meta{font-size:12px;color:var(--gris);margin-bottom:6px}
.inst{font-size:15px;font-weight:600;margin-bottom:14px}
.nota{display:flex;gap:10px;align-items:flex-start;background:#f1f5f9;border-radius:8px;padding:10px 12px;margin:8px 0;font-size:14px;cursor:pointer;border:2px solid transparent}
.nota:hover{background:#e8eef6}
.nota.sel{border-color:var(--verde);background:#ecfdf5}
.nota .bd{flex:1}
.nota .nm{font-size:11px;color:var(--gris);margin-bottom:2px}
.nota input{margin-top:3px;width:17px;height:17px;flex:none}
.ninguna{display:inline-block;margin-top:12px;border:2px solid var(--rojo);color:var(--rojo);background:#fff;border-radius:8px;padding:9px 16px;font-size:14px;font-weight:600;cursor:pointer}
.ninguna.sel{background:var(--rojo);color:#fff}
.pill{display:inline-block;font-size:11px;padding:2px 8px;border-radius:999px;background:#e2e8f0;color:#475569;margin-left:6px}
.pill.si{background:#dcfce7;color:#166534}.pill.no{background:#fee2e2;color:#991b1b}
.done{border-color:var(--verde)}
#export{position:fixed;bottom:20px;right:20px;background:#0f172a;color:#fff;border:0;border-radius:10px;padding:14px 20px;font-size:14px;font-weight:600;cursor:pointer;box-shadow:0 4px 12px rgba(0,0,0,.25)}
.nohay{color:var(--gris);font-style:italic;font-size:14px}
.q{font-size:13px;color:#334155;margin:6px 0;font-weight:600}
</style></head><body>
<header><h1>FIAT · Auditoría por nota (marca CUÁL aplica, o Ninguna)</h1>
<div class="prog" id="prog">0 / 0 revisados</div></header>
<div class="wrap" id="app"></div>
<button id="export" onclick="exportar()">⬇︎ Exportar etiquetas</button>
<script>
const CASOS = __DATA__;
const KEY = "fiat_audit_v3";
// marcas[id] = {sel:[indices de notas que aplican], ninguna:bool}
let marcas = JSON.parse(localStorage.getItem(KEY) || "{}");
function get(id){return marcas[id]||{sel:[],ninguna:false};}
function toggleNota(id,i){
  const m=get(id); m.ninguna=false;
  const p=m.sel.indexOf(i); if(p>=0)m.sel.splice(p,1); else m.sel.push(i);
  marcas[id]=m; localStorage.setItem(KEY,JSON.stringify(marcas)); render();
}
function marcarNinguna(id){
  const m=get(id); m.sel=[]; m.ninguna=!m.ninguna;
  marcas[id]=m; localStorage.setItem(KEY,JSON.stringify(marcas)); render();
}
function revisado(m){return m && (m.sel.length>0 || m.ninguna);}
function prog(){const n=CASOS.filter(c=>revisado(marcas[c.id])).length;
  document.getElementById("prog").textContent=n+" / "+CASOS.length+" revisados";}
function render(){
  const app=document.getElementById("app");app.innerHTML="";
  CASOS.forEach(c=>{
    const m=get(c.id);
    const d=document.createElement("div");d.className="card"+(revisado(m)?" done":"");
    let notas = c.notas.length ? c.notas.map((n,i)=>
      `<label class="nota ${m.sel.includes(i)?'sel':''}">
         <input type="checkbox" ${m.sel.includes(i)?'checked':''} onchange="toggleNota('${c.id}',${i})">
         <div class="bd"><div class="nm">${n.fecha} · ${n.fuente}</div>${n.texto}</div></label>`).join("")
      : `<div class="nohay">El matcher no encontró ninguna nota candidata en la ventana.</div>`;
    const prev = c.marca_previa ? `<span class="pill ${c.marca_previa}">antes: ${c.marca_previa.toUpperCase()}</span>` : "";
    d.innerHTML=`<div class="meta">${c.id} · ${c.stratum} · legislador ${c.lid} · instrumento ${c.fecha} ${prev}</div>
      <div class="inst">${c.titulo}</div>
      <div class="q">Marca la(s) nota(s) que sean el MISMO evento que responde el instrumento. Si ninguna aplica, pulsa «Ninguna».</div>
      ${notas}
      <div><span class="ninguna ${m.ninguna?'sel':''}" onclick="marcarNinguna('${c.id}')">Ninguna aplica</span></div>`;
    app.appendChild(d);
  });
  prog();
}
function exportar(){
  const out={etiquetador:"usuario",version:"v3_por_nota",casos:CASOS.map(c=>{
    const m=get(c.id);
    return {id:c.id,stratum:c.stratum,lid:c.lid,fecha:c.fecha,titulo:c.titulo,
      ninguna_aplica:m.ninguna, notas_correctas:m.sel,
      notas:c.notas.map((n,i)=>({...n, correcta:m.sel.includes(i)}))};
  })};
  const blob=new Blob([JSON.stringify(out,null,1)],{type:"application/json"});
  const a=document.createElement("a");a.href=URL.createObjectURL(blob);a.download="fiat_matcher_labels_v3.json";a.click();
}
render();
</script></body></html>""".replace("__DATA__", data)


if __name__ == "__main__":
    main()
