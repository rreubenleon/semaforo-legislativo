"""Vinculador de producción evento→ley, ALTA CONFIANZA.

Para cada instrumento (sil_documentos): genera notas candidatas (local, $0:
ancla-entidad ∪ embedding), las ordena con el cross-encoder local, y pide a la IA
barata (Haiku) que CONFIRME si el top-k es el mismo evento. Solo se guarda el
vínculo si el juez dice SÍ → precisión ~7-8 de cada 10 (alta confianza).

Uso:
  .venv/bin/python scripts/vincular_produccion.py --limite 40           # piloto
  .venv/bin/python scripts/vincular_produccion.py --desde 2026-01-01    # completa
Salida: eval/vinculos_produccion.json  (solo vínculos confirmados)
El juez es SOLO para etiquetar/producir el lote; a futuro se destila a local.
"""
import argparse
import json
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from scripts.matcher_entidades import entidades
from scripts.matcher_evento import terms
from scripts.generar_auditoria_matcher import cargar_corpus
from collections import Counter
import sqlite3

CACHE = ROOT / "eval" / "_emb_cache"

SYS_JUEZ = (
    "Eres analista legislativo mexicano. Te doy una propuesta con punto de acuerdo "
    "y una nota de prensa previa. Decide si la propuesta RESPONDE a lo que trata la "
    "nota. SÍ cuando reacciona a un asunto público CONCRETO que la nota cubre: un "
    "caso, incidente, controversia en curso, cifra/decisión reciente, o un "
    "programa/lugar/persona específicos. NO si es solo mismo tema amplio o misma "
    "institución sin un hecho concreto compartido, o si la nota no trata el asunto. "
    "Responde SOLO: SI o NO.")


def cargar_api_key():
    for line in (ROOT / ".env").read_text().splitlines():
        if line.startswith("ANTHROPIC_API_KEY="):
            os.environ["ANTHROPIC_API_KEY"] = line.split("=", 1)[1].strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limite", type=int, default=0, help="máx instrumentos (0 = todos)")
    ap.add_argument("--desde", default="2026-01-01")
    ap.add_argument("--topk", type=int, default=2, help="candidatos a juzgar por instrumento")
    ap.add_argument("--relevantes", action="store_true",
                    help="solo iniciativas + proposiciones con PA (excluye trámite)")
    args = ap.parse_args()

    from sentence_transformers import SentenceTransformer, CrossEncoder
    from anthropic import Anthropic

    media = cargar_corpus()
    mdate = np.array([date.fromisoformat(d).toordinal() for d, _, _ in media])
    mtxt = [t for _, t, _ in media]
    mfte = [f for _, _, f in media]
    mterms = [terms(t) for t in mtxt]
    df = Counter()
    for s in mterms:
        df.update(s)

    emb = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
    cache = CACHE / f"emb_{len(media)}.npy"
    if not cache.exists():
        print("No hay cache de embeddings; genera primero la auditoría."); return
    M = np.load(cache)

    ce = CrossEncoder("cross-encoder/mmarco-mMiniLMv2-L12-H384-v1", max_length=256)

    con = sqlite3.connect(str(ROOT / "semaforo.db"))
    filtro = ""
    if args.relevantes:
        filtro = ("AND (tipo_grupo LIKE '%PA%' OR lower(tipo_grupo) LIKE '%iniciativa%' "
                  "OR lower(tipo_grupo) LIKE '%punto de acuerdo%') ")
    q = ("SELECT seguimiento_id, titulo, fecha_presentacion, presentador, tipo_grupo "
         "FROM sil_documentos WHERE fecha_presentacion >= ? AND titulo IS NOT NULL "
         "AND es_duplicado_cross_camara IS NOT 1 " + filtro + "ORDER BY fecha_presentacion DESC")
    rows = con.execute(q, (args.desde,)).fetchall()
    if args.limite:
        # muestreo espaciado para que el piloto cubra todo el periodo, no solo lo último
        paso = max(1, len(rows) // args.limite)
        rows = rows[::paso][:args.limite]
    print(f"instrumentos a procesar: {len(rows)}")

    cargar_api_key()
    cli = Anthropic()
    Qi = emb.encode([r[1] for r in rows], normalize_embeddings=True)

    vinculos = []
    tin = tout = ncalls = 0
    for k, (sid, titulo, fecha, present, tg) in enumerate(rows):
        d0 = date.fromisoformat(fecha[:10]).toordinal()
        win = np.where((mdate >= d0 - 21) & (mdate <= d0 + 3))[0]
        if not len(win):
            continue
        sims = M[win] @ Qi[k]
        cand = set(win[np.argsort(-sims)[:30]].tolist())
        E = entidades(titulo)
        Ed = {e for e in E if 0 < df.get(e, 0) < 80}
        if E:
            for i in win:
                if E & mterms[i]:
                    cand.add(int(i))
        # excluir feeds de síntesis legislativa (talla_*): cubren la propia ley,
        # no son un evento mediático externo que la detone (evita auto-referencia
        # y no gasta llamadas al juez en ellos)
        cand = [i for i in cand if "talla" not in mfte[i].lower()]
        if not cand:
            continue
        sc = ce.predict([[titulo, mtxt[i]] for i in cand])
        comb = sorted(
            ((float(sc[p]) + 2.0 * min(len(Ed & mterms[cand[p]]), 2), cand[p]) for p in range(len(cand))),
            reverse=True)
        # juzgar los topk mejores; enlazar el primero que el juez confirme
        for _, i in comb[:args.topk]:
            msg = (f"PROPUESTA [{fecha[:10]}]: {titulo}\n\n"
                   f"NOTA [{media[i][0]} {mfte[i]}]: {mtxt[i]}\n\n¿Responde? SI o NO:")
            for intento in range(4):
                try:
                    r = cli.messages.create(model="claude-haiku-4-5-20251001", max_tokens=6,
                                            system=SYS_JUEZ, messages=[{"role": "user", "content": msg}])
                    break
                except Exception:
                    if intento == 3:
                        raise
                    time.sleep(8)
            tin += r.usage.input_tokens; tout += r.usage.output_tokens; ncalls += 1
            if "SI" in r.content[0].text.upper():
                vinculos.append({"sil_id": sid, "fecha": fecha[:10], "presentador": present,
                                 "tipo_grupo": tg, "titulo": titulo, "nota_fecha": media[i][0],
                                 "nota_fuente": mfte[i], "nota_titulo": mtxt[i],
                                 "lead_dias": int(d0 - mdate[i])})
                break
            time.sleep(0.2)
        if (k + 1) % 20 == 0:
            print(f"  {k+1}/{len(rows)} · vínculos: {len(vinculos)} · llamadas: {ncalls} · ${tin/1e6+tout/1e6*5:.3f}")

    out = ROOT / "eval" / "vinculos_produccion.json"
    out.write_text(json.dumps({"generado": args.desde, "n_instrumentos": len(rows),
                               "n_vinculos": len(vinculos), "vinculos": vinculos},
                              ensure_ascii=False, indent=1))
    costo = tin / 1e6 + tout / 1e6 * 5
    print(f"\n✅ {out}")
    print(f"   {len(vinculos)} vínculos de alta confianza de {len(rows)} instrumentos "
          f"({100*len(vinculos)//max(len(rows),1)}%)")
    print(f"   llamadas al juez: {ncalls} · costo REAL: ${costo:.3f}")


if __name__ == "__main__":
    main()
