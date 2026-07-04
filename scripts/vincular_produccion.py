"""Vinculador de producción evento→ley, ALTA CONFIANZA.

Para cada instrumento (sil_documentos): genera notas candidatas (local, $0:
ancla-entidad ∪ embedding), las ordena con el cross-encoder local, y CONFIRMA
el top-k con un juez. Solo se guarda el vínculo confirmado.

Dos jueces:
  --local  → modelo destilado (eval/modelo_vinculador.joblib, entrenado con los
             5,779 veredictos del lote pagado). $0. En umbral 0.7 replica los SÍ
             del juez Haiku con ~89% de acuerdo pero encuentra ~1/3 de ellos:
             ALTA precisión, cobertura parcial — no inventa, pero no ve todo.
  (default)→ juez Haiku (~7-8 de 10 vs usuario). SOLO con OK explícito de costo
             (regla feedback_preflight_pagado): fue para el lote one-time.

Modo incremental (producción semanal, workflow vincular.yml):
  .venv/bin/python scripts/vincular_produccion.py --relevantes --local --solo-nuevos
  - procesa solo instrumentos fuera de eval/vinculados_procesados.json (ledger)
  - embeddings SOLO de las notas en las ventanas de los pendientes (rápido en CI)
  - APPEND a eval/vinculos_produccion.json + actualiza el ledger
Los vínculos alimentan REACCIÓN NAC. (reactividad_pipeline) y Hit Rate
(radar_pipeline) — ver esos scripts; aquí solo se produce el insumo.
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
import math
import sqlite3

CACHE = ROOT / "eval" / "_emb_cache"
VINCULOS = ROOT / "eval" / "vinculos_produccion.json"
LEDGER = ROOT / "eval" / "vinculados_procesados.json"
MODELO = ROOT / "eval" / "modelo_vinculador.joblib"
UMBRAL_LOCAL = 0.7  # punto de alta precisión validado (89% acuerdo con el juez)

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
    ap.add_argument("--local", action="store_true",
                    help="juez = modelo destilado local ($0) en vez de Haiku")
    ap.add_argument("--solo-nuevos", action="store_true",
                    help="solo instrumentos fuera del ledger; append + actualiza ledger")
    args = ap.parse_args()

    from sentence_transformers import SentenceTransformer, CrossEncoder

    media = cargar_corpus()
    mdate = np.array([date.fromisoformat(d).toordinal() for d, _, _ in media])
    mtxt = [t for _, t, _ in media]
    mfte = [f for _, _, f in media]
    # df/idf SIEMPRE sobre el corpus COMPLETO (las features del modelo destilado
    # se entrenaron así; un df de corpus acotado las corrompería en silencio)
    mterms = [terms(t) for t in mtxt]
    df = Counter()
    for s in mterms:
        df.update(s)
    N = len(media)
    idf = lambda x: math.log(1 + N / (1 + df.get(x, 0)))

    con = sqlite3.connect(str(ROOT / "semaforo.db"))
    filtro = ""
    if args.relevantes:
        filtro = ("AND (tipo_grupo LIKE '%PA%' OR lower(tipo_grupo) LIKE '%iniciativa%' "
                  "OR lower(tipo_grupo) LIKE '%punto de acuerdo%') ")
    q = ("SELECT seguimiento_id, titulo, fecha_presentacion, presentador, tipo_grupo "
         "FROM sil_documentos WHERE fecha_presentacion >= ? AND titulo IS NOT NULL "
         "AND es_duplicado_cross_camara IS NOT 1 " + filtro + "ORDER BY fecha_presentacion DESC")
    rows = con.execute(q, (args.desde,)).fetchall()

    ledger = set()
    if args.solo_nuevos:
        if LEDGER.exists():
            ledger = set(json.loads(LEDGER.read_text()).get("procesados", []))
        rows = [r for r in rows if r[0] not in ledger]
        print(f"instrumentos NUEVOS (fuera del ledger): {len(rows)}")
        if not rows:
            print("nada nuevo que vincular."); return
    if args.limite:
        paso = max(1, len(rows) // args.limite)
        rows = rows[::paso][:args.limite]
    print(f"instrumentos a procesar: {len(rows)}")

    emb = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
    cache = CACHE / f"emb_{len(media)}.npy"
    if cache.exists():
        M = np.load(cache)
        idx_local = None  # matriz completa
    else:
        # CI / incremental: embeber SOLO las notas en las ventanas de los pendientes
        need = set()
        for r in rows:
            d0 = date.fromisoformat(r[2][:10]).toordinal()
            need.update(np.where((mdate >= d0 - 21) & (mdate <= d0 + 3))[0].tolist())
        need = sorted(need)
        print(f"embeddings acotados a ventana: {len(need)} notas (de {N})")
        vecs = emb.encode([mtxt[i] for i in need], batch_size=256,
                          normalize_embeddings=True, show_progress_bar=False)
        idx_local = {i: k for k, i in enumerate(need)}
        M = vecs  # indexada vía idx_local

    def vec_nota(i):
        return M[i] if idx_local is None else M[idx_local[i]]

    def vecs_de(idxs):
        if idx_local is None:
            return M[idxs]
        return M[[idx_local[i] for i in idxs]]

    ce = CrossEncoder("cross-encoder/mmarco-mMiniLMv2-L12-H384-v1", max_length=256)

    modelo = None
    cli = None
    if args.local:
        import joblib
        modelo = joblib.load(MODELO)
        print(f"juez LOCAL (destilado), umbral {UMBRAL_LOCAL}")
    else:
        from anthropic import Anthropic
        cargar_api_key()
        cli = Anthropic()

    Qi = emb.encode([r[1] for r in rows], batch_size=128, normalize_embeddings=True,
                    show_progress_bar=False)

    vinculos = []
    tin = tout = ncalls = 0
    for k, (sid, titulo, fecha, present, tg) in enumerate(rows):
        d0 = date.fromisoformat(fecha[:10]).toordinal()
        win = np.where((mdate >= d0 - 21) & (mdate <= d0 + 3))[0]
        if not len(win):
            continue
        sims = vecs_de(win) @ Qi[k]
        cand = set(win[np.argsort(-sims)[:30]].tolist())
        E = entidades(titulo)
        Ed = {e for e in E if 0 < df.get(e, 0) < 80}
        if E:
            for i in win:
                if E & mterms[i]:
                    cand.add(int(i))
        # excluir feeds de síntesis legislativa (talla_*): cubren la propia ley,
        # no son un evento mediático externo que la detone
        cand = [i for i in cand if "talla" not in mfte[i].lower()]
        if not cand:
            continue
        sc = ce.predict([[titulo, mtxt[i]] for i in cand])
        comb = sorted(
            ((float(sc[p]) + 2.0 * min(len(Ed & mterms[cand[p]]), 2), float(sc[p]), cand[p])
             for p in range(len(cand))), reverse=True)
        ti = terms(titulo)
        for _, ce_raw, i in comb[:args.topk]:
            if modelo is not None:
                # features IDÉNTICAS al entrenamiento (destilar_2)
                nt = mterms[i]
                sh = ti & nt
                feats = [[ce_raw, float(vec_nota(i) @ Qi[k]), len(Ed & nt),
                          len({e for e in Ed if df.get(e, 0) < 20} & nt),
                          sum(idf(x) for x in sh), float(d0 - mdate[i]), len(sh)]]
                es_vinculo = float(modelo.predict_proba(np.array(feats))[0][1]) >= UMBRAL_LOCAL
            else:
                msg = (f"PROPUESTA [{fecha[:10]}]: {titulo}\n\n"
                       f"NOTA [{media[i][0]} {mfte[i]}]: {mtxt[i]}\n\n¿Responde? SI o NO:")
                for intento in range(4):
                    try:
                        r = cli.messages.create(model="claude-haiku-4-5-20251001", max_tokens=6,
                                                system=SYS_JUEZ,
                                                messages=[{"role": "user", "content": msg}])
                        break
                    except Exception:
                        if intento == 3:
                            raise
                        time.sleep(8)
                tin += r.usage.input_tokens; tout += r.usage.output_tokens; ncalls += 1
                es_vinculo = "SI" in r.content[0].text.upper()
                time.sleep(0.2)
            if es_vinculo:
                vinculos.append({"sil_id": sid, "fecha": fecha[:10], "presentador": present,
                                 "tipo_grupo": tg, "titulo": titulo, "nota_fecha": media[i][0],
                                 "nota_fuente": mfte[i], "nota_titulo": mtxt[i],
                                 "lead_dias": int(d0 - mdate[i]),
                                 "juez": "local" if modelo is not None else "haiku"})
                break
        if (k + 1) % 20 == 0:
            extra = f" · ${tin/1e6+tout/1e6*5:.3f}" if cli else ""
            print(f"  {k+1}/{len(rows)} · vínculos: {len(vinculos)}{extra}")

    if args.solo_nuevos:
        prev = json.loads(VINCULOS.read_text()) if VINCULOS.exists() else {"vinculos": []}
        ya_vinculados = {v["sil_id"] for v in prev["vinculos"]}
        prev["vinculos"].extend(v for v in vinculos if v["sil_id"] not in ya_vinculados)
        prev["n_vinculos"] = len(prev["vinculos"])
        VINCULOS.write_text(json.dumps(prev, ensure_ascii=False, indent=1))
        ledger |= {r[0] for r in rows}
        LEDGER.write_text(json.dumps({"procesados": sorted(ledger)}))
        print(f"\n✅ append: +{len(vinculos)} vínculos (total {prev['n_vinculos']}) · "
              f"ledger: {len(ledger)}")
    else:
        VINCULOS.write_text(json.dumps({"generado": args.desde, "n_instrumentos": len(rows),
                                        "n_vinculos": len(vinculos), "vinculos": vinculos},
                                       ensure_ascii=False, indent=1))
        print(f"\n✅ {VINCULOS}: {len(vinculos)} vínculos de {len(rows)}")
    if cli:
        print(f"   llamadas juez Haiku: {ncalls} · costo REAL: ${tin/1e6+tout/1e6*5:.3f}")


if __name__ == "__main__":
    main()
