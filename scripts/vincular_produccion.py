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
from scripts.sanar_titulos_truncados import contaminada, na
from scripts.nucleo_titulos import key12, ctoks, apellidos, jaccard
from collections import Counter
import math
import sqlite3

CACHE = ROOT / "eval" / "_emb_cache"
CORPUS_REGIONAL = ROOT / "eval" / "_corpus_regional"  # shards de fiat-corpus (gh release download)


def cargar_corpus_regional():
    """Notas regionales desde los shards descargados de fiat-corpus.
    Devuelve lista de (fecha, titulo, fuente, entidad, resumen, url). Vacía si
    no hay shards (el vinculador funciona igual, solo con corpus nacional)."""
    import gzip, glob
    out = []
    for f in sorted(glob.glob(str(CORPUS_REGIONAL / "corpus-*.jsonl.gz"))):
        with gzip.open(f, "rt", encoding="utf-8") as fh:
            for line in fh:
                try:
                    n = json.loads(line)
                    out.append((n["fecha"], n["titulo"], n["medio"], n["entidad"],
                                (n.get("resumen") or "").strip(), n.get("url") or ""))
                except Exception:
                    continue
    return out
VINCULOS = ROOT / "eval" / "vinculos_produccion.json"
LEDGER = ROOT / "eval" / "vinculados_procesados.json"
MODELO = ROOT / "eval" / "modelo_vinculador.joblib"
UMBRAL_LOCAL = 0.7  # punto de alta precisión validado (89% acuerdo con el juez)

import re as _re
_PREFIJO_AUTORES = _re.compile(
    r"^.{0,600}?(con\s+(?:punto de acuerdo|proyecto de decreto|proposici[oó]n)|por el que|que\s+(?:reforma|adiciona|expide|deroga))",
    _re.I | _re.S)


def texto_instrumento(titulo, sinopsis):
    """Texto COMPLETO del instrumento para matching y juicio: quita el prefijo
    de autores (que hacía anclar por NOMBRES de legisladores — causa de la
    cuarentena jul-2026) y agrega la sinopsis, que trae el objeto íntegro."""
    t = titulo or ""
    m = _PREFIJO_AUTORES.search(t)
    if m and m.start(1) > 40:
        t = t[m.start(1):]
    sin = (sinopsis or "").strip()
    return (t + (" " + sin if sin else "")).strip()

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
    ap.add_argument("--sin-vinculo", action="store_true",
                    help="re-pase: solo instrumentos YA procesados que quedaron sin vínculo "
                         "(útil tras ampliar el corpus); append dedupeado")
    args = ap.parse_args()

    from sentence_transformers import SentenceTransformer, CrossEncoder

    media6 = cargar_corpus(con_resumen=True)
    media = [m[:3] for m in media6]
    mres = [m[3] for m in media6]   # cuerpo/resumen (para el juez; "" en hemeroteca)
    morig = [m[4] for m in media6]  # trazabilidad al corpus canónico
    murl = [m[5] for m in media6]
    ment = [None] * len(media)  # entidad de la nota (solo regionales)
    reg = cargar_corpus_regional()
    if reg:
        print(f"corpus regional: +{len(reg)} notas (fiat-corpus)")
        for fch, tit, med, ent, res, u in reg:
            media.append((fch, tit, med))
            ment.append(ent)
            mres.append(res)
            morig.append("regional")
            murl.append(u)
    mdate = np.array([date.fromisoformat(d[:10]).toordinal() for d, _, _ in media])
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

    con = sqlite3.connect(str(os.environ.get("SANAR_DB", ROOT / "semaforo.db")))
    from scripts.leer_documentos_oficiales import ensure_cols
    ensure_cols(con)
    filtro = ""
    if args.relevantes:
        filtro = ("AND (tipo_grupo LIKE '%PA%' OR lower(tipo_grupo) LIKE '%iniciativa%' "
                  "OR lower(tipo_grupo) LIKE '%punto de acuerdo%') ")
    q = ("SELECT seguimiento_id, titulo, fecha_presentacion, presentador, tipo_grupo, "
         "COALESCE(sinopsis,''), COALESCE(fecha_presentacion_real,''), COALESCE(url,'') "
         "FROM sil_documentos WHERE fecha_presentacion >= ? AND titulo IS NOT NULL "
         "AND es_duplicado_cross_camara IS NOT 1 " + filtro + "ORDER BY fecha_presentacion DESC")
    rows = con.execute(q, (args.desde,)).fetchall()

    # ── índice de SERIALES (gate v3): la MISMA propuesta re-presentada antes
    # (etapas del propio asunto o re-presentación del autor) invalida el
    # "responde a la nota" — la fecha que manda es la PRIMERA aparición.
    # Incluye PLANTILLAS de autor ("exhorta a diversas autoridades federales y
    # estatales en relación con…"): el gate v4 encontró un falso serial por
    # boilerplate reusado en temas distintos — el Jaccard debe correr sobre
    # tokens de ASUNTO, no de plantilla.
    _MARCO = {"reforma", "reforman", "adiciona", "adicionan", "deroga", "expide",
              "modifica", "articulo", "articulos", "ley", "leyes", "codigo",
              "general", "federal", "federales", "constitucion", "politica",
              "estados", "unidos", "mexicanos", "diversas", "disposiciones",
              "materia", "decreto", "proyecto", "punto", "acuerdo", "exhorta",
              "exhorto", "respetuosamente", "secretaria", "gobierno", "camara",
              "senado", "congreso", "republica", "nacional", "fraccion",
              "parrafo", "bis", "ter", "autoridades", "estatales",
              "municipales", "relacion", "informe", "informar", "solicita",
              "solicitud", "urgente", "obvia", "resolucion", "tramite",
              "competentes", "correspondientes", "ambitos", "acciones",
              "medidas", "implementar", "garantizar", "fortalecer"}
    _serial_todos = []
    for _t, _f, _a in con.execute(
            "SELECT titulo, substr(fecha_presentacion,1,10), COALESCE(presentador,'') "
            "FROM sil_documentos WHERE titulo IS NOT NULL AND fecha_presentacion IS NOT NULL"):
        _serial_todos.append((_f, key12(_t), frozenset(ctoks(_t)), apellidos(_a, _t)))
    for _f, _t, _a in con.execute(
            "SELECT substr(fecha,1,10), titulo, COALESCE(autor,'') FROM gaceta "
            "WHERE titulo IS NOT NULL AND fecha IS NOT NULL "
            "AND tipo IN ('iniciativa','proposicion','minuta')"):
        _serial_todos.append((_f, key12(_t), frozenset(ctoks(_t)), apellidos(_a, _t)))
    _por_key, _por_ap = {}, {}
    for _i, (_f, _k, _ct, _ap) in enumerate(_serial_todos):
        if _k:
            _por_key.setdefault(_k, []).append((_f, _ap))
        for _a in _ap:
            _por_ap.setdefault(_a, []).append(_i)

    def fecha_primera(titulo, pres, f_ef):
        """Primera aparición de la misma propuesta (guard de autor medido:
        apellidos∩≥2, o clave ≥8 tokens si un lado no trae autor)."""
        k = key12(titulo)
        ap = apellidos(pres, titulo)
        best = f_ef
        ntok = len(k.split()) if k else 0
        for fm, apm in _por_key.get(k, []):
            if fm < best and (len(ap & apm) >= 2
                              or ((not ap or not apm) and ntok >= 8)):
                best = fm
        if ap:  # T2: re-presentación con redacción distinta (Jaccard + especificidad)
            ct = frozenset(ctoks(titulo))
            espec = ct - _MARCO
            cand = set()
            for a in ap:
                cand.update(_por_ap.get(a, ()))
            for i in cand:
                fm, km, ctm, apm = _serial_todos[i]
                if fm >= best or km == k:
                    continue
                if (len(ap & apm) >= 2 and jaccard(ct, ctm) >= 0.7
                        and len(espec & ctm) >= 4):
                    best = fm
        return best

    ledger = set()
    if args.sin_vinculo:
        ya_vinc = set()
        if VINCULOS.exists():
            ya_vinc = {v["sil_id"] for v in json.loads(VINCULOS.read_text())["vinculos"]}
        rows = [r for r in rows if r[0] not in ya_vinc]
        print(f"re-pase sin-vínculo: {len(rows)} instrumentos")
    if args.solo_nuevos:
        if LEDGER.exists():
            ledger = set(json.loads(LEDGER.read_text()).get("procesados", []))
        rows = [r for r in rows if r[0] not in ledger]
        print(f"instrumentos NUEVOS (fuera del ledger): {len(rows)}")
        if not rows:
            print("nada nuevo que vincular."); return
    # EFEMÉRIDES fuera (Día Mundial/Internacional/Nacional): empatan con
    # cualquier nota del día — no son respuesta a evento (gate Escéptico 11-jul)
    _EFE = _re.compile(r"d[ií]a (mundial|internacional|nacional)\s", _re.I)
    antes = len(rows)
    rows = [r for r in rows if not _EFE.search(r[1] or "")]
    if antes - len(rows):
        print(f"efemérides excluidas: {antes - len(rows)}")
    # DEDUPE de gemelos (mismo instrumento con dos sil_id): 464 en el lote viejo
    _vistos = set(); _unicos = []
    for r in rows:
        k = ((r[2] or "")[:10], (r[1] or "")[:150])
        if k in _vistos:
            continue
        _vistos.add(k); _unicos.append(r)
    if len(rows) - len(_unicos):
        print(f"gemelos dedupeados: {len(rows) - len(_unicos)}")
    rows = _unicos
    if args.limite:
        paso = max(1, len(rows) // args.limite)
        rows = rows[::paso][:args.limite]
    print(f"instrumentos a procesar: {len(rows)}")

    # ── fecha EFECTIVA por instrumento (gate v3): fecha real del documento si
    # existe; si la ingesta fue una vista de etapa (cuadro de comisión SITL,
    # registro de turno en gaceta) y no hay fecha real → ABSTENCIÓN; y en
    # todos los casos, la primera aparición serial manda.
    # "Se dio turno DIRECTO" = presentado y turnado en la MISMA sesión → la
    # fecha de sesión SÍ es la de presentación (canarios Buró/Tijuana, 13-jul).
    # Solo es vista de etapa la remisión posterior.
    _TRAMITE = _re.compile(r"se turn[oó]\s+(?:a|para)|se remiti[oó]", _re.I)
    fechas_ef = []
    abst_etapa = 0
    for r in rows:
        _freal = (r[6] if len(r) > 6 else "") or ""
        _url_i = (r[7] if len(r) > 7 else "") or ""
        if not _freal and ("cuadro_asuntos_por_comision" in _url_i
                           or _TRAMITE.search(r[1] or "")):
            fechas_ef.append(None)
            abst_etapa += 1
            continue
        fechas_ef.append(fecha_primera(r[1], r[3], (_freal or r[2])[:10]))
    if abst_etapa:
        print(f"abstención por fecha de etapa sin resolver: {abst_etapa}")

    emb = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
    cache = CACHE / f"emb_{len(media)}.npy"
    if cache.exists():
        M = np.load(cache)
        idx_local = None  # matriz completa
    else:
        # CI / incremental: embeber SOLO las notas en las ventanas de los pendientes
        need = set()
        for k, r in enumerate(rows):
            if fechas_ef[k] is None:
                continue
            d0 = date.fromisoformat(fechas_ef[k][:10]).toordinal()
            need.update(np.where((mdate >= d0 - 21) & (mdate < d0))[0].tolist())
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
        # VETO durante la cuarentena: el destilado se entrenó con veredictos
        # emitidos sobre texto contaminado (gate v2) — re-destilar con
        # veredictos v3 y re-validar antes de volver a usarlo como juez.
        if (ROOT / "eval" / "VINCULOS_EN_CUARENTENA").exists():
            print("juez local VETADO durante la cuarentena de vínculos "
                  "(re-destilar tras el gate v3). No se corre; salida limpia.")
            return
        import joblib
        modelo = joblib.load(MODELO)
        print(f"juez LOCAL (destilado), umbral {UMBRAL_LOCAL}")
    else:
        from anthropic import Anthropic
        cargar_api_key()
        cli = Anthropic()

    textos = [texto_instrumento(r[1], r[5] if len(r) > 5 else "") for r in rows]
    Qi = emb.encode(textos, batch_size=128, normalize_embeddings=True,
                    show_progress_bar=False)

    vinculos = []
    tin = tout = ncalls = 0
    for k, fila in enumerate(rows):
        sid, titulo, fecha, present, tg = fila[0], fila[1], fila[2], fila[3], fila[4]
        # RECHAZO DE OFICIO (gates Escéptico 11/12-jul): si el título sigue
        # mocho y la sinopsis trae firma de contaminación (detección por
        # PREFIJO — la substring dejaba pasar 1,836), el instrumento NO se
        # juzga — abstención honesta hasta sanarlo.
        _sin = fila[5] if len(fila) > 5 else ""
        if len(fila[1] or "") in (199, 200, 499, 500) and contaminada(
                fila[1], _sin, present):
            continue
        titulo = textos[k]  # texto COMPLETO (sin prefijo de autores, con sinopsis)
        # Ventana [-21d, -1d] alrededor de la fecha EFECTIVA (real del documento
        # + primera aparición serial). Gate v4: lead 0 quedó FUERA — el estrato
        # mismo-día era auto-cobertura (la nota reporta el propio instrumento)
        # o paralelismo del mismo partido; un vínculo evento→respuesta exige
        # que la nota preceda al instrumento al menos un día.
        if fechas_ef[k] is None:
            continue
        d0 = date.fromisoformat(fechas_ef[k][:10]).toordinal()
        win = np.where((mdate >= d0 - 21) & (mdate < d0))[0]
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
                # CUERPO de la nota al juez: los titulares genéricos tumbaron
                # el gate v2 (Caracas≠Aleppo, derrame≠desapariciones). Si no
                # hay cuerpo (hemeroteca), el juez lo sabe y es estricto.
                _cuerpo = (mres[i] or "")[:700]
                _bloq = (f"\nCUERPO: {_cuerpo}" if _cuerpo else
                         "\n(SIN CUERPO: solo tienes el titular. Sé estricto: "
                         "si el titular es genérico o ambiguo, responde NO.)")
                msg = (f"PROPUESTA [{fecha[:10]}]: {titulo}\n\n"
                       f"NOTA [{media[i][0]} {mfte[i]}]: {mtxt[i]}{_bloq}\n\n"
                       "¿Responde? SI o NO:")
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
                # tipo_nota: la nota ¿cubre un evento EXTERNO o el propio proceso
                # legislativo? Además del léxico legislativo, es CIRCULAR
                # (cobertura del propio instrumento) si es mismo día y la nota
                # nombra al autor o arranca con verbo de proposición en 3ª
                # (gate v3: 4/4 detectados, 0 falsos en 47 CUADRA).
                _PROC = _re.compile(
                    r"\b(iniciativa|reforma[sn]?|congreso|senado|diputad|"
                    r"comisi[oó]n permanente|c[aá]mara|dictamen|pleno|legislador)\b", _re.I)
                _lead = int(d0 - mdate[i])
                _nota_na = na((mtxt[i] or "") + " " + (mres[i] or "")[:300])
                _aps = apellidos(present, fila[1])
                _circular = _lead == 0 and (
                    any(a in _nota_na for a in _aps)
                    or _re.match(r"^\s*(pide|exhorta|urge|propone|plantea|"
                                 r"presenta|busca|va por)\b", mtxt[i] or "", _re.I))
                vinculos.append({"sil_id": sid, "fecha": fecha[:10],
                                 "fecha_efectiva": fechas_ef[k], "presentador": present,
                                 "tipo_grupo": tg, "titulo": titulo, "nota_fecha": media[i][0],
                                 "nota_fuente": mfte[i], "nota_titulo": mtxt[i],
                                 "lead_dias": _lead,
                                 "mismo_dia": _lead == 0,
                                 "nota_entidad": ment[i],
                                 "nota_origen": morig[i], "nota_url": murl[i],
                                 "sin_cuerpo": not (mres[i] or "").strip(),
                                 "juez": "local" if modelo is not None else "haiku",
                                 "tipo_nota": "proceso" if (_circular or _PROC.search(mtxt[i]))
                                 else "externo"})
                break
        if (k + 1) % 20 == 0:
            extra = f" · ${tin/1e6+tout/1e6*5:.3f}" if cli else ""
            print(f"  {k+1}/{len(rows)} · vínculos: {len(vinculos)}{extra}")

    if args.solo_nuevos or args.sin_vinculo:
        prev = json.loads(VINCULOS.read_text()) if VINCULOS.exists() else {"vinculos": []}
        ya_vinculados = {v["sil_id"] for v in prev["vinculos"]}
        prev["vinculos"].extend(v for v in vinculos if v["sil_id"] not in ya_vinculados)
        prev["n_vinculos"] = len(prev["vinculos"])
        VINCULOS.write_text(json.dumps(prev, ensure_ascii=False, indent=1))
        if args.solo_nuevos:
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
