"""
Predictor de Autoría Legislativa - FIAT
Modelo Reactivo de Correlación: dado un evento mediático en categoría X,
¿qué legisladores son más probables de presentar una iniciativa o punto de acuerdo?

Factores (pesos):
  1. Correlación Reactiva (35%)      — ¿reacciona este legislador cuando sube el score?
  2. Especialización Temática (25%)   — ¿qué concentración tiene en este tema?
  3. Comisión + Agenda Setting (20%)  — ¿está en la comisión y hay actividad reciente?
  4. Patrón de Instrumento (10%)      — ¿tiene un patrón claro de tipo/ley?
  5. Penalización Inactividad (10%)   — ¿está activo o dormido?

TODO (Permanente exclusion): este predictor consume `actividad_legislador`
sin filtrar Comisión Permanente. Para hacerlo period-aware sin cambiar
10+ queries hay que denormalizar `camara` en actividad_legislador
(ALTER TABLE + UPDATE backfill desde sil_documentos), luego agregar
`AND camara != 'Comisión Permanente'` en los SELECT relevantes.
ELO y H2H (los score overall principales) ya lo excluyen.
"""

import logging
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CATEGORIAS
from db import get_connection

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent

# Pesos del modelo reactivo (5 factores)
PESOS = {
    "correlacion_reactiva": 0.35,
    "especializacion_tematica": 0.25,
    "comision_agenda": 0.20,
    "patron_instrumento": 0.10,
    "penalizacion_inactividad": 0.10,
}

# Mapeo: categoría → comisiones afines (substrings para matching)
COMISIONES_POR_CATEGORIA = {
    "seguridad_justicia": ["Justicia", "Seguridad", "Defensa", "Gobernación"],
    "economia_hacienda": ["Hacienda", "Economía", "Presupuesto", "Comercio"],
    "energia": ["Energía"],
    "salud": ["Salud"],
    "educacion": ["Educación"],
    "trabajo": ["Trabajo", "Previsión Social"],
    "electoral_politico": ["Gobernación", "Puntos Constitucionales", "Reforma Política"],
    "derechos_humanos": ["Derechos Humanos", "Igualdad", "Género"],
    "infraestructura": ["Infraestructura", "Comunicaciones", "Transportes", "Movilidad"],
    "agro_rural": ["Rural", "Agrícola", "Autosuficiencia", "Pesca", "Desarrollo Rural"],
    "relaciones_exteriores": ["Relaciones Exteriores", "América"],
    "anticorrupcion": ["Transparencia", "Anticorrupción"],
    "medio_ambiente": ["Medio Ambiente", "Cambio Climático", "Recursos Naturales"],
    "inteligencia_artificial": ["Ciencia", "Tecnología", "Innovación"],
    "igualdad_genero": ["Igualdad de Género", "Igualdad", "Género"],
}

# Regex para extraer nombre de ley/código de títulos legislativos
_RE_LEY = re.compile(
    r"((?:Ley\s+(?:General|Federal|Orgánica|Reglamentaria|Nacional)?\s*"
    r"(?:de(?:l)?|para|sobre|que)?\s*[\w\sáéíóúñÁÉÍÓÚÑ,]+?)"
    r"|(?:Código\s+[\w\sáéíóúñÁÉÍÓÚÑ]+?)"
    r"|(?:Constitución\s+Política[\w\sáéíóúñÁÉÍÓÚÑ]*?)"
    r"|(?:Reglamento\s+[\w\sáéíóúñÁÉÍÓÚÑ]+?))"
    r"(?=\s*[,;.\-]|\s*$|\s+y\s+|\s+en\s+|\s+del?\s+|\s+a\s+la|\s+para)",
    re.IGNORECASE,
)


def extraer_ley_de_titulo(titulo):
    """
    Extrae el nombre de la ley o código de un título legislativo.

    Ejemplos:
      "Que reforma los artículos 3° y 4° de la Constitución Política..."
        → "Constitución Política"
      "Que adiciona el artículo 28 Bis a la Ley de los Derechos de las
       Personas Adultas Mayores"
        → "Ley de los Derechos de las Personas Adultas Mayores"
    """
    if not titulo:
        return None

    # Intentar patrones explícitos primero (más confiables)
    # Cortamos en " y " para no capturar "Ley X y de la Ley Y" completo
    titulo_corto = re.split(r"\s+y\s+(?:de\s+la|del|a\s+la)", titulo, maxsplit=1)[0]

    patrones = [
        # "de la Ley ..." o "a la Ley ..." o "la Ley ..."
        r"(?:de\s+la|a\s+la|la)\s+(Ley\s+[\w\sáéíóúñÁÉÍÓÚÑ]+?)(?:\s*[,;.]|\s*$)",
        # "del Código ..."
        r"(?:del|el)\s+(Código\s+[\w\sáéíóúñÁÉÍÓÚÑ]+?)(?:\s*[,;.]|\s*$)",
        # "de la Constitución Política..."
        r"(?:de\s+la|a\s+la)\s+(Constitución\s+Política[\w\sáéíóúñÁÉÍÓÚÑ]*?)(?:\s*[,;.]|\s*$)",
        # "del Reglamento ..."
        r"(?:del|el)\s+(Reglamento\s+[\w\sáéíóúñÁÉÍÓÚÑ]+?)(?:\s*[,;.]|\s*$)",
    ]

    for patron in patrones:
        match = re.search(patron, titulo_corto, re.IGNORECASE)
        if match:
            ley = match.group(1).strip()
            # Limpiar artículos y conectores al final
            ley = re.sub(r"\s+(y|en|del?|a|para|por|con|los|las|el|la|que|se|su)\s*$",
                         "", ley, flags=re.IGNORECASE).strip()
            if len(ley) > 10:  # Mínimo razonable
                return ley

    # Fallback: buscar patrón general
    match = _RE_LEY.search(titulo)
    if match:
        ley = match.group(1).strip()
        ley = re.sub(r"\s+(y|en|del?|a|para|por|con|los|las|el|la|que|se|su)\s*$",
                     "", ley, flags=re.IGNORECASE).strip()
        if len(ley) > 10:
            return ley

    return None


def _detectar_picos_score(conn, categoria, ref_date=None):
    """
    Detecta picos en los scores de una categoría.
    Un pico = incremento >5 puntos día a día O score absoluto > 50.

    ref_date: si se da (YYYY-MM-DD), solo considera scores HASTA esa fecha
    (modo punto-en-el-tiempo para backtest sin fuga de datos).

    Returns:
        Lista de dicts: [{"fecha": "YYYY-MM-DD", "score": float, "delta": float}, ...]
    """
    rows = conn.execute("""
        SELECT fecha, score_total
        FROM scores
        WHERE categoria = ?""" + (" AND fecha <= ?" if ref_date else "") + """
        ORDER BY fecha ASC
    """, (categoria, ref_date) if ref_date else (categoria,)).fetchall()

    if not rows:
        return []

    picos = []
    prev_score = None
    for row in rows:
        fecha = row["fecha"]
        score = row["score_total"] or 0
        delta = (score - prev_score) if prev_score is not None else 0

        es_pico = delta > 5 or score > 50
        if es_pico:
            picos.append({
                "fecha": fecha[:10],  # Normalizar a YYYY-MM-DD
                "score": score,
                "delta": round(delta, 2),
            })
        prev_score = score

    return picos


def predecir_autores(categoria, top_n=10, ref_date=None):
    """
    Predice los legisladores más probables de presentar un instrumento
    en la categoría dada.

    MODELO CONSOLIDADO (jun 2026): construye sobre las MISMAS métricas del
    tab Legisladores (radar_pipeline) — no un modelo paralelo. Tres señales:

      1. Volumen ponderado en la categoría (55%) — misma ponderación que la
         "categoría dominante" del radar (iniciativa indiv 2.0, prop indiv
         1.0, iniciativa colectiva 0.6, prop colectiva 0.3). El backtest
         demostró que el volumen es la señal #1: el baseline por volumen le
         ganaba al modelo anterior de 5 factores en todos los top-K.
      2. Hit rate en la categoría (30%) — MISMA metodología del radar
         (paso_hit_rate): picos = días con score_media ≥ PICO_SCORE_MEDIA_MIN,
         últimos HITRATE_VENTANA_PICOS, ventana bidireccional de
         HITRATE_VENTANA_DIAS días centrada en el pico.
      3. Recencia (15%) — días desde su última presentación en la categoría
         (la base de proy_15d): quien presentó hace poco tiende a repetir.

    Las constantes se importan de scripts.radar_pipeline (una sola fuente de
    verdad metodológica). El modelo anterior de 5 factores (correlación
    reactiva paralela, comisión-agenda, patrón de instrumento) se ELIMINÓ:
    duplicaba el hit_rate con otra metodología y reprobó el backtest
    (top-5 2.5% vs baseline 6.5%).

    Args:
        categoria: clave de categoría (ej: 'seguridad_justicia')
        top_n: número de resultados
        ref_date: si se da (YYYY-MM-DD), usa SOLO datos hasta esa fecha
            (backtest punto-en-el-tiempo, sin fuga). None = producción.

    Returns:
        Lista de dicts con el MISMO contrato que el modelo anterior
        (legislador_id, nombre, …, score_total 0-100, desglose, metadatos).
    """
    from config import (
        FECHA_INICIO_LXVI, PICO_SCORE_MEDIA_MIN,
        HITRATE_VENTANA_PICOS, HITRATE_VENTANA_DIAS,
    )

    conn = get_connection()
    conn.row_factory = sqlite3.Row
    if categoria not in CATEGORIAS:
        return []

    # Corte temporal para backtest punto-en-el-tiempo (sin fuga).
    _fc = " AND fecha_presentacion <= ?" if ref_date else ""
    _fp = [ref_date] if ref_date else []

    # ── Señales 1 y 3: volumen ponderado + última fecha, en la categoría ──
    vol = {}        # leg_id -> volumen ponderado (pesos del radar)
    docs_cat = {}   # leg_id -> count crudo
    ultima = {}     # leg_id -> última fecha de presentación
    for row in conn.execute("""
        SELECT legislador_id,
               LOWER(COALESCE(tipo_instrumento,'')) AS tipo,
               CASE WHEN co_firmantes IS NULL OR co_firmantes = ''
                    THEN 1 ELSE 0 END AS es_indiv,
               COUNT(*) AS n, MAX(fecha_presentacion) AS fmax
        FROM actividad_legislador
        WHERE legislador_id IS NOT NULL AND categoria = ?
          AND fecha_presentacion >= ?""" + _fc + """
        GROUP BY legislador_id, tipo, es_indiv
    """, (categoria, FECHA_INICIO_LXVI, *_fp)):
        lid = row["legislador_id"]
        es_ini = "iniciativa" in row["tipo"]
        peso = (2.0 if es_ini else 1.0) if row["es_indiv"] else (0.6 if es_ini else 0.3)
        vol[lid] = vol.get(lid, 0.0) + row["n"] * peso
        docs_cat[lid] = docs_cat.get(lid, 0) + row["n"]
        if row["fmax"] and (lid not in ultima or row["fmax"] > ultima[lid]):
            ultima[lid] = row["fmax"][:10]

    if not vol:
        return []

    # ── Señal 2: hit_rate en la categoría (metodología del radar) ──
    q_picos = ("SELECT fecha FROM scores WHERE categoria = ? "
               "AND score_media >= ? AND fecha >= ?")
    p_picos = [categoria, PICO_SCORE_MEDIA_MIN, FECHA_INICIO_LXVI]
    if ref_date:
        q_picos += " AND fecha <= ?"
        p_picos.append(ref_date)
    q_picos += " ORDER BY fecha DESC"
    picos = [r["fecha"][:10] for r in conn.execute(q_picos, p_picos)][:HITRATE_VENTANA_PICOS]
    total_picos = len(picos)

    actividad_fechas = {}   # leg_id -> set de fechas con actividad en la cat
    for row in conn.execute("""
        SELECT legislador_id, fecha_presentacion FROM actividad_legislador
        WHERE legislador_id IS NOT NULL AND categoria = ?
          AND fecha_presentacion IS NOT NULL AND fecha_presentacion != ''""" + _fc + """
    """, (categoria, *_fp)):
        actividad_fechas.setdefault(row["legislador_id"], set()).add(
            row["fecha_presentacion"][:10])

    mitad = HITRATE_VENTANA_DIAS // 2
    hit_por_leg = {}
    if total_picos:
        for lid, fechas in actividad_fechas.items():
            respondio = 0
            for pico in picos:
                try:
                    d0 = datetime.strptime(pico, "%Y-%m-%d").date()
                except ValueError:
                    continue
                if any((d0 + timedelta(days=dd)).isoformat() in fechas
                       for dd in range(-mitad, HITRATE_VENTANA_DIAS - mitad + 1)):
                    respondio += 1
            hit_por_leg[lid] = respondio / total_picos

    # ── Metadatos: roster + tipos/títulos (instrumento/ley probable) ──
    roster = {r["id"]: r for r in conn.execute("""
        SELECT id, nombre, camara, partido, estado, comisiones, foto_url
        FROM legisladores WHERE nombre != ''
    """)}
    tipos_por_leg, titulos_por_leg = defaultdict(list), defaultdict(list)
    for row in conn.execute("""
        SELECT legislador_id, tipo_instrumento, titulo FROM actividad_legislador
        WHERE legislador_id IS NOT NULL AND categoria = ?""" + _fc + """
    """, (categoria, *_fp)):
        if row["tipo_instrumento"] and row["tipo_instrumento"] != "Asunto":
            tipos_por_leg[row["legislador_id"]].append(row["tipo_instrumento"])
        if row["titulo"]:
            titulos_por_leg[row["legislador_id"]].append(row["titulo"])

    comisiones_afines = COMISIONES_POR_CATEGORIA.get(categoria, [])
    hoy = datetime.strptime(ref_date, "%Y-%m-%d") if ref_date else datetime.now()
    vol_max = max(vol.values()) or 1.0

    predicciones = []
    for lid, v in vol.items():
        leg = roster.get(lid)
        if leg is None:
            continue
        s_vol = 100.0 * v / vol_max
        s_hit = 100.0 * hit_por_leg.get(lid, 0.0)
        f_ult = ultima.get(lid)
        dias = 999
        if f_ult:
            try:
                dias = (hoy - datetime.strptime(f_ult, "%Y-%m-%d")).days
            except ValueError:
                pass
        s_rec = 100.0 if dias <= 30 else 60.0 if dias <= 60 else 30.0 if dias <= 90 else 0.0

        score_total = 0.55 * s_vol + 0.30 * s_hit + 0.15 * s_rec
        if score_total <= 0:
            continue

        tipos = tipos_por_leg.get(lid, [])
        instrumento_probable = Counter(tipos).most_common(1)[0][0] if tipos else None
        leyes = [l for l in (extraer_ley_de_titulo(t) for t in titulos_por_leg.get(lid, [])) if l]
        ley_probable = Counter(leyes).most_common(1)[0][0] if leyes else None
        veces = round(hit_por_leg.get(lid, 0.0) * total_picos)
        comisiones_leg = (leg["comisiones"] or "").lower()

        predicciones.append({
            "legislador_id": lid,
            "nombre": leg["nombre"],
            "camara": leg["camara"],
            "partido": leg["partido"],
            "estado": leg["estado"],
            "foto_url": leg["foto_url"],
            "score_total": round(score_total, 2),
            "desglose": {
                "volumen_categoria": round(s_vol, 1),
                "hit_rate": round(s_hit, 1),
                "recencia": round(s_rec, 1),
            },
            "docs_en_categoria": docs_cat.get(lid, 0),
            "instrumento_probable": instrumento_probable,
            "ley_probable": ley_probable,
            "patron_narrativo": _generar_narrativa(
                nombre=leg["nombre"], partido=leg["partido"], categoria=categoria,
                veces_reaccionado=veces, total_picos=total_picos,
                instrumento_probable=instrumento_probable, ley_probable=ley_probable,
                docs_cat=docs_cat.get(lid, 0), comision_base=0,
            ),
            "correlacion_score": round(s_hit, 1),
            "veces_reaccionado": veces,
            "total_picos": total_picos,
            "comisiones_afines": [c for c in comisiones_afines if c.lower() in comisiones_leg],
        })

    predicciones.sort(key=lambda x: x["score_total"], reverse=True)
    return predicciones[:top_n]


def _generar_narrativa(nombre, partido, categoria, veces_reaccionado,
                       total_picos, instrumento_probable, ley_probable,
                       docs_cat, comision_base):
    """Genera una narrativa corta en español explicando el patrón del legislador."""
    cat_nombre = CATEGORIAS.get(categoria, {}).get("nombre", categoria)
    partes = []

    # Correlación reactiva
    if total_picos > 0 and veces_reaccionado > 0:
        pct = round((veces_reaccionado / total_picos) * 100)
        partes.append(
            f"Ha reaccionado al {pct}% de los picos mediáticos en {cat_nombre} "
            f"({veces_reaccionado} de {total_picos} veces)"
        )
    elif docs_cat > 0:
        partes.append(
            f"Tiene {docs_cat} instrumento(s) presentado(s) en {cat_nombre}"
        )

    # Instrumento y ley
    if instrumento_probable and ley_probable:
        partes.append(
            f"Suele presentar {instrumento_probable.lower()} sobre {ley_probable}"
        )
    elif instrumento_probable:
        partes.append(f"Su instrumento habitual es {instrumento_probable.lower()}")
    elif ley_probable:
        partes.append(f"Trabaja frecuentemente sobre {ley_probable}")

    # Comisión
    if comision_base >= 80:
        partes.append("Ocupa cargo directivo en comisión relevante")
    elif comision_base >= 60:
        partes.append("Es integrante de comisión afín al tema")

    if not partes:
        return f"{nombre} ({partido}) tiene actividad registrada en {cat_nombre}."

    return f"{nombre} ({partido}): " + ". ".join(partes) + "."


def calcular_reacciones_historicas():
    """
    Calcula las reacciones históricas usando los scores de categoría.
    Para cada legislador y categoría, identifica picos en el score y mide
    si el legislador presentó algo dentro de los 30 días siguientes.

    También almacena el score_delta que disparó la reacción.
    """
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # Limpiar reacciones anteriores
    conn.execute("DELETE FROM reacciones_historicas")

    # Obtener todas las categorías con scores
    categorias_con_scores = conn.execute("""
        SELECT DISTINCT categoria FROM scores
    """).fetchall()
    categorias_con_scores = [r["categoria"] for r in categorias_con_scores]

    # Pre-cargar todas las presentaciones
    todas_presentaciones = conn.execute("""
        SELECT legislador_id, categoria, fecha_presentacion, tipo_instrumento
        FROM actividad_legislador
        WHERE legislador_id IS NOT NULL AND categoria != ''
          AND fecha_presentacion IS NOT NULL AND fecha_presentacion != ''
        ORDER BY fecha_presentacion ASC
    """).fetchall()

    pres_por_cat_leg = defaultdict(lambda: defaultdict(list))
    for p in todas_presentaciones:
        pres_por_cat_leg[p["categoria"]][p["legislador_id"]].append({
            "fecha": p["fecha_presentacion"][:10],
            "tipo": p["tipo_instrumento"] or "",
        })

    batch = []

    for cat in categorias_con_scores:
        picos = _detectar_picos_score(conn, cat)
        if not picos:
            continue

        legs_en_cat = pres_por_cat_leg.get(cat, {})

        for leg_id, presentaciones in legs_en_cat.items():
            fechas_pres = [p["fecha"] for p in presentaciones]
            tipos_pres = {p["fecha"]: p["tipo"] for p in presentaciones}

            for pico in picos:
                fecha_pico = pico["fecha"]
                score_pico = pico["score"]
                delta_pico = pico["delta"]

                for fp in fechas_pres:
                    if fp >= fecha_pico:
                        try:
                            d_pico = datetime.strptime(fecha_pico, "%Y-%m-%d")
                            d_pres = datetime.strptime(fp, "%Y-%m-%d")
                            dias = (d_pres - d_pico).days
                        except ValueError:
                            continue

                        if 0 <= dias <= 90:
                            batch.append((
                                leg_id, cat, fecha_pico,
                                f"Score={score_pico:.0f}, delta={delta_pico:+.1f}",
                                fp, dias,
                                tipos_pres.get(fp, ""),
                                min(score_pico, 100),
                            ))
                        break  # Solo la primera presentación posterior

    if batch:
        conn.executemany("""
            INSERT INTO reacciones_historicas
                (legislador_id, categoria, evento_fecha,
                 evento_descripcion, presentacion_fecha,
                 dias_reaccion, tipo_instrumento, score_media_evento)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, batch)

    total_reacciones = len(batch)
    conn.commit()

    logger.info(f"Reacciones históricas calculadas: {total_reacciones}")
    return total_reacciones


def obtener_ranking_global(top_n=20):
    """
    Ranking de los legisladores más activos en todas las categorías.
    Útil para el dashboard general.
    """
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    ranking = conn.execute("""
        SELECT l.id, l.nombre, l.partido, l.camara, l.estado, l.foto_url,
               COUNT(a.id) as total_instrumentos,
               COUNT(DISTINCT a.categoria) as categorias_activas,
               MAX(a.fecha_presentacion) as ultima_actividad
        FROM legisladores l
        INNER JOIN actividad_legislador a ON a.legislador_id = l.id
        WHERE a.fecha_presentacion >= date('now', '-180 days')
        GROUP BY l.id
        ORDER BY total_instrumentos DESC
        LIMIT ?
    """, (top_n,)).fetchall()

    return [dict(r) for r in ranking]


def obtener_predicciones_para_dashboard():
    """
    Genera predicciones para todas las categorías activas.
    Retorna dict: {categoria: [top 5 legisladores probables]}
    """
    resultado = {}

    for cat_clave in CATEGORIAS:
        predicciones = predecir_autores(cat_clave, top_n=5)
        if predicciones:
            resultado[cat_clave] = predicciones

    return resultado


def obtener_estadisticas_autoria():
    """Estadísticas generales del módulo de autoría."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    stats = {}

    stats["total_legisladores"] = conn.execute(
        "SELECT COUNT(*) FROM legisladores"
    ).fetchone()[0]

    stats["con_actividad"] = conn.execute("""
        SELECT COUNT(DISTINCT legislador_id)
        FROM actividad_legislador
        WHERE legislador_id IS NOT NULL
    """).fetchone()[0]

    stats["total_actividad"] = conn.execute(
        "SELECT COUNT(*) FROM actividad_legislador"
    ).fetchone()[0]

    stats["reacciones_historicas"] = conn.execute(
        "SELECT COUNT(*) FROM reacciones_historicas"
    ).fetchone()[0]

    # Picos detectados por categoría
    picos_por_cat = {}
    for cat in CATEGORIAS:
        picos = _detectar_picos_score(conn, cat)
        if picos:
            picos_por_cat[cat] = len(picos)
    stats["picos_por_categoria"] = picos_por_cat

    # Top partidos por actividad
    stats["por_partido"] = [dict(r) for r in conn.execute("""
        SELECT l.partido, COUNT(a.id) as total
        FROM legisladores l
        INNER JOIN actividad_legislador a ON a.legislador_id = l.id
        WHERE l.partido != ''
        GROUP BY l.partido
        ORDER BY total DESC
        LIMIT 10
    """).fetchall()]

    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    print("=== Predictor de Autoría Legislativa (Modelo Reactivo) ===\n")

    for cat in ["seguridad_justicia", "economia_hacienda", "salud", "energia"]:
        print(f"\n{'='*60}")
        print(f"Categoría: {CATEGORIAS[cat]['nombre']}")
        print(f"{'='*60}")

        predicciones = predecir_autores(cat, top_n=5)
        for i, p in enumerate(predicciones, 1):
            print(f"\n  {i}. {p['nombre']} ({p['partido']}) — {p['camara']}")
            print(f"     Score: {p['score_total']:.1f}")
            print(f"     Correlación: {p['correlacion_score']:.0f}% "
                  f"({p['veces_reaccionado']}/{p['total_picos']} picos)")
            print(f"     Instrumento probable: {p['instrumento_probable'] or 'N/D'}")
            print(f"     Ley probable: {p['ley_probable'] or 'N/D'}")
            print(f"     Desglose: {p['desglose']}")
            print(f"     Narrativa: {p['patron_narrativo']}")
