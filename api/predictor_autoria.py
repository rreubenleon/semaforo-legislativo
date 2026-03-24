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


def _detectar_picos_score(conn, categoria):
    """
    Detecta picos en los scores de una categoría.
    Un pico = incremento >5 puntos día a día O score absoluto > 50.

    Returns:
        Lista de dicts: [{"fecha": "YYYY-MM-DD", "score": float, "delta": float}, ...]
    """
    rows = conn.execute("""
        SELECT fecha, score_total
        FROM scores
        WHERE categoria = ?
        ORDER BY fecha ASC
    """, (categoria,)).fetchall()

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


def predecir_autores(categoria, top_n=10):
    """
    Predice los legisladores más probables de presentar un instrumento
    legislativo sobre la categoría dada, usando el modelo reactivo de
    correlación.

    Args:
        categoria: clave de categoría (ej: 'seguridad_justicia')
        top_n: número de resultados a retornar

    Returns:
        Lista de dicts con legislador, score, desglose y metadatos enriquecidos
    """
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    if categoria not in CATEGORIAS:
        return []

    # ══════════════════════════════════════════════════════════════
    # CARGA BULK DE DATOS (evitar N+1)
    # ══════════════════════════════════════════════════════════════

    # Legisladores
    legisladores = conn.execute("""
        SELECT id, nombre, camara, partido, estado, distrito,
               comisiones, comisiones_cargo, foto_url
        FROM legisladores
        WHERE nombre != ''
    """).fetchall()

    if not legisladores:
        return []

    # --- Factor 1: datos para correlación reactiva ---
    picos = _detectar_picos_score(conn, categoria)
    total_picos = len(picos)

    # Presentaciones por legislador en esta categoría con fecha
    presentaciones_cat = conn.execute("""
        SELECT legislador_id, fecha_presentacion
        FROM actividad_legislador
        WHERE categoria = ?
          AND legislador_id IS NOT NULL
          AND fecha_presentacion IS NOT NULL AND fecha_presentacion != ''
        ORDER BY fecha_presentacion ASC
    """, (categoria,)).fetchall()

    # Agrupar por legislador
    pres_por_leg = defaultdict(list)
    for row in presentaciones_cat:
        pres_por_leg[row["legislador_id"]].append(row["fecha_presentacion"][:10])

    # Pre-calcular: por cada legislador, cuántos picos "respondió"
    # (presentó algo dentro de 30 días después del pico)
    reacciones_por_leg = {}
    if total_picos > 0:
        for leg_id, fechas_pres in pres_por_leg.items():
            veces = 0
            for pico in picos:
                fecha_pico = pico["fecha"]
                # Buscar si presentó algo en los 30 días siguientes
                for fp in fechas_pres:
                    if fp >= fecha_pico:
                        try:
                            d_pico = datetime.strptime(fecha_pico, "%Y-%m-%d")
                            d_pres = datetime.strptime(fp, "%Y-%m-%d")
                            dias = (d_pres - d_pico).days
                        except ValueError:
                            continue
                        if 0 <= dias <= 30:
                            veces += 1
                            break  # Solo cuenta una vez por pico
                        elif dias > 30:
                            break  # Ya pasó la ventana, siguiente pico
            reacciones_por_leg[leg_id] = veces

    # --- Factor 2: datos para especialización temática ---
    docs_en_cat_por_leg = {}
    for row in conn.execute("""
        SELECT legislador_id, COUNT(*) as cnt
        FROM actividad_legislador
        WHERE categoria = ?
        GROUP BY legislador_id
    """, (categoria,)).fetchall():
        docs_en_cat_por_leg[row["legislador_id"]] = row["cnt"]

    docs_total_por_leg = {}
    for row in conn.execute("""
        SELECT legislador_id, COUNT(*) as cnt
        FROM actividad_legislador
        WHERE legislador_id IS NOT NULL
        GROUP BY legislador_id
    """).fetchall():
        docs_total_por_leg[row["legislador_id"]] = row["cnt"]

    # --- Factor 3: datos para comisión + agenda setting ---
    comisiones_afines = COMISIONES_POR_CATEGORIA.get(categoria, [])

    # Actividad reciente de comisiones en Gaceta
    comisiones_con_actividad = set()
    if comisiones_afines:
        try:
            eventos_gaceta = conn.execute("""
                SELECT comision FROM gaceta
                WHERE fecha >= date('now', '-30 days')
                  AND comision != 'No especificada'
            """).fetchall()
            for ev in eventos_gaceta:
                com_gaceta = (ev["comision"] or "").upper()
                for com_afin in comisiones_afines:
                    if com_afin.upper() in com_gaceta:
                        comisiones_con_actividad.add(com_afin)
        except Exception:
            pass

    # --- Factor 4: datos para patrón de instrumento ---
    # tipo_instrumento y titulo por legislador en esta categoría
    instrumentos_por_leg = defaultdict(list)
    titulos_por_leg = defaultdict(list)
    for row in conn.execute("""
        SELECT legislador_id, tipo_instrumento, titulo
        FROM actividad_legislador
        WHERE categoria = ?
          AND legislador_id IS NOT NULL
    """, (categoria,)).fetchall():
        if row["tipo_instrumento"]:
            instrumentos_por_leg[row["legislador_id"]].append(row["tipo_instrumento"])
        if row["titulo"]:
            titulos_por_leg[row["legislador_id"]].append(row["titulo"])

    # --- Factor 5: última actividad por legislador ---
    ultima_actividad_por_leg = {}
    for row in conn.execute("""
        SELECT legislador_id, MAX(fecha_presentacion) as ultima
        FROM actividad_legislador
        WHERE legislador_id IS NOT NULL
          AND fecha_presentacion IS NOT NULL AND fecha_presentacion != ''
        GROUP BY legislador_id
    """).fetchall():
        ultima_actividad_por_leg[row["legislador_id"]] = row["ultima"][:10]

    hoy = datetime.now()

    # ══════════════════════════════════════════════════════════════
    # CÁLCULO DE SCORES POR LEGISLADOR
    # ══════════════════════════════════════════════════════════════

    predicciones = []

    for leg in legisladores:
        leg_id = leg["id"]
        scores = {}

        # ── Factor 1: Correlación Reactiva (35%) ──
        if total_picos > 0:
            veces = reacciones_por_leg.get(leg_id, 0)
            scores["correlacion_reactiva"] = (veces / total_picos) * 100
        else:
            # Sin picos detectados, usar presencia en categoría como proxy
            docs_cat = docs_en_cat_por_leg.get(leg_id, 0)
            scores["correlacion_reactiva"] = min(docs_cat * 10, 50)

        # ── Factor 2: Especialización Temática (25%) ──
        docs_cat = docs_en_cat_por_leg.get(leg_id, 0)
        docs_total = docs_total_por_leg.get(leg_id, 0)

        if docs_cat >= 3 and docs_total > 0:
            ratio = docs_cat / docs_total
            scores["especializacion_tematica"] = ratio * 100
        elif docs_cat > 0 and docs_total > 0:
            # Menos de 3 docs: penalización proporcional
            ratio = docs_cat / docs_total
            scores["especializacion_tematica"] = ratio * 100 * (docs_cat / 3)
        else:
            scores["especializacion_tematica"] = 0

        # ── Factor 3: Comisión + Agenda Setting (20%) ──
        comisiones_leg = (leg["comisiones"] or "").lower()
        comisiones_cargo = leg["comisiones_cargo"] or ""

        comision_base = 0
        for com_afin in comisiones_afines:
            if com_afin.lower() in comisiones_leg:
                # Determinar cargo
                cargos = comisiones_cargo.split("|")
                if any(f"{com_afin}:President" in cc for cc in cargos):
                    comision_base = 100
                elif any(f"{com_afin}:Secretar" in cc for cc in cargos):
                    comision_base = max(comision_base, 80)
                else:
                    comision_base = max(comision_base, 60)

        # Boost si su comisión tiene actividad reciente en Gaceta
        if comision_base > 0 and comisiones_con_actividad:
            tiene_actividad = any(
                c.lower() in comisiones_leg for c in comisiones_con_actividad
            )
            if tiene_actividad:
                comision_base = min(comision_base * 1.3, 100)

        scores["comision_agenda"] = comision_base

        # ── Factor 4: Patrón de Instrumento (10%) ──
        tipos = instrumentos_por_leg.get(leg_id, [])
        # Filtrar "Asunto" que es genérico
        tipos_sustantivos = [t for t in tipos if t != "Asunto"]

        if tipos_sustantivos:
            counter = Counter(tipos_sustantivos)
            mas_comun, freq = counter.most_common(1)[0]
            total_sustantivos = len(tipos_sustantivos)
            # Score = qué tan concentrado está en un tipo
            concentracion = freq / total_sustantivos
            scores["patron_instrumento"] = concentracion * 100
        elif tipos:
            # Solo tiene "Asunto" -- bajo score
            scores["patron_instrumento"] = 20
        else:
            scores["patron_instrumento"] = 0

        # ── Factor 5: Penalización por Inactividad (10%) ──
        ultima = ultima_actividad_por_leg.get(leg_id)
        if ultima:
            try:
                d_ultima = datetime.strptime(ultima, "%Y-%m-%d")
                dias_desde = (hoy - d_ultima).days
            except ValueError:
                dias_desde = 999

            if dias_desde > 90:
                scores["penalizacion_inactividad"] = 0
            elif dias_desde > 60:
                scores["penalizacion_inactividad"] = 30
            elif dias_desde > 30:
                scores["penalizacion_inactividad"] = 60
            else:
                scores["penalizacion_inactividad"] = 100
        else:
            scores["penalizacion_inactividad"] = 0

        # ── Score final ponderado ──
        score_total = sum(scores[k] * PESOS[k] for k in PESOS)

        if score_total <= 0:
            continue

        # ── Metadatos enriquecidos ──

        # Instrumento probable
        instrumento_probable = None
        if tipos_sustantivos:
            instrumento_probable = Counter(tipos_sustantivos).most_common(1)[0][0]

        # Ley probable (la más mencionada en sus títulos para esta categoría)
        ley_probable = None
        titulos = titulos_por_leg.get(leg_id, [])
        if titulos:
            leyes = []
            for t in titulos:
                ley = extraer_ley_de_titulo(t)
                if ley:
                    leyes.append(ley)
            if leyes:
                ley_probable = Counter(leyes).most_common(1)[0][0]

        # Narrativa del patrón
        veces_reaccionado = reacciones_por_leg.get(leg_id, 0)
        patron_narrativo = _generar_narrativa(
            nombre=leg["nombre"],
            partido=leg["partido"],
            categoria=categoria,
            veces_reaccionado=veces_reaccionado,
            total_picos=total_picos,
            instrumento_probable=instrumento_probable,
            ley_probable=ley_probable,
            docs_cat=docs_cat,
            comision_base=comision_base,
        )

        predicciones.append({
            "legislador_id": leg_id,
            "nombre": leg["nombre"],
            "camara": leg["camara"],
            "partido": leg["partido"],
            "estado": leg["estado"],
            "foto_url": leg["foto_url"],
            "score_total": round(score_total, 2),
            "desglose": {k: round(v, 1) for k, v in scores.items()},
            "docs_en_categoria": docs_cat,
            "instrumento_probable": instrumento_probable,
            "ley_probable": ley_probable,
            "patron_narrativo": patron_narrativo,
            "correlacion_score": round(scores["correlacion_reactiva"], 1),
            "veces_reaccionado": veces_reaccionado,
            "total_picos": total_picos,
            "comisiones_afines": [
                c for c in comisiones_afines
                if c.lower() in comisiones_leg
            ],
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
