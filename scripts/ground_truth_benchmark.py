"""
Ground truth benchmark v2 — con keywords expandidas y métrica de
"actividad legislativa post-pico" (no solo "hubo ley X específica").

Principios:
  - Keywords de medios: populares/amplias (como habla la prensa)
  - Keywords de instrumentos: técnicas/formales (como titulan legisladores)
  - Hit: aumento de actividad legislativa en la categoría tras el pico
    mediático, comparado con el baseline mensual de la categoría.

Uso:
    python scripts/ground_truth_benchmark.py
    python scripts/ground_truth_benchmark.py --caso reforma_judicial_2024
    python scripts/ground_truth_benchmark.py --solo-positivos
    python scripts/ground_truth_benchmark.py --tabla  # solo tabla final
"""
import argparse
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"

# === Casos con ground truth — keywords expandidas ===
CASOS = {
    "reforma_judicial_2024": {
        "tipo": "positivo",
        "titulo": "Reforma judicial y elección popular de jueces",
        "categoria": "seguridad_justicia",
        "categoria_alt": "administracion",
        "keywords_media": [
            "reforma judicial", "poder judicial", "eleccion de jueces",
            "elección de jueces", "elección popular", "jueces por voto",
            "ministros scjn", "scjn", "norma piña", "norma pina",
            "consejo de la judicatura", "judicatura federal",
            "arturo zaldívar", "arturo zaldivar", "zaldívar",
            "ministros electos", "juezas", "9 ministros",
        ],
        "keywords_legis": [
            "Poder Judicial", "Ley Orgánica del Poder Judicial",
            "Carrera Judicial", "Consejo de la Judicatura",
            "elección de jueces", "ministros",
        ],
        "ventana_media": ("2024-06-01", "2024-10-15"),
        "fecha_legislacion": "2024-09-11",
    },
    "reforma_seguridad_curp_biometrica_2025": {
        "tipo": "positivo",
        "titulo": "Reforma seguridad + CURP biométrica",
        "categoria": "seguridad_justicia",
        "keywords_media": [
            "curp biométrica", "curp biometrica", "identidad biométrica",
            "reforma seguridad", "centralización seguridad", "centralizacion seguridad",
            "sistema nacional de inteligencia", "omar garcía harfuch",
            "omar garcia harfuch", "harfuch", "inteligencia centralizada",
            "registro biométrico", "cédula biométrica",
            "sistema nacional de seguridad", "ssa secretaría",
        ],
        "keywords_legis": [
            "CURP", "biométric", "Sistema Nacional de Seguridad",
            "Ley del Sistema Nacional", "Ley General del Sistema Nacional",
            "inteligencia",
        ],
        "ventana_media": ("2025-08-01", "2025-12-31"),
        "fecha_legislacion": "2025-12-01",
    },
    "reforma_electoral_2026": {
        "tipo": "positivo",
        "titulo": "Reforma electoral 2026",
        "categoria": "electoral_politico",
        "keywords_media": [
            "reforma electoral", "reforma al ine", "ine desaparece",
            "plurinominales", "consejeros electorales", "consejera electoral",
            "autoridad electoral", "guadalupe taddei", "taddei",
            "reducción diputados", "pablo gómez", "pablo gomez",
            "ine reforma", "ley electoral", "lgipe",
        ],
        "keywords_legis": [
            "Ley General de Instituciones y Procedimientos Electorales",
            "Ley General de Partidos", "INE", "reforma electoral",
            "LGIPE",
        ],
        "ventana_media": ("2025-10-01", "2026-04-21"),
        "fecha_legislacion": "2026-04-15",
    },
    "ley_economia_circular_2025": {
        "tipo": "positivo",
        "titulo": "Ley de Economía Circular",
        "categoria": "medio_ambiente",
        "keywords_media": [
            "economía circular", "economia circular",
            "reciclaje", "residuos", "plásticos", "plasticos",
            "sustentabilidad ley", "basura ley", "reciclar ley",
            "prohíbe plásticos", "prohibe plasticos", "desechos",
            "gestión de residuos", "gestion de residuos",
        ],
        "keywords_legis": [
            "Economía Circular", "Economia Circular", "residuos",
            "Ley General para la Prevención", "Ley de Residuos",
        ],
        "ventana_media": ("2025-10-01", "2026-01-10"),
        "fecha_legislacion": "2025-12-15",
    },
    "ley_general_aguas_2025": {
        "tipo": "positivo",
        "titulo": "Ley General de Aguas",
        "categoria": "infraestructura",
        "keywords_media": [
            "reforma hídrica", "reforma hidrica",
            "ley del agua", "ley general de aguas", "ley general del agua",
            "conagua reforma", "conagua", "derecho al agua",
            "crisis hídrica", "crisis hidrica", "agua concesiones",
            "concesión agua", "concesion agua", "tanda de agua",
            "ley de aguas nacionales", "aguas nacionales",
        ],
        "keywords_legis": [
            "Ley General de Aguas", "Ley General del Agua",
            "Ley de Aguas Nacionales", "aguas nacionales",
        ],
        "ventana_media": ("2025-06-01", "2025-12-31"),
        "fecha_legislacion": "2025-10-01",
    },
    "ley_inteligencia_artificial_2026": {
        "tipo": "positivo",
        "titulo": "Ley de Inteligencia Artificial",
        "categoria": "inteligencia_artificial",
        "keywords_media": [
            "inteligencia artificial", "ia méxico", "ia mexico",
            "regular ia", "regulación ia", "regulacion ia",
            "ley de ia", "ley ia", "chatgpt regulación",
            "chatgpt regulacion", "deepfake", "ia regulación",
            "algoritmos regulación", "ia ética", "ia etica",
            "inteligencia artificial ley", "ia riesgo",
        ],
        "keywords_legis": [
            "Inteligencia Artificial", "regulación IA", "IA",
            "algoritmo",
        ],
        "ventana_media": ("2026-03-01", "2026-04-21"),
        "fecha_legislacion": "2026-04-22",
    },
    "reforma_ley_amparo_2025": {
        "tipo": "positivo",
        "titulo": "Reforma a la Ley de Amparo",
        "categoria": "seguridad_justicia",
        "keywords_media": [
            "ley de amparo", "reforma amparo", "juicio de amparo",
            "amparo fiscal", "ley amparo", "juicio amparo",
            "reforma al amparo", "amparo sat", "amparo indirecto",
            "amparo suspensión", "amparo directo", "reforma amparo hacienda",
        ],
        "keywords_legis": [
            "Ley de Amparo", "juicio de amparo",
        ],
        "ventana_media": ("2025-03-01", "2025-12-31"),
        "fecha_legislacion": "2025-09-01",
    },
    "ley_infraestructura_2026": {
        "tipo": "positivo",
        "titulo": "Ley de Infraestructura",
        "categoria": "infraestructura",
        "keywords_media": [
            "ley de infraestructura", "obras públicas reforma",
            "obras publicas reforma", "ley de obras públicas",
            "ley obras publicas", "megaproyecto", "concesiones infraestructura",
            "licitaciones obras", "secretaría de infraestructura",
        ],
        "keywords_legis": [
            "Ley de Obras Públicas", "Ley de Infraestructura",
            "Obras Públicas", "Infraestructura",
        ],
        "ventana_media": ("2026-02-15", "2026-04-21"),
        "fecha_legislacion": "2026-04-01",
    },
    "eliminacion_pensiones_cfe_pemex": {
        "tipo": "positivo",
        "titulo": "Eliminación pensiones funcionarios CFE/PEMEX/LyF",
        "categoria": "trabajo",
        "categoria_alt": "energia",
        "keywords_media": [
            "pensiones cfe", "pensiones pemex", "pensiones funcionarios",
            "pensiones millonarias", "luz y fuerza pensiones",
            "jubilaciones pemex", "jubilaciones cfe", "fondo bienestar",
            "pensiones petroleras", "pensiones privilegio",
            "pensiones ex funcionarios", "pensiones doradas",
            "retiro ejecutivos pemex", "retiro ejecutivos cfe",
            "ex directores pemex", "issste reforma",
        ],
        "keywords_legis": [
            "pensión", "pensiones", "jubilación", "issste",
            "CFE pensión", "Pemex pensión",
        ],
        "ventana_media": ("2025-01-01", "2025-12-31"),
        "fecha_legislacion": "2025-10-01",
    },
    # ─── NEGATIVOS (ruido mediático sin legislación directa) ───
    "aranceles_trump_2025": {
        "tipo": "negativo",
        "titulo": "Aranceles Trump sobre México",
        "categoria": "relaciones_exteriores",
        "keywords_media": [
            "aranceles trump", "aranceles méxico", "aranceles mexico",
            "t-mec aranceles", "trump aranceles", "aranceles 25",
            "trump amenaza", "donald trump méxico", "donald trump mexico",
            "guerra comercial", "aranceles acero", "aranceles automotriz",
        ],
        "keywords_legis": [
            "aranceles", "T-MEC", "comercio exterior",
        ],
        "ventana_media": ("2025-01-01", "2025-12-31"),
        "fecha_legislacion": None,
    },
    "captura_el_mencho_2026": {
        "tipo": "negativo",
        "titulo": "Captura de El Mencho",
        "categoria": "seguridad_justicia",
        "keywords_media": [
            "el mencho", "nemesio oseguera", "captura cjng",
            "detención el mencho", "detencion el mencho",
            "cae el mencho", "mencho cjng", "jalisco nueva generación",
            "jalisco nueva generacion", "líder cjng", "lider cjng",
        ],
        "keywords_legis": [
            "CJNG", "Mencho", "crimen organizado",
        ],
        "ventana_media": ("2026-01-01", "2026-04-21"),
        "fecha_legislacion": None,
    },
    "violencia_periodistas_recurrente": {
        "tipo": "negativo",
        "titulo": "Violencia contra periodistas (recurrente)",
        "categoria": "derechos_humanos",
        "keywords_media": [
            "periodista asesinado", "periodistas ataque",
            "violencia contra periodistas", "libertad de prensa",
            "agresión prensa", "agresion prensa",
            "mecanismo de protección periodistas",
            "mecanismo de proteccion periodistas",
            "asesinato periodista", "matan periodista",
            "periodista muerto", "amenaza periodista",
        ],
        "keywords_legis": [
            "periodistas", "libertad de expresión",
            "Ley de Protección", "Mecanismo",
        ],
        "ventana_media": ("2024-09-01", "2026-04-21"),
        "fecha_legislacion": None,
    },
    "manejo_lluvias_2025": {
        "tipo": "negativo",
        "titulo": "Manejo de lluvias / inundaciones 2025",
        "categoria": "infraestructura",
        "keywords_media": [
            "inundaciones", "lluvias cdmx", "temporal cdmx",
            "drenaje colapsado", "temporal valle", "lluvias intensas",
            "inundación", "inundacion", "granizada",
            "mazatlán huracán", "tabasco lluvias", "veracruz lluvias",
            "temporada de lluvias",
        ],
        "keywords_legis": [
            "protección civil", "inundaciones", "desastre natural",
            "declaratoria de emergencia",
        ],
        "ventana_media": ("2025-06-01", "2025-10-31"),
        "fecha_legislacion": None,
    },
}


def contar_articulos_ventana(conn, keywords, desde, hasta):
    """Conteo diario de artículos. Busca en título Y resumen."""
    if not keywords:
        return {}, set()
    # Buscar en titulo o resumen (si existe la columna)
    tiene_resumen = False
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(articulos)").fetchall()]
        tiene_resumen = "resumen" in cols
    except sqlite3.OperationalError:
        pass

    if tiene_resumen:
        likes = " OR ".join(["LOWER(titulo) LIKE ? OR LOWER(resumen) LIKE ?" for _ in keywords])
        params = []
        for kw in keywords:
            params.extend([f"%{kw.lower()}%", f"%{kw.lower()}%"])
    else:
        likes = " OR ".join(["LOWER(titulo) LIKE ?" for _ in keywords])
        params = [f"%{kw.lower()}%" for kw in keywords]
    params.extend([desde, hasta])

    rows = conn.execute(f"""
        SELECT DISTINCT id, DATE(fecha) fecha, fuente FROM articulos
        WHERE ({likes}) AND DATE(fecha) BETWEEN ? AND ?
        ORDER BY fecha
    """, params).fetchall()

    por_fecha = {}
    fuentes = set()
    for _, f, fuente in rows:
        por_fecha[f] = por_fecha.get(f, 0) + 1
        fuentes.add(fuente)
    return por_fecha, fuentes


def contar_instrumentos(conn, keywords_legis, desde, hasta):
    """Instrumentos legislativos (Gaceta + SIL). Busca en título (case insensitive)."""
    resultados = {"gaceta": [], "sil": []}
    if not keywords_legis:
        return resultados
    likes = " OR ".join(["LOWER(titulo) LIKE ?" for _ in keywords_legis])
    params = [f"%{kw.lower()}%" for kw in keywords_legis] + [desde, hasta]
    for r in conn.execute(f"""
        SELECT fecha, tipo, camara, SUBSTR(titulo,1,180)
        FROM gaceta WHERE ({likes}) AND fecha BETWEEN ? AND ?
        ORDER BY fecha
    """, params).fetchall():
        resultados["gaceta"].append({"fecha": r[0], "tipo": r[1], "camara": r[2], "titulo": r[3]})
    for r in conn.execute(f"""
        SELECT fecha_presentacion, tipo, camara, SUBSTR(titulo,1,180)
        FROM sil_documentos WHERE ({likes}) AND fecha_presentacion BETWEEN ? AND ?
        ORDER BY fecha_presentacion
    """, params).fetchall():
        resultados["sil"].append({"fecha": r[0], "tipo": r[1], "camara": r[2], "titulo": r[3]})
    return resultados


def actividad_post_pico(conn, categoria, fecha_pico, ventana_dias=30):
    """
    Medir aumento de actividad legislativa en la categoría en las
    siguientes N días vs. la media mensual de los 6 meses previos.

    Retorna:
        count_post: docs SIL+Gaceta en categoría en ventana post
        baseline: promedio de docs por ventana de 30 días en los 6 meses previos
        ratio: count_post / baseline
        hit: ratio >= 1.3 (30% sobre baseline = señal de reacción)
    """
    fp = datetime.strptime(fecha_pico, "%Y-%m-%d")
    post_ini = fp.strftime("%Y-%m-%d")
    post_fin = (fp + timedelta(days=ventana_dias)).strftime("%Y-%m-%d")
    # baseline: 6 meses previos al pico, en ventanas de 30 días
    base_fin = (fp - timedelta(days=1)).strftime("%Y-%m-%d")
    base_ini = (fp - timedelta(days=180)).strftime("%Y-%m-%d")

    cat_pattern = f"%{categoria}%"

    def contar(desde, hasta):
        g = conn.execute(
            "SELECT COUNT(*) FROM gaceta WHERE categorias LIKE ? AND fecha BETWEEN ? AND ?",
            (cat_pattern, desde, hasta),
        ).fetchone()[0] or 0
        s = conn.execute(
            "SELECT COUNT(*) FROM sil_documentos WHERE categoria LIKE ? AND fecha_presentacion BETWEEN ? AND ?",
            (cat_pattern, desde, hasta),
        ).fetchone()[0] or 0
        return g + s

    count_post = contar(post_ini, post_fin)
    count_base_total = contar(base_ini, base_fin)
    # baseline por ventana de 30 días
    baseline = count_base_total / 6.0 if count_base_total else 0
    ratio = (count_post / baseline) if baseline > 0 else float("inf") if count_post > 0 else 0
    hit = ratio >= 1.3

    return {
        "count_post": count_post,
        "baseline_30d": round(baseline, 1),
        "ratio": round(ratio, 2) if ratio != float("inf") else "inf",
        "hit": hit,
    }


def scores_en_ventana(conn, categoria, desde, hasta):
    rows = conn.execute("""
        SELECT fecha, score_total, score_media, score_trends, score_congreso
        FROM scores WHERE categoria = ? AND fecha BETWEEN ? AND ?
        ORDER BY fecha
    """, (categoria, desde, hasta)).fetchall()
    return [{"fecha": r[0], "total": r[1], "media": r[2], "trends": r[3], "congreso": r[4]} for r in rows]


def analizar_caso(conn, key, caso, tabla_resumen=None):
    desde, hasta = caso["ventana_media"]
    print()
    print("━" * 80)
    print(f"  {caso['titulo']}  ({'✅ POSITIVO' if caso['tipo']=='positivo' else '⚠️  NEGATIVO'})")
    print(f"  Categoría: {caso['categoria']}  ·  ventana: {desde} → {hasta}")
    print("━" * 80)

    # Artículos
    art_por_fecha, fuentes = contar_articulos_ventana(conn, caso["keywords_media"], desde, hasta)
    total_art = sum(art_por_fecha.values())
    dias_con_art = len(art_por_fecha)
    top_dias = sorted(art_por_fecha.items(), key=lambda kv: -kv[1])[:5]

    print(f"\n  📰 MEDIOS: {total_art} artículos en {dias_con_art} días · {len(fuentes)} fuentes distintas")
    for f, n in top_dias:
        print(f"     {f} → {n} artículos")

    # Instrumentos
    instr = contar_instrumentos(conn, caso["keywords_legis"], desde, hasta)
    total_instr = len(instr["gaceta"]) + len(instr["sil"])
    print(f"\n  🏛  INSTRUMENTOS legislativos (nombre específico): {total_instr} docs")
    print(f"     Gaceta: {len(instr['gaceta'])}  ·  SIL: {len(instr['sil'])}")

    # Fecha del pico mediático (día con más artículos)
    fecha_pico = top_dias[0][0] if top_dias else None

    # Actividad legislativa post-pico (métrica nueva)
    actividad = None
    if fecha_pico:
        actividad = actividad_post_pico(conn, caso["categoria"], fecha_pico, ventana_dias=30)
        print(f"\n  📈 ACTIVIDAD LEG. POST-PICO en {caso['categoria']} ({fecha_pico} + 30d):")
        print(f"     Docs categoría post-pico: {actividad['count_post']}")
        print(f"     Baseline mensual (6m previos): {actividad['baseline_30d']}")
        print(f"     Ratio: {actividad['ratio']}  ·  Hit reactivo: {'SÍ ✓' if actividad['hit'] else 'NO'}")

    # Score FIAT
    scores = scores_en_ventana(conn, caso["categoria"], desde, hasta)
    score_pico = None
    if scores:
        media_vals = [s["media"] or 0 for s in scores]
        total_vals = [s["total"] or 0 for s in scores]
        dias_sat = sum(1 for v in media_vals if v >= 99.5)
        print(f"\n  🎯 FIAT (v1): {len(scores)} días registrados")
        print(f"     score_total max={max(total_vals):.1f}  avg={sum(total_vals)/len(total_vals):.1f}")
        print(f"     score_media max={max(media_vals):.1f}  avg={sum(media_vals)/len(media_vals):.1f}  saturado 100%: {dias_sat}/{len(scores)} ({100*dias_sat/len(scores):.0f}%)")
        if fecha_pico:
            s_pico = next((s for s in scores if s["fecha"] == fecha_pico), None)
            if s_pico:
                score_pico = s_pico
                print(f"     Score el día del pico ({fecha_pico}): total={s_pico['total']:.1f}  media={s_pico['media']:.1f}  congreso={s_pico['congreso']:.1f}")

    # Acumular para la tabla
    if tabla_resumen is not None:
        tabla_resumen.append({
            "caso": caso["titulo"][:45],
            "tipo": caso["tipo"],
            "cat": caso["categoria"][:18],
            "art": total_art,
            "dias_art": dias_con_art,
            "instr": total_instr,
            "ratio_post": actividad["ratio"] if actividad else "—",
            "hit_reactivo": "SÍ" if (actividad and actividad["hit"]) else "NO",
            "score_max": max([s["total"] or 0 for s in scores], default=0),
            "media_max": max([s["media"] or 0 for s in scores], default=0),
            "saturado_pct": 100 * sum(1 for s in scores if (s["media"] or 0) >= 99.5) / len(scores) if scores else 0,
        })


def imprimir_tabla(rows):
    print()
    print("━" * 140)
    print(f"  {'Caso':<45} {'Tipo':<4} {'Cat':<18} {'Art':>5} {'Días':>5} {'Instr':>6} {'Ratio':>7} {'Hit':>4} {'Score':>6} {'Media':>6} {'Sat%':>5}")
    print("━" * 140)
    for r in rows:
        tipo = "+" if r["tipo"] == "positivo" else "—"
        print(f"  {r['caso']:<45} {tipo:<4} {r['cat']:<18} {r['art']:>5} {r['dias_art']:>5} {r['instr']:>6} {str(r['ratio_post']):>7} {r['hit_reactivo']:>4} {r['score_max']:>6.1f} {r['media_max']:>6.1f} {r['saturado_pct']:>4.0f}%")
    print("━" * 140)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--caso", help="Un caso específico")
    parser.add_argument("--solo-positivos", action="store_true")
    parser.add_argument("--solo-negativos", action="store_true")
    parser.add_argument("--tabla", action="store_true", help="Solo mostrar tabla final (sin detalle)")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB))

    casos_a_correr = CASOS
    if args.caso:
        if args.caso not in CASOS:
            print(f"Casos: {list(CASOS.keys())}")
            return
        casos_a_correr = {args.caso: CASOS[args.caso]}
    elif args.solo_positivos:
        casos_a_correr = {k: v for k, v in CASOS.items() if v["tipo"] == "positivo"}
    elif args.solo_negativos:
        casos_a_correr = {k: v for k, v in CASOS.items() if v["tipo"] == "negativo"}

    tabla_resumen = [] if args.tabla or True else None
    for key, caso in casos_a_correr.items():
        if args.tabla:
            import io, contextlib
            with contextlib.redirect_stdout(io.StringIO()):
                analizar_caso(conn, key, caso, tabla_resumen)
        else:
            analizar_caso(conn, key, caso, tabla_resumen)

    if tabla_resumen:
        imprimir_tabla(tabla_resumen)


if __name__ == "__main__":
    main()
