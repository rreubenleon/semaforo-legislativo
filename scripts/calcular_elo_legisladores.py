"""
ELO para legisladores basado en su track record de instrumentos legislativos.

Diseño:
  - Cada instrumento presentado por un legislador es una "partida"
  - S (resultado real) basado en estatus final:
      1.0 si fue "Resuelto / Aprobado" (llegó a publicación o dictamen aprobado)
      0.3 si sigue "Pendiente" entre 90 y 180 días
      0.0 si fue "Desechado" o "Pendiente" > 180 días
      (se excluyen instrumentos con < 90 días — muy reciente para juzgar)
  - E (score esperado): tasa histórica de dictamen de la comisión LXVI
  - Update ELO: ΔR = K × (S − E), K = 24
  - ELO inicial: 1500

El multiplicador implícito es la comisión: dictaminar en comisión donde
casi nadie dictamina (E bajo) da muchos puntos; no lograr en comisión
fluida (E alto) quita muchos puntos. Esto penaliza el "teatro legislativo"
de presentar mucho sin efecto real.

Uso:
    python scripts/calcular_elo_legisladores.py
    python scripts/calcular_elo_legisladores.py --top 20
    python scripts/calcular_elo_legisladores.py --guardar  # persiste en DB
"""
import argparse
import re
import sqlite3
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
sys.path.insert(0, str(ROOT))

# Definición canónica de "sustantivo" (una sola verdad — ver config.py).
# Antes el ELO filtraba clasificacion='legislativo_sustantivo', que dejaba
# fuera los SIL-numéricos (los únicos con estatus) → 0 desenlaces. Ahora usa
# el filtro por tipo, igual que Partidos.
from config import SQL_SUSTANTIVO_T

# Parámetros del sistema
K = 24                  # factor de ajuste ELO (reducido para estabilidad)
ELO_INICIAL = 1500.0
MIN_PARTIDAS_INDICE = 1     # mínimo de instrumentos RESUELTOS para recibir índice
                            # de efectividad. Basta 1 desenlace; con 0 no hay
                            # tasa de éxito posible (fuente única de verdad del corte)
DIAS_MIN_PENDIENTE = 90      # < 90 días: muy reciente, excluir
DIAS_MAX_PENDIENTE = 180     # 90-180 días: S=0.3, >180 días: S=0
HOY = datetime.now().date()

ESTATUS_APROBADO = ["Resuelto / Aprobado", "Aprobado", "Publicado"]
ESTATUS_DESECHADO = ["Desechado", "Rechazado", "Retirado"]


def clasificar_estatus(estatus_raw: str, fecha_presentacion: str) -> float | None:
    """
    Devuelve S (score real) para el ELO:
      1.0 si aprobado / resuelto
      0.0 si desechado
      0.3 si pendiente 90-180 días
      0.0 si pendiente > 180 días
      None si pendiente < 90 días (excluir)
    """
    if not estatus_raw:
        return None
    est_lower = estatus_raw.lower()

    if "aprobado" in est_lower or "resuelto" in est_lower:
        return 1.0
    if "desechado" in est_lower or "rechazado" in est_lower or "retirado" in est_lower:
        return 0.0
    if "pendiente" in est_lower:
        try:
            fp = datetime.strptime(fecha_presentacion, "%Y-%m-%d").date()
            dias = (HOY - fp).days
            if dias < DIAS_MIN_PENDIENTE:
                return None  # muy reciente
            if dias > DIAS_MAX_PENDIENTE:
                return 0.0
            return 0.3
        except (ValueError, TypeError):
            return None
    return None


def extraer_legislador(presentador: str) -> tuple[str, str] | None:
    """
    Parsea 'Sen. José Clemente Castañeda Hoeflich (MC)' → (nombre, partido).
    Solo acepta legisladores individuales (Sen./Dip.).
    Retorna None para Ejecutivo, Mesa Directiva, tribunales, etc.
    """
    if not presentador:
        return None
    # Filtrar no-legisladores
    excluir = ["ejecutivo", "mesa directiva", "tribunal", "instituto", "cámara de senadores",
               "cámara de diputados", "congreso", "comisión permanente", "grupo parlamentario"]
    pres_lower = presentador.lower().strip()
    if any(x in pres_lower and not pres_lower.startswith(("sen.", "dip.")) for x in excluir):
        if not re.match(r"^(sen\.|dip\.)", pres_lower):
            return None

    m = re.match(r"^(Sen\.|Dip\.)\s+([^(]+?)(?:\s*\(([^)]+)\))?\s*$", presentador.strip())
    if not m:
        return None
    titulo = m.group(1)
    nombre = m.group(2).strip()
    partido = (m.group(3) or "").strip()
    if len(nombre) < 4:
        return None
    return (f"{titulo} {nombre}", partido)


def calcular_tasas_comision(conn):
    """
    Para cada comisión: tasa de dictamen = aprobados / (aprobados + desechados + pendientes >180d).
    Los pendientes <180 días NO cuentan aún. Mínimo 10 instrumentos para dar tasa.
    Fallback global: tasa media del universo.
    """
    # Solo instrumentos legislativos sustantivos (filtra licencias,
    # informes, comunicaciones, efemérides, intervenciones genéricas).
    # Ver scripts/clasificar_instrumentos.py para las reglas.
    # Excluir Comisión Permanente: la actividad de la Permanente (sus
    # comisiones de trabajo efímeras) NO debe incidir en las tasas
    # históricas de las comisiones ordinarias del Pleno.
    rows = conn.execute("""
        SELECT comision, estatus, fecha_presentacion
        FROM sil_documentos
        WHERE fecha_presentacion >= '2024-09-01'
          AND comision IS NOT NULL AND comision != ''
          AND """ + SQL_SUSTANTIVO_T("") + """
          AND (camara IS NULL OR camara != 'Comisión Permanente')
    """).fetchall()

    por_comision = {}  # nombre → [aprobados, desechados, pendiente_largo]
    for comision, estatus, fp in rows:
        s = clasificar_estatus(estatus or "", fp or "")
        if s is None:
            continue
        por_comision.setdefault(comision, {"apr": 0, "des": 0, "tot": 0})
        por_comision[comision]["tot"] += 1
        if s >= 0.7:
            por_comision[comision]["apr"] += 1
        elif s == 0.0:
            por_comision[comision]["des"] += 1

    tasas = {}
    for com, counts in por_comision.items():
        if counts["tot"] >= 10:
            tasas[com] = counts["apr"] / counts["tot"]

    # Tasa global como fallback
    tot_global = sum(c["tot"] for c in por_comision.values())
    apr_global = sum(c["apr"] for c in por_comision.values())
    tasa_global = apr_global / tot_global if tot_global else 0.15
    return tasas, tasa_global


def _clean_com_sil(s):
    """'1.-Senado -Medio Ambiente, ... 2.-...' → 'Medio Ambiente, ...' (primera comisión)."""
    if not s:
        return None
    parts = [p for p in re.split(r"\s*\d+\.-", s) if p.strip()]
    c = parts[0] if parts else s
    c = re.sub(r"^(Senado|Diputados)\s*-\s*", "", c.strip())
    c = re.sub(r"\.-Para.*$", "", c).strip()
    return c or None


def _rows_y_tasas_desde_sil(conn, recs_por_leg):
    """Construye rows + tasas por comisión desde el reconteo del SIL (limpio,
    'como promovente') en vez de actividad_legislador (contaminado con efemérides
    + duplicados). Mismo shape que la query original; el resto no cambia."""
    info = {row[0]: (row[1], row[2], row[3])
            for row in conn.execute("SELECT id, nombre, camara, partido FROM legisladores").fetchall()}
    rows, com_apr, com_des = [], {}, {}
    glob_apr = glob_res = 0
    for lid_s, recs in recs_por_leg.items():
        lid = int(lid_s)
        nm, cam, par = info.get(lid, (None, None, None))
        if not nm:
            continue
        for rec in recs:
            com = _clean_com_sil(rec.get("com"))
            est = rec.get("est") or ""
            fp = rec.get("f") or ""
            tipo = "Iniciativa" if rec.get("t") == "ini" else "Proposición con punto de acuerdo"
            rows.append((lid, nm, cam, par, tipo, com, est, fp, ""))
            el = est.lower()
            if "aprobad" in el and "no aprob" not in el:
                if com:
                    com_apr[com] = com_apr.get(com, 0) + 1
                glob_apr += 1; glob_res += 1
            elif any(k in el for k in ("desech", "rechaz", "retir")):
                if com:
                    com_des[com] = com_des.get(com, 0) + 1
                glob_res += 1
    tasas = {}
    for com in set(list(com_apr) + list(com_des)):
        a, d = com_apr.get(com, 0), com_des.get(com, 0)
        if a + d >= 5:
            tasas[com] = a / (a + d)
    tasa_global = (glob_apr / glob_res) if glob_res else 0.3
    return rows, tasas, tasa_global


def calcular_elos(conn):
    import json as _json, os as _os
    _sil_path = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))),
                              "eval", "instrumentos_sil.json")
    rows_sil = None
    if _os.path.exists(_sil_path):
        print("  ELO desde el SIL (instrumentos_sil.json) — fuente de verdad…")
        rows_sil, tasas_com, tasa_global = _rows_y_tasas_desde_sil(
            conn, _json.loads(open(_sil_path, encoding="utf-8").read()))
        print(f"  Tasa global (SIL): {100*tasa_global:.1f}% · {len(tasas_com)} comisiones")
    else:
        print("  Calculando tasas históricas por comisión…")
        tasas_com, tasa_global = calcular_tasas_comision(conn)
        print(f"  Tasa global LXVI: {100*tasa_global:.1f}% · {len(tasas_com)} comisiones con data suficiente")

    # DECISIÓN PRODUCTO 12-may-2026: ELO mide SOLO esfuerzo individual.
    # No queremos calificar a las bancadas — un legislador que solo firma
    # con bloque no recibe ELO. Eso es honesto, no es bug.
    # El widget del Radar ya muestra los totales (ind+grupo) en el desglose
    # de la columna Ind/Grupo, separados de la calificación de Efectividad.
    # Excluir Comisión Permanente del ELO overall: la actividad de un
    # legislador en la Permanente NO debe sumar/restar a su rating
    # general. Es un mecanismo distinto (sesiona solo en receso, otras
    # reglas). Si se quiere medir, va aparte (no en este script).
    rows = rows_sil if rows_sil is not None else conn.execute("""
        SELECT
            al.legislador_id,
            l.nombre AS leg_nombre,
            l.camara AS leg_camara,
            l.partido AS leg_partido,
            sd.tipo,
            sd.comision,
            sd.estatus,
            sd.fecha_presentacion,
            al.co_firmantes
        FROM actividad_legislador al
        JOIN legisladores l ON l.id = al.legislador_id
        JOIN sil_documentos sd ON sd.id = al.sil_documento_id
        WHERE al.legislador_id IS NOT NULL
          AND sd.fecha_presentacion >= '2024-09-01'
          AND """ + SQL_SUSTANTIVO_T("sd") + """
          AND (al.co_firmantes IS NULL OR al.co_firmantes = '')
          AND (sd.camara IS NULL OR sd.camara != 'Comisión Permanente')
        ORDER BY sd.fecha_presentacion
    """).fetchall()

    elos = {}  # nombre canónico → {rating, partidas, ...}
    procesados = 0

    for leg_id, leg_nombre, leg_camara, leg_partido, tipo, comision, estatus, fp, co_firmantes in rows:
        if not leg_nombre:
            continue
        titulo = "Sen." if leg_camara and "Senado" in leg_camara else "Dip."
        nombre_clave = f"{titulo} {leg_nombre}"

        S = clasificar_estatus(estatus or "", fp or "")
        if S is None:
            continue

        E = tasas_com.get(comision, tasa_global) if comision else tasa_global

        info = elos.setdefault(nombre_clave, {
            "rating": ELO_INICIAL, "partidas": 0, "wins": 0, "losses": 0,
            "draws": 0, "aprobados": 0, "desechados": 0, "pendientes_largo": 0,
            "partido": leg_partido or "", "camara": leg_camara or "",
            # legislador_id que YA trae la query (JOIN por al.legislador_id).
            # Antes se descartaba y guardar_en_db lo re-resolvía por nombre
            # (lossy): "Sen. Lilly Téllez" no matcheaba → legislador_id NULL →
            # el radar la mostraba sin Efectividad pese a tener 40 partidas.
            "legislador_id": leg_id,
        })

        delta = K * (S - E)
        info["rating"] += delta
        info["partidas"] += 1

        if S >= 0.7:
            info["wins"] += 1
            info["aprobados"] += 1
        elif S == 0.0 and "desechado" in (estatus or "").lower():
            info["losses"] += 1
            info["desechados"] += 1
        elif S == 0.0:
            info["losses"] += 1
            info["pendientes_largo"] += 1
        else:
            info["draws"] += 1

        procesados += 1

    print(f"  Partidas procesadas (solo individuales): {procesados:,}")
    print(f"  Legisladores con ELO: {len(elos)}")
    return elos


def imprimir_ranking(elos, top_n=20):
    legisladores = sorted(
        ((n, v) for n, v in elos.items() if v["partidas"] >= MIN_PARTIDAS_INDICE),
        key=lambda kv: -kv[1]["rating"]
    )
    print(f"\n  Legisladores con índice (≥{MIN_PARTIDAS_INDICE} resuelto): {len(legisladores)}")
    if not legisladores:
        print("  (sin legisladores que cumplan filtro — posible exceso de filtro individual)")
        return
    print()
    print("  " + "═" * 110)
    print(f"  {'#':<3} {'Legislador':<50} {'Partido':<8} {'ELO':>6} {'P':>3} {'W':>3} {'L':>3} {'D':>3} {'%Apr':>5}")
    print("  " + "─" * 110)
    for i, (nombre, v) in enumerate(legisladores[:top_n], 1):
        pct_apr = 100 * v["aprobados"] / v["partidas"] if v["partidas"] else 0
        print(
            f"  {i:<3} {nombre[:48]:<50} {v['partido'][:6]:<8} "
            f"{v['rating']:>6.0f} {v['partidas']:>3} "
            f"{v['wins']:>3} {v['losses']:>3} {v['draws']:>3} {pct_apr:>4.0f}%"
        )
    print("  " + "─" * 110)
    print(f"  Bottom 10:")
    for i, (nombre, v) in enumerate(legisladores[-10:], len(legisladores) - 9):
        pct_apr = 100 * v["aprobados"] / v["partidas"] if v["partidas"] else 0
        print(
            f"  {i:<3} {nombre[:48]:<50} {v['partido'][:6]:<8} "
            f"{v['rating']:>6.0f} {v['partidas']:>3} "
            f"{v['wins']:>3} {v['losses']:>3} {v['draws']:>3} {pct_apr:>4.0f}%"
        )
    print("  " + "═" * 110)

    # Distribución
    ratings = [v["rating"] for _, v in legisladores]
    print(f"\n  Distribución ELO:")
    print(f"    max:    {max(ratings):.0f}")
    print(f"    p90:    {sorted(ratings)[int(0.9*len(ratings))]:.0f}")
    print(f"    mediana: {sorted(ratings)[len(ratings)//2]:.0f}")
    print(f"    p10:    {sorted(ratings)[int(0.1*len(ratings))]:.0f}")
    print(f"    min:    {min(ratings):.0f}")


_STOPWORDS = {"de", "la", "del", "los", "las", "y", "san"}


def _tokenizar_nombre(s: str) -> list:
    """Tokeniza un nombre para match difuso (devuelve LISTA, no set, para
    conservar multiplicidad — casos "García García", "Castro Castro"):
    - quita título (Sen./Dip./Lic./...)
    - quita acentos y puntuación
    - lowercase
    - filtra tokens cortos (≤2) y stopwords ("de", "la", "del"…)
    """
    if not s:
        return []
    s = s.lower()
    s = re.sub(r"^(sen\.|dip\.|sra\.|sr\.|lic\.|mtro\.|dra\.|dr\.|c\.)\s*", "", s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return [t for t in re.findall(r"[a-z]+", s) if len(t) >= 3 and t not in _STOPWORDS]


def _construir_indice_legisladores(conn):
    """Construye lookup invertido: dict[token] → set(legislador_ids).
    Retorna también tokens_by_id con listas (Counter-compatible) por id."""
    from collections import Counter
    rows = conn.execute("SELECT id, nombre FROM legisladores").fetchall()
    inverted = {}        # token → set(ids)
    tokens_by_id = {}    # id → Counter(tokens)
    for id_, nombre in rows:
        toks = _tokenizar_nombre(nombre or "")
        if not toks:
            continue
        tokens_by_id[id_] = Counter(toks)
        for t in set(toks):
            inverted.setdefault(t, set()).add(id_)
    return inverted, tokens_by_id


def _matchear_legislador(nombre_elo: str, inverted: dict, tokens_by_id: dict) -> int | None:
    """Matchea por intersección multiset (Counter) para respetar apellidos
    duplicados (García García, Márquez Márquez). Regla:
      |intersección_multiset| ≥ 3 AND Jaccard_multiset ≥ 0.5
    """
    from collections import Counter
    toks_elo = _tokenizar_nombre(nombre_elo)
    if len(toks_elo) < 3:
        return None
    c_elo = Counter(toks_elo)
    candidatos = set()
    for t in c_elo:
        candidatos |= inverted.get(t, set())
    if not candidatos:
        return None
    mejor_id = None
    mejor_score = (0, 0.0)
    for cid in candidatos:
        c_leg = tokens_by_id[cid]
        inter = sum((c_elo & c_leg).values())   # multiset intersection count
        if inter < 3:
            continue
        union = sum((c_elo | c_leg).values())   # multiset union count
        jac = inter / union if union else 0
        if jac < 0.5:
            continue
        if (inter, jac) > mejor_score:
            mejor_score = (inter, jac)
            mejor_id = cid
    return mejor_id


def _calcular_indices(elos):
    """Remapea el rating ELO a dos escalas amigables:
      - indice (0-100): percentil global dentro de LXVI. 50 = mediana.
      - percentil_camara (0-100): percentil dentro de la cámara del legislador.
    Ambas son percentiles de rango (no lineales sobre el rating), así que
    son robustas a la cola larga del ELO (Lilia Aguilar con 2767 no distorsiona
    el resto)."""
    # Filtro para el ranking: basta UN instrumento resuelto (aprobado/desechado)
    # para recibir índice. La efectividad mide tasa de éxito y eso requiere al
    # menos un desenlace; con 0 resueltos no hay nada que medir (esos quedan
    # fuera y se muestran como "sin resultados aún", no con un número inventado).
    # La poca-evidencia de 1-2 resueltos se comunica con el campo de confianza
    # (n = partidas), no escondiéndolos.
    elegibles = [(n, v) for n, v in elos.items() if v["partidas"] >= MIN_PARTIDAS_INDICE]

    # Ranking global
    global_sorted = sorted(elegibles, key=lambda kv: kv[1]["rating"])
    n_global = len(global_sorted)
    indice_por_nombre = {}
    for i, (nombre, _) in enumerate(global_sorted):
        # Percentil: 0 = peor, 100 = mejor. Centrado en 50 para la mediana.
        indice_por_nombre[nombre] = round(100 * i / max(1, n_global - 1), 1)

    # Ranking por cámara
    por_camara = {}
    for nombre, v in elegibles:
        por_camara.setdefault(v["camara"] or "—", []).append((nombre, v))
    percentil_camara_por_nombre = {}
    for cam, lista in por_camara.items():
        lista_sorted = sorted(lista, key=lambda kv: kv[1]["rating"])
        n_cam = len(lista_sorted)
        for i, (nombre, _) in enumerate(lista_sorted):
            percentil_camara_por_nombre[nombre] = round(100 * i / max(1, n_cam - 1), 1)

    return indice_por_nombre, percentil_camara_por_nombre


def guardar_en_db(conn, elos):
    """Persiste ratings en tabla nueva legisladores_elo.
    Incluye legislador_id resolviendo match contra tabla legisladores y
    los índices percentiles user-facing (0-100)."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS legisladores_elo (
            nombre TEXT PRIMARY KEY,
            partido TEXT,
            camara TEXT,
            rating REAL,
            partidas INTEGER,
            aprobados INTEGER,
            desechados INTEGER,
            pendientes_largo INTEGER,
            draws INTEGER,
            fecha_calculo TEXT,
            legislador_id INTEGER,
            indice REAL,
            percentil_camara REAL
        )
    """)
    # Migraciones idempotentes
    for col, tipo in [("legislador_id", "INTEGER"), ("indice", "REAL"), ("percentil_camara", "REAL")]:
        try:
            conn.execute(f"ALTER TABLE legisladores_elo ADD COLUMN {col} {tipo}")
        except sqlite3.OperationalError:
            pass

    # IMPORTANTE: borrar filas viejas antes de insertar. Sin esto, runs
    # previos con formatos distintos de nombre (ej. extraer_legislador
    # de presentador SIL vs leg_nombre de tabla legisladores) dejan
    # filas residuales con MISMO legislador_id pero NOMBRE distinto.
    # Como PK es 'nombre', el ON CONFLICT no las detecta y quedan duplicadas,
    # haciendo que el JOIN del worker explote y muestre cada legislador
    # dos veces en /radar.
    conn.execute("DELETE FROM legisladores_elo")

    indices, percentiles_cam = _calcular_indices(elos)
    ahora = datetime.now().isoformat()
    matches = 0
    sin_match = []
    for nombre, v in elos.items():
        # Usar el legislador_id que ya viene de actividad_legislador (vía JOIN
        # en la query, que filtra legislador_id IS NOT NULL). NO re-resolver por
        # nombre — eso perdía a quien tuviera prefijo/variante (Lilly Téllez:
        # "Sen. Lilly Téllez" no matcheaba → id NULL → sin Efectividad).
        leg_id = v.get("legislador_id")
        if leg_id:
            matches += 1
        else:
            sin_match.append(nombre)
        indice = indices.get(nombre)
        pct_cam = percentiles_cam.get(nombre)
        conn.execute("""
            INSERT INTO legisladores_elo (
                nombre, partido, camara, rating, partidas,
                aprobados, desechados, pendientes_largo, draws, fecha_calculo,
                legislador_id, indice, percentil_camara
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(nombre) DO UPDATE SET
                rating=excluded.rating,
                partidas=excluded.partidas,
                aprobados=excluded.aprobados,
                desechados=excluded.desechados,
                pendientes_largo=excluded.pendientes_largo,
                draws=excluded.draws,
                fecha_calculo=excluded.fecha_calculo,
                legislador_id=excluded.legislador_id,
                indice=excluded.indice,
                percentil_camara=excluded.percentil_camara
        """, (nombre, v["partido"], v["camara"], round(v["rating"], 1),
              v["partidas"], v["aprobados"], v["desechados"],
              v["pendientes_largo"], v["draws"], ahora, leg_id, indice, pct_cam))
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM legisladores_elo").fetchone()[0]
    con_indice = conn.execute("SELECT COUNT(*) FROM legisladores_elo WHERE indice IS NOT NULL").fetchone()[0]
    print(f"\n  ✓ Guardados {n} legisladores en tabla legisladores_elo")
    print(f"  ✓ Match con tabla legisladores: {matches}/{len(elos)} ({100*matches/len(elos):.0f}%)")
    print(f"  ✓ Con índice (≥3 partidas): {con_indice}/{n}")
    if sin_match:
        print(f"  ⚠ Sin match ({len(sin_match)}): {sin_match[:5]}{'…' if len(sin_match) > 5 else ''}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--guardar", action="store_true", help="Persistir en DB")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB))
    print(f"\n  ELO Legisladores — data: sil_documentos LXVI (sep 2024 → hoy)")
    print(f"  Parámetros: K={K}, ELO inicial={ELO_INICIAL}")
    print()

    elos = calcular_elos(conn)
    imprimir_ranking(elos, args.top)

    # ── TRIPWIRE anti-degeneración (lo que faltó en abr-may 2026) ──
    # El ELO estuvo degenerado SEMANAS sin que nada avisara: un filtro roto
    # dejó 0 desenlaces y el workflow seguía commiteando basura en silencio.
    # Si el cálculo se degenera otra vez (por la causa que sea: cambio de
    # `tipo`, estatus borrado, vínculos rotos), esto TRUENA EN ROJO en vez
    # de persistir un ELO vacío. Pisos conservadores sobre la realidad LXVI
    # (jun-2026: 485 con ≥3 partidas, 2,555 desenlaces).
    con_3 = sum(1 for v in elos.values() if v["partidas"] >= 3)
    desenlaces = sum(v["wins"] + v["losses"] for v in elos.values())
    PISO_LEGISLADORES = 150
    PISO_DESENLACES = 800
    if con_3 < PISO_LEGISLADORES or desenlaces < PISO_DESENLACES:
        raise SystemExit(
            f"\n  🔴 ELO DEGENERADO — abortando para NO commitear basura.\n"
            f"     legisladores con ≥3 partidas: {con_3} (piso {PISO_LEGISLADORES})\n"
            f"     desenlaces (aprob+desech):    {desenlaces} (piso {PISO_DESENLACES})\n"
            f"     Causa probable: el filtro de 'sustantivo' o el estatus dejó\n"
            f"     de capturar instrumentos. Revisar config.SQL_SUSTANTIVO y\n"
            f"     la cobertura de `estatus` en sil_documentos ANTES de re-sync.")
    print(f"  ✓ Tripwire OK: {con_3} legisladores con ≥3, {desenlaces} desenlaces")

    if args.guardar:
        guardar_en_db(conn, elos)


if __name__ == "__main__":
    main()
