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

# Parámetros del sistema
K = 24                  # factor de ajuste ELO (reducido para estabilidad)
ELO_INICIAL = 1500.0
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
    rows = conn.execute("""
        SELECT comision, estatus, fecha_presentacion
        FROM sil_documentos
        WHERE fecha_presentacion >= '2024-09-01'
          AND comision IS NOT NULL AND comision != ''
          AND (clasificacion = 'legislativo_sustantivo' OR clasificacion IS NULL)
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


def calcular_elos(conn):
    print("  Calculando tasas históricas por comisión…")
    tasas_com, tasa_global = calcular_tasas_comision(conn)
    print(f"  Tasa global LXVI: {100*tasa_global:.1f}% · {len(tasas_com)} comisiones con data suficiente")

    # Iterar instrumentos cronológicamente — solo legislativo sustantivo
    rows = conn.execute("""
        SELECT presentador, partido, camara, tipo, comision, estatus, fecha_presentacion
        FROM sil_documentos
        WHERE fecha_presentacion >= '2024-09-01' AND presentador != ''
          AND (clasificacion = 'legislativo_sustantivo' OR clasificacion IS NULL)
        ORDER BY fecha_presentacion
    """).fetchall()

    elos = {}  # nombre → {rating, partidas, wins, losses, ...}
    procesados = 0

    for presentador, partido_raw, camara, tipo, comision, estatus, fp in rows:
        info_leg = extraer_legislador(presentador)
        if not info_leg:
            continue
        nombre, partido = info_leg
        if partido_raw and not partido:
            partido = partido_raw

        S = clasificar_estatus(estatus or "", fp or "")
        if S is None:
            continue  # excluir (muy reciente)

        # E (expected): tasa de dictamen de la comisión
        E = tasas_com.get(comision, tasa_global) if comision else tasa_global

        # Obtener ELO actual
        info = elos.setdefault(nombre, {
            "rating": ELO_INICIAL, "partidas": 0, "wins": 0, "losses": 0,
            "draws": 0, "aprobados": 0, "desechados": 0, "pendientes_largo": 0,
            "partido": partido, "camara": camara,
        })

        # Update
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

    print(f"  Partidas procesadas: {procesados}")
    print(f"  Legisladores con ELO: {len(elos)}")
    return elos


def imprimir_ranking(elos, top_n=20):
    legisladores = sorted(
        ((n, v) for n, v in elos.items() if v["partidas"] >= 3),
        key=lambda kv: -kv[1]["rating"]
    )
    print(f"\n  Legisladores con ≥ 3 partidas: {len(legisladores)}")
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
    # Filtro para el ranking: solo legisladores con ≥3 partidas cuentan para
    # el percentil. Los que tienen <3 quedan marcados como None y no reciben
    # índice (muy poco evidencia).
    elegibles = [(n, v) for n, v in elos.items() if v["partidas"] >= 3]

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

    inverted, tokens_by_id = _construir_indice_legisladores(conn)
    indices, percentiles_cam = _calcular_indices(elos)
    ahora = datetime.now().isoformat()
    matches = 0
    sin_match = []
    for nombre, v in elos.items():
        leg_id = _matchear_legislador(nombre, inverted, tokens_by_id)
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

    if args.guardar:
        guardar_en_db(conn, elos)


if __name__ == "__main__":
    main()
