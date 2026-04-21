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
    rows = conn.execute("""
        SELECT comision, estatus, fecha_presentacion
        FROM sil_documentos
        WHERE fecha_presentacion >= '2024-09-01' AND comision IS NOT NULL AND comision != ''
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

    # Iterar instrumentos cronológicamente
    rows = conn.execute("""
        SELECT presentador, partido, camara, tipo, comision, estatus, fecha_presentacion
        FROM sil_documentos
        WHERE fecha_presentacion >= '2024-09-01' AND presentador != ''
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


def _normalizar_nombre(s: str) -> str:
    """Normaliza nombre para match: sin título, sin acentos, sin puntuación,
    tokens ordenados alfabéticamente. Así 'Sen. José Clemente Castañeda' y
    'Castañeda Hoeflich José Clemente' colisionan en el mismo hash."""
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"^(sen\.|dip\.|sra\.|sr\.|lic\.|mtro\.|dra\.|dr\.)\s*", "", s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    tokens = sorted(t for t in re.findall(r"[a-z0-9]+", s) if len(t) >= 2)
    return " ".join(tokens)


def _construir_indice_legisladores(conn):
    """Dict: nombre_normalizado → id de la tabla legisladores."""
    rows = conn.execute("SELECT id, nombre FROM legisladores").fetchall()
    idx = {}
    for id_, nombre in rows:
        key = _normalizar_nombre(nombre or "")
        if key and key not in idx:
            idx[key] = id_
    return idx


def guardar_en_db(conn, elos):
    """Persiste ratings en tabla nueva legisladores_elo.
    Incluye legislador_id resolviendo match contra tabla legisladores."""
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
            legislador_id INTEGER
        )
    """)
    # Migración: agregar columna si tabla ya existía
    try:
        conn.execute("ALTER TABLE legisladores_elo ADD COLUMN legislador_id INTEGER")
    except sqlite3.OperationalError:
        pass  # ya existe

    idx = _construir_indice_legisladores(conn)
    ahora = datetime.now().isoformat()
    matches = 0
    for nombre, v in elos.items():
        key = _normalizar_nombre(nombre)
        leg_id = idx.get(key)
        if leg_id:
            matches += 1
        conn.execute("""
            INSERT INTO legisladores_elo (
                nombre, partido, camara, rating, partidas,
                aprobados, desechados, pendientes_largo, draws, fecha_calculo,
                legislador_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(nombre) DO UPDATE SET
                rating=excluded.rating,
                partidas=excluded.partidas,
                aprobados=excluded.aprobados,
                desechados=excluded.desechados,
                pendientes_largo=excluded.pendientes_largo,
                draws=excluded.draws,
                fecha_calculo=excluded.fecha_calculo,
                legislador_id=excluded.legislador_id
        """, (nombre, v["partido"], v["camara"], round(v["rating"], 1),
              v["partidas"], v["aprobados"], v["desechados"],
              v["pendientes_largo"], v["draws"], ahora, leg_id))
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM legisladores_elo").fetchone()[0]
    print(f"\n  ✓ Guardados {n} legisladores en tabla legisladores_elo")
    print(f"  ✓ Match con tabla legisladores: {matches}/{len(elos)} ({100*matches/len(elos):.0f}%)")


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
