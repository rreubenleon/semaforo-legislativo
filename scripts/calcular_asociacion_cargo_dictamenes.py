"""
Calcula la métrica "asociación cargo-dictámenes" para cada legislador
con presidencia de comisión.

Pregunta que responde: cuando un legislador preside una comisión,
¿cuántos de sus instrumentos aprobados pasaron por esa comisión que
él/ella controla? Es una señal de auto-promoción institucional.

Para cada legislador con `:Presidente` en `comisiones_cargo`:
  - aprobados_total: cuántos instrumentos suyos están "Aprobado/Resuelto"
  - aprobados_en_propia: subset que fue turnado a la comisión que preside
  - pct_propia: ratio

Guarda en columnas nuevas de `legisladores`:
  - aprobados_total INTEGER
  - aprobados_en_propia INTEGER
  - comision_presidida TEXT (la principal si tiene varias)

Sync a D1 al final (--no-sync-d1 para skip).

Uso:
  python scripts/calcular_asociacion_cargo_dictamenes.py
  python scripts/calcular_asociacion_cargo_dictamenes.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import subprocess
import sys
import tempfile
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def crear_columnas(conn):
    """Idempotente."""
    for col, tipo in [
        ("aprobados_total", "INTEGER"),
        ("aprobados_en_propia", "INTEGER"),
        ("comision_presidida", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE legisladores ADD COLUMN {col} {tipo}")
        except sqlite3.OperationalError:
            pass  # ya existe
    conn.commit()


def normalizar(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()


_PARTICULAS = {"de", "del", "la", "los", "las", "y", "san", "santa"}


def tokens_distintivos(nombre):
    """Tokens significativos del nombre normalizado (sin acentos, sin
    partículas). Usados para construir LIKE pattern contra `presentador`
    cuyo formato es 'Sen. Nombre Apellido Apellido (PARTIDO)'."""
    n = normalizar(nombre)
    n = re.sub(r"[^a-z0-9 ]", " ", n)
    return [t for t in n.split() if len(t) >= 4 and t not in _PARTICULAS]


def cargar_aprobados_lxvi(conn):
    """Carga TODOS los instrumentos aprobados/resueltos LXVI a memoria,
    con presentador y comisión NORMALIZADOS (sin acentos). Mucho más
    eficiente que N queries con LIKE acent-aware."""
    rows = conn.execute("""
        SELECT seguimiento_id, presentador, comision
          FROM sil_documentos
         WHERE legislatura = 'LXVI'
           AND (LOWER(estatus) LIKE '%aprob%' OR LOWER(estatus) LIKE '%resuelto%')
    """).fetchall()
    out = []
    for sid, pres, com in rows:
        out.append({
            "sid": sid,
            "pres_norm": normalizar(pres or ""),
            "com_norm": normalizar(com or ""),
        })
    return out


def calcular_para_legislador(aprobados_idx, nombre, comision_presidida):
    """Devuelve (total, en_propia)."""
    toks = tokens_distintivos(nombre)
    if len(toks) < 2:
        return 0, 0
    sig1 = toks[0]
    sig2 = toks[-1] if len(toks) > 2 else toks[1]

    com_tokens = [t for t in re.split(r"[^a-z0-9]+", normalizar(comision_presidida))
                  if len(t) >= 5 and t not in _PARTICULAS]
    com_clave = com_tokens[0] if com_tokens else normalizar(comision_presidida)

    total = 0
    propia = 0
    seen = set()
    for r in aprobados_idx:
        # Match por nombre: ambos sig1 y sig2 en presentador normalizado
        if sig1 in r["pres_norm"] and sig2 in r["pres_norm"]:
            if r["sid"] in seen:
                continue
            seen.add(r["sid"])
            total += 1
            if com_clave in r["com_norm"]:
                propia += 1
    return total, propia


def es_presidente(rol):
    return "presiden" in (rol or "").lower()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-sync-d1", action="store_true")
    args = p.parse_args()

    conn = sqlite3.connect(str(DB))
    crear_columnas(conn)

    # Cargar índice una sola vez (sin acentos)
    logger.info("Cargando índice de aprobados LXVI...")
    idx = cargar_aprobados_lxvi(conn)
    logger.info(f"Índice: {len(idx)} instrumentos aprobados")

    # Identificar legisladores con presidencia
    rows = conn.execute("""
        SELECT id, nombre, comisiones_cargo
          FROM legisladores
         WHERE comisiones_cargo IS NOT NULL
           AND comisiones_cargo LIKE '%:Presidente%'
    """).fetchall()
    logger.info(f"Legisladores con presidencia: {len(rows)}")

    actualizados = 0
    para_d1 = []

    for leg_id, nombre, cargo in rows:
        # Extraer la primera comisión que preside
        com_presidida = None
        for parte in cargo.split("|"):
            if ":" in parte:
                c, r = parte.split(":", 1)
                if es_presidente(r) and c.strip() and c.strip().upper() != "A LAS QUE PERTENECE":
                    com_presidida = c.strip()
                    break
        if not com_presidida:
            continue

        total, propia = calcular_para_legislador(idx, nombre, com_presidida)
        if total == 0 and not args.dry_run:
            continue
        actualizados += 1
        pct = (propia / total * 100) if total > 0 else 0
        logger.info(f"  {nombre[:40]:40} | preside {com_presidida[:35]:35} | "
                    f"{propia}/{total} aprobados ({pct:.0f}%)")

        if args.dry_run:
            continue

        conn.execute("""
            UPDATE legisladores
               SET aprobados_total = ?,
                   aprobados_en_propia = ?,
                   comision_presidida = ?
             WHERE id = ?
        """, (total, propia, com_presidida, leg_id))
        para_d1.append((leg_id, total, propia, com_presidida))

    if not args.dry_run:
        conn.commit()
        logger.info(f"BD local: {actualizados} legisladores con métrica")

        if not args.no_sync_d1 and para_d1:
            sync_d1(para_d1)


def sync_d1(rows):
    """UPSERT a Cloudflare D1: agregar columnas si no existen + UPDATE."""
    ddl = """
        CREATE TABLE IF NOT EXISTS _ignored_for_alter (id INTEGER);
    """
    # ALTER TABLE en D1 — debe ser idempotente. wrangler --command uno por uno
    for col, tipo in [
        ("aprobados_total", "INTEGER"),
        ("aprobados_en_propia", "INTEGER"),
        ("comision_presidida", "TEXT"),
    ]:
        cmd = ["npx", "wrangler", "d1", "execute", "fiat-busqueda",
               "--remote",
               "--command", f"ALTER TABLE legisladores ADD COLUMN {col} {tipo}"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30,
                           cwd=str(ROOT / "worker"))
        if "duplicate column" in r.stderr.lower() or r.returncode == 0:
            logger.info(f"  Columna {col}: OK")
        else:
            logger.warning(f"  Columna {col}: {r.stderr[:200]}")

    # UPDATEs en batch
    def esc(s):
        if s is None: return 'NULL'
        if isinstance(s, (int, float)): return str(s)
        return "'" + str(s).replace("'", "''") + "'"

    statements = []
    for leg_id, total, propia, com in rows:
        statements.append(
            f"UPDATE legisladores SET "
            f"aprobados_total={total}, aprobados_en_propia={propia}, "
            f"comision_presidida={esc(com)} WHERE id={leg_id};"
        )

    BATCH = 30
    for i in range(0, len(statements), BATCH):
        batch = statements[i:i + BATCH]
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
            f.write("\n".join(batch))
            sql_file = f.name
        try:
            r = subprocess.run(
                ["npx", "wrangler", "d1", "execute", "fiat-busqueda",
                 "--file", sql_file, "--remote"],
                capture_output=True, text=True, timeout=60,
                cwd=str(ROOT / "worker"),
            )
            if r.returncode == 0:
                logger.info(f"  Batch {i // BATCH + 1}: {len(batch)} updates OK")
            else:
                logger.error(f"  Batch falló: {r.stderr[:300]}")
        finally:
            Path(sql_file).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
