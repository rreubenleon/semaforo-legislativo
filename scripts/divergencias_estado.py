"""
Histéresis para divergencias entre fuentes — Nivel 2 de "sustain".

PROBLEMA: La tabla `divergencia` se sobre-escribe cada pipeline (4h). Si una
divergencia aparece a las 8am y los datos cambian a las 12pm, desaparece.
Eso falsea la realidad: la presión social/legislativa NO se evapora en 4h.

SOLUCIÓN: Estado persistente con dos umbrales (histéresis):
  - UMBRAL_A (activación): KL ≥ 0.30 → abre estado nuevo
  - UMBRAL_B (mantenimiento): KL ≥ 0.15 → mantiene abierta
  - N_CIERRE: tras N pipelines consecutivos por debajo de UMBRAL_B → cierra

Resultado: una divergencia activa dura mínimo N×4h = 24h desde que cae bajo
UMBRAL_B. Modela mejor la inercia real de la presión.

Tabla `divergencias_estado` (UNIQUE por categoria abierta a la vez):
  id, categoria, abierta_en, cerrada_en (NULL=activa), kl_apertura, kl_pico,
  kl_actual, patron_apertura, patron_actual, sobre_apertura, sub_apertura,
  pipelines_bajo_b, ultima_evaluacion

Se corre en el pipeline después de calcular_divergencia.py:
  python scripts/divergencias_estado.py
  python scripts/divergencias_estado.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

# ── Configuración de histéresis ──────────────────────────────────────────────
UMBRAL_A = 0.30   # activación: matchea DIVERGENCIA_THRESHOLD del frontend
UMBRAL_B = 0.15   # mantenimiento: la mitad de A — debajo es ruido
N_CIERRE = 6      # pipelines consecutivos bajo UMBRAL_B para cerrar (≈24h)

# Patrones técnicos que NO abren estados (ruido de modelo)
PATRONES_OCULTOS = {"rebalanceo_tecnico", "convergencia"}


def crear_tabla(conn):
    """Tabla idempotente para tracking del estado de cada divergencia."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS divergencias_estado (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            categoria TEXT NOT NULL,
            abierta_en TEXT NOT NULL,
            cerrada_en TEXT,
            kl_apertura REAL NOT NULL,
            kl_pico REAL NOT NULL,
            kl_actual REAL NOT NULL,
            patron_apertura TEXT,
            patron_actual TEXT,
            sobre_apertura TEXT,
            sub_apertura TEXT,
            pipelines_bajo_b INTEGER NOT NULL DEFAULT 0,
            ultima_evaluacion TEXT NOT NULL
        )
    """)
    # Índice compuesto para query rápido del estado activo de una categoría
    conn.execute("CREATE INDEX IF NOT EXISTS idx_divest_activa ON divergencias_estado(categoria, cerrada_en)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_divest_abierta ON divergencias_estado(abierta_en DESC)")


def get_estado_abierto(conn, categoria):
    """Retorna dict del estado abierto activo (cerrada_en IS NULL) o None."""
    row = conn.execute("""
        SELECT id, categoria, abierta_en, cerrada_en, kl_apertura, kl_pico,
               kl_actual, patron_apertura, patron_actual, sobre_apertura,
               sub_apertura, pipelines_bajo_b, ultima_evaluacion
        FROM divergencias_estado
        WHERE categoria = ? AND cerrada_en IS NULL
        ORDER BY abierta_en DESC LIMIT 1
    """, (categoria,)).fetchone()
    if not row:
        return None
    keys = ["id","categoria","abierta_en","cerrada_en","kl_apertura","kl_pico",
            "kl_actual","patron_apertura","patron_actual","sobre_apertura",
            "sub_apertura","pipelines_bajo_b","ultima_evaluacion"]
    return dict(zip(keys, row))


def abrir_estado(conn, categoria, kl, patron, sobre, sub, ahora, dry_run=False):
    """INSERT nuevo estado abierto."""
    logger.info(f"  ABRE {categoria}: KL={kl:.3f} → '{patron}' (↑{sobre} ↓{sub})")
    if dry_run:
        return
    conn.execute("""
        INSERT INTO divergencias_estado
          (categoria, abierta_en, cerrada_en, kl_apertura, kl_pico, kl_actual,
           patron_apertura, patron_actual, sobre_apertura, sub_apertura,
           pipelines_bajo_b, ultima_evaluacion)
        VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, 0, ?)
    """, (categoria, ahora, kl, kl, kl, patron, patron, sobre, sub, ahora))


def actualizar_estado(conn, estado, kl, patron, ahora, bajo_b, dry_run=False):
    """UPDATE kl_actual, kl_pico (max), patron_actual, pipelines_bajo_b."""
    nuevo_pico = max(estado["kl_pico"], kl)
    nuevo_pbb = (estado["pipelines_bajo_b"] + 1) if bajo_b else 0
    logger.info(f"  MANTIENE {estado['categoria']}: KL={kl:.3f} (pico={nuevo_pico:.3f}, bajo_B={nuevo_pbb}/{N_CIERRE})")
    if dry_run:
        return
    conn.execute("""
        UPDATE divergencias_estado
           SET kl_actual = ?, kl_pico = ?, patron_actual = ?,
               pipelines_bajo_b = ?, ultima_evaluacion = ?
         WHERE id = ?
    """, (kl, nuevo_pico, patron, nuevo_pbb, ahora, estado["id"]))


def cerrar_estado(conn, estado, kl_final, ahora, dry_run=False):
    """UPDATE cerrada_en. Marca el estado como concluido."""
    duracion_h = (datetime.fromisoformat(ahora) - datetime.fromisoformat(estado["abierta_en"])).total_seconds() / 3600
    logger.info(f"  CIERRA {estado['categoria']}: duró {duracion_h:.1f}h, pico KL={estado['kl_pico']:.3f}")
    if dry_run:
        return
    conn.execute("""
        UPDATE divergencias_estado
           SET cerrada_en = ?, kl_actual = ?, ultima_evaluacion = ?
         WHERE id = ?
    """, (ahora, kl_final, ahora, estado["id"]))


def procesar_pipeline(conn, dry_run=False):
    """
    Lógica principal:
    1. Leer la divergencia más reciente de cada categoría desde tabla `divergencia`
    2. Para cada una, decidir abrir / mantener / cerrar el estado
    """
    crear_tabla(conn)
    ahora = datetime.utcnow().isoformat(timespec='seconds')

    # Última divergencia por categoría (snapshot del último pipeline)
    rows = conn.execute("""
        SELECT d.categoria, d.kl, d.patron_id, d.patron_label,
               d.sobre_representada, d.sub_representada, d.fecha
          FROM divergencia d
         WHERE d.fecha = (SELECT MAX(fecha) FROM divergencia)
    """).fetchall()

    if not rows:
        logger.warning("Sin divergencias en BD — nada que procesar")
        return {"abiertas": 0, "mantenidas": 0, "cerradas": 0, "ignoradas": 0}

    stats = {"abiertas": 0, "mantenidas": 0, "cerradas": 0, "ignoradas": 0}

    for cat, kl, patron_id, patron_label, sobre, sub, fecha in rows:
        kl = float(kl or 0)
        estado = get_estado_abierto(conn, cat)
        es_ruido = patron_id in PATRONES_OCULTOS

        # Caso 1: NO hay estado abierto
        if estado is None:
            if kl >= UMBRAL_A and not es_ruido:
                abrir_estado(conn, cat, kl, patron_label, sobre, sub, ahora, dry_run)
                stats["abiertas"] += 1
            else:
                stats["ignoradas"] += 1
            continue

        # Caso 2: HAY estado abierto
        if kl >= UMBRAL_B and not es_ruido:
            # Mantener: reset contador
            actualizar_estado(conn, estado, kl, patron_label, ahora, bajo_b=False, dry_run=dry_run)
            stats["mantenidas"] += 1
        else:
            # Bajó del umbral B (o se volvió ruido). Incrementar contador.
            actualizar_estado(conn, estado, kl, patron_label, ahora, bajo_b=True, dry_run=dry_run)
            # ¿Llegó al umbral de cierre?
            if estado["pipelines_bajo_b"] + 1 >= N_CIERRE:
                cerrar_estado(conn, estado, kl, ahora, dry_run)
                stats["cerradas"] += 1

    if not dry_run:
        conn.commit()

    logger.info(f"Procesamiento: abiertas={stats['abiertas']} "
                f"mantenidas={stats['mantenidas']} cerradas={stats['cerradas']} "
                f"ignoradas={stats['ignoradas']}")
    return stats


def listar_activas(conn):
    """Helper para inspeccionar manualmente."""
    rows = conn.execute("""
        SELECT categoria, abierta_en, kl_actual, kl_pico, patron_actual,
               pipelines_bajo_b, ultima_evaluacion
          FROM divergencias_estado
         WHERE cerrada_en IS NULL
         ORDER BY kl_pico DESC
    """).fetchall()
    if not rows:
        print("Sin divergencias activas")
        return
    print(f"\n{'CATEGORÍA':<25} {'ABIERTA':<22} {'KL_ACT':>7} {'KL_PICO':>8} {'PATRÓN':<35} {'BAJO_B':>7}")
    for cat, ab, kl, pico, patron, pbb, _ in rows:
        ab_short = ab[:19] if ab else '—'
        print(f"{cat[:23]:<25} {ab_short:<22} {kl:>7.3f} {pico:>8.3f} {(patron or '')[:33]:<35} {pbb:>3}/{N_CIERRE}")
    print()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="No escribe a BD")
    p.add_argument("--listar", action="store_true", help="Solo lista activas y sale")
    args = p.parse_args()

    conn = sqlite3.connect(str(DB))

    if args.listar:
        crear_tabla(conn)
        listar_activas(conn)
        return

    procesar_pipeline(conn, dry_run=args.dry_run)
    listar_activas(conn)


if __name__ == "__main__":
    main()
