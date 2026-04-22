"""
Completa la tabla `legisladores` con stubs para legisladores que firman
en SIL pero no aparecen en el listado oficial SITL/Senado.

Root cause: SITL Diputados devuelve 500 titulares originales (sep 2024)
y no lista suplentes que tomaron curul después. El Senado tiene el mismo
gap. Esto deja ~56 legisladores activos sin fila en `legisladores`.

Criterio simple: si una persona tiene fila en `legisladores_elo` con
`legislador_id IS NULL`, significa que firmó al menos 1 instrumento en
SIL. Por lo tanto merece stub en `legisladores` para que el Radar y las
demás vistas la puedan mostrar.

Los ex-titulares que pasaron al Ejecutivo SIN firmar nada nunca aparecen
en SIL y por tanto no se procesan aquí — el filtro es automático.

Marcamos cada stub con `origen = 'sil_inferido'` para poder distinguirlo
del `'sitl_oficial'` en el futuro.

Uso:
    python scripts/completar_legisladores_desde_elo.py
    python scripts/completar_legisladores_desde_elo.py --dry-run
"""
import argparse
import logging
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def normalizar_nombre_sitl(nombre):
    """Mismo criterio que scrapers/legisladores.py _normalizar_nombre."""
    nombre = re.sub(r"^(Dip\.|Sen\.|C\.|Diputad[oa]|Senador[a]?)\s*", "", nombre.strip())
    nombre = re.sub(r"\s*\([^)]*\)\s*", "", nombre)
    nombre = nombre.lower().strip()
    reemplazos = {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n", "ü": "u"}
    for orig, remp in reemplazos.items():
        nombre = nombre.replace(orig, remp)
    nombre = re.sub(r"[^a-z\s]", "", nombre)
    return re.sub(r"\s+", " ", nombre).strip()


def inferir_camara(nombre_elo, camara_original):
    """La columna camara del ELO puede decir 'H. Congreso General' o
    'Comisión Permanente'. El prefijo del nombre (Dip./Sen.) es más
    confiable para inferir la cámara real."""
    if nombre_elo.startswith("Dip."):
        return "Cámara de Diputados"
    if nombre_elo.startswith("Sen."):
        return "Cámara de Senadores"
    return camara_original


def limpiar_nombre(nombre_elo):
    """'Dip. Ivonne Aracely Ortega Pacheco' → 'Ivonne Aracely Ortega Pacheco'"""
    return re.sub(r"^(Dip\.|Sen\.)\s*", "", nombre_elo).strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    # Agregar columna `origen` si no existe (migración idempotente)
    try:
        conn.execute("ALTER TABLE legisladores ADD COLUMN origen TEXT DEFAULT 'sitl_oficial'")
        conn.commit()
        logger.info("Columna 'origen' agregada a legisladores")
    except sqlite3.OperationalError:
        pass  # ya existe

    huerfanos = conn.execute("""
        SELECT nombre, partido, camara, partidas, rating, aprobados, desechados
        FROM legisladores_elo
        WHERE legislador_id IS NULL
        ORDER BY rating DESC
    """).fetchall()
    logger.info(f"Huérfanos a procesar: {len(huerfanos)}")

    ahora = datetime.now().isoformat()
    creados = 0
    ya_existia = 0
    errores = 0
    muestras = []

    for row in huerfanos:
        nombre_limpio = limpiar_nombre(row["nombre"])
        camara = inferir_camara(row["nombre"], row["camara"] or "")
        nombre_norm = normalizar_nombre_sitl(nombre_limpio)

        if not nombre_norm or not camara:
            errores += 1
            continue

        existente = conn.execute(
            "SELECT id FROM legisladores WHERE nombre_normalizado = ? AND camara = ?",
            (nombre_norm, camara),
        ).fetchone()
        if existente:
            ya_existia += 1
            continue

        if args.dry_run:
            muestras.append(f"  → {nombre_limpio} ({row['partido']}, {camara}, {row['partidas']}p)")
            creados += 1
            continue

        try:
            conn.execute("""
                INSERT INTO legisladores (
                    nombre, nombre_normalizado, camara, partido,
                    estado, distrito, principio_eleccion,
                    fecha_scraping, origen
                ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, ?, 'sil_inferido')
            """, (nombre_limpio, nombre_norm, camara, row["partido"], ahora))
            creados += 1
            muestras.append(f"  → {nombre_limpio} ({row['partido']}, {camara.replace('Cámara de ', '')}, {row['partidas']}p)")
        except sqlite3.IntegrityError as e:
            logger.warning(f"Skip {nombre_limpio}: {e}")
            errores += 1

    if not args.dry_run:
        conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM legisladores").fetchone()[0]
    inferidos = conn.execute(
        "SELECT COUNT(*) FROM legisladores WHERE origen = 'sil_inferido'"
    ).fetchone()[0]

    print()
    print(f"  ═══ Completar legisladores desde ELO ═══")
    print(f"  Stubs creados:  {creados}")
    print(f"  Ya existían:    {ya_existia}")
    print(f"  Errores:        {errores}")
    print(f"  Total ahora:    {total} ({inferidos} inferidos · {total - inferidos} oficiales)")
    if muestras:
        print(f"\n  Muestra (primeros 15):")
        for m in muestras[:15]:
            print(m)
        if len(muestras) > 15:
            print(f"  … y {len(muestras) - 15} más")

    if args.dry_run:
        print("\n  (dry-run: no se tocó la DB)")


if __name__ == "__main__":
    main()
