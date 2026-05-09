"""
Limpia entradas basura en `legisladores.comisiones_cargo` (Diputados).

Bug: el scraper `scrapers/legisladores.py` parseó mal el HTML del SITL
Diputados y metió headers genéricos del menú como "comisiones":
  - "A LAS QUE PERTENECE"  (header de la sección)
  - "COMITÉ"               (header de bloque de comités)
  - "GRUPOS DE AMISTAD"    (otra sección)
  - etc.

Resultado: 500/532 diputados tienen "A LAS QUE PERTENECE" como
"comisión", inflando los datos visibles en el dashboard. Los conteos
de efectividad por comisión también se distorsionan.

Solución: filtrar `comisiones_cargo` con una blacklist explícita +
heurísticas (TODO MAYÚSCULAS, longitud, etc.) similar al limpiador
del Senado pero adaptado al SITL Diputados.

Idempotente.

Uso:
    python scripts/limpiar_cargos_diputados.py
    python scripts/limpiar_cargos_diputados.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


# Blacklist explícita: tokens de items del menú/headers que el scraper
# metió como "comisiones". Match parcial case-insensitive.
BLACKLIST_EXACTA = {
    "a las que pertenece",
    "comité",
    "comites",
    "grupos de amistad",
    "ordinarias",
    "especiales",
    "comisiones bicamarales",
    "investigación",
    "investigacion",
    "informe de comisiones",
    "directorio",
    "buscador",
    "calendario de reuniones",
    "memoria de labores",
    "trabajos legislativos",
    "asistencias",
    "votaciones",
    "intervenciones",
    "iniciativas",
    "proposiciones",
    "excitativas",
    "comunicaciones",
    "comisiones",   # header genérico, NO comisión real
    "presidencia",  # header
    "secretaría",   # header
    "secretaria",
    "integrante",
    # navegación SITL
    "diario de los debates",
    "documentos de apoyo parlamentario",
    "expedientes legislativos digitales",
    "gaceta del senado",
    "gaceta parlamentaria de la comisión permanente",
    "iniciativas, minutas, instrumentos",
    "transparencia parlamentaria",
    "marco jurídico",
    "marco juridico",
    "sesión al minuto",
    "sesion al minuto",
    "versión estenográfica",
    "version estenografica",
    "tratados internacionales",
    "nombramientos de personal diplomático",
    "respuestas a proposiciones aprobadas",
    "seguimiento a reformas constitucionales",
    "plazos para la publicación de decretos",
    "sinopsis legislativa",
    "legislación aprobada",
    "legislacion aprobada",
    "internacionales y proposiciones",
    "votacione",   # cortado, también basura
    "orden del día",
    "orden del dia",
    "micrositio comisión permanente",
    "micrositio comision permanente",
    # Más basura del scraper SITL Diputados (sección biográfica)
    "escolaridad",
    "administración pública local",
    "administracion publica local",
    "administración pública federal",
    "administracion publica federal",
    "experiencia legislativa",
    "cargos de elección popular",
    "cargos de eleccion popular",
    "trayectoria política",
    "trayectoria politica",
    # Partidos como "comisión" (bug del scraper)
    "morena",
    "pan",
    "pri",
    "pvem",
    "pt",
    "mc",
    "prd",
    "na",
    "sin partido",
}

# Patrones regex de basura adicional (no en blacklist exacta).
# CUIDADO: NO incluir patrones genéricos como "todo mayúsculas" porque
# el scraper a veces guarda comisiones legítimas en CAPS (ej GANADERÍA,
# MARINA), y esas son comisiones reales del Congreso.
BLACKLIST_REGEX = [
    r"^\d{4}\s*[-–]\s*\d{2,4}$",     # "2021 - 2024", "2015-18"
]
RE_BLACKLIST = [re.compile(p) for p in BLACKLIST_REGEX]


def es_comision_real(nombre: str) -> bool:
    n = (nombre or "").strip().lower()
    raw = (nombre or "").strip()
    if not n or len(n) < 4:
        return False
    if n in BLACKLIST_EXACTA:
        return False
    # Match parcial SOLO con tokens largos (≥ 6 chars). Tokens cortos como
    # 'na' (Nueva Alianza) o 'pt' generan falsos positivos:
    # 'gaNAdería' contiene 'na'. Sin esto descartamos comisiones reales.
    for bl in BLACKLIST_EXACTA:
        if len(bl) >= 6 and bl in n and len(n) <= len(bl) + 8:
            return False
    for rgx in RE_BLACKLIST:
        if rgx.match(raw):
            return False
    return True


def limpiar(cargo_raw: str) -> tuple[str, list[str]]:
    """Devuelve (cargo_limpio, lista_descartados)."""
    if not cargo_raw:
        return "", []
    items = []
    descartados = []
    for parte in cargo_raw.split("|"):
        if ":" not in parte:
            continue
        com, rol = parte.split(":", 1)
        com = com.strip()
        rol = rol.strip()
        if es_comision_real(com):
            items.append(f"{com}:{rol}")
        else:
            descartados.append(parte)
    return "|".join(items), descartados


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    conn = sqlite3.connect(str(DB))
    rows = conn.execute("""
        SELECT id, nombre, comisiones_cargo
          FROM legisladores
         WHERE camara = 'Cámara de Diputados'
           AND comisiones_cargo IS NOT NULL AND comisiones_cargo <> ''
    """).fetchall()
    logger.info(f"A revisar: {len(rows)} diputados con cargo")

    n_limpios, n_quedaron_vacios, n_intactos = 0, 0, 0
    descartados_por_token = {}
    para_re_scrape = []

    for leg_id, nombre, cargo in rows:
        nuevo, descartados = limpiar(cargo)
        if not descartados:
            n_intactos += 1
            continue
        n_limpios += 1
        for d in descartados:
            tok = d.split(":")[0]
            descartados_por_token[tok] = descartados_por_token.get(tok, 0) + 1
        if not nuevo:
            n_quedaron_vacios += 1
            para_re_scrape.append((leg_id, nombre))
        if args.dry_run:
            continue
        com_lista = ", ".join(p.split(":", 1)[0] for p in nuevo.split("|") if p)
        conn.execute("""
            UPDATE legisladores
               SET comisiones_cargo = ?, comisiones = ?
             WHERE id = ?
        """, (nuevo, com_lista, leg_id))

    if not args.dry_run:
        conn.commit()

    print()
    print("=" * 60)
    print("== Limpieza Diputados ==")
    print("=" * 60)
    print(f"  Intactos:            {n_intactos}")
    print(f"  Limpiados:           {n_limpios}")
    print(f"  Quedaron vacíos:     {n_quedaron_vacios}")
    print()
    print("Tokens descartados (top 15):")
    for tok, n in sorted(descartados_por_token.items(), key=lambda x: -x[1])[:15]:
        print(f"  {tok[:50]:50} {n}")

    if args.dry_run:
        print("\n*** DRY RUN ***")


if __name__ == "__main__":
    main()
