"""
Carga la composición oficial de la Comisión Permanente
LXVI · Segundo Receso del Segundo Año (mayo-agosto 2026).

Datos extraídos del Tríptico oficial publicado el 29-04-2026.
Fuente: Cámara de Diputados, DGAP.

Crea/llena tabla `permanente_integrantes` en BD local y sync a D1.

Uso:
    python scripts/cargar_permanente_lxvi.py
    python scripts/cargar_permanente_lxvi.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

# ── Composición oficial 2do Receso 2do Año (mayo-agosto 2026) ─────────────
# Formato: (camara, partido, rol, nombre)
# rol: 'Titular' | 'Sustituto'

PERMANENTE_LXVI_2REC_2A = [
    # ── SENADO ──
    ('Senado', 'MORENA', 'Titular',   'Laura Itzel Castillo Juárez'),
    ('Senado', 'MORENA', 'Sustituto', 'Adán Augusto López Hernández'),
    ('Senado', 'MORENA', 'Titular',   'Verónica Noemí Camino Farjat'),
    ('Senado', 'MORENA', 'Sustituto', 'Moisés Ignacio Mier Velazco'),
    ('Senado', 'MORENA', 'Titular',   'Enrique Inzunza Cázarez'),
    ('Senado', 'MORENA', 'Sustituto', 'Sasil De León Villard'),
    ('Senado', 'MORENA', 'Titular',   'Óscar Cantón Zetina'),
    ('Senado', 'MORENA', 'Sustituto', 'Nora Elena Yu Hernández'),
    ('Senado', 'MORENA', 'Titular',   'Homero Davis Castro'),
    ('Senado', 'MORENA', 'Sustituto', 'Cuauhtémoc Ochoa Fernández'),
    ('Senado', 'MORENA', 'Titular',   'Julieta Andrea Ramírez Padilla'),
    ('Senado', 'MORENA', 'Sustituto', 'Martha Lucía Micher Camarena'),
    ('Senado', 'MORENA', 'Titular',   'Manuel Huerta Ladrón de Guevara'),
    ('Senado', 'MORENA', 'Sustituto', 'Alejandro Ismael Murat Hinojosa'),
    ('Senado', 'MORENA', 'Titular',   'María Martina Kantún Can'),
    ('Senado', 'MORENA', 'Sustituto', 'José Gerardo Rodolfo Fernández Noroña'),
    ('Senado', 'MORENA', 'Titular',   'Ana Lilia Rivera Rivera'),
    ('Senado', 'MORENA', 'Sustituto', 'Sandra Simey Olvera Bautista'),

    ('Senado', 'PAN', 'Titular',   'Enrique Vargas del Villar'),
    ('Senado', 'PAN', 'Sustituto', 'Erik Iván Jaimes Archundia'),
    ('Senado', 'PAN', 'Titular',   'Mayuli Latifa Martínez Simón'),
    ('Senado', 'PAN', 'Sustituto', 'Ricardo Anaya Cortés'),
    ('Senado', 'PAN', 'Titular',   'Lilly Téllez García'),
    ('Senado', 'PAN', 'Sustituto', 'José Máximo García López'),

    ('Senado', 'PVEM', 'Titular',   'Juanita Guerra Mena'),
    ('Senado', 'PVEM', 'Sustituto', 'Karen Castrejón Trujillo'),
    ('Senado', 'PVEM', 'Titular',   'Jorge Carlos Ramírez Marín'),
    ('Senado', 'PVEM', 'Sustituto', 'Luis Armando Melgar Bravo'),

    ('Senado', 'PRI', 'Titular',   'Alma Carolina Viggiano Austria'),
    ('Senado', 'PRI', 'Sustituto', 'Anabell Ávalos Zempoalteca'),
    ('Senado', 'PRI', 'Titular',   'Manuel Añorve Baños'),
    ('Senado', 'PRI', 'Sustituto', 'Rafael Alejandro Moreno Cárdenas'),

    ('Senado', 'PT', 'Titular',   'Lizeth Sánchez García'),
    ('Senado', 'PT', 'Sustituto', 'Geovanna del Carmen Bañuelos De la Torre'),

    ('Senado', 'MC', 'Titular',   'José Clemente Castañeda Hoeflich'),
    ('Senado', 'MC', 'Sustituto', 'Néstor Camarillo Medina'),

    # ── DIPUTADOS ──
    ('Diputados', 'MORENA', 'Titular',   'Gabriel García Hernández'),
    ('Diputados', 'MORENA', 'Sustituto', 'Carina Piceno'),
    ('Diputados', 'MORENA', 'Titular',   'Alma Lidia De La Vega Sánchez'),
    ('Diputados', 'MORENA', 'Sustituto', 'María Teresa Ealy Díaz'),
    ('Diputados', 'MORENA', 'Titular',   'Arturo Ávila Anaya'),
    ('Diputados', 'MORENA', 'Sustituto', 'Zaria Aguilera Claro'),
    ('Diputados', 'MORENA', 'Titular',   'Margarita Corro Mendoza'),
    ('Diputados', 'MORENA', 'Sustituto', 'Martha Olivia García Vidaña'),
    ('Diputados', 'MORENA', 'Titular',   'Joaquín Zebadúa Alva'),
    ('Diputados', 'MORENA', 'Sustituto', 'Karina Margarita del Río Zenteno'),
    ('Diputados', 'MORENA', 'Titular',   'María del Rosario Orozco Caballero'),
    ('Diputados', 'MORENA', 'Sustituto', 'Mario Miguel Carrillo Cubillas'),
    ('Diputados', 'MORENA', 'Titular',   'Alberto Maldonado Chavarín'),
    ('Diputados', 'MORENA', 'Sustituto', 'Beatriz Carranza Gómez'),
    ('Diputados', 'MORENA', 'Titular',   'Beatriz Andrea Navarro Pérez'),
    ('Diputados', 'MORENA', 'Sustituto', 'Gilberto Herrera Solórzano'),
    ('Diputados', 'MORENA', 'Titular',   'Gabino Morales Mendoza'),
    ('Diputados', 'MORENA', 'Sustituto', 'María Magdalena Rosales Cruz'),
    ('Diputados', 'MORENA', 'Titular',   'Yoloczin Lizbeth Domínguez Serna'),
    ('Diputados', 'MORENA', 'Sustituto', 'Ricardo Monreal Ávila'),

    ('Diputados', 'PAN', 'Titular',   'Kenia López Rabadán'),
    ('Diputados', 'PAN', 'Sustituto', 'José Elías Lixa Abimerhi'),
    ('Diputados', 'PAN', 'Titular',   'Verónica Pérez Herrera'),
    ('Diputados', 'PAN', 'Sustituto', 'Ma. Lorena García Jimeno Alcocer'),
    ('Diputados', 'PAN', 'Titular',   'Marcelo de Jesús Torres Cofiño'),
    ('Diputados', 'PAN', 'Sustituto', 'Homero Ricardo Niño de Rivera Vela'),

    ('Diputados', 'PVEM', 'Titular',   'Cindy Winkler Trujillo'),
    ('Diputados', 'PVEM', 'Sustituto', 'Héctor Alfonso de la Garza Villarreal'),
    ('Diputados', 'PVEM', 'Titular',   'Marco Antonio De La Mora Torreblanca'),
    ('Diputados', 'PVEM', 'Sustituto', 'Ricardo Madrid Pérez'),

    ('Diputados', 'PT', 'Titular',   'Reginaldo Sandoval Flores'),
    ('Diputados', 'PT', 'Sustituto', 'Francisco Amadeo Espinosa Ramos'),
    ('Diputados', 'PT', 'Titular',   'Mary Carmen Bernal Martínez'),
    ('Diputados', 'PT', 'Sustituto', 'Margarita García García'),

    ('Diputados', 'PRI', 'Titular',   'Rubén Ignacio Moreira Valdez'),
    ('Diputados', 'PRI', 'Sustituto', 'Christian Mishel Castro Bello'),

    ('Diputados', 'MC', 'Titular',   'Gibrán Ramírez Reyes'),
    ('Diputados', 'MC', 'Sustituto', 'Pablo Vázquez Ahued'),
]

PERIODO_ID = 'LXVI_2REC_2A'  # Segundo Receso del Segundo Año (mayo-ago 2026)


def crear_tabla(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS permanente_integrantes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            periodo_id TEXT NOT NULL,
            camara TEXT NOT NULL,
            partido TEXT NOT NULL,
            rol TEXT NOT NULL,
            nombre TEXT NOT NULL,
            legislador_id INTEGER,
            sitl_id TEXT,
            cargado_en TEXT NOT NULL,
            UNIQUE(periodo_id, nombre)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_perm_periodo_camara ON permanente_integrantes(periodo_id, camara)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_perm_legislador ON permanente_integrantes(legislador_id)")


def matchear_legislador(conn, nombre, camara):
    """Busca el legislador_id por nombre + cámara via LIKE flexible."""
    import unicodedata
    def norm(s):
        s = unicodedata.normalize('NFKD', s or '')
        return ''.join(c for c in s if not unicodedata.combining(c)).lower().strip()
    tokens = norm(nombre).split()
    if len(tokens) < 2:
        return None, None
    # Match con 2 tokens distintivos (apellido + nombre principal)
    sig1 = tokens[0]
    sig2 = tokens[-1] if len(tokens) > 2 else tokens[1]
    pattern = f'%{sig1}%' if sig1 != sig2 else f'%{sig1}%'
    cam_filter = "Senado" if camara == "Senado" else "Diputados"
    rows = conn.execute(f"""
        SELECT id, sitl_id, nombre FROM legisladores
         WHERE camara LIKE '%{cam_filter}%'
    """).fetchall()
    # Buscar el que tenga AMBOS tokens en su nombre normalizado
    for r in rows:
        nn = norm(r[2])
        if sig1 in nn and sig2 in nn:
            return r[0], r[1]
    return None, None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    conn = sqlite3.connect(str(DB))
    crear_tabla(conn)
    ahora = datetime.utcnow().isoformat(timespec='seconds')

    matched = 0
    no_matched = []
    for camara, partido, rol, nombre in PERMANENTE_LXVI_2REC_2A:
        leg_id, sitl_id = matchear_legislador(conn, nombre, camara)
        if not leg_id:
            no_matched.append((camara, nombre))
        else:
            matched += 1
        if args.dry_run:
            tag = '✓' if leg_id else '✗'
            logger.info(f"  {tag} {camara[:3]:3} {partido:6} {rol:9} {nombre[:40]:40} → leg_id={leg_id}")
            continue
        conn.execute("""
            INSERT INTO permanente_integrantes
              (periodo_id, camara, partido, rol, nombre, legislador_id, sitl_id, cargado_en)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(periodo_id, nombre) DO UPDATE SET
              camara=excluded.camara, partido=excluded.partido, rol=excluded.rol,
              legislador_id=excluded.legislador_id, sitl_id=excluded.sitl_id,
              cargado_en=excluded.cargado_en
        """, (PERIODO_ID, camara, partido, rol, nombre, leg_id, sitl_id, ahora))

    if not args.dry_run:
        conn.commit()

    # Stats
    print(f"\n=== Carga Permanente {PERIODO_ID} ===")
    print(f"  Total integrantes:       {len(PERMANENTE_LXVI_2REC_2A)}")
    print(f"  Matched con BD legisladores: {matched}/{len(PERMANENTE_LXVI_2REC_2A)}")
    if no_matched:
        print(f"\n  Sin match ({len(no_matched)}):")
        for cam, n in no_matched[:10]:
            print(f"    {cam}: {n}")

    # Composición política
    from collections import Counter
    titulares = [(c, p, r, n) for c, p, r, n in PERMANENTE_LXVI_2REC_2A if r == 'Titular']
    print(f"\n  Titulares: {len(titulares)}")
    print(f"    Senado: {sum(1 for c,_,_,_ in titulares if c == 'Senado')}")
    print(f"    Diputados: {sum(1 for c,_,_,_ in titulares if c == 'Diputados')}")
    print(f"  Por partido (titulares):")
    for partido, n in Counter(p for _, p, _, _ in titulares).most_common():
        pct = n / len(titulares) * 100
        print(f"    {partido:8} {n:2} ({pct:.0f}%)")


if __name__ == "__main__":
    main()
