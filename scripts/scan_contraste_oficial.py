"""
SCAN DE PRECISIÓN: contrasta nuestros datos vs fuentes oficiales.

Verifica:
  1. INSTRUMENTOS por senador: nuestro conteo vs senado.gob.mx perfil
  2. COMISIONES Senado: integrantes nuestros vs página oficial comisión

Para cada caso, calcula:
  - Diferencia absoluta y porcentual
  - "Acierto" = diferencia <= 5% del oficial

Resultado: reporte con accuracy global y por categoría.

Uso:
  python3 scripts/scan_contraste_oficial.py
  python3 scripts/scan_contraste_oficial.py --muestra 30
"""
import argparse
import logging
import random
import re
import sqlite3
import sys
import time
from pathlib import Path

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
HEADERS = {"User-Agent": UA, "Accept": "text/html,*/*;q=0.8", "Accept-Language": "es-MX,es;q=0.9"}

# Catálogo de comisiones a verificar (id, nombre)
from scrapers.comisiones_senado_asuntos import CATALOGO_LXVI, _slug_referer


def fetch_perfil_senador(senador_id: int, partido: str):
    """Cuenta iniciativas + proposiciones del senador desde senado.gob.mx."""
    url_base = "https://www.senado.gob.mx/66/doc/asuntoSenador.php"
    counts = {}
    for tipo, var1 in [("iniciativas", "iniciativas"), ("proposiciones", "proposiciones")]:
        try:
            r = requests.get(
                f"{url_base}?var1={var1}&var2={senador_id}&var3={partido or ''}",
                headers=HEADERS, timeout=30, verify=False,
            )
            if r.status_code != 200:
                counts[tipo] = None
                continue
            # Buscar "Resultados encontrados : <strong>N</strong>"
            m = re.search(r"Resultados encontrados\s*:\s*<strong>(\d+)</strong>", r.text)
            counts[tipo] = int(m.group(1)) if m else 0
        except Exception as e:
            logger.warning(f"  Error {tipo} {senador_id}: {e}")
            counts[tipo] = None
        time.sleep(1.5)
    return counts


def fetch_comision_senado_integrantes(id_com: int, nombre: str):
    """Cuenta integrantes de una comisión Senado desde su página oficial."""
    slug = _slug_referer(nombre)
    url = f"https://comisiones.senado.gob.mx/{slug}/integrantes.php"
    try:
        r = requests.get(url, headers=HEADERS, timeout=30, verify=False)
        if r.status_code != 200:
            return None
        # Cada integrante es <li> dentro de ul.list-group o un patrón similar.
        # Heurística: contar enlaces a perfiles de senador.
        n_perfil = len(re.findall(r"/66/senador/\d+", r.text))
        # Fallback: contar "Sen." mentions
        if n_perfil == 0:
            n_perfil = len(re.findall(r"\bSen\.\s+[A-ZÁÉÍÓÚÑ]", r.text))
        return n_perfil
    except Exception as e:
        logger.warning(f"  Error comision {id_com}: {e}")
        return None


def scan_senadores(conn, muestra: int = 20):
    print("\n" + "=" * 70)
    print("SCAN 1: INSTRUMENTOS por senador (vs senado.gob.mx perfil)")
    print("=" * 70)
    # Obtener senador_id_senado del scrape oficial
    rows = conn.execute("""
        SELECT DISTINCT senador_id_senado, senador_nombre, senador_partido,
            (SELECT COUNT(*) FROM senador_instrumento si2
             WHERE si2.senador_id_senado = si.senador_id_senado
               AND si2.tipo_instrumento LIKE '%Iniciativa%') as nuestro_ini,
            (SELECT COUNT(*) FROM senador_instrumento si2
             WHERE si2.senador_id_senado = si.senador_id_senado
               AND si2.tipo_instrumento LIKE '%Proposici%') as nuestro_prop
        FROM senador_instrumento si
        WHERE senador_id_senado > 0
        ORDER BY RANDOM()
        LIMIT ?
    """, (muestra,)).fetchall()

    print(f"{'Senador':<35} {'Partido':<6} {'Ini ofi':>8} {'Ini FIAT':>10} {'diff':>6} | {'Prop ofi':>9} {'Prop FIAT':>11} {'diff':>6}")
    print("-" * 100)

    aciertos_ini = 0
    aciertos_prop = 0
    total = 0
    deltas = []

    for r in rows:
        sid = r[0]
        nombre = r[1] or "?"
        partido = r[2] or ""
        n_ini = r[3] or 0
        n_prop = r[4] or 0

        oficial = fetch_perfil_senador(sid, partido)
        ofi_ini = oficial.get("iniciativas")
        ofi_prop = oficial.get("proposiciones")
        if ofi_ini is None or ofi_prop is None:
            continue

        total += 1
        d_ini = n_ini - ofi_ini
        d_prop = n_prop - ofi_prop
        deltas.append((d_ini, d_prop, ofi_ini + ofi_prop))

        # Acierto si dentro de 5% (o ±2 absoluto para conteos chicos)
        umbral_ini = max(2, int(ofi_ini * 0.05))
        umbral_prop = max(2, int(ofi_prop * 0.05))
        if abs(d_ini) <= umbral_ini:
            aciertos_ini += 1
        if abs(d_prop) <= umbral_prop:
            aciertos_prop += 1

        print(f"{nombre[:33]:<35} {partido[:5]:<6} {ofi_ini:>8} {n_ini:>10} {d_ini:>+6} | {ofi_prop:>9} {n_prop:>11} {d_prop:>+6}")

    print("-" * 100)
    if total > 0:
        print(f"\nMuestra: {total} senadores")
        print(f"Acierto Iniciativas (±5%): {aciertos_ini}/{total} = {100*aciertos_ini/total:.0f}%")
        print(f"Acierto Proposiciones (±5%): {aciertos_prop}/{total} = {100*aciertos_prop/total:.0f}%")
        # Diff promedio
        avg_d_ini = sum(d[0] for d in deltas) / len(deltas)
        avg_d_prop = sum(d[1] for d in deltas) / len(deltas)
        print(f"Diferencia promedio Iniciativas: {avg_d_ini:+.1f} (negativo = nos faltan)")
        print(f"Diferencia promedio Proposiciones: {avg_d_prop:+.1f}")
    return (aciertos_ini + aciertos_prop) / (2 * total) if total > 0 else 0


def scan_comisiones(conn, muestra: int = 10):
    print("\n" + "=" * 70)
    print("SCAN 2: INTEGRANTES por comisión Senado (vs página oficial)")
    print("=" * 70)
    # Tomar muestra aleatoria del catálogo
    ids_sample = random.sample(list(CATALOGO_LXVI.items()), min(muestra, len(CATALOGO_LXVI)))

    print(f"{'Comisión':<55} {'Ofi':>5} {'FIAT':>6} {'diff':>6}")
    print("-" * 80)

    aciertos = 0
    total = 0
    for id_com, nombre in ids_sample:
        # Nuestro conteo: cuántos legisladores con esa comisión en comisiones_cargo
        n_fiat = conn.execute(
            "SELECT COUNT(*) FROM legisladores WHERE camara LIKE '%Senado%' AND comisiones_cargo LIKE ?",
            (f"%{nombre}%",)
        ).fetchone()[0]

        n_ofi = fetch_comision_senado_integrantes(id_com, nombre)
        time.sleep(2.0)
        if n_ofi is None or n_ofi == 0:
            print(f"{nombre[:53]:<55} {'?':>5} {n_fiat:>6} (no se pudo verificar)")
            continue
        total += 1
        diff = n_fiat - n_ofi
        if abs(diff) <= 2:  # ±2 miembros tolerancia
            aciertos += 1
        print(f"{nombre[:53]:<55} {n_ofi:>5} {n_fiat:>6} {diff:>+6}")

    print("-" * 80)
    if total > 0:
        print(f"\nMuestra: {total} comisiones")
        print(f"Acierto (±2 miembros): {aciertos}/{total} = {100*aciertos/total:.0f}%")
    return aciertos / total if total > 0 else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--muestra", type=int, default=20)
    ap.add_argument("--solo", choices=["senadores", "comisiones"], default=None)
    args = ap.parse_args()

    from db import get_connection
    conn = get_connection()

    print("\n╔" + "═" * 68 + "╗")
    print("║" + "  SCAN DE PRECISIÓN FIAT vs DATOS OFICIALES SENADO  ".center(68) + "║")
    print("╚" + "═" * 68 + "╝")

    if args.solo != "comisiones":
        acc1 = scan_senadores(conn, muestra=args.muestra)
    else:
        acc1 = None
    if args.solo != "senadores":
        acc2 = scan_comisiones(conn, muestra=min(args.muestra, 15))
    else:
        acc2 = None

    print("\n" + "=" * 70)
    print("RESUMEN GLOBAL")
    print("=" * 70)
    if acc1 is not None:
        print(f"  Senadores instrumentos: {acc1*100:.0f}% acierto")
    if acc2 is not None:
        print(f"  Comisiones integrantes: {acc2*100:.0f}% acierto")
    print()


if __name__ == "__main__":
    main()
