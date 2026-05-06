"""
Integra el scrape oficial del Senado (senado.gob.mx) a la tabla
sil_documentos. Reemplaza las iniciativas y proposiciones de Senado
LXVI por los datos limpios — ya validados contra Excélsior (10/10).

Estrategia:
  1. Borra todas las filas LXVI Cámara de Senadores con tipo_grupo
     en {Iniciativa, Proposición con PA} de sil_documentos.
  2. Inserta las nuevas desde dashboard/senadores_lxvi_oficial.json.
  3. Imprime validación contra los 10 senadores top de Robles.

NO toca:
  · Diputados (otra fuente)
  · Comisiones de Permanente (otra fuente)
  · Dictámenes, comunicados, efemérides (no vienen del scrape oficial)

Idempotente: correr varias veces deja el mismo estado.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
JSON_INPUT = ROOT / "dashboard" / "senadores_lxvi_oficial.json"

MESES = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}


def parsear_fecha_es(fecha_str: str) -> str:
    """'Miércoles 29 de abril de 2026' -> '2026-04-29' (vacío si no parsea)."""
    if not fecha_str:
        return ""
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", fecha_str.lower())
    if not m:
        return ""
    dia, mes_nombre, anio = m.group(1), m.group(2), m.group(3)
    mes = MESES.get(mes_nombre, "")
    if not mes:
        return ""
    return f"{anio}-{mes}-{dia.zfill(2)}"


def derivar_periodo(fecha_iso: str) -> str:
    """LXVI: ord 1 sept-15 dic, rec 16 dic-31 ene, ord 1 feb-30 abr, rec 1 may-31 ago."""
    if not fecha_iso or len(fecha_iso) < 10:
        return ""
    try:
        anio, mes, dia = int(fecha_iso[:4]), int(fecha_iso[5:7]), int(fecha_iso[8:10])
    except Exception:
        return ""
    if mes >= 9 or (mes == 12 and dia <= 15):
        return "1er Ordinario"
    if (mes == 12 and dia >= 16) or mes == 1:
        return "1er Receso"
    if 2 <= mes <= 4:
        return "2do Ordinario"
    return "2do Receso"


def extraer_comision_de_turno(turno: str) -> str:
    """De 'Se dio turno directo a las Comisiones Unidas de X y de Y' devuelve X / Y."""
    if not turno:
        return ""
    # Quitar prefijos típicos
    s = re.sub(
        r"^.*?(?:turn[óo]\s+(?:directo\s+)?a\s+(?:las?\s+)?(?:Comisi[oó]n(?:es)?\s+(?:Unidas\s+)?(?:de\s+)?)?)",
        "",
        turno,
        flags=re.IGNORECASE,
    )
    s = re.sub(r"\.$", "", s).strip()
    return s[:300]


def extraer_gaceta_id(enlace: str) -> str:
    """De URL 'gaceta_del_senado/documento/156216' devuelve '156216'."""
    if not enlace:
        return ""
    m = re.search(r"gaceta_del_senado/documento/(\d+)", enlace)
    return m.group(1) if m else ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="No escribe BD")
    ap.add_argument("--input", default=str(JSON_INPUT))
    args = ap.parse_args()

    if not Path(args.input).exists():
        print(f"ERROR: no existe {args.input}", file=sys.stderr)
        sys.exit(1)

    data = json.loads(Path(args.input).read_text())
    print(f"Cargado: {len(data['senadores'])} senadores, {len(data['instrumentos'])} instrumentos")

    conn = sqlite3.connect(str(DB))
    conn.execute("PRAGMA journal_mode=WAL")

    # Asegurar que las columnas que necesitamos existan (cache viejo del seed
    # puede no traer tipo_grupo, clasificacion, etc.).
    cols_existentes = {
        r[1] for r in conn.execute("PRAGMA table_info(sil_documentos)").fetchall()
    }
    for col, ddl in [
        ("tipo_grupo", "ALTER TABLE sil_documentos ADD COLUMN tipo_grupo TEXT DEFAULT ''"),
        ("clasificacion", "ALTER TABLE sil_documentos ADD COLUMN clasificacion TEXT DEFAULT ''"),
        ("tipo_inferido", "ALTER TABLE sil_documentos ADD COLUMN tipo_inferido TEXT DEFAULT ''"),
        ("presentador", "ALTER TABLE sil_documentos ADD COLUMN presentador TEXT DEFAULT ''"),
        ("tipo_presentador", "ALTER TABLE sil_documentos ADD COLUMN tipo_presentador TEXT DEFAULT ''"),
    ]:
        if col not in cols_existentes:
            print(f"  Schema migration: agregando columna {col}")
            conn.execute(ddl)
    conn.commit()

    # 1. Borrar filas existentes (solo Senado, solo iniciativas/proposiciones)
    cur = conn.execute("""
        SELECT COUNT(*) FROM sil_documentos
        WHERE legislatura = 'LXVI'
          AND camara = 'Cámara de Senadores'
          AND tipo_grupo IN ('Iniciativa', 'Proposición con PA')
    """)
    pre_count = cur.fetchone()[0]
    print(f"Filas LXVI Senado iniciativas/proposiciones existentes: {pre_count}")

    if not args.dry_run:
        deleted = conn.execute("""
            DELETE FROM sil_documentos
            WHERE legislatura = 'LXVI'
              AND camara = 'Cámara de Senadores'
              AND tipo_grupo IN ('Iniciativa', 'Proposición con PA')
        """).rowcount
        print(f"  → borradas {deleted}")
        conn.commit()

    # 2. Insertar las nuevas
    ahora = datetime.now().isoformat()
    insertadas = 0
    saltadas = 0
    sin_fecha = 0
    for inst in data["instrumentos"]:
        fecha = parsear_fecha_es(inst.get("fecha", ""))
        if not fecha:
            sin_fecha += 1
        gaceta_id = extraer_gaceta_id(inst.get("enlace_gaceta", ""))
        if not gaceta_id:
            # Fallback: usar hash deterministico para evitar colisiones
            import hashlib
            seed = f"{inst.get('senador_id')}|{inst.get('titulo','')[:80]}|{fecha}|{inst.get('tipo')}"
            gaceta_id = hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]
        seg_id = f"SEN_{gaceta_id}"
        asu_id = f"SEN_{gaceta_id}"

        es_iniciativa = inst.get("tipo") == "iniciativa"
        tipo_grupo = "Iniciativa" if es_iniciativa else "Proposición con PA"
        tipo_oficial = "Iniciativa" if es_iniciativa else "Proposición con punto de acuerdo"

        partido = inst.get("senador_partido", "") or ""
        senador_nombre = inst.get("senador_nombre", "") or ""
        presentador = f"Sen. {senador_nombre}" + (f" ({partido})" if partido else "")
        comision = extraer_comision_de_turno(inst.get("turno", ""))
        periodo = derivar_periodo(fecha)
        clasificacion = "legislativa"  # Iniciativas y proposiciones son siempre actos legislativos

        if args.dry_run:
            insertadas += 1
            continue

        try:
            conn.execute("""
                INSERT OR IGNORE INTO sil_documentos
                  (seguimiento_id, asunto_id, tipo, titulo, sinopsis, camara,
                   fecha_presentacion, legislatura, periodo, estatus, partido,
                   comision, categoria, fecha_scraping, presentador,
                   tipo_presentador, tipo_grupo, clasificacion)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                seg_id, asu_id, tipo_oficial,
                inst.get("titulo", "")[:500],
                inst.get("promoventes_raw", "")[:500],
                "Cámara de Senadores",
                fecha, "LXVI", periodo,
                "",  # estatus se llena después con cruce a comisiones_stats
                partido, comision, "",
                ahora, presentador, "legislador", tipo_grupo, clasificacion,
            ))
            insertadas += 1
        except sqlite3.IntegrityError:
            saltadas += 1
        except Exception as e:
            print(f"ERROR insertando: {e}", file=sys.stderr)

    if not args.dry_run:
        conn.commit()
    print(f"Insertadas: {insertadas}")
    print(f"Saltadas (duplicado por seg_id): {saltadas}")
    print(f"Sin fecha parseable: {sin_fecha}")

    # 3. Validación contra Robles
    print()
    print("=== Validación post-inserción contra Robles ===")
    ROBLES = [
        ("Pablo Guillermo Angulo", 222),
        ("Karen Michel González", 165),
        ("Rocío Corona Nakamura", 193),
        ("Olga Patricia Sosa", 87),
        ("Enrique Vargas del Villar", 206),
        ("Saúl Monreal", 79),
        ("Martina Kantún", 84),
        ("Juan Antonio Martín del Campo", 99),
        ("Virgilio Mendoza Amezcua", 126),
        ("Miguel Ángel Riquelme", 100),
    ]
    for nombre, robles in ROBLES:
        n = conn.execute("""
            SELECT COUNT(*) FROM sil_documentos
            WHERE legislatura = 'LXVI'
              AND camara = 'Cámara de Senadores'
              AND tipo_grupo = 'Iniciativa'
              AND presentador LIKE ?
        """, (f"%{nombre}%",)).fetchone()[0]
        delta = abs(n - robles) / robles * 100 if robles else 0
        marker = "✓" if delta <= 5 else "⚠"
        print(f"  {nombre:38s} BD={n:3d} Robles={robles:3d}  Δ={delta:5.1f}%  {marker}")


if __name__ == "__main__":
    main()
