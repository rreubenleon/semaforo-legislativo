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
        # n_firmantes en sil_documentos es info DEL INSTRUMENTO (no del
        # legislador). La relación N:M legislador↔instrumento vive en la
        # tabla senador_instrumento (creada abajo).
        ("n_firmantes", "ALTER TABLE sil_documentos ADD COLUMN n_firmantes INTEGER DEFAULT 1"),
        ("es_individual", "ALTER TABLE sil_documentos ADD COLUMN es_individual INTEGER DEFAULT 1"),
        ("url", "ALTER TABLE sil_documentos ADD COLUMN url TEXT DEFAULT ''"),
    ]:
        if col not in cols_existentes:
            print(f"  Schema migration: agregando columna {col}")
            conn.execute(ddl)

    # Tabla relacional 1:N. Cada iniciativa firmada por 13 senadores
    # genera 13 filas aquí (una por perfil de senador). Permite
    # responder "iniciativas individuales de Angulo" filtrando por
    # senador_id_perfil + es_individual_perfil sin que el dedup de
    # sil_documentos por seg_id colapse información.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS senador_instrumento (
            seguimiento_id TEXT NOT NULL,
            asunto_id TEXT NOT NULL,
            senador_id_senado INTEGER NOT NULL,
            senador_nombre TEXT,
            senador_partido TEXT,
            es_individual_perfil INTEGER NOT NULL DEFAULT 0,
            n_firmantes_perfil INTEGER NOT NULL DEFAULT 1,
            tipo_instrumento TEXT,
            fecha_scraping TEXT,
            PRIMARY KEY (seguimiento_id, asunto_id, senador_id_senado)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_si_senador ON senador_instrumento(senador_id_senado)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_si_individual ON senador_instrumento(senador_id_senado, es_individual_perfil)")
    conn.commit()

    # 1. Borrar filas existentes — Senado iniciativas/proposiciones.
    # CUIDADO: hay filas del SIL Gobernación que tienen tipo_grupo VACÍO
    # (no clasificadas) pero sí están como tipo='Iniciativa' o
    # 'Proposición con punto de acuerdo'. Esas también deben borrarse o se
    # duplican con las nuevas del scrape oficial del Senado.
    # MODO ENRIQUECIMIENTO con DEDUPE: para cada entry del scrape oficial,
    # buscar si ya existe en sil_documentos por (título primer 80 chars,
    # fecha, senador en presentador). Si match → UPDATE flags (es_individual,
    # n_firmantes). Si no match → INSERT como SEN_*.
    # Resultado:
    #  - Sin duplicados (las que SIL Gob ya tenía se enriquecen)
    #  - Sin pérdida (las que SIL Gob no tenía se añaden)
    #  - Sin tocar comunicados/dictámenes/efemérides
    print("MODO ENRIQUECIMIENTO: dedupe por título+fecha+senador")

    if not args.dry_run:
        deleted_rel = conn.execute("""
            DELETE FROM senador_instrumento
            WHERE seguimiento_id LIKE 'SEN_%'
        """).rowcount
        print(f"  → borradas senador_instrumento (tabla auxiliar Senado): {deleted_rel}")
        conn.commit()

    # Pre-cargar índice de iniciativas+proposiciones LXVI Senado existentes
    # para hacer matching en memoria (más rápido que SELECT por cada uno).
    # Key: (titulo_primer_60_lower, fecha) → lista de (id, presentador)
    existentes_idx = {}
    rows_existentes = conn.execute("""
        SELECT id, titulo, fecha_presentacion, presentador
        FROM sil_documentos
        WHERE legislatura = 'LXVI'
          AND camara = 'Cámara de Senadores'
          AND tipo_presentador = 'legislador'
          AND (
            tipo_grupo IN ('Iniciativa', 'Proposición con PA')
            OR tipo LIKE 'Iniciativa%'
            OR tipo LIKE 'Proposici%con%punto%acuerdo%'
          )
    """).fetchall()
    print(f"Filas SIL Gob LXVI Senado ini/prop existentes para match: {len(rows_existentes)}")
    for row_id, titulo, fecha, presentador in rows_existentes:
        if not titulo or not fecha:
            continue
        key = (titulo[:60].lower().strip(), fecha)
        existentes_idx.setdefault(key, []).append((row_id, presentador or ""))

    def _buscar_match(titulo_scrape, fecha_scrape, senador_apellido):
        """Busca fila SIL Gob que matchee con esta entry del scrape."""
        if not titulo_scrape or not fecha_scrape:
            return None
        # Normalizar título: quitar acentos, lowercase, primer 60 chars
        import unicodedata as _ud
        t = _ud.normalize('NFKD', titulo_scrape.lower())
        t = ''.join(c for c in t if not _ud.combining(c))
        key = (t[:60].strip(), fecha_scrape)
        candidatos = existentes_idx.get(key, [])
        if not candidatos:
            return None
        # De los candidatos por título+fecha, preferir el que tiene el
        # senador en el campo presentador
        ap_norm = _ud.normalize('NFKD', senador_apellido.lower())
        ap_norm = ''.join(c for c in ap_norm if not _ud.combining(c))
        for row_id, pres in candidatos:
            pres_norm = _ud.normalize('NFKD', (pres or '').lower())
            pres_norm = ''.join(c for c in pres_norm if not _ud.combining(c))
            if ap_norm in pres_norm:
                return row_id
        # Si no, devolver primer candidato (matchea por titulo+fecha)
        return candidatos[0][0]

    # 2. AGRUPAR por instrumento (seg_id). Cada iniciativa firmada por 13
    # senadores aparece 13 veces en el JSON (una por perfil). Para
    # sil_documentos necesitamos UNA fila por instrumento con la lista
    # completa de firmantes en formato que _parsear_presentadores reconoce:
    #     'Sen. NOMBRE (PARTIDO)Sen. OTRO (PARTIDO)...'
    # Esto hace que actividad_legislador reciba N filas por iniciativa
    # colectiva (una por cada firmante) y los rankings personales cuadren.
    from collections import defaultdict
    import hashlib
    grupos = defaultdict(list)
    for inst in data["instrumentos"]:
        fecha = parsear_fecha_es(inst.get("fecha", ""))
        gaceta_id = extraer_gaceta_id(inst.get("enlace_gaceta", ""))
        if not gaceta_id:
            seed = f"{inst.get('titulo','')[:80]}|{fecha}|{inst.get('tipo')}"
            gaceta_id = hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]
        seg_id = f"SEN_{gaceta_id}"
        grupos[(seg_id, inst.get("tipo"))].append(inst)

    # 3. Insertar UNA fila por instrumento, con presentador formateado
    ahora = datetime.now().isoformat()
    insertadas = 0
    saltadas = 0
    sin_fecha = 0
    for (seg_id, tipo_inst), entries in grupos.items():
        primer = entries[0]
        # Firmantes únicos (por senador_id) preservando orden de aparición
        firmantes_seen = set()
        firmantes_orden = []
        for e in entries:
            sid = e.get("senador_id")
            if sid in firmantes_seen:
                continue
            firmantes_seen.add(sid)
            firmantes_orden.append(e)

        # Formato CRÍTICO compatible con scrapers/legisladores._parsear_presentadores:
        # "Sen. NOMBRE (PARTIDO)Sen. OTRO (PARTIDO)..."
        presentador_formateado = "".join(
            f"Sen. {f.get('senador_nombre','')} ({f.get('senador_partido','')})"
            for f in firmantes_orden
            if f.get('senador_nombre')
        )[:1000]  # cap para no romper schema

        n_firmantes = len(firmantes_orden)
        es_individual_int = 1 if n_firmantes == 1 else 0

        fecha = parsear_fecha_es(primer.get("fecha", ""))
        if not fecha:
            sin_fecha += 1
        asu_id = seg_id  # mismo valor

        es_iniciativa = tipo_inst == "iniciativa"
        tipo_grupo = "Iniciativa" if es_iniciativa else "Proposición con PA"
        tipo_oficial = "Iniciativa" if es_iniciativa else "Proposición con punto de acuerdo"

        # Partido: del primer firmante (el principal)
        partido = primer.get("senador_partido", "") or ""
        comision = extraer_comision_de_turno(primer.get("turno", ""))
        periodo = derivar_periodo(fecha)
        clasificacion = "legislativo_sustantivo"

        if args.dry_run:
            insertadas += 1
            continue

        # Buscar match en SIL Gob existente
        senador_apellido = (primer.get("senador_nombre", "") or "").split()[-1] if primer.get("senador_nombre") else ""
        match_id = _buscar_match(
            primer.get("titulo", ""),
            fecha,
            senador_apellido
        )

        if match_id is not None:
            # ENRIQUECER fila existente: añadir es_individual y n_firmantes
            try:
                conn.execute("""
                    UPDATE sil_documentos
                    SET es_individual = ?, n_firmantes = ?
                    WHERE id = ?
                """, (es_individual_int, n_firmantes, match_id))
                # Para tabla relacional, usar seg_id real de la fila SIL Gob
                row_match = conn.execute(
                    "SELECT seguimiento_id, asunto_id FROM sil_documentos WHERE id = ?",
                    (match_id,)
                ).fetchone()
                if row_match:
                    seg_id_real = row_match[0]
                    asu_id_real = row_match[1]
                    # Actualizar el seg_id que usaremos para senador_instrumento
                    seg_id = seg_id_real
                    asu_id = asu_id_real
                saltadas += 1
            except Exception as e:
                print(f"ERROR enriqueciendo: {e}", file=sys.stderr)
        else:
            # NO match → INSERT como SEN_*
            url_doc = primer.get("enlace_gaceta", "")
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO sil_documentos
                      (seguimiento_id, asunto_id, tipo, titulo, sinopsis, camara,
                       fecha_presentacion, legislatura, periodo, estatus, partido,
                       comision, categoria, fecha_scraping, presentador,
                       tipo_presentador, tipo_grupo, clasificacion,
                       n_firmantes, es_individual, url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    seg_id, asu_id, tipo_oficial,
                    primer.get("titulo", "")[:500],
                    primer.get("promoventes_raw", "")[:500],
                    "Cámara de Senadores",
                    fecha, "LXVI", periodo,
                    "", partido, comision, "",
                    ahora, presentador_formateado, "legislador", tipo_grupo, clasificacion,
                    n_firmantes, es_individual_int, url_doc,
                ))
                insertadas += 1
            except sqlite3.OperationalError:
                # url column might not exist yet (legacy schema). Insert sin url.
                conn.execute("""
                    INSERT OR IGNORE INTO sil_documentos
                      (seguimiento_id, asunto_id, tipo, titulo, sinopsis, camara,
                       fecha_presentacion, legislatura, periodo, estatus, partido,
                       comision, categoria, fecha_scraping, presentador,
                       tipo_presentador, tipo_grupo, clasificacion,
                       n_firmantes, es_individual)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    seg_id, asu_id, tipo_oficial,
                    primer.get("titulo", "")[:500],
                    primer.get("promoventes_raw", "")[:500],
                    "Cámara de Senadores",
                    fecha, "LXVI", periodo,
                    "", partido, comision, "",
                    ahora, presentador_formateado, "legislador", tipo_grupo, clasificacion,
                    n_firmantes, es_individual_int,
                ))
                insertadas += 1
            except sqlite3.IntegrityError:
                saltadas += 1
            except Exception as e:
                print(f"ERROR insertando sil_documentos: {e}", file=sys.stderr)

        # Tabla relacional: una fila por (instrumento, firmante).
        for f in firmantes_orden:
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO senador_instrumento
                      (seguimiento_id, asunto_id, senador_id_senado, senador_nombre,
                       senador_partido, es_individual_perfil, n_firmantes_perfil,
                       tipo_instrumento, fecha_scraping)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    seg_id, asu_id,
                    int(f.get("senador_id") or 0),
                    f.get("senador_nombre", "") or "",
                    f.get("senador_partido", "") or "",
                    es_individual_int,
                    n_firmantes,
                    tipo_grupo,
                    ahora,
                ))
            except Exception as e:
                print(f"ERROR insertando senador_instrumento: {e}", file=sys.stderr)

    if not args.dry_run:
        conn.commit()
    print(f"Insertadas: {insertadas}")
    print(f"Saltadas (duplicado por seg_id): {saltadas}")
    print(f"Sin fecha parseable: {sin_fecha}")

    # 3. Validación contra Robles — individuales y colectivas separados
    # Robles publicó: (individuales, firmadas con bancada/otros)
    print()
    print("=== Validación post-inserción contra Robles (Excélsior 4-may-2026) ===")
    print(f"{'Senador':<38s} {'IND':>4s} {'(Rob)':>6s}  {'COL':>4s} {'(Rob)':>6s}")
    ROBLES = [
        ("Pablo Guillermo Angulo", 137, 85),
        ("Karen Michel González", 116, 49),
        ("Rocío Corona Nakamura", 109, 84),
        ("Olga Patricia Sosa", 79, 8),
        ("Enrique Vargas del Villar", 78, 128),
        ("Saúl Monreal", 66, 13),
        ("Martina Kantún", 55, 29),
        ("Juan Antonio Martín del Campo", 48, 51),
        ("Virgilio Mendoza Amezcua", 22, 104),
        ("Miguel Ángel Riquelme", 24, 76),
    ]
    # Conteos correctos: usar la tabla relacional senador_instrumento.
    # Validación por senador_id_senado (no por nombre) porque los nombres
    # en BD pueden venir acortados ("Michel González" sin "Karen") mientras
    # Robles usa el nombre completo.
    ROBLES_BY_ID = [
        (1698, "Pablo Guillermo Angulo", 137, 85),
        (1730, "Karen Michel González", 116, 49),
        (1546, "Rocío Corona Nakamura", 109, 84),
        (1584, "Olga Patricia Sosa", 79, 8),
        (1535, "Enrique Vargas del Villar", 78, 128),
        (1597, "Saúl Monreal", 66, 13),
        (1512, "Martina Kantún", 55, 29),
        (1503, "Juan Antonio Martín del Campo", 48, 51),
        (1527, "Virgilio Mendoza Amezcua", 22, 104),
        (1526, "Miguel Ángel Riquelme", 24, 76),
    ]
    deltas_ind = []
    deltas_col = []
    for sid, nombre, r_ind, r_col in ROBLES_BY_ID:
        row = conn.execute("""
            SELECT
              SUM(CASE WHEN es_individual_perfil = 1 THEN 1 ELSE 0 END) AS ind,
              SUM(CASE WHEN es_individual_perfil = 0 THEN 1 ELSE 0 END) AS col
            FROM senador_instrumento
            WHERE tipo_instrumento = 'Iniciativa'
              AND senador_id_senado = ?
        """, (sid,)).fetchone()
        n_ind = int(row[0] or 0)
        n_col = int(row[1] or 0)
        d_ind = abs(n_ind - r_ind) / r_ind * 100 if r_ind else 0
        d_col = abs(n_col - r_col) / r_col * 100 if r_col else 0
        deltas_ind.append(d_ind)
        deltas_col.append(d_col)
        marker = "✓" if d_ind <= 10 and d_col <= 10 else "⚠"
        print(f"{nombre:<38s} {n_ind:>4d} {r_ind:>6d}  {n_col:>4d} {r_col:>6d}  {marker}")
    print(f"\nΔ promedio individuales: {sum(deltas_ind)/len(deltas_ind):.1f}%")
    print(f"Δ promedio colectivas:   {sum(deltas_col)/len(deltas_col):.1f}%")


if __name__ == "__main__":
    main()
