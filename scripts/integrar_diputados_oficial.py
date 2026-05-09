"""
Integra el scrape oficial de SITL Diputados (sitl.diputados.gob.mx/LXVI_leg/)
a la tabla sil_documentos. MODO ENRIQUECIMIENTO — NO destruye datos existentes.

Estrategia:
  1. Lee dashboard/diputados_lxvi_oficial.json (output del scraper).
  2. Pre-carga índice de iniciativas+proposiciones LXVI Diputados existentes.
  3. Para cada instrumento del scrape:
     - Match por (core_titulo[:60], fecha, apellido_diputado).
     - Si match → ya está en BD, no tocar.
     - Si no match → INSERT como DIP_* (nuevo).
  4. seg_id determinístico POR INSTRUMENTO (no por firmante) — si una
     iniciativa colectiva aparece N veces (1 por co-firmante), los N
     generan el mismo seg_id y solo se inserta UNA fila.
  5. Imprime stats + diferencias vs BD pre-existente.

NO toca: Comunicados/Dictámenes/Efemérides, Senado, Comisión Permanente.

Idempotente.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sqlite3
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "semaforo.db"
JSON_INPUT = ROOT / "dashboard" / "diputados_lxvi_oficial.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def normalizar(s: str) -> str:
    """
    Normaliza string para match cross-fuente.

    Repara mojibake comunes (artÃ­culo → articulo) y quita acentos.
    Algunos docs en BD vienen del scraper viejo del SIL Gob que guardó
    con encoding latin-1 erróneamente leído como utf-8, generando
    "artÃ­culo" en vez de "artículo". El matcher tiene que tratarlos
    igual que el scrape limpio del SITL.
    """
    if not s:
        return ""
    s = s.lower()
    # Reparar mojibake: secuencias UTF-8 mal-leídas como latin-1
    try:
        s_repaired = s.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
        # Solo usar si la reparación tiene SENTIDO (no introduce bytes raros)
        if len(s_repaired) > 0 and "Ã" not in s_repaired:
            s = s_repaired
    except Exception:
        pass
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    # Quitar caracteres no-ASCII residuales (ej Ã, º si quedan)
    s = re.sub(r"[^\x00-\x7f]", "", s)
    return s


def core_titulo(s: str) -> str:
    """
    Normaliza título para match cross-fuente entre SITL y SIL Gob.

    Diferencias observadas que hay que neutralizar:
      · SITL prefija "Proyecto de decreto", SIL Gob no
      · SITL: "Por el que se exhorta"  vs  SIL Gob: "Por el que exhorta"
      · SITL: "artículo 9o."  vs  SIL Gob: "artículo 9°"
      · Puntuación inconsistente al final
      · Variantes "º"/"o."/"°" para ordinales
    """
    n = normalizar(s)
    prefijos = [
        r"^proyecto\s+de\s+decreto\s+",
        r"^iniciativa\s+(?:con\s+proyecto\s+de\s+decreto\s+)?",
        r"^proposicion\s+con\s+punto\s+de\s+acuerdo\s*,?\s*",
        r"^con\s+proyecto\s+de\s+decreto\s+",
        r"^acuerdo\s+",
    ]
    for p in prefijos:
        n = re.sub(p, "", n).strip()
    # Ordinales: "9o.", "9º", "9°" → "9"
    n = re.sub(r"(\d+)\s*[o°º]\.?", r"\1", n)
    # "se exhorta" / "se reforma" / "se adiciona" → "exhorta" / etc
    # (SITL los pone con "se", SIL Gob a veces sin "se")
    n = re.sub(r"\b(por el que|que)\s+se\s+(exhorta|reforma|adiciona|deroga|abroga|expide|modifica|aprueba|declara)\b",
               r"\1 \2", n)
    n = re.sub(r"\s+", " ", n).strip(" .,;:")
    return n


def derivar_periodo_lxvi(fecha_iso: str) -> str:
    if not fecha_iso or len(fecha_iso) < 10:
        return ""
    try:
        anio, mes = int(fecha_iso[:4]), int(fecha_iso[5:7])
    except Exception:
        return ""
    if anio == 2024 and mes >= 9:
        return "1er Ordinario"
    if anio == 2025 and mes <= 1:
        return "1er Receso"
    if anio == 2025 and 2 <= mes <= 4:
        return "2do Ordinario"
    if anio == 2025 and 5 <= mes <= 8:
        return "2do Receso"
    if anio == 2025 and mes >= 9:
        return "1er Ordinario 2do Año"
    if anio == 2026 and mes <= 1:
        return "1er Receso 2do Año"
    if anio == 2026 and 2 <= mes <= 4:
        return "2do Ordinario 2do Año"
    if anio == 2026 and mes >= 5:
        return "2do Receso 2do Año"
    return ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--input", default=str(JSON_INPUT))
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    if not Path(args.input).exists():
        logger.error(f"No existe {args.input}")
        sys.exit(1)

    data = json.loads(Path(args.input).read_text())
    logger.info(
        f"Cargado: {data.get('total_diputados','?')} dip, "
        f"{data.get('total_instrumentos','?')} instrumentos "
        f"({data.get('total_iniciativas','?')} ini + "
        f"{data.get('total_proposiciones','?')} prop)"
    )

    conn = sqlite3.connect(str(DB))
    conn.execute("PRAGMA journal_mode=WAL")

    cols = {r[1] for r in conn.execute("PRAGMA table_info(sil_documentos)").fetchall()}
    for col, ddl in [
        ("tipo_grupo", "ALTER TABLE sil_documentos ADD COLUMN tipo_grupo TEXT DEFAULT ''"),
        ("clasificacion", "ALTER TABLE sil_documentos ADD COLUMN clasificacion TEXT DEFAULT ''"),
        ("tipo_inferido", "ALTER TABLE sil_documentos ADD COLUMN tipo_inferido TEXT DEFAULT ''"),
        ("presentador", "ALTER TABLE sil_documentos ADD COLUMN presentador TEXT DEFAULT ''"),
        ("tipo_presentador", "ALTER TABLE sil_documentos ADD COLUMN tipo_presentador TEXT DEFAULT ''"),
        ("n_firmantes", "ALTER TABLE sil_documentos ADD COLUMN n_firmantes INTEGER DEFAULT 1"),
        ("es_individual", "ALTER TABLE sil_documentos ADD COLUMN es_individual INTEGER DEFAULT 1"),
        ("url", "ALTER TABLE sil_documentos ADD COLUMN url TEXT DEFAULT ''"),
    ]:
        if col not in cols:
            logger.info(f"  Schema migration: agregando columna {col}")
            conn.execute(ddl)
            cols.add(col)

    # Tabla relacional N:M para vincular legisladores con instrumentos
    # según su ROL. Permite contar correctamente "Iniciativas vinculadas"
    # por diputado sin duplicar el instrumento en sil_documentos.
    # Ej. Gibrán Ramírez (MC): 6 Iniciante + 2 Adherente + 47 De Grupo = 55.
    # En sil_documentos solo viven los instrumentos como Iniciante; los
    # roles Adherente/De Grupo se registran aquí.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS diputado_instrumento (
            legislador_id INTEGER NOT NULL,
            sitl_id_dip TEXT NOT NULL,
            seguimiento_id TEXT NOT NULL,
            rol TEXT NOT NULL,
            tipo TEXT NOT NULL,
            fecha TEXT,
            PRIMARY KEY (legislador_id, seguimiento_id, rol)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_di_legislador ON diputado_instrumento(legislador_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_di_seg ON diputado_instrumento(seguimiento_id)")
    conn.commit()

    rows = conn.execute("""
        SELECT id, titulo, fecha_presentacion, presentador, tipo_grupo, seguimiento_id
        FROM sil_documentos
        WHERE legislatura = 'LXVI'
          AND camara = 'Cámara de Diputados'
          AND (tipo_grupo IN ('Iniciativa', 'Proposición con Punto de Acuerdo')
               OR tipo LIKE 'Iniciativa%'
               OR tipo LIKE 'Proposici%con%punto%acuerdo%')
    """).fetchall()
    logger.info(f"Filas LXVI Diputados ini/prop existentes: {len(rows)}")

    existentes_idx = {}
    # Índice secundario: (fecha, apellido) → lista de docs. Usado como
    # fallback cuando el título no matchea exacto por reformateos del SITL.
    fecha_apellido_idx = {}
    for row_id, titulo, fecha, presentador, tg, seg_id in rows:
        if not titulo or not fecha:
            continue
        key = (core_titulo(titulo)[:60], fecha)
        info = {"id": row_id, "presentador": presentador or "",
                "tipo_grupo": tg, "seg_id": seg_id, "titulo": titulo}
        existentes_idx.setdefault(key, []).append(info)
        # Construir índice por apellido extraído del presentador.
        # Presentador BD: 'Dip. Rocío Adriana Abreu Artiñano (Morena)'
        # → tokens útiles para match: ['rocio','adriana','abreu','artinano']
        pres_norm = normalizar(presentador or "")
        # Quitar prefijos Dip./Sen.
        pres_clean = re.sub(r"^dip\.?\s+|^sen\.?\s+", "", pres_norm).strip()
        # Quitar partido entre paréntesis
        pres_clean = re.sub(r"\(.*$", "", pres_clean).strip()
        for tok in pres_clean.split():
            if len(tok) < 4:
                continue
            fecha_apellido_idx.setdefault((fecha, tok), []).append(info)

    instrumentos = data["instrumentos"]
    if args.limit > 0:
        instrumentos = instrumentos[: args.limit]

    # Mapping sitl_id_dip → legislador_id (BD) para tabla relacional
    sitl_to_leg = {}
    for r in conn.execute(
        "SELECT sitl_id, id FROM legisladores WHERE camara='Cámara de Diputados' AND sitl_id IS NOT NULL"
    ).fetchall():
        sitl_to_leg[str(r[0])] = r[1]

    ahora = datetime.now().isoformat(timespec="seconds")
    matched = 0
    inserted = 0
    skipped_sin_fecha = 0
    ambiguos = 0
    relacional_inserted = 0
    inserted_por_tipo = {"Iniciativa": 0, "Proposición con Punto de Acuerdo": 0}
    seg_ids_creados = set()
    # Tracking de roles por (legislador, seg_id) para mostrar conteo final
    rol_por_leg = {}  # leg_id → Counter(rol → n)
    from collections import defaultdict, Counter

    for inst in instrumentos:
        titulo = inst.get("titulo", "")
        fecha = inst.get("fecha", "")
        tipo = inst.get("tipo", "")
        sitl_id_dip = str(inst.get("sitl_id_dip", ""))
        rol_valor = inst.get("rol_valor", "")
        rol = inst.get("rol", "Iniciante")

        if not fecha:
            skipped_sin_fecha += 1
            continue

        # Roles que NO son "Iniciante/Promovente/Proponente" (los principales)
        # se registran SOLO en diputado_instrumento sin tocar sil_documentos.
        # Esto evita inflar conteos globales con docs duplicados.
        es_principal = rol in ("Iniciante", "Promovente", "Proponente")

        # Si el rol es Adherente/De Grupo/Suscriptor: solo registrar la
        # vinculación N:M. NO tocar sil_documentos.
        if not es_principal:
            leg_id = sitl_to_leg.get(sitl_id_dip)
            if leg_id and not args.dry_run:
                # Construir seg_id determinístico para que el INSERT/UPSERT
                # converja al mismo doc del Iniciante principal.
                seed = f"{titulo[:80]}|{fecha}|{tipo}"
                sid_h = hashlib.md5(seed.encode("utf-8")).hexdigest()[:14]
                seg_target = f"DIP_{sid_h}"
                try:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO diputado_instrumento
                          (legislador_id, sitl_id_dip, seguimiento_id, rol, tipo, fecha)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (leg_id, sitl_id_dip, seg_target, rol, tipo, fecha),
                    )
                    relacional_inserted += 1
                except Exception as e:
                    logger.warning(f"  rel insert fail: {e}")
            rol_por_leg.setdefault(leg_id, Counter())[rol] += 1
            continue  # ya registrado, no procesar como sil_documentos

        apellido = rol_valor.split()[0].lower() if rol_valor else ""
        apellido_norm = normalizar(apellido)

        key = (core_titulo(titulo)[:60], fecha)
        candidatos = existentes_idx.get(key, [])
        match = None
        for c in candidatos:
            pres_norm = normalizar(c["presentador"])
            if apellido_norm and apellido_norm in pres_norm:
                match = c
                break
        if not match and candidatos:
            match = candidatos[0]

        # Fallback: match por (fecha, tokens del rol_valor) + jaccard de
        # tokens del título. Necesario porque a veces los títulos se
        # reformatean entre SITL y SIL Gob (símbolos ordinales, "se" extra).
        cands_apellido_unique = []
        if not match:
            rol_norm = normalizar(rol_valor)
            rol_clean = re.sub(r"\(.*$", "", rol_norm).strip()
            tokens_pres = [t for t in rol_clean.split() if len(t) >= 4]
            cands_apellido = []
            for tok in tokens_pres:
                cands_apellido.extend(fecha_apellido_idx.get((fecha, tok), []))
            seen_ids = set()
            for c in cands_apellido:
                if c["id"] not in seen_ids:
                    seen_ids.add(c["id"])
                    cands_apellido_unique.append(c)
            # Match por jaccard ≥ 0.4 (más permisivo)
            tokens_scrape = set(core_titulo(titulo).split())
            tokens_scrape = {t for t in tokens_scrape if len(t) >= 4}
            best_score = 0.0
            for c in cands_apellido_unique:
                tokens_bd = set(core_titulo(c["titulo"]).split())
                tokens_bd = {t for t in tokens_bd if len(t) >= 4}
                if not tokens_bd or not tokens_scrape:
                    continue
                inter = len(tokens_scrape & tokens_bd)
                union = len(tokens_scrape | tokens_bd)
                jacc = inter / union if union else 0
                # Containment: ¿el conjunto más pequeño está casi contenido
                # en el más grande? Es signal fuerte cuando uno es versión
                # corta del otro (BD: "Que reforma X y adiciona Y";
                # SITL: "Proyecto de decreto que reforma X").
                contain = inter / min(len(tokens_scrape), len(tokens_bd)) if min(len(tokens_scrape), len(tokens_bd)) > 0 else 0
                # Score híbrido: max(jaccard, containment*0.7)
                score = max(jacc, contain * 0.7)
                if score > best_score and (jacc >= 0.3 or contain >= 0.7):
                    best_score = score
                    match = c

        if match:
            matched += 1
            continue

        # CONSERVATIVO: Si HAY candidatos por (fecha, apellido) pero el
        # título no matchea suficiente, NO insertar como nuevo. Es muy
        # probable que sea el mismo doc con reformato de título y meterlo
        # como DIP_* nuevo crearía duplicado. Lo contamos como "ambiguo"
        # y lo skip — preferimos sub-cubrir que duplicar.
        if cands_apellido_unique:
            ambiguos += 1
            continue

        # No match → INSERT como DIP_* nuevo.
        # seg_id determinístico POR INSTRUMENTO. Si una iniciativa colectiva
        # aparece N veces (1 por co-firmante) los N generan el mismo seg_id
        # y solo se inserta UNA fila (el segundo y siguientes hacen UPSERT
        # idempotente sobre la misma fila).
        seed = f"{titulo[:80]}|{fecha}|{tipo}"
        sid = hashlib.md5(seed.encode("utf-8")).hexdigest()[:14]
        seg_id = f"DIP_{sid}"
        es_nuevo_seg_id = seg_id not in seg_ids_creados
        seg_ids_creados.add(seg_id)

        partido_m = re.search(r"\(([^)]+)\)", rol_valor)
        partido = partido_m.group(1) if partido_m else ""
        nombre_pres = rol_valor.split('(')[0].strip() if rol_valor else ""
        presentador = f"Dip. {nombre_pres} ({partido})" if partido and nombre_pres else rol_valor

        tipo_grupo = "Iniciativa" if tipo == "Iniciativa" else "Proposición con Punto de Acuerdo"
        clasificacion = "legislativo_sustantivo"

        if not args.dry_run:
            try:
                conn.execute(
                    """
                    INSERT INTO sil_documentos
                      (seguimiento_id, asunto_id, tipo, titulo, sinopsis, camara,
                       fecha_presentacion, legislatura, periodo, estatus, partido,
                       comision, categoria, fecha_scraping, presentador,
                       tipo_presentador, tipo_grupo, clasificacion, n_firmantes, es_individual)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(seguimiento_id, asunto_id) DO UPDATE SET
                      titulo=excluded.titulo,
                      tipo_grupo=excluded.tipo_grupo,
                      clasificacion=excluded.clasificacion
                    """,
                    (
                        seg_id, seg_id, tipo, titulo[:500],
                        inst.get("sinopsis", "")[:500],
                        "Cámara de Diputados",
                        fecha, "LXVI", derivar_periodo_lxvi(fecha),
                        "", partido, "", "",
                        ahora, presentador,
                        "legislador",
                        tipo_grupo, clasificacion,
                        1, 1,
                    ),
                )
                if es_nuevo_seg_id:
                    inserted += 1
                    inserted_por_tipo[tipo_grupo] = inserted_por_tipo.get(tipo_grupo, 0) + 1
                # También registrar en tabla relacional como Iniciante
                leg_id = sitl_to_leg.get(sitl_id_dip)
                if leg_id:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO diputado_instrumento
                          (legislador_id, sitl_id_dip, seguimiento_id, rol, tipo, fecha)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (leg_id, sitl_id_dip, seg_id, rol, tipo, fecha),
                    )
                    relacional_inserted += 1
                    rol_por_leg.setdefault(leg_id, Counter())[rol] += 1
            except Exception as e:
                logger.warning(f"Error insertando {seg_id}: {e}")

    if not args.dry_run:
        conn.commit()

    print()
    print("=" * 60)
    print("== Integración SITL Diputados → sil_documentos ==")
    print("=" * 60)
    print(f"  Instrumentos en JSON (incluye co-firmantes): {len(instrumentos)}")
    print(f"  Match con BD existente:                      {matched}")
    print(f"  Nuevos seg_ids únicos creados:               {len(seg_ids_creados)}")
    print(f"  Insertados como instrumentos NUEVOS:         {inserted}")
    print(f"    · Iniciativas:             {inserted_por_tipo.get('Iniciativa', 0)}")
    print(f"    · Proposiciones:           {inserted_por_tipo.get('Proposición con Punto de Acuerdo', 0)}")
    print(f"  Skipped (sin fecha):         {skipped_sin_fecha}")
    print(f"  Ambiguos (mismo apellido+fecha en BD, skip): {ambiguos}")
    print(f"  Relacional diputado_instrumento inserted:    {relacional_inserted}")
    # Top 5 diputados con más vinculaciones
    if rol_por_leg:
        print()
        print("  Top 5 diputados por vinculaciones:")
        top5 = sorted(rol_por_leg.items(), key=lambda x: -sum(x[1].values()))[:5]
        for leg_id, counter in top5:
            r = conn.execute("SELECT nombre FROM legisladores WHERE id=?", (leg_id,)).fetchone()
            nombre = r[0] if r else f"id={leg_id}"
            total = sum(counter.values())
            roles_str = " + ".join(f"{n} {rol}" for rol, n in counter.most_common())
            print(f"    {nombre[:40]:40} total={total} ({roles_str})")
    if args.dry_run:
        print("\n  *** DRY RUN — no se escribió la BD ***")


if __name__ == "__main__":
    main()
