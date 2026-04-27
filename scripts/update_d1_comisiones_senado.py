"""
Sync de datos de comisiones del Senado a Cloudflare D1.

Lee el JSON producido por scrapers/comisiones_senado_asuntos.py y hace UPSERT
a la tabla comisiones_stats reusando las columnas sitl_* que ya existen
para Diputados. De esa forma el endpoint /comisiones del Worker devuelve
c.sitl.{turnadas, ini_turnadas, prop_turnadas, ...} para AMBAS cámaras y
el frontend (consola-pro + dashboard) lo consume sin cambios.

Mapeo de campos JSON → columnas D1:
    iniciativas    → sitl_ini_turnadas (tambien dict_ini_turnadas si existe)
    proposiciones  → sitl_prop_turnadas
    dictamenes     → docs_dictamen (Senado: tambien sumamos a sitl_*)
    minutas       (no hay columna directa; las metemos en sitl_desglose JSON)
    turnadas       → sitl_turnadas (suma de ini+prop+dict+min)

NOTA importante: la fuente Senado solo da CONTEOS por tipo, NO desglose
de aprobadas/pendientes/desechadas. Por eso esos campos quedan NULL.
Si en el futuro queremos eso, hay que parsear cada panel del HTML del
endpoint asuntosComisiones.php (cada uno tiene "Estatus: ...").

Uso:
    python scripts/update_d1_comisiones_senado.py --input /tmp/senado_asuntos.json
    python scripts/update_d1_comisiones_senado.py --input /tmp/senado_asuntos.json --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("update_d1_comisiones_senado")

D1_DB = "fiat-busqueda"


def _sql_escape(s):
    """Escape para SQL (single quotes)."""
    if s is None:
        return "NULL"
    if isinstance(s, (int, float)):
        return str(s)
    return "'" + str(s).replace("'", "''") + "'"


def ejecutar_sql_d1(sql: str, remote: bool = True) -> dict:
    """Ejecuta SQL en D1 vía wrangler. Acepta string o lista de statements."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
        f.write(sql)
        sql_file = f.name
    try:
        cmd = ["npx", "wrangler", "d1", "execute", D1_DB,
               "--file", sql_file]
        if remote:
            cmd.append("--remote")
        else:
            cmd.append("--local")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            logger.error(f"wrangler error: {result.stderr[:500]}")
            return {"ok": False, "error": result.stderr[:500]}
        return {"ok": True, "stdout": result.stdout[:500]}
    finally:
        Path(sql_file).unlink(missing_ok=True)


def construir_sql(data: dict) -> list[str]:
    """
    data = {"id|nombre": {iniciativas, proposiciones, dictamenes, minutas,
                          turnadas, scrape_ts}}
    """
    ahora = datetime.utcnow().isoformat()
    sqls = []
    for key, v in data.items():
        try:
            id_str, nombre = key.split("|", 1)
            id_com = int(id_str)
        except Exception:
            logger.warning(f"Key inválida: {key}")
            continue

        ini = v.get("iniciativas", 0)
        prop = v.get("proposiciones", 0)
        dict_n = v.get("dictamenes", 0)
        minutas_n = v.get("minutas", 0)
        turnadas = v.get("turnadas", ini + prop + dict_n + minutas_n)

        # Desglose JSON para guardar también minutas (no tiene columna propia)
        desglose = json.dumps({
            "fuente": "comisiones.senado.gob.mx/asuntosComisiones.php",
            "id_comision_senado": id_com,
            "iniciativas": ini,
            "proposiciones": prop,
            "dictamenes": dict_n,
            "minutas": minutas_n,
            "scrape_ts": v.get("scrape_ts"),
        }, ensure_ascii=False)

        # UPSERT — UNIQUE(nombre, camara). Senado nombres no llevan "Comisión de" prefix.
        sql = f"""
INSERT INTO comisiones_stats
  (nombre, camara, comt_id,
   sitl_turnadas, sitl_ini_turnadas, sitl_prop_turnadas,
   docs_dictamen, total_docs,
   sitl_desglose, actualizado_sitl)
VALUES
  ({_sql_escape(nombre)}, 'Senado', {id_com},
   {turnadas}, {ini}, {prop},
   {dict_n}, {turnadas},
   {_sql_escape(desglose)}, {_sql_escape(ahora)})
ON CONFLICT(nombre, camara) DO UPDATE SET
   comt_id = excluded.comt_id,
   sitl_turnadas = excluded.sitl_turnadas,
   sitl_ini_turnadas = excluded.sitl_ini_turnadas,
   sitl_prop_turnadas = excluded.sitl_prop_turnadas,
   docs_dictamen = excluded.docs_dictamen,
   total_docs = excluded.total_docs,
   sitl_desglose = excluded.sitl_desglose,
   actualizado_sitl = excluded.actualizado_sitl;
""".strip()
        sqls.append(sql)
    return sqls


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="/tmp/senado_asuntos.json")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--batch-size", type=int, default=20)
    args = p.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        # Permitir leer desde el checkpoint también
        ckpt = Path("/tmp/senado_asuntos_ckpt.json")
        if ckpt.exists():
            logger.info(f"Input no existe, usando checkpoint {ckpt}")
            in_path = ckpt
        else:
            logger.error(f"No existe {in_path} ni el checkpoint")
            return 1

    data = json.loads(in_path.read_text())
    logger.info(f"Leídos {len(data)} comisiones desde {in_path}")

    sqls = construir_sql(data)
    logger.info(f"Generados {len(sqls)} statements UPSERT")

    if args.dry_run:
        print("=== DRY-RUN — primer SQL ===")
        print(sqls[0] if sqls else "(vacío)")
        return 0

    # Ejecutar en batches
    ok_total = 0
    for i in range(0, len(sqls), args.batch_size):
        batch = sqls[i:i + args.batch_size]
        logger.info(f"Batch {i // args.batch_size + 1}/{(len(sqls) - 1) // args.batch_size + 1}: "
                    f"{len(batch)} statements")
        result = ejecutar_sql_d1("\n".join(batch))
        if result.get("ok"):
            ok_total += len(batch)
        else:
            logger.error(f"Batch falló: {result.get('error', 'unknown')[:300]}")
        time.sleep(0.5)

    logger.info(f"COMPLETADO: {ok_total}/{len(sqls)} statements ejecutados en D1")
    return 0


if __name__ == "__main__":
    sys.exit(main())
