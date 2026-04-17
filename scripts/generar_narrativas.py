"""
Generador de narrativas IA para el Radar de Legisladores.

Usa Claude Haiku 4.5 para generar un párrafo conciso (2-3 oraciones)
por legislador, describiendo su perfil legislativo en lenguaje natural.
Las narrativas se escriben a D1 (`legisladores_stats.narrativa`)
y se sirven vía el Worker /radar.

Uso:
    ANTHROPIC_API_KEY=sk-ant-... python scripts/generar_narrativas.py
    ANTHROPIC_API_KEY=sk-ant-... python scripts/generar_narrativas.py --dry-run
    ANTHROPIC_API_KEY=sk-ant-... python scripts/generar_narrativas.py --limit 10

Cadencia recomendada: mensual (las stats no cambian radicalmente día a día).
Costo estimado: ~$0.03–0.06 por ejecución completa (628 legisladores).

Requiere: ANTHROPIC_API_KEY y CLOUDFLARE_API_TOKEN (para D1).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

from anthropic import Anthropic

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.radar_pipeline import ejecutar_sql_d1, _sql_escape

logger = logging.getLogger("generar_narrativas")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

MODEL = "claude-haiku-4-5-20251001"

# ────────────────────────────────────────────
# Prompt del sistema
# ────────────────────────────────────────────
SYSTEM_PROMPT = """\
Eres un analista legislativo de FIAT, un monitor legislativo mexicano.
Tu tarea es escribir una narrativa BREVE (2-3 oraciones, máximo 60 palabras)
sobre un legislador, basándote exclusivamente en los datos proporcionados.

Reglas estrictas:
- Escribe en español neutro/formal, en tercera persona.
- NO inventes datos. Solo usa lo que se te da.
- NO uses superlativos vacíos ("destacado", "reconocido", "importante").
- Sé concreto y cuantitativo: cita números, porcentajes, comisiones.
- Si el legislador tiene poca actividad, dilo sin eufemismos.
- NO incluyas saludos, despedidas ni texto fuera de la narrativa.
- La narrativa debe caber en un tooltip o card pequeña.
"""


def construir_prompt_usuario(leg: dict) -> str:
    """Construye el prompt con los datos del legislador."""
    hit_rate = leg.get("hit_rate")
    hit_str = f"{round(hit_rate * 100)}%" if hit_rate is not None else "sin datos"

    matchup = leg.get("matchup_grade") or "sin datos"
    matchup_com = leg.get("matchup_comision") or "no asignada"
    matchup_tasa = leg.get("matchup_tasa")
    matchup_str = f"{matchup} ({round(matchup_tasa * 100)}% dictaminado)" if matchup_tasa else matchup

    ini_proy = leg.get("iniciativas_proy_15d")
    prop_proy = leg.get("proposiciones_proy_15d")
    l3p_ini = leg.get("l3p_iniciativas", 0) or 0
    l3p_prop = leg.get("l3p_proposiciones", 0) or 0
    l3p_total = l3p_ini + l3p_prop

    cat = leg.get("categoria_dominante") or "sin categoría dominante"
    comisiones = leg.get("comisiones_cargo") or "sin comisiones registradas"

    return f"""\
Legislador: {leg['nombre']}
Partido: {leg.get('partido', '?')}
Cámara: {leg.get('camara', '?')}
Estado: {leg.get('estado', '?')}
Categoría dominante: {cat}
Hit rate (reacción a picos mediáticos): {hit_str}
Matchup grade (comisión {matchup_com}): {matchup_str}
Iniciativas proyectadas 15d: {ini_proy if ini_proy is not None else 'N/A'}
Proposiciones proyectadas 15d: {prop_proy if prop_proy is not None else 'N/A'}
Total instrumentos LXVI: {l3p_total} (ini: {l3p_ini}, prop: {l3p_prop})
Comisiones: {comisiones}

Escribe la narrativa (2-3 oraciones, máximo 60 palabras):"""


def obtener_legisladores_d1() -> list[dict]:
    """Obtiene legisladores con stats desde el Worker /radar."""
    import urllib.request

    WORKER_URL = "https://fiat-busqueda.rreubenleon.workers.dev"
    all_legs = []
    pagina = 1

    while True:
        url = f"{WORKER_URL}/radar?pagina={pagina}&limite=100"
        req = urllib.request.Request(url, headers={"User-Agent": "FIAT-Narrativas/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())

        legs = data.get("legisladores", [])
        all_legs.extend(legs)

        if pagina >= data.get("total_paginas", 1):
            break
        pagina += 1
        time.sleep(0.5)

    return all_legs


def generar_narrativa(client: Anthropic, leg: dict) -> str | None:
    """Genera narrativa para un legislador. Devuelve None si falla."""
    try:
        r = client.messages.create(
            model=MODEL,
            max_tokens=150,
            system=SYSTEM_PROMPT,
            messages=[{
                "role": "user",
                "content": construir_prompt_usuario(leg),
            }],
        )
        texto = r.content[0].text.strip()
        # Truncar si excede 200 chars (safety)
        if len(texto) > 500:
            texto = texto[:497] + "..."
        return texto
    except Exception as e:
        logger.warning(f"  Error generando narrativa para {leg['nombre']}: {e}")
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Genera narrativas IA para legisladores")
    ap.add_argument("--dry-run", action="store_true", help="No escribe a D1")
    ap.add_argument("--limit", type=int, default=0, help="Limitar a N legisladores (0=todos)")
    ap.add_argument("--delay", type=float, default=0.3, help="Delay entre requests API (seg)")
    args = ap.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY no definida")
        return 1

    client = Anthropic(api_key=api_key)

    logger.info("Obteniendo legisladores desde Worker /radar…")
    legisladores = obtener_legisladores_d1()
    logger.info(f"Total legisladores: {len(legisladores)}")

    if args.limit > 0:
        legisladores = legisladores[:args.limit]
        logger.info(f"Limitado a {args.limit}")

    # Generar narrativas
    narrativas: list[tuple[int, str]] = []  # (legislador_id, texto)
    errores = 0

    for i, leg in enumerate(legisladores, 1):
        nombre = leg.get("nombre", "?")
        leg_id = leg.get("id")
        if not leg_id:
            continue

        if i % 50 == 0 or i <= 3:
            logger.info(f"[{i:>3}/{len(legisladores)}] {nombre[:40]}")

        texto = generar_narrativa(client, leg)
        if texto:
            narrativas.append((leg_id, texto))
            if i <= 3:
                logger.info(f"  → {texto[:120]}")
        else:
            errores += 1

        time.sleep(args.delay)

    logger.info(f"Narrativas generadas: {len(narrativas)} · errores: {errores}")

    if args.dry_run:
        logger.info("[DRY-RUN] No se escribe a D1")
        for lid, txt in narrativas[:5]:
            print(f"\n[{lid}] {txt}")
        return 0

    # Escribir a D1 en batches
    ahora_iso = __import__("datetime").datetime.utcnow().isoformat()
    batch_size = 100
    for i in range(0, len(narrativas), batch_size):
        batch = narrativas[i:i + batch_size]
        sqls = []
        for leg_id, texto in batch:
            sqls.append(
                f"UPDATE legisladores_stats SET "
                f"narrativa = {_sql_escape(texto)}, "
                f"narrativa_generada = {_sql_escape(ahora_iso)} "
                f"WHERE legislador_id = {leg_id};"
            )
        ejecutar_sql_d1("\n".join(sqls))
        logger.info(f"  Batch {i // batch_size + 1}: {len(batch)} escritos a D1")

    logger.info(f"Narrativas empujadas a D1: {len(narrativas)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
