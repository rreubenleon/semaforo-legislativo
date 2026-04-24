"""
Clasifica cada instrumento de sil_documentos por su función real:

  legislativo_sustantivo: iniciativa, proposición con punto de acuerdo,
    minuta, dictamen, acuerdo parlamentario con contenido normativo.
    Es lo que cuenta como "actividad legislativa" para rankings, ELO,
    H2H, estadísticas del Radar, etc.

  administrativo_licencia: solicitudes de licencia (separación de
    funciones por tiempo indefinido). Son trámites administrativos,
    no legislación.

  administrativo_informe: informes de viajes de senadores, informes de
    comisiones, informes de austeridad republicana, informes de
    trabajo de comisiones, informes de atribuciones. Administrativos,
    no legislativos.

  comunicacion: comunicaciones oficiales, respuestas a puntos de
    acuerdo, acuses de recibo, ratificaciones.

  intervencion_generica: "para referirse a", intervenciones de tribuna
    sin proposición formal, minutos de aplausos, pronunciamientos
    personales sin instrumento.

  efemeride: conmemoraciones, inscripción de nombres en el Muro de
    Honor, emisión de monedas conmemorativas, declaratorias de días
    nacionales.

  administrativo_otro: comparecencias, ratificaciones, nombramientos,
    solicitudes administrativas varias.

  otro: no clasificable por reglas (fallback).

Uso:
    python scripts/clasificar_instrumentos.py          # aplica y reporta
    python scripts/clasificar_instrumentos.py --dry-run  # solo reporta
"""
import argparse
import logging
import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from db import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Reglas de clasificación (orden importa: primera que matchea, gana)
# ──────────────────────────────────────────────────────────────────────

# Patrones de título que indican instrumento NO sustantivo
_RE_LICENCIA = re.compile(
    r"(solicita|concede)\s+licencia|separa(?:r|rse)\s+de\s+sus\s+funciones"
    r"|licencia\s+(?:temporal|por\s+tiempo|para\s+separa)|solicitud\s+de\s+licencia",
    re.IGNORECASE,
)

_RE_INFORME = re.compile(
    r"^(informe|informes|segundo\s+informe|tercer\s+informe|cuarto\s+informe|quinto\s+informe|sexto\s+informe|primer\s+informe)\b"
    r"|informe\s+de\s+(trabajo|actividades|austeridad|labores|resultados|gestión|gestion|ejecución|ejecucion)"
    r"|rinde\s+informe|presenta\s+informe"
    r"|informe\s+anual|informe\s+semestral|informe\s+trimestral"
    r"|informe\s+sobre\s+el\s+uso\s+de\s+la\s+facultad|informe\s+del\s+ejecutivo"
    r"|^correspondiente\s+(a|al)\s+(la\s+sesión|primer|segundo|tercer|cuarto)"
    r"|^correspondientes?\s+al\s+ejercicio\s+fiscal",
    re.IGNORECASE,
)

_RE_VIAJE = re.compile(
    r"viaje\s+oficial|gira\s+de\s+trabajo|visita\s+oficial|misión\s+oficial"
    r"|asistencia\s+a\s+(la|el)\s+(foro|reunión|cumbre|asamblea|congreso)"
    r"|cuenta\s+del\s+viaje|informe\s+del\s+viaje",
    re.IGNORECASE,
)

_RE_COMUNICACION = re.compile(
    r"^comunicación|^comunica\b|^acuse\b|ratifica(?:ción|)\s+de\s+nombramiento"
    r"|acusa\s+(recibo|recepción)|transmite|transmitido|remitido\s+por|reenvía",
    re.IGNORECASE,
)

_RE_INTERVENCION_GENERICA = re.compile(
    r"^para\s+referirse\s+a|^en\s+relación\s+con\s+(?:la|el|los|las)"
    r"|minuto\s+de\s+aplausos|pronuncia(?:miento|do)\s+(?:personal|de\s+congratulación)"
    r"|pronunciamiento\s+en\s+tribuna|intervención\s+de\s+tribuna",
    re.IGNORECASE,
)

_RE_EFEMERIDE = re.compile(
    r"conmemor(?:a|ar|ación)|con\s+motivo\s+del?\s+(aniversario|día|centenario|bicentenario)"
    r"|efeméride|letras\s+de\s+oro|muro\s+de\s+honor|inscripción\s+del?\s+nombre"
    r"|emisión\s+de\s+(moneda|billete)\s+conmemorativ|declaratoria\s+de\s+día\s+nacional"
    r"|declara\s+al\s+año\s+\d+\s+como",
    re.IGNORECASE,
)

_RE_RATIFICACION = re.compile(
    r"ratificación\s+de|ratifica\s+el\s+nombramiento|nombramiento\s+de\s+(ministro|fiscal|embajador|cónsul)"
    r"|designa(?:ción|r)\s+(?:como\s+)?(?:ministro|comisionad|consejer)|comparecencia\s+de",
    re.IGNORECASE,
)

# Tipos de instrumento "puros" (no necesitan revisar título)
TIPOS_SUSTANTIVOS = {
    "Iniciativa",
    "Iniciativa de inicio en cámara de diputados",
    "Iniciativa de inicio en cámara de senadores",
    "Minuta",
    "Dictamen a discusión",
    "Dictamen de primera lectura / declaratoria de publicidad",
    "Dictamen en sentido negativo",
    "Acuerdo parlamentario",
}

TIPOS_RUIDO_EXPLICITO = {
    "Otro:comunicación",
    "Otro:respuesta_punto_acuerdo",
    "Otro:intervencion_parlamentaria",
    "Otro:integracion_comisiones",
    "Otro:retiro",
}


def clasificar(titulo: str, tipo: str) -> str:
    """Aplica reglas en orden. Devuelve slug de clasificación."""
    t = (titulo or "").strip()
    tp = (tipo or "").strip()

    # 1. Tipos ruidosos explícitos
    if tp in TIPOS_RUIDO_EXPLICITO:
        if tp == "Otro:comunicación":
            return "comunicacion"
        if tp == "Otro:respuesta_punto_acuerdo":
            return "comunicacion"
        if tp == "Otro:intervencion_parlamentaria":
            return "intervencion_generica"
        if tp == "Otro:integracion_comisiones":
            return "administrativo_otro"
        if tp == "Otro:retiro":
            return "administrativo_otro"

    # 2. Patrones de título (orden importa)
    if _RE_LICENCIA.search(t):
        return "administrativo_licencia"
    if _RE_VIAJE.search(t) or _RE_INFORME.search(t):
        return "administrativo_informe"
    if _RE_COMUNICACION.search(t):
        return "comunicacion"
    if _RE_EFEMERIDE.search(t):
        return "efemeride"
    if _RE_RATIFICACION.search(t):
        return "administrativo_otro"
    if _RE_INTERVENCION_GENERICA.search(t):
        return "intervencion_generica"

    # 3. Tipos sustantivos (si pasaron todos los filtros de título arriba)
    if tp in TIPOS_SUSTANTIVOS:
        return "legislativo_sustantivo"
    if tp == "Proposición con punto de acuerdo":
        return "legislativo_sustantivo"

    # 4. Tipos derivados tipo "Otro:*" no catalogados
    if tp.startswith("Otro:"):
        return "administrativo_otro"

    # 5. Tipo vacío o raro: fallback
    if tp == "Asunto":
        # "Asunto" es un tipo genérico del SIL — si llegó aquí sin matchear
        # ninguna regla, lo marcamos como administrativo
        return "administrativo_otro"

    return "otro"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="No escribe cambios")
    args = parser.parse_args()

    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # Agregar columna si no existe
    if not args.dry_run:
        try:
            conn.execute(
                "ALTER TABLE sil_documentos ADD COLUMN clasificacion TEXT DEFAULT ''"
            )
            conn.commit()
            logger.info("Columna `clasificacion` agregada a sil_documentos")
        except sqlite3.OperationalError:
            pass  # ya existe

    rows = conn.execute("""
        SELECT id, titulo, tipo
        FROM sil_documentos
        WHERE fecha_presentacion >= '2024-09-01'
    """).fetchall()
    logger.info(f"Instrumentos LXVI: {len(rows):,}")

    conteo = {}
    cambios = 0
    for r in rows:
        cat = clasificar(r["titulo"], r["tipo"])
        conteo[cat] = conteo.get(cat, 0) + 1
        if not args.dry_run:
            conn.execute(
                "UPDATE sil_documentos SET clasificacion = ? WHERE id = ?",
                (cat, r["id"])
            )
            cambios += 1

    if not args.dry_run:
        conn.commit()

    print()
    print(f"  ═══ Clasificación de instrumentos LXVI ═══")
    print(f"  Total: {len(rows):,}")
    print()
    total = sum(conteo.values())
    for cat, n in sorted(conteo.items(), key=lambda x: -x[1]):
        pct = 100 * n / total
        print(f"    {cat:<30} {n:>6}  ({pct:>5.1f}%)")
    print()
    legislativo = conteo.get("legislativo_sustantivo", 0)
    print(f"  LEGISLATIVO REAL:          {legislativo:,} ({100*legislativo/total:.1f}%)")
    print(f"  RUIDO (todo lo demás):     {total-legislativo:,} ({100*(total-legislativo)/total:.1f}%)")

    if args.dry_run:
        print("\n  (dry-run: no se escribió)")
    else:
        print(f"\n  ✓ Aplicados {cambios:,} updates en sil_documentos.clasificacion")


if __name__ == "__main__":
    main()
