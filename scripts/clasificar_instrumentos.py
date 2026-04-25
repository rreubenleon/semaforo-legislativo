"""
Clasificador de instrumentos SIL alineado con las CATEGORÍAS OFICIALES
del SIL, no con categorías inventadas por FIAT.

Hace dos cosas:

  1. INFERIR EL TIPO SIL desde el título cuando el scraper puso 'Asunto'
     o tipo vacío (bug del scraper SIL — ver scrapers/sil.py). El SIL
     oficial siempre tiene un tipo concreto en su columna izquierda; el
     scraper se lo come en muchos casos.

  2. ASIGNAR FUNCIÓN (legislativa / administrativa / ceremonial) basada
     en el tipo + reglas específicas del título (ej. retiro de iniciativa
     dentro de proposición con PA es administrativa, no legislativa).

Tipos oficiales del SIL:
  - Iniciativa
  - Proposición con punto de acuerdo
  - Dictamen
  - Minuta
  - Acuerdo parlamentario
  - Comunicado
  - Efeméride
  - Otro (fallback)

Funciones derivadas:
  - legislativa: cuenta como actividad legislativa para rankings/ELO/H2H
  - administrativa: trámites, comunicados, informes, licencias
  - ceremonial: efemérides, conmemoraciones (no son legislación pero
    no son "ruido" en el sentido administrativo)

Output: actualiza dos columnas en sil_documentos:
  - tipo_inferido: el tipo SIL real (corrige al scraper)
  - clasificacion: la función (legislativa/administrativa/ceremonial)

Uso:
    python scripts/clasificar_instrumentos.py            # aplica
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
# Tipos oficiales del SIL
# ──────────────────────────────────────────────────────────────────────
TIPO_INICIATIVA = "Iniciativa"
TIPO_PROPOSICION = "Proposición con punto de acuerdo"
TIPO_DICTAMEN = "Dictamen"
TIPO_MINUTA = "Minuta"
TIPO_ACUERDO = "Acuerdo parlamentario"
TIPO_COMUNICADO = "Comunicado"
TIPO_EFEMERIDE = "Efeméride"
TIPO_OTRO = "Otro"

# Funciones (segunda dimensión)
FUNC_LEGISLATIVA = "legislativa"
FUNC_ADMINISTRATIVA = "administrativa"
FUNC_CEREMONIAL = "ceremonial"


# ──────────────────────────────────────────────────────────────────────
# Inferencia de tipo SIL desde título (cuando tipo viene como 'Asunto'
# o vacío por bug del scraper)
# ──────────────────────────────────────────────────────────────────────
_RE_INICIATIVA = re.compile(
    r"^(que\s+(reforma|adiciona|expide|abroga|declara|crea|propone|deroga|modifica)"
    r"|de\s+decreto\s+por\s+el\s+que\s+se?\s*(reforma|adiciona|expide|abroga|declara|crea|deroga|modifica))",
    re.IGNORECASE,
)

_RE_PROPOSICION = re.compile(
    r"^(por\s+(el|la)\s+que\s+(se\s+)?(exhorta|solicita|cita|condena|reconoce|expresa|manifiesta|hace|invita|convoca|propone|aprueba|determina)"
    r"|para\s+exhortar"
    r"|por\s+(el|la)\s+que\s+el\s+(senado|congreso|h\.\s*congreso)\s+(condena|reconoce|expresa|manifiesta))",
    re.IGNORECASE,
)

_RE_COMUNICADO = re.compile(
    r"^(con\s+(el|la|las|los)\s+que|comunica|para\s+informar|ante\s+la\s+próxima"
    r"|protesta\s+de"
    r"|en\s+relación\s+con\s+la\s+comparecencia"
    r"|^solicitud\s+(de\s+licencia|para\s+separarse))",
    re.IGNORECASE,
)

# Informes son comunicados que llevan el reporte adjunto
_RE_INFORME = re.compile(
    r"^(informes?|correspondiente|segundo\s+informe|tercer\s+informe|primer\s+informe)\b"
    r"|^acta\s+de\s+la\s+sesión|^cuenta\s+(del\s+viaje|de\s+la\s+gira)",
    re.IGNORECASE,
)

_RE_EFEMERIDE = re.compile(
    r"^efeméride|^pronunciamiento\s+sobre|^posicionamiento\b"
    r"|^para\s+brindar\s+un\s+minuto|^acta\s+de\s+la\s+sesión\s+solemne"
    r"|^con\s+motivo\s+del\s+(aniversario|día|conmemoración)"
    r"|^para\s+referirse\s+a",
    re.IGNORECASE,
)

_RE_ACUERDO_PARLAMENTARIO = re.compile(
    r"^acuerdo\s+aprobado|^sobre\s+la\s+integración\s+de\s+la\s+comisión"
    r"|^en\s+relación\s+con\s+la\s+integración"
    r"|^en\s+relación\s+(a|con)\s+las\s+personas\s+que",
    re.IGNORECASE,
)

_RE_MINUTA = re.compile(r"^minuta\s+(que|con|de)", re.IGNORECASE)

# Sub-patrones para detectar trámites administrativos DENTRO de tipos
# legislativos (ej. una proposición con PA puede ser un retiro)
_RE_RETIRO_LICENCIA = re.compile(
    r"(solicita|solicito|se\s+solicita)\s+(el\s+)?retiro"
    r"|retiro\s+de\s+(la\s+)?iniciativa"
    r"|(solicita|concede|otorga)\s+licencia"
    r"|(separarse|separa(r)?se)\s+de\s+sus?\s+funciones"
    r"|^para\s+separarse|^solicitud\s+de\s+licencia",
    re.IGNORECASE,
)


def inferir_tipo_sil(tipo_scraper: str, titulo: str) -> str:
    """Determina el tipo oficial del SIL. Si el scraper trajo un tipo
    válido, lo respeta. Si vino como 'Asunto', vacío, o algo raro,
    infiere desde el título."""
    t = (tipo_scraper or "").strip()
    titulo = (titulo or "").strip()

    # Tipos válidos del scraper que respetamos directo
    if t in ("Iniciativa", "Iniciativa de inicio en cámara de diputados",
             "Iniciativa de inicio en cámara de senadores"):
        return TIPO_INICIATIVA
    if t == "Proposición con punto de acuerdo":
        return TIPO_PROPOSICION
    if t in ("Dictamen a discusión", "Dictamen de primera lectura / declaratoria de publicidad",
             "Dictamen en sentido negativo"):
        return TIPO_DICTAMEN
    if t == "Minuta":
        return TIPO_MINUTA
    if t == "Acuerdo parlamentario":
        return TIPO_ACUERDO

    # Tipos "Otro:*" del scraper son administrativos, mapeo a Comunicado
    # excepto intervención que es Efeméride en términos prácticos
    if t == "Otro:comunicación" or t == "Otro:respuesta_punto_acuerdo":
        return TIPO_COMUNICADO
    if t == "Otro:intervencion_parlamentaria":
        return TIPO_EFEMERIDE
    if t == "Otro:integracion_comisiones":
        return TIPO_ACUERDO
    if t == "Otro:retiro" or t == "Otro:dictamen_negativo":
        return TIPO_PROPOSICION  # retiros van con proposiciones; función decidirá

    # Si tipo es 'Asunto' o vacío → inferir desde título
    if _RE_MINUTA.search(titulo):
        return TIPO_MINUTA
    if _RE_INICIATIVA.search(titulo):
        return TIPO_INICIATIVA
    if _RE_PROPOSICION.search(titulo):
        return TIPO_PROPOSICION
    if _RE_INFORME.search(titulo):
        return TIPO_COMUNICADO  # informes son comunicados
    if _RE_EFEMERIDE.search(titulo):
        return TIPO_EFEMERIDE
    if _RE_ACUERDO_PARLAMENTARIO.search(titulo):
        return TIPO_ACUERDO
    if _RE_COMUNICADO.search(titulo):
        return TIPO_COMUNICADO

    return TIPO_OTRO


def asignar_funcion(tipo_sil: str, titulo: str) -> str:
    """Determina la función (legislativa/administrativa/ceremonial)
    basada en el tipo SIL más reglas específicas del título."""
    titulo = (titulo or "").strip()

    if tipo_sil in (TIPO_INICIATIVA, TIPO_DICTAMEN, TIPO_MINUTA):
        return FUNC_LEGISLATIVA

    if tipo_sil == TIPO_PROPOSICION:
        # Excepción: retiros y licencias dentro de proposición
        if _RE_RETIRO_LICENCIA.search(titulo):
            return FUNC_ADMINISTRATIVA
        return FUNC_LEGISLATIVA

    if tipo_sil == TIPO_EFEMERIDE:
        return FUNC_CEREMONIAL

    if tipo_sil in (TIPO_COMUNICADO, TIPO_ACUERDO):
        return FUNC_ADMINISTRATIVA

    return FUNC_ADMINISTRATIVA  # Otro defaultea a administrativa


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # Migración: agregar columna tipo_inferido + sobrescribir clasificacion
    if not args.dry_run:
        try:
            conn.execute("ALTER TABLE sil_documentos ADD COLUMN tipo_inferido TEXT DEFAULT ''")
            conn.commit()
            logger.info("Columna `tipo_inferido` agregada")
        except sqlite3.OperationalError:
            pass

    rows = conn.execute("""
        SELECT id, titulo, tipo
        FROM sil_documentos
        WHERE fecha_presentacion >= '2024-09-01'
    """).fetchall()
    logger.info(f"Procesando {len(rows):,} instrumentos LXVI…")

    conteo_tipo = {}
    conteo_func = {}
    conteo_recategorizados = 0  # casos donde inferimos tipo distinto al del scraper

    for r in rows:
        tipo_orig = (r["tipo"] or "").strip()
        tipo_sil = inferir_tipo_sil(tipo_orig, r["titulo"])
        funcion = asignar_funcion(tipo_sil, r["titulo"])

        conteo_tipo[tipo_sil] = conteo_tipo.get(tipo_sil, 0) + 1
        conteo_func[funcion] = conteo_func.get(funcion, 0) + 1

        # Detectar si recategorizamos (Asunto → Iniciativa, etc.)
        if tipo_orig == "Asunto" and tipo_sil != TIPO_OTRO:
            conteo_recategorizados += 1

        if not args.dry_run:
            conn.execute(
                "UPDATE sil_documentos SET tipo_inferido = ?, clasificacion = ? WHERE id = ?",
                (tipo_sil, funcion, r["id"])
            )

    if not args.dry_run:
        conn.commit()

    print()
    print(f"  ═══ Clasificación de instrumentos LXVI (categorías SIL oficiales) ═══")
    print(f"  Total procesados: {len(rows):,}")
    print(f"  Recategorizados desde 'Asunto': {conteo_recategorizados:,}")
    print()
    print(f"  Por TIPO SIL inferido:")
    for tipo, n in sorted(conteo_tipo.items(), key=lambda x: -x[1]):
        pct = 100 * n / len(rows)
        print(f"    {tipo:<35} {n:>6}  ({pct:>5.1f}%)")
    print()
    print(f"  Por FUNCIÓN:")
    for func, n in sorted(conteo_func.items(), key=lambda x: -x[1]):
        pct = 100 * n / len(rows)
        print(f"    {func:<35} {n:>6}  ({pct:>5.1f}%)")

    if args.dry_run:
        print("\n  (dry-run: no se escribió)")
    else:
        print(f"\n  ✓ Aplicados {len(rows):,} updates en sil_documentos")


if __name__ == "__main__":
    main()
