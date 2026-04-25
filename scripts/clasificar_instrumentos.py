"""
Clasificador de instrumentos SIL — confía en el tipo oficial del SIL
(arreglado en scrapers/sil.py post-abr 2026) y solo agrupa los 30+
tipos en 7 grupos canónicos + función (legislativa/administrativa/
ceremonial).

Ya NO infiere desde el título (eso era un parche temporal). El campo
`tipo` de sil_documentos contiene el tipo SIL oficial gracias al fix
del scraper.

Output: dos columnas en sil_documentos
  - tipo_grupo: agrupación en 7 grupos canónicos
  - clasificacion: función (legislativa/administrativa/ceremonial)

Uso:
    python scripts/clasificar_instrumentos.py
    python scripts/clasificar_instrumentos.py --dry-run
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
# 30+ tipos SIL → 7 grupos canónicos
# ──────────────────────────────────────────────────────────────────────
GRUPO_INICIATIVA = "Iniciativa"
GRUPO_PROPOSICION = "Proposición con Punto de Acuerdo"
GRUPO_DICTAMEN = "Dictamen"
GRUPO_MINUTA = "Minuta"
GRUPO_ACUERDO = "Acuerdo Parlamentario"
GRUPO_COMUNICADO = "Comunicado"
GRUPO_EFEMERIDE = "Efeméride"

# Mapeo: clave es el tipo SIL en lowercase
TIPO_A_GRUPO = {
    # Iniciativas
    "iniciativa": GRUPO_INICIATIVA,
    "iniciativa de inicio en cámara de diputados": GRUPO_INICIATIVA,
    "iniciativa de inicio en cámara de senadores": GRUPO_INICIATIVA,

    # Proposiciones con PA
    "proposición con punto de acuerdo": GRUPO_PROPOSICION,
    "excitativa": GRUPO_PROPOSICION,  # exhorta a comisión a dictaminar

    # Dictámenes
    "dictamen": GRUPO_DICTAMEN,
    "dictamen a discusión": GRUPO_DICTAMEN,
    "dictamen de primera lectura / declaratoria de publicidad": GRUPO_DICTAMEN,
    "dictamen en sentido negativo": GRUPO_DICTAMEN,
    "declaratoria de reforma constitucional": GRUPO_DICTAMEN,

    # Minutas
    "minuta": GRUPO_MINUTA,

    # Instrumentos internacionales (tratados)
    "instrumento internacional": GRUPO_INICIATIVA,  # se vota como iniciativa de aprobación de tratado

    # Acuerdos parlamentarios y administrativos internos
    "acuerdo parlamentario": GRUPO_ACUERDO,
    "modificación en la integración de comisiones": GRUPO_ACUERDO,
    "integración de comisiones de trabajo de la comisión permanente": GRUPO_ACUERDO,
    "elección de integrantes de la mesa directiva": GRUPO_ACUERDO,
    "elección de integrantes de la comisión permanente": GRUPO_ACUERDO,
    "instalación": GRUPO_ACUERDO,
    "clausura": GRUPO_ACUERDO,

    # Comunicados (todo lo administrativo que llega o sale del pleno)
    "comunicado": GRUPO_COMUNICADO,
    "informe": GRUPO_COMUNICADO,
    "oficio": GRUPO_COMUNICADO,
    "respuestas de instancias en el pleno": GRUPO_COMUNICADO,
    "protesta": GRUPO_COMUNICADO,
    "licencia / reincorporación": GRUPO_COMUNICADO,
    "permiso al ejecutivo": GRUPO_COMUNICADO,
    "comparecencia": GRUPO_COMUNICADO,
    "ratificación de nombramientos": GRUPO_COMUNICADO,

    # Efemérides y intervenciones (ceremonial)
    "efeméride": GRUPO_EFEMERIDE,
    "efemérides": GRUPO_EFEMERIDE,
    "intervención de la mesa directiva": GRUPO_EFEMERIDE,
    "intervención de legisladora/legislador": GRUPO_EFEMERIDE,
    "intervención de funcionaria/funcionario": GRUPO_EFEMERIDE,
    "lectura del acta de la sesión anterior": GRUPO_EFEMERIDE,
    "agenda política": GRUPO_EFEMERIDE,

    # Tipos legacy del scraper viejo (formato "Otro:*"). Quedan en BD
    # mientras el pipeline rotativo los va re-fetcheando con el scraper
    # arreglado. Mapeo conservador.
    "otro:comunicación": GRUPO_COMUNICADO,
    "otro:respuesta_punto_acuerdo": GRUPO_COMUNICADO,
    "otro:intervencion_parlamentaria": GRUPO_EFEMERIDE,
    "otro:integracion_comisiones": GRUPO_ACUERDO,
    "otro:retiro": GRUPO_PROPOSICION,  # función se evalúa por título
    "otro:dictamen_negativo": GRUPO_DICTAMEN,
}

# Funciones derivadas
FUNC_LEGISLATIVA = "legislativa"
FUNC_ADMINISTRATIVA = "administrativa"
FUNC_CEREMONIAL = "ceremonial"

GRUPO_A_FUNCION_DEFAULT = {
    GRUPO_INICIATIVA: FUNC_LEGISLATIVA,
    GRUPO_DICTAMEN: FUNC_LEGISLATIVA,
    GRUPO_MINUTA: FUNC_LEGISLATIVA,
    GRUPO_PROPOSICION: FUNC_LEGISLATIVA,  # default; excepciones por título
    GRUPO_ACUERDO: FUNC_ADMINISTRATIVA,
    GRUPO_COMUNICADO: FUNC_ADMINISTRATIVA,
    GRUPO_EFEMERIDE: FUNC_CEREMONIAL,
}

# Excepción: una proposición con PA cuyo título es retiro o licencia
# es administrativa, no legislativa
_RE_PROPOSICION_ADMIN = re.compile(
    r"(solicita|solicito|se\s+solicita)\s+(el\s+)?retiro"
    r"|retiro\s+de\s+(la\s+)?iniciativa"
    r"|^para\s+separarse|^solicitud\s+de\s+licencia",
    re.IGNORECASE,
)


def asignar_grupo_y_funcion(tipo_raw: str, titulo: str) -> tuple:
    """Devuelve (grupo, funcion) para un instrumento."""
    tipo_lower = (tipo_raw or "").strip().lower()
    grupo = TIPO_A_GRUPO.get(tipo_lower, "Otro")

    if grupo == "Otro":
        return (grupo, FUNC_ADMINISTRATIVA)

    funcion = GRUPO_A_FUNCION_DEFAULT.get(grupo, FUNC_ADMINISTRATIVA)

    # Excepción: proposición con PA + título de retiro/licencia → admin
    if grupo == GRUPO_PROPOSICION and _RE_PROPOSICION_ADMIN.search(titulo or ""):
        funcion = FUNC_ADMINISTRATIVA

    return (grupo, funcion)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = get_connection()
    conn.row_factory = sqlite3.Row

    if not args.dry_run:
        for col, tipo_sql in [("tipo_grupo", "TEXT DEFAULT ''"),
                              ("clasificacion", "TEXT DEFAULT ''")]:
            try:
                conn.execute(f"ALTER TABLE sil_documentos ADD COLUMN {col} {tipo_sql}")
                conn.commit()
                logger.info(f"Columna `{col}` agregada")
            except sqlite3.OperationalError:
                pass

    rows = conn.execute("""
        SELECT id, titulo, tipo
        FROM sil_documentos
        WHERE fecha_presentacion >= '2024-09-01'
    """).fetchall()
    logger.info(f"Procesando {len(rows):,} instrumentos LXVI…")

    tipos_no_mapeados = {}
    conteo_grupo = {}
    conteo_func = {}

    for r in rows:
        grupo, funcion = asignar_grupo_y_funcion(r["tipo"], r["titulo"])
        conteo_grupo[grupo] = conteo_grupo.get(grupo, 0) + 1
        conteo_func[funcion] = conteo_func.get(funcion, 0) + 1
        if grupo == "Otro" and r["tipo"]:
            tipos_no_mapeados[r["tipo"]] = tipos_no_mapeados.get(r["tipo"], 0) + 1

        if not args.dry_run:
            conn.execute(
                "UPDATE sil_documentos SET tipo_grupo = ?, clasificacion = ? WHERE id = ?",
                (grupo, funcion, r["id"])
            )

    if not args.dry_run:
        conn.commit()

    print()
    print(f"  ═══ Clasificación de instrumentos LXVI (taxonomía SIL oficial) ═══")
    print(f"  Total: {len(rows):,}")
    print()
    print(f"  Por GRUPO SIL (7 canónicos):")
    for grupo, n in sorted(conteo_grupo.items(), key=lambda x: -x[1]):
        pct = 100 * n / len(rows)
        print(f"    {grupo:<35} {n:>6}  ({pct:>5.1f}%)")
    print()
    print(f"  Por FUNCIÓN:")
    for func, n in sorted(conteo_func.items(), key=lambda x: -x[1]):
        pct = 100 * n / len(rows)
        print(f"    {func:<35} {n:>6}  ({pct:>5.1f}%)")
    if tipos_no_mapeados:
        print()
        print(f"  Tipos no mapeados (revisar):")
        for t, n in sorted(tipos_no_mapeados.items(), key=lambda x: -x[1])[:15]:
            print(f"    {t:<60} {n}")

    if args.dry_run:
        print("\n  (dry-run)")
    else:
        print(f"\n  ✓ Aplicados {len(rows):,} updates")


if __name__ == "__main__":
    main()
