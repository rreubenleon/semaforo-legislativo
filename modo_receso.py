"""
Detector de modo "Receso Legislativo" para FIAT.

El Congreso de la Unión sesiona en periodos ordinarios:
  - 1er Periodo Ordinario:  1 sept - 15 dic
  - 2do Periodo Ordinario:  1 feb - 30 abr

Y entra en RECESO:
  - 1er Receso:  16 dic - 31 ene
  - 2do Receso:   1 may - 31 ago

Durante el receso solo sesiona la Comisión Permanente (37 titulares,
1x/semana). Las comisiones ordinarias prácticamente no sesionan.

Este módulo es la fuente única de verdad para "¿estamos en receso?"
y "¿debe correr este scraper hoy?".

Uso:
    from modo_receso import es_modo_receso, debe_correr_gaceta, debe_correr_comisiones

    if not debe_correr_gaceta():
        return  # skip
"""
from __future__ import annotations

import os
from datetime import date, datetime

# ──────────────────────────────────────────────────────────────────────
# OVERRIDE MANUAL — el cierre del periodo ordinario varía cada legislatura
#
# El periodo ordinario CONSTITUCIONALMENTE termina 30 abr / 15 dic, pero
# en la práctica suele cerrarse unos días antes (LXVI cerró el 29 abr
# 2026). Como esa fecha varía sesión a sesión, NO se hardcodea: cuando
# la Permanente se instala, mover esta variable a True y dejarla así
# hasta que arranque el siguiente periodo ordinario.
#
# Valores:
#   True   → fuerza receso (Permanente activa)
#   False  → fuerza ordinario (anula incluso fechas de mayo-ago / dic-ene)
#   None   → automático según calendario constitucional
#
# También se puede sobreescribir con env var FIAT_RECESO_OVERRIDE=true|false|auto
# (útil en CI sin tocar código).
# ──────────────────────────────────────────────────────────────────────
OVERRIDE_RECESO: bool | None = True  # LXVI 2do receso instalado 29 abr 2026


def _override_efectivo() -> bool | None:
    env = os.getenv("FIAT_RECESO_OVERRIDE", "").strip().lower()
    if env in ("true", "1", "yes", "si"):
        return True
    if env in ("false", "0", "no"):
        return False
    if env in ("auto", ""):
        return OVERRIDE_RECESO
    return OVERRIDE_RECESO


def es_modo_receso(hoy: date | None = None) -> bool:
    """
    Devuelve True si la fecha está dentro de uno de los recesos
    constitucionales. False si está en periodo ordinario.

    Si OVERRIDE_RECESO (o FIAT_RECESO_OVERRIDE) está set a True/False,
    fuerza ese valor sin importar la fecha.

    NOTA: NO toma en cuenta periodos extraordinarios (cuando se convoca
    al pleno fuera de calendario). Para esos casos hay que activar un
    override manual o detectar via SIL.
    """
    ov = _override_efectivo()
    if ov is not None:
        return ov

    hoy = hoy or date.today()
    m, d = hoy.month, hoy.day

    # 1er Receso constitucional: 16 dic - 31 ene
    if (m == 12 and d >= 16) or m == 1:
        return True
    # 2do Receso constitucional: 1 may - 31 ago
    if 5 <= m <= 8:
        return True
    return False


def es_primera_semana_del_mes(hoy: date | None = None) -> bool:
    """Día 1-7 del mes. Útil para 'primer lunes/martes del mes'."""
    hoy = hoy or date.today()
    return 1 <= hoy.day <= 7


def es_miercoles(hoy: date | None = None) -> bool:
    hoy = hoy or date.today()
    return hoy.weekday() == 2  # 0=Mon, 2=Wed


def debe_correr_gaceta(hoy: date | None = None) -> tuple[bool, str]:
    """
    ¿Debe correr el scraper de Gaceta hoy?

    - Periodo ordinario: SIEMPRE (cada 4h del pipeline principal).
    - Receso: solo los miércoles (cuando sesiona la Permanente).

    Returns: (correr: bool, motivo: str)
    """
    hoy = hoy or date.today()
    if not es_modo_receso(hoy):
        return True, "periodo ordinario — cadencia normal"
    if es_miercoles(hoy):
        return True, "receso, pero hoy es miércoles (sesión Permanente)"
    return False, "receso y no es miércoles — skip Gaceta hasta el próximo miércoles"


def debe_correr_comisiones(hoy: date | None = None) -> tuple[bool, str]:
    """
    ¿Debe correr el refresh de comisiones ordinarias hoy?

    - Periodo ordinario: SIEMPRE en su cron (lunes/martes).
    - Receso: solo el primer lunes/martes del mes (las ordinarias casi
      no sesionan; basta 1x/mes).

    Returns: (correr: bool, motivo: str)
    """
    hoy = hoy or date.today()
    if not es_modo_receso(hoy):
        return True, "periodo ordinario — cadencia normal"
    if es_primera_semana_del_mes(hoy):
        return True, "receso, pero es primera semana del mes (refresh mensual)"
    return False, "receso y no es primera semana del mes — skip hasta el mes siguiente"


def debe_correr_permanente(hoy: date | None = None) -> tuple[bool, str]:
    """
    ¿Debe correr el scraper de la Permanente hoy?
    Solo durante receso, todos los miércoles.
    """
    hoy = hoy or date.today()
    if not es_modo_receso(hoy):
        return False, "periodo ordinario — la Permanente no opera"
    if es_miercoles(hoy):
        return True, "receso + miércoles — sesión Permanente"
    return False, "receso pero no es miércoles — skip hasta el próximo miércoles"


def estado_actual(hoy: date | None = None) -> dict:
    """Diagnóstico para logs / dashboard."""
    hoy = hoy or date.today()
    return {
        "fecha": hoy.isoformat(),
        "es_receso": es_modo_receso(hoy),
        "es_miercoles": es_miercoles(hoy),
        "es_primera_semana_del_mes": es_primera_semana_del_mes(hoy),
        "gaceta_correr": debe_correr_gaceta(hoy)[0],
        "comisiones_correr": debe_correr_comisiones(hoy)[0],
        "permanente_correr": debe_correr_permanente(hoy)[0],
    }


if __name__ == "__main__":
    import json
    print(json.dumps(estado_actual(), indent=2, ensure_ascii=False))
