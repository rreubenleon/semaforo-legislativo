"""
Candado: conteos de senadores en FIAT vs la fuente OFICIAL (senado.gob.mx).

POR QUÉ EXISTE
  En may-2026 se detectó que FIAT inflaba a los senadores (Camarillo: 174 ini /
  780 prop en FIAT contra 73 / 140 en senado.gob.mx). El parser del campo
  `presentador` del SIL adjudicaba los bloques colectivos a UN solo senador.
  Se arregló reconstruyendo `actividad_legislador` desde `senador_instrumento`
  (scraper directo de senado.gob.mx)… pero el arreglo era un script de una sola
  vez y el paso `poblar_actividad_desde_sil` del pipeline lo revertía cada 4 h.

  Al 18-jul-2026 la regresión llevaba dos meses viva: 103 de 111 senadores
  estaban >15% arriba del oficial, con factor promedio 1.84. Nadie se enteró
  porque no había candado. Este archivo es el candado.

QUÉ HACE
  Compara, por senador, el número de instrumentos (iniciativas + proposiciones)
  en `actividad_legislador` contra `senador_instrumento` (oficial). No escribe
  nada: reporta. El pipeline lo llama y escribe un logger.error si falla, para
  que se vea en rojo en GitHub Actions.

Uso:
    python3 scripts/validar_senadores_vs_oficial.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection

# Tolerancia: el SIL y senado.gob.mx no cuadran al decimal (criterios de corte
# distintos, instrumentos de la Permanente, altas tardías). ±15% por senador es
# ruido normal; más que eso indica que el parser volvió a inflar.
TOLERANCIA = 1.15
# Si más de este % de los senadores comparables se sale, se considera falla
# sistémica y no un caso aislado.
MAX_PCT_FUERA = 10.0


def validar(verbose: bool = False) -> dict:
    conn = get_connection()

    oficial = dict(conn.execute("""
        SELECT senador_nombre, COUNT(*)
        FROM senador_instrumento
        GROUP BY senador_nombre
    """).fetchall())

    fiat = dict(conn.execute("""
        SELECT l.nombre, COUNT(*)
        FROM actividad_legislador a
        JOIN legisladores l ON l.id = a.legislador_id
        WHERE l.camara = 'Senado'
          AND (LOWER(a.tipo_instrumento) LIKE '%iniciativa%'
               OR LOWER(a.tipo_instrumento) LIKE '%proposici%')
        GROUP BY l.nombre
    """).fetchall())

    comparables, inflados, factores, detalle = 0, 0, [], []
    for nombre, n_ofi in oficial.items():
        if not n_ofi:
            continue
        n_fiat = fiat.get(nombre)
        if n_fiat is None:
            continue
        comparables += 1
        factor = n_fiat / n_ofi
        factores.append(factor)
        if factor > TOLERANCIA:
            inflados += 1
            detalle.append((nombre, n_ofi, n_fiat, round(factor, 2)))

    pct_fuera = (100.0 * inflados / comparables) if comparables else 0.0
    factor_prom = round(sum(factores) / len(factores), 2) if factores else 0.0
    ok = comparables > 0 and pct_fuera <= MAX_PCT_FUERA

    if verbose:
        print(f"Senadores comparables: {comparables}")
        print(f"Fuera de tolerancia (>{int((TOLERANCIA-1)*100)}%): {inflados} "
              f"({pct_fuera:.1f}%)  · factor promedio {factor_prom}")
        for nombre, o, f, fac in sorted(detalle, key=lambda x: -x[3])[:15]:
            print(f"  {nombre[:38]:38} oficial={o:>4}  fiat={f:>4}  x{fac}")
        print("RESULTADO:", "OK" if ok else "FALLA — FIAT no cuadra con senado.gob.mx")

    return {
        "ok": ok,
        "comparables": comparables,
        "inflados": inflados,
        "pct_fuera": round(pct_fuera, 1),
        "factor_promedio": factor_prom,
        "detalle": detalle[:25],
    }


if __name__ == "__main__":
    r = validar(verbose=True)
    sys.exit(0 if r["ok"] else 1)
