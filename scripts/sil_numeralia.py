"""
Scraper del watermark oficial: la Numeralia de asuntos del SIL.

La página https://sil.gobernacion.gob.mx/Numeralia/Asuntos/ es el tablero
oficial que publica el totalizador del universo legislativo por legislatura,
cámara, presentador y ventana de fechas. Es la única fuente que concilia
contra las cifras institucionales.

Truco del POST:
  El select PRESENTADOR usa value='*' para "Todos los presentadores". El valor
  '0' corresponde a "Selecciona..." y el backend lo rechaza con "No hay
  resultados con los criterios seleccionados". Dos horas de depuración cortesía
  del SIL.

Salida: dict con los conteos por tipo de asunto y columna (presentados,
aprobados, desechados, pendientes, atendidos, retirados), más el total.

Uso directo:
    python3 scripts/sil_numeralia.py
    python3 scripts/sil_numeralia.py --desde 01/09/2024 --hasta 10/04/2026
"""

import argparse
import sys
import urllib3

import requests
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NUMERALIA_FORM = "https://sil.gobernacion.gob.mx/Numeralia/Asuntos/"
NUMERALIA_POST = "https://sil.gobernacion.gob.mx/Numeralia/Asuntos/NumeraliaAsuntos.php"

CAMARAS = {
    "diputados": "1",
    "senadores": "2",
    "permanente": "5",
    "todas": "9",
}

COLUMNAS = [
    "presentados",
    "aprobados",
    "desechados",
    "pendientes",
    "atendidos",
    "retirados",
]


def obtener_numeralia(
    legislatura: str = "66",
    camara: str = "todas",
    fecha_desde: str = "",
    fecha_hasta: str = "",
    timeout: int = 30,
) -> dict:
    """
    Devuelve la tabla oficial de asuntos por tipo.

    Args:
        legislatura: número de legislatura (66 = LXVI).
        camara: 'diputados', 'senadores', 'permanente' o 'todas'.
        fecha_desde: dd/mm/yyyy o cadena vacía para todo el periodo.
        fecha_hasta: dd/mm/yyyy o cadena vacía.

    Returns:
        dict de la forma:
        {
            'Iniciativa': {'presentados': 7415, 'aprobados': 139, ...},
            'Proposición con punto de acuerdo': {...},
            ...
            'Totales de Asuntos': {...},
        }
    """
    if camara not in CAMARAS:
        raise ValueError(f"camara debe ser una de {list(CAMARAS)}")

    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-MX,es;q=0.9",
        "Referer": NUMERALIA_FORM,
    })

    # Precargar el form (buena práctica, aunque el backend no valida cookies)
    s.get(NUMERALIA_FORM, verify=False, timeout=timeout)

    data = {
        "LEGISLATURA": legislatura,
        "CAMARA": CAMARAS[camara],
        "PRESENTADOR": "*",  # imprescindible: '*' = todos
        "FECHA_INIC": fecha_desde,
        "FECHA_FIN": fecha_hasta,
    }
    r = s.post(NUMERALIA_POST, data=data, verify=False, timeout=timeout)
    r.raise_for_status()

    if "No hay resultados" in r.text:
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    resultado = {}
    for table in soup.find_all("table"):
        txt = table.get_text(" ", strip=True)
        if "Totales" not in txt or "Iniciativa" not in txt:
            continue
        for tr in table.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) < 7:
                continue
            etiqueta = cells[0]
            # filas de datos: primer celda es texto, siguientes 6 son números
            nums = []
            for c in cells[1:7]:
                try:
                    nums.append(int(c.replace(",", "")))
                except ValueError:
                    nums = None
                    break
            if nums is None:
                continue
            resultado[etiqueta] = dict(zip(COLUMNAS, nums))
        break
    return resultado


def _imprimir(tabla: dict) -> None:
    if not tabla:
        print("(sin resultados)")
        return
    print(f"{'TIPO':40} {'PRES':>8} {'APRO':>6} {'DESE':>6} {'PEND':>6} {'ATEN':>6} {'RETI':>6}")
    for tipo, cuentas in tabla.items():
        print(
            f"{tipo[:40]:40} "
            f"{cuentas['presentados']:>8} "
            f"{cuentas['aprobados']:>6} "
            f"{cuentas['desechados']:>6} "
            f"{cuentas['pendientes']:>6} "
            f"{cuentas['atendidos']:>6} "
            f"{cuentas['retirados']:>6}"
        )


def main() -> None:
    ap = argparse.ArgumentParser(description="Scraper de numeralia oficial SIL.")
    ap.add_argument("--legislatura", default="66")
    ap.add_argument("--camara", default="todas", choices=list(CAMARAS))
    ap.add_argument("--desde", default="", help="dd/mm/yyyy")
    ap.add_argument("--hasta", default="", help="dd/mm/yyyy")
    args = ap.parse_args()

    tabla = obtener_numeralia(
        legislatura=args.legislatura,
        camara=args.camara,
        fecha_desde=args.desde,
        fecha_hasta=args.hasta,
    )
    ventana = f"{args.desde or 'inicio'} → {args.hasta or 'hoy'}"
    print(f"Numeralia SIL | Legislatura {args.legislatura} | Cámara {args.camara} | {ventana}")
    print()
    _imprimir(tabla)


if __name__ == "__main__":
    main()
