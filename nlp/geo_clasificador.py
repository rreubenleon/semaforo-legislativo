"""
Semaforo Legislativo - Clasificador Geografico por Estado
Clasifica articulos de noticias por entidad federativa mexicana
a partir de menciones de estados y ciudades en titulo y resumen.
"""

import re
import sqlite3
import json
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuracion de imports del proyecto
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DATABASE

ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# ESTADOS_MEXICO - 32 entidades federativas
# ---------------------------------------------------------------------------
ESTADOS_MEXICO = {
    "aguascalientes": {
        "nombre": "Aguascalientes",
        "abreviatura": "AGU",
    },
    "baja_california": {
        "nombre": "Baja California",
        "abreviatura": "BCN",
    },
    "baja_california_sur": {
        "nombre": "Baja California Sur",
        "abreviatura": "BCS",
    },
    "campeche": {
        "nombre": "Campeche",
        "abreviatura": "CAM",
    },
    "chiapas": {
        "nombre": "Chiapas",
        "abreviatura": "CHP",
    },
    "chihuahua": {
        "nombre": "Chihuahua",
        "abreviatura": "CHH",
    },
    "ciudad_de_mexico": {
        "nombre": "Ciudad de Mexico",
        "abreviatura": "CMX",
    },
    "coahuila": {
        "nombre": "Coahuila",
        "abreviatura": "COA",
    },
    "colima": {
        "nombre": "Colima",
        "abreviatura": "COL",
    },
    "durango": {
        "nombre": "Durango",
        "abreviatura": "DUR",
    },
    "estado_de_mexico": {
        "nombre": "Estado de Mexico",
        "abreviatura": "MEX",
    },
    "guanajuato": {
        "nombre": "Guanajuato",
        "abreviatura": "GUA",
    },
    "guerrero": {
        "nombre": "Guerrero",
        "abreviatura": "GRO",
    },
    "hidalgo": {
        "nombre": "Hidalgo",
        "abreviatura": "HID",
    },
    "jalisco": {
        "nombre": "Jalisco",
        "abreviatura": "JAL",
    },
    "michoacan": {
        "nombre": "Michoacan",
        "abreviatura": "MIC",
    },
    "morelos": {
        "nombre": "Morelos",
        "abreviatura": "MOR",
    },
    "nayarit": {
        "nombre": "Nayarit",
        "abreviatura": "NAY",
    },
    "nuevo_leon": {
        "nombre": "Nuevo Leon",
        "abreviatura": "NLE",
    },
    "oaxaca": {
        "nombre": "Oaxaca",
        "abreviatura": "OAX",
    },
    "puebla": {
        "nombre": "Puebla",
        "abreviatura": "PUE",
    },
    "queretaro": {
        "nombre": "Queretaro",
        "abreviatura": "QUE",
    },
    "quintana_roo": {
        "nombre": "Quintana Roo",
        "abreviatura": "ROO",
    },
    "san_luis_potosi": {
        "nombre": "San Luis Potosi",
        "abreviatura": "SLP",
    },
    "sinaloa": {
        "nombre": "Sinaloa",
        "abreviatura": "SIN",
    },
    "sonora": {
        "nombre": "Sonora",
        "abreviatura": "SON",
    },
    "tabasco": {
        "nombre": "Tabasco",
        "abreviatura": "TAB",
    },
    "tamaulipas": {
        "nombre": "Tamaulipas",
        "abreviatura": "TAM",
    },
    "tlaxcala": {
        "nombre": "Tlaxcala",
        "abreviatura": "TLA",
    },
    "veracruz": {
        "nombre": "Veracruz",
        "abreviatura": "VER",
    },
    "yucatan": {
        "nombre": "Yucatan",
        "abreviatura": "YUC",
    },
    "zacatecas": {
        "nombre": "Zacatecas",
        "abreviatura": "ZAC",
    },
}

# ---------------------------------------------------------------------------
# CIUDADES_ESTADO - Mapeo de ciudades principales a su estado
# Al menos 50 ciudades relevantes de Mexico
# ---------------------------------------------------------------------------
CIUDADES_ESTADO = {
    # Nuevo Leon
    "Monterrey": "nuevo_leon",
    "San Pedro Garza Garcia": "nuevo_leon",
    "San Nicolas de los Garza": "nuevo_leon",
    "Apodaca": "nuevo_leon",
    "Guadalupe": "nuevo_leon",
    "Santa Catarina": "nuevo_leon",
    # Jalisco
    "Guadalajara": "jalisco",
    "Zapopan": "jalisco",
    "Tlaquepaque": "jalisco",
    "Tonala": "jalisco",
    "Puerto Vallarta": "jalisco",
    "Lagos de Moreno": "jalisco",
    # Quintana Roo
    "Cancun": "quintana_roo",
    "Playa del Carmen": "quintana_roo",
    "Chetumal": "quintana_roo",
    "Tulum": "quintana_roo",
    "Cozumel": "quintana_roo",
    # Baja California
    "Tijuana": "baja_california",
    "Mexicali": "baja_california",
    "Ensenada": "baja_california",
    "Rosarito": "baja_california",
    "Tecate": "baja_california",
    # Ciudad de Mexico
    "CDMX": "ciudad_de_mexico",
    "Coyoacan": "ciudad_de_mexico",
    "Iztapalapa": "ciudad_de_mexico",
    "Tlalpan": "ciudad_de_mexico",
    "Xochimilco": "ciudad_de_mexico",
    "Azcapotzalco": "ciudad_de_mexico",
    "Gustavo A. Madero": "ciudad_de_mexico",
    # Estado de Mexico
    "Toluca": "estado_de_mexico",
    "Ecatepec": "estado_de_mexico",
    "Naucalpan": "estado_de_mexico",
    "Nezahualcoyotl": "estado_de_mexico",
    "Tlalnepantla": "estado_de_mexico",
    "Atizapan": "estado_de_mexico",
    "Texcoco": "estado_de_mexico",
    # Puebla
    "Cholula": "puebla",
    "Tehuacan": "puebla",
    "Atlixco": "puebla",
    # Veracruz
    "Xalapa": "veracruz",
    "Coatzacoalcos": "veracruz",
    "Poza Rica": "veracruz",
    "Orizaba": "veracruz",
    "Cordoba": "veracruz",
    "Boca del Rio": "veracruz",
    "Minatitlan": "veracruz",
    # Sonora
    "Hermosillo": "sonora",
    "Ciudad Obregon": "sonora",
    "Nogales": "sonora",
    "Guaymas": "sonora",
    "San Luis Rio Colorado": "sonora",
    # Chihuahua
    "Ciudad Juarez": "chihuahua",
    "Delicias": "chihuahua",
    "Cuauhtemoc": "chihuahua",
    "Parral": "chihuahua",
    # Sinaloa
    "Culiacan": "sinaloa",
    "Mazatlan": "sinaloa",
    "Los Mochis": "sinaloa",
    "Guasave": "sinaloa",
    # Guanajuato
    "Leon": "guanajuato",
    "Irapuato": "guanajuato",
    "Celaya": "guanajuato",
    "Salamanca": "guanajuato",
    "Silao": "guanajuato",
    # Tamaulipas
    "Reynosa": "tamaulipas",
    "Matamoros": "tamaulipas",
    "Nuevo Laredo": "tamaulipas",
    "Ciudad Victoria": "tamaulipas",
    "Tampico": "tamaulipas",
    # Coahuila
    "Saltillo": "coahuila",
    "Torreon": "coahuila",
    "Monclova": "coahuila",
    "Piedras Negras": "coahuila",
    # Michoacan
    "Morelia": "michoacan",
    "Uruapan": "michoacan",
    "Zamora": "michoacan",
    "Lazaro Cardenas": "michoacan",
    # Oaxaca
    "Juchitan": "oaxaca",
    "Salina Cruz": "oaxaca",
    "Huatulco": "oaxaca",
    # Guerrero
    "Acapulco": "guerrero",
    "Chilpancingo": "guerrero",
    "Zihuatanejo": "guerrero",
    "Iguala": "guerrero",
    "Taxco": "guerrero",
    # Yucatan
    "Merida": "yucatan",
    "Valladolid": "yucatan",
    "Progreso": "yucatan",
    # Tabasco
    "Villahermosa": "tabasco",
    "Cardenas": "tabasco",
    "Comalcalco": "tabasco",
    # Chiapas
    "Tuxtla Gutierrez": "chiapas",
    "San Cristobal de las Casas": "chiapas",
    "Tapachula": "chiapas",
    "Comitan": "chiapas",
    "Palenque": "chiapas",
    # San Luis Potosi
    "Ciudad Valles": "san_luis_potosi",
    "Matehuala": "san_luis_potosi",
    "Soledad de Graciano Sanchez": "san_luis_potosi",
    # Queretaro
    "San Juan del Rio": "queretaro",
    # Hidalgo
    "Pachuca": "hidalgo",
    "Tulancingo": "hidalgo",
    "Tula de Allende": "hidalgo",
    # Morelos
    "Cuernavaca": "morelos",
    "Cuautla": "morelos",
    "Jiutepec": "morelos",
    # Durango
    "Gomez Palacio": "durango",
    "Lerdo": "durango",
    # Aguascalientes
    "Calvillo": "aguascalientes",
    # Nayarit
    "Tepic": "nayarit",
    "Bahia de Banderas": "nayarit",
    # Zacatecas
    "Fresnillo": "zacatecas",
    "Jerez": "zacatecas",
    # Colima
    "Manzanillo": "colima",
    "Tecoman": "colima",
    # Tlaxcala
    "Apizaco": "tlaxcala",
    "Huamantla": "tlaxcala",
    # Campeche
    "Ciudad del Carmen": "campeche",
    "Calkini": "campeche",
    # Baja California Sur
    "La Paz": "baja_california_sur",
    "Los Cabos": "baja_california_sur",
    "Cabo San Lucas": "baja_california_sur",
    "San Jose del Cabo": "baja_california_sur",
}

# ---------------------------------------------------------------------------
# Patrones de busqueda por estado (nombre del estado y variantes textuales)
# Se usa word boundary (\b) para evitar falsos positivos con nombres cortos.
# ---------------------------------------------------------------------------
PATRONES_ESTADO = {
    "aguascalientes": [r"\bAguascalientes\b"],
    "baja_california": [r"\bBaja\s+California\b(?!\s+Sur)"],
    "baja_california_sur": [r"\bBaja\s+California\s+Sur\b", r"\bBCS\b"],
    "campeche": [r"\bCampeche\b"],
    "chiapas": [r"\bChiapas\b"],
    "chihuahua": [r"\bChihuahua\b"],
    "ciudad_de_mexico": [
        r"\bCiudad\s+de\s+M[eé]xico\b",
        r"\bCDMX\b",
        r"\bDistrito\s+Federal\b",
        r"\bD\.?\s*F\.?\b",
    ],
    "coahuila": [r"\bCoahuila\b"],
    "colima": [r"\bColima\b"],
    "durango": [r"\bDurango\b"],
    "estado_de_mexico": [
        r"\bEstado\s+de\s+M[eé]xico\b",
        r"\bEdom[eé]x\b",
    ],
    "guanajuato": [r"\bGuanajuato\b"],
    "guerrero": [r"\bGuerrero\b"],
    "hidalgo": [r"\bHidalgo\b"],
    "jalisco": [r"\bJalisco\b"],
    "michoacan": [r"\bMichoac[aá]n\b"],
    "morelos": [r"\bMorelos\b"],
    "nayarit": [r"\bNayarit\b"],
    "nuevo_leon": [r"\bNuevo\s+Le[oó]n\b"],
    "oaxaca": [r"\bOaxaca\b"],
    "puebla": [r"\bPuebla\b"],
    "queretaro": [r"\bQuer[eé]taro\b"],
    "quintana_roo": [r"\bQuintana\s+Roo\b"],
    "san_luis_potosi": [r"\bSan\s+Luis\s+Potos[ií]\b"],
    "sinaloa": [r"\bSinaloa\b"],
    "sonora": [r"\bSonora\b"],
    "tabasco": [r"\bTabasco\b"],
    "tamaulipas": [r"\bTamaulipas\b"],
    "tlaxcala": [r"\bTlaxcala\b"],
    "veracruz": [r"\bVeracruz\b"],
    "yucatan": [r"\bYucat[aá]n\b"],
    "zacatecas": [r"\bZacatecas\b"],
}

# Pre-compilar todas las expresiones regulares
_PATRONES_COMPILADOS = {}
for _estado, _patrones in PATRONES_ESTADO.items():
    _PATRONES_COMPILADOS[_estado] = [
        re.compile(p, re.IGNORECASE) for p in _patrones
    ]

# Pre-compilar patrones de ciudades con word boundary
_CIUDADES_COMPILADAS = []
for _ciudad, _estado in CIUDADES_ESTADO.items():
    # Escapar caracteres especiales en nombres de ciudades y agregar word boundary
    patron = re.compile(r"\b" + re.escape(_ciudad) + r"\b", re.IGNORECASE)
    _CIUDADES_COMPILADAS.append((patron, _estado))


# ---------------------------------------------------------------------------
# Funciones de normalizacion de texto
# ---------------------------------------------------------------------------
def _normalizar_texto(texto):
    """Elimina acentos comunes para facilitar la comparacion.
    No reemplaza el texto original, solo genera una version auxiliar.
    """
    if not texto:
        return ""
    return texto


def _texto_combinado(titulo, resumen):
    """Combina titulo y resumen en un solo texto para buscar menciones."""
    partes = []
    if titulo:
        partes.append(titulo.strip())
    if resumen:
        partes.append(resumen.strip())
    return " ".join(partes)


# ---------------------------------------------------------------------------
# clasificar_estado(titulo, resumen)
# ---------------------------------------------------------------------------
def clasificar_estado(titulo, resumen):
    """Escanea titulo y resumen en busca de menciones a estados mexicanos
    (por nombre de estado o por nombre de ciudad).

    Retorna una lista de claves de estados encontrados (sin duplicados),
    por ejemplo: ["sonora", "jalisco"].
    """
    texto = _texto_combinado(titulo, resumen)
    if not texto:
        return []

    estados_encontrados = set()

    # 1. Buscar por patrones de nombre de estado
    for estado, patrones in _PATRONES_COMPILADOS.items():
        for patron in patrones:
            if patron.search(texto):
                estados_encontrados.add(estado)
                break  # Ya encontramos este estado, no seguir con otros patrones

    # 2. Buscar por nombres de ciudades
    for patron, estado in _CIUDADES_COMPILADAS:
        if estado not in estados_encontrados:
            if patron.search(texto):
                estados_encontrados.add(estado)

    return sorted(estados_encontrados)


# ---------------------------------------------------------------------------
# clasificar_articulos_por_estado()
# ---------------------------------------------------------------------------
def clasificar_articulos_por_estado():
    """Lee todos los articulos de la tabla 'articulos' en semaforo.db,
    clasifica cada uno por estado y retorna un diccionario agrupado.

    Retorno:
        {
            "sonora": [
                {
                    "titulo": "...",
                    "fecha": "...",
                    "url": "...",
                    "categorias": "..."
                },
                ...
            ],
            "jalisco": [...],
            ...
        }

    Solo incluye estados que tengan al menos un articulo asociado.
    """
    db_path = ROOT / DATABASE["archivo"]

    if not db_path.exists():
        return {}

    resultado = {}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(
            "SELECT titulo, fecha, resumen, url, categorias FROM articulos"
        )
        filas = cursor.fetchall()

        for fila in filas:
            titulo = fila["titulo"] or ""
            resumen = fila["resumen"] or ""
            fecha = fila["fecha"] or ""
            url = fila["url"] or ""
            categorias = fila["categorias"] or ""

            estados = clasificar_estado(titulo, resumen)

            if not estados:
                continue

            articulo = {
                "titulo": titulo,
                "fecha": fecha,
                "url": url,
                "categorias": categorias,
            }

            for estado in estados:
                if estado not in resultado:
                    resultado[estado] = []
                resultado[estado].append(articulo)

    except sqlite3.OperationalError as e:
        print(f"Error al acceder a la base de datos: {e}")
        return {}
    finally:
        if conn:
            conn.close()

    return resultado


# ---------------------------------------------------------------------------
# obtener_mapa_datos()
# ---------------------------------------------------------------------------
def obtener_mapa_datos():
    """Genera un resumen por estado listo para alimentar una visualizacion
    de mapa o dashboard.

    Retorno:
        {
            "sonora": {
                "nombre": "Sonora",
                "abreviatura": "SON",
                "total_articulos": 5,
                "categorias": ["salud", "energia"],
                "articulos": [top 5 articulos mas recientes]
            },
            ...
        }

    Solo incluye estados que tengan al menos un articulo clasificado.
    """
    articulos_por_estado = clasificar_articulos_por_estado()

    mapa = {}

    # Incluir TODOS los 32 estados, aunque no tengan articulos
    for estado_key, info_estado in ESTADOS_MEXICO.items():
        articulos = articulos_por_estado.get(estado_key, [])

        # Recopilar todas las categorias unicas de los articulos de este estado
        todas_categorias = set()
        for art in articulos:
            cats_raw = art.get("categorias", "")
            if cats_raw:
                # Las categorias pueden estar separadas por comas o ser JSON
                try:
                    cats_list = json.loads(cats_raw)
                    if isinstance(cats_list, list):
                        for c in cats_list:
                            if isinstance(c, str) and c.strip():
                                todas_categorias.add(c.strip())
                    elif isinstance(cats_list, str) and cats_list.strip():
                        todas_categorias.add(cats_list.strip())
                except (json.JSONDecodeError, TypeError):
                    # Tratar como texto separado por comas
                    for c in cats_raw.split(","):
                        c = c.strip()
                        if c:
                            todas_categorias.add(c)

        # Ordenar articulos por fecha descendente para obtener los mas recientes
        articulos_ordenados = sorted(
            articulos,
            key=lambda a: a.get("fecha", ""),
            reverse=True,
        )

        # Tomar los 5 articulos mas recientes
        top_articulos = articulos_ordenados[:5]

        mapa[estado_key] = {
            "nombre": info_estado["nombre"],
            "abreviatura": info_estado["abreviatura"],
            "total_articulos": len(articulos),
            "categorias": sorted(todas_categorias),
            "articulos": top_articulos,
        }

    return mapa


# ---------------------------------------------------------------------------
# Ejecucion directa para pruebas
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=== Clasificador Geografico por Estado ===")
    print()

    # Prueba rapida de clasificar_estado
    pruebas = [
        ("Incendio forestal en Sonora afecta 500 hectareas", ""),
        ("", "Autoridades de Monterrey implementan nuevo plan de seguridad"),
        ("Turismo en Cancun rompe record", "Quintana Roo recibe millones de visitantes"),
        ("Colima registra sismo de magnitud 4.5", "No se reportan danos en la entidad"),
        ("Nota sobre Colombia y su economia", "Exportaciones crecen en el pais sudamericano"),
        ("Guadalajara y Tijuana lideran en innovacion", "Jalisco y Baja California compiten"),
    ]

    for titulo, resumen in pruebas:
        estados = clasificar_estado(titulo, resumen)
        texto_muestra = titulo if titulo else resumen
        if len(texto_muestra) > 60:
            texto_muestra = texto_muestra[:60] + "..."
        print(f"  Texto: {texto_muestra}")
        print(f"  Estados: {estados}")
        print()

    # Clasificacion completa desde la base de datos
    print("=== Clasificacion de articulos desde la base de datos ===")
    mapa = obtener_mapa_datos()
    if mapa:
        for estado_key, datos in sorted(mapa.items()):
            print(
                f"  {datos['nombre']} ({datos['abreviatura']}): "
                f"{datos['total_articulos']} articulos - "
                f"Categorias: {', '.join(datos['categorias']) if datos['categorias'] else 'sin categoria'}"
            )
    else:
        print("  No se encontraron articulos o la base de datos no existe.")
    print()
    print("=== Fin ===")
