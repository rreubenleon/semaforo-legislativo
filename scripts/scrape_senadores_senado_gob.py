"""
Scraper de instrumentos de senadores LXVI desde www.senado.gob.mx.

Fuente: páginas oficiales de cada senador en su perfil del Senado.
  Listado: /66/senadores/por_grupo_parlamentario  (128 IDs)
  Perfil:  /66/senador/{id}
  Detalle: /66/doc/asuntoSenador.php?var1={iniciativas|proposiciones}&var2={id}&var3={partido}

Esta es la fuente oficial del Senado (la que también usan periodistas
como Leticia Robles para sus rankings). Es ESTRUCTURAL — no depende de
keywords ni de adivinanza. Para Pablo Angulo (id 1698) devuelve 222
iniciativas, exactamente lo que Robles publicó (137 ind + 85 firmadas).

Uso:
    python scripts/scrape_senadores_senado_gob.py
    python scripts/scrape_senadores_senado_gob.py --limite 5  # debug
    python scripts/scrape_senadores_senado_gob.py --solo 1698 # 1 senador
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import warnings
from dataclasses import asdict, dataclass
from pathlib import Path

import requests
from bs4 import BeautifulSoup

warnings.filterwarnings('ignore')

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

BASE = 'https://www.senado.gob.mx'
LISTADO_URL = f'{BASE}/66/senadores/por_grupo_parlamentario'
DETALLE_URL = f'{BASE}/66/doc/asuntoSenador.php'

UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 Chrome/120 Safari/537.36'
HEADERS = {'User-Agent': UA}

DELAY = 0.8  # seg entre requests; Senado tolera bien


@dataclass
class Instrumento:
    senador_id: int
    senador_nombre: str
    senador_partido: str
    tipo: str            # 'iniciativa' | 'proposicion'
    n_firmantes: int     # 1 = individual del senador / N = N senadores firman
    es_individual: bool  # True si n_firmantes == 1
    titulo: str
    promoventes_raw: str  # primer párrafo crudo
    turno: str           # "Se dio turno directo a las Comisiones..."
    fecha: str           # "Miércoles 29 de abril de 2026"
    enlace_gaceta: str   # URL al doc en gaceta del Senado


def obtener_listado_senadores() -> list[dict]:
    """Devuelve lista de {id, nombre, partido} para los 128 senadores LXVI."""
    r = requests.get(LISTADO_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')

    # Cada senador aparece como link a /66/senador/{id} con su nombre cerca
    # y el partido inferible del agrupamiento de la página.
    senadores = {}
    # Estructura: bloques por grupo parlamentario (h2/h3 con nombre del grupo)
    # seguido de tarjetas con <a href="/66/senador/{id}">
    partido_actual = ''
    for el in soup.find_all(['h1','h2','h3','h4','a']):
        if el.name in ('h1','h2','h3','h4'):
            txt = el.get_text(strip=True).upper()
            for p in ('MORENA', 'PAN', 'PRI', 'PVEM', 'PT', 'MC', 'INDEPENDIENTE', 'SIN GRUPO'):
                if p in txt:
                    partido_actual = p
                    break
        elif el.name == 'a':
            href = el.get('href', '')
            m = re.search(r'/66/senador/(\d+)', href)
            if m:
                sid = int(m.group(1))
                if sid not in senadores:
                    nombre = el.get_text(strip=True) or el.find('img', alt=True).get('alt', '') if el.find('img', alt=True) else ''
                    nombre = re.sub(r'\s+', ' ', nombre).strip()
                    senadores[sid] = {'id': sid, 'nombre': nombre, 'partido': partido_actual or 'SIN GRUPO'}

    # Si los nombres salieron vacíos (caso común porque el <a> envuelve solo la <img>),
    # rellenar consultando cada perfil — pero más eficiente: extraer del alt de la <img>
    return list(senadores.values())


def parsear_perfil_para_partido(senador_id: int) -> tuple[str, str]:
    """
    Devuelve (nombre, partido) leídos del perfil individual del senador.
    Detecta partido buscando frases típicas en el HTML.
    """
    r = requests.get(f'{BASE}/66/senador/{senador_id}', headers=HEADERS, timeout=30)
    if r.status_code != 200 or len(r.text) < 1000:
        return '', ''
    soup = BeautifulSoup(r.text, 'html.parser')
    nombre = ''
    if soup.title and soup.title.string:
        nombre = re.sub(r'^Sen\.\s*', '', soup.title.string).strip()

    text = r.text  # mantenemos HTML para no perder contextos
    # Patrones por partido (ordenados por especificidad).
    # Senado escribe el partido en frases como "Grupo Parlamentario de MORENA",
    # "Grupo Parlamentario del Partido Acción Nacional", etc.
    PATRONES = [
        ('MORENA', [r'Grupo\s+Parlamentario\s+de\s+MORENA', r'>\s*MORENA\s*<', r'Movimiento\s+Regeneraci[oó]n\s+Nacional']),
        ('PAN',    [r'Partido\s+Acci[oó]n\s+Nacional', r'>\s*PAN\s*<']),
        ('PRI',    [r'Revolucionario\s+Institucional', r'>\s*PRI\s*<']),
        ('PVEM',   [r'Verde\s+Ecologista', r'>\s*PVEM\s*<']),
        ('PT',     [r'Partido\s+del\s+Trabajo', r'Grupo\s+Parlamentario\s+del?\s+PT', r'>\s*PT\s*<']),
        ('MC',     [r'Movimiento\s+Ciudadano', r'>\s*MC\s*<']),
    ]
    for partido, patrones in PATRONES:
        for p in patrones:
            if re.search(p, text, flags=re.IGNORECASE):
                return nombre, partido
    return nombre, ''


def _contar_firmantes(txt: str) -> int:
    """
    Devuelve el número de senadores firmantes en el preámbulo del bloque.

    Heurística validada contra Robles (Excélsior 4-may-2026): para Pablo
    Angulo da 137 individuales + 85 colectivas = 222, que es el total
    publicado por el Senado y reportado por Robles. Match perfecto.

    Reglas:
      1. Cortar el preámbulo en "con proyecto de" o "con punto de"
      2. Quitar prefijo opcional "de Ciudadanos Legisladores"
      3. Si arranca con "Del Sen./Senador" o "De la Sen./Senadora" Y
         no contiene "y los/las senadores", es individual (1).
      4. Si no, contar nombres tipo "Pablo Guillermo Angulo Briceño"
         (1-4 palabras + apellido capitalizado).
    """
    # Cortar preámbulo
    pre = re.split(r'\s*,?\s*(?:con\s+proyecto\s+de|con\s+punto\s+de)\s+',
                   txt, maxsplit=1)[0][:600]
    # Iniciativa Ciudadana usa prefijo decorativo: ignorar
    pre_eval = re.sub(r'^\s*de\s+Ciudadanos\s+Legisladores\s*',
                      '', pre, flags=re.IGNORECASE).strip()

    es_singular = bool(re.match(
        r'^\s*Del?\s+Sen(?:\.|ador[a]?)\s', pre_eval, flags=re.IGNORECASE
    ))
    if es_singular and re.search(
        r'\b(?:y\s+(?:los?|las?)\s+senador|los?\s+senadores|las?\s+senadoras)\b',
        pre_eval, flags=re.IGNORECASE
    ):
        es_singular = False

    if es_singular:
        return 1
    # Plural: contar nombres "Nombre [Segundo] Apellido [Apellido]"
    nombres = re.findall(
        r'(?:[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+\s+){1,4}[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+',
        pre_eval
    )
    return max(2, len(nombres))


def parsear_bloque(bloque_html: str, senador_id: int, senador_nombre: str,
                   senador_partido: str, tipo: str) -> Instrumento | None:
    """Convierte un bloque HTML separado por <hr> en un Instrumento."""
    soup = BeautifulSoup(bloque_html, 'html.parser')
    txt = soup.get_text(' ', strip=True)
    # Quitar el header "Iniciativas" o "Proposiciones" del label
    txt = re.sub(r'^(Iniciativas|Proposiciones?\s+con\s+Punto\s+de\s+Acuerdo|Proposiciones)\s*',
                 '', txt, flags=re.IGNORECASE).strip()
    if 'Fecha de publicación' not in txt:
        return None

    n_firmantes = _contar_firmantes(txt)
    es_individual = (n_firmantes == 1)

    # Extraer turno
    m_turno = re.search(r'(Se dio turno[^.]+\.|Se turn[óo][^.]+\.)', txt, flags=re.IGNORECASE)
    turno = m_turno.group(1) if m_turno else ''

    # Fecha
    m_fecha = re.search(r'Fecha de publicación:\s*([^.]+?)(?=\s*Promovente|$)', txt)
    fecha = m_fecha.group(1).strip() if m_fecha else ''

    # Enlace a la gaceta
    m_link = soup.find('a', href=re.compile(r'gaceta_del_senado'))
    enlace = m_link['href'] if m_link else ''

    # Título: lo que viene entre el preámbulo y "Se dio turno"
    # Buscamos "con proyecto de decreto" o "con punto de acuerdo" como pivot
    titulo = ''
    for piv in ('con proyecto de decreto', 'con punto de acuerdo', 'que', 'que reforma',
                'que adiciona', 'que expide'):
        idx = txt.lower().find(piv)
        if idx != -1:
            # Tomar 150 chars después del pivot como título
            titulo = txt[idx:idx+200].strip()
            break
    if not titulo:
        titulo = txt[:150]

    return Instrumento(
        senador_id=senador_id,
        senador_nombre=senador_nombre,
        senador_partido=senador_partido,
        tipo=tipo,
        n_firmantes=n_firmantes,
        es_individual=es_individual,
        titulo=titulo[:250],
        promoventes_raw=txt[:300],
        turno=turno[:300],
        fecha=fecha,
        enlace_gaceta=enlace,
    )


def scrape_senador(senador_id: int, nombre: str, partido: str) -> list[Instrumento]:
    instrumentos: list[Instrumento] = []
    for tipo, var1 in [('iniciativa', 'iniciativas'), ('proposicion', 'proposiciones')]:
        url = f'{DETALLE_URL}?var1={var1}&var2={senador_id}&var3={partido or ""}'
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                continue
            html = r.text
            bloques = [b for b in html.split('<hr>') if 'Fecha de publicación' in b]
            for b in bloques:
                inst = parsear_bloque(b, senador_id, nombre, partido, tipo)
                if inst:
                    instrumentos.append(inst)
        except Exception as e:
            print(f'  ERROR {tipo} {senador_id}: {e}', file=sys.stderr)
        time.sleep(DELAY)
    return instrumentos


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limite', type=int, default=None,
                    help='solo procesar primeros N senadores (debug)')
    ap.add_argument('--solo', type=int, default=None,
                    help='procesar solo este senador_id')
    ap.add_argument('--out', default='dashboard/senadores_lxvi_oficial.json',
                    help='archivo de salida JSON')
    args = ap.parse_args()

    if args.solo:
        # Buscar partido del único pedido
        nombre, partido = parsear_perfil_para_partido(args.solo)
        if not partido:
            partido = 'PRI'  # fallback razonable
        senadores = [{'id': args.solo, 'nombre': nombre, 'partido': partido}]
    else:
        print('Obteniendo listado de senadores LXVI...')
        senadores = obtener_listado_senadores()
        print(f'  {len(senadores)} senadores con ID')
        # Llenar nombre+partido con perfiles para los que vinieron vacíos
        for s in senadores:
            if not s['nombre'] or not s['partido']:
                nm, pt = parsear_perfil_para_partido(s['id'])
                if nm:
                    s['nombre'] = nm
                if pt:
                    s['partido'] = pt
                time.sleep(DELAY)

    if args.limite:
        senadores = senadores[:args.limite]

    todos: list[Instrumento] = []
    t0 = time.time()
    for i, s in enumerate(senadores, 1):
        print(f'[{i}/{len(senadores)}] {s["nombre"]} ({s["partido"]}) id={s["id"]}',
              file=sys.stderr)
        inst = scrape_senador(s['id'], s['nombre'], s['partido'])
        todos.extend(inst)
        n_ind = sum(1 for x in inst if x.es_individual)
        print(f'  → {len(inst)} instrumentos ({n_ind} individuales)', file=sys.stderr)

    dur = time.time() - t0
    print(f'\nTotal: {len(todos)} instrumentos en {dur:.0f}s', file=sys.stderr)

    # Salvar a JSON
    out_path = ROOT / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(
        {'senadores': senadores, 'instrumentos': [asdict(x) for x in todos]},
        ensure_ascii=False, indent=2
    ))
    print(f'Guardado en {out_path}')


if __name__ == '__main__':
    main()
