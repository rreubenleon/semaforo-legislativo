"""Scrape de la hemeroteca de La Jornada para backfill de señal mediática
histórica (2024→presente), para calibrar el score con más historia.

Por día baja las secciones relevantes y extrae los titulares (sección + texto).
Guarda crudo {fecha: {seccion: [titulares]}} en data/jornada_hemeroteca.json,
para clasificar después contra cualquiera de las 19 categorías (offline).

- Educado: delay entre requests, retries suaves, tolera 404 (días sin edición).
- Incremental + resumible: guarda cada 10 días; si re-corre, salta lo hecho.
- $0, sin Haiku. La Jornada YA es una de nuestras fuentes (mismo origen).
"""
import json, re, time, sys
import urllib.request
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "jornada_hemeroteca.json"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
SECCIONES = ["politica", "economia", "sociedad", "capital", "estados", "mundo"]
INICIO = date(2024, 8, 29)   # arranque de outcomes SIL
FIN = date(2026, 6, 28)
DELAY = 1.0                   # s entre requests (educado)
ANCHOR = re.compile(r'<a [^>]*href="([^"]+)"[^>]*>([^<]{18,})</a>')
ES_NOTA = re.compile(r'[a-z]+/\d{3}[a-z]\d')   # ej. politica/002n1pol


def fetch(url):
    for intento in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=25) as r:
                return r.read().decode("utf-8", "ignore")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None          # día/sección sin edición
            time.sleep(3 * (intento + 1))
        except Exception:
            time.sleep(3 * (intento + 1))
    return None


def titulares(html):
    out = []
    for href, txt in ANCHOR.findall(html or ""):
        if ES_NOTA.search(href):
            t = re.sub(r"\s+", " ", txt).strip()
            if t:
                out.append(t)
    return list(dict.fromkeys(out))   # dedup preservando orden


def main():
    data = json.loads(OUT.read_text()) if OUT.exists() else {}
    dias = []
    d = INICIO
    while d <= FIN:
        dias.append(d.isoformat())
        d += timedelta(days=1)
    pendientes = [x for x in dias if x not in data]
    print(f"Total días {len(dias)} | ya hechos {len(dias)-len(pendientes)} | pendientes {len(pendientes)}",
          flush=True)

    for i, f in enumerate(pendientes, 1):
        y, m, dd = f.split("-")
        dia = {}
        for s in SECCIONES:
            html = fetch(f"https://www.jornada.com.mx/{y}/{m}/{dd}/{s}")
            if html:
                tt = titulares(html)
                if tt:
                    dia[s] = tt
            time.sleep(DELAY)
        data[f] = dia
        if i % 10 == 0:
            OUT.write_text(json.dumps(data, ensure_ascii=False))
            tot = sum(len(v) for dd2 in data.values() for v in dd2.values())
            print(f"  {i}/{len(pendientes)} ({f}) · {tot} titulares acumulados", flush=True)

    OUT.write_text(json.dumps(data, ensure_ascii=False))
    con_notas = sum(1 for v in data.values() if v)
    tot = sum(len(v) for dd2 in data.values() for v in dd2.values())
    print(f"Listo: {len(data)} días ({con_notas} con notas), {tot} titulares → {OUT}", flush=True)


if __name__ == "__main__":
    main()
