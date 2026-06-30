"""Hemeroteca de Proceso vía sus sitemaps mensuales (articles/YYYY-MM.xml),
que llegan hasta 1994. Por cada mes extrae (fecha, slug) de cada URL de nota.
El slug es descriptivo → se clasifica por keywords sin bajar el artículo.

Salida: data/proceso_hemeroteca.json  {fecha: [slugs]}.
Multi-fuente con La Jornada para el histórico de señal mediática. $0.
"""
import json, re, time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "proceso_hemeroteca.json"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
# meses 2024-08 → 2026-06 (cubre los outcomes SIL)
MESES = [f"{y}-{m:02d}" for y in (2024, 2025, 2026) for m in range(1, 13)
         if "2024-08" <= f"{y}-{m:02d}" <= "2026-06"]
URL_NOTA = re.compile(r'proceso\.com\.mx/(?:[a-z]+/)*(\d{4})/(\d{1,2})/(\d{1,2})/([a-z0-9-]+)')


def fetch(url, n=8):
    for _ in range(n):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode("utf-8", "ignore")
        except Exception:
            time.sleep(15)
    return None


def main():
    data = json.loads(OUT.read_text()) if OUT.exists() else {}
    for mes in MESES:
        if any(d.startswith(mes) for d in data):   # mes ya hecho
            continue
        x = fetch(f"https://www.proceso.com.mx/sitemaps/articles/{mes}.xml")
        if x is None:
            print(f"{mes}: red no abrió", flush=True)
            continue
        n = 0
        for y, mm, dd, slug in URL_NOTA.findall(x):
            f = f"{y}-{int(mm):02d}-{int(dd):02d}"
            data.setdefault(f, []).append(slug)
            n += 1
        OUT.write_text(json.dumps(data, ensure_ascii=False))
        print(f"{mes}: {n} notas", flush=True)
        time.sleep(2)
    tot = sum(len(v) for v in data.values())
    print(f"Listo: {len(data)} días, {tot} notas → {OUT}", flush=True)


if __name__ == "__main__":
    main()
