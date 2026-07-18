"""Panel fijo de medios nacionales con archivo histórico — sombra de la
gráfica "Evolución de temas".

Cuenta notas POR DÍA y POR TEMA de 7 medios cuyos sitemaps permiten pedir
cualquier fecha pasada (el RSS solo trae lo reciente y dejó huecos:
arranque 13-feb-2026 y migración Turso 12-mar→8-abr-2026). Metodología
UNIFORME para toda la línea de tiempo: mismos 7 medios, misma
clasificación (keywords FIAT de config.CATEGORIAS sobre el slug de la
URL, con fronteras de palabra — 'presa' NO matchea 'empresa'), cada día.

Incremental: lee eval/panel_medios_diario.json y solo baja días
faltantes; los 3 más recientes se refrescan siempre (los sitemaps del
día siguen creciendo). Costo $0 — HTTP con UA de navegador, ~1s entre
requests al mismo dominio.
"""
import datetime as dt
import json
import re
import sys
import time
import unicodedata
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from config import CATEGORIAS

SALIDA = ROOT / "eval" / "panel_medios_diario.json"
DESDE = "2026-02-01"
REFRESCA_ULTIMOS = 3  # días recientes que siempre se re-bajan

UA = {"User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/126.0 Safari/537.36")}

# Arc Publishing: un sitemap por día. Los mensuales van aparte.
ARC = {
    "razon": "https://www.razon.com.mx/arc/outboundfeeds/sitemap/{d}/?outputType=xml",
    "cronica": "https://www.cronica.com.mx/arc/outboundfeeds/sitemap/{d}/?outputType=xml",
    "elfinanciero": "https://www.elfinanciero.com.mx/arc/outboundfeeds/sitemap3/{d}/?outputType=xml",
    "politico": "https://www.politico.mx/arc/outboundfeeds/sitemap3/{d}/?outputType=xml",
    "bloomberglinea": "https://www.bloomberglinea.com/arc/outboundfeeds/sitemap/{d}/?outputType=xml",
}
MENSUALES = {
    "eleconomista": "https://www.eleconomista.com.mx/sitemap-noticias-{ym}.xml",
    "ovaciones": "https://ovaciones.com/sitemapnoticias/{ym}",
}
PANEL = sorted(list(ARC) + list(MENSUALES))


def na(s):
    return "".join(c for c in unicodedata.normalize("NFD", (s or "").lower())
                   if unicodedata.category(c) != "Mn")


def _kw_cat():
    """categoria -> regex de TODOS sus keywords (subcategorias), \\b-delimitado."""
    out = {}
    for cat, v in CATEGORIAS.items():
        kws = set()
        for sv in (v.get("subcategorias") or {}).values():
            lst = sv.get("keywords", []) if isinstance(sv, dict) else list(sv)
            for k in lst:
                k = na(k).strip()
                if len(k) >= 3:
                    kws.add(re.escape(k).replace(r"\ ", r"\s+"))
        if kws:
            out[cat] = re.compile(r"\b(?:" + "|".join(sorted(kws)) + r")\b")
    return out


RX_CAT = _kw_cat()
ORDEN_CAT = list(CATEGORIAS)


def clasifica(texto):
    """Slug normalizado -> categoría primaria (más keywords) o None."""
    mejor, n_mejor = None, 0
    for cat in ORDEN_CAT:
        rx = RX_CAT.get(cat)
        if not rx:
            continue
        n = len(rx.findall(texto))
        if n > n_mejor:
            mejor, n_mejor = cat, n
    return mejor


def slug(url):
    p = url.rstrip("/").rsplit("/", 1)[-1]
    p = re.sub(r"\.html?$", "", p)
    p = re.sub(r"-\d{8}-\d+$", "", p)  # sufijo economista -20260331-806879
    return na(p.replace("-", " "))


def get(url, timeout=25, reintentos=2):
    for i in range(reintentos):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", "ignore")
        except Exception as e:
            if i == reintentos - 1:
                print(f"    ✗ {url.split('/')[2]}: {str(e)[:80]}")
                return None
            time.sleep(2 * (i + 1))


def main():
    hoy = dt.date.today().isoformat()
    if SALIDA.exists():
        data = json.loads(SALIDA.read_text())
    else:
        data = {"metadata": {}, "dias": {}}
    dias_data = data["dias"]

    d0 = dt.date.fromisoformat(DESDE)
    d1 = dt.date.today()
    todos = [(d0 + dt.timedelta(days=i)).isoformat()
             for i in range((d1 - d0).days + 1)]
    refresca = set(todos[-REFRESCA_ULTIMOS:])
    # re-intenta también días con panel incompleto (un 404 pasado se autocura)
    pendientes = [d for d in todos if d not in dias_data or d in refresca
                  or len(dias_data[d].get("fuentes", {})) < len(PANEL)]
    print(f"panel_medios: {len(pendientes)} días por bajar "
          f"(de {len(todos)}; cache {len(dias_data)})")

    # ── mensuales: 1 request por mes tocado, fecha real por nota ──
    meses = sorted({d[:7].replace("-", "") for d in pendientes})
    notas_mes = {m: {} for m in MENSUALES}  # medio -> fecha -> [slugs]
    for medio, patron in MENSUALES.items():
        for ym in meses:
            body = get(patron.format(ym=ym))
            time.sleep(1.0)
            if not body:
                continue
            fechas = re.findall(
                r"<(?:lastmod|news:publication_date)>(\d{4}-\d{2}-\d{2})", body)
            locs = re.findall(r"<loc>([^<]+)</loc>", body)
            for f, u in zip(fechas, locs):
                notas_mes[medio].setdefault(f, []).append(slug(u))
        print(f"  {medio}: {sum(len(v) for v in notas_mes[medio].values())} notas "
              f"en {len(meses)} meses")

    # ── por día: Arc dailies + rebanada de los mensuales ──
    for i, d in enumerate(pendientes):
        dia = {"total": 0, "fuentes": {}, "cats": {}}
        for medio, patron in ARC.items():
            body = get(patron.format(d=d))
            time.sleep(1.0)
            if body is None:
                continue
            locs = re.findall(r"<loc>([^<]+)</loc>", body)
            if medio == "bloomberglinea":
                locs = [u for u in locs if "/mexico/" in u]
            dia["fuentes"][medio] = len(locs)
            for u in locs:
                dia["total"] += 1
                c = clasifica(slug(u))
                if c:
                    dia["cats"][c] = dia["cats"].get(c, 0) + 1
        for medio in MENSUALES:
            slugs = notas_mes[medio].get(d, [])
            dia["fuentes"][medio] = len(slugs)
            for s in slugs:
                dia["total"] += 1
                c = clasifica(s)
                if c:
                    dia["cats"][c] = dia["cats"].get(c, 0) + 1
        dias_data[d] = dia
        if i % 10 == 0 or i == len(pendientes) - 1:
            print(f"  [{i+1}/{len(pendientes)}] {d}: total {dia['total']} · "
                  f"{len(dia['fuentes'])} fuentes · "
                  f"clasificadas {sum(dia['cats'].values())}", flush=True)
            SALIDA.parent.mkdir(exist_ok=True)
            data["metadata"] = {
                "panel": PANEL, "desde": DESDE, "actualizado": hoy,
                "metodo": "sitemaps por día/mes; clasificación keywords FIAT "
                          "sobre slug de URL con fronteras de palabra",
            }
            SALIDA.write_text(json.dumps(data, ensure_ascii=False))

    SALIDA.write_text(json.dumps(data, ensure_ascii=False))
    tot = sum(v["total"] for v in dias_data.values())
    print(f"✅ panel_medios: {len(dias_data)} días · {tot} notas · → {SALIDA}")


if __name__ == "__main__":
    main()
