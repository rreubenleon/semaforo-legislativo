"""
Genera selección estratificada de 100 títulos para benchmark manual del
clasificador. Produce un HTML local auto-contenido con form interactivo.

Composición:
  - 4 por categoría × 19 = 76 confiados (score top ≥ 0.60)
  - 1 por categoría × 19 = 19 ambiguos (score top 0.30–0.50)
  - 5 wild cards (score 0.25–0.40 de cualquier categoría)
  = 100 total

Output:
  eval/benchmark_100/index.html   — formulario interactivo
  eval/benchmark_100/data.json    — datos crudos (para auditoría)
"""
import json
import random
import sqlite3
from pathlib import Path

random.seed(42)  # reproducible

ROOT = Path(__file__).resolve().parent.parent.parent
DB = ROOT / "semaforo.db"
OUT_DIR = Path(__file__).resolve().parent
OUT_DATA = OUT_DIR / "data.json"
OUT_HTML = OUT_DIR / "index.html"

# Categorías activas (del data.json)
CATS = sorted([c["categoria"] for c in json.load(open(ROOT / "dashboard" / "data.json"))["semaforo"]])


def parse_top(cats_str):
    """'electoral_politico:0.75|salud:0.32' → ('electoral_politico', 0.75)"""
    first = cats_str.split("|")[0]
    # Split maxsplit=1 para manejar casos con múltiples ':'
    partes = first.split(":", 1)
    if len(partes) < 2:
        return first.strip(), 0.0
    cat = partes[0].strip()
    try:
        return cat, float(partes[1].strip())
    except ValueError:
        return cat, 0.0


def cargar_articulos(conn):
    rows = conn.execute("""
        SELECT id, titulo, fuente, fecha, categorias, resumen
        FROM articulos
        WHERE fecha >= '2026-03-01'
          AND categorias IS NOT NULL AND categorias != ''
          AND titulo IS NOT NULL AND LENGTH(titulo) > 20
        ORDER BY fecha DESC
    """).fetchall()
    out = []
    for r in rows:
        top_cat, top_score = parse_top(r["categorias"])
        if top_cat in CATS:
            out.append({
                "id": r["id"],
                "titulo": r["titulo"],
                "fuente": r["fuente"] or "",
                "fecha": (r["fecha"] or "")[:10],
                "resumen": (r["resumen"] or "")[:400],
                "cat_clasificador": top_cat,
                "score": round(top_score, 2),
            })
    return out


def estratificar(articulos):
    por_cat = {c: [] for c in CATS}
    for a in articulos:
        por_cat[a["cat_clasificador"]].append(a)
    for c in CATS:
        por_cat[c].sort(key=lambda a: -a["score"])

    seleccion = []
    usados = set()

    # 4 confiados por cat (score ≥ 0.60)
    for c in CATS:
        confiados = [a for a in por_cat[c] if a["score"] >= 0.60 and a["id"] not in usados]
        random.shuffle(confiados)
        for a in confiados[:4]:
            a["tipo"] = "confiado"
            seleccion.append(a)
            usados.add(a["id"])

    # 1 ambiguo por cat (score 0.30–0.50)
    for c in CATS:
        ambiguos = [a for a in por_cat[c] if 0.30 <= a["score"] < 0.50 and a["id"] not in usados]
        random.shuffle(ambiguos)
        for a in ambiguos[:1]:
            a["tipo"] = "ambiguo"
            seleccion.append(a)
            usados.add(a["id"])

    # Wild cards: muy ambiguos (0.25–0.40) de cualquier cat
    pool = [a for c in CATS for a in por_cat[c]
            if 0.25 <= a["score"] < 0.40 and a["id"] not in usados]
    random.shuffle(pool)
    for a in pool[:100 - len(seleccion)]:
        a["tipo"] = "wildcard"
        seleccion.append(a)
        usados.add(a["id"])

    # Completar hasta 100 con medio-score adicionales si faltan
    faltan = 100 - len(seleccion)
    if faltan > 0:
        extras_pool = [a for c in CATS for a in por_cat[c]
                       if 0.40 <= a["score"] < 0.60 and a["id"] not in usados]
        random.shuffle(extras_pool)
        for a in extras_pool[:faltan]:
            a["tipo"] = "ambiguo"
            seleccion.append(a)
            usados.add(a["id"])

    # Mezclar para que no quede agrupado por categoría
    random.shuffle(seleccion)
    return seleccion


def generar_html(seleccion):
    cats_json = json.dumps(CATS, ensure_ascii=False)
    datos_json = json.dumps(seleccion, ensure_ascii=False, indent=2)

    cat_label = {
        "administracion": "Administración", "agro_rural": "Agro y Desarrollo Rural",
        "anticorrupcion": "Anticorrupción", "derechos_humanos": "Derechos Humanos",
        "economia_hacienda": "Economía y Hacienda", "educacion": "Educación",
        "electoral_politico": "Electoral y Político", "energia": "Energía",
        "igualdad_genero": "Igualdad de Género", "infraestructura": "Infraestructura",
        "inteligencia_artificial": "Inteligencia Artificial", "medio_ambiente": "Medio Ambiente",
        "medios_comunicacion": "Medios de Comunicación", "politica_social": "Política Social",
        "relaciones_exteriores": "Relaciones Exteriores", "salud": "Salud",
        "seguridad_justicia": "Seguridad y Justicia", "trabajo": "Trabajo",
        "turismo": "Turismo",
    }
    cat_label_json = json.dumps(cat_label, ensure_ascii=False)

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>FIAT · Benchmark 100 títulos</title>
<style>
  :root {{
    --acento: #2e7d32;
    --borde: #e5e7eb;
    --fondo: #fafafa;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    font-size: 14px;
    line-height: 1.5;
    color: #111827;
    background: var(--fondo);
    margin: 0;
    padding: 0;
  }}
  .mono {{ font-family: "JetBrains Mono", ui-monospace, monospace; }}
  header {{
    position: sticky; top: 0; z-index: 10;
    background: white; border-bottom: 1px solid var(--borde);
    padding: 12px 24px; display: flex; align-items: center; gap: 20px;
  }}
  header h1 {{ margin: 0; font-size: 16px; font-weight: 600; }}
  .prog {{
    flex: 1; max-width: 400px; height: 6px; background: #e5e7eb; border-radius: 3px; overflow: hidden;
  }}
  .prog > div {{ height: 100%; background: var(--acento); transition: width 0.3s; }}
  .count {{ font-variant-numeric: tabular-nums; font-weight: 600; }}
  button {{
    padding: 6px 14px; border: 1px solid var(--borde); background: white; border-radius: 6px;
    cursor: pointer; font-size: 13px; font-weight: 500;
  }}
  button.primary {{ background: var(--acento); color: white; border-color: var(--acento); }}
  button:hover {{ background: #f3f4f6; }}
  button.primary:hover {{ background: #266229; }}
  .filters {{
    background: white; padding: 10px 24px; border-bottom: 1px solid var(--borde);
    display: flex; gap: 10px; align-items: center; font-size: 12px;
  }}
  .filters select, .filters label {{ font-size: 12px; }}
  .tbl {{ padding: 16px 24px 80px; }}
  .row {{
    background: white; border: 1px solid var(--borde); border-radius: 8px;
    margin-bottom: 8px; padding: 12px 16px;
    display: grid; grid-template-columns: 40px 1fr 280px; gap: 14px; align-items: start;
  }}
  .row.etiquetado {{ border-left: 3px solid var(--acento); }}
  .row.acierto {{ background: #f0fdf4; }}
  .row.error {{ background: #fef2f2; }}
  .idx {{ font-weight: 600; color: #9ca3af; font-size: 12px; padding-top: 2px; }}
  .cuerpo .titulo {{ font-weight: 600; color: #111827; margin-bottom: 4px; line-height: 1.4; }}
  .cuerpo .meta {{ font-size: 11px; color: #6b7280; display: flex; gap: 8px; flex-wrap: wrap; }}
  .cuerpo .meta > * {{}}
  .cuerpo .resumen {{
    font-size: 12px; color: #4b5563; margin-top: 6px; line-height: 1.4;
    max-height: 60px; overflow: hidden; text-overflow: ellipsis;
  }}
  .pill {{ padding: 1px 6px; border-radius: 3px; font-size: 10px; font-weight: 600; }}
  .pill.confiado {{ background: #dcfce7; color: #166534; }}
  .pill.ambiguo {{ background: #fef3c7; color: #92400e; }}
  .pill.wildcard {{ background: #e0e7ff; color: #3730a3; }}
  .pill.score {{ background: #f3f4f6; color: #374151; font-family: "JetBrains Mono", monospace; }}
  .pred {{
    padding: 4px 10px; border-radius: 4px; background: #f9fafb;
    font-size: 11px; color: #6b7280; margin-bottom: 6px;
  }}
  .pred strong {{ color: #111827; font-weight: 600; }}
  select.categoria {{
    width: 100%; padding: 6px 8px; font-size: 13px;
    border: 1px solid var(--borde); border-radius: 6px; background: white; cursor: pointer;
  }}
  select.categoria:focus {{ outline: 2px solid var(--acento); outline-offset: -1px; }}
  .hint {{ font-size: 10px; color: #9ca3af; margin-top: 4px; font-style: italic; }}
  .export-bar {{
    position: fixed; bottom: 0; left: 0; right: 0; background: white;
    border-top: 1px solid var(--borde); padding: 12px 24px;
    display: flex; gap: 10px; justify-content: flex-end; align-items: center;
  }}
  .export-bar .info {{ flex: 1; font-size: 12px; color: #6b7280; }}
</style>
</head>
<body>

<header>
  <h1>FIAT · Benchmark de clasificador</h1>
  <div class="prog"><div id="progBar" style="width:0%"></div></div>
  <span class="count"><span id="progText">0</span>/100</span>
</header>

<div class="filters">
  <label>Filtrar:</label>
  <select id="filtroTipo">
    <option value="todos">Todos</option>
    <option value="pendiente">Pendientes</option>
    <option value="etiquetado">Etiquetados</option>
    <option value="confiado">Confiados</option>
    <option value="ambiguo">Ambiguos</option>
    <option value="wildcard">Wild cards</option>
  </select>
  <label>Categoría clasificador:</label>
  <select id="filtroCat">
    <option value="">Todas</option>
  </select>
  <span style="margin-left:auto; color:#9ca3af; font-size:11px;">
    💾 Auto-guarda en este navegador. Exporta al terminar.
  </span>
</div>

<div class="tbl" id="tabla"></div>

<div class="export-bar">
  <div class="info" id="infoBar">Se guardará en tu navegador automáticamente. Usa "Exportar" cuando termines.</div>
  <button onclick="resetAll()">Borrar todo</button>
  <button class="primary" onclick="exportar()">Exportar JSON</button>
</div>

<script>
const TITULOS = {datos_json};
const CATS = {cats_json};
const LABEL = {cat_label_json};
const STORAGE_KEY = 'fiat_benchmark_100_v1';

let etiquetas = {{}};
try {{ etiquetas = JSON.parse(localStorage.getItem(STORAGE_KEY) || '{{}}'); }} catch (e) {{}}

function guardar() {{
  localStorage.setItem(STORAGE_KEY, JSON.stringify(etiquetas));
  renderProg();
}}

function renderProg() {{
  const n = Object.keys(etiquetas).filter(k => etiquetas[k]).length;
  document.getElementById('progText').textContent = n;
  document.getElementById('progBar').style.width = (n / TITULOS.length * 100) + '%';
}}

function renderFiltros() {{
  const sel = document.getElementById('filtroCat');
  CATS.forEach(c => {{
    const opt = document.createElement('option');
    opt.value = c;
    opt.textContent = LABEL[c] || c;
    sel.appendChild(opt);
  }});
}}

function render() {{
  const tabla = document.getElementById('tabla');
  const filtroTipo = document.getElementById('filtroTipo').value;
  const filtroCat = document.getElementById('filtroCat').value;
  tabla.innerHTML = '';
  TITULOS.forEach((t, i) => {{
    const marcado = etiquetas[t.id];
    if (filtroTipo === 'pendiente' && marcado) return;
    if (filtroTipo === 'etiquetado' && !marcado) return;
    if (filtroTipo === 'confiado' && t.tipo !== 'confiado') return;
    if (filtroTipo === 'ambiguo' && t.tipo !== 'ambiguo') return;
    if (filtroTipo === 'wildcard' && t.tipo !== 'wildcard') return;
    if (filtroCat && t.cat_clasificador !== filtroCat) return;

    const row = document.createElement('div');
    row.className = 'row' + (marcado ? ' etiquetado' : '');
    if (marcado === t.cat_clasificador) row.classList.add('acierto');
    if (marcado && marcado !== t.cat_clasificador && marcado !== 'ambigua') row.classList.add('error');

    row.innerHTML = `
      <div class="idx">#${{i+1}}</div>
      <div class="cuerpo">
        <div class="titulo">${{escapeHTML(t.titulo)}}</div>
        <div class="meta">
          <span class="pill ${{t.tipo}}">${{t.tipo}}</span>
          <span class="pill score">score ${{t.score.toFixed(2)}}</span>
          <span>${{t.fuente}}</span>
          <span>${{t.fecha}}</span>
          <span class="mono" style="color:#9ca3af">id ${{t.id}}</span>
        </div>
        ${{t.resumen ? `<div class="resumen">${{escapeHTML(t.resumen.slice(0,300))}}${{t.resumen.length>300?'…':''}}</div>` : ''}}
      </div>
      <div>
        <div class="pred">FIAT dice: <strong>${{LABEL[t.cat_clasificador] || t.cat_clasificador}}</strong></div>
        <select class="categoria" data-id="${{t.id}}">
          <option value="">— selecciona categoría correcta —</option>
          ${{CATS.map(c => `<option value="${{c}}">${{LABEL[c] || c}}</option>`).join('')}}
          <option value="ninguna">— ninguna (no aplica)</option>
          <option value="ambigua">— ambigua (varias)</option>
        </select>
        <div class="hint">FIAT acertó si coincide con la selección.</div>
      </div>
    `;
    const sel = row.querySelector('select.categoria');
    if (marcado) sel.value = marcado;
    sel.addEventListener('change', (e) => {{
      const id = e.target.getAttribute('data-id');
      if (e.target.value) {{
        etiquetas[id] = e.target.value;
      }} else {{
        delete etiquetas[id];
      }}
      guardar();
      render();
    }});
    tabla.appendChild(row);
  }});
}}

function escapeHTML(s) {{
  return (s||'').replace(/[&<>"']/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c]));
}}

function exportar() {{
  const n = Object.keys(etiquetas).filter(k => etiquetas[k]).length;
  if (n < 100 && !confirm(`Solo has etiquetado ${{n}}/100. ¿Exportar de todos modos?`)) return;
  const payload = {{
    fecha: new Date().toISOString(),
    n: n,
    etiquetas: etiquetas,
  }};
  const blob = new Blob([JSON.stringify(payload, null, 2)], {{type: 'application/json'}});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = 'benchmark_etiquetas.json';
  a.click();
  URL.revokeObjectURL(url);
}}

function resetAll() {{
  if (!confirm('¿Borrar todas las etiquetas?')) return;
  etiquetas = {{}};
  localStorage.removeItem(STORAGE_KEY);
  render();
}}

document.getElementById('filtroTipo').addEventListener('change', render);
document.getElementById('filtroCat').addEventListener('change', render);

renderFiltros();
render();
renderProg();
</script>

</body>
</html>
"""


def main():
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    articulos = cargar_articulos(conn)
    print(f"Artículos disponibles (sep 2026+): {len(articulos):,}")

    seleccion = estratificar(articulos)
    print(f"Selección final: {len(seleccion)}")

    # Distribución por tipo
    por_tipo = {}
    por_cat = {}
    for a in seleccion:
        por_tipo[a["tipo"]] = por_tipo.get(a["tipo"], 0) + 1
        por_cat[a["cat_clasificador"]] = por_cat.get(a["cat_clasificador"], 0) + 1
    print(f"Por tipo: {por_tipo}")
    print(f"Por categoría: {dict(sorted(por_cat.items(), key=lambda x: -x[1]))}")

    # Escribir data.json para auditoría
    OUT_DATA.write_text(json.dumps(seleccion, ensure_ascii=False, indent=2))
    print(f"\n✓ {OUT_DATA.relative_to(ROOT)}")

    # Escribir HTML
    html = generar_html(seleccion)
    OUT_HTML.write_text(html)
    print(f"✓ {OUT_HTML.relative_to(ROOT)} ({OUT_HTML.stat().st_size:,} bytes)")
    print(f"\nAbre en browser: file://{OUT_HTML.absolute()}")


if __name__ == "__main__":
    main()
