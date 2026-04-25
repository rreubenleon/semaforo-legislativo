"""
Genera HTML interactivo para validar la clasificación de instrumentos
SIL (legislativo_sustantivo, administrativo_licencia, informe, etc.)
sobre 4 legisladores con perfiles distintos.

Uso:
    python eval/instrumentos_4legs/generar.py
    open eval/instrumentos_4legs/index.html
"""
import json
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
DB = ROOT / "semaforo.db"
OUT_DIR = Path(__file__).resolve().parent
OUT_HTML = OUT_DIR / "index.html"
OUT_DATA = OUT_DIR / "data.json"

LEGISLADORES = [
    ("Dip. Ivonne Aracely Ortega Pacheco (MC)", "Ivonne Ortega Pacheco", "MC · Diputados · Yucatán"),
    ("Sen. Olga Patricia Sosa Ruíz (Morena)", "Olga Patricia Sosa Ruíz", "Morena · Senado · Tamaulipas"),
    ("Dip. Lilia Aguilar Gil (PT)", "Lilia Aguilar Gil", "PT · Diputados · Chihuahua"),
    ("Sen. Pablo Guillermo Angulo Briceño (PRI)", "Pablo Angulo Briceño", "PRI · Senado · Sinaloa"),
]

CLASES = [
    ("Iniciativa", "Iniciativa", "Que reforma/adiciona/expide/abroga/declara/crea... Cualquier iniciativa de ley o decreto, incluyendo Día Nacional, Letras de Oro, etc."),
    ("Proposición con punto de acuerdo", "Proposición con PA", "Por el que exhorta/solicita/condena/reconoce. Incluye exhortos al Ejecutivo y entidades."),
    ("Dictamen", "Dictamen", "Dictamen a discusión, primera lectura, declaratoria de publicidad, dictamen negativo"),
    ("Minuta", "Minuta", "Minuta del Senado a Diputados o viceversa"),
    ("Acuerdo parlamentario", "Acuerdo parlamentario", "Acuerdo aprobado, integración de comisiones, ratificación de mesa"),
    ("Comunicado", "Comunicado", "Informes (de actividad, viaje, austeridad), comunicaciones, protestas, respuestas a PA, solicitudes de licencia"),
    ("Efeméride", "Efeméride", "Conmemoraciones, posicionamientos personales, minutos de aplausos, intervenciones de tribuna sin proposición"),
    ("Otro", "Otro / no clasificable", "Casos raros que no caen claramente en las categorías SIL"),
]


def main():
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    todos = []
    for presentador_db, nombre_corto, info in LEGISLADORES:
        rows = conn.execute("""
            SELECT id, titulo, tipo, tipo_inferido, comision, estatus,
                   fecha_presentacion, clasificacion,
                   seguimiento_id, asunto_id
            FROM sil_documentos
            WHERE fecha_presentacion >= '2024-09-01'
              AND presentador = ?
            ORDER BY fecha_presentacion DESC
        """, (presentador_db,)).fetchall()
        for r in rows:
            todos.append({
                "id": r["id"],
                "legislador": nombre_corto,
                "info_leg": info,
                "titulo": r["titulo"],
                "tipo_scraper": r["tipo"] or "",
                "tipo_sil": r["tipo_inferido"] or r["tipo"] or "",
                "funcion_fiat": r["clasificacion"] or "",
                "comision": r["comision"] or "",
                "estatus": (r["estatus"] or "").split("/")[0][:60],
                "fecha": r["fecha_presentacion"],
                "clasificacion_fiat": r["tipo_inferido"] or "",  # mantener compat con HTML
                "seguimiento_id": r["seguimiento_id"],
                "asunto_id": r["asunto_id"],
            })

    OUT_DATA.write_text(json.dumps(todos, ensure_ascii=False, indent=2))
    print(f"Instrumentos: {len(todos)}")
    for nombre, nc, _ in LEGISLADORES:
        n = sum(1 for x in todos if x["legislador"] == nc)
        print(f"  {nc}: {n}")

    # Stats actuales por clase
    por_clase = {}
    for x in todos:
        por_clase[x["clasificacion_fiat"]] = por_clase.get(x["clasificacion_fiat"], 0) + 1
    print("\nClasificación actual FIAT:")
    for c, n in sorted(por_clase.items(), key=lambda x: -x[1]):
        print(f"  {c or '(sin clasificar)'}: {n}")

    html = render_html(todos)
    OUT_HTML.write_text(html)
    print(f"\n✓ {OUT_HTML.relative_to(ROOT)} ({OUT_HTML.stat().st_size:,} bytes)")
    print(f"  Abre: open '{OUT_HTML}'")


def render_html(todos):
    datos_json = json.dumps(todos, ensure_ascii=False)
    clases_json = json.dumps(CLASES, ensure_ascii=False)

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>FIAT · Validación clasificación de instrumentos</title>
<style>
  :root {{ --acento: #2e7d32; --borde: #e5e7eb; --fondo: #fafafa; }}
  * {{ box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    font-size: 13px; line-height: 1.5; color: #111827; background: var(--fondo);
    margin: 0; padding: 0;
  }}
  .mono {{ font-family: "JetBrains Mono", ui-monospace, monospace; }}
  header {{
    position: sticky; top: 0; z-index: 10;
    background: white; border-bottom: 1px solid var(--borde);
    padding: 12px 24px; display: flex; align-items: center; gap: 20px; flex-wrap: wrap;
  }}
  header h1 {{ margin: 0; font-size: 15px; font-weight: 600; }}
  .prog {{
    flex: 1; max-width: 350px; height: 6px; background: #e5e7eb; border-radius: 3px; overflow: hidden;
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
    display: flex; gap: 10px; align-items: center; font-size: 12px; flex-wrap: wrap;
  }}
  .filters select {{ font-size: 12px; padding: 4px 8px; }}
  .leg-tabs {{
    background: white; padding: 8px 24px; border-bottom: 1px solid var(--borde);
    display: flex; gap: 6px; flex-wrap: wrap;
  }}
  .leg-tab {{
    padding: 6px 12px; border: 1px solid var(--borde); border-radius: 16px;
    cursor: pointer; font-size: 12px; background: white;
  }}
  .leg-tab.active {{ background: var(--acento); color: white; border-color: var(--acento); }}
  .leg-tab .badge {{ font-size: 10px; opacity: 0.7; margin-left: 4px; }}
  .tbl {{ padding: 16px 24px 80px; }}
  .row {{
    background: white; border: 1px solid var(--borde); border-radius: 8px;
    margin-bottom: 6px; padding: 10px 14px;
    display: grid; grid-template-columns: 30px 1fr 240px; gap: 12px; align-items: start;
  }}
  .row.etiquetado {{ border-left: 3px solid var(--acento); }}
  .row.acierto {{ background: #f0fdf4; }}
  .row.error {{ background: #fef2f2; }}
  .idx {{ font-weight: 600; color: #9ca3af; font-size: 11px; padding-top: 2px; }}
  .cuerpo .titulo {{ font-weight: 500; color: #111827; line-height: 1.35; font-size: 13px; }}
  .cuerpo .meta {{ font-size: 10px; color: #6b7280; display: flex; gap: 8px; flex-wrap: wrap; margin-top: 4px; }}
  .pill {{ padding: 1px 6px; border-radius: 3px; font-size: 9px; font-weight: 600; }}
  .pill.tipo {{ background: #e0e7ff; color: #3730a3; }}
  .pill.estatus {{ background: #f3f4f6; color: #374151; }}
  .pill.estatus.aprobado {{ background: #d1fae5; color: #065f46; }}
  .pill.estatus.desechado {{ background: #fee2e2; color: #991b1b; }}
  .pill.estatus.pendiente {{ background: #fef3c7; color: #92400e; }}
  .pill.comision {{ background: #fef3c7; color: #92400e; max-width: 320px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  .pred {{
    padding: 4px 8px; border-radius: 4px; background: #f9fafb;
    font-size: 10px; color: #6b7280; margin-bottom: 5px;
  }}
  .pred strong {{ color: #111827; font-weight: 600; }}
  select.clasificacion {{
    width: 100%; padding: 5px 7px; font-size: 12px;
    border: 1px solid var(--borde); border-radius: 6px; background: white; cursor: pointer;
  }}
  select.clasificacion:focus {{ outline: 2px solid var(--acento); outline-offset: -1px; }}
  .legend {{
    background: #fffbeb; border: 1px solid #fcd34d; border-radius: 8px;
    padding: 10px 14px; margin-bottom: 12px; font-size: 11px;
  }}
  .legend dt {{ font-weight: 600; margin-top: 4px; color: #78350f; }}
  .legend dd {{ margin: 0 0 0 0; color: #78350f; opacity: 0.85; }}
  .export-bar {{
    position: fixed; bottom: 0; left: 0; right: 0; background: white;
    border-top: 1px solid var(--borde); padding: 12px 24px;
    display: flex; gap: 10px; justify-content: flex-end; align-items: center;
  }}
  .export-bar .info {{ flex: 1; font-size: 12px; color: #6b7280; }}
  .titulo-link {{ color: inherit; text-decoration: none; }}
  .titulo-link:hover {{ text-decoration: underline; color: var(--acento); }}
</style>
</head>
<body>

<header>
  <h1>FIAT · Validación clasificación de instrumentos</h1>
  <div class="prog"><div id="progBar" style="width:0%"></div></div>
  <span class="count"><span id="progText">0</span>/<span id="progTotal">0</span></span>
</header>

<div class="leg-tabs" id="legTabs"></div>

<div class="filters">
  <label>Filtro:</label>
  <select id="filtroEstado">
    <option value="todos">Todos</option>
    <option value="pendiente">Pendientes de validar</option>
    <option value="etiquetado">Validados</option>
    <option value="acierto">FIAT acertó</option>
    <option value="error">FIAT erró</option>
  </select>
  <label>Clasificación FIAT:</label>
  <select id="filtroClass">
    <option value="">Cualquiera</option>
  </select>
  <span style="margin-left:auto; color:#9ca3af; font-size:11px;">
    💾 Auto-guarda en este navegador
  </span>
</div>

<div class="tbl" id="tabla"></div>

<div class="export-bar">
  <div class="info" id="infoBar">Selecciona la clasificación correcta. Verde = FIAT acertó. Rojo = FIAT erró.</div>
  <button onclick="resetAll()">Borrar todo</button>
  <button class="primary" onclick="exportar()">Exportar JSON</button>
</div>

<script>
const TODOS = {datos_json};
const CLASES = {clases_json};
const STORAGE_KEY = 'fiat_instrumentos_4legs_v2_silcats';

let etiquetas = {{}};
try {{ etiquetas = JSON.parse(localStorage.getItem(STORAGE_KEY) || '{{}}'); }} catch (e) {{}}

let legActivo = TODOS[0]?.legislador || null;

function guardar() {{
  localStorage.setItem(STORAGE_KEY, JSON.stringify(etiquetas));
  renderProg();
}}

function getRowsActivos() {{
  return TODOS.filter(t => t.legislador === legActivo);
}}

function renderProg() {{
  const activos = getRowsActivos();
  const n = activos.filter(t => etiquetas[t.id]).length;
  document.getElementById('progText').textContent = n;
  document.getElementById('progTotal').textContent = activos.length;
  document.getElementById('progBar').style.width = (n / activos.length * 100) + '%';
}}

function renderLegTabs() {{
  const tabs = document.getElementById('legTabs');
  const counts = {{}};
  TODOS.forEach(t => {{ counts[t.legislador] = (counts[t.legislador] || 0) + 1; }});
  const validados = {{}};
  TODOS.forEach(t => {{
    if (etiquetas[t.id]) validados[t.legislador] = (validados[t.legislador] || 0) + 1;
  }});
  const legs = [...new Set(TODOS.map(t => t.legislador))];
  tabs.innerHTML = legs.map(l => {{
    const t = TODOS.find(x => x.legislador === l);
    const v = validados[l] || 0;
    const total = counts[l];
    return `<div class="leg-tab ${{l === legActivo ? 'active' : ''}}" onclick="setLeg('${{l}}')">
      ${{l}}<span class="badge">${{v}}/${{total}}</span>
    </div>`;
  }}).join('');
}}

function setLeg(l) {{
  legActivo = l;
  renderLegTabs();
  render();
  renderProg();
  window.scrollTo(0, 0);
}}

function renderFiltros() {{
  const sel = document.getElementById('filtroClass');
  CLASES.forEach(([slug, nombre]) => {{
    const opt = document.createElement('option');
    opt.value = slug;
    opt.textContent = nombre;
    sel.appendChild(opt);
  }});
}}

function pillEstatus(estatus) {{
  const e = (estatus || '').toLowerCase();
  const cls = e.includes('aprobado') ? 'aprobado'
            : e.includes('desechado') ? 'desechado'
            : e.includes('pendiente') ? 'pendiente'
            : '';
  return `<span class="pill estatus ${{cls}}">${{escapeHTML(estatus)}}</span>`;
}}

function urlSil(seguimiento, asunto) {{
  if (!seguimiento || !asunto) return null;
  return `http://sil.gobernacion.gob.mx/Librerias/pp_ReporteSeguimiento.php?SID=&Seguimiento=${{seguimiento}}&Asunto=${{asunto}}`;
}}

function render() {{
  const tabla = document.getElementById('tabla');
  const fEstado = document.getElementById('filtroEstado').value;
  const fClass = document.getElementById('filtroClass').value;
  tabla.innerHTML = '';

  // Legend
  const legend = document.createElement('div');
  legend.className = 'legend';
  legend.innerHTML = '<strong>Categorías:</strong> ' + CLASES.map(([slug, nombre, desc]) =>
    `<dt>${{nombre}}</dt><dd>${{desc}}</dd>`
  ).join('');
  tabla.appendChild(legend);

  const rows = getRowsActivos();
  rows.forEach((t, i) => {{
    const marcado = etiquetas[t.id];
    const acierto = marcado && marcado === t.clasificacion_fiat;
    const error = marcado && marcado !== t.clasificacion_fiat;

    if (fEstado === 'pendiente' && marcado) return;
    if (fEstado === 'etiquetado' && !marcado) return;
    if (fEstado === 'acierto' && !acierto) return;
    if (fEstado === 'error' && !error) return;
    if (fClass && t.clasificacion_fiat !== fClass) return;

    const row = document.createElement('div');
    row.className = 'row' + (marcado ? ' etiquetado' : '');
    if (acierto) row.classList.add('acierto');
    if (error) row.classList.add('error');

    const url = urlSil(t.seguimiento_id, t.asunto_id);
    const tituloHtml = url
      ? `<a href="${{url}}" target="_blank" class="titulo-link">${{escapeHTML(t.titulo)}}</a>`
      : escapeHTML(t.titulo);

    const tipoCambio = t.tipo_scraper && t.tipo_scraper !== t.tipo_sil
      ? `<span style="color:#9ca3af; font-size:9px"> (scraper dijo: ${{escapeHTML(t.tipo_scraper)}})</span>` : '';

    const funcColors = {{ legislativa: '#065f46', administrativa: '#92400e', ceremonial: '#5b21b6' }};
    const funcBg = {{ legislativa: '#d1fae5', administrativa: '#fef3c7', ceremonial: '#ede9fe' }};
    const funcPill = t.funcion_fiat
      ? `<span style="background:${{funcBg[t.funcion_fiat] || '#f3f4f6'}}; color:${{funcColors[t.funcion_fiat] || '#374151'}}; padding:1px 6px; border-radius:3px; font-size:9px; font-weight:600;">FN: ${{t.funcion_fiat}}</span>`
      : '';

    row.innerHTML = `
      <div class="idx">#${{i+1}}</div>
      <div class="cuerpo">
        <div class="titulo">${{tituloHtml}}</div>
        <div class="meta">
          <span class="pill tipo">${{escapeHTML(t.tipo_sil || t.tipo_scraper || '?')}}</span>
          ${{funcPill}}
          ${{pillEstatus(t.estatus)}}
          ${{t.comision ? `<span class="pill comision" title="${{escapeHTML(t.comision)}}">${{escapeHTML(t.comision.slice(0, 50))}}${{t.comision.length>50?'…':''}}</span>` : ''}}
          <span style="color:#9ca3af">${{t.fecha}}</span>
          ${{tipoCambio}}
        </div>
      </div>
      <div>
        <div class="pred">FIAT lo tipificó como: <strong>${{escapeHTML((CLASES.find(c => c[0] === t.clasificacion_fiat) || [t.clasificacion_fiat, t.clasificacion_fiat])[1])}}</strong></div>
        <select class="clasificacion" data-id="${{t.id}}">
          <option value="">— ¿qué tipo SIL es realmente? —</option>
          ${{CLASES.map(([slug, nombre]) => `<option value="${{slug}}">${{nombre}}</option>`).join('')}}
        </select>
      </div>
    `;
    const sel = row.querySelector('select.clasificacion');
    if (marcado) sel.value = marcado;
    sel.addEventListener('change', (e) => {{
      const id = e.target.getAttribute('data-id');
      if (e.target.value) {{
        etiquetas[id] = e.target.value;
      }} else {{
        delete etiquetas[id];
      }}
      guardar();
      renderLegTabs();
      render();
    }});
    tabla.appendChild(row);
  }});
}}

function escapeHTML(s) {{
  return (s||'').replace(/[&<>"']/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c]));
}}

function exportar() {{
  const total = TODOS.length;
  const n = TODOS.filter(t => etiquetas[t.id]).length;
  if (n < total && !confirm(`Etiquetados ${{n}}/${{total}}. ¿Exportar de todos modos?`)) return;
  const payload = {{
    fecha: new Date().toISOString(),
    n: n,
    total: total,
    etiquetas: etiquetas,
  }};
  const blob = new Blob([JSON.stringify(payload, null, 2)], {{type: 'application/json'}});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = 'instrumentos_etiquetas.json';
  a.click();
  URL.revokeObjectURL(url);
}}

function resetAll() {{
  if (!confirm('¿Borrar todas las etiquetas?')) return;
  etiquetas = {{}};
  localStorage.removeItem(STORAGE_KEY);
  renderLegTabs();
  render();
  renderProg();
}}

document.getElementById('filtroEstado').addEventListener('change', render);
document.getElementById('filtroClass').addEventListener('change', render);

renderFiltros();
renderLegTabs();
render();
renderProg();
</script>

</body>
</html>
"""


if __name__ == "__main__":
    main()
