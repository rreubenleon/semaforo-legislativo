"""
Mini-web local (stdlib http.server) para etiquetar el ML Evaluation Set.

Lanza un server en http://127.0.0.1:8765 con una UI sencilla:
- Una tarjeta por documento (título, resumen, fuente, fecha, predicción del modelo)
- Botones para marcar relevancia (Sí / No / No sé)
- Multi-select de categorías cuando es relevante
- Auto-guarda y avanza al siguiente

Uso:
    python eval/etiquetar_web.py [--port 8765]
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import CATEGORIAS
from db import get_connection

logger = logging.getLogger(__name__)

CATEGORIAS_ORDENADAS = [
    (clave, conf["nombre"]) for clave, conf in CATEGORIAS.items()
]

# ──────────────────────────────────────────────────────────────────
# HTML embebido. UI minimalista, oscura, una tarjeta a la vez.
# ──────────────────────────────────────────────────────────────────
HTML = """<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>FIAT — Etiquetar eval set</title>
<style>
  :root {
    --bg: #0f1115; --fg: #e6e8eb; --muted: #8a93a6;
    --card: #181b22; --border: #262b36; --accent: #4fc3f7;
    --green: #52c41a; --red: #ff4d4f; --yellow: #faad14;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--bg); color: var(--fg);
    font: 15px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  }
  .top {
    padding: 12px 24px; border-bottom: 1px solid var(--border);
    display: flex; justify-content: space-between; align-items: center;
    background: var(--card);
  }
  .top h1 { font-size: 16px; margin: 0; font-weight: 600; }
  .progress { color: var(--muted); font-size: 13px; }
  .container { max-width: 820px; margin: 32px auto; padding: 0 24px; }
  .card {
    background: var(--card); border: 1px solid var(--border);
    border-radius: 10px; padding: 24px;
  }
  .meta {
    color: var(--muted); font-size: 12px; text-transform: uppercase;
    letter-spacing: 0.05em; margin-bottom: 12px;
  }
  .meta a { color: var(--accent); text-decoration: none; }
  .titulo { font-size: 20px; font-weight: 600; margin: 0 0 16px; line-height: 1.35; }
  .resumen {
    color: #c5c9d3; white-space: pre-wrap; margin-bottom: 20px;
    max-height: 320px; overflow-y: auto; padding-right: 8px;
  }
  .pred {
    background: #11141a; border: 1px solid var(--border);
    border-radius: 8px; padding: 10px 14px; margin-bottom: 24px;
    font-size: 13px; color: var(--muted);
  }
  .pred strong { color: var(--accent); }
  .pred .vacio { color: var(--yellow); }
  .section-label {
    font-size: 11px; text-transform: uppercase; color: var(--muted);
    margin-bottom: 8px; letter-spacing: 0.05em;
  }
  .relevancia { display: flex; gap: 10px; margin-bottom: 24px; }
  .btn {
    flex: 1; padding: 12px; border-radius: 8px; border: 1px solid var(--border);
    background: #11141a; color: var(--fg); font: inherit; cursor: pointer;
    font-weight: 500; transition: all 0.15s;
  }
  .btn:hover { background: #1d212a; }
  .btn.yes.on { background: var(--green); border-color: var(--green); color: #0a0c10; }
  .btn.no.on { background: var(--red); border-color: var(--red); color: #fff; }
  .btn.skip.on { background: var(--yellow); border-color: var(--yellow); color: #0a0c10; }
  .cats { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 20px; }
  .cats.disabled { opacity: 0.4; pointer-events: none; }
  .cat-pill {
    padding: 8px 12px; border: 1px solid var(--border); border-radius: 6px;
    background: #11141a; cursor: pointer; font-size: 13px;
    transition: all 0.15s;
  }
  .cat-pill:hover { background: #1d212a; }
  .cat-pill.on { background: var(--accent); border-color: var(--accent); color: #0a0c10; font-weight: 600; }
  textarea {
    width: 100%; padding: 10px; background: #11141a; border: 1px solid var(--border);
    border-radius: 6px; color: var(--fg); font: inherit; resize: vertical;
    margin-bottom: 16px;
  }
  .actions { display: flex; gap: 10px; }
  .save {
    flex: 2; padding: 14px; background: var(--accent); color: #0a0c10;
    border: none; border-radius: 8px; font: inherit; font-weight: 600;
    cursor: pointer; font-size: 15px;
  }
  .save:hover { background: #6dcfff; }
  .save:disabled { background: #2a3038; color: var(--muted); cursor: not-allowed; }
  .nav { padding: 14px 18px; background: #1d212a; color: var(--fg);
         border: 1px solid var(--border); border-radius: 8px; cursor: pointer; font: inherit; }
  .nav:hover { background: #262b36; }
  .empty {
    text-align: center; padding: 60px 20px; color: var(--muted);
  }
  .empty h2 { color: var(--fg); }
  kbd {
    background: #262b36; border: 1px solid #3a4050; padding: 2px 6px;
    border-radius: 4px; font-size: 11px;
  }
  .hint { color: var(--muted); font-size: 12px; margin-top: 12px; text-align: center; }
</style>
</head>
<body>
<div class="top">
  <h1>FIAT — Etiquetado manual</h1>
  <div class="progress" id="progress">Cargando…</div>
</div>
<div class="container" id="root"></div>

<script>
const CATEGORIAS = __CATEGORIAS_JSON__;
let estado = { doc: null, gold_relevante: null, gold_categorias: new Set(), notas: "" };

async function cargar(modo) {
  const url = modo === "next-pendiente" ? "/api/next-pendiente" : "/api/next";
  const res = await fetch(url);
  const data = await res.json();
  estado.doc = data.doc;
  estado.gold_relevante = data.doc?.gold_relevante ?? null;
  estado.gold_categorias = new Set(
    (data.doc?.gold_categorias || "").split(",").filter(Boolean)
  );
  estado.notas = data.doc?.notas || "";
  document.getElementById("progress").textContent =
    `${data.etiquetados}/${data.total} etiquetados · ${data.pendientes} pendientes`;
  render();
}

function render() {
  const root = document.getElementById("root");
  if (!estado.doc) {
    root.innerHTML = `<div class="empty">
      <h2>🎉 No quedan documentos pendientes</h2>
      <p>Corre <code>python eval/calcular_metricas.py</code> para ver el resultado.</p>
    </div>`;
    return;
  }
  const d = estado.doc;
  const meta = [
    d.fuente_tabla,
    d.fuente_extra ? `· ${d.fuente_extra}` : "",
    d.fecha ? `· ${d.fecha.substring(0, 10)}` : "",
    d.url ? `· <a href="${d.url}" target="_blank">abrir ↗</a>` : "",
  ].join(" ");

  const predHtml = d.pred_categorias
    ? d.pred_categorias.split(",").map(p => {
        const [cat, score] = p.split(":");
        return `<strong>${cat}</strong> (${score})`;
      }).join(" · ")
    : `<span class="vacio">∅ el clasificador NO devolvió categorías</span>`;

  const cats = CATEGORIAS.map(([clave, nombre]) => {
    const on = estado.gold_categorias.has(clave) ? "on" : "";
    return `<div class="cat-pill ${on}" data-cat="${clave}">${nombre}</div>`;
  }).join("");

  const yesOn = estado.gold_relevante === 1 ? "on" : "";
  const noOn = estado.gold_relevante === 0 ? "on" : "";
  const skipOn = estado.gold_relevante === -1 ? "on" : "";
  const catsDisabled = estado.gold_relevante !== 1 ? "disabled" : "";

  root.innerHTML = `
    <div class="card">
      <div class="meta">${meta}</div>
      <h2 class="titulo">${escapeHtml(d.titulo)}</h2>
      <div class="resumen">${escapeHtml(d.resumen || "(sin resumen)")}</div>
      <div class="pred">Predicción del modelo: ${predHtml}</div>

      <div class="section-label">¿Es legislativamente relevante?</div>
      <div class="relevancia">
        <button class="btn yes ${yesOn}" data-rel="1">Sí <kbd>1</kbd></button>
        <button class="btn no ${noOn}" data-rel="0">No <kbd>2</kbd></button>
        <button class="btn skip ${skipOn}" data-rel="-1">No sé / saltar <kbd>3</kbd></button>
      </div>

      <div class="section-label">Categorías correctas (multi-select)</div>
      <div class="cats ${catsDisabled}">${cats}</div>

      <textarea rows="2" placeholder="Notas (opcional)…">${escapeHtml(estado.notas)}</textarea>

      <div class="actions">
        <button class="nav" id="prev">← Anterior</button>
        <button class="save" id="save">Guardar y siguiente <kbd>↵</kbd></button>
        <button class="nav" id="next">Siguiente →</button>
      </div>
      <div class="hint">Atajos: <kbd>1</kbd>=sí, <kbd>2</kbd>=no, <kbd>3</kbd>=skip, <kbd>↵</kbd>=guardar</div>
    </div>
  `;

  root.querySelectorAll(".btn[data-rel]").forEach(b => {
    b.onclick = () => { estado.gold_relevante = parseInt(b.dataset.rel); render(); };
  });
  root.querySelectorAll(".cat-pill").forEach(p => {
    p.onclick = () => {
      const c = p.dataset.cat;
      if (estado.gold_categorias.has(c)) estado.gold_categorias.delete(c);
      else estado.gold_categorias.add(c);
      render();
    };
  });
  root.querySelector("textarea").oninput = (e) => { estado.notas = e.target.value; };
  root.querySelector("#save").onclick = guardar;
  root.querySelector("#prev").onclick = () => navegar(-1);
  root.querySelector("#next").onclick = () => navegar(1);
}

async function guardar() {
  if (!estado.doc) return;
  if (estado.gold_relevante === null) {
    alert("Marca relevancia primero");
    return;
  }
  const body = {
    id: estado.doc.id,
    gold_relevante: estado.gold_relevante,
    gold_categorias: Array.from(estado.gold_categorias).join(","),
    notas: estado.notas,
  };
  await fetch("/api/guardar", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify(body),
  });
  await cargar("next-pendiente");
}

async function navegar(delta) {
  if (!estado.doc) return;
  const res = await fetch(`/api/navegar?id=${estado.doc.id}&delta=${delta}`);
  const data = await res.json();
  estado.doc = data.doc;
  estado.gold_relevante = data.doc?.gold_relevante ?? null;
  estado.gold_categorias = new Set(
    (data.doc?.gold_categorias || "").split(",").filter(Boolean)
  );
  estado.notas = data.doc?.notas || "";
  document.getElementById("progress").textContent =
    `${data.etiquetados}/${data.total} etiquetados · ${data.pendientes} pendientes`;
  render();
}

function escapeHtml(s) {
  return (s || "").replace(/[&<>"']/g, c => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[c]));
}

document.addEventListener("keydown", (e) => {
  if (e.target.tagName === "TEXTAREA") return;
  if (e.key === "1") { estado.gold_relevante = 1; render(); }
  else if (e.key === "2") { estado.gold_relevante = 0; render(); }
  else if (e.key === "3") { estado.gold_relevante = -1; render(); }
  else if (e.key === "Enter") guardar();
});

cargar("next-pendiente");
</script>
</body>
</html>
"""


def serializar_doc(row):
    if row is None:
        return None
    return {
        "id": row[0],
        "fuente_tabla": row[1],
        "fuente_id": row[2],
        "titulo": row[3],
        "resumen": row[4],
        "fecha": row[5],
        "fuente_extra": row[6],
        "url": row[7],
        "pred_categorias": row[8],
        "gold_relevante": row[9],
        "gold_categorias": row[10],
        "notas": row[11],
    }


COLS = (
    "id, fuente_tabla, fuente_id, titulo, resumen, fecha, fuente_extra, "
    "url, pred_categorias, gold_relevante, gold_categorias, notas"
)


def stats(conn):
    total = conn.execute("SELECT COUNT(*) FROM eval_set").fetchone()[0]
    etiquetados = conn.execute(
        "SELECT COUNT(*) FROM eval_set WHERE gold_relevante IS NOT NULL"
    ).fetchone()[0]
    return {"total": total, "etiquetados": etiquetados, "pendientes": total - etiquetados}


def get_next_pendiente(conn):
    row = conn.execute(
        f"SELECT {COLS} FROM eval_set WHERE gold_relevante IS NULL ORDER BY id LIMIT 1"
    ).fetchone()
    return serializar_doc(row)


def get_next(conn):
    row = conn.execute(f"SELECT {COLS} FROM eval_set ORDER BY id LIMIT 1").fetchone()
    return serializar_doc(row)


def get_navegar(conn, doc_id, delta):
    if delta < 0:
        row = conn.execute(
            f"SELECT {COLS} FROM eval_set WHERE id < ? ORDER BY id DESC LIMIT 1",
            (doc_id,),
        ).fetchone()
    else:
        row = conn.execute(
            f"SELECT {COLS} FROM eval_set WHERE id > ? ORDER BY id LIMIT 1",
            (doc_id,),
        ).fetchone()
    if row is None:
        # Si no hay siguiente/anterior, devolver el actual
        row = conn.execute(f"SELECT {COLS} FROM eval_set WHERE id = ?", (doc_id,)).fetchone()
    return serializar_doc(row)


def guardar(conn, doc_id, gold_relevante, gold_categorias, notas):
    # gold_relevante: 1, 0, -1 (saltado). Para el cálculo de métricas, -1 se trata como NULL.
    rel_db = gold_relevante if gold_relevante in (0, 1) else None
    conn.execute(
        """
        UPDATE eval_set
           SET gold_relevante = ?,
               gold_categorias = ?,
               notas = ?,
               etiquetado_en = ?
         WHERE id = ?
        """,
        (rel_db, gold_categorias, notas, datetime.now().isoformat(timespec="seconds"), doc_id),
    )
    conn.commit()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Silenciar logs default; mantener solo errores
        if "error" in format.lower():
            super().log_message(format, *args)

    def _json(self, payload, status=200):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        conn = get_connection()

        if parsed.path == "/" or parsed.path == "/index.html":
            html = HTML.replace(
                "__CATEGORIAS_JSON__",
                json.dumps(CATEGORIAS_ORDENADAS, ensure_ascii=False),
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html)))
            self.end_headers()
            self.wfile.write(html)
            return

        if parsed.path == "/api/next-pendiente":
            doc = get_next_pendiente(conn)
            self._json({"doc": doc, **stats(conn)})
            return

        if parsed.path == "/api/next":
            doc = get_next(conn)
            self._json({"doc": doc, **stats(conn)})
            return

        if parsed.path == "/api/navegar":
            qs = parse_qs(parsed.query)
            doc_id = int(qs.get("id", ["0"])[0])
            delta = int(qs.get("delta", ["1"])[0])
            doc = get_navegar(conn, doc_id, delta)
            self._json({"doc": doc, **stats(conn)})
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/api/guardar":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8")
        body = json.loads(raw)
        conn = get_connection()
        guardar(
            conn,
            int(body["id"]),
            int(body["gold_relevante"]),
            body.get("gold_categorias", "") or "",
            body.get("notas", "") or "",
        )
        self._json({"ok": True, **stats(conn)})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # Verificar que la tabla y datos existen
    conn = get_connection()
    try:
        s = stats(conn)
    except Exception:
        logger.error("No existe la tabla eval_set. Corre primero: python eval/sample_eval_set.py")
        return
    if s["total"] == 0:
        logger.error("eval_set está vacía. Corre primero: python eval/sample_eval_set.py")
        return

    server = HTTPServer((args.host, args.port), Handler)
    logger.info(f"\n📝 Etiquetado FIAT corriendo en http://{args.host}:{args.port}")
    logger.info(f"   {s['etiquetados']}/{s['total']} ya etiquetados, {s['pendientes']} pendientes")
    logger.info(f"   Ctrl+C para detener.\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("\n👋 Server detenido.")


if __name__ == "__main__":
    main()
