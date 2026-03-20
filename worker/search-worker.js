/**
 * FIAT - API de Búsqueda Full-Text
 * Cloudflare Worker que consulta FTS5 en D1
 *
 * Endpoint: GET /buscar?q=reforma+fiscal&tipo=gaceta&desde=2025-01-01&hasta=2026-03-12&pagina=1&limite=20
 */

const ALLOWED_ORIGINS = [
  'https://fiatmx.com',
  'https://www.fiatmx.com',
  'https://rreubenleon.github.io',
  'http://localhost:8000',
  'http://localhost:8080',
  'http://127.0.0.1:8000',
  'http://127.0.0.1:8080',
  'http://localhost:3000',
];

export default {
  async fetch(request, env) {
    // CORS preflight
    if (request.method === 'OPTIONS') {
      return corsResponse(null, 204, request);
    }

    const url = new URL(request.url);

    if (url.pathname === '/') {
      return corsResponse({ status: 'ok', endpoints: ['/buscar', '/registro'] }, 200, request);
    }

    // ── Registro de interesados ──
    if (url.pathname === '/registro') {
      return handleRegistro(request, env);
    }

    if (url.pathname !== '/buscar') {
      return corsResponse({ error: 'Not found' }, 404, request);
    }

    if (request.method !== 'GET') {
      return corsResponse({ error: 'Solo GET' }, 405, request);
    }

    // Parámetros
    const q = (url.searchParams.get('q') || '').trim();
    const tipo = url.searchParams.get('tipo') || '';
    const desde = url.searchParams.get('desde') || '';
    const hasta = url.searchParams.get('hasta') || '';
    const pagina = Math.max(1, parseInt(url.searchParams.get('pagina') || '1') || 1);
    const limite = Math.min(50, Math.max(1, parseInt(url.searchParams.get('limite') || '20') || 20));

    // Validación
    if (!q || q.length < 2) {
      return corsResponse({ error: 'Consulta muy corta (mínimo 2 caracteres)', resultados: [] }, 400, request);
    }
    if (q.length > 200) {
      return corsResponse({ error: 'Consulta muy larga (máximo 200 caracteres)', resultados: [] }, 400, request);
    }

    try {
      const db = env.DB;
      const ftsQuery = sanitizeFTS(q);
      const offset = (pagina - 1) * limite;

      // Construir filtros
      const filters = [];
      const params = [ftsQuery];

      if (tipo && ['articulo', 'gaceta', 'sil'].includes(tipo)) {
        filters.push('fuente_tipo = ?');
        params.push(tipo);
      }
      if (desde) {
        filters.push('fecha >= ?');
        params.push(desde);
      }
      if (hasta) {
        filters.push('fecha <= ?');
        params.push(hasta);
      }

      const where = filters.length > 0 ? 'AND ' + filters.join(' AND ') : '';

      // Conteo total
      const countResult = await db
        .prepare(`SELECT count(*) as total FROM busqueda_fts WHERE busqueda_fts MATCH ? ${where}`)
        .bind(...params)
        .first();
      const total = Number(countResult.total);

      // Búsqueda con ranking
      const searchResult = await db
        .prepare(`
          SELECT titulo, contenido, fuente_tipo, fuente_nombre,
                 categoria, fecha, url, extra_json, doc_id, rank
          FROM busqueda_fts
          WHERE busqueda_fts MATCH ?
          ${where}
          ORDER BY rank
          LIMIT ? OFFSET ?
        `)
        .bind(...params, limite, offset)
        .all();

      // Formatear resultados
      const resultados = searchResult.results.map(row => {
        let extra = {};
        try {
          if (row.extra_json) extra = JSON.parse(row.extra_json);
        } catch {}

        return {
          titulo: row.titulo || '',
          extracto: generarExtracto(row.contenido || '', q),
          fuente_tipo: row.fuente_tipo || '',
          fuente_nombre: row.fuente_nombre || '',
          categoria: row.categoria || '',
          fecha: row.fecha || '',
          url: row.url || '',
          extra,
          score_momento: null,
          color_momento: null,
        };
      });

      // ── Enriquecer con score del momento ──
      const catSet = new Set();
      let minFecha = '9999', maxFecha = '0000';
      for (const r of resultados) {
        const cat = extraerCategoriaFIAT(r.categoria, r.fuente_tipo);
        if (cat) catSet.add(cat);
        if (r.fecha && r.fecha < minFecha) minFecha = r.fecha;
        if (r.fecha && r.fecha > maxFecha) maxFecha = r.fecha;
      }

      if (catSet.size > 0 && minFecha < '9999') {
        try {
          const cats = [...catSet];
          const ph = cats.map(() => '?').join(',');
          const scoresResult = await db
            .prepare(`SELECT categoria, fecha, score_total, color FROM scores
                      WHERE categoria IN (${ph}) AND fecha >= ? AND fecha <= ?
                      ORDER BY categoria, fecha DESC`)
            .bind(...cats, minFecha, maxFecha)
            .all();

          // Agrupar: categoria → [{fecha, score, color}] (desc por fecha)
          const porCat = {};
          for (const s of scoresResult.results) {
            if (!porCat[s.categoria]) porCat[s.categoria] = [];
            porCat[s.categoria].push({ fecha: s.fecha, score: s.score_total, color: s.color });
          }

          // Asignar el score más cercano a cada resultado
          for (const r of resultados) {
            const cat = extraerCategoriaFIAT(r.categoria, r.fuente_tipo);
            if (cat && porCat[cat]) {
              const match = porCat[cat].find(s => s.fecha <= r.fecha);
              if (match) {
                r.score_momento = Math.round(match.score * 10) / 10;
                r.color_momento = match.color;
              }
            }
          }
        } catch (e) {
          // No es crítico — los resultados siguen sin score
        }
      }

      return corsResponse({
        consulta: q,
        total,
        pagina,
        total_paginas: Math.ceil(total / limite),
        resultados,
      }, 200, request);

    } catch (err) {
      return corsResponse(
        { error: 'Error de búsqueda', detalle: err.message, resultados: [] },
        500, request
      );
    }
  },
};

/**
 * Extrae la categoría FIAT principal del campo categoria.
 * - articulos: "seguridad_justicia:0.85,economia:0.42" → "seguridad_justicia"
 * - sil: "seguridad_justicia" → "seguridad_justicia"
 * - gaceta: "iniciativa" → null (es tipo de documento, no categoría FIAT)
 */
function extraerCategoriaFIAT(categoriaStr, fuenteTipo) {
  if (!categoriaStr) return null;
  if (fuenteTipo === 'gaceta') return null;
  if (fuenteTipo === 'sil') return categoriaStr;
  const first = categoriaStr.split(',')[0];
  if (first.includes(':')) return first.split(':')[0].trim();
  return first.trim() || null;
}

/**
 * Maneja POST /registro — guarda email en D1 y notifica via Resend
 */
async function handleRegistro(request, env) {
  if (request.method !== 'POST') {
    return corsResponse({ error: 'Solo POST' }, 405, request);
  }

  let body;
  try {
    body = await request.json();
  } catch {
    return corsResponse({ error: 'JSON inválido' }, 400, request);
  }

  const email = (body.email || '').trim().toLowerCase();
  if (!email || !email.includes('@') || email.length > 200) {
    return corsResponse({ error: 'Correo inválido' }, 400, request);
  }

  const db = env.DB;

  try {
    // Crear tabla si no existe
    await db.prepare(`
      CREATE TABLE IF NOT EXISTS registros (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL UNIQUE,
        fecha TEXT NOT NULL DEFAULT (datetime('now')),
        ip TEXT,
        user_agent TEXT
      )
    `).run();

    // Insertar (ignorar duplicados)
    const ip = request.headers.get('CF-Connecting-IP') || 'desconocido';
    const ua = (request.headers.get('User-Agent') || '').slice(0, 300);
    const result = await db.prepare(
      'INSERT OR IGNORE INTO registros (email, ip, user_agent) VALUES (?, ?, ?)'
    ).bind(email, ip, ua).run();

    const nuevo = result.meta?.changes > 0;

    // Enviar notificación por email via Resend (si hay API key)
    if (nuevo && env.RESEND_API_KEY) {
      try {
        const totalResult = await db.prepare('SELECT count(*) as total FROM registros').first();
        const totalRegistros = totalResult?.total || '?';

        await fetch('https://api.resend.com/emails', {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${env.RESEND_API_KEY}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            from: 'Fiat <notificaciones@fiatmx.com>',
            to: ['contacto@fiatmx.com'],
            subject: `Nuevo registro en Fiat: ${email}`,
            html: `
              <div style="font-family: Inter, system-ui, sans-serif; max-width: 480px; margin: 0 auto; padding: 32px 24px;">
                <h2 style="color: #1a1a1a; font-size: 20px; margin-bottom: 8px;">Nuevo interesado en Fiat</h2>
                <p style="color: #666; font-size: 14px; margin-bottom: 24px;">Alguien dejó su correo para enterarse del lanzamiento.</p>
                <div style="background: #f8faf8; border: 1px solid #e0e7e0; border-radius: 12px; padding: 20px; margin-bottom: 24px;">
                  <p style="color: #888; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; margin: 0 0 6px;">Correo registrado</p>
                  <p style="color: #1a1a1a; font-size: 16px; font-weight: 600; margin: 0;">${email}</p>
                </div>
                <p style="color: #999; font-size: 12px;">Total de registros: <strong>${totalRegistros}</strong></p>
                <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
                <p style="color: #bbb; font-size: 11px;">Fiat — Sistema Predictivo Legislativo</p>
              </div>
            `,
          }),
        });
      } catch (emailErr) {
        // No bloquear el registro si falla el email
        console.error('Error enviando notificación:', emailErr.message);
      }
    }

    return corsResponse({
      ok: true,
      nuevo,
      mensaje: nuevo ? 'Registro exitoso' : 'Ya estabas registrado',
    }, 200, request);

  } catch (err) {
    return corsResponse({ error: 'Error al registrar', detalle: err.message }, 500, request);
  }
}

/** Limpia la query para FTS5 (elimina caracteres especiales) */
function sanitizeFTS(q) {
  return q.replace(/['"()*{}[\]^~<>:]/g, ' ').replace(/\s+/g, ' ').trim();
}

/** Genera un extracto del contenido con contexto alrededor del primer match */
function generarExtracto(contenido, query) {
  if (!contenido) return '';
  const terms = query.toLowerCase().split(/\s+/).filter(t => t.length >= 2);
  const lower = contenido.toLowerCase();

  let bestPos = -1;
  for (const term of terms) {
    const pos = lower.indexOf(term);
    if (pos !== -1 && (bestPos === -1 || pos < bestPos)) {
      bestPos = pos;
    }
  }

  if (bestPos === -1) return contenido.slice(0, 200);

  const start = Math.max(0, bestPos - 80);
  const end = Math.min(contenido.length, bestPos + 120);
  let extracto = contenido.slice(start, end);
  if (start > 0) extracto = '...' + extracto;
  if (end < contenido.length) extracto += '...';
  return extracto;
}

/** Respuesta JSON con CORS */
function corsResponse(data, status, request) {
  const origin = request ? (request.headers.get('Origin') || '') : '';
  const allowedOrigin = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];

  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': allowedOrigin,
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '86400',
  };

  if (status === 200) {
    headers['Cache-Control'] = 'public, max-age=60';
  }

  if (data === null) {
    return new Response(null, { status, headers });
  }

  return new Response(JSON.stringify(data), { status, headers });
}
