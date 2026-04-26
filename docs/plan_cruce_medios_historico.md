# Plan: Cruce histórico instrumentos × cobertura mediática

**Estado**: pausado abr 26 2026 hasta presupuesto disponible.
**Retomar**: cuando el user tenga ~$75 USD para SerpAPI básico (1 mes).

## Objetivo

Para los 218 reelectos LXIV/LXV → LXVI, encontrar la cobertura mediática
**previa al 2024** que respondió a sus iniciativas/proposiciones. Es un
**estudio one-shot**, no parte del pipeline.

## Por qué necesitamos esto

El cruce con datos actuales (post-ago 2024) NO sirve porque:
- Los instrumentos LXIV (2018-2021) y LXV (2021-2024) no tienen cobertura
  mediática en nuestra BD
- Sin cruce histórico no podemos analizar *a qué responden los legisladores*
- Es la pieza que falta para conectar comportamiento legislativo histórico
  con atención mediática histórica

## Datos ya cargados (input)

- `sil_documentos_historicos` en D1: 11,029 instrumentos LXIV+LXV de 202
  reelectos (Iniciativas + Proposiciones con PA)
- Campos clave: `legislador_id`, `denominacion`, `fecha_presentacion`,
  `tema`, `turnado_a`, `estatus`
- `eval/reelectos/diagnostico_unificado.json`: 222 reelectos con sus
  legislaturas previas

## Diagnóstico Fase 0 (hecho abr 26 2026)

Probé 4 métodos para descubrir cobertura histórica:

| Método | Resultado | Por qué |
|---|---|---|
| Bing site:* search | ❌ CAPTCHA | Anti-bot al primer intento |
| DuckDuckGo site:* search | ❌ Challenge | Anti-bot al primer intento |
| Sitemaps medios | ❌ Solo Jornada (categorías, no notas) | Universal/El País 404 |
| Wayback Machine CDX | ⚠️ Funciona pero solo por URL pattern | No filtra por contenido |
| Chrome MCP autenticado | ⏸️ No probado (no se dio permiso a dominios) | Requiere autorización |

**Conclusión**: scrape directo de buscadores externos NO es viable a
escala sin pagar API. Las 3 opciones reales son las 3 abajo.

## Tres opciones (decidir cuando retomemos)

### A. Chrome MCP autenticado — $0 USD, ~6-8 horas

- Yo automatizo los buscadores internos de El Universal y El País con tu
  sesión paga (sin paywall)
- Para Jornada uso Wayback + scrape directo (sin paywall)
- Tu Chrome ocupado durante el scrape — partir en bloques de 50 reelectos
- Necesita: autorizar dominios `eluniversal.com.mx`, `elpais.com`, `jornada.com.mx`
  en la extensión Claude

### B. SerpAPI Google Search — $75 USD por 1 mes, ~30 min

- Plan básico SerpAPI: 5,000 búsquedas/mes = $75 USD
- Necesitamos 654 búsquedas (218 × 3 medios) → cabe sobrado
- Cancelas el plan después del estudio (no es subscripción permanente)
- SerpAPI bypassa CAPTCHAs, devuelve título + URL + snippet en JSON
- Tu `SERPAPI_KEY` ya está en GitHub Actions (lo usas para Trends)
- Para texto completo de notas paywalled: aún requiere browser puntual

### C. Híbrido SerpAPI + Chrome — $75 USD, ~1-2 horas total ⭐ RECOMENDADO

- SerpAPI encuentra todas las URLs candidatas (~30 min)
- Para Jornada (sin paywall) → HTTP directo extrae texto
- Para Universal y El País (paywall) → Chrome MCP autenticado solo para
  las URLs ya filtradas (~50-100 notas relevantes)
- Tu browser ocupado solo ~30-45 min en lugar de 6-8 horas

## Storage planificado

**Tabla nueva en D1** (NO toca pipeline):

```sql
CREATE TABLE cobertura_historica_legisladores (
  id INTEGER PRIMARY KEY,
  legislador_id INTEGER,           -- FK a legisladores LXVI
  instrumento_id INTEGER,          -- FK a sil_documentos_historicos (NULL si match es genérico)
  medio TEXT,                      -- 'jornada' | 'universal' | 'el_pais'
  fecha_articulo TEXT,
  url_articulo TEXT,
  titulo_articulo TEXT,
  snippet TEXT,                    -- 200 chars de contexto
  tipo_match TEXT,                 -- 'instrumento_explicito' | 'mencion_directa' | 'tema_relacionado'
  dias_delta_instrumento INTEGER,  -- días entre instrumento y artículo
  confianza REAL                   -- 0.0-1.0
);
```

**Volumen estimado**: ~2,200 rows total (218 × ~10 menciones promedio).
~1 MB en D1. Trivial.

## Algoritmo del matcher (3 niveles de confianza)

Solo guardamos matches con `confianza >= 0.5`:

1. **`instrumento_explicito`** (0.9-1.0): título cita textual de la
   iniciativa o nombre + ley específica + fecha cercana
2. **`mencion_directa_periodo`** (0.5-0.7): nota habla del legislador en
   su periodo activo, fecha entre presentación y aprobación/desechamiento
3. **`tema_relacionado_periodo`** (0.3-0.5): legislador + tema + fecha
   cercana, sin cita explícita → DESCARTADO (ruido)

## Endpoint Worker (después del scrape)

```
GET /cobertura?legislador_id=N
→ {
    legislador_id, total_menciones,
    por_medio: {jornada, universal, el_pais},
    por_instrumento: [{instrumento_id, n_menciones, primeras_3_urls}],
    timeline: [{mes, n_menciones}]
  }
```

## UI propuesta

Sub-sección dentro de la pestaña **Trayectoria** existente del Radar:
"Cobertura mediática histórica" con timeline + breakdown por medio.

## Checklist de 6 puntos (opción C)

| # | Punto | Status |
|---|---|---|
| 1 | Toca | Tabla nueva en D1, script nuevo, NO toca pipeline ni `articulos`/`sil_documentos` |
| 2 | Costo happy path | $75 USD (1 mes SerpAPI) |
| 3 | Costo worst case | $75 USD máximo (cancelas después) |
| 4 | Interacción | Off-pipeline. Chrome ocupado ~30-45 min |
| 5 | Circuit breaker | Cap 50 búsquedas/medio/día. Solo guarda matches ≥0.5 confianza |
| 6 | Rollback | `DROP TABLE cobertura_historica_legisladores` (1 línea) |

## Pasos concretos para retomar

1. **Verificar saldo SerpAPI** (`SERPAPI_KEY` ya configurada como secret)
2. Subir el plan a básico ($75/mes) si está en free
3. Crear `scripts/scrape_cobertura_historica.py` con:
   - Itera 218 reelectos
   - Por cada uno, 3 búsquedas SerpAPI (`site:jornada.com.mx`,
     `site:eluniversal.com.mx`, `site:elpais.com`)
   - Filtra por fecha entre `min(fecha_instrumento) - 7d` y
     `max(fecha_instrumento) + 30d`
   - Para Jornada (sin paywall): HTTP fetch del texto
   - Para Universal/El País: Chrome MCP fetch autenticado
   - Aplica matcher de confianza
   - Cache JSON durable en `eval/cobertura/scrape_cache.json`
4. Crear `scripts/sync_cobertura_d1.py` (mismo patrón que `sync_historicos_d1.py`)
5. Crear endpoint `/cobertura` en Worker
6. Sub-sección en TabTrayectoria del dashboard
7. **Cancelar plan SerpAPI** una vez completado el estudio

## Notas importantes

- **NO va al pipeline**. Es script estático, una sola corrida.
- **NO requiere cuenta nueva** (cabe en D1, todas las cuentas existentes alcanzan)
- El user dijo claramente: "no quiero romper ningún pipeline" y "no necesitamos
  guardar información que no nos va a servir" → solo guardar matches útiles,
  descartar el resto
- Usuario tiene suscripciones pagas a El Universal y El País → bypass paywall
  completo con Chrome MCP autenticado
