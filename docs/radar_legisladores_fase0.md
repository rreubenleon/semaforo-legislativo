# Radar de Legisladores — Fase 0 (Scope firmado)

**Fecha:** 2026-04-11
**Estado:** En construcción
**Inspiración directa:** PFF Player Prop Tool
**Ejecutivos:** Rubén (producto) + Claude (build)

---

## Qué es

Una vista tipo "Player Prop Tool" de PFF aplicada al universo legislativo
mexicano. Cada legislador recibe una ficha con tres proyecciones forward,
baseline histórico L3P, hit rate sobre picos mediáticos, matchup grade
contra la comisión dictaminadora, y narrativa corta generada con IA.
Vive en `fiatmx.com/radar` como pestaña pública. Arquitectura preparada
para cortar la segunda mitad detrás de paywall Pro cuando el usuario lo
decida.

La tesis: ningún monitor legislativo mexicano hoy ofrece proyecciones
forward por legislador con hit rate y matchup visible. El Radar abre una
categoría nueva en el mercado.

---

## Decisiones de producto (firmadas el 2026-04-11)

### Los tres stats headline de la tarjeta

| Slot | Proyección forward | Baseline abajo |
|------|--------------------|----------------|
| 1 | Iniciativas proyectadas próximos 15 días | Promedio L3P |
| 2 | Proposiciones con punto de acuerdo proyectadas próximos 15 días | Promedio L3P |
| 3 | Probabilidad de reacción a pico en categoría dominante | Hit rate L10 picos |

**L3P = últimos 3 periodos parlamentarios.** Un periodo parlamentario
equivale a un periodo ordinario de sesiones del Congreso (aprox.
septiembre–diciembre y febrero–abril). Esta ventana reemplaza al
"L10 Average" de PFF y evita castigar a legisladores novatos en LXVI.

### Hit rate

Métrica única, auditable, accionable.

> En los últimos 10 picos mediáticos de su categoría dominante,
> ¿cuántas veces respondió con un instrumento formal en menos de
> 7 días?

Un pico mediático es un evento de `reacciones_historicas` con
`score_media_evento` por encima del percentil 80. La categoría
dominante se calcula como la categoría con mayor número de instrumentos
presentados por el legislador en el último año. El resultado es un
entero sobre diez (por ejemplo 7/10) que además se puede expresar como
porcentaje (70%).

### Matchup grade

Un legislador recibe grade **GREAT / GOOD / FAIR / POOR** según el
cruce entre:

1. Su categoría dominante.
2. La comisión donde previsiblemente se dictamina su próximo instrumento
   (turno inferido por tipo y tema).
3. La tasa histórica de dictamen positivo de esa comisión para
   iniciativas del partido del legislador.

**GREAT:** categoría dominante del legislador coincide con
especialidad de la comisión receptora, y esa comisión tiene ≥65% de
tasa histórica de dictamen positivo para iniciativas de su partido.

**GOOD:** coincidencia temática y 50–65% de tasa de dictamen positivo.

**FAIR:** coincidencia parcial o tasa entre 35–50%.

**POOR:** sin coincidencia temática o tasa <35%.

### Visibilidad

Pestaña pública total en `fiatmx.com/radar`. Arquitectura frontend
diseñada para soportar dos modos sin refactor: `public` y `pro`.
Cuando llegue el día del switch, una variable CSS corta la mitad
inferior con degradado a blanco y sobrepone el paywall. Cero cambio
de backend.

### Narrativa IA

Dos o tres frases por legislador generadas con Claude Haiku vía API,
alimentadas con los stats precomputados. Costo estimado: bajo,
procesando solo los legisladores que aparecen en la vista principal
del día. Refresco cada 24 horas, no cada run.

---

## Alojamiento

Stack Cloudflare extendido. Cero costo adicional mientras FIAT siga por
debajo de 100k requests diarios.

| Componente | Rol en el Radar |
|------------|-----------------|
| SQLite local (`semaforo.db`) | Source of truth en el pipeline CI |
| Cloudflare D1 (`fiat-busqueda`) | Copia servible para lecturas del Worker |
| Cloudflare R2 | Fotos de legisladores y blobs históricos |
| Cloudflare Workers KV | Cachés calientes (top 20 del día) |
| Worker (`fiat-busqueda`) | API `/api/legislador/*` |
| `dashboard/data.json` | Solo índice ligero de legisladores para pintar la tabla |

El detalle de cada legislador se pide on-demand al Worker cuando el
usuario hace clic en una fila. `data.json` deja de crecer sin control
y el dashboard recupera velocidad.

---

## Schema nuevo

Cuatro tablas nuevas, todas idempotentes en su carga.

### `legisladores_perfil`

Enriquecimiento biográfico y de contacto. Una fila por legislador.

```sql
CREATE TABLE IF NOT EXISTS legisladores_perfil (
    legislador_id      INTEGER PRIMARY KEY REFERENCES legisladores(id),
    biografia          TEXT,
    año_nacimiento     INTEGER,
    genero             TEXT,
    profesion          TEXT,
    estudios           TEXT,
    twitter_handle     TEXT,
    web_personal       TEXT,
    foto_hd_url        TEXT,
    wikipedia_url      TEXT,
    fuente_scraping    TEXT,
    fecha_scraping     TEXT
);
```

### `legisladores_trayectoria`

Una fila por cada paso histórico. Permite cruzar legislaturas.

```sql
CREATE TABLE IF NOT EXISTS legisladores_trayectoria (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    legislador_id      INTEGER REFERENCES legisladores(id),
    legislatura        TEXT NOT NULL,
    cargo              TEXT,
    camara             TEXT,
    partido            TEXT,
    estado             TEXT,
    distrito           TEXT,
    principio_eleccion TEXT,
    comisiones         TEXT,
    fecha_inicio       TEXT,
    fecha_fin          TEXT,
    fuente             TEXT,
    UNIQUE(legislador_id, legislatura, cargo)
);
```

### `legisladores_stats`

Snapshot recalculado en cada run del pipeline. Fila única por legislador
(sin historia; si se quiere histórico, se deriva de series temporales
existentes).

```sql
CREATE TABLE IF NOT EXISTS legisladores_stats (
    legislador_id                 INTEGER PRIMARY KEY REFERENCES legisladores(id),
    fecha_calculo                 TEXT NOT NULL,
    categoria_dominante           TEXT,
    iniciativas_proy_15d          REAL,
    proposiciones_proy_15d        REAL,
    prob_reaccion_dominante       REAL,
    promedio_l3p_iniciativas      REAL,
    promedio_l3p_proposiciones    REAL,
    matchup_grade                 TEXT,
    matchup_comision_target       TEXT,
    matchup_tasa_dictamen         REAL,
    narrativa                     TEXT,
    narrativa_generada            TEXT
);
```

### `legisladores_hit_rate`

Una fila por legislador por ventana. Permite que el modal grafique la
historia sin recalcular en vivo.

```sql
CREATE TABLE IF NOT EXISTS legisladores_hit_rate (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    legislador_id       INTEGER REFERENCES legisladores(id),
    categoria           TEXT NOT NULL,
    ventana_picos       INTEGER NOT NULL DEFAULT 10,
    ventana_dias        INTEGER NOT NULL DEFAULT 7,
    respondio           INTEGER NOT NULL,
    total_oportunidades INTEGER NOT NULL,
    fecha_calculo       TEXT NOT NULL,
    UNIQUE(legislador_id, categoria, ventana_picos, ventana_dias)
);
```

---

## Fases de ejecución

Mismo plan que propuse ayer, acotado con lo que aprendimos al revisar
el terreno.

### Fase 0 — Scope (HOY)

Documento firmado. Schema aplicado en SQLite local y D1. Sin código
del dominio todavía.

### Fase 1 — Enriquecimiento de datos (2 semanas)

- Scraper individual por legislador contra su página personal en SITL
  (Diputados) y en el portal del Senado. Saca biografía, comisiones
  con cargo, fotos HD, suplente, fracción.
- Backfill de trayectoria cross-legislatura para reelectos usando
  `nombre_normalizado` + `sitl_id` histórico.

**Nota de terreno:** `legisladores` ya incluye 500 diputados y 128
senadores LXVI con vínculos a instrumentos y reacciones. El Radar
arranca con 628 fichas desde el día uno. No hay fase de backfill de
Senado porque ya existe.

### Fase 2 — Modelos y agregados (2 semanas)

- `proyector_legislador.py`: proyecciones forward 15 días por tipo.
- `hit_rate_calculator.py`: carga `legisladores_hit_rate` desde
  `reacciones_historicas` filtrando por percentil.
- `matchup_grader.py`: carga `matchup_grade` en `legisladores_stats`
  cruzando categoría dominante, comisión turno y tasa histórica de
  dictamen positivo por comisión y partido.
- Integración en `main.py` como pasos idempotentes post-scoring.

### Fase 3 — Worker API y narrador (1 semana)

- `narrador_legisladores.py`: dos o tres frases con Claude Haiku,
  cacheadas 24 horas.
- Extensión del Worker `fiat-busqueda` con endpoints:
  - `GET /api/legislador/:id/overview`
  - `GET /api/legislador/:id/matchup`
  - `GET /api/legislador/:id/hitrate?categoria=X`
  - `GET /api/legisladores/props?camara=&partido=&cat=`
- Migración: lo pesado se sirve del Worker, `data.json` solo indexa.

### Fase 4 — Frontend Radar (3 semanas)

- Nueva pestaña `/radar` en el dashboard.
- Tabla filtrable con columnas Legislador, Categoría Dominante,
  Proyección Principal, L3P, Cov Prob, Edge, Matchup Grade, Hit Rate.
- Modal de detalle con los cuatro tabs
  (Overview, Matchup, Hit Rate, Trayectoria).
- Scraping y hosting de fotos en R2.
- Render condicional `public` vs `pro`.

### Fase 5 — Showcase Carolina Viggiano (1 semana)

- Correlación entre instrumentos presentados y picos mediáticos
  específicos (timeline visual con marcadores de evento + instrumento).
- Trayectoria cross-legislatura mostrando cómo cambia su
  comportamiento de una curul a otra.
- Documento o video corto vendible hacia afuera.

### Fase 6 — Terminal IA (roadmap)

Chat dentro de FIAT que consulta el mismo Worker con acceso a stats
agregados del Radar. Respuestas siempre con respaldo cuantitativo.
No arranca hasta que Fase 4 esté sólida.

---

## Limitaciones aceptadas

- **Odds reales multi-casa:** no existen para el mercado legislativo
  mexicano. Sustituimos por consensus FIAT vs escenario opositor.
- **Transcripciones de pleno:** fuera de scope en esta iteración.
- **Trayectoria cross-legislatura:** LXV, LXIV y anteriores requieren
  scraping histórico; Fase 1 arranca solo con LXVI.
- **Fotos profesionales:** usamos lo disponible en SITL y Senado.
- **Diario de Debates voto por voto:** no en esta vuelta.

---

## Presupuesto

$0/mes mientras FIAT no rebase 100k requests diarios.
Narrador con Claude Haiku: <$5/mes a ritmo normal.
Cuando llegue el momento del Pro, Cloudflare Workers paid plan son $5/mes
y siguen soportando el volumen sin ajustes.
