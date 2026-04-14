-- Radar de Legisladores — Schema v1 (Fase 0)
-- Aplicable tanto a SQLite local (semaforo.db) como a Cloudflare D1.
-- Todas las tablas son IF NOT EXISTS para ser idempotentes.

-- ─────────────────────────────────────────────────────────────
-- legisladores (snapshot en D1)
-- En semaforo.db ya existe con más columnas; en D1 la creamos
-- con el subset mínimo para que las tablas del Radar puedan
-- resolver sus foreign keys. El pipeline paralelo del Radar
-- la sincroniza cada run desde semaforo.db en modo RO.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS legisladores (
    id                 INTEGER PRIMARY KEY,
    nombre             TEXT NOT NULL,
    nombre_normalizado TEXT NOT NULL,
    camara             TEXT NOT NULL,
    partido            TEXT,
    estado             TEXT,
    distrito           TEXT,
    foto_url           TEXT,
    legislatura        TEXT DEFAULT 'LXVI'
);

CREATE INDEX IF NOT EXISTS idx_legisladores_camara
    ON legisladores(camara);
CREATE INDEX IF NOT EXISTS idx_legisladores_partido
    ON legisladores(partido);

-- ─────────────────────────────────────────────────────────────
-- legisladores_perfil
-- Enriquecimiento biográfico. Una fila por legislador.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS legisladores_perfil (
    legislador_id      INTEGER PRIMARY KEY REFERENCES legisladores(id),
    biografia          TEXT,
    anio_nacimiento    INTEGER,
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

CREATE INDEX IF NOT EXISTS idx_perfil_twitter
    ON legisladores_perfil(twitter_handle);

-- ─────────────────────────────────────────────────────────────
-- legisladores_trayectoria
-- Paso histórico del legislador cámara × legislatura.
-- Soporta cruce cross-legislatura para reelectos.
-- ─────────────────────────────────────────────────────────────
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

CREATE INDEX IF NOT EXISTS idx_trayectoria_legis
    ON legisladores_trayectoria(legislador_id);
CREATE INDEX IF NOT EXISTS idx_trayectoria_legislatura
    ON legisladores_trayectoria(legislatura);

-- ─────────────────────────────────────────────────────────────
-- legisladores_stats
-- Snapshot recalculado cada run del pipeline.
-- Una fila por legislador; se sobrescribe con UPSERT.
-- ─────────────────────────────────────────────────────────────
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

CREATE INDEX IF NOT EXISTS idx_stats_categoria
    ON legisladores_stats(categoria_dominante);
CREATE INDEX IF NOT EXISTS idx_stats_grade
    ON legisladores_stats(matchup_grade);

-- ─────────────────────────────────────────────────────────────
-- legisladores_hit_rate
-- Hit rate precomputado. Una fila por (legislador, categoria, ventana).
-- ─────────────────────────────────────────────────────────────
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

CREATE INDEX IF NOT EXISTS idx_hitrate_legis
    ON legisladores_hit_rate(legislador_id);
CREATE INDEX IF NOT EXISTS idx_hitrate_categoria
    ON legisladores_hit_rate(categoria);
