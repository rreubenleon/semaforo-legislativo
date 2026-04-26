# FIAT Premium — Propuesta de Monetización

**Fecha**: 26 abr 2026
**Precio objetivo**: $699 MXN/mes/usuario (~$35 USD)
**Margen objetivo**: ≥85% bruto
**Tiempo a primer dólar**: 5-6 semanas

---

## 1. Inventario actual (lo que ya tenemos)

### Datos en producción

| Activo | Volumen | Donde |
|---|---|---|
| Artículos medios | 66,052 (22 fuentes) | SQLite local + FTS5 en D1 |
| Instrumentos SIL LXVI | 16,352 | SQLite + D1 |
| Instrumentos LXIV/LXV reelectos | 11,029 | D1 (`sil_documentos_historicos`) |
| Documentos gaceta (Dip+Sen) | 8,461 | SQLite + D1 |
| Tweets monitoreados | 1,175 (18 cuentas: 4 periodistas + 14 coordinadores) | SQLite |
| Mañaneras | 345 | SQLite |
| Legisladores con perfil completo | 682 | SQLite + D1 (con ELO, H2H, hit rate, matchup) |
| Comisiones SITL | 53 | D1 (`comisiones_stats`) |
| Síntesis legislativa diaria | 28 días | SQLite |
| Reacciones históricas calculadas | 46,929 | SQLite |
| Categorías temáticas | 19 | config.py |

### Infraestructura

| Recurso | Uso | Costo actual |
|---|---|---|
| Cloudflare D1 | 117 MB / 5 GB free | $0 |
| Cloudflare Workers | <1% del free tier | $0 |
| Cloudflare Pages | ilimitado free | $0 |
| GitHub Actions | dentro de 2K min/mes | $0 |
| Anthropic Haiku 4.5 | $7.50/mes cap | $7.50 (cuando se use) |
| Resend (email) | 0 enviado | $0 |
| SerpAPI | free 250/mes | $0 |
| Dominio fiatmx.com | — | $12/año |
| **Total operativo** | | **~$8 USD/mes** |

### Endpoints del Worker actuales

- `/buscar` — FTS5 sobre 105K documentos (gaceta + articulos + sil)
- `/radar` — 682 legisladores con perfil completo
- `/comisiones` — 53 comisiones con productividad
- `/h2h` — head-to-head legislador × comisión
- `/historicos` — trayectoria LXIV/LXV de reelectos
- `/registro` — captura emails de waitlist (0 registros hoy)

### Lo que NO tiene FIAT hoy

- Auth de usuarios (login, password, sesiones)
- Sistema de pagos
- Terminal IA conversacional
- Alertas configurables
- Monitoreo personalizado de legisladores
- API pública con keys
- Consola personalizable separada del home
- Exports descargables (PDF/Excel)

---

## 2. El producto: FIAT Premium

### Promesa de valor (el "navaja suiza")

Un periodista, asesor, lobbyist, consultora política o analista paga $699 MXN/mes y obtiene:

**1. Terminal IA conversacional ("FIAT Brain")**
Pregunta cualquier cosa en lenguaje natural y obtiene respuesta con citas.

Ejemplos de preguntas que debe responder bien:
- *"Cuál es la constante de Rubén Moreira y siempre ha presentado que es legislador?"*
  → Análisis cruzando trayectoria LXIV+LXV+LXVI: "Moreira presenta constantemente en Justicia (32 instrumentos LXIV+LXV) y Hacienda. En LXVI sigue el patrón con 89 instrumentos del mismo perfil. Tasa aprobación histórica 44%."

- *"Cada vez que hay incendio forestal, qué legisladores se manifiestan con puntos de acuerdo?"*
  → Cruza tendencia "incendio forestal" en medios + proposiciones con punto de acuerdo en SIL filtradas por keyword: lista de 15 legisladores con N proposiciones cada uno, partidos, fechas.

- *"Resúmeme qué pasó esta semana en seguridad y justicia"*
  → Genera resumen de artículos top + instrumentos nuevos + intervenciones de los coordinadores parlamentarios + síntesis legislativa.

**2. Monitoreo personal**
Selecciona hasta 20 legisladores → dashboard custom con:
- Sus instrumentos nuevos en tiempo real
- Sus tweets / menciones en medios
- Comparativa con su trayectoria histórica
- Alerta cuando se desvía de su patrón

**3. Alertas configurables**
Crea reglas tipo "cuando aparezca X tema con score > umbral, mándame email":
- "Avísame cuando reforma electoral pase a alta urgencia"
- "Avísame cuando Monreal presente en seguridad"
- "Avísame cuando Comisión de Justicia dictamine algo"
- Email vía Resend (ya configurado), Telegram (a futuro)

**4. Consola personalizable**
Página `/console` distinta del home pública. Widgets que el usuario arma:
- Tema favorito (uno o varios)
- Legisladores favoritos
- Comisiones que vigila
- Mini-feeds de tweets
- Tarjetas de matchup
- Como Bloomberg Terminal pero para Congreso MX

**5. Exports descargables**
- PDF "Trayectoria del legislador X" listo para imprimir
- Excel "Comparativa de productividad por partido"
- Excel "Cobertura mediática del tema X últimos 30 días"
- Reportes son lo que un consultor político le entregaría a un cliente — automatizado

**6. API pública con keys**
Para integraciones (CRM de relaciones gubernamentales, Slack bots, etc.)
- Endpoint `/api/v1/...` con `X-API-Key` header
- 10,000 calls/mes incluidas en $699
- Overage a $0.10 MXN/call adicional
- Documentación tipo Stripe/Twilio

---

## 3. Arquitectura técnica

### Capa de Auth (nueva)

**Decisión: usar Cloudflare D1 + Workers (no añadir backend Node/Python)**

Razones:
- Ya tenemos toda la infra ahí
- D1 escala a millones de usuarios
- Workers gratis hasta 100K req/día
- Cero deploy nuevo, cero servidor que mantener

**Schema D1 nuevo**:

```sql
CREATE TABLE usuarios (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,           -- bcrypt o scrypt
  nombre TEXT,
  organizacion TEXT,
  fecha_alta TEXT DEFAULT (datetime('now')),
  email_verificado INTEGER DEFAULT 0,
  ultimo_login TEXT,
  estado TEXT DEFAULT 'activo'           -- 'activo' | 'suspendido' | 'cancelado'
);

CREATE TABLE suscripciones (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  usuario_id INTEGER NOT NULL,
  stripe_customer_id TEXT,
  stripe_subscription_id TEXT,
  plan TEXT NOT NULL,                    -- 'premium_mensual' | 'premium_anual' | 'team'
  estado TEXT NOT NULL,                  -- 'activa' | 'cancelada' | 'past_due'
  fecha_inicio TEXT NOT NULL,
  fecha_fin TEXT,
  precio_mxn INTEGER NOT NULL,           -- 699 o 6990 (anual con descuento)
  FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
);

CREATE TABLE sesiones (
  token TEXT PRIMARY KEY,                -- random 64 chars
  usuario_id INTEGER NOT NULL,
  expira TEXT NOT NULL,
  ip TEXT,
  user_agent TEXT,
  FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
);

CREATE TABLE usuario_seguimiento (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  usuario_id INTEGER NOT NULL,
  legislador_id INTEGER,
  tema TEXT,
  comision TEXT,
  notif_email INTEGER DEFAULT 1,
  fecha TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
);

CREATE TABLE alertas_personalizadas (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  usuario_id INTEGER NOT NULL,
  nombre TEXT NOT NULL,
  trigger_tipo TEXT NOT NULL,            -- 'tema_score' | 'legislador_actividad' | 'comision_dictamen' | 'keyword'
  trigger_config TEXT NOT NULL,          -- JSON con la config de la regla
  activa INTEGER DEFAULT 1,
  ultima_disparada TEXT,
  FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
);

CREATE TABLE ai_queries_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  usuario_id INTEGER NOT NULL,
  pregunta TEXT NOT NULL,
  modelo TEXT,
  tokens_input INTEGER,
  tokens_output INTEGER,
  costo_usd REAL,
  fecha TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
);

CREATE TABLE api_keys (
  key_hash TEXT PRIMARY KEY,             -- hash de la key (la key se le da al user solo una vez)
  usuario_id INTEGER NOT NULL,
  nombre TEXT,                           -- "Producción", "Dev", etc.
  fecha_creacion TEXT DEFAULT (datetime('now')),
  ultima_uso TEXT,
  calls_mes_actual INTEGER DEFAULT 0,
  FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
);
```

**Endpoints nuevos del Worker**:

```
POST /auth/registrar      → crea usuario, envía email de verificación
POST /auth/login          → email+password → token de sesión
POST /auth/logout         → invalida token
GET  /auth/me             → datos del usuario actual
POST /auth/reset-password → magic link a email
POST /auth/verificar      → click desde email para verificar

POST /pago/checkout       → crea Stripe checkout session
POST /pago/webhook        → recibe eventos de Stripe (payment success, cancel, etc.)
POST /pago/cancelar       → cancela suscripción

GET  /seguimiento         → mis legisladores/temas seguidos
POST /seguimiento         → agregar uno
DELETE /seguimiento/:id   → quitar uno

GET  /alertas             → mis alertas
POST /alertas             → crear nueva
PATCH /alertas/:id        → activar/desactivar/editar
DELETE /alertas/:id

POST /ai/terminal         → pregunta IA (premium only)
GET  /ai/historial        → mis queries pasadas

GET  /api/v1/legisladores → API pública con key
GET  /api/v1/temas
GET  /api/v1/instrumentos
... (espejo de los endpoints internos pero con API key auth)

GET  /export/trayectoria/:id    → PDF de trayectoria
GET  /export/comparativa/...    → Excel de comparativa
```

### Capa de Pagos: Stripe (recomendado) o Conekta

**Comparación**:

| | Stripe | Conekta |
|---|---|---|
| Fee | 3.6% + $3 MXN internacional | 3.5% + $3 MXN |
| CFDI integrado | No (requiere Facturapi) | Sí, nativo |
| Setup | 1 hora | 2-3 horas (más papeleo) |
| Soporte SDK Workers | Excelente | Limitado |
| País del usuario | Cualquiera | México |

**Recomendación**: **Stripe primero** (faster to market). CFDI lo añadimos via Facturapi cuando tengamos suficientes usuarios que lo pidan ($199/mes Facturapi).

### Capa de Terminal IA

**Modelo recomendado**: Claude **Haiku 4.5** para la mayoría de queries, **Sonnet 4.5** para queries complejas (el usuario marca "Análisis profundo").

**Por qué no GPT/Gemini**:
- Haiku 4.5 con prompt caching es 5x más barato que GPT-4o-mini
- Sonnet 4.5 tiene mejor reasoning legislativo en español
- Ya tenemos cuenta y SDK integrado

**Arquitectura**:

```
Usuario: "Cada vez que hay incendio forestal, qué legisladores presentan PA?"
    ↓
Worker /ai/terminal (auth check, rate limit, log)
    ↓
Claude Haiku 4.5 con system prompt:
  - Schema de las tablas D1 (esquema legible)
  - Lista de funciones SQL/D1 disponibles
  - Cita: SIEMPRE devuelve respuesta con referencias [1][2][3] que linkean
    a articulos.id o sil_documentos.id
    ↓
Modelo decide: query D1 SQL o multi-query
    ↓
Worker ejecuta queries D1
    ↓
Claude formatea respuesta natural con citas
    ↓
Usuario ve: respuesta + tarjetas clicables a las fuentes citadas
```

**Tipos de query soportadas en MVP**:
1. "Resumen de tema X últimos N días"
2. "Patrón de legislador X en su carrera"
3. "Quién está presentando sobre tema X ahora?"
4. "Comparar legisladores A y B en tema X"
5. "Comisión X qué dictaminó este mes?"
6. "Tendencia de tema X últimos 90 días"

**Costo por query**:
- Haiku: ~$0.001-0.005 USD por query (con caching)
- Sonnet: ~$0.02-0.05 USD por query
- Promedio esperado: $0.01 USD/query con mix 80% Haiku + 20% Sonnet

**Cap por usuario**:
- 200 queries/mes incluidas en $699
- Overage: $0.50 MXN por query adicional (cubre ~$0.025 USD costo + margen)

### Capa de Alertas

Cron en GitHub Actions cada 4h después del pipeline existente:
1. Lee `alertas_personalizadas WHERE activa=1`
2. Por cada alerta, evalúa su trigger contra estado actual
3. Si dispara, manda email vía Resend al usuario
4. Marca `ultima_disparada` para no repetir spam

Tipos de trigger:
- `tema_score`: cuando score de categoría X > umbral
- `legislador_actividad`: cuando legislador X presenta nuevo instrumento
- `comision_dictamen`: cuando comisión X dictamina algo
- `keyword`: cuando aparece keyword X en N+ artículos del día

### Capa de Frontend (Consola Premium)

**Decisión: NO crear app aparte**. Extender `dashboard/index.html`:
- Misma React SPA
- Nueva ruta `/console` (premium only)
- Login flow → cookie de sesión
- Si user no tiene suscripción activa → redirect a `/pricing`

Componentes nuevos:
- `<Login />`, `<Register />`, `<Pricing />`
- `<Console />` con widgets configurables
- `<TerminalIA />` chat-like
- `<MisAlertas />`, `<MiSeguimiento />`
- `<Settings />` (cambiar password, ver suscripción, cancelar)

### Capa de blindaje

**BD/datos:**
- Backup diario D1 → R2 (~$5/mes para 100GB de backups)
- Backup semanal SQLite local → GitHub Release privado
- Snapshot de schema en migraciones versionadas (`worker/migrations/`)

**Algoritmo:**
- Pesos del scoring NO se exponen en `/buscar`, `/radar`, etc. — solo el resultado final
- Documentación interna del algoritmo en `docs/algoritmo_propietario.md` (privado, no en repo público)
- Si vamos a abrir código, eventualmente: extraer scoring core a un módulo privado

**Anti-abuso:**
- Cloudflare WAF rules: bloquear bots agresivos (free tier)
- Rate limit por IP: 60 req/min en Worker
- Rate limit por user_id: 10 queries IA/min, 200/mes total
- Rate limit por API key: 10K calls/mes (premium), overage tracked
- Validación de input estricta (ya está)
- Logs de queries IA por usuario para detectar abuso patrón
- Email verification obligatorio antes de dar acceso premium

**Cuentas:**
- Password con scrypt o bcrypt (12+ rounds)
- Tokens de sesión rotables, expiración 30 días
- 2FA opcional (TOTP) en V2

---

## 4. Costos por usuario premium

### Costo unitario marginal

| Costo por usuario activo/mes | $ MXN | $ USD |
|---|---:|---:|
| Stripe fees (3.5% de $699 + $3) | ~$28 | ~$1.4 |
| Anthropic Haiku/Sonnet (200 queries promedio) | ~$50 | ~$2.5 |
| Resend (50 emails de alertas/mes) | ~$1 | $0.05 |
| Cloudflare D1/Workers (incremental) | $0 | $0 |
| Backup R2 (proporcional) | ~$2 | $0.10 |
| **Total costo unitario** | **~$81 MXN** | **~$4.05 USD** |

### Margen bruto

| | $ MXN |
|---|---:|
| Ingreso por usuario/mes | **$699** |
| Costo unitario | $81 |
| **Margen bruto unitario** | **$618 (88%)** |

### Escenarios

| Usuarios | Ingreso/mes | Costos var. | Margen bruto/mes | Margen anual |
|---:|---:|---:|---:|---:|
| 10 (beta) | $6,990 | $810 | $6,180 | $74,160 |
| 50 | $34,950 | $4,050 | $30,900 | $370,800 |
| **100** | **$69,900** | **$8,100** | **$61,800** | **$741,600** |
| 250 | $174,750 | $20,250 | $154,500 | $1.85M |
| 500 | $349,500 | $40,500 | $309,000 | $3.7M |
| 1,000 | $699,000 | $81,000 | $618,000 | $7.4M |

**Break-even**: con tu actual costo operativo ($8/mes), break-even es a **1 usuario**. A partir del 2do, ya generas margen.

A 100 usuarios, recuperas el costo de tiempo de desarrollo en 2-3 meses (asumiendo $30K MXN/mes de "salario equivalente").

---

## 5. Inversión necesaria

### Inversión cash inicial (mes 0-2)

| Item | $ USD | $ MXN |
|---|---:|---:|
| Anthropic top-up beta | $50 | ~$1,000 |
| Stripe activación | $0 | $0 |
| Resend (suficiente con free tier inicial) | $0 | $0 |
| Backup R2 | $0 (arranca con free 10GB) | $0 |
| Dominio fiatmx.com | ya tienes | $0 |
| Facturapi (CFDI, opcional V1) | $10/mes | $200 |
| **Total cash inicial** | **~$60** | **~$1,200** |

### Inversión cash escalando (mes 3-6, con 50-100 usuarios)

| Item | $ USD/mes | $ MXN/mes |
|---|---:|---:|
| Anthropic (escala con usuarios) | $50-200 | $1,000-4,000 |
| Stripe fees | proporcional | proporcional |
| Resend Pro | $20 | $400 |
| Cloudflare Workers Paid (si pasas 100K req/día) | $5 | $100 |
| Facturapi | $10 | $200 |
| Marketing (FB/LinkedIn ads, opcional) | $200-500 | $4,000-10,000 |
| **Total escalando** | **~$300-750** | **~$6,000-15,000** |

### Inversión tiempo (semanas de desarrollo)

| Fase | Tiempo | Output |
|---|---|---|
| Semana 1 | ~30h | Auth + Stripe sandbox + landing premium con waitlist |
| Semana 2 | ~30h | Schema D1 usuarios + login/registro UI + verificación email |
| Semana 3 | ~30h | Terminal IA backend + system prompts + caching |
| Semana 4 | ~30h | Console UI + widgets configurables |
| Semana 5 | ~30h | Alertas backend + emails + monitoreo personal |
| Semana 6 | ~30h | Exports + API pública v1 + docs |
| **Total** | **~180h** | **MVP Premium completo** |

A tu ritmo (yo trabajando contigo en sesiones), son **5-6 semanas calendario** con sesiones de trabajo regulares.

---

## 6. Roadmap

### Hito 0: Validación de demanda (esta semana)

- Cambiar landing page actual: agregar bloque "FIAT Premium llega pronto, $699 MXN/mes" con preview de features
- Form de waitlist (email + organización + caso de uso)
- Meta: **50 emails en waitlist en 14 días**

Si no llegas a 50, tenemos que pivotar precio o features ANTES de invertir en construcción.

### Hito 1: MVP Beta cerrado (semana 2-4)

- 10-20 usuarios beta gratuitos invitados de la waitlist
- Funciones mínimas: auth + terminal IA + 5 alertas predefinidas + monitoreo de hasta 10 legisladores
- Recibo feedback + iteración rápida
- Duración: 2-3 semanas

### Hito 2: Lanzamiento Premium (semana 5-6)

- Activar pagos Stripe en producción
- Onboarding: video 5min + opción "demo personalizada" 30min
- Pricing: $699 MXN/mes o $6,990 MXN/año (15% descuento, $5,940 ahorro)
- Meta: **30 usuarios pagados en 60 días post-lanzamiento**

### Hito 3: Crecimiento (mes 3-6)

- Marketing pagado: $200-500 USD/mes en FB/LinkedIn ads
- Publicación de "papers" tipo "Cómo medimos efectividad legislativa" — establecimiento de autoridad
- Integraciones: Slack bot, CRM bridges
- Meta: **100 usuarios pagados al mes 6** = $69K MXN MRR

### Hito 4: Sostenibilidad y escalado (mes 6+)

- Pricing tiers:
  - **Individual** $699 MXN/mes (lo que tenemos)
  - **Equipo** $2,499 MXN/mes (5 usuarios + reportes compartidos)
  - **Empresa** $9,999 MXN/mes (15 usuarios + SLA + onboarding dedicado)
- API pública pública con planes
- Meta: **300+ usuarios, $200K+ MRR** = empresa sostenible

### Métricas a monitorear desde día 1

- **MRR** (monthly recurring revenue)
- **Churn rate** (% que cancela cada mes)
- **CAC** (costo de adquisición de usuario)
- **LTV** (lifetime value, target 12+ meses)
- **AI cost per active user** (controlar margen)
- **NPS** (net promoter score, encuesta trimestral)

---

## 7. Riesgos y mitigaciones

| Riesgo | Impacto | Mitigación |
|---|---|---|
| Demanda menor a esperada | Alto | Hito 0 valida antes de invertir tiempo |
| Costo IA explosión (caso Haiku abr 25) | Medio | Hard caps por usuario + alerta cuando 75% del cap mensual |
| Usuarios abusan API/IA | Medio | Rate limits + logs + ban manual |
| Cancelación masiva | Alto | Onboarding fuerte + emails educativos + caso de uso semanal |
| Competidor copia | Bajo | La data tarda años en construir, FIAT tiene 1+ año de ventaja |
| Cambio en SIL/sources | Medio | Múltiples fuentes redundantes + alertas si scrapers fallan |
| Bug en algoritmo daña reputación | Alto | Backtest y calibración semanal ya implementados |

---

## 8. Decisiones que necesitas tomar antes de arrancar

1. **¿Stripe o Conekta?** (recomiendo Stripe por velocidad)
2. **¿Mensual + anual con descuento, o solo mensual al inicio?** (recomiendo ambos desde día 1, anual reduce churn)
3. **¿Activar API pública desde V1 o esperar a V2?** (recomiendo esperar V2, simplifica MVP)
4. **¿Cobrar IVA o pasarlo al cliente?** (precio sugerido $699 incluye IVA, mejor UX)
5. **¿Período de prueba gratuito 7 días?** (recomiendo SÍ, reduce fricción para registrarse)
6. **¿Quién es el primer usuario beta ideal?** (necesito que me digas 5 nombres reales para invitar)

---

## 9. Próximos pasos concretos

Si apruebas la propuesta general, en orden:

1. **Hoy/mañana**: rediseño de landing page con bloque premium + waitlist mejorado (2-3h)
2. **Esta semana**: schema D1 de auth + endpoints `/auth/*` (5-7h)
3. **Próxima semana**: integración Stripe sandbox + UI login/registro (8-10h)
4. **Semana 3**: Terminal IA backend con Haiku 4.5 + caching (10-15h)
5. **Semana 4**: Consola premium frontend + widgets (10-15h)
6. **Semana 5-6**: Alertas + monitoreo + onboarding (10-15h)

Cada paso aplica el **checklist de 6 puntos** que pactamos antes de mergear.
