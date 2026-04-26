# FIAT Premium — Diseño de Producto

**Complemento del doc** `propuesta_monetizacion_fiat_premium.md` (ese tiene los números).
Este doc tiene **el cómo se ve y se siente**.

---

## 1. Mapa de la experiencia (3 momentos)

```
┌───────────────────────────────────────────────────────────────┐
│  MOMENTO 1: VISITANTE NUEVO (no registrado)                  │
│  Ve la página pública con paywall a partir de un punto exacto│
│  Llamada a la acción: registrarse / pagar                     │
└───────────────────────────────────────────────────────────────┘
                              ↓ Hace click en Regístrate
┌───────────────────────────────────────────────────────────────┐
│  MOMENTO 2: REGISTRO + PAGO                                  │
│  Sign up con Google o email · Stripe Checkout                 │
│  Trial 48h · detección de correo institucional                │
└───────────────────────────────────────────────────────────────┘
                              ↓ Pago confirmado
┌───────────────────────────────────────────────────────────────┐
│  MOMENTO 3: USUARIO PREMIUM ACTIVO                           │
│  Dashboard completo + hamburguesa de configuración            │
│  Terminal IA accesible desde cualquier página                 │
│  Console personalizable                                       │
└───────────────────────────────────────────────────────────────┘
```

---

## 2. Vista pública (visitante nuevo)

### Lo que se ve completo (gratis, sin login)

Mantenemos exactamente como está hoy de arriba a abajo, hasta donde dije:

```
┌─────────────────────────────────────────────────────────────┐
│  HEADER (idéntico al actual)                                │
│  [Logo] EN VIVO  Actividad · Radar · Mapa · Comisiones · …  │
│                            [search]  Inicia sesión [Regístrate] │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  HERO MÉTRICAS (el actual: 5 chips con números FIAT)        │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  MAÑANERA + TWEETS FIAT (widget actual lateral)             │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  TOP 3 CATEGORÍAS (hero cards con score, color, momentum)   │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  HISTORIAL DE SCORES (gráfica de tendencia, multi-línea)    │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│  ←—————————— FIN DE LA VISTA GRATUITA ——————————→            │
└─────────────────────────────────────────────────────────────┘
```

### El paywall (a partir de "Ordenar por...")

```
┌─────────────────────────────────────────────────────────────┐
│  Ordenar por  [Score]  [Volumen]  [Aprobación]    ← este es │
│                                                  el primer  │
│  ┌────┐  ┌────┐  ┌────┐                          elemento   │
│  │SMG │  │EDU │  │SEG │  ← cards visibles parcial           │
│  │ 87 │  │ 71 │  │ 65 │     comienzan a fundirse al fondo   │
│  └────┘  └────┘  └────┘                                     │
│  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░       │
│  ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒       │
│  (degradado vertical de blanco a beige #faf6ee)             │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │      ✦ FIAT Premium                                  │  │
│  │                                                      │  │
│  │   Acceso completo a la inteligencia legislativa     │  │
│  │   más avanzada de México                             │  │
│  │                                                      │  │
│  │   ✓ Terminal IA con citas a fuentes                 │  │
│  │   ✓ Monitoreo personal de hasta 20 legisladores     │  │
│  │   ✓ Alertas configurables por tema y comisión       │  │
│  │   ✓ Consola personalizable                          │  │
│  │   ✓ Stats cruzadas: constantes, respuestas, patrones│  │
│  │   ✓ Exports en PDF y Excel                          │  │
│  │                                                      │  │
│  │  ┌─────────────────┐  ┌────────────────────┐       │  │
│  │  │ MENSUAL         │  │ ANUAL · ⭐ Popular │       │  │
│  │  │                 │  │                    │       │  │
│  │  │ $699 MXN/mes    │  │ $7,699 MXN/año     │       │  │
│  │  │                 │  │ (1 mes gratis)     │       │  │
│  │  │ [Empezar trial] │  │ [Empezar trial]    │       │  │
│  │  └─────────────────┘  └────────────────────┘       │  │
│  │                                                      │  │
│  │   48 horas de prueba gratis · Cancela cuando quieras│  │
│  │                                                      │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  ¿Trabajas en gobierno, academia o medios?                  │
│  Haz click aquí para tarifa institucional →                 │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

**Detalles técnicos del corte**:

- El degradado es CSS puro: `linear-gradient(to bottom, transparent 0%, #faf6ee 80%)` sobre los siguientes 200-300 px de contenido real, blureando con `backdrop-filter: blur(8px)`.
- El contenido real sigue ahí en el DOM (no eliminado), solo está visualmente cubierto.
- Esto es importante porque Google indexa el contenido completo (mejor SEO).
- Para asegurar que no haya bypass via inspect element / desactivar CSS: el frontend hace fetch a `/radar`, `/comisiones`, `/h2h` con header de auth → si no hay token premium válido, los endpoints devuelven datos limitados o 401. **El paywall NO es solo cosmético, es backend.**

---

## 3. Onboarding y registro

### Flujo de signup completo

```
[Click "Empezar trial"]
       ↓
┌──────────────────────────┐
│  Modal: Crea tu cuenta   │
│                          │
│  ┌────────────────────┐  │
│  │ G  Continuar con   │  │
│  │    Google          │  │
│  └────────────────────┘  │
│                          │
│  ── o ──                 │
│                          │
│  Email:   [_________]    │
│  Password:[_________]    │
│  Nombre:  [_________]    │
│  Org:     [_________] (opcional) │
│                          │
│  [Crear cuenta]          │
│                          │
│  Al continuar aceptas... │
└──────────────────────────┘
       ↓
[Detecta dominio del email]
       ↓
┌──────────────────────────────────────────────────────┐
│  Si email termina en @diputados.gob.mx,             │
│     @senado.gob.mx, @*.gob.mx, @*.edu.mx, @*.org.mx │
│  → Banner: "Detectamos correo institucional.         │
│     Tu organización puede acceder con descuento      │
│     del 50%. Te enviamos info por email."            │
│  → Crea cuenta normal pero con flag                  │
│     `tipo_correo='institucional'` y nos llega aviso  │
│     a contacto@fiatmx.com para hacer follow-up       │
│     manual con propuesta de tarifa especial          │
└──────────────────────────────────────────────────────┘
       ↓
[Email de verificación enviado vía Resend]
       ↓
[Usuario hace click en link]
       ↓
[Redirige a /pago/checkout?plan=mensual o anual]
       ↓
[Stripe Checkout (página hosted de Stripe, super pulida)]
       ↓
[Usuario paga con tarjeta]
       ↓
[Webhook a /pago/webhook → marca suscripción activa]
       ↓
[Usuario regresa a FIAT con sesión activa]
       ↓
[Onboarding tour: 3 pantallas que muestran terminal IA, monitoreo, alertas]
```

### Google SSO — implementación técnica

**Stack**: Cloudflare Workers + Google OAuth 2.0 + cookies seguras

```javascript
// Flow
1. Click "Continuar con Google"
   → Worker /auth/google/start genera state random + nonce
   → Redirige a accounts.google.com/o/oauth2/v2/auth con:
     client_id, redirect_uri=https://fiatmx.com/auth/google/callback
     scope=openid email profile, state, nonce

2. User autoriza
   → Google redirige a /auth/google/callback?code=...&state=...

3. Worker /auth/google/callback:
   → Valida state matchea
   → POST a oauth2.googleapis.com/token con code → recibe id_token
   → Decodifica JWT del id_token → email, nombre, picture
   → SI usuario existe en D1 por ese email → crea sesión
     SI no → crea usuario nuevo + sesión + redirige a /pago/checkout

4. Cookie httponly, secure, samesite=lax con session token
   → Token en D1 tabla sesiones, expira 30 días
```

**Costo**: $0. Google no cobra por OAuth.

**Setup**: 1 hora máximo
1. Crear OAuth client en Google Cloud Console (gratis)
2. Configurar redirect_uri = `https://fiatmx.com/auth/google/callback`
3. Guardar `GOOGLE_CLIENT_ID` y `GOOGLE_CLIENT_SECRET` como Worker secrets

### Detección de correos institucionales

**Lista de dominios privilegiados** (configurable):

```javascript
const DOMINIOS_INSTITUCIONALES = {
  // Gobierno federal
  '@diputados.gob.mx':  { tipo: 'congreso',   tier: 'gobierno' },
  '@senado.gob.mx':     { tipo: 'congreso',   tier: 'gobierno' },
  '@gobernacion.gob.mx': { tipo: 'ejecutivo', tier: 'gobierno' },
  '@gob.mx':            { tipo: 'ejecutivo',  tier: 'gobierno' },

  // Catch-all gobierno
  /\.gob\.mx$/:         { tipo: 'estatal',    tier: 'gobierno' },

  // Academia
  /\.edu\.mx$/:         { tipo: 'academia',   tier: 'academia' },
  '@unam.mx':           { tipo: 'academia',   tier: 'academia' },
  '@itam.mx':           { tipo: 'academia',   tier: 'academia' },
  '@cide.edu':          { tipo: 'academia',   tier: 'academia' },

  // Medios verificados (lista manual de top 30 medios MX)
  '@eluniversal.com.mx': { tipo: 'medios',    tier: 'medios' },
  '@reforma.com':       { tipo: 'medios',     tier: 'medios' },
  '@milenio.com':       { tipo: 'medios',     tier: 'medios' },
  // ... etc

  // ONGs y think tanks (lista manual)
  '@imco.org.mx':       { tipo: 'thinktank',  tier: 'ong' },
  '@fundar.org.mx':     { tipo: 'thinktank',  tier: 'ong' },
  // ... etc
};
```

**Acciones por tier**:
- `gobierno`: tarifa institucional 50% off ($349/mes) **previa verificación manual** del dominio (Stripe coupon code aplicado por nosotros)
- `academia`: 60% off ($279/mes) o gratis con verificación de credencial académica
- `medios`: 30% off ($489/mes) — periodistas son público clave
- `ong`: 50% off ($349/mes)
- Particulares: $699/mes precio normal

**Implementación**:
- Si email matchea, banner aparece pero NO se aplica descuento automático (anti-fraude)
- Click "Solicitar tarifa institucional" → form más completo (organización, cargo, justificación) → email a contacto@fiatmx.com → tú apruebas manualmente y mandas link de Stripe con coupon

---

## 4. Vista post-pago (usuario premium activo)

### Header rediseñado

```
┌─────────────────────────────────────────────────────────────────────┐
│ [Logo] EN VIVO  Actividad · Radar · Mapa · Comisiones · Métricas   │
│                  [search]                          ✦ Premium  ☰    │
└─────────────────────────────────────────────────────────────────────┘
                                                       ↑       ↑
                                            Badge       Hamburguesa (3 líneas)
                                          dorado/morado
                                          que indica que
                                          eres premium
```

### Click en hamburguesa → drawer lateral derecho

```
┌────────────────────────────┐
│  ✕                          │
│                             │
│  Hola, Rubén                │
│  rreubenleon@gmail.com      │
│  ✦ Premium · activo         │
│                             │
│  ────────────────           │
│                             │
│  📊 Mi Console              │
│     Dashboard personalizado │
│                             │
│  🤖 Terminal IA             │
│     Pregunta lo que sea     │
│                             │
│  👥 Mis legisladores        │
│     5 / 20 seleccionados    │
│                             │
│  🎯 Mis temas               │
│     3 categorías            │
│                             │
│  🔔 Mis alertas             │
│     2 activas               │
│                             │
│  📥 Exports                 │
│     PDFs y Excel            │
│                             │
│  ────────────────           │
│                             │
│  💳 Suscripción             │
│  ⚙️  Configuración          │
│  💬 Soporte                 │
│  🚪 Cerrar sesión           │
│                             │
└────────────────────────────┘
```

### Vista de dashboard post-login

Igual a la pública pero **sin paywall** — todo destapado. El semáforo grid completo, divergencias, mapa, comisiones, etc. **Visualmente idéntico al estado actual** que ya tienes. Lo único nuevo es:

1. Header con badge "✦ Premium" + hamburguesa
2. **Botón flotante de Terminal IA** (siguiente sección)

---

## 5. Terminal IA — la pieza más importante

### Dónde vive

**Botón flotante (FAB) en esquina inferior derecha**, persistente en TODA la app:

```
                                                          ┌──────┐
                                                          │ ✦ AI │
                                                          └──────┘
                                                          (botón circular
                                                           dorado/morado
                                                           con animación
                                                           sutil de pulse)
```

### Click en el FAB → panel lateral derecho

```
┌─────────────────────────────────────────────────────────────────┐
│                                          ┌──────────────────────┤
│  [contenido del dashboard normal,        │  ✦ FIAT Brain    ✕  │
│   sigue visible en izquierda]            ├──────────────────────┤
│                                          │                      │
│                                          │  Pregúntame lo que   │
│                                          │  quieras sobre el    │
│                                          │  Congreso.           │
│                                          │                      │
│                                          │  Ejemplos:           │
│                                          │  · ¿Qué patrón tiene │
│                                          │    Moreira en su     │
│                                          │    carrera?          │
│                                          │  · Cuando hay        │
│                                          │    incendios         │
│                                          │    forestales, ¿quién│
│                                          │    presenta puntos   │
│                                          │    de acuerdo?       │
│                                          │  · Resumen de        │
│                                          │    seguridad esta    │
│                                          │    semana            │
│                                          │                      │
│                                          ├──────────────────────┤
│                                          │  [_________________] │
│                                          │  Pregunta...         │
│                                          │  Análisis profundo ☐ │
│                                          │  [Enviar]            │
│                                          ├──────────────────────┤
│                                          │ 187/200 queries      │
│                                          │ del mes              │
│                                          └──────────────────────┘
└─────────────────────────────────────────────────────────────────┘
```

### Cómo se ve una respuesta

```
┌──────────────────────────────────────────┐
│  Tú:                                     │
│  ¿Cuál es la constante de Rubén Moreira? │
│                                          │
│  ✦ FIAT Brain:                          │
│  Rubén Moreira tiene una constante muy  │
│  marcada en **Justicia y Hacienda      │
│  Pública** a lo largo de sus 3          │
│  legislaturas (LXIV, LXV, LXVI):        │
│                                          │
│  📊 Tema dominante: Justicia            │
│  • LXIV: 32 instrumentos [1]           │
│  • LXV: 27 instrumentos [2]            │
│  • LXVI: en curso, 12 hasta hoy [3]    │
│                                          │
│  📊 Tasa de aprobación histórica: 44.5%│
│  Por encima del promedio de su partido  │
│  (PRI: 38%) [4]                         │
│                                          │
│  ⚖ Patrón único: 87% de sus PA en LXV   │
│  fueron exhortos al Ejecutivo en        │
│  materia federal [5]. Es de los pocos   │
│  legisladores priístas que sostiene     │
│  esta línea desde 2018.                 │
│                                          │
│  Fuentes:                                │
│  [1] ↗ Trayectoria LXIV en SIL         │
│  [2] ↗ Trayectoria LXV en SIL          │
│  [3] ↗ Actividad LXVI                  │
│  [4] ↗ Comparativa partidos            │
│  [5] ↗ Análisis sub-clasificación      │
│                                          │
│  [👍] [👎] [📋 Copiar] [📤 Exportar]     │
└──────────────────────────────────────────┘
```

Cada `[N]` es un link clicable que abre el modal correspondiente (perfil del legislador, instrumento específico, etc.).

### Cómo funciona técnicamente

**Stack**:
- Frontend: chat UI en React (puede ser componente custom o reutilizar lib como `react-chatbot-kit`, mejor custom)
- Backend: endpoint `/ai/terminal` en Worker
- Modelo: Claude Haiku 4.5 default, Sonnet 4.5 si checkbox "Análisis profundo" marcado

**Pipeline de una pregunta**:

```
[Usuario envía pregunta]
       ↓
[Worker /ai/terminal recibe]
       ↓
[Auth check: user tiene sub activa? 401 si no]
[Rate limit: 10/min, cap 200/mes ya consumido?]
       ↓
[System prompt con contexto FIAT (cacheado en Anthropic):
   - Schema de tablas D1 legibles
   - Lista de 19 categorías
   - Lista de funciones SQL disponibles tipo "tool use"
   - Instrucciones de citar TODO con [N] referenciando IDs reales
   - Ejemplos few-shot]
       ↓
[Claude Haiku/Sonnet decide:
   - Genera 1-5 SQL queries que mandar a D1
   - O directamente responde si es pregunta general]
       ↓
[Worker ejecuta queries]
       ↓
[Worker re-llama a Claude con resultados]
       ↓
[Claude formatea respuesta con citas estructuradas]
       ↓
[Worker formatea citas con links del frontend]
       ↓
[Stream de respuesta al frontend (Server-Sent Events o WebSocket)]
       ↓
[Log en ai_queries_log: tokens, costo, modelo, fecha]
```

### Costo de la Terminal IA — cálculo real

**System prompt**: ~6,000 tokens (schema + instrucciones + ejemplos)
**Pregunta promedio**: ~30 tokens
**Resultado SQL promedio**: ~500-1,500 tokens
**Respuesta final**: ~300-500 tokens

**Con prompt caching** (Haiku 4.5):
- Cache read: $0.10/M tokens × 6,000 = $0.0006 por query
- Input fresh: $1/M × 1,500 = $0.0015
- Output: $5/M × 500 = $0.0025
- **Total Haiku: ~$0.005 USD por query** = ~$0.10 MXN

**Sin caching (Sonnet "análisis profundo")**:
- Input: $3/M × 7,000 = $0.021
- Output: $15/M × 1,000 = $0.015
- **Total Sonnet: ~$0.036 USD por query** = ~$0.72 MXN

**Mix esperado por usuario** (200 queries/mes incluidas):
- 80% Haiku × $0.005 = $0.80 USD
- 20% Sonnet × $0.036 = $1.44 USD
- **Total: ~$2.24 USD/mes/usuario** = ~$45 MXN

Cómodamente dentro del costo unitario que proyecté.

**Hard caps anti-runaway**:
- Por usuario: 200 queries/mes incluidas. Después: 10 más por $50 MXN, o cap.
- Por minuto: 10 queries/min (humano normal no excede)
- Por sistema: si el costo total del mes excede $200 USD (40+ usuarios saturando), alerta y throttle

### Stats cruzadas — los dos tipos importantes

**Tipo A: "Constante de un legislador"** (barato, cacheable)

Pregunta: "¿Cuál es la constante de X?"

Backend:
```sql
-- Tema dominante histórico
SELECT tema, COUNT(*) as n
FROM (
  SELECT tema FROM sil_documentos_historicos WHERE legislador_id = ?
  UNION ALL
  SELECT tema FROM sil_documentos
    WHERE id IN (SELECT sil_documento_id FROM actividad_legislador WHERE legislador_id = ?)
) GROUP BY tema ORDER BY n DESC LIMIT 5;

-- Tasa de aprobación cruzada
SELECT
  SUM(CASE WHEN estatus LIKE '%aprobado%' OR estatus LIKE '%publicado%' THEN 1 ELSE 0 END) as aprobados,
  SUM(CASE WHEN estatus LIKE '%desechado%' THEN 1 ELSE 0 END) as desechados
FROM sil_documentos_historicos WHERE legislador_id = ?;

-- Sub-clasificaciones únicas (su "firma")
SELECT sub_clasificacion, COUNT(*) as n
FROM sil_documentos_historicos WHERE legislador_id = ?
GROUP BY sub_clasificacion ORDER BY n DESC LIMIT 5;
```

**Costo computacional**: ~3 queries D1, total ~5ms. **Cache 24h** por legislador.
**Costo total**: $0 (D1 free tier sobra). El cache evita re-cómputos.

---

**Tipo B: "Cuando pasa X, quién responde?"** (caro, pre-computado)

Pregunta: "Cuando hay incendios forestales, ¿qué legisladores presentan PA?"

**Estrategia: pre-computar mensualmente** los temas top y guardar resultados.

Cron mensual (`scripts/calcular_responsivos.py`):
1. Lee top 50 keywords/temas de los últimos 90 días en `articulos`
2. Por cada tema, identifica eventos (clusters de artículos en el mismo periodo)
3. Por cada evento, mira instrumentos SIL en ventana [evento, evento+14d] que matchean tema
4. Agrupa por legislador → tabla `responsivos_temas`

```sql
CREATE TABLE responsivos_temas (
  id INTEGER PRIMARY KEY,
  tema TEXT NOT NULL,                  -- 'incendio_forestal'
  legislador_id INTEGER NOT NULL,
  n_eventos INTEGER,                   -- en cuántos eventos respondió
  n_instrumentos INTEGER,              -- cuántos instrumentos presentó relacionados
  promedio_dias INTEGER,               -- cuántos días tras el evento responde
  ultima_actualizacion TEXT
);
```

Cuando user pregunta vía Terminal IA, query es **trivial**:
```sql
SELECT l.nombre, r.n_eventos, r.n_instrumentos, r.promedio_dias
FROM responsivos_temas r
JOIN legisladores l ON l.id = r.legislador_id
WHERE r.tema = ? ORDER BY r.n_instrumentos DESC LIMIT 20;
```

**Costo computacional pre-cómputo mensual**: ~10 minutos de procesamiento, $0 (corre en GitHub Actions)
**Costo de query**: ~5ms

### Programas/servicios de los que dependemos

| Servicio | Para qué | Costo |
|---|---|---|
| **Anthropic API** | Terminal IA (Haiku + Sonnet) | $50-200/mes según uso |
| **Cloudflare D1** | BD principal (queries de IA y dashboard) | $0 free tier |
| **Cloudflare Workers** | Backend de auth, IA, todo | $0-5/mes |
| **Cloudflare Pages** | Frontend hosting | $0 |
| **Cloudflare R2** | Backups + exports PDF/Excel | $0-5/mes |
| **Stripe** | Pagos | 3.6% + $3 por transacción |
| **Resend** | Emails (verificación, alertas) | $0-20/mes |
| **Google OAuth** | Login social | $0 |
| **GitHub Actions** | Pipeline + crons | $0 |
| **Facturapi** | CFDI mexicano (V2) | $10/mes |

**Total estimado mes 1 (10 usuarios)**: ~$80 USD/mes
**Total estimado mes 6 (100 usuarios)**: ~$300-500 USD/mes

---

## 6. Configuración de dashboard (pick de legisladores)

### Acceso desde la hamburguesa → "Mis legisladores"

```
┌──────────────────────────────────────────────────────────┐
│  ← Atrás                              Mis legisladores  │
├──────────────────────────────────────────────────────────┤
│                                                           │
│  Selecciona hasta 20 legisladores para monitorear        │
│  Recibirás alertas cuando presenten algo nuevo o cuando  │
│  haya menciones relevantes en medios.                    │
│                                                           │
│  Buscar: [_________________]    Cámara ▾  Partido ▾      │
│                                                           │
│  Seleccionados: 5 / 20                                   │
│  ─────────────────                                        │
│  ✓ Rubén Moreira (PRI · Diputados)             [✕]      │
│  ✓ Manuel Añorve (PRI · Senado)                [✕]      │
│  ✓ Ricardo Monreal (Morena · Diputados)        [✕]      │
│  ✓ Lilly Téllez (PAN · Senado)                 [✕]      │
│  ✓ Ivonne Ortega (MC · Diputados)              [✕]      │
│                                                           │
│  Resultados de búsqueda:                                 │
│  ─────────────────                                        │
│  ☐ Alejandro Moreno (PRI · Senado)             [+]      │
│  ☐ Carolina Viggiano (PRI · Senado)            [+]      │
│  ☐ Higinio Martínez (Morena · Senado)          [+]      │
│  ...                                                      │
│                                                           │
└──────────────────────────────────────────────────────────┘
```

### Después de seleccionar — vista en /console

```
┌──────────────────────────────────────────────────────────┐
│  Mi Console                                              │
├──────────────────────────────────────────────────────────┤
│                                                           │
│  ┌──────────────────┐  ┌──────────────────┐              │
│  │ Mis legisladores │  │ Mis temas        │              │
│  │                  │  │                  │              │
│  │ 5 monitoreados   │  │ Seguridad ▲ 87   │              │
│  │ 2 con actividad  │  │ Educación ▼ 71   │              │
│  │ esta semana      │  │ Justicia → 65    │              │
│  └──────────────────┘  └──────────────────┘              │
│                                                           │
│  ┌────────────────────────────────────────────────┐      │
│  │ Actividad de mis legisladores (últimos 7 días) │      │
│  │                                                │      │
│  │ • R. Moreira presentó 2 PA en Justicia [→]   │      │
│  │ • M. Añorve participó en sesión 4/22 [→]     │      │
│  │ • R. Monreal mencionado en Universal x3 [→]  │      │
│  │ ... etc                                        │      │
│  └────────────────────────────────────────────────┘      │
│                                                           │
│  ┌────────────────────────────────────────────────┐      │
│  │ Mis alertas                                    │      │
│  │                                                │      │
│  │ 🔔 "Reforma electoral score >75"               │      │
│  │    Última disparada hace 2 días                │      │
│  │                                                │      │
│  │ 🔔 "Ricardo Monreal presenta en seguridad"    │      │
│  │    Sin disparar este mes                       │      │
│  │                                                │      │
│  │ [+ Crear alerta]                              │      │
│  └────────────────────────────────────────────────┘      │
│                                                           │
└──────────────────────────────────────────────────────────┘
```

---

## 7. Listado de cambios visuales/técnicos al `dashboard/index.html`

Para implementar todo lo anterior, el `dashboard/index.html` actual (5,330 líneas) recibe estos cambios:

| Sección | Cambio | Aprox líneas nuevas |
|---|---|---|
| Header | Agregar `<UserBadge />` + `<HamburgerMenu />` | +80 |
| Después de HistorialScoresChart | Agregar `<PaywallGate />` que envuelve todo lo siguiente | +120 |
| Footer / floating | Agregar `<TerminalIAFAB />` + `<TerminalIAPanel />` | +250 |
| Nueva ruta `/login` | `<Login />`, `<Register />`, `<ResetPassword />` | +200 |
| Nueva ruta `/console` | `<Console />` con widgets | +300 |
| Nueva ruta `/console/legisladores` | `<MisLegisladores />` | +150 |
| Nueva ruta `/console/alertas` | `<MisAlertas />` | +150 |
| Nueva ruta `/pricing` | `<Pricing />` (cuando se hace click en paywall) | +80 |
| Nueva ruta `/cuenta` | `<MiCuenta />` (suscripción, cancelar) | +100 |
| Hooks de auth | `useAuth()`, `useSubscription()`, etc. | +100 |
| **Total** | | **+1,530 líneas** |

El archivo crece de 5,330 a ~6,860 líneas. Sigue siendo manejable single-file (mantenemos consistencia).

**Alternativa**: dividir en archivos separados con `<script type="module">`. Más escalable pero rompe el patrón actual. **Recomiendo mantener single-file** por ahora.

---

## 8. Roadmap de implementación visual (orden sugerido)

Antes de tocar código, **necesito que apruebes este diseño visual**. Cuando apruebes:

### Sprint 1 (semana 1-2): Lo que el visitante ve
1. Paywall visual con degradado en la página actual
2. Bloque de pricing $699/mes y $7,699/año
3. Página `/pricing` con detalle completo
4. Sin login todavía — solo el corte visual

### Sprint 2 (semana 3): Auth
1. Schema D1 usuarios + sesiones
2. `/auth/registrar`, `/auth/login`, `/auth/google/callback`
3. UI de Login/Register con Google + email/password
4. Detección de correos institucionales con banner

### Sprint 3 (semana 4): Pago
1. Stripe sandbox
2. `/pago/checkout`, `/pago/webhook`
3. Estados: trial 48h → paid → cancel
4. Email transaccionales (Resend)

### Sprint 4 (semana 5): Console y configuración
1. Header con `<UserBadge />` + `<HamburgerMenu />`
2. `/console` con widgets básicos
3. `/console/legisladores` (pick)
4. Pre-cómputo de "responsivos" en cron mensual

### Sprint 5 (semana 6): Terminal IA
1. Backend `/ai/terminal` con Haiku 4.5 + caching
2. UI `<TerminalIAFAB />` + `<TerminalIAPanel />`
3. Logs y rate limits
4. Sistema de citas con links clicables

### Sprint 6 (semana 7+): Alertas, exports, polish
1. Sistema de alertas configurables
2. Exports PDF/Excel
3. Onboarding tour
4. QA exhaustivo

---

## 9. Lo que necesito que decidas antes de programar

1. **¿El degradado del paywall comienza exactamente donde "Ordenar por..."?** ✓ confirmado
2. **¿La hamburguesa abre drawer derecho o dropdown?** Recomiendo drawer derecho (más espacio para info)
3. **¿La terminal IA es FAB persistente o tab del header?** Recomiendo FAB
4. **¿Permitimos sign up con Google solo o también email/password?** Recomiendo ambos
5. **Tarifa institucional: ¿automática o aprobación manual?** Recomiendo manual al inicio (anti-fraude), automatizar después
6. **¿Trial 48h da acceso completo o limitado (ej. 10 queries IA)?** Recomiendo completo pero con cap de queries (50 en 48h) para limitar abuso
7. **¿Qué pasa al final del trial sin pago: pierde acceso inmediato o degradado a free durante 7 días?** Recomiendo inmediato (sentido de urgencia)
8. **Pre-cómputo de "responsivos por tema": ¿lo hacemos para los 50 temas top o solo bajo demanda del usuario?** Recomiendo top 50 mensual (mejor UX, costo ~10 min de cron)
