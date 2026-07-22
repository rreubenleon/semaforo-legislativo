# Consola personalizada — guía de arquitectura y estilo para portar a otro proyecto

Documento de referencia extraído del código real de FIAT (`dashboard/index.html`,
10,286 líneas) el 21-jul-2026. Cada afirmación aquí está leída del archivo, con
el número de línea donde vive. El objetivo es que puedas recrear el mismo patrón
en un proyecto de monitoreo ambiental sin tener que leer el original.

---

## 1. La idea en una frase

**Un solo `data.json` público con TODO el universo de datos, y una configuración
por usuario (5 listas de IDs) que actúa como filtro en el cliente.**

No hay endpoints por usuario, no hay tablas por usuario, no hay render en
servidor. La "personalización" es intersección de conjuntos en el navegador. Eso
la hace baratísima de operar (costo marginal por usuario ≈ 0) y trivial de
portar.

```
data.json (universo completo, mismo para todos)
        +
config del usuario  { legisladores[], temas[], estados[], comisiones[], alertas[] }
        =
consola personalizada (filtrado 100% client-side)
```

En tu proyecto ambiental el mapeo directo sería:

| FIAT | Ambiental (equivalente) |
|---|---|
| legisladores | estaciones de monitoreo / sensores |
| temas (categorías) | contaminantes o parámetros (PM2.5, O₃, ruido…) |
| estados | cuencas / municipios / zonas |
| comisiones | plantas, sitios industriales, permisos |
| alertas | umbrales de excedencia normativa |

---

## 2. Configuración por usuario — el corazón del patrón

### Forma del objeto
Cinco arreglos, nada más (línea 250):

```js
{ legisladores: [], temas: [], estados: [], comisiones: [], alertas: [] }
```

Los primeros cuatro son **arreglos de IDs o claves**, no objetos. `alertas` sí
guarda objetos (reglas). Esa simplicidad es deliberada: la config pesa <2 KB y
se serializa a JSON sin ceremonia.

### Dónde vive: localStorage, con clave por usuario
Líneas 242-252. La clave se **sufija con el user id** de Supabase:

```js
const _CONSOLA_CFG_BASE = 'fiat_consola_pro_config_v3';
function consolaConfigKey(user) {
    const uid = user && user.id ? user.id : null;
    return uid ? `${_CONSOLA_CFG_BASE}::${uid}` : _CONSOLA_CFG_BASE;
}
function loadConsolaConfig(user) {
    try {
        const c = JSON.parse(localStorage.getItem(consolaConfigKey(user))) || {};
        return { legisladores: [], temas: [], estados: [], comisiones: [], alertas: [], ...c };
    } catch { return { legisladores: [], temas: [], estados: [], comisiones: [], alertas: [] }; }
}
```

**Tres detalles que valen oro y que salieron de bugs reales:**

1. **El sufijo `::${uid}`** existe porque sin él dos cuentas en el mismo
   navegador se pisaban la configuración. Usuario nuevo o anónimo → config vacía.
2. **El spread `...c` sobre los defaults** hace que agregar una categoría nueva
   más adelante no rompa las configs ya guardadas: las viejas simplemente traen
   el arreglo nuevo vacío. Migración gratis.
3. **El `v3` en el nombre de la clave** permite invalidar todas las configs de
   golpe si cambias el esquema de forma incompatible.

### Cómo se guarda (líneas 9937-9947)
Estado en React + escritura a localStorage en el mismo setter, y **recarga al
cambiar de usuario**:

```js
const [consolaConfig, setConsolaConfigState] = useState(() => loadConsolaConfig(null));

useEffect(() => {                       // cambia de usuario → recarga su config
    setConsolaConfigState(loadConsolaConfig(authUser));
}, [authUser ? authUser.id : null]);

const setConsolaConfig = (next) => {
    setConsolaConfigState(next);
    try { localStorage.setItem(consolaConfigKey(authUser), JSON.stringify(next)); } catch {}
};
```

El `try/catch` alrededor del `setItem` no es paranoia: en modo privado de Safari
`localStorage` lanza excepción y sin él se cae toda la app.

### Detectar si ya configuró (línea 255)
```js
function consolaConfigurada(user) {
    const c = loadConsolaConfig(user);
    return ((c.legisladores||[]).length + (c.temas||[]).length + (c.estados||[]).length) > 0;
}
```
Sirve para decidir entre mostrar el wizard de bienvenida o la consola.

### ⚠️ Limitación honesta de este diseño
La config **no viaja entre dispositivos** — vive en el navegador. Si en tu
proyecto ambiental necesitas que un técnico vea su consola desde el celular y la
laptop, hay que persistirla en el backend. La migración es directa: misma forma
de objeto, guardado en una tabla `user_config (user_id, config_json)` y
`loadConsolaConfig` se vuelve `async`. Todo lo demás del patrón no cambia.

---

## 3. El wizard de configuración

`ConsolaProConfigModal` (línea 7282). **Cinco pasos**, cada uno alimenta un
arreglo de la config:

```js
const PASOS = ['legisladores', 'temas', 'estados', 'comisiones', 'perfil'];
const PASOS_LABEL = {
    legisladores: 'Legisladores',  temas: 'Temas',  estados: 'Estados',
    comisiones: 'Comisiones',      perfil: 'Afinar',
};
```

Reglas de interacción que funcionaron bien:

- **Cada paso exige ≥1 selección** para poder avanzar. Evita consolas vacías.
- **Tope de selección** (20 legisladores, línea 7300): `if (set.size < 20) set.add(id)`.
  Sin tope, la consola se vuelve ilegible y lenta.
- El botón "Siguiente" se convierte en **"Listo"** en el último paso.
- Búsqueda por paso (`search`, `searchCom`) y orden configurable
  (`ordenCom: 'nombre' | 'asuntos'`).

### Dos componentes reutilizables que conviene copiar tal cual

**`ChipInput`** (línea 7205) — entrada de palabras clave libres. El usuario
teclea y con `Enter` o coma se crea un chip; `Backspace` con el campo vacío borra
el último; click en `×` quita uno.

> **Trampa crítica documentada en el propio código:** debe definirse a **nivel de
> módulo**, no dentro del render del padre. Si se define adentro, React lo
> considera un componente nuevo en cada render, remonta el `<input>` y **pierde
> el foco tras cada letra**. Es un bug desesperante y sutil.

**`RiesgoTemasSelector`** (línea 7245) — toma los temas que el usuario ya eligió
en el paso anterior y le deja marcar nivel Alto/Medio/Bajo con toggles de
semáforo. Es un patrón muy portable: **reusar la selección del paso previo como
insumo del siguiente**, en vez de pedir todo otra vez. Si no hay temas elegidos
muestra un estado vacío explicativo en vez de un control roto.

Para el proyecto ambiental: `RiesgoTemasSelector` mapea casi 1:1 a "nivel de
criticidad por parámetro" (p. ej. PM2.5 = Alto, ruido = Bajo).

---

## 4. De dónde salen los datos

Tres fuentes, con criterios distintos de carga:

| Fuente | Qué trae | Cuándo se carga |
|---|---|---|
| `data.json` | universo completo: series, mapa, categorías, fuentes | al abrir la app, siempre |
| Worker HTTP | catálogos grandes y consultas | **solo con sesión iniciada** |
| `ultimas_instrumentos.json` | detalle pesado (~1.1 MB) | diferido, solo dentro de la consola |

### Carga diferida — la regla de oro (líneas 8282-8290)

```js
const [ultimasInst, setUltimasInst] = useState(null);
useEffect(() => {
    let cancel = false;
    fetch('ultimas_instrumentos.json').then(r => r.ok ? r.json() : {})
      .then(d => { if (!cancel) setUltimasInst(d); }).catch(() => { if (!cancel) setUltimasInst({}); });
    return () => { cancel = true; };
}, []);
```

Ese bloque de ~1.1 MB **se sacó de `data.json` a propósito** para no penalizar a
los visitantes anónimos que nunca abren la consola. El flag `cancel` evita el
warning de "setState en componente desmontado".

### Catálogos solo con sesión (líneas 9967-9989)
El fetch de catálogos está condicionado a `if (!authUser || !data) return;` —
literalmente comentado como *"no cargar costo en visitantes anónimos"*. Además
**pagina de 100 en 100**: primero pide una página, lee el `total`, y si hay más
dispara el resto en paralelo con `Promise.all`.

**Principio portable:** el usuario anónimo paga solo el `data.json`; todo lo caro
se activa al iniciar sesión.

---

## 5. Cómo se filtra (el mecanismo real de personalización)

Es sorprendentemente simple. Al inicio de `PageConsolePro` (líneas 8314-8320) se
convierten los arreglos de config en `Set` y con eso se filtra todo:

```js
const temasSelectos      = new Set(config.temas);
const estadosSelectos    = new Set(config.estados);
const comisionesSelectos = new Set(config.comisiones || []);
const legisladoresIds    = new Set(config.legisladores);

const legisladoresEnConfig = allLegisladores.filter(l => legisladoresIds.has(l.id));
const temasEnConfig        = semaforo.filter(s => temasSelectos.has(s.categoria));

const estadosFiltradosArr = Array.from(estadosSelectos)
    .map(key => [key, mapa[key]])
    .filter(([, info]) => info);
```

`Set` en vez de `Array.includes` porque el filtrado corre en cada render sobre
listas de cientos de elementos.

Fíjate en el `.filter(([, info]) => info)` del final: **descarta claves de config
que ya no existen en los datos**. Si un usuario guardó un estado y esa clave
desaparece del universo, la consola no truena. Vale la pena replicar esa defensa
en cada filtro.

---

## 6. Los paneles y su orden

Orden real dentro de `PageConsolePro` (línea 8280 en adelante):

1. **Tendencia de mis temas** — `ConsolaProTendencia` (8912): Chart.js sobre el
   historial filtrado a los temas del usuario.
2. **Panel de partidos compacto** — `ConsolaPartidosCompacto` (8206).
3. **Mis estados** — `MapaMexico` limitado a los estados elegidos, con panel de
   DETALLE al hacer clic. Trae filtros de periodo (`1w | 2w | 1m`), paginación y
   tres vistas de mapa (`calor | red | sat`).
4. **Menciones de la autoridad** — filtrado por `filtroCSP`.
5. **Comité Watch** — `ComiteWatch` (9056): tabla full-width con sparkline de
   actividad a 6 meses, próxima sesión y Δ30d.
6. **Mis legisladores** + **Actividad reciente** — con pestañas
   (`legisladores | medios | todo`), búsqueda y un toggle `soloKeywords` que
   filtra a las palabras clave del usuario.
7. **Trigger Desk** — `TriggerDesk` (9422): alertas configurables.

Nota de implementación (línea 8022): la consola usa una **variante propia de la
fila de tabla** (`RadarRowCP`) porque ordena por criterios distintos a la tabla
general. Cuando el mismo dato se muestra en dos contextos con orden distinto,
sale más barato duplicar la fila que parametrizarla hasta lo ilegible.

---

## 7. Trigger Desk — alertas con probabilidad real

Es la pieza más diferenciadora y la que más valor tendría en ambiental
(excedencias normativas). Líneas 9417-9460.

- Reglas **combinables con AND / OR**, cada condición con `{tipo, campo,
  operador, valor}`.
- La probabilidad **no se inventa en el cliente**: se pide al endpoint
  `/probabilidad` del Worker.
- Composición de probabilidades, explícita en el comentario del código:
  - **AND** → `p1 · p2` (producto)
  - **OR** → `1 − (1−p1)(1−p2)`
  - ⚠️ *asume independencia entre condiciones* — está anotado en el código como
    supuesto, no como verdad. Si tus parámetros ambientales están correlacionados
    (PM2.5 y PM10 casi siempre lo están), esta fórmula **subestima o sobreestima**
    y conviene documentarlo igual de explícito.
- Cache local de resultados por alerta: `probs[alerta.id] = {prob, confianza,
  descripcion, loading}`.

En el backend, ese endpoint calcula la probabilidad por frecuencia empírica sobre
ventana móvil (365 d) para umbrales de serie, y con **Poisson `P(≥1) = 1 − e^(−λ)`**
para conteos de eventos. Ambos métodos son directamente aplicables a excedencias
ambientales.

**Estado real, para que no te sorprenda:** el envío por correo **no está
activo** — las alertas se crean y guardan, pero la notificación está marcada en
el código como "siguiente versión". Si lo portas, ese es trabajo pendiente, no
heredado.

---

## 8. Estilo visual

### Tokens (líneas 26-45)

```js
fontFamily: {
    sans: ['Inter', 'system-ui', 'sans-serif'],
    mono: ['JetBrains Mono', 'ui-monospace', 'monospace'],
},
colors: {
    semaforo: { verde: '#2e7d32', amarillo: '#c6972b', rojo: '#c62828' },
    fondo:   '#fafafa',
    tarjeta: '#ffffff',
    borde:   '#d1d5db',
    acento:  '#1a365d',
}
```

```css
body  { background: #fafafa; font-family: 'Inter', system-ui, sans-serif; }
.glass { background: #fff; border: 1px solid #e5e7eb; box-shadow: 0 1px 2px rgba(0,0,0,.04); }
.glass:hover { box-shadow: 0 2px 8px rgba(0,0,0,.06); }
```

### Reglas de uso que dan la personalidad

- **Dos tipografías con roles estrictos:** Inter para texto; **JetBrains Mono
  para todo número, etiqueta técnica, fecha y encabezado de tabla.** Es lo que
  hace que se vea "instrumento de medición" y no "página web". Es la regla de
  estilo más rentable de copiar.
- **Etiquetas** en `uppercase tracking-wider`, tamaños `9-11px`, color gris medio.
- **Escala tipográfica muy chica y deliberada:** `text-[9px]` a `text-[13px]` en
  la interfaz; los números grandes destacan por contraste, no por saturación.
- **Semáforo de tres colores reservado para estado**, nunca decorativo. En
  ambiental mapea directo a los umbrales de la norma.
- **Superficies planas:** blanco sobre `#fafafa`, borde de 1 px, sombra casi
  imperceptible. Sin gradientes ni sombras marcadas.
- **Densidad alta:** paddings `p-2.5`, gaps `gap-1.5`. Es un panel operativo, no
  una landing.
- **`text-acento` (#1a365d)** para lo interactivo y el orden activo de tablas.

---

## 9. Ruteo y acceso

- Ruta limpia `/consola`, mapeada con `PATH_TO_TAB` / `TAB_TO_PATH`.
- **No aparece en la navegación principal**: se entra desde el menú de cuenta o
  desde el wizard. Es un espacio privado, no una pestaña más.
- Requiere sesión (Supabase). Sin sesión, los catálogos ni se piden.

---

## 10. Checklist para portarlo a monitoreo ambiental

1. Define tus **4 ejes de personalización** (sugerido: estaciones, parámetros,
   zonas, sitios) y deja `alertas` como quinto.
2. Publica **un `data.json` con el universo completo** + un JSON pesado aparte
   con el detalle, cargado en diferido.
3. Copia el trío `consolaConfigKey` / `loadConsolaConfig` / `consolaConfigurada`
   **incluyendo el sufijo por usuario y el spread sobre defaults**.
4. Wizard de N pasos con tope de selección y "cada paso exige ≥1".
5. `ChipInput` **a nivel de módulo** (o pierdes el foco por tecla).
6. Filtrado con `Set` + `.filter` defensivo contra claves huérfanas.
7. Endpoint de probabilidad en el backend (frecuencia empírica + Poisson);
   documenta el supuesto de independencia en AND/OR.
8. Tokens: Inter + JetBrains Mono con roles estrictos, superficies planas,
   semáforo solo para estado.
9. Si necesitas config multi-dispositivo, persiste en backend desde el día uno:
   misma forma de objeto, `loadConsolaConfig` async.

---

## Archivos de referencia en el original

| Qué | Dónde |
|---|---|
| Config: clave, carga, detección | `dashboard/index.html` 242-260 |
| Estado y persistencia | 9937-9947 |
| Carga de catálogos con sesión | 9967-9989 |
| Wizard | 7194-7282 (`ChipInput` 7205, `RiesgoTemasSelector` 7245) |
| Consola | `PageConsolePro` 8280 |
| Tendencia | `ConsolaProTendencia` 8912 |
| Comité Watch | `ComiteWatch` 9056 |
| Trigger Desk | `TriggerDesk` 9417 |
| Tokens de diseño | 26-50 |
