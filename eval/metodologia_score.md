# Metodología del Score FIAT (Semáforo Legislativo)

> ⚠️ **DOCUMENTO HISTÓRICO — NO ES EL ESTADO ACTUAL. NO CITAR COMO VERDAD VIVA.**
>
> Retrata el sistema en **junio-2026**. Partes ya NO son ciertas. Verificado
> el 18-jul-2026 contra la BD viva (`db-latest`):
>
> - ❌ **"score_legisladores = 0 en todas las categorías" es FALSO hoy.** Ese
>   componente está vivo desde jun-2026: hoy vale 38.7–69.0 por categoría y sí
>   pondera (10%). Se comprobó recalculando las 19 categorías desde sus
>   componentes: el `score_total` guardado solo coincide con la versión que
>   INCLUYE Legisladores.
> - ⚠️ El número del score ya **no se llama "score"** de cara al usuario: es
>   **Índice de Actividad** (0-100, sin "%"). No es una probabilidad.
>
> **Regla para cualquiera (humano o agente) que lea esto:** este archivo sirve
> para entender el DISEÑO y el historial de auditoría. Para afirmar cualquier
> NÚMERO hay que consultar la fuente viva (`db-latest`, `data.json`, el worker
> o la fuente oficial), nunca este documento. Un hallazgo citado de aquí es una
> PISTA, no un hecho verificado.

> Documento de referencia para buscar literatura. Describe EXACTAMENTE lo que
> corre hoy (código), no lo idealizado. Fuentes: `config.py`, `scrapers/*.py`,
> `api/correlacion.py`, `backfill_scores.py`. Generado tras auditoría jun-2026.

## Qué es, en una línea
Un **índice compuesto** (composite indicator) por categoría temática: combina 7
señales normalizadas a 0-100 mediante una **suma lineal ponderada**, sobre
ventanas móviles, recalculado a diario. El resultado 0-100 se umbraliza en un
semáforo (verde/amarillo/rojo).

## Fórmula
```
SCORE_total = 0.20·Media + 0.15·Trends + 0.25·Congreso + 0.10·Mañanera
            + 0.15·Urgencia + 0.05·Dominancia + 0.10·Legisladores
            (cap a 100)
```
Pesos en `config.py:SCORING`. Suman 1.0. (El comentario `config.py:1406` está
desactualizado: dice dominancia 0.15 y omite legisladores — el código real es el
de arriba.)

## Los 7 componentes (cada uno 0-100)

| Componente | Ventana | Fuente | Cálculo (resumen) |
|---|---|---|---|
| **Media** (0.20) | 7 d | `scrapers/medios.py` | 4 subfactores: Volumen/Share 40% + Concentración temporal 20% + Diversidad de medios 20% + Streak 20% |
| **Trends** (0.15) | 7 d | `scrapers/trends.py` | Google Trends MX (`now 7-d`), interés de búsqueda |
| **Congreso** (0.25) | 7 d | `scrapers/gaceta.py` | proporción de docs de gaceta relevantes vs total |
| **Mañanera** (0.10) | 14 d | `scrapers/mananera.py` | menciones del tema en conferencia presidencial |
| **Urgencia** (0.15) | 14/60 d | SIL | aceleración de actividad SIL (70%) + factor calendario (30%) |
| **Dominancia** (0.05) | 30 d | `api/correlacion.py` | dominancia discursiva del tema |
| **Legisladores** (0.10) | — | `api/predictor_autoria.py` | promedio del score de los top-5 legisladores con más probabilidad de presentar instrumento |

### Normalización (clave para la literatura)
Cada componente se "aplasta" a 0-100 con funciones que **saturan**:
- **Media / Volumen**: `vol_score = 50 + 50·tanh( ln(share / share_esperado) / 1.5 )`.
  Para una categoría dominante (share alto) → tanh→1 → **100**. Sumado a Streak
  (`min(días_con_cobertura·15, 100)` → 100 con ≥7 días) y Concentración, una
  categoría con cobertura diaria fuerte **se clava en 100**.
- **Congreso**: `ratio = min( docs_relevantes / (total_docs·0.1), 1.0 )·100`.
  Cualquier categoría con ≥10% de los documentos **se clava en 100**.
- **Dominancia** y **Urgencia**: también con techos `min(x, 100)`.

### Umbrales (`config.py:SCORING.umbrales`)
- **Verde** ≥ 70 · **Amarillo** 40–69 · **Rojo** < 40

### Cadencia
Recalculado en cada corrida del pipeline (cada 4 h). Para la gráfica de tendencia
se promedia a un punto diario por categoría (`scores.score_total`), ventana de 60
días para el chart.

---

## Problemas detectados en la auditoría (jun-2026) — IMPORTANTE
Estos explican la "planitud" que se observa y son lo que conviene contrastar con
la literatura:

1. **Saturación de techo (ceiling effect).** En los temas top, **Media, Congreso
   y Dominancia están los tres pegados a 100** (verificado en `data.json`). Eso es
   0.20+0.25+0.05 = **0.50 del peso convertido en constante**. El score de los
   temas líderes queda estructuralmente ~70 y solo se mueve por los componentes
   chicos (Trends/Urgencia/Mañanera). → la tendencia se ve plana por diseño de la
   normalización, no porque "no pase nada".

2. **Componente muerto.** `score_legisladores` (0.10) = **0 en todas las
   categorías** porque `predecir_autores()` devuelve vacío. El 10% del peso no
   aporta señal y deprime todos los scores por una constante.

3. **Frescura de fuentes volátiles.** En la copia de trabajo, `trends` (peso
   0.15) y `tweets` (alimentan boost de Media) estaban congelados desde abril.
   Pendiente confirmar si es solo la copia o el sistema. Si están muertos, dos de
   las señales más móviles no entran → más planitud.

---

## Marco académico / términos para buscar literatura
- **Composite indicators / índices compuestos**: OECD/JRC *Handbook on
  Constructing Composite Indicators* (normalización min-max vs z-score vs
  ranking; ponderación equal-weight vs experto vs PCA/data-driven; agregación
  lineal vs geométrica).
- **Ceiling / floor effects** y **pérdida de poder discriminante** por saturación
  (por qué tanh/caps aplanan la serie temporal de los líderes).
- **Agenda-setting**: McCombs & Shaw (saliencia mediática → agenda pública);
  Baumgartner & Jones, *punctuated equilibrium* en agendas de políticas.
- **Attention indices / nowcasting** con Google Trends (Choi & Varian).
- **Signal aggregation / sensor fusion**: combinar señales de distinta varianza
  (una señal saturada a 100 con varianza ~0 domina/ahoga a las demás en una suma
  lineal de peso fijo).

## Hipótesis de mejora a evaluar (no implementadas)
- Normalización **relativa-en-el-tiempo** (z-score o percentil sobre ventana
  móvil por categoría) en lugar de techos absolutos → recupera variación.
- **Ponderación sensible a varianza** (que una señal saturada pese menos).
- Revivir `score_legisladores` o redistribuir su 0.10.

---

## Vinculación evento↔legislación (CORRECCIÓN CLAVE, jul-2026)

El análisis por categoría (arriba) concluía erróneamente que media "no
predice/atribuye" (~azar). **Era artefacto** de (1) clasificar en 19 categorías
amplias que fragmentan un mismo evento en 6 buckets, (2) misclasificación
(un exhorto sobre un rector detenido caía en "trabajo"), (3) usar pocas fuentes.

Medido **a nivel evento/entidad** (matcher `scripts/matcher_evento.py`: términos
distintivos compartidos + stem + IDF, cruzando categorías) y con **las 22 fuentes**
de `articulos`, el matcher léxico reportó 86% de instrumentos con precedente
(vs 21% placebo) y lead mediana 13d.

### ⚠️ RETRACTACIÓN (2026-07-02): el 86% estaba INFLADO
El usuario etiquetó a ciegas 60 casos estratificados (eval set GOLD,
`eval/matcher_eval_set.json`). Resultado contra su criterio:

- **Matcher v2 (evento/entidad): precisión 30%** (11/37), recall 79%.
- **Modelo viejo (subcat-keyword, el de reactividad en producción): precisión 12%** (3/24).
- Ambos **pierden eventos reales** (Abud, despidos GM Ramos Arizpe, GNL Saguaro):
  27% de falsos negativos en el estrato "ambos_no".
- **Tasa real estimada de PPAs que responden a un evento: ~28%** (ponderada por
  estrato), NO 86%. El matching léxico laxo contaba solapamientos temáticos/
  institucionales como "mismo evento".

Lo que sí sobrevive: (a) el vínculo evento→legislación EXISTE y es demostrable en
casos fuertes (derrame del Golfo, Abud, Choapas, PACIC, tribunal agrario dist. 16);
(b) v2 > viejo (30% vs 12%); (c) el criterio correcto es "MISMO evento específico",
no solapamiento temático — estándar fijado por el etiquetado del usuario.

**REGLA DE ADOPCIÓN:** ningún matcher entra a reactividad/producción sin
precisión≥90% Y recall≥90% contra el eval set GOLD del usuario
(`scripts/test_matcher_eval.py`; el set solo crece). Estado: ❌ 30%/79%.

Pipeline `scripts/vincular_eventos.py` → tabla `evento_vinculos`: sus conteos
heredan la inflación del matcher léxico — NO usar/publicar hasta pasar el umbral.
Siguiente iteración: capa de verificación semántica sobre los candidatos léxicos
(validar juez-Haiku contra las 60 etiquetas del usuario antes de escalar).

---

## Tendencia por tema: veredicto sobre "probabilidad de presentación" (10-jul-2026)

Se backtestearon DOS formulaciones probabilísticas para reemplazar la gráfica
de Tendencia, con 22 meses de historia y el corpus completo (22 nacionales +
96 regionales clasificados por tema):

1. **P(≥1 instrumento del tema en 7 días)** — tasa base 82% (casi siempre sí
   en temas con volumen): Brier 0.159 vs 0.147 sin-modelo. PEOR que nada, no
   calibra (bin 50-60% → 88% observado). La curva viviría pegada arriba.
2. **P(semana ATÍPICA del tema)** (próx. 7d > 1.3× su semana típica) — base
   29%: Brier 0.206 vs 0.205. Sin poder discriminante; z mediático nac=0.0,
   reg=0.11.

**Conclusión metodológica:** a nivel tema-semana el Congreso NO es
pronosticable con ritmo+calendario+medios — los arranques son decisiones
internas de actores (ej. paquete IA: 22 iniciativas de un senador en un día).
La atribución retrospectiva (vínculos evento→instrumento) es real; la
PREDICCIÓN agregada por tema, no. Coincide con el hallazgo de jun-2026
(predictor_probabilidad ~44% test). NO publicar probabilidades por tema.

**Propuesta en pie (aritmética, sin modelo, no falseable):** "observado vs
esperado" — instrumentos/semana del tema vs rango típico P25-P75 móvil de 8
semanas, con semanas de pique marcadas. Prototipo con datos reales entregado
al usuario para decisión. Alternativa/complemento: normalización percentil del
score_congreso (simulada 8-jul: 11/12 categorías recuperan varianza).
