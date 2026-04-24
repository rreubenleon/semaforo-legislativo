# FIAT — Notas del etiquetado manual (eval set v1)

Documento generado a partir de las notas que Rubén escribió durante el etiquetado
manual de los 100 artículos del eval set. Son el *por qué* detrás del gold label:
criterios de interpretación legislativa que un clasificador keyword-based nunca
va a inferir solo.

**Fuente:** columna `notas` de la tabla `eval_set` (semaforo.db)
**Etiquetador:** Rubén
**Fecha:** 2026-04-09
**Baseline asociado:** `eval/resultados_v1.csv`

---

## 1. Criterio rector — qué cuenta como "legislativamente relevante"

La relevancia legislativa **no** es "¿ya hay una iniciativa sobre esto?". Es:

> *¿Puede generarse una proposición con punto de acuerdo, o alimenta presión
> política que llegue al Congreso?*

Esta definición es más amplia que la que asume el clasificador actual. Un hecho
(crimen, decisión administrativa, evento internacional) puede ser legislativamente
relevante **aunque no haya iniciativa asociada**, siempre que exista la
posibilidad real de que un legislador lo retome en el pleno o en comisión.

### Ejemplos (todos etiquetados como relevantes)

- **#1 — Captura de "El Donas"** (seguridad_justicia)
  > Es importante porque se pueden presentar proposiciones con punto de acuerdo
  > en este tema. No impacta en iniciativas de ley de ninguna manera. Solo
  > alimenta la idea de colocar presión sobre el gobierno y visibilizar el
  > problema de seguridad.

- **#9 — Pemex garantiza abasto tras muerte de 'El Mencho'**
  > Si es relevante porque podrían generarse proposiciones con punto de
  > acuerdo a partir de esto.

- **#19 — Profeco operativos rumbo al Mundial 2026**
  > Es importante, porque aunque tiene que ver con el mundial, sí está
  > buscando la protección de consumidores y es probable que se emanen puntos
  > de acuerdo. Los legisladores en el vacío del receso van a exhortar al
  > gobierno para que opere en las mejores condiciones posibles.

- **#24 — Alfredo Ramírez Bedolla / Michoacán**
  > Se pueden generar proposiciones con punto de acuerdo.

- **#32 — Tribunal pide a SCJN atraer caso de maestra Lupita Barajas**
  > Se puede generar un punto de acuerdo.

### Contra-ejemplo (NO relevante)

- **#3 — Lotería Nacional y SEP presentan billete por centenario de normales rurales**
  > No es relevante porque están presentando un billete de lotería. No hay
  > forma de accionar legislativamente en esto.

- **#43 — El Mencho: cuánto ofrecía EU por su captura**
  > No es relevante.

**Implicación para el clasificador:** el modelo actual es demasiado conservador
con el filtro `_es_contexto_no_legislativo` y con el filtro de relevancia-México.
Está descartando docs que, aunque no mencionen instrumentos legislativos
directamente, tienen potencial de presión política.

---

## 2. Contenido internacional con impacto legislativo mexicano

El filtro `calcular_relevancia_mexico` está bloqueando artículos internacionales
que **sí** son legislativamente relevantes en México.

- **#28 — Countries in Asia Try to Contain the Economic Fallout of the Mideast Conflict**
  - pred del modelo: *(vacío — filtro México lo bloqueó)*
  - gold: `relaciones_exteriores + energia`
  > Reconoce un fenómeno en Asia, y particularmente en el Senado se podrían
  > generar puntos de acuerdo que se vinculen con el tema energético.

**Implicación:** el filtro no debe rechazar documentos internacionales cuando la
categoría propuesta es `relaciones_exteriores` o cuando el tema tiene impacto
energético / comercial directo en México. La penalización debería ser blanda
(multiplicador 0.8) en esos casos, no 0.3.

---

## 3. Género y derechos humanos — criterio temático, no léxico

**El hallazgo más importante del eval.** El clasificador tiene un recall de
**0.125** para `igualdad_genero` (detecta 1 de 8 casos reales) porque busca
palabras literales ("mujer", "género", "feminicidio"). El criterio humano es
distinto: agrega `igualdad_genero` cuando el cambio propuesto **afecta
desproporcionadamente a mujeres**, aunque el texto no diga ninguna de esas palabras.

### Casos con multi-label de género que el modelo no detectó

- **#50 — Ley de Premios, Estímulos y Recompensas Civiles**
  - pred: *(vacío)*
  - gold: `igualdad_genero`
  > Hay que fijarse en lo que dice la iniciativa. Quiere crear un premio
  > para mujeres.

- **#56 — Reforma Ley General de Salud en materia de lactancia materna**
  - pred: `salud`
  - gold: `salud + trabajo + igualdad_genero`
  > Sí es salud, pero también es trabajo y género porque es para solucionar
  > un tema de mujeres en espacios de trabajo.

- **#73 — Reforma a Ley Orgánica del Congreso (comisiones paritarias)**
  - pred: `electoral_politico + relaciones_exteriores`
  - gold: `electoral_politico + igualdad_genero`
  > Es una reforma para que las comisiones del congreso se integren de
  > manera paritaria. Por eso hay género.

- **#95 — Ley para Impulsar el Incremento Sostenido de la Productividad**
  - pred: `economia_hacienda`
  - gold: `economia_hacienda + igualdad_genero + derechos_humanos`
  > Es importante que veas por qué estoy integrando género. Es mucho porque
  > tiene que ver con mujeres y personas, y por eso derechos humanos igual.

### Cómo atacar esto

No se soluciona agregando keywords literales. Se necesita:

1. **Detectar sujetos temáticos** (mujeres, niñas, madres, embarazadas, víctimas)
   como señales de `igualdad_genero`, no solo las palabras "género" o "igualdad".
2. **Entender dominios** (lactancia, paridad, violencia de género, brecha salarial)
   como proxies directos.
3. A largo plazo, un clasificador ML o un prompt a Claude API encima del
   keyword-matching actual resolvería esto mucho mejor.

---

## 4. Correcciones específicas del modelo

### #4 — Adición al Código Penal Federal
- pred: `seguridad_justicia:2.25 + anticorrupcion:0.96`
- gold: `anticorrupcion + seguridad_justicia`
> Es más relevante situarla nada más en Anticorrupción. Seguridad y Justicia
> podría ir en Justicia porque es Código Penal Federal.

**Interpretación:** la jerarquía es correcta pero el modelo invirtió los pesos.
Las reformas al CPF en materia de corrupción deberían rankear `anticorrupcion`
como primaria, no secundaria.

### #13 — Exhorto sobre precios de garantía
- pred: `educacion:0.60 + agro_rural:0.49`
- gold: `economia_hacienda + agro_rural`
> Economía y Hacienda porque es un exhorto para atender cosas en relación a
> precios de garantía. No tiene nada que ver con Educación.

**Bug:** algún keyword de `educacion` está matcheando falsos positivos en docs
agrícolas/económicos. Revisar keywords de `educacion` en `config.py`.

### #31 — Reforma al art. 32 Ley Protección al Consumidor
- pred: `economia_hacienda:3.25 + inteligencia_artificial:0.60 + seguridad_justicia:0.57`
- gold: `medio_ambiente + economia_hacienda`
> Es un tema de productos y sustentabilidad que tiene que ver con medio ambiente.

**Bug:** `inteligencia_artificial` y `seguridad_justicia` son falsos positivos
claros. Keywords demasiado amplias. Y falta cobertura semántica de "sustentabilidad
de productos" en `medio_ambiente`.

### #11 — Reforma a Ley Federal de Defensoría Pública
- pred: `seguridad_justicia + derechos_humanos`
- gold: `seguridad_justicia + salud + medio_ambiente + derechos_humanos`
> Aplica a las cuatro categorías porque son mencionadas implícitamente.

**Implicación:** el `max_categorias=3` del NLP_CONFIG está limitando casos
multi-tema. Tal vez subir a 4 o eliminarlo cuando hay evidencia fuerte.

### #23 — Reforma a Ley del ISR (deducciones)
- pred: `trabajo + economia_hacienda`
- gold: `trabajo + economia_hacienda + educacion`
> Sumo educación porque es un requisito directo para la implementación de
> la reforma propuesta.

---

## 5. Gaps estructurales — cosas que el clasificador no puede hacer hoy

### 5.1 Falta la categoría "Administración" (propuesta)

- **#21 — Reforma constitucional al art. 25 y 73**
  - pred: `seguridad_justicia + electoral_politico + relaciones_exteriores`
  - gold: `electoral_politico`
  > Realmente no tiene mucho que ver, pero es el único lugar donde se podría
  > poner, Electoral y Político. Creo que tenemos que crear una categoría
  > que se llame **Administración**, y ahí entraría muy natural.

**Propuesta concreta:** agregar 19ª categoría `administracion` que cubra:
- Reformas constitucionales sobre estructura del Estado
- Ley Orgánica de la Administración Pública Federal
- Ley del Servicio Profesional de Carrera
- Organización de órganos autónomos (no electorales)
- Descentralización / municipalismo / federalismo

Requiere revisar qué docs actualmente clasificados como `electoral_politico`
deberían migrar a `administracion`.

### 5.2 Retiros de iniciativa — no se distinguen de presentación

- **#25 — Diputadas solicitan el retiro de iniciativa**
  - pred: `trabajo:0.55`
  - gold: NO relevante (retiro)
  > Aquí ojo. Están solicitando retiro de una iniciativa. Es importante leer
  > esto y entender que la están quitando.

**Bug crítico:** el clasificador trata "Solicitan el retiro de la iniciativa X"
igual que "Presentan la iniciativa X". Para FIAT, un retiro es **señal negativa**
o al menos debería reducir el peso del doc en scoring temporal.

**Fix propuesto:** detectar patrones de retiro (`retirar`, `retiro de`, `solicitan
que se tenga por no presentada`, `desistimiento`) y marcar con un flag `es_retiro`
o bajar el score a cero.

### 5.3 Columnas de opinión — valor distinto

- **#14 — Crecimiento, inversión y confianza** (columna de opinión)
  - pred: `economia_hacienda:0.67`
  - gold: `economia_hacienda` (sí relevante, pero con matiz)
  > Puede ser relevante, pero hay que tener cuidado porque es una columna
  > de opinión. Normalmente los asesores sacan datos de este tipo de columnas
  > para construir columnas completas.

**Implicación:** las columnas son **insumo** para legisladores, no señal de
actividad legislativa. Deberían tener un `peso_fuente` distinto en el scoring,
o un flag `es_opinion` para que el dashboard pueda filtrarlas.

### 5.4 Contexto de "crear comisión" vs legislación temática

- **#48 — Reforma al art. 39 Ley Orgánica del Congreso**
  - pred: `electoral_politico:1.15 + relaciones_exteriores:0.57`
  - gold: `electoral_politico + seguridad_justicia`
  > Hay que leer de qué trata, porque es de crear una comisión en materia
  > de seguridad y justicia.

**Implicación:** cuando el texto menciona "comisión de X" como objeto a crear,
la categoría X debería sumarse al match. Actualmente el boost por comisión
solo aplica cuando la comisión **es** el autor del documento.

### 5.5 Contexto político sobre nombramientos

- **#33 — Morena frena a Colmenares, coloca cercano a Sheinbaum en Auditoría**
  - pred: `anticorrupcion:0.59`
  - gold: `anticorrupcion + electoral_politico`
  > La manera en que está frameada la nota indica que hay cierta influencia
  > política sobre este cargo. Por eso es electoral y político.

**Implicación:** los nombramientos en órganos de control (Auditoría, INE, IFAI)
no son solo la categoría técnica del órgano — también son eventos
`electoral_politico` porque implican reparto de poder. Esto es inferencia
contextual, difícil con keywords.

---

## 6. Acciones concretas priorizadas

| # | Acción | Impacto esperado | Dificultad |
|---|---|---|---|
| 1 | Auditar keywords de `politica_social` (F1 = 0.00) | alto | baja |
| 2 | Agregar keywords temáticas a `igualdad_genero` (mujeres, madres, paritaria, lactancia, brecha salarial, violencia de género, víctimas mujeres, etc.) | alto | baja |
| 3 | Relajar filtro México cuando la categoría propuesta es `relaciones_exteriores` | medio | baja |
| 4 | Auditar falsos positivos de `electoral_politico` (16 FPs) y `relaciones_exteriores` (7 FPs) | medio | media |
| 5 | Limpiar keywords de `educacion` que matchean exhortos agrícolas (caso #13) | bajo | baja |
| 6 | Subir `max_categorias` de 3 a 4 para casos multi-tema claros | bajo | trivial |
| 7 | Detectar retiros de iniciativa con patrón regex | medio | baja |
| 8 | **Nueva categoría `administracion`** | medio | alta (requiere decisión + re-evaluación) |
| 9 | Flag `es_opinion` en scraping de medios | bajo | media |
| 10 | (Largo plazo) Capa ML o prompt Claude encima del keyword-matching | alto | alta |

---

## 7. Datos sueltos útiles

- **Mojibake en un artículo:** #24 tiene caracteres corruptos (`Ramĭrez Bedolla`,
  `â€ś`). Bug del scraper de medios: UTF-8 decodificado como Latin-1. Revisar
  `scrapers/medios.py` o el script que lo insertó.
- **Dos FPs binarios:** solo 2 artículos donde el modelo dijo "relevante" y el
  humano dijo "no". Son casos borderline razonables, no bugs graves.
- **12 FNs binarios:** el modelo dijo "no relevante" a 12 artículos que sí lo
  eran. La mayoría son por el criterio amplio de "proposiciones con punto de
  acuerdo" y por el filtro México.

---

## 8. Changelog v3 — categoría `administracion`

**Fecha:** 2026-04-09
**Baseline:** `eval/resultados_v3.csv`

### Qué se hizo

Se creó la 19ª categoría `administracion` (acción priorizada #8 de la tabla
de la sección 6) para cerrar el gap estructural detectado en el caso #21
("Reforma constitucional al art. 25 y 73 en materia de simplificación
administrativa y digitalización"), que el etiquetador colocó en
`electoral_politico` con la nota *"creo que tenemos que crear una categoría
que se llame Administración, y ahí entraría muy natural."*

**Cambios concretos en `config.py`:**

1. **Nueva categoría `administracion`** con 5 subcategorías:
   - `estructura_estado` — rectoría del Estado, división de poderes,
     facultades del Congreso, atribuciones del ejecutivo
   - `administracion_publica_federal` — LOAPF, secretarías de Estado,
     entidades paraestatales, organismos descentralizados, empresas
     productivas del Estado, fideicomisos públicos federales
   - `servicio_civil` — servicio profesional de carrera, mandos medios
   - `procedimiento_administrativo` — trámites burocráticos, simplificación
     administrativa, firma electrónica avanzada, gobierno digital
   - `planeacion_federalismo` — Plan Nacional de Desarrollo, planeación
     democrática, pacto federal, fortalecimiento municipal

2. **`COMISION_A_CATEGORIA`**: se movieron 4 fragmentos desde
   `electoral_politico` → `administracion`:
   - "gobernación" / "gobernacion"
   - "reforma del estado"
   - "puntos constitucionales"
   - "federalismo"
   - "desarrollo municipal"

   Se mantuvieron en `electoral_politico`: "reglamentos y prácticas
   parlamentarias", "participación ciudadana", "estudios legislativos",
   "medalla belisario", "reforma política-electoral".

3. **`LEYES_FEDERALES`**: 10 leyes movidas de `electoral_politico` →
   `administracion`:
   - Ley Orgánica de la Administración Pública Federal
   - Ley de Planeación
   - Ley de Entidades Paraestatales
   - Ley del Diario Oficial
   - Ley del Servicio Profesional de Carrera
   - Ley de Procedimiento Administrativo
   - Ley de Premios Estímulos y Recompensas Civiles
   - Ley Nacional para Eliminar Trámites Burocráticos
   - Ley de Firma Electrónica Avanzada
   - Estatuto de Gobierno del Distrito Federal

   Se mantuvieron en `electoral_politico` las propiamente políticas:
   leyes electorales, Constitución Política, Ley Orgánica del Congreso,
   reglamentos de cada cámara, Ley General de Comunicación Social, etc.

4. **Limpieza paralela** de `electoral_politico.gobernabilidad.keywords`:
   se eliminaron las leyes administrativas duplicadas (LOAPF, Planeación,
   Entidades Paraestatales, Servicio Profesional, Procedimiento
   Administrativo, etc.) que estaban listadas en ambos lados del registro.

### Casos verificados

| # | Título | v2 | v3 |
|---|---|---|---|
| 21 | Reforma art. 25/73 (simplificación administrativa) | `seguridad_justicia + electoral_politico + relaciones_exteriores` | **`administracion:1.03` + electoral_politico + seguridad_justicia** ✓ |
| 67 | Reforma diversa LOAPF | `electoral_politico + infraestructura + seguridad_justicia` | **`administracion:2.65` + infraestructura + seguridad_justicia** ✓ |
| 50 | Ley de Premios, Estímulos y Recompensas Civiles | `igualdad_genero` | `igualdad_genero + administracion` (multi-label, gold incluye igualdad_genero) ✓ |

### Lección de tooling: token vs substring

El primer intento incluía keywords como `"reforma constitucional"`,
`"reforma al artículo 25"`, `"servidores públicos"` y `"funcionario público"`.
Estas dispararon **20 FPs de `administracion`** porque el clasificador es
**token-based + substring**: cada keyword se tokeniza a nivel palabra y
cualquier doc que contenga `reforma`, `artículo`, `servidor` o `público`
sumaba puntos al score administrativo. La solución fue:

- Eliminar todas las variantes con tokens genéricos (`reforma`, `artículo`,
  `constitucional`, `servidor`, `público`).
- Mantener sólo frases compuestas con tokens distintivos (`rectoría del
  Estado`, `simplificación administrativa`, `firma electrónica avanzada`,
  `Plan Nacional de Desarrollo`, etc.).
- Confiar en `COMISION_A_CATEGORIA` (`puntos constitucionales` →
  `administracion`) para casos donde la señal viene del organismo
  dictaminador, no del texto.

Resultado tras la limpieza: **20 FPs → 6 FPs** de admin.

### Comparación v2 vs v3 (eval set v1)

| Métrica | v2 | v3 | Δ |
|---|---|---|---|
| Binario F1 | — | 0.909 | — |
| Binario precision | — | 0.970 | — |
| Binario recall | — | 0.855 | — |
| Macro F1 | 0.634 | 0.596 | **−0.038** |
| Micro F1 | 0.642 | 0.620 | **−0.022** |
| Top-1 accuracy | — | 0.743 | — |
| `electoral_politico` F1 | 0.512 | 0.465 | −0.047 |
| `relaciones_exteriores` FP | 7 | 6 | −1 ✓ |
| `administracion` (TP/FP/FN) | n/a | 0/6/0 | nueva |

**El "regression" macro/micro NO es real**. El gold del eval set v1 fue
etiquetado **antes** de que existiera `administracion`, así que cualquier
predicción de `administracion` cuenta como FP, aun cuando sea
semánticamente correcta (#21, #67 son los casos paradigmáticos). El
ejercicio de re-etiquetado del 2026-04-10 resolverá esto: el etiquetador
verá la nueva categoría y la podrá agregar a los gold labels.

### Pendientes derivados

- [ ] **Tomorrow's exercise**: re-etiquetar el eval set incluyendo
  `administracion` como opción. Casos prioritarios para revisar gold:
  #4, #8, #15, #21, #45, #50, #67.
- [ ] Corregir `electoral_politico` FPs restantes (16) — ya bajaron 1
  con la migración pero el grueso sigue (señales débiles tipo "Ley
  Orgánica del Congreso" matcheando casi cualquier doc parlamentario).
- [ ] Investigar por qué `electoral_politico` perdió 1 TP en v3 (probable
  efecto colateral de la limpieza de leyes administrativas).
- [ ] Habilitar el flag de "categoría nueva — sin gold previo" en
  `calcular_metricas.py` para no penalizar `administracion` en este eval
  set hasta que se re-etiquete.
