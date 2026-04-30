# Análisis Histórico de Recesos — LXVI Legislatura

_Generado el 2026-04-29 21:29 desde 12,770 instrumentos legislativos clasificados._

## 1. Volumen por periodo

| Periodo | Total | Iniciativas | Proposiciones PA | Dictámenes | Minutas |
|---|---:|---:|---:|---:|---:|
| 1er Ordinario | 5,409 | 3,624 | 1,553 | 194 | 38 |
| 2do Ordinario | 4,976 | 3,342 | 1,407 | 193 | 34 |
| 1er Receso | 671 | 354 | 313 | 4 | 0 |
| 2do Receso | 1,714 | 741 | 899 | 74 | 0 |

## 2. Receso vs Ordinario (LXVI completo)

| Métrica | Receso | Ordinario | Δ |
|---|---:|---:|---:|
| Total instrumentos | 2,385 | 10,385 | 23% del ordinario |
| Iniciativas | 1,095 | 6,966 | 16% |
| Proposiciones PA | 1,212 | 2,960 | 41% |
| Dictámenes | 78 | 387 | 20% |
| Aprobados | 482 | 1,503 | — |
| Tasa de aprobación | **20.2%** | **14.5%** | — |

## 3. Categorías más activas en RECESO

¿Qué temas SÍ se mueven cuando no hay sesiones ordinarias?

| Categoría | En Receso | En Ordinario | Ratio (Rec/Ord) | Interpretación |
|---|---:|---:|---:|---|
| trabajo | 580 | 659 | 0.88 | Muy reactiva en receso |
| agro_rural | 80 | 280 | 0.29 | Muy reactiva en receso |
| turismo | 31 | 110 | 0.28 | Muy reactiva en receso |
| politica_social | 70 | 250 | 0.28 | Muy reactiva en receso |
| economia_hacienda | 234 | 1018 | 0.23 | Reactiva moderada |
| medio_ambiente | 94 | 454 | 0.21 | Reactiva moderada |
| derechos_humanos | 147 | 714 | 0.21 | Reactiva moderada |
| infraestructura | 167 | 860 | 0.19 | Reactiva moderada |
| energia | 33 | 179 | 0.18 | Reactiva moderada |
| seguridad_justicia | 305 | 1683 | 0.18 | Reactiva moderada |
| educacion | 177 | 998 | 0.18 | Reactiva moderada |
| salud | 145 | 836 | 0.17 | Reactiva moderada |
| relaciones_exteriores | 68 | 395 | 0.17 | Reactiva moderada |
| electoral_politico | 131 | 831 | 0.16 | Reactiva moderada |
| medios_comunicacion | 23 | 146 | 0.16 | Reactiva moderada |

_Ratio = receso / ordinario por categoría. Más alto = tema más reactivo a coyuntura (no necesita sesión ordinaria para aparecer)._

## 4. ¿Qué pasa con los instrumentos presentados en receso?

| Estatus | N | % |
|---|---:|---:|
| Pendiente | 1,472 | 61.7% |
| Aprobado/Resuelto | 482 | 20.2% |
| Desechado | 401 | 16.8% |
| Retirado | 28 | 1.2% |
| Otro | 2 | 0.1% |

## 5. Composición por tipo de instrumento

¿Cómo cambia el mix de tipos entre receso y ordinario?

| Tipo | Receso | Ordinario |
|---|---:|---:|
| Iniciativa | 1,095 (46%) | 6,966 (67%) |
| Proposición con Punto de Acuerdo | 1,212 (51%) | 2,960 (29%) |
| Dictamen | 78 (3%) | 387 (4%) |
| Minuta | 0 (0%) | 72 (1%) |
| Acuerdo Parlamentario | 0 (0%) | 0 (0%) |
| Comunicado | 0 (0%) | 0 (0%) |

## 6. Cámara que más actúa en receso

| Cámara | N | % |
|---|---:|---:|
| Comisión Permanente | 1,272 | 53.3% |
| Diputados | 828 | 34.7% |
| Senadores | 269 | 11.3% |
| ? | 16 | 0.7% |

_Nota: durante receso, la Comisión Permanente puede sesionar en cualquiera de las dos cámaras según el receso. Esta tabla incluye todo lo presentado durante esos meses._

## 7. Implicaciones para el modelo FIAT

1. **Volumen en receso:** 19% del volumen total LXVI ocurre en receso. El score `Congreso` baseline DEBE recalibrarse por categoría.
2. **Tasa de aprobación:** receso 20.2% vs ordinario 14.5%. Receso APROBA MÁS.
3. **Categorías reactivas en receso** (las que sí se mueven sin sesión): trabajo, agro_rural, turismo, politica_social, economia_hacienda. Para estas categorías el modo receso debe seguir alimentando `Congreso` normal.
5. **Predicción de actividad**: la métrica `iniciativas_proy_15d` carece de sentido en receso. Sugerir reemplazarla por `prob_aparicion_permanente_15d` durante el modo receso.

## 8. Anexo: muestras de aprobaciones en receso

Total aprobados en recesos LXVI: 482

| Periodo | Tipo | Cámara | Categoría | Título (truncado) |
|---|---|---|---|---|
| 1er Receso | Proposición con Punto de Acuerdo | Comisión Perma | trabajo | Por la que exhorta a la SICT, a llevar a cabo las acciones necesarias, para la c |
| 1er Receso | Proposición con Punto de Acuerdo | Comisión Perma | igualdad_genero | Por la que exhorta al Congreso de la Unión y a sus dos Cámaras a impulsar divers |
| 1er Receso | Proposición con Punto de Acuerdo | Comisión Perma | infraestructura | Por el que se exhorta, a la Secretaría de Infraestructura, Comunicaciones y Tran |
| 1er Receso | Proposición con Punto de Acuerdo | Comisión Perma | medio_ambiente | Por el que se exhorta a la Semarnat y a la Conafor a que fortalezcan las accione |
| 1er Receso | Proposición con Punto de Acuerdo | Comisión Perma | medio_ambiente | Por el que se exhorta a la Comisión Nacional Forestal a fortalecer las acciones  |
| 1er Receso | Proposición con Punto de Acuerdo | Comisión Perma | inteligencia_artificial | Por el que se exhorta a las Secretarías de Movilidad y homólogas de las 32 entid |
| 1er Receso | Proposición con Punto de Acuerdo | Comisión Perma | trabajo | Por la que exhorta a las secretarías de movilidad y homólogos de las 32 entidade |
| 1er Receso | Proposición con Punto de Acuerdo | Comisión Perma | derechos_humanos | Relativo al Informe de Actividades correspondiente al año 2025 de la Presidenta  |
| 1er Receso | Dictamen | Comisión Perma | trabajo | Por el que se aprueban proposiciones con punto de acuerdo para fortalecer las ac |
| 1er Receso | Dictamen | Comisión Perma | relaciones_exteriores | Por el que se ratifica el nombramiento que la Presidenta de la República, hace a |
| 1er Receso | Dictamen | Comisión Perma | salud | Por el que se aprueban proposiciones con punto de acuerdo en materia de procedim |
| 1er Receso | Proposición con Punto de Acuerdo | Comisión Perma | infraestructura | Por el que se exhorta a la Agencia Federal de Aviación Civil y a la Procuraduría |
| 1er Receso | Proposición con Punto de Acuerdo | Comisión Perma | trabajo | Por el que se exhorta a la STPS, así como a las autoridades laborales de las ent |
| 1er Receso | Proposición con Punto de Acuerdo | Comisión Perma | trabajo | Por la que exhorta al Titular de la STPS, así como a las autoridades laborales d |
| 1er Receso | Proposición con Punto de Acuerdo | Comisión Perma | salud | Por la que exhorta a la Secretaría de Salud, a tomar las medidas pertinentes par |
