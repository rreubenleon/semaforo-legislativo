# Diagnóstico — Diputados reelectos LXIV/LXV → LXVI

Fuente: SITL Diputados (sitl.diputados.gob.mx)
Sin escrituras en BD. Solo diagnóstico.

## Conteos

- LXIV: **503** propietarios
- LXV:  **500** propietarios
- LXVI: **500** propietarios
- BD `legisladores` (Diputados activos LXVI, incluye suplentes en funciones): **532**

## Reelectos en LXVI

- **Total reelectos**: 106 (21.2% del roster LXVI)
- 3 periodos (LXIV+LXV+LXVI): **36**
- 2 periodos consecutivos (LXV+LXVI): **55**
- Saltaron una legislatura (LXIV+LXVI, sin LXV): **15**

## Match con BD

- Reelectos que coinciden con `legisladores`: **106**
- Reelectos NO encontrados en BD (probable propietario sustituido por suplente): **0**

## Top 30 reelectos con 3 periodos (LXIV+LXV+LXVI)

| Nombre LXVI | Estado | Distrito | LXIV estado | LXV estado |
|---|---|---|---|---|
| Antonio Altamirano Carol | Oaxaca | Dtto.  5 | Oaxaca | Oaxaca |
| Baldenebro Arredondo Manuel de Jesús | Sonora | Dtto.  1 | Sonora | Sonora |
| Bautista Bravo Juan Angel | México | Dtto.  31 | México | México |
| Bautista Peláez María del Carmen | Oaxaca | Dtto.  9 | Oaxaca | Oaxaca |
| Borrego Adame Francisco Javier | Coahuila | Dtto.  2 | Coahuila | Coahuila |
| Carrazco Macías Olegaria | Sinaloa | Dtto.  6 | Sinaloa | Sinaloa |
| Carvajal Hidalgo Alejandro | Puebla | Dtto.  6 | Puebla | Puebla |
| Domínguez Rodríguez Roberto Ángel | México | Dtto.  28 | México | México |
| Gutiérrez Luna Sergio Carlos | Veracruz | Circ.  5 | México | México |
| Hernández Pérez César Agustín | México | Dtto.  30 | México | México |
| Hernández Tapia Arturo Roberto | México | Dtto.  35 | México | México |
| Juan Carlos Irma | Oaxaca | Dtto.  2 | Oaxaca | Oaxaca |
| Navarrete Rivera Alma Delia | México | Dtto.  13 | México | México |
| Pérez Bernabe Jaime Humberto | Veracruz | Dtto.  6 | Veracruz | Veracruz |
| Sánchez Barrios Carlos | Guerrero | Dtto.  7 | Guerrero | Guerrero |
| Tenorio Adame Paola | Veracruz | Dtto.  19 | Veracruz | Veracruz |
| Vargas Meraz Teresita de Jesús | Chihuahua | Dtto.  2 | Chihuahua | Chihuahua |
| Vázquez García Dionicia | México | Dtto.  2 | México | México |
| Vences Valencia Julieta Kristal | Puebla | Circ.  4 | Puebla | Puebla |
| Villegas Guarneros Dulce María Corina | Veracruz | Dtto.  15 | Veracruz | Veracruz |
| Gómez Cárdenas Annia Sarahí | Nuevo León | Dtto.  6 | Nuevo León | Nuevo León |
| Lixa Abimerhi José Elías | Yucatán | Circ.  3 | Yucatán | Yucatán |
| Ramírez Barba Éctor Jaime | Guanajuato | Dtto.  5 | Guanajuato | Guanajuato |
| Tejeda Cid Armando | Michoacán | Circ.  5 | Michoacán | Michoacán |
| Fernández Cruz Nayeli Arlen | Ciudad de México | Circ.  4 | Ciudad de México | Ciudad de México |
| Puente Salas Carlos Alberto | Zacatecas | Circ.  2 | Zacatecas | Zacatecas |
| Bernal Martínez Mary Carmen | Michoacán | Dtto.  3 | Michoacán | Michoacán |
| Elizondo Guerra Olga Juliana | Tamaulipas | Dtto.  7 | Tamaulipas | Tamaulipas |
| García García Margarita | Oaxaca | Dtto.  3 | Oaxaca | Oaxaca |
| García Hernández Jesús Fernando | Sinaloa | Dtto.  3 | Sinaloa | Sinaloa |

## Notas

- El cruce solo se hace por **nombre normalizado** (lower + sin acentos + sin prefijos).
- No considera cambio de cámara (Diputado→Senador). Para eso hace falta scrapear Senado LXIV/LXV (pendiente: senado.gob.mx redirige en bucle, requiere otro path).
- Casos como **Alejandro Moreno** (Diputado LXV → Senador LXVI) NO aparecen aquí porque no está en el roster de Diputados LXVI.
- Casos como **Rubén Moreira** (Diputado LXIV → LXV → LXVI) sí aparecen.
- Caso **Manuel Añorve** (Senador 2018→presente) requiere scrape de Senado para detectarlo.