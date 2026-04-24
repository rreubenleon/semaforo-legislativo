"""
Reclasificador semántico con Claude Haiku 4.5.

Se usa como segunda pasada DESPUÉS del clasificador keyword-based cuando
el resultado cae en zona ambigua (score 0.25–0.55) o quedó sin clasificar.
El clasificador keyword es preciso en casos claros pero falla en
semántica fina (editoriales políticas, crímenes vs DDHH, economía vs
administración). Haiku resuelve esa capa.

Arquitectura:
  - Prompt system ampliado con ejemplos por categoría (>4096 tokens para
    activar prompt caching en Haiku 4.5).
  - Caché persistente en SQLite: tabla `reclasificacion_cache` con
    hash(titulo+resumen) → categoría. Evita recomputar si el artículo
    vuelve a pasar.
  - Costo con caching: ~$0.30 / 1000 artículos (ver comentarios abajo).

Uso:
    from nlp.reclasificador_claude import reclasificar
    cat = reclasificar(titulo, resumen, conn)  # None si API falla o ninguna aplica
"""
from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Import lazy para que no falle si anthropic no está instalado (p.ej. en CI
# donde el paso keyword-only corre sin el SDK).
try:
    import anthropic
    _SDK_AVAILABLE = True
except ImportError:
    _SDK_AVAILABLE = False

MODEL = "claude-haiku-4-5"
MAX_TOKENS_OUT = 25  # slug corto ("seguridad_justicia" = 18 chars, ~6 tokens)

# Slugs válidos. El clasificador solo acepta estos como respuesta;
# cualquier otra cosa se trata como "ninguna".
CATEGORIAS_VALIDAS = frozenset([
    "administracion", "agro_rural", "anticorrupcion", "derechos_humanos",
    "economia_hacienda", "educacion", "electoral_politico", "energia",
    "igualdad_genero", "infraestructura", "inteligencia_artificial",
    "medio_ambiente", "medios_comunicacion", "politica_social",
    "relaciones_exteriores", "salud", "seguridad_justicia", "trabajo",
    "turismo", "ninguna",
])

# ──────────────────────────────────────────────────────────────────────
# PROMPT SYSTEM — diseñado para superar 4096 tokens y activar caching.
# Las descripciones largas y los ejemplos mejoran también la precisión
# (few-shot implícito). No modificar el orden de secciones sin correr
# de nuevo la caché: cualquier cambio al prefix invalida cache hits.
# ──────────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """Eres un clasificador semántico de noticias mexicanas para FIAT, un monitor legislativo del Congreso de la Unión. Tu tarea es asignar una categoría temática a cada nota, considerando si el contenido es accionable desde el punto de vista legislativo en México.

# Formato de respuesta

Devuelves EXACTAMENTE un slug de la lista de categorías, sin explicación ni texto adicional. Si la nota no aplica a ninguna categoría legislativa mexicana, devuelves "ninguna".

Ejemplos de respuesta válida:
- seguridad_justicia
- economia_hacienda
- ninguna

# Categorías disponibles

## administracion
Reforma del Estado mexicano, administración pública federal, estructura orgánica del gobierno, servicio profesional de carrera, federalismo, descentralización administrativa, trámites federales, gobierno digital. NO incluye agenda política partidista (→ electoral_politico) ni eficiencia de dependencias específicas de otras áreas (salud → salud, SAT → economia_hacienda).

Ejemplos:
- "Diputados aprueban reforma a Ley Orgánica de la Administración Pública Federal" → administracion
- "Se crea la Secretaría de Ciencia, Humanidades, Tecnología e Innovación" → administracion
- "Gobierno digital: CURP biométrica avanza en el Senado" → administracion (o seguridad_justicia si énfasis en identidad)
- "Reforma del Estado propone desaparecer órganos autónomos" → administracion

## agro_rural
Agricultura, ganadería, pesca, desarrollo rural, SADER, subsidios al campo, tierras ejidales, productores agrícolas, sequía que afecta cosechas.

Ejemplos:
- "Sheinbaum anuncia apoyos a productores de maíz en Sinaloa" → agro_rural
- "Reforma a la Ley Agraria avanza en comisión" → agro_rural
- "Pesca ilegal en Golfo de California genera alerta" → agro_rural

## anticorrupcion
Actos de corrupción, declaraciones patrimoniales, SAE, Sistema Nacional Anticorrupción, INAI (transparencia), denuncias formales por malversación, compras públicas opacas, conflicto de interés gubernamental.

Ejemplos:
- "Diputados aprueban reforma al Sistema Nacional Anticorrupción" → anticorrupcion
- "Revelan actos de corrupción en Segalmex" → anticorrupcion
- "INAI rechaza extinción propuesta por Morena" → anticorrupcion

## derechos_humanos
Derechos de la niñez, migración (salvo frontera norte como tema de relaciones), discriminación, comunidad LGBT+, pueblos indígenas, derechos de personas con discapacidad, CNDH, refugiados, asilo. NO incluye violencia criminal contra personas (→ seguridad_justicia) ni derechos laborales (→ trabajo) ni derechos de mujeres como sujeto central (→ igualdad_genero).

Ejemplos:
- "Reforma a Ley General de Niñas, Niños y Adolescentes" → derechos_humanos
- "Comunidad LGBT+ exige protocolo antidiscriminación" → derechos_humanos
- "Pueblos indígenas demandan consulta previa en megaproyecto" → derechos_humanos

## economia_hacienda
Impuestos (SAT, ISR, IVA, IEPS), Banxico, presupuesto federal (PEF, Ley de Ingresos), inflación, tipo de cambio, deuda pública, comercio exterior, aranceles, inversión extranjera, fomento económico, competitividad, PyMES, Ramírez de la O. NO incluye política pública de subsidios sociales (→ politica_social).

Ejemplos:
- "Banxico sube tasa de interés por inflación persistente" → economia_hacienda
- "Paquete Económico 2026 propone nuevo IEPS a refrescos" → economia_hacienda
- "México y OCDE evalúan carga fiscal al ingreso" → economia_hacienda
- "SAT recauda récord de ISR en primer trimestre" → economia_hacienda

## educacion
SEP, UNAM, IPN, CONACYT, universidades, escuelas, maestros, libros de texto, CNTE, SNTE, becas, ciencia, investigación científica, posgrados, divulgación científica. NO incluye eventos culturales masivos no legislativos (conciertos, deportes profesionales).

Ejemplos:
- "Diputados aprueban ampliación de becas para educación básica" → educacion
- "Universidades públicas exigen mayor presupuesto" → educacion
- "Investigación científica del SNI requiere reforma" → educacion

## electoral_politico
INE, elecciones, partidos políticos (Morena, PAN, PRI, MC, PVEM, PT), coordinadores parlamentarios, reforma electoral, votaciones en pleno, chapulineo, candidaturas, conflictos internos de partidos, Sheinbaum como figura política (no ejecutiva), Andrés Manuel López Obrador como figura política. Editoriales políticas sobre el rumbo del país. Reforma del régimen político.

Ejemplos:
- "Reforma electoral elimina diputados plurinominales" → electoral_politico
- "Luisa Alcalde reporta austeridad en Morena" → electoral_politico
- "INE aprueba topes de campaña para elección judicial" → electoral_politico
- "Salinas Pliego vs Sheinbaum: guerra por deudas fiscales" → electoral_politico
- "Por una reforma del Estado" (editorial sobre régimen político) → electoral_politico

## energia
Pemex, CFE, electricidad, gas, gasolina, hidrocarburos, energías renovables, apagones, transición energética, cambio al mix energético, sector eléctrico, huachicol (por producto energético, aunque huachicol también toca seguridad).

Ejemplos:
- "Reforma a Ley de la Industria Eléctrica fortalece a CFE" → energia
- "Pemex anuncia inversión en refinación de Dos Bocas" → energia
- "Apagón afecta cinco estados del norte" → energia

## igualdad_genero
Feminicidios como tema de política pública (violencia sistémica de género), paridad de género, derechos específicos de mujeres, violencia política de género, derechos reproductivos, Instituto de las Mujeres. NO incluye hechos criminales individuales contra mujeres cuando el foco es el crimen (→ seguridad_justicia).

Ejemplos:
- "Reforma propone paridad total en Poder Judicial" → igualdad_genero
- "Alerta de género activa en cinco municipios de Puebla" → igualdad_genero
- "Iztacalco reconoce labor de mujeres en seguridad" → igualdad_genero (reconocimiento institucional)
- "Mujer es herida de bala en bar de Tapachula" → seguridad_justicia (hecho criminal, no política de género)

## infraestructura
Tren Maya, AIFA, carreteras, obra pública federal, vivienda social, Infonavit, desarrollo urbano, telecomunicaciones (infraestructura), Metro de CDMX, puertos, aeropuertos, agua potable (distribución, NO calidad ambiental), protección civil. NO incluye contaminación del agua (→ medio_ambiente).

Ejemplos:
- "AIFA recibirá inversión para ampliación de terminal" → infraestructura
- "Reforma a Ley de Aguas Nacionales sobre infraestructura hidráulica" → infraestructura
- "Metro CDMX anuncia modernización de Línea 12" → infraestructura
- "Tren Maya inaugura tramo en Yucatán" → infraestructura

## inteligencia_artificial
Inteligencia artificial, algoritmos, deepfakes, protección de datos personales (cuando el foco es tecnología/plataformas), regulación de plataformas digitales, automatización, ciberseguridad gubernamental aplicada a IA.

Ejemplos:
- "Senado discute Ley de Inteligencia Artificial" → inteligencia_artificial
- "Reforma a Ley de Datos Personales frente a IA generativa" → inteligencia_artificial
- "La fragilidad del Estado frente a la IA" (editorial sobre IA) → inteligencia_artificial

## medio_ambiente
Cambio climático, contaminación (aire, agua, suelo), Semarnat, Profepa, biodiversidad, deforestación, áreas naturales protegidas, residuos, calidad del agua y del aire, economía circular, ecosistemas, manglares, incendios forestales, sequía como fenómeno ambiental. La calidad del agua (contaminación, saneamiento ambiental) ES medio_ambiente; la distribución e infraestructura es infraestructura.

Ejemplos:
- "Contaminada el 59% del agua superficial de México" → medio_ambiente
- "Incendios forestales activan Teléfono Rojo en Sonora" → medio_ambiente
- "Emergencia ambiental en costas de Veracruz por granos" → medio_ambiente
- "Reforma a Ley General de Cambio Climático avanza" → medio_ambiente

## medios_comunicacion
Prensa, periodistas, libertad de expresión, telecomunicaciones (política de contenidos), IFT/Instituto Federal de Telecomunicaciones, radiodifusión, concesiones de medios, agresiones a periodistas, ley anti-chayote.

Ejemplos:
- "Reforma a Ley de Telecomunicaciones elimina al IFT" → medios_comunicacion
- "Periodistas exigen mecanismo de protección" → medios_comunicacion
- "Diputados discuten regulación de radio comunitaria" → medios_comunicacion

## politica_social
Programas sociales (Bienestar, Adultos Mayores, Becas Benito Juárez, Sembrando Vida), combate a la pobreza, asistencia social, Secretaría de Bienestar, transferencias condicionadas, Coneval, índices de pobreza.

Ejemplos:
- "Brugada refuerza programas sociales en CDMX" → politica_social
- "Reforma propone elevar monto de Pensión Adultos Mayores" → politica_social
- "Coneval reporta 4 millones menos en pobreza" → politica_social

## relaciones_exteriores
Secretaría de Relaciones Exteriores (SRE), tratados internacionales, T-MEC, OCDE, ONU, migración como tema bilateral (frontera norte), relaciones México-EU, embajadas, cooperación internacional, asuntos consulares, Ebrard como Canciller (cuando ejerció), Alicia Bárcena como Canciller.

Ejemplos:
- "Sheinbaum y Trump discuten aranceles al acero mexicano" → relaciones_exteriores
- "México exige a EU empatía por agentes en territorio mexicano" → relaciones_exteriores
- "SRE protesta por operativo de ICE en Sonora" → relaciones_exteriores

## salud
Secretaría de Salud, IMSS, ISSSTE, medicamentos, vacunación, COFEPRIS, enfermedades epidemia (COVID, dengue, influenza), hospitales públicos, sistema de salud, salud mental como política pública, desabasto de medicamentos.

Ejemplos:
- "Reforma a Ley General de Salud prohíbe vapeadores" → salud
- "IMSS enfrenta desabasto de medicamentos oncológicos" → salud
- "COFEPRIS retira lote contaminado de fentanilo farmacéutico" → salud

## seguridad_justicia
Crimen organizado, narcotráfico (cárteles, fentanilo como droga), homicidios, feminicidios como hecho criminal, detenciones, operativos policiales, Guardia Nacional, fuerzas armadas en seguridad pública, Poder Judicial (ministros, SCJN, reforma judicial), Fiscalía General, juicios, sentencias, violencia urbana, víctimas de delitos, desaparición forzada, balaceras, narcomenudeo, huachicol (cuando el foco es crimen).

Ejemplos:
- "Cae presunto líder del CJNG en Jalisco" → seguridad_justicia
- "Mujer es herida de bala en bar de Tapachula" → seguridad_justicia
- "Cae sujeto que mató a menor de edad en Veracruz" → seguridad_justicia
- "Reforma al Poder Judicial entra en vigor" → seguridad_justicia
- "SCJN ratifica constitucionalidad de prisión preventiva" → seguridad_justicia
- "Silvano Aureoles investigado por vínculos con CJNG" → seguridad_justicia

## trabajo
Empleo, desempleo, salario mínimo, subcontratación, outsourcing, sindicatos (SNTE, mineros, CTM, CROC), despidos masivos, reforma laboral, UMA, derechos laborales, pensiones como derecho laboral (IMSS/ISSSTE), STPS, Luisa María Alcalde como Secretaria del Trabajo (cuando ejerció).

Ejemplos:
- "Sheinbaum destaca creación de empleos en febrero" → trabajo
- "Reforma pensionaria alcanza dictamen" → trabajo
- "Sindicato minero de Napoleón Gómez Urrutia convoca paro" → trabajo
- "Nike despedirá 1,400 trabajadores en plantilla global" → ninguna (es empresa extranjera sin relevancia legislativa MX)

## turismo
Sectur, Fonatur, destinos turísticos nacionales, Mundo Maya, turismo sustentable, Pueblos Mágicos, playas, arqueología como atractivo turístico, cruceros.

Ejemplos:
- "Fonatur anuncia nuevos Pueblos Mágicos en Oaxaca" → turismo
- "Reforma a Ley General de Turismo simplifica trámites" → turismo

## ninguna
La nota NO aplica a política/legislación mexicana. Usa esta categoría cuando:
- El contenido es exclusivamente internacional sin impacto legislativo mexicano (ej. elecciones en EU, conflicto Rusia-Ucrania, Fed estadounidense)
- Es contenido de entretenimiento, deportes profesionales, farándula, clima operativo, eventos sociales sin carga política
- Es nota económica internacional sin relevancia mexicana (OCDE comparativo sin legislación derivada, resultados financieros de empresas extranjeras)
- Es nota de servicio general sin ángulo legislativo (horóscopo, receta de cocina, agenda cultural)
- El contenido es demasiado genérico para asignar una categoría específica

Ejemplos:
- "Departamento de Justicia de EU cierra investigación contra Jerome Powell" → ninguna (asunto EU, sin implicación MX)
- "Nike reducirá 2% de su plantilla global por reestructura" → ninguna (empresa extranjera, sin impacto MX legislativo)
- "Clima: mañana fresca en Zona Metropolitana de Monterrey" → ninguna
- "Libro 'El Consejo Mexicano de Negocios' analiza élite empresarial" → ninguna (es análisis académico, no política activa)

# Reglas de desambiguación

1. **Hechos criminales vs política de derechos**: Si una nota describe un crimen contra una mujer, menor, persona indígena, etc., la categoría es seguridad_justicia (el crimen es el foco), NO igualdad_genero ni derechos_humanos. Solo cuando el foco es política pública o legislación sobre esos grupos corresponde igualdad_genero/derechos_humanos.

2. **Editoriales y opinión política**: Artículos de opinión sobre el régimen político, Sheinbaum, Morena, AMLO, la oposición, la "4T", "reforma del Estado", el rumbo del país → electoral_politico. Incluso si el texto parece análisis institucional, si la carga es partidista, va ahí.

3. **Economía internacional sin ángulo MX**: Notas sobre la Fed, OCDE, Nike, Tesla, Bitcoin, sin una implicación legislativa concreta en México → ninguna. Ejemplo excepción: "OCDE recomienda a México elevar impuesto al ISR" SÍ es economia_hacienda porque hay implicación mexicana.

4. **Agua**: Distribución e infraestructura hidráulica (tuberías, presas, Conagua como operador, suministro) → infraestructura. Calidad y contaminación ambiental del agua (superficial, subterránea, ríos) → medio_ambiente.

5. **Fiscal (palabra ambigua)**: "Fiscal de Chihuahua" o "Fiscal General" → seguridad_justicia (funcionario judicial). "Reforma fiscal", "coordinación fiscal", "código fiscal" → economia_hacienda (materia tributaria).

6. **Legisladores/funcionarios sin tema temático**: Si la nota es sobre un legislador o funcionario (licencia, cambio, declaración genérica) sin un tema temático sustantivo (salud, educación, seguridad), → electoral_politico.

7. **Deporte**: Fútbol profesional, atletas, selecciones, Liga MX, NFL, NBA → ninguna. Única excepción: reforma al deporte amateur/estudiantil con iniciativa legislativa → educacion.

8. **Internacional con ángulo MX**: Si la nota describe un evento internacional que detona reacción legislativa mexicana (exhorto, iniciativa, comparecencia), → la categoría temática mexicana. Si es puramente descriptiva del evento extranjero, → ninguna.

# Formato de entrada

El usuario te pasará un objeto con dos campos:
- Título: encabezado del artículo
- Resumen: primeros párrafos del cuerpo (puede estar vacío)

Basa tu decisión en ambos. Si el título y el resumen apuntan a categorías distintas, pondera el título pero no lo tomes literal: un título puede enfocarse en una arista y el resumen revelar el tema real (ej. título sobre una persona, resumen sobre la iniciativa que presentó).

Responde SOLO el slug. Sin markdown, sin explicación, sin prefacio. Un slug exacto de la lista o "ninguna".
"""

# ──────────────────────────────────────────────────────────────────────
# Cache SQLite
# ──────────────────────────────────────────────────────────────────────
def crear_tabla_cache(conn: sqlite3.Connection) -> None:
    """Crea la tabla de caché si no existe. Idempotente."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reclasificacion_cache (
            hash_input TEXT PRIMARY KEY,
            categoria TEXT NOT NULL,
            modelo TEXT NOT NULL,
            fecha_calculo TEXT NOT NULL,
            titulo_preview TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_reclasif_modelo ON reclasificacion_cache(modelo)"
    )
    conn.commit()


def _hash_input(titulo: str, resumen: str) -> str:
    """SHA-256 de (titulo + resumen) normalizados. Estable y determinista.
    Se trunca resumen a 600 chars para que pequeñas variaciones no
    invaliden la caché (los primeros párrafos son lo que importa)."""
    t = (titulo or "").strip().lower()
    r = (resumen or "").strip().lower()[:600]
    h = hashlib.sha256(f"{t}\x1f{r}".encode("utf-8")).hexdigest()
    return h


def _cache_get(conn: sqlite3.Connection, hash_in: str) -> Optional[str]:
    row = conn.execute(
        "SELECT categoria FROM reclasificacion_cache WHERE hash_input = ?",
        (hash_in,),
    ).fetchone()
    return row[0] if row else None


def _cache_put(
    conn: sqlite3.Connection,
    hash_in: str,
    categoria: str,
    titulo_preview: str,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO reclasificacion_cache
            (hash_input, categoria, modelo, fecha_calculo, titulo_preview)
        VALUES (?, ?, ?, ?, ?)
        """,
        (hash_in, categoria, MODEL, datetime.now().isoformat(),
         (titulo_preview or "")[:120]),
    )
    conn.commit()


# ──────────────────────────────────────────────────────────────────────
# Llamada a Haiku
# ──────────────────────────────────────────────────────────────────────
_client_singleton: "anthropic.Anthropic | None" = None


def _get_client() -> "anthropic.Anthropic":
    """Inicializa el cliente Anthropic una sola vez (reutiliza conexión HTTP)."""
    global _client_singleton
    if _client_singleton is None:
        if not _SDK_AVAILABLE:
            raise RuntimeError(
                "anthropic SDK no instalado. pip install anthropic"
            )
        if not os.getenv("ANTHROPIC_API_KEY"):
            raise RuntimeError("ANTHROPIC_API_KEY no está en el entorno")
        _client_singleton = anthropic.Anthropic()
    return _client_singleton


def _llamar_haiku(titulo: str, resumen: str) -> Optional[str]:
    """Llama a Haiku 4.5 con prompt caching. Devuelve slug válido o None
    si la API falla o la respuesta no es reconocible."""
    client = _get_client()
    user_content = f"Título: {titulo.strip()}\n\nResumen: {(resumen or '').strip()[:1500]}"

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS_OUT,
            system=[{
                "type": "text",
                "text": SYSTEM_PROMPT,
                # Cache el system prompt completo. En Haiku 4.5 el mínimo
                # para cachear es 4096 tokens; el prompt está diseñado
                # para superarlo (categorías + ejemplos + reglas).
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{"role": "user", "content": user_content}],
        )

        # Extraer texto de la respuesta
        text = ""
        for block in response.content:
            if block.type == "text":
                text += block.text
        slug = text.strip().lower()
        # Limpiar posible markdown/prefijos
        slug = slug.split()[0] if slug else ""
        slug = slug.strip(" .`*\"'")

        # Log de caching para debug / costo tracking
        u = response.usage
        if u.cache_read_input_tokens > 0:
            logger.debug(
                "Haiku cache HIT: read=%d write=%d input=%d output=%d",
                u.cache_read_input_tokens, u.cache_creation_input_tokens,
                u.input_tokens, u.output_tokens,
            )
        elif u.cache_creation_input_tokens > 0:
            logger.info(
                "Haiku cache WRITE (prefijo nuevo): %d tokens (pagados a 1.25x)",
                u.cache_creation_input_tokens,
            )
        else:
            logger.debug(
                "Haiku sin caching (prefijo < 4096 tokens?): input=%d output=%d",
                u.input_tokens, u.output_tokens,
            )

        if slug in CATEGORIAS_VALIDAS:
            return slug
        logger.warning("Haiku devolvió slug no válido: %r → %r", text, slug)
        return None

    except anthropic.RateLimitError:
        logger.warning("Haiku rate limit hit, skipping reclasificación")
        return None
    except anthropic.APIStatusError as e:
        logger.warning("Haiku API error %d: %s", e.status_code, e.message[:200])
        return None
    except anthropic.APIConnectionError as e:
        logger.warning("Haiku connection error: %s", str(e)[:200])
        return None
    except Exception as e:
        logger.error("Haiku llamada falló inesperadamente: %s", e)
        return None


# ──────────────────────────────────────────────────────────────────────
# API pública
# ──────────────────────────────────────────────────────────────────────
def reclasificar(
    titulo: str,
    resumen: str = "",
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[str]:
    """
    Reclasifica (título, resumen) usando Haiku 4.5.
    Caché SQLite si se pasa `conn`. Devuelve slug válido o None.

    "ninguna" es una respuesta legítima (significa: no aplica a política
    mexicana). El caller puede tratarla como "no clasificar" si quiere.
    """
    if not titulo or len(titulo.strip()) < 5:
        return None

    # Sin SDK ni API key → no podemos reclasificar
    if not _SDK_AVAILABLE or not os.getenv("ANTHROPIC_API_KEY"):
        logger.debug("Reclasificador deshabilitado (SDK o API key ausente)")
        return None

    hash_in = _hash_input(titulo, resumen)

    # Consulta caché
    if conn is not None:
        try:
            crear_tabla_cache(conn)
            cached = _cache_get(conn, hash_in)
            if cached is not None:
                logger.debug("Reclasificador cache hit: %s → %s", hash_in[:10], cached)
                return cached
        except sqlite3.Error as e:
            logger.warning("SQLite cache error (continúo sin caché): %s", e)

    # Llamada a Haiku
    slug = _llamar_haiku(titulo, resumen)
    if slug is None:
        return None

    # Guardar en caché
    if conn is not None:
        try:
            _cache_put(conn, hash_in, slug, titulo)
        except sqlite3.Error as e:
            logger.warning("SQLite cache write error: %s", e)

    return slug


# ──────────────────────────────────────────────────────────────────────
# CLI de prueba
# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if len(sys.argv) < 2:
        print("Uso: python reclasificador_claude.py \"<título>\" [\"<resumen>\"]")
        sys.exit(1)

    titulo = sys.argv[1]
    resumen = sys.argv[2] if len(sys.argv) > 2 else ""

    # Usar BD principal como caché si existe
    ROOT = Path(__file__).resolve().parent.parent
    db_path = ROOT / "semaforo.db"
    conn = sqlite3.connect(str(db_path)) if db_path.exists() else None

    print(f"\nTítulo: {titulo}")
    if resumen:
        print(f"Resumen: {resumen[:120]}")
    print(f"Modelo: {MODEL}")
    print()

    slug = reclasificar(titulo, resumen, conn)
    print(f"→ {slug if slug else '(sin resultado)'}")
