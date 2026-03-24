"""
Semáforo Legislativo - Configuración Central
Sistema predictivo: evento mediático → presión sostenida → iniciativa legislativa
"""

# ─────────────────────────────────────────────
# MEDIOS A MONITOREAR (14 fuentes RSS)
# ─────────────────────────────────────────────
MEDIOS = {
    "animal_politico": {
        "nombre": "Animal Político",
        "rss": "",  # RSS eliminado por el medio; cubierto por medios_html.py
        "peso": 1.2,  # Mayor peso por enfoque político
    },
    "el_universal": {
        "nombre": "El Universal",
        "rss": "https://www.eluniversal.com.mx/arc/outboundfeeds/rss/?outputType=xml",
        "peso": 1.1,
    },
    "el_economista": {
        "nombre": "El Economista",
        "rss": "https://www.eleconomista.com.mx/rss/",
        "peso": 1.0,
    },
    "la_jornada": {
        "nombre": "La Jornada",
        "rss": "https://www.jornada.com.mx/rss/edicion.xml",
        "peso": 1.1,
    },
    "milenio": {
        "nombre": "Milenio",
        "rss": "",  # RSS eliminado por el medio; cubierto por medios_html.py
        "peso": 1.0,
    },
    "proceso": {
        "nombre": "Proceso",
        "rss": "",  # RSS eliminado por el medio; cubierto por medios_html.py
        "peso": 1.2,
    },
    "excelsior": {
        "nombre": "Excélsior",
        "rss": "",  # RSS eliminado por el medio; cubierto por medios_html.py
        "peso": 1.0,
    },
    "la_razon": {
        "nombre": "La Razón",
        "rss": "https://www.razon.com.mx/arc/outboundfeeds/rss/?outputType=xml",
        "peso": 0.9,
    },
    "el_heraldo": {
        "nombre": "El Heraldo",
        "rss": "",  # RSS eliminado por el medio; cubierto por medios_html.py
        "peso": 0.8,
    },
    "24_horas": {
        "nombre": "24 Horas",
        "rss": "https://www.24-horas.mx/feed/",
        "peso": 0.8,
    },
    "cronica": {
        "nombre": "Crónica",
        "rss": "https://www.cronica.com.mx/arc/outboundfeeds/rss/?outputType=xml",
        "peso": 0.8,
    },
    "sol_de_mexico": {
        "nombre": "El Sol de México",
        "rss": "",  # RSS eliminado por el medio; cubierto por medios_html.py
        "peso": 0.8,
    },
    "nyt": {
        "nombre": "New York Times (México)",
        "rss": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
        "peso": 1.3,  # Peso alto: cobertura internacional presiona más
    },
    "ovaciones": {
        "nombre": "Ovaciones",
        "rss": "https://www.ovaciones.com/feed/",
        "peso": 0.7,
    },
    "bloomberg_linea": {
        "nombre": "Bloomberg Línea",
        "rss": "https://www.bloomberglinea.com/arc/outboundfeeds/rss/?outputType=xml",
        "peso": 1.3,  # Peso alto: cobertura financiera/política de calidad
    },
    "el_financiero": {
        "nombre": "El Financiero",
        "rss": "https://www.elfinanciero.com.mx/arc/outboundfeeds/rss/?outputType=xml",
        "peso": 1.2,  # Peso alto: cobertura política/económica de referencia
    },
    "politico_mx": {
        "nombre": "Político.mx",
        "rss": "https://www.politico.mx/arc/outboundfeeds/rss/?outputType=xml",
        "peso": 1.3,  # Peso alto: medio especializado en política mexicana
    },
    "la_politica_online": {
        "nombre": "La Política Online",
        "rss": "",
        "peso": 1.1,  # Cobertura política con fuentes internas
    },
    "el_pais": {
        "nombre": "El País",
        "rss": "",  # Sin RSS público para sección México; cubierto por medios_html.py
        "peso": 1.3,  # Peso alto: cobertura internacional de referencia
    },
}

# ─────────────────────────────────────────────
# CUENTAS DE TWITTER / X A MONITOREAR
# ─────────────────────────────────────────────
TWITTER_ACCOUNTS = [
    {
        "handle": "@letroblesrosa",
        "nombre": "Leti Robles Rosa",
        "medio": "Reforma",
        "peso": 1.2,
    },
    {
        "handle": "@PollsMX_",
        "nombre": "Polls MX",
        "medio": "Encuestas y opinión pública",
        "peso": 1.0,
    },
    {
        "handle": "@MLopezSanMartin",
        "nombre": "Manuel López San Martín",
        "medio": "Periodista político",
        "peso": 1.1,
    },
    {
        "handle": "@SaraPantoja",
        "nombre": "Sara Pantoja",
        "medio": "Proceso",
        "peso": 1.1,
    },
    {
        "handle": "@ivonnemelgar",
        "nombre": "Ivonne Melgar",
        "medio": "Excélsior",
        "peso": 1.1,
    },
    {
        "handle": "@Juan_OrtizMX",
        "nombre": "Juan Ortiz",
        "medio": "Freelance / político",
        "peso": 1.0,
    },
    # ── Coordinadores parlamentarios ──
    # Cámara de Diputados
    {
        "handle": "@RicardoMonrealA",
        "nombre": "Ricardo Monreal Ávila",
        "medio": "Coord. Morena – Diputados",
        "peso": 1.5,
    },
    {
        "handle": "@JorgeRoHe",
        "nombre": "Jorge Romero Herrera",
        "medio": "Coord. PAN – Diputados",
        "peso": 1.4,
    },
    {
        "handle": "@rubenmoreiravdz",
        "nombre": "Rubén Moreira Valdez",
        "medio": "Coord. PRI – Diputados",
        "peso": 1.4,
    },
    {
        "handle": "@CarlosPuenteZAC",
        "nombre": "Carlos Alberto Puente Salas",
        "medio": "Coord. PVEM – Diputados",
        "peso": 1.3,
    },
    {
        "handle": "@ReginaldoSF",
        "nombre": "Reginaldo Sandoval Flores",
        "medio": "Coord. PT – Diputados",
        "peso": 1.3,
    },
    {
        "handle": "@AlvarezMaynez",
        "nombre": "Jorge Álvarez Máynez",
        "medio": "Coord. MC – Diputados",
        "peso": 1.3,
    },
    # Senado
    {
        "handle": "@NachoMierV",
        "nombre": "Ignacio Mier Velazco",
        "medio": "Coord. Morena – Senado",
        "peso": 1.5,
    },
    {
        "handle": "@RicardoAnayaC",
        "nombre": "Ricardo Anaya Cortés",
        "medio": "Coord. PAN – Senado",
        "peso": 1.4,
    },
    {
        "handle": "@ManuelAnorve",
        "nombre": "Manuel Añorve Baños",
        "medio": "Coord. PRI – Senado",
        "peso": 1.4,
    },
    {
        "handle": "@AlbertoAnayaG",
        "nombre": "Alberto Anaya Gutiérrez",
        "medio": "Coord. PT – Senado",
        "peso": 1.3,
    },
    {
        "handle": "@VelascoM_",
        "nombre": "Manuel Velasco Coello",
        "medio": "Coord. PVEM – Senado",
        "peso": 1.3,
    },
    {
        "handle": "@ClementeCH",
        "nombre": "Clemente Castañeda Hoeflich",
        "medio": "Coord. MC – Senado",
        "peso": 1.3,
    },
    {
        "handle": "@RoDeleyja",
        "nombre": "Rodrigo Deleyja",
        "medio": "Periodista político",
        "peso": 1.0,
    },
    {
        "handle": "@RutaElectoralmx",
        "nombre": "Ruta Electoral MX",
        "medio": "Análisis electoral",
        "peso": 1.0,
    },
]

# Twitter/X API v2 — Bearer Token (PPU plan)
import os
TWITTER_BEARER_TOKEN = os.environ.get("TWITTER_BEARER_TOKEN", "")

# ─────────────────────────────────────────────
# 17 CATEGORÍAS LEGISLATIVAS CON SUBCATEGORÍAS
# ─────────────────────────────────────────────
CATEGORIAS = {
    "seguridad_justicia": {
        "nombre": "Seguridad y Justicia",
        "comisiones": [
            # Diputados
            "Justicia", "Seguridad Pública", "Defensa Nacional",
            # Senado LXVI
            "Justicia", "Seguridad Pública", "Defensa Nacional",
            "Guardia Nacional", "Marina",
        ],
        "trends_keywords": ["inseguridad", "guardia nacional", "narcotráfico", "feminicidio", "crimen organizado"],
        "subcategorias": {
            "seguridad_publica": {
                "nombre": "Seguridad pública y policía",
                "keywords": ["policía", "guardia nacional", "seguridad pública", "operativo", "patrullaje", "estrategia de seguridad",
                             "balacera", "enfrentamiento armado", "emboscada", "levantón", "toque de queda", "militarización",
                             "autodefensa", "grupo armado", "célula criminal", "abuso policial", "ejecución extrajudicial"],
            },
            "justicia_penal": {
                "nombre": "Justicia penal y reforma judicial",
                "keywords": ["código penal", "ministerio público", "proceso penal", "jueces", "fiscalía", "reforma judicial", "poder judicial", "penal",
                             "código civil", "Ley de Amparo", "solución de controversias",
                             "Norma Piña", "ministro", "ministra", "magistrado", "amparo", "suspensión judicial",
                             "tribunal", "juzgado", "sentencia", "Arturo Zaldívar", "reforma al poder judicial"],
            },
            "crimen_organizado": {
                "nombre": "Crimen organizado",
                "keywords": ["narcotráfico", "crimen organizado", "extorsión", "secuestro", "cártel", "fentanilo", "delito",
                             "huachicol", "robo de combustible", "célula criminal", "plaza", "sicario", "halcón"],
            },
            "fuerzas_armadas": {
                "nombre": "Fuerzas armadas y defensa",
                "keywords": ["sedena", "semar", "fuerzas armadas", "defensa nacional", "seguridad nacional", "ejército", "marina"],
            },
            "sistema_penitenciario": {
                "nombre": "Sistema penitenciario",
                "keywords": ["cárcel", "prisión", "sistema penitenciario", "reclusorio", "reinserción social"],
            },
            "violencia_victimas": {
                "nombre": "Violencia y víctimas",
                "keywords": ["homicidio", "feminicidio", "desaparición forzada", "violencia", "impunidad", "víctimas",
                             "fosa clandestina", "fosas", "asesinato", "muertos", "restos humanos",
                             "desaparecidos", "persona desaparecida", "búsqueda de personas", "tortura", "detención arbitraria",
                             "cadáver", "cadáveres", "cuerpo sin vida", "cuerpos sin vida",
                             "abandonan cuerpo", "abandonan tres", "localizan cuerpo", "hallan cuerpo",
                             "agresor sexual", "agresores sexuales", "abuso sexual", "abusador", "abusadores",
                             "delito sexual", "delitos sexuales", "violación sexual", "pederastia", "pedofilia",
                             "registro de agresores", "trata de personas", "acoso sexual"],
            },
            "justicia_constitucional": {
                "nombre": "Justicia constitucional",
                "keywords": ["suprema corte", "SCJN", "controversia constitucional", "acción de inconstitucionalidad",
                             "constitucionalidad", "división de poderes", "tribunal constitucional",
                             "controversia", "amparo colectivo"],
            },
        },
    },
    "economia_hacienda": {
        "nombre": "Economía y Hacienda",
        "comisiones": [
            # Diputados
            "Hacienda y Crédito Público", "Economía, Comercio y Competitividad",
            "Presupuesto y Cuenta Pública",
            # Senado LXVI
            "Hacienda y Crédito Público", "Economía",
            "Fomento Económico y al Emprendimiento",
            "Defensa de los Consumidores",
            "Desarrollo Regional",
        ],
        "trends_keywords": ["inflación", "SAT impuestos", "peso mexicano", "economía México", "deuda pública"],
        "subcategorias": {
            "politica_fiscal": {
                "nombre": "Política fiscal y recaudación",
                "keywords": ["impuestos", "SAT", "reforma fiscal", "IEPS", "IVA", "ISR", "recaudación", "contribuyente",
                             "evasión fiscal", "coordinación fiscal", "código fiscal",
                             "SHCP", "Secretaría de Hacienda", "miscelánea fiscal", "Paquete Económico", "Ley de Ingresos"],
            },
            "presupuesto_gasto": {
                "nombre": "Presupuesto y gasto público",
                "keywords": ["presupuesto", "gasto público", "PEF", "deuda pública", "déficit", "austeridad", "deuda",
                             "Rogelio Ramírez de la O", "secretario de hacienda", "subasta", "Cetes", "bonos gubernamentales"],
            },
            "politica_monetaria": {
                "nombre": "Política monetaria y macroeconomía",
                "keywords": ["inflación", "tipo de cambio", "peso mexicano", "Banxico", "tasa de interés", "PIB", "Banco de México",
                             "recesión", "crecimiento económico", "estancamiento", "precio del dólar", "depreciación",
                             "volatilidad cambiaria", "calificadora", "Moody's", "Standard & Poor's", "Fitch",
                             "riesgo país", "mercados financieros", "bolsa mexicana"],
            },
            "comercio_exterior": {
                "nombre": "Comercio exterior e inversión",
                "keywords": ["aranceles", "comercio exterior", "inversión extranjera", "exportación", "importación", "balanza comercial",
                             "nearshoring", "relocalización"],
            },
            "fomento_economico": {
                "nombre": "Fomento económico",
                "keywords": ["economía", "inversión pública", "fomento económico", "emprendimiento", "Pymes", "competitividad", "hacienda", "política fiscal",
                             "Protección al Consumidor",
                             "clase media", "poder adquisitivo", "canasta básica"],
            },
            "competencia_mercados": {
                "nombre": "Competencia y mercados",
                "keywords": ["competencia económica", "Cofece", "monopolio", "prácticas monopólicas",
                             "regulación de mercado", "control de precios", "libre competencia"],
            },
        },
    },
    "energia": {
        "nombre": "Energía",
        "comisiones": [
            # Diputados
            "Energía",
            # Senado LXVI
            "Energía", "Minería",
        ],
        "trends_keywords": ["Pemex", "CFE", "precio gasolina", "luz eléctrica", "apagón"],
        "subcategorias": {
            "pemex_petroleo": {
                "nombre": "Pemex y petróleo",
                "keywords": ["Pemex", "petróleo", "hidrocarburos", "gasolina", "refinería", "producción petrolera",
                             "Octavio Romero", "director de Pemex", "plataforma petrolera", "producción de barriles",
                             "precio del petróleo", "mezcla mexicana", "huachicoleo", "toma clandestina", "fracking", "perforación"],
            },
            "cfe_electricidad": {
                "nombre": "CFE y electricidad",
                "keywords": ["CFE", "electricidad", "tarifas eléctricas", "apagón", "generación eléctrica", "subsidio energético",
                             "apagón masivo", "corte de luz", "falla eléctrica", "contrato de electricidad", "tarifa doméstica"],
            },
            "energias_renovables": {
                "nombre": "Energías renovables",
                "keywords": ["renovable", "solar", "eólica", "transición energética", "energía limpia"],
            },
            "mineria_recursos": {
                "nombre": "Minería y recursos",
                "keywords": ["litio", "minería", "concesión minera", "gas natural", "gas LP", "gasoducto", "soberanía energética",
                             "reforma energética", "Rocío Nahle", "secretaria de energía"],
            },
        },
    },
    "salud": {
        "nombre": "Salud",
        "comisiones": [
            # Diputados
            "Salud",
            # Senado LXVI
            "Salud", "Seguridad Social",
        ],
        "trends_keywords": ["IMSS", "medicamentos", "salud pública", "hospitales México", "vacuna"],
        "subcategorias": {
            "sistema_salud": {
                "nombre": "Sistema de salud",
                "keywords": ["IMSS", "ISSSTE", "IMSS-Bienestar", "hospital", "clínica", "atención médica", "INSABI",
                             "doctor", "médico", "enfermera", "personal de salud", "lista de espera", "cirugía",
                             "urgencias", "sector salud", "sistema de salud pública", "consultorio",
                             "primer nivel de atención", "Alcocer", "secretario de salud", "seguro popular"],
            },
            "medicamentos_abasto": {
                "nombre": "Medicamentos y abasto",
                "keywords": ["medicamento", "desabasto", "farmacia", "vacuna", "compra consolidada",
                             "receta médica", "sustancia química", "sustancias químicas", "registro de sustancias",
                             "sustancia tóxica", "sustancias tóxicas", "producto químico", "productos químicos",
                             "químicos peligrosos", "cofepris", "regulación sanitaria", "control sanitario"],
            },
            "salud_mental_adicciones": {
                "nombre": "Salud mental y adicciones",
                "keywords": ["salud mental", "adicciones", "fentanilo", "rehabilitación", "prevención de adicciones"],
            },
            "epidemiologia": {
                "nombre": "Epidemiología",
                "keywords": ["epidemia", "pandemia", "enfermedad", "brote", "vigilancia epidemiológica",
                             "alerta epidemiológica", "tamizaje",
                             "OMS", "Organización Mundial de la Salud", "dengue", "influenza", "covid",
                             "cáncer", "diabetes", "obesidad", "hipertensión"],
            },
        },
    },
    "educacion": {
        "nombre": "Educación",
        "comisiones": [
            # Diputados
            "Educación", "Ciencia, Tecnología e Innovación",
            # Senado LXVI
            "Educación",
            "Ciencia, Humanidades, Tecnología e Innovación",
            "Cultura", "Juventud", "Deporte",
        ],
        "trends_keywords": ["SEP", "UNAM", "becas", "libros de texto", "escuelas México"],
        "subcategorias": {
            "educacion_basica": {
                "nombre": "Educación básica",
                "keywords": ["SEP", "escuela", "maestro", "profesor", "libros de texto", "rezago educativo", "deserción escolar",
                             "Ley General de Educación", "educación", "uniforme escolar",
                             "CNTE", "sindicato de maestros", "SNTE", "paro de maestros", "plantón magisterial",
                             "plan de estudios", "currículo", "nueva escuela mexicana", "evaluación docente",
                             "carrera magisterial", "infraestructura escolar", "acoso escolar", "bullying",
                             "Leticia Ramírez", "secretaria de educación", "inscripción", "ciclo escolar", "calendario escolar"],
            },
            "educacion_superior": {
                "nombre": "Educación superior e investigación",
                "keywords": ["universidad", "UNAM", "IPN", "Conahcyt", "CONACYT", "investigación", "becas", "posgrado",
                             "beca universal"],
            },
            "ciencia_tecnologia": {
                "nombre": "Ciencia y tecnología",
                "keywords": ["ciencia", "tecnología", "innovación", "desarrollo tecnológico", "patente"],
            },
            "cultura_deporte": {
                "nombre": "Cultura y deporte",
                "keywords": ["cultura", "patrimonio cultural", "juventud", "deporte", "biblioteca"],
            },
        },
    },
    "trabajo": {
        "nombre": "Trabajo",
        "comisiones": [
            # Diputados
            "Trabajo y Previsión Social",
            # Senado LXVI
            "Trabajo y Previsión Social", "Seguridad Social",
        ],
        "trends_keywords": ["empleo México", "salario mínimo", "desempleo", "pensiones", "AFORE"],
        "subcategorias": {
            "empleo_salario": {
                "nombre": "Empleo y salarios",
                "keywords": ["empleo", "salario", "salario mínimo", "desempleo", "informalidad", "mercado laboral",
                             "despido", "liquidación", "indemnización", "empleo juvenil", "primer empleo", "reforma laboral"],
            },
            "derechos_laborales": {
                "nombre": "Derechos laborales",
                "keywords": ["jornada laboral", "prestaciones", "aguinaldo", "vacaciones dignas", "subcontratación", "outsourcing",
                             "derechos laborales", "Ley Federal del Trabajo", "permisos laborales",
                             "trabajo remoto", "teletrabajo", "home office", "plataforma digital",
                             "trabajador de aplicación", "accidente laboral", "riesgo de trabajo",
                             "brecha salarial", "igualdad salarial"],
            },
            "seguridad_social_pensiones": {
                "nombre": "Seguridad social y pensiones",
                "keywords": ["pensión", "AFORE", "seguridad social", "jubilación", "retiro", "Seguro Social", "ISSSTE"],
            },
            "relaciones_laborales": {
                "nombre": "Relaciones laborales",
                "keywords": ["sindicato", "huelga", "STPS", "contrato colectivo", "conflicto laboral",
                             "Marath Bolaños", "secretario del trabajo", "inspección laboral", "Profedet"],
            },
        },
    },
    "electoral_politico": {
        "nombre": "Electoral y Político",
        "comisiones": [
            # Diputados
            "Gobernación y Población", "Reforma Política-Electoral",
            # Senado LXVI
            "Gobernación", "Puntos Constitucionales",
            "Participación Ciudadana",
            "Estudios Legislativos",
            "Estudios Legislativos, Primera",
            "Estudios Legislativos, Segunda",
            "Federalismo",
            "Desarrollo Municipal",
            "Reglamentos y Prácticas Parlamentarias",
            "Medalla Belisario Domínguez",
        ],
        "trends_keywords": ["elecciones México", "INE", "Morena partido", "congreso México", "reforma electoral"],
        "subcategorias": {
            "reforma_electoral": {
                "nombre": "Reforma electoral",
                "keywords": ["reforma electoral", "INE", "TEPJF", "voto", "campaña electoral", "elección", "casilla",
                             "jornada electoral", "proceso electoral", "padrón electoral", "credencial de elector",
                             "lista nominal", "PREP", "conteo rápido", "resultados electorales", "urna", "boleta",
                             "casilla electoral", "distrito electoral", "circunscripción", "fiscalización de campañas",
                             "Guadalupe Taddei", "consejero electoral", "consejera electoral", "tribunal electoral",
                             "impugnación electoral", "tómbola", "insaculación"],
            },
            "partidos_coaliciones": {
                "nombre": "Partidos y coaliciones",
                "keywords": ["coalición legislativa", "oposición parlamentaria",
                             "partido político", "financiamiento de partidos",
                             "Ley de Partidos Políticos", "alianza electoral",
                             "fracción parlamentaria", "grupo parlamentario",
                             "mayoría calificada", "mayoría simple", "mayoría absoluta",
                             "sobrerrepresentación", "plurinominal", "representación proporcional",
                             "coalición opositora", "bloque legislativo", "pacto político"],
            },
            "gobernabilidad": {
                "nombre": "Gobernabilidad",
                "keywords": ["gobernabilidad", "sistema político", "crisis política", "desafuero",
                             "juicio político", "división de poderes", "Ley Orgánica del Congreso",
                             "periodo de sesiones", "sesión plenaria", "tribuna", "coordinador parlamentario",
                             "líder parlamentario", "ejecutivo federal",
                             "crisis de gobierno", "ingobernabilidad", "vacío de poder"],
            },
            "participacion_ciudadana": {
                "nombre": "Participación ciudadana",
                "keywords": ["consulta popular", "revocación de mandato", "democracia", "referéndum", "plebiscito",
                             "encuesta", "preferencia electoral", "intención de voto", "precampaña", "proselitismo",
                             "candidatura", "candidato", "candidata", "debate", "debate presidencial",
                             "gubernatura", "gobernador", "gobernadora", "alcalde", "alcaldesa", "elección intermedia"],
            },
        },
    },
    "derechos_humanos": {
        "nombre": "Derechos Humanos",
        "comisiones": [
            # Diputados
            "Derechos Humanos", "Igualdad de Género",
            "Asuntos Migratorios",
            # Senado LXVI
            "Derechos Humanos",
            "Derechos de la Niñez y de la Adolescencia",
            "Derechos Digitales",
            "Para la Igualdad de Género",
            "Asuntos Migratorios",
            "Pueblos Indígenas y Afromexicanos",
        ],
        "trends_keywords": ["derechos humanos México", "migración México", "feminismo", "discriminación", "CNDH"],
        "subcategorias": {
            "derechos_fundamentales": {
                "nombre": "Derechos fundamentales",
                "keywords": ["derechos humanos", "CNDH", "discriminación", "igualdad", "libertad de expresión", "defensor",
                             "Rosario Piedra", "ombudsman", "detención arbitraria", "preso político", "activista",
                             "defensor de derechos humanos", "Corte Interamericana", "CIDH"],
            },
            "genero_diversidad": {
                "nombre": "Género y diversidad",
                "keywords": ["género", "violencia de género", "aborto", "diversidad sexual", "LGBT", "feminismo",
                             "identidad de género", "matrimonio igualitario"],
            },
            "migracion_refugio": {
                "nombre": "Migración y refugio",
                "keywords": ["migración", "migrante", "refugiado", "asilo", "deportación", "caravana migrante",
                             "persona desplazada", "desplazamiento forzado", "trata de personas", "tráfico de personas"],
            },
            "pueblos_indigenas": {
                "nombre": "Pueblos indígenas",
                "keywords": ["indígena", "pueblos originarios", "lengua indígena", "autonomía indígena", "afromexicano"],
            },
            "derechos_ninez": {
                "nombre": "Derechos de la niñez",
                "keywords": ["niñez", "adolescencia", "trabajo infantil", "adopción", "menor de edad",
                             "matrimonio infantil", "matrimonio forzado"],
            },
        },
    },
    "infraestructura": {
        "nombre": "Infraestructura",
        "comisiones": [
            # Diputados
            "Infraestructura", "Comunicaciones y Transportes",
            "Recursos Hidráulicos, Agua Potable y Saneamiento",
            # Senado LXVI
            "Comunicaciones y Transportes",
            "Infraestructura Ferroviaria",
            "Puertos e Infraestructura Marítima",
            "Zonas Metropolitanas y Movilidad",
            "Desarrollo Urbano y Ordenamiento Territorial",
            "Recursos Hídricos e Infraestructura Hidráulica",
            "Reordenamiento Urbano y Vivienda",
            "Desarrollo Municipal",
            "Desarrollo Regional",
        ],
        "trends_keywords": ["Tren Maya", "AIFA", "agua potable", "vivienda", "carreteras México"],
        "subcategorias": {
            "transporte_movilidad": {
                "nombre": "Transporte y movilidad",
                "keywords": ["sistema de transporte", "red de transporte", "Tren Maya", "tren interurbano", "tren suburbano", "carretera", "aeropuerto", "AIFA",
                             "seguridad vial", "movilidad", "proyectos carreteros", "carretero", "red carretera",
                             "transporte público", "Metro", "Metrobús", "Línea 12", "autopista", "cuota", "peaje",
                             "Capufe", "bache", "socavón",
                             "aviación", "aviación civil", "Ley de Aviación", "autotransporte", "autotransporte federal",
                             "caminos y puentes", "Ley de Caminos", "transporte federal", "transporte terrestre",
                             "transporte aéreo", "transporte marítimo", "cabotaje", "ferrocarril", "vía férrea"],
            },
            "obra_publica": {
                "nombre": "Obra pública",
                "keywords": ["obra pública", "obra de infraestructura", "corredor interoceánico", "licitación de obra", "puente",
                             "concesión", "APP", "asociación público privada", "Jorge Nuño Lara", "secretario de infraestructura"],
            },
            "agua_saneamiento": {
                "nombre": "Agua y saneamiento",
                "keywords": ["Conagua", "abastecimiento de agua", "crisis hídrica", "drenaje", "saneamiento", "presa",
                             "acueducto", "tubería", "fuga de agua", "corte de agua", "tandeo", "desabasto de agua"],
            },
            "vivienda_urbano": {
                "nombre": "Vivienda y desarrollo urbano",
                "keywords": ["vivienda", "vivienda social", "Infonavit", "desarrollo urbano", "ordenamiento territorial",
                             "Conavi", "crédito de vivienda", "hipoteca", "Ley de Vivienda",
                             "vivienda digna", "vivienda adecuada", "vivienda popular",
                             "Fovissste", "subsidio de vivienda", "rezago habitacional"],
            },
            "telecomunicaciones": {
                "nombre": "Telecomunicaciones",
                "keywords": ["telecomunicaciones", "banda ancha", "conectividad", "cobertura digital"],
            },
            "proteccion_civil": {
                "nombre": "Protección civil y desastres",
                "keywords": ["terremoto", "sismo", "inundación", "huracán", "desastre natural", "protección civil",
                             "reconstrucción", "declaratoria de emergencia", "damnificados", "derrumbe", "evacuación"],
            },
            "megaproyectos": {
                "nombre": "Megaproyectos federales",
                "keywords": ["Tren Maya", "corredor interoceánico", "Dos Bocas", "AIFA",
                             "megaproyecto", "proyecto estratégico", "infraestructura federal"],
            },
        },
    },
    "agro_rural": {
        "nombre": "Agro y Desarrollo Rural",
        "comisiones": [
            # Diputados
            "Desarrollo y Conservación Rural, Agrícola y Autosuficiencia Alimentaria",
            # Senado LXVI
            "Agricultura", "Ganadería", "Desarrollo Rural",
            "Pesca y Acuacultura",
            "Reforma Agraria",
            "Autosuficiencia Alimentaria",
        ],
        "trends_keywords": ["agricultura México", "maíz", "campo mexicano", "fertilizante", "precio tortilla"],
        "subcategorias": {
            "agricultura_cultivos": {
                "nombre": "Agricultura y cultivos",
                "keywords": ["agricultura", "maíz", "glifosato", "transgénico", "fertilizante", "cosecha", "Segalmex", "soberanía alimentaria",
                             "sequía agrícola", "helada", "granizada", "pérdida de cosecha", "precio del maíz",
                             "precio del frijol", "tortilla", "Sader", "secretario de agricultura", "riego",
                             "temporal", "distrito de riego", "plaga", "importación de maíz", "maíz transgénico"],
            },
            "ganaderia_pesca": {
                "nombre": "Ganadería y pesca",
                "keywords": ["ganadería", "pesca", "acuacultura", "producción pecuaria",
                             "ganado", "producción lechera", "rastro", "jornalero", "trabajador agrícola"],
            },
            "desarrollo_rural_tierra": {
                "nombre": "Desarrollo rural y tierra",
                "keywords": ["campo mexicano", "campesino", "ejido", "tenencia de tierra", "reforma agraria", "Sembrando Vida",
                             "desarrollo rural"],
            },
            "comercio_agropecuario": {
                "nombre": "Comercio agropecuario",
                "keywords": ["productos agropecuarios", "exportación agropecuaria", "certificación agropecuaria", "precio de garantía"],
            },
        },
    },
    "relaciones_exteriores": {
        "nombre": "Relaciones Exteriores",
        "comisiones": [
            # Diputados
            "Relaciones Exteriores",
            # Senado LXVI
            "Relaciones Exteriores",
            "Relaciones Exteriores América del Norte",
            "Relaciones Exteriores América Latina y el Caribe",
            "Relaciones Exteriores Asia-Pacífico",
            "Relaciones Exteriores Europa",
            "Relaciones Exteriores África",
            "Organismos Internacionales",
            "Seguimiento a la Implementación y Revisión del T-MEC",
            "Asuntos de la Frontera Norte",
            "Asuntos de la Frontera Sur",
        ],
        "trends_keywords": ["T-MEC", "aranceles México", "frontera México", "Estados Unidos México", "deportación"],
        "subcategorias": {
            "relacion_eeuu": {
                "nombre": "Relación con EE.UU.",
                "keywords": ["Estados Unidos", "T-MEC", "frontera", "aranceles", "relación bilateral",
                             "Trump", "Biden", "Casa Blanca", "embajador", "Ken Salazar", "arancel",
                             "guerra comercial", "proteccionismo", "fentanilo frontera", "tráfico transnacional",
                             "cooperación bilateral", "cumbre", "reunión bilateral"],
            },
            "diplomacia_organismos": {
                "nombre": "Diplomacia y organismos",
                "keywords": ["diplomacia", "embajada", "ONU", "cancillería", "SRE", "consulado", "tratado", "organismos internacionales",
                             "Alicia Bárcena", "canciller", "secretaria de relaciones", "visa", "pasaporte",
                             "cita consular", "sanciones", "lista negra", "OFAC"],
            },
            "america_latina": {
                "nombre": "América Latina",
                "keywords": ["América Latina", "integración regional", "CELAC", "alianza del pacífico",
                             "Centroamérica", "Guatemala", "Honduras"],
            },
            "soberania_fronteras": {
                "nombre": "Soberanía y fronteras",
                "keywords": ["soberanía nacional", "intervención extranjera", "extradición", "deportación", "frontera norte", "frontera sur",
                             "remesas", "paisano", "diáspora mexicana"],
            },
        },
    },
    "anticorrupcion": {
        "nombre": "Anticorrupción",
        "comisiones": [
            # Diputados
            "Transparencia y Anticorrupción",
            # Senado LXVI
            "Anticorrupción y Transparencia",
            "Jurisdiccional",
        ],
        "trends_keywords": ["corrupción México", "transparencia", "INAI", "lavado de dinero", "auditoría"],
        "subcategorias": {
            "sistema_anticorrupcion": {
                "nombre": "Sistema anticorrupción",
                "keywords": ["corrupción", "Sistema Nacional Anticorrupción", "SNA", "ASF", "auditoría", "fiscalización",
                             "moches", "desvío de recursos", "peculado", "impunidad", "expediente",
                             "carpeta de investigación", "Función Pública", "secretario de la función pública"],
            },
            "transparencia_acceso": {
                "nombre": "Transparencia y acceso a información",
                "keywords": ["transparencia", "INAI", "acceso a la información", "rendición de cuentas", "datos abiertos",
                             "denuncia ciudadana", "denunciante"],
            },
            "delitos_financieros": {
                "nombre": "Delitos financieros",
                "keywords": ["lavado de dinero", "UIF", "enriquecimiento ilícito", "extinción de dominio", "soborno",
                             "empresa fantasma", "facturera", "Pablo Gómez", "Santiago Nieto", "patrimonio inexplicable"],
            },
            "etica_publica": {
                "nombre": "Ética pública",
                "keywords": ["conflicto de interés", "nepotismo", "declaración patrimonial", "contrato público", "licitación",
                             "contratación directa", "adjudicación directa"],
            },
        },
    },
    "medio_ambiente": {
        "nombre": "Medio Ambiente y Cambio Climático",
        "comisiones": [
            # Diputados
            "Medio Ambiente, Sustentabilidad, Cambio Climático y Recursos Naturales",
            "Cambio Climático y Sostenibilidad",
            # Senado LXVI
            "Medio Ambiente, Recursos Naturales y Cambio Climático",
        ],
        "trends_keywords": ["cambio climático", "contaminación", "sequía México", "incendio forestal", "calidad del aire"],
        "subcategorias": {
            "cambio_climatico": {
                "nombre": "Cambio climático",
                "keywords": ["cambio climático", "calentamiento global", "emisiones de carbono", "gases de efecto invernadero", "Acuerdo de París"],
            },
            "conservacion_biodiversidad": {
                "nombre": "Conservación y biodiversidad",
                "keywords": ["biodiversidad", "área natural protegida", "Semarnat", "Profepa", "deforestación", "especies en peligro",
                             "vida silvestre", "equilibrio ecológico", "ecosistema", "áreas naturales",
                             "jaguar", "tortuga marina", "manglar", "arrecife", "minería a cielo abierto",
                             "concesión minera", "ambientalista", "Greenpeace"],
            },
            "contaminacion_residuos": {
                "nombre": "Contaminación y residuos",
                "keywords": ["contaminación ambiental", "residuos peligrosos", "contingencia ambiental", "calidad del aire",
                             "economía circular", "ley ambiental", "norma ambiental", "impacto ambiental", "política ambiental",
                             "gestión de residuos", "residuos sólidos", "Gestión Integral de los Residuos",
                             "agua contaminada", "río contaminado", "derrame", "basura", "relleno sanitario",
                             "tiradero", "smog", "mala calidad del aire", "contingencia"],
            },
            "recursos_forestales": {
                "nombre": "Recursos forestales",
                "keywords": ["Conafor", "forestal", "reforestación", "tala", "silvicultura", "incendio forestal",
                             "productos forestales", "desarrollo forestal", "certificación ambiental",
                             "tala ilegal", "tala clandestina", "incendio", "temporada de incendios", "quema"],
            },
            "fenomenos_naturales": {
                "nombre": "Fenómenos naturales",
                "keywords": ["sequía", "ola de calor", "fenómeno natural"],
            },
        },
    },
    "inteligencia_artificial": {
        "nombre": "Inteligencia Artificial",
        "comisiones": [
            # Diputados
            "Ciencia, Tecnología e Innovación",
            # Senado LXVI
            "Análisis, Seguimiento y Evaluación sobre la aplicación y desarrollo de la Inteligencia Artificial en México",
            "Ciencia, Humanidades, Tecnología e Innovación",
            "Ciberseguridad",
            "Derechos Digitales",
        ],
        "trends_keywords": ["inteligencia artificial", "ciberseguridad", "datos personales", "deepfake", "ChatGPT"],
        "subcategorias": {
            "regulacion_ia": {
                "nombre": "Regulación de IA",
                "keywords": ["inteligencia artificial", "regulación de inteligencia artificial", "ley de inteligencia artificial",
                             "iniciativa inteligencia artificial", "ética de la inteligencia artificial",
                             "ChatGPT", "OpenAI", "Claude", "Gemini", "automatización", "robot", "empleo automatizado"],
            },
            "gobernanza_digital": {
                "nombre": "Gobernanza digital",
                "keywords": ["regulación tecnológica", "regulación algorítmica", "gobernanza digital",
                             "regulación de plataformas digitales", "sesgo algorítmico",
                             "brecha digital", "inclusión digital", "startup", "emprendimiento tecnológico"],
            },
            "ciberseguridad_datos": {
                "nombre": "Ciberseguridad y datos",
                "keywords": ["ley de ciberseguridad", "protección de datos personales", "ley de datos", "ciberataque", "privacidad digital",
                             "hackeo", "ransomware", "ataque informático", "reconocimiento facial",
                             "vigilancia masiva", "datos biométricos", "huella digital"],
            },
            "contenido_digital": {
                "nombre": "Contenido digital",
                "keywords": ["deepfake", "desinformación", "contenido sintético", "moderación de contenido"],
            },
        },
    },
    "politica_social": {
        "nombre": "Política Social",
        "comisiones": [
            # Senado LXVI
            "Bienestar",
        ],
        "trends_keywords": ["pensión bienestar", "programas sociales", "pobreza México", "becas Benito Juárez", "bienestar"],
        "subcategorias": {
            "programas_federales": {
                "nombre": "Programas federales",
                "keywords": ["programa social", "pensión bienestar", "beca benito juárez", "jóvenes construyendo el futuro",
                             "tandas para el bienestar", "Sembrando Vida"],
            },
            "pobreza_desigualdad": {
                "nombre": "Pobreza y desigualdad",
                "keywords": ["pobreza", "pobreza extrema", "desigualdad", "marginación", "rezago social", "Coneval", "carencia social"],
            },
            "grupos_vulnerables": {
                "nombre": "Grupos vulnerables",
                "keywords": ["adultos mayores", "personas con discapacidad", "vulnerabilidad", "asistencia social"],
            },
            "bienestar_desarrollo": {
                "nombre": "Bienestar y desarrollo social",
                "keywords": ["bienestar", "desarrollo social", "subsidio", "ingreso mínimo", "transferencia directa"],
            },
        },
    },
    "medios_comunicacion": {
        "nombre": "Medios de Comunicación",
        "comisiones": [
            # Diputados
            "Cultura y Cinematografía",
            # Senado LXVI
            "Radio, Televisión y Cinematografía",
        ],
        "trends_keywords": ["libertad de prensa", "periodistas México", "televisión", "censura", "IFT"],
        "subcategorias": {
            "regulacion_medios": {
                "nombre": "Regulación de medios",
                "keywords": ["regulación de medios", "ley de telecomunicaciones", "concesión de radiodifusión", "IFT",
                             "espectro radioeléctrico", "ley de radiodifusión",
                             "Ley en Materia de Telecomunicaciones", "telecomunicaciones y radiodifusión"],
            },
            "television_radio": {
                "nombre": "Televisión y radio",
                "keywords": ["televisión abierta", "televisión pública", "medio radiofónico", "concesión de televisión", "radiodifusión",
                             "transmisiones deportivas", "transmisión televisiva", "señal abierta",
                             "televisión de paga", "contenido audiovisual", "canal de televisión"],
            },
            "libertad_prensa": {
                "nombre": "Libertad de prensa",
                "keywords": ["censura mediática", "derecho a la información", "libertad de prensa", "periodista amenazado", "regulación de contenidos"],
            },
            "industria_audiovisual": {
                "nombre": "Industria audiovisual",
                "keywords": ["cinematografía", "producción audiovisual", "industria cinematográfica",
                             "cine mexicano", "cine nacional", "película", "guión cinematográfico",
                             "cine", "audiovisual", "Ley Federal de Cinematografía",
                             "Ley de Cine", "Ley Federal de Cine", "sector audiovisual",
                             "derechos de creadores", "productor cinematográfico", "cortometraje",
                             "largometraje", "documental cinematográfico", "IMCINE",
                             "estímulo fiscal cinematográfico", "EFICINE"],
            },
        },
    },
    "turismo": {
        "nombre": "Turismo",
        "comisiones": [
            # Senado LXVI
            "Turismo",
        ],
        "trends_keywords": ["turismo México", "hoteles Cancún", "pueblo mágico", "turistas", "vuelos baratos"],
        "subcategorias": {
            "politica_turistica": {
                "nombre": "Política turística",
                "keywords": ["turismo", "Sectur", "política turística", "turismo sustentable"],
            },
            "infraestructura_turistica": {
                "nombre": "Infraestructura turística",
                "keywords": ["hotel", "hotelería", "industria hotelera", "infraestructura turística", "ocupación hotelera", "crucero"],
            },
            "destinos_programas": {
                "nombre": "Destinos y programas",
                "keywords": ["destino turístico", "pueblo mágico", "ecoturismo", "turismo cultural", "turismo de naturaleza"],
            },
            "economia_turistica": {
                "nombre": "Economía turística",
                "keywords": ["turista", "viajero", "derrama económica", "divisas turísticas", "empleo turístico"],
            },
        },
    },
    "igualdad_genero": {
        "nombre": "Igualdad de género",
        "comisiones": [
            # Diputados
            "Igualdad de Género",
            # Senado LXVI
            "Para la Igualdad de Género",
        ],
        "trends_keywords": [
            "igualdad de género México", "feminicidio México",
            "violencia de género", "INMUJERES", "paridad de género",
        ],
        "subcategorias": {
            "violencia_genero": {
                "nombre": "Violencia de género",
                "keywords": [
                    "violencia de género", "feminicidio", "alerta de género",
                    "violencia doméstica", "violencia contra la mujer",
                    "refugios para mujeres", "orden de protección",
                ],
            },
            "igualdad_sustantiva": {
                "nombre": "Igualdad sustantiva",
                "keywords": [
                    "igualdad de género", "igualdad sustantiva", "brecha salarial",
                    "igualdad laboral", "discriminación de género", "equidad salarial",
                ],
            },
            "participacion_politica_mujeres": {
                "nombre": "Participación política de mujeres",
                "keywords": [
                    "paridad de género", "paridad en todo",
                    "violencia política de género",
                    "participación política de mujeres", "cuotas de género",
                ],
            },
            "derechos_reproductivos": {
                "nombre": "Derechos reproductivos",
                "keywords": [
                    "aborto", "derechos reproductivos",
                    "interrupción legal del embarazo",
                    "salud reproductiva", "maternidad",
                ],
            },
            "instituciones_genero": {
                "nombre": "Instituciones de género",
                "keywords": [
                    "INMUJERES", "instituto de las mujeres",
                    "política de género", "perspectiva de género",
                    "igualdad institucional",
                ],
            },
        },
    },
}


# ─────────────────────────────────────────────
# HELPER: Obtener keywords planos por categoría
# ─────────────────────────────────────────────
def obtener_keywords_categoria(cat_clave):
    """Retorna la unión de keywords de todas las subcategorías de una categoría.
    Backward-compatible: si la categoría aún tiene 'keywords' (legacy), los retorna directamente."""
    cat = CATEGORIAS[cat_clave]
    if "keywords" in cat:  # fallback legacy
        return cat["keywords"]
    todos = set()
    for sub in cat.get("subcategorias", {}).values():
        todos.update(sub["keywords"])
    return list(todos)

# ─────────────────────────────────────────────
# SCORING - Fórmula del Semáforo
# ─────────────────────────────────────────────
SCORING = {
    "pesos": {
        "media": 0.20,       # Cobertura mediática (volumen + concentración + diversidad)
        "trends": 0.15,      # Google Trends (atención pública, no intención política)
        "congreso": 0.25,    # Actividad en Gaceta Parlamentaria (señal institucional)
        "mananera": 0.10,    # Mención de la Presidenta en conferencia matutina
        "urgencia": 0.15,    # Factor de urgencia condicional (amplifica si convergen señales)
        "dominancia": 0.15,  # Dominancia discursiva: relación media vs congreso
    },
    "umbrales": {
        "verde": 70,         # ≥70: alta probabilidad de actividad legislativa
        "amarillo": 40,      # 40-69: actividad posible, monitorear
        "rojo": 0,           # <40: baja probabilidad
    },
}

# SCORE = (0.25×Media) + (0.15×Trends) + (0.30×Congreso) + (0.15×Mañanera) + (0.15×Urgencia)

# ─────────────────────────────────────────────
# CONGRESO - Gaceta Parlamentaria
# ─────────────────────────────────────────────
GACETA = {
    "base_url": "https://gaceta.diputados.gob.mx",
    "rss_url": "https://gaceta.diputados.gob.mx/SIL/",
    "senado_url": "https://www.senado.gob.mx/65/gaceta_del_senado",
    "tipos_documento": [
        "iniciativa",
        "punto_de_acuerdo",
        "dictamen",
        "minuta",
        "proposicion",
        "comunicacion",
    ],
    "intervalo_scraping_min": 30,  # minutos entre cada scraping
}

# ─────────────────────────────────────────────
# GOOGLE TRENDS
# ─────────────────────────────────────────────
GOOGLE_TRENDS = {
    "geo": "MX",
    "timeframe": "now 7-d",   # Últimos 7 días
    "language": "es",
    "max_keywords_per_request": 5,  # Límite de API
}

# ─────────────────────────────────────────────
# ANÁLISIS TEMPORAL (LAG)
# ─────────────────────────────────────────────
LAG_CONFIG = {
    "ventana_dias": 90,                 # Ventana de análisis (90 días para patrones robustos)
    "max_lag_dias": 30,                 # Lag máximo a evaluar
    "granger_max_lag": 14,              # Lags para test de Granger
    "p_value_threshold": 0.05,          # Significancia estadística
    "min_observaciones": 20,            # Mínimo de datos para análisis
    "cross_correlation_lags": 30,       # Lags para cross-correlation
}

# ─────────────────────────────────────────────
# NLP - Clasificador
# ─────────────────────────────────────────────
NLP_CONFIG = {
    "modelo": "keyword_matching",  # Fase 1: keywords. Fase 2: transformers
    "idioma": "es",
    "min_confianza": 0.5,          # Umbral mínimo para asignar categoría
    "max_categorias": 3,           # Máximo de categorías por artículo
    "stopwords_extra": [
        "México", "mexicano", "país", "república",
    ],
}

# ─────────────────────────────────────────────
# MAPEO COMISIÓN → CATEGORÍA
# Fragmentos del nombre de comisión (lowercase) → categoría FIAT.
# Se busca con "in" sobre el nombre normalizado de la comisión.
# Orden importa: el primer match gana.
# ─────────────────────────────────────────────
COMISION_A_CATEGORIA = [
    # Seguridad y Justicia
    ("seguridad pública", "seguridad_justicia"),
    ("justicia", "seguridad_justicia"),
    ("defensa nacional", "seguridad_justicia"),
    ("marina", "seguridad_justicia"),
    # Economía y Hacienda
    ("hacienda", "economia_hacienda"),
    ("economía", "economia_hacienda"),
    ("economia", "economia_hacienda"),
    ("defensa de los consumidores", "economia_hacienda"),
    ("comercio", "economia_hacienda"),
    # Energía
    ("energía", "energia"),
    ("energia", "energia"),
    ("hidrocarburos", "energia"),
    # Salud
    ("salud", "salud"),
    # Educación
    ("educación", "educacion"),
    ("educacion", "educacion"),
    ("ciencia", "educacion"),
    # Trabajo
    ("trabajo", "trabajo"),
    ("seguridad social", "trabajo"),
    # Electoral y Político
    ("gobernación", "electoral_politico"),
    ("gobernacion", "electoral_politico"),
    ("reforma del estado", "electoral_politico"),
    ("puntos constitucionales", "electoral_politico"),
    ("reglamentos y prácticas parlamentarias", "electoral_politico"),
    # Derechos Humanos
    ("derechos humanos", "derechos_humanos"),
    ("igualdad de género", "derechos_humanos"),
    ("igualdad de genero", "derechos_humanos"),
    ("niñez", "derechos_humanos"),
    ("pueblos indígenas", "derechos_humanos"),
    ("asuntos migratorios", "derechos_humanos"),
    ("diversidad sexual", "derechos_humanos"),
    # Infraestructura
    ("comunicaciones y transportes", "infraestructura"),
    ("infraestructura", "infraestructura"),
    ("recursos hídricos", "infraestructura"),
    ("recursos hidricos", "infraestructura"),
    ("desarrollo urbano", "infraestructura"),
    ("vivienda", "infraestructura"),
    # Agro y Desarrollo Rural
    ("agricultura", "agro_rural"),
    ("desarrollo rural", "agro_rural"),
    ("pesca", "agro_rural"),
    # Relaciones Exteriores
    ("relaciones exteriores", "relaciones_exteriores"),
    ("frontera norte", "relaciones_exteriores"),
    ("frontera sur", "relaciones_exteriores"),
    ("américa del norte", "relaciones_exteriores"),
    # Anticorrupción
    ("anticorrupción", "anticorrupcion"),
    ("anticorrupcion", "anticorrupcion"),
    ("transparencia", "anticorrupcion"),
    ("fiscalización", "anticorrupcion"),
    ("fiscalizacion", "anticorrupcion"),
    # Medio Ambiente
    ("medio ambiente", "medio_ambiente"),
    ("cambio climático", "medio_ambiente"),
    ("cambio climatico", "medio_ambiente"),
    ("recursos naturales", "medio_ambiente"),
    # Inteligencia Artificial
    ("inteligencia artificial", "inteligencia_artificial"),
    ("ciberseguridad", "inteligencia_artificial"),
    # Medios de Comunicación
    ("radio, televisión", "medios_comunicacion"),
    ("radio y televisión", "medios_comunicacion"),
    ("comunicación social", "medios_comunicacion"),
    ("cinematografía", "medios_comunicacion"),
    ("cinematografia", "medios_comunicacion"),
    ("cultura y cinemat", "medios_comunicacion"),
    # Turismo
    ("turismo", "turismo"),
    # Política Social
    ("bienestar", "politica_social"),
    ("desarrollo social", "politica_social"),
    # Igualdad de Género
    ("igualdad de género", "igualdad_genero"),
    ("igualdad de genero", "igualdad_genero"),
    ("contra la trata", "igualdad_genero"),
    # Deporte (descartable — no es categoría FIAT)
    ("deporte", None),
    ("cultura", "educacion"),
]


def comision_a_categoria(nombre_comision):
    """Dado un nombre de comisión, retorna la categoría FIAT o None."""
    if not nombre_comision or nombre_comision == "No especificada":
        return None
    nombre_lower = nombre_comision.lower()
    for fragmento, categoria in COMISION_A_CATEGORIA:
        if fragmento in nombre_lower:
            return categoria
    return None

# ─────────────────────────────────────────────
# FILTRO DE RELEVANCIA MÉXICO
# ─────────────────────────────────────────────

# Términos que indican que el artículo NO trata sobre legislación mexicana
KEYWORDS_NEGATIVOS = [
    # Deportes internacionales
    "NFL", "NBA", "MLB", "Super Bowl", "touchdown", "quarterback",
    "Premier League", "Champions League", "La Liga española",
    "Serie A", "Bundesliga", "Grand Slam", "Wimbledon",
    # Deportes / selecciones nacionales (NO son tema legislativo)
    "selección mexicana", "selección nacional de futbol",
    "Javier Aguirre", "director técnico", "convocatoria mundialista",
    "Mundial 2026", "eliminatoria mundialista",
    "Liga MX", "Club América", "Chivas", "Cruz Azul futbol",
    "Pumas UNAM futbol", "Tigres UANL", "Rayados",
    "Juegos Olímpicos", "medallista", "atletismo", "clavadista",
    "sprint femenino", "sprint masculino", "velocista",
    "Copa del Mundo FIFA", "World Series", "Stanley Cup",
    "UFC", "boxeo profesional", "Fórmula 1",
    "gol de", "anotó gol", "marcador final", "medio tiempo",
    "entrenador del", "fichaje", "transferencia de jugador",
    "torneo de tenis", "Grand Prix", "maratón deportivo",
    # Países como sujeto (no como tema de política exterior mexicana)
    "Congreso de Perú", "parlamento europeo", "parlamento británico",
    "Congreso de Colombia", "Congreso de Argentina", "Congreso de Chile",
    "Congreso de Brasil", "Asamblea Nacional de Venezuela",
    "Westminster", "Bundestag", "Dieta de Japón",
    # Figuras internacionales no-México
    "Steffon Diggs", "Stefon Diggs", "Tom Brady", "LeBron James",
    "Taylor Swift", "Elon Musk", "Jeff Bezos",
    "Milei", "Boric", "Petro", "Lula",
    # Entretenimiento / farándula / virales
    "Hollywood", "Bollywood", "Oscar de Hollywood",
    "Grammy", "Emmy", "Golden Globe",
    "reality show", "telenovela", "Netflix", "influencer",
    "se vuelve viral", "video viral", "meme",
    "horóscopo", "signo zodiacal",
    "farándula", "espectáculos",
    "perro lobo", "mascota viral",
]

# Términos que confirman que el artículo es relevante para México
KEYWORDS_MEXICO = [
    # Instituciones del Estado mexicano
    "México", "mexicano", "mexicana", "Congreso de la Unión",
    "Cámara de Diputados", "Senado de la República",
    "gobierno federal", "gobierno de México", "gobierno mexicano",
    # Poder Ejecutivo
    "Sheinbaum", "AMLO", "López Obrador", "presidencia de México",
    "secretaría de estado",
    # Instituciones clave
    "SAT", "INE", "INAI", "CNDH", "Pemex", "CFE", "IMSS", "ISSSTE",
    "Guardia Nacional", "Fiscalía General de la República",
    "Banxico", "Banco de México", "Conacyt", "Conahcyt",
    "Gaceta Parlamentaria", "Diario Oficial de la Federación",
    # Estados y ciudades
    "CDMX", "Ciudad de México", "Jalisco", "Nuevo León", "Veracruz",
    "Chiapas", "Oaxaca", "Guerrero", "Puebla", "Sinaloa", "Sonora",
    "Chihuahua", "Tamaulipas", "Michoacán", "Guanajuato",
    "Estado de México", "Tabasco", "Quintana Roo", "Yucatán",
    "Baja California", "Coahuila", "Durango", "Hidalgo",
    "Aguascalientes", "Zacatecas", "San Luis Potosí",
    # Partidos políticos mexicanos
    "Morena", "PAN", "PRI", "PRD", "Movimiento Ciudadano",
    "Partido Verde", "PT", "Grupo Parlamentario",
    # Términos legislativos mexicanos
    "diputado", "diputada", "senador", "senadora",
    "iniciativa de ley", "punto de acuerdo", "dictamen",
    "periodo ordinario", "reforma constitucional",
    "Constitución Política", "Ley General", "Código Penal Federal",
    "DOF", "Cámara de Senadores",
]

# ─────────────────────────────────────────────
# URGENCIA - Factores multiplicadores
# ─────────────────────────────────────────────
URGENCIA = {
    "periodo_ordinario": 1.5,       # Sep-Dic, Feb-Abr: más actividad
    "periodo_extraordinario": 2.0,  # Sesiones extraordinarias
    "receso": 0.5,                  # Periodos de receso
    "fin_legislatura": 1.8,         # Últimos meses de legislatura
    "evento_crisis": 2.0,           # Crisis nacional (manual)
    "periodos_ordinarios": [
        {"inicio": "09-01", "fin": "12-15"},  # Primer período
        {"inicio": "02-01", "fin": "04-30"},  # Segundo período
    ],
    "amplificacion": {
        "umbral_media": 50,             # Media debe superar este score
        "umbral_congreso": 60,          # Congreso debe superar este score
        "factor_max_convergente": 1.4,  # Ambas señales activas → hasta 1.4×
        "factor_max_parcial": 1.15,     # Una señal activa → hasta 1.15×
    },
}

# ─────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────
DASHBOARD = {
    "puerto": 8050,
    "actualizacion_seg": 300,  # Refrescar cada 5 min
    "max_alertas": 50,
    "colores_semaforo": {
        "verde": "#22c55e",
        "amarillo": "#eab308",
        "rojo": "#ef4444",
    },
}

# ─────────────────────────────────────────────
# ALMACENAMIENTO
# ─────────────────────────────────────────────
DATABASE = {
    "tipo": "sqlite",
    "archivo": "semaforo.db",
    "tablas": [
        "articulos",        # Noticias scrapeadas
        "trends",           # Datos de Google Trends
        "gaceta",           # Documentos del Congreso
        "scores",           # Scores calculados por categoría
        "alertas",          # Historial de alertas
        "correlaciones",    # Resultados de análisis temporal
        "mananera",              # Menciones de CSP en conferencias matutinas
        "sintesis_legislativa",  # Síntesis diaria de Cámara de Diputados
        "tweets",                # Tweets de periodistas y coordinadores parlamentarios
        "resoluciones",          # Tracking de precisión predictiva semanal
    ],
}

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
LOGGING = {
    "nivel": "INFO",
    "archivo": "semaforo.log",
    "formato": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    "max_bytes": 10_000_000,  # 10 MB
    "backup_count": 5,
}
