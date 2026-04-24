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
    # Talla Política — seguimiento legislativo granular por cámara + secretarías.
    # Tres feeds RSS separados para detectar señales que anticipen iniciativas.
    "talla_diputados": {
        "nombre": "Talla Política (Diputados)",
        "rss": "https://www.tallapolitica.com.mx/category/camara_de_diputados/feed/",
        "peso": 1.3,  # Peso alto: cobertura legislativa específica de Diputados
    },
    "talla_senadores": {
        "nombre": "Talla Política (Senadores)",
        "rss": "https://www.tallapolitica.com.mx/category/camara_de_senadores/feed/",
        "peso": 1.3,  # Peso alto: cobertura legislativa específica del Senado
    },
    "talla_secretarias": {
        "nombre": "Talla Política (Secretarías)",
        "rss": "https://www.tallapolitica.com.mx/category/secretarias_de_estado/feed/",
        "peso": 1.1,  # Señales tempranas de temas que pueden volverse legislativos
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
                             "autodefensa", "grupo armado", "célula criminal", "abuso policial", "ejecución extrajudicial",
                             # Nuevos (abr 2026 — gap detectado en benchmark ground truth):
                             # CURP biométrica fue un tema legislativo clave 2025-2026 que sub-detectaba.
                             "CURP biométrica", "CURP biometrica", "identidad biométrica",
                             "Sistema Nacional de Inteligencia", "Sistema Nacional de Seguridad"],
            },
            "justicia_penal": {
                "nombre": "Justicia penal y reforma judicial",
                "keywords": ["código penal", "ministerio público", "proceso penal", "jueces", "fiscalía", "reforma judicial", "poder judicial", "penal",
                             "código civil", "Ley de Amparo", "solución de controversias",
                             "Norma Piña", "ministro", "ministra", "magistrado", "amparo", "suspensión judicial",
                             "tribunal", "juzgado", "sentencia", "Arturo Zaldívar", "reforma al poder judicial",
                             # Leyes federales
                             "Código Penal Federal", "Código Nacional de Procedimientos Penales",
                             "Código de Justicia Militar", "Código Militar de Procedimientos Penales",
                             "Código Civil Federal", "Código Federal de Procedimientos Civiles",
                             "Código Nacional de Procedimientos Civiles y Familiares",
                             "Ley de Carrera Judicial", "Ley Orgánica del Poder Judicial",
                             "Ley Nacional de Ejecución Penal",
                             "Ley Nacional de Mecanismos Alternativos en Materia Penal",
                             "Ley Nacional del Sistema Integral de Justicia para Adolescentes"],
            },
            "crimen_organizado": {
                "nombre": "Crimen organizado",
                "keywords": ["narcotráfico", "crimen organizado", "extorsión", "secuestro", "cártel", "fentanilo", "delito",
                             "huachicol", "robo de combustible", "célula criminal", "plaza", "sicario", "halcón",
                             # Leyes federales
                             "Ley Federal contra la Delincuencia Organizada", "Ley Nacional de Extinción de Dominio",
                             "Ley Federal de Armas de Fuego y Explosivos", "Ley de Extradición Internacional",
                             "Ley para Prevenir y Sancionar Delitos de Secuestro", "Ley de Delitos de Extorsión",
                             "Ley para Prevenir e Identificar Operaciones con Recursos Ilícitos"],
            },
            "fuerzas_armadas": {
                "nombre": "Fuerzas armadas y defensa",
                "keywords": ["sedena", "semar", "fuerzas armadas", "defensa nacional", "seguridad nacional", "ejército", "marina",
                             # Leyes federales
                             "Ley de la Guardia Nacional", "Ley de Seguridad Nacional",
                             "Ley Orgánica del Ejército y Fuerza Aérea", "Ley Orgánica de la Armada",
                             "Ley de Ascensos de la Armada", "Ley de Ascensos del Ejército",
                             "Ley de Disciplina del Ejército", "Ley del Servicio Militar",
                             "Ley del Instituto de Seguridad Social para las Fuerzas Armadas",
                             "Ley de Protección del Espacio Aéreo",
                             "Ley del Sistema Nacional de Investigación e Inteligencia",
                             "Ley Nacional sobre el Uso de la Fuerza",
                             "Ley de la Policía Federal", "Ley de Seguridad Privada"],
            },
            "sistema_penitenciario": {
                "nombre": "Sistema penitenciario",
                "keywords": ["cárcel", "prisión", "sistema penitenciario", "reclusorio", "reinserción social"],
            },
            "violencia_victimas": {
                "nombre": "Violencia y víctimas",
                # Señales de hechos criminales agregadas abr 2026 (benchmark):
                # casos tipo "mujer herida de bala", "cae presunto", "mató a menor"
                # no clasificaban o iban a DDHH/igualdad. Agregan patrones de
                # hechos delictivos con víctimas para que Seguridad gane cuando
                # haya evidencia textual clara.
                "keywords": ["homicidio", "desaparición forzada", "violencia", "impunidad", "víctimas",
                             "fosa clandestina", "fosas", "asesinato", "muertos", "restos humanos",
                             "desaparecidos", "persona desaparecida", "búsqueda de personas", "tortura", "detención arbitraria",
                             "cadáver", "cadáveres", "cuerpo sin vida", "cuerpos sin vida",
                             "abandonan cuerpo", "abandonan tres", "localizan cuerpo", "hallan cuerpo",
                             "agresor sexual", "agresores sexuales", "abuso sexual", "abusador", "abusadores",
                             "delito sexual", "delitos sexuales", "violación sexual", "pederastia", "pedofilia",
                             "registro de agresores", "trata de personas", "acoso sexual",
                             # Señales de hechos criminales (benchmark abr 2026)
                             "herido de bala", "herida de bala", "heridos de bala", "heridas de bala",
                             "a balazos", "por impacto de bala", "disparos",
                             "ráfaga de disparos", "tiroteo", "balacera con saldo",
                             "mató a", "mata a", "mataron a", "dispararon a",
                             "asesinó a", "asesinaron a", "apuñaló", "apuñalado",
                             "cae presunto", "detienen a presunto", "presunto responsable",
                             "vinculado a proceso", "captura a presunto",
                             # Leyes federales
                             "Ley General de Víctimas", "Ley para Prevenir y Sancionar la Tortura",
                             "Ley de Prevención Social de la Violencia",
                             "Ley de Declaración Especial de Ausencia para Personas Desaparecidas",
                             "Ley de Desaparición Forzada",
                             "Ley de Protección a Personas que Intervienen en el Procedimiento Penal"],
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
                             "SHCP", "Secretaría de Hacienda", "miscelánea fiscal", "Paquete Económico", "Ley de Ingresos",
                             # Leyes federales
                             "Código Fiscal de la Federación", "Ley de Ingresos de la Federación",
                             "Ley del Impuesto al Valor Agregado", "Ley del ISR", "Ley del IEPS",
                             "Ley Federal de Derechos", "Ley de Coordinación Fiscal",
                             "Ley Federal de Presupuesto y Responsabilidad Hacendaria",
                             "Presupuesto de Egresos de la Federación", "Ley del SAT",
                             "Ley Federal de Deuda Pública", "Ley de Disciplina Financiera",
                             "Ley Aduanera", "Ley de los Impuestos Generales de Importación y Exportación",
                             "Ley del Impuesto sobre Automóviles Nuevos", "Ley de Contribución de Mejoras",
                             "Ley de los Derechos del Contribuyente", "Ley de Tesorería"],
            },
            "presupuesto_gasto": {
                "nombre": "Presupuesto y gasto público",
                # "austeridad" quitada abr 2026: ambigua (austeridad política,
                # personal, republicana — no siempre económica). Si se habla de
                # austeridad presupuestal, ya matcheará "presupuesto".
                "keywords": ["presupuesto", "gasto público", "PEF", "deuda pública", "déficit", "deuda",
                             "Rogelio Ramírez de la O", "secretario de hacienda", "subasta", "Cetes", "bonos gubernamentales"],
            },
            "politica_monetaria": {
                "nombre": "Política monetaria y macroeconomía",
                "keywords": ["inflación", "tipo de cambio", "peso mexicano", "Banxico", "tasa de interés", "PIB", "Banco de México",
                             "recesión", "crecimiento económico", "estancamiento", "precio del dólar", "depreciación",
                             "volatilidad cambiaria", "calificadora", "Moody's", "Standard & Poor's", "Fitch",
                             "riesgo país", "mercados financieros", "bolsa mexicana",
                             # Leyes federales
                             "Ley de Instituciones de Crédito", "Ley del Mercado de Valores",
                             "Ley de la CNBV", "Ley del Banco de México",
                             "Ley de Ahorro y Crédito Popular", "Ley de Uniones de Crédito",
                             "Ley de Fondos de Inversión", "Ley de Instituciones de Seguros y de Fianzas",
                             "Ley de Protección al Ahorro Bancario", "Ley de Sistemas de Pagos",
                             "Ley de la Casa de Moneda", "Ley Monetaria", "Ley de CONDUSEF",
                             "Ley de Sociedades de Información Crediticia",
                             "Ley para Regular Agrupaciones Financieras", "Ley de Tecnología Financiera",
                             "Ley de los Sistemas de Ahorro para el Retiro",
                             "Ley de Transparencia de Servicios Financieros",
                             "Ley de Transparencia en el Crédito Garantizado",
                             "Ley del Seguro", "Ley de Organizaciones Auxiliares del Crédito",
                             "Ley de Cooperativas de Ahorro y Préstamo",
                             "Ley Orgánica de Nacional Financiera", "Ley Orgánica de Sociedad Hipotecaria Federal",
                             "Ley Orgánica de Bancomext", "Ley Orgánica de Banobras",
                             "Ley Orgánica de Banjercito", "Ley Orgánica del Banco del Bienestar"],
            },
            "comercio_exterior": {
                "nombre": "Comercio exterior e inversión",
                "keywords": ["aranceles", "comercio exterior", "inversión extranjera", "exportación", "importación", "balanza comercial",
                             "nearshoring", "relocalización",
                             # Leyes federales
                             "Código de Comercio", "Ley de Comercio Exterior",
                             "Ley de Inversión Extranjera",
                             "Ley de los Impuestos Generales de Importación y Exportación"],
            },
            "fomento_economico": {
                "nombre": "Fomento económico",
                "keywords": ["economía", "inversión pública", "fomento económico", "emprendimiento", "Pymes", "competitividad", "hacienda", "política fiscal",
                             "Protección al Consumidor",
                             "clase media", "poder adquisitivo", "canasta básica",
                             # Leyes federales
                             "Ley de Concursos Mercantiles", "Ley Federal de Protección al Consumidor",
                             "Ley General de Sociedades Mercantiles",
                             "Ley General de Títulos y Operaciones de Crédito",
                             "Ley de Cámaras Empresariales", "Ley de Fomento a la Microindustria",
                             "Ley de Infraestructura de la Calidad",
                             "Ley de Productividad y Competitividad",
                             "Ley de Economía Social y Solidaria", "Ley de Propiedad Industrial",
                             "Ley de Sociedades Cooperativas", "Ley de Adquisiciones del Sector Público",
                             "Ley de Zonas Económicas Especiales", "Ley de Fomento a la Confianza Ciudadana",
                             "Ley de Sociedades de Responsabilidad Limitada",
                             "Ley de Sociedades de Solidaridad Social", "Ley de Expropiación"],
            },
            "competencia_mercados": {
                "nombre": "Competencia y mercados",
                "keywords": ["competencia económica", "Cofece", "monopolio", "prácticas monopólicas",
                             "regulación de mercado", "control de precios", "libre competencia",
                             # Leyes federales
                             "Ley Federal de Competencia Económica"],
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
                             "precio del petróleo", "mezcla mexicana", "huachicoleo", "toma clandestina", "fracking", "perforación",
                             # Leyes federales
                             "Ley del Sector Hidrocarburos", "Ley de la Empresa Pública Pemex",
                             "Ley de Ingresos sobre Hidrocarburos", "Ley del Fondo Mexicano del Petróleo",
                             "Ley de la ASEA",
                             "Ley para Prevenir y Sancionar Delitos en Materia de Hidrocarburos"],
            },
            "cfe_electricidad": {
                "nombre": "CFE y electricidad",
                "keywords": ["CFE", "electricidad", "tarifas eléctricas", "apagón", "generación eléctrica", "subsidio energético",
                             "apagón masivo", "corte de luz", "falla eléctrica", "contrato de electricidad", "tarifa doméstica",
                             # Leyes federales
                             "Ley del Sector Eléctrico", "Ley de la Empresa Pública CFE",
                             "Ley de la Comisión Nacional de Energía"],
            },
            "energias_renovables": {
                "nombre": "Energías renovables",
                "keywords": ["renovable", "solar", "eólica", "transición energética", "energía limpia",
                             # Leyes federales
                             "Ley de Planeación y Transición Energética", "Ley de Geotermia",
                             "Ley de Biocombustibles", "Ley de Responsabilidad Civil por Daños Nucleares"],
            },
            "mineria_recursos": {
                "nombre": "Minería y recursos",
                "keywords": ["litio", "minería", "concesión minera", "gas natural", "gas LP", "gasoducto", "soberanía energética",
                             "reforma energética", "Rocío Nahle", "secretaria de energía",
                             # Leyes federales
                             "Ley de Minería"],
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
                             "primer nivel de atención", "Alcocer", "secretario de salud", "seguro popular",
                             # Leyes federales
                             "Ley General de Salud", "Ley de los Institutos Nacionales de Salud"],
            },
            "medicamentos_abasto": {
                "nombre": "Medicamentos y abasto",
                "keywords": ["medicamento", "desabasto", "farmacia", "vacuna", "compra consolidada",
                             "receta médica", "sustancia química", "sustancias químicas", "registro de sustancias",
                             "sustancia tóxica", "sustancias tóxicas", "producto químico", "productos químicos",
                             "químicos peligrosos", "cofepris", "regulación sanitaria", "control sanitario",
                             # Leyes federales
                             "Ley de Bioseguridad de Organismos Genéticamente Modificados",
                             "Ley Federal para el Control de Precursores Químicos",
                             "Ley Federal para el Control de Sustancias Químicas"],
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
                             "cáncer", "diabetes", "obesidad", "hipertensión",
                             # Leyes federales
                             "Ley General para el Control del Tabaco",
                             "Ley General para la Detección Oportuna del Cáncer",
                             "Ley General de la Alimentación Adecuada y Sostenible",
                             "Ley de Ayuda Alimentaria para los Trabajadores"],
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
                             "Leticia Ramírez", "secretaria de educación", "inscripción", "ciclo escolar", "calendario escolar",
                             # Leyes federales
                             "Ley General de Educación Superior", "Ley de Mejora Continua de la Educación",
                             "Ley del Sistema para la Carrera de las Maestras y los Maestros"],
            },
            "educacion_superior": {
                "nombre": "Educación superior e investigación",
                # "investigación" atómica quitada abr 2026: ambigua (investigación
                # criminal, periodística, administrativa). Reemplazada por frases
                # específicas del dominio académico.
                "keywords": ["universidad", "UNAM", "IPN", "Conahcyt", "CONACYT", "becas", "posgrado",
                             "beca universal",
                             "investigación científica", "investigación académica",
                             "centros de investigación", "investigadores del SNI", "SNII",
                             # Leyes federales
                             "Ley Orgánica de la UNAM", "Ley Orgánica de la UAM",
                             "Ley Orgánica del IPN", "Ley de la Universidad Autónoma Chapingo",
                             "Ley del Seminario de Cultura Mexicana"],
            },
            "ciencia_tecnologia": {
                "nombre": "Ciencia y tecnología",
                # Atómicas ambiguas quitadas abr 2026: "ciencia", "tecnología",
                # "innovación" y "patente". Todas aparecen en contextos económicos,
                # empresariales y políticos. Se mantiene "desarrollo tecnológico"
                # y se agregan frases específicas del dominio científico.
                "keywords": ["desarrollo tecnológico", "política científica",
                             "divulgación científica", "fomento a la ciencia",
                             "innovación educativa", "innovación tecnológica",
                             "sistema nacional de ciencia",
                             # Leyes federales
                             "Ley General de Humanidades Ciencias Tecnologías e Innovación"],
            },
            "cultura_deporte": {
                "nombre": "Cultura y deporte",
                # Atómicas ambiguas quitadas abr 2026: "cultura", "juventud",
                # "deporte", "biblioteca". Todas demasiado amplias en notas
                # generales (cultura popular, deporte profesional, juventud
                # política). Reemplazadas por frases del dominio político-cultural.
                "keywords": ["patrimonio cultural", "política cultural",
                             "fomento cultural", "industrias culturales",
                             "biblioteca pública", "red de bibliotecas",
                             "política de juventud", "apoyos a la juventud",
                             "deporte estudiantil", "fomento al deporte",
                             "política deportiva",
                             # Leyes federales
                             "Ley General de Cultura y Derechos Culturales", "Ley General de Bibliotecas",
                             "Ley de Fomento para la Lectura y el Libro", "Ley de Derechos Lingüísticos",
                             "Ley de Protección del Patrimonio Cultural de Pueblos Indígenas",
                             "Ley sobre Monumentos y Zonas Arqueológicos",
                             "Ley del INAH", "Ley del INBAL"],
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
                             "despido", "liquidación", "indemnización", "empleo juvenil", "primer empleo", "reforma laboral",
                             # Leyes federales
                             "Ley Federal de los Trabajadores al Servicio del Estado",
                             "Ley del Centro Federal de Conciliación y Registro Laboral"],
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
                "keywords": ["pensión", "AFORE", "seguridad social", "jubilación", "retiro laboral", "edad de retiro", "fondo de retiro", "cuenta de retiro", "ahorro para el retiro", "Seguro Social", "ISSSTE",
                             # Leyes federales
                             "Ley del Seguro Social", "Ley del ISSSTE",
                             "Ley de los Sistemas de Ahorro para el Retiro", "Ley del FONACOT"],
            },
            "relaciones_laborales": {
                "nombre": "Relaciones laborales",
                "keywords": ["sindicato", "huelga", "STPS", "contrato colectivo", "conflicto laboral",
                             "Marath Bolaños", "secretario del trabajo", "inspección laboral", "Profedet",
                             # Leyes federales
                             "Ley del Infonavit"],
            },
        },
    },
    "electoral_politico": {
        "nombre": "Electoral y Político",
        "comisiones": [
            # Diputados
            "Reforma Política-Electoral",
            # Senado LXVI
            "Participación Ciudadana",
            "Estudios Legislativos",
            "Estudios Legislativos, Primera",
            "Estudios Legislativos, Segunda",
            "Reglamentos y Prácticas Parlamentarias",
            "Medalla Belisario Domínguez",
            # Nota v3: "Gobernación", "Gobernación y Población", "Puntos Constitucionales",
            # "Federalismo" y "Desarrollo Municipal" se movieron a la categoría
            # `administracion` tras el eval set v1 (caso #21).
        ],
        "trends_keywords": ["elecciones México", "INE", "Morena partido", "congreso México", "reforma electoral"],
        "subcategorias": {
            "reforma_electoral": {
                "nombre": "Reforma electoral",
                # Endurecido abr 2026: "voto" solo capturaba "voto a favor",
                # "voto de los diputados", etc. Se sustituye por variantes
                # explícitas del ejercicio del sufragio.
                "keywords": ["reforma electoral", "INE", "TEPJF", "campaña electoral", "jornada electoral",
                             "voto popular", "voto electoral", "voto en urna", "voto razonado",
                             "proceso electoral", "padrón electoral", "credencial de elector",
                             "lista nominal", "PREP", "conteo rápido", "resultados electorales", "urna", "boleta electoral",
                             "casilla electoral", "distrito electoral", "circunscripción electoral", "fiscalización de campañas",
                             "Guadalupe Taddei", "consejero electoral", "consejera electoral", "tribunal electoral",
                             "impugnación electoral", "tómbola", "insaculación",
                             # Leyes federales
                             "Ley General de Instituciones y Procedimientos Electorales",
                             "Ley General de Partidos Políticos", "Ley General de Delitos Electorales",
                             "Ley del Sistema de Medios de Impugnación Electoral",
                             "Ley de Consulta Popular", "Ley de Revocación de Mandato"],
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
                # Endurecido abr 2026: se quitaron "sesión plenaria", "tribuna",
                # "coordinador parlamentario", "líder parlamentario", "ejecutivo
                # federal" y "periodo de sesiones" — aparecen en CUALQUIER nota
                # legislativa rutinaria y metían falsos positivos por centenares.
                "keywords": ["gobernabilidad", "sistema político", "crisis política", "desafuero",
                             "juicio político", "Ley Orgánica del Congreso",
                             "crisis de gobierno", "ingobernabilidad", "vacío de poder",
                             "ruptura del orden", "polarización política",
                             # Leyes federales — sólo las propiamente políticas/parlamentarias.
                             # Las leyes administrativas (LOAPF, Planeación, Entidades Paraestatales,
                             # Servicio Profesional, Procedimiento Administrativo, etc.) se movieron
                             # a la categoría `administracion` en v3.
                             "Constitución Política",
                             "Reglamento de la Cámara de Diputados", "Reglamento del Senado",
                             "Ley General de Comunicación Social",
                             "Ley General de Población",
                             "Ley Reglamentaria del Artículo 5o Constitucional",
                             "Ley de los Husos Horarios", "Ley sobre el Escudo la Bandera y el Himno"],
            },
            "participacion_ciudadana": {
                "nombre": "Participación ciudadana",
                # Endurecido abr 2026: "candidato/a", "debate", "gobernador/a",
                # "alcalde/sa" y "encuesta" sueltos capturaban cualquier nota
                # de un gobernador o alcalde en funciones, cualquier candidato
                # a concurso o premio, cualquier debate público. Se sustituyen
                # por variantes explícitamente electorales.
                "keywords": ["consulta popular", "revocación de mandato", "democracia representativa", "referéndum", "plebiscito",
                             "encuesta electoral", "preferencia electoral", "intención de voto", "precampaña", "proselitismo",
                             "candidatura electoral",
                             "candidato a presidente", "candidato a la presidencia", "candidata a presidenta",
                             "candidato a gobernador", "candidata a gobernadora",
                             "candidato a senador", "candidata a senadora",
                             "candidato a diputado", "candidata a diputada",
                             "candidato a alcalde", "candidata a alcaldesa",
                             "debate presidencial", "debate electoral", "debate de candidatos",
                             "elección de gobernador", "elección de senadores", "elección de diputados",
                             "gubernatura", "elección intermedia", "elección presidencial", "elección federal"],
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
                             "defensor de derechos humanos", "Corte Interamericana", "CIDH",
                             # Leyes federales
                             "Ley de la CNDH", "Ley de Amnistía",
                             "Ley Federal para Prevenir y Eliminar la Discriminación"],
            },
            "genero_diversidad": {
                "nombre": "Género y diversidad",
                "keywords": ["género", "violencia de género", "aborto", "diversidad sexual", "LGBT", "feminismo",
                             "identidad de género", "matrimonio igualitario",
                             # Leyes federales
                             "Ley de Asociaciones Religiosas",
                             "Ley General para la Inclusión de Personas con Discapacidad",
                             "Ley de los Derechos de Personas Adultas Mayores"],
            },
            "migracion_refugio": {
                "nombre": "Migración y refugio",
                "keywords": ["migración", "migrante", "refugiado", "asilo", "deportación", "caravana migrante",
                             "persona desplazada", "desplazamiento forzado", "trata de personas", "tráfico de personas",
                             # Leyes federales
                             "Ley de Migración", "Ley sobre Refugiados y Asilo Político",
                             "Ley de Nacionalidad"],
            },
            "pueblos_indigenas": {
                "nombre": "Pueblos indígenas",
                "keywords": ["indígena", "pueblos originarios", "lengua indígena", "autonomía indígena", "afromexicano",
                             # Leyes federales
                             "Ley del Instituto Nacional de los Pueblos Indígenas",
                             "Ley de Derechos Lingüísticos de los Pueblos Indígenas"],
            },
            "derechos_ninez": {
                "nombre": "Derechos de la niñez",
                # "menor de edad" atómica quitada abr 2026: FP en noticias
                # criminales como "cae sujeto que mató a una menor de edad" que
                # son Seguridad, no DDHH. Se reemplaza por compuestas específicas
                # del lenguaje legislativo de niñez.
                "keywords": ["niñez", "adolescencia", "trabajo infantil", "adopción",
                             "matrimonio infantil", "matrimonio forzado",
                             "derechos de menores", "protección de menores",
                             "derechos de niñas y niños", "derechos de la niñez",
                             "derechos de niñez y adolescencia", "interés superior del niño",
                             "interés superior de la niñez",
                             # Leyes federales
                             "Ley General de los Derechos de Niñas Niños y Adolescentes",
                             "Ley General de Prestación de Servicios para Cuidado Infantil",
                             "Ley de la Condición del Espectro Autista"],
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
        "trends_keywords": ["Tren Maya", "AIFA", "agua potable", "vivienda social", "carreteras México"],
        "subcategorias": {
            "transporte_movilidad": {
                "nombre": "Transporte y movilidad",
                # Endurecido v3 (abr 2026): se quitó "movilidad" suelta y "cuota"
                # suelta porque generan FPs (movilidad social/laboral, cuota de
                # género/sindical). Se agregaron compuestas específicas.
                "keywords": ["sistema de transporte", "red de transporte", "Tren Maya", "tren interurbano", "tren suburbano", "carretera", "aeropuerto", "AIFA",
                             "seguridad vial", "movilidad urbana", "movilidad vial", "movilidad sostenible",
                             "proyectos carreteros", "carretero", "red carretera",
                             "transporte público", "Metro", "Metrobús", "Línea 12", "autopista", "peaje",
                             "Capufe", "bache", "socavón",
                             "aviación", "aviación civil", "Ley de Aviación", "autotransporte", "autotransporte federal",
                             "caminos y puentes", "Ley de Caminos", "transporte federal", "transporte terrestre",
                             "transporte aéreo", "transporte marítimo", "cabotaje", "ferrocarril", "vía férrea",
                             # Leyes federales
                             "Ley de Aeropuertos", "Ley de Aviación Civil",
                             "Ley de Caminos Puentes y Autotransporte Federal",
                             "Ley Reglamentaria del Servicio Ferroviario",
                             "Ley de Vías Generales de Comunicación",
                             "Ley General de Movilidad y Seguridad Vial",
                             "Ley de Navegación y Comercio Marítimos", "Ley de Puertos"],
            },
            "obra_publica": {
                "nombre": "Obra pública",
                # Endurecido abr 2026: "puente" solo matcheaba "megapuente" en
                # notas de días feriados y cualquier uso figurado. Se sustituye
                # por variantes explícitas de infraestructura. Lo mismo con
                # "concesión" y "APP" que son siglas ambiguas.
                "keywords": ["obra pública", "obra de infraestructura", "corredor interoceánico", "licitación de obra",
                             "puente vehicular", "puente peatonal", "construcción de puente", "puente federal",
                             "concesión de obra", "concesión carretera", "concesión federal",
                             "asociación público privada", "Jorge Nuño Lara", "secretario de infraestructura",
                             # Leyes federales
                             "Ley de Obras Públicas", "Ley de Asociaciones Público Privadas"],
            },
            "agua_saneamiento": {
                "nombre": "Agua y saneamiento",
                "keywords": ["Conagua", "abastecimiento de agua", "crisis hídrica", "drenaje", "saneamiento", "presa",
                             "acueducto", "tubería", "fuga de agua", "corte de agua", "tandeo", "desabasto de agua",
                             # Leyes federales
                             "Ley de Aguas Nacionales", "Ley General de Aguas"],
            },
            "vivienda_urbano": {
                "nombre": "Vivienda y desarrollo urbano",
                # Endurecido v3 (abr 2026): se quitó "vivienda" suelta porque
                # matchea nombres oficiales genéricos como "censo de población
                # y vivienda" (INEGI) que NO son propuestas de política de
                # vivienda. Caso testigo: propuesta de Damaris Silva (abr 2026)
                # sobre inclusión LGBT+ en censo 2030 caía aquí falsamente.
                # Se mantienen TODAS las compuestas porque son específicas.
                "keywords": ["vivienda social", "Infonavit", "desarrollo urbano", "ordenamiento territorial",
                             "Conavi", "crédito de vivienda", "hipoteca", "Ley de Vivienda",
                             "vivienda digna", "vivienda adecuada", "vivienda popular",
                             "política de vivienda", "déficit habitacional",
                             "Fovissste", "subsidio de vivienda", "rezago habitacional",
                             # Leyes federales
                             "Ley General de Asentamientos Humanos", "Ley del Infonavit"],
            },
            "telecomunicaciones": {
                "nombre": "Telecomunicaciones",
                "keywords": ["telecomunicaciones", "banda ancha", "conectividad", "cobertura digital",
                             # Leyes federales
                             "Ley en Materia de Telecomunicaciones y Radiodifusión"],
            },
            "proteccion_civil": {
                "nombre": "Protección civil y desastres",
                # Endurecido abr 2026: "sismo" suelto capturaba todas las notas
                # de sismos internacionales (caso testigo: sismo Japón 7.7 apr-26).
                # "terremoto" y "huracán" se limitan a contexto mexicano.
                "keywords": ["sismo en México", "sismo México", "sismo CDMX", "sismo Ciudad de México",
                             "terremoto México", "terremoto en México", "temblor en México",
                             "huracán México", "huracán en México",
                             "inundación", "desastre natural", "protección civil",
                             "reconstrucción", "declaratoria de emergencia", "damnificados", "derrumbe", "evacuación",
                             "réplica sísmica", "zona sísmica"],
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
                "keywords": ["agricultura", "maíz", "glifosato", "transgénico", "fertilizante", "cosecha", "Segalmex", "soberanía alimentaria", "autosuficiencia alimentaria",
                             "sequía agrícola", "helada", "granizada", "pérdida de cosecha", "precio del maíz",
                             "precio del frijol", "tortilla", "Sader", "secretario de agricultura", "riego",
                             "temporal", "distrito de riego", "plaga", "importación de maíz", "maíz transgénico",
                             # Leyes federales
                             "Ley Agraria", "Ley de Desarrollo Rural Sustentable",
                             "Ley de Capitalización del Procampo",
                             "Ley de Fondos de Aseguramiento Agropecuario",
                             "Ley de Energía para el Campo"],
            },
            "ganaderia_pesca": {
                "nombre": "Ganadería y pesca",
                "keywords": ["ganadería", "pesca", "acuacultura", "producción pecuaria",
                             "ganado", "producción lechera", "rastro", "jornalero", "trabajador agrícola",
                             # Leyes federales
                             "Ley de Organizaciones Ganaderas", "Ley General de Pesca y Acuacultura",
                             "Ley Federal de Sanidad Animal",
                             "Ley de Fondo de Garantía para Agricultura Ganadería y Avicultura"],
            },
            "desarrollo_rural_tierra": {
                "nombre": "Desarrollo rural y tierra",
                "keywords": ["campo mexicano", "campesino", "ejido", "tenencia de tierra", "reforma agraria", "Sembrando Vida",
                             "desarrollo rural"],
            },
            "comercio_agropecuario": {
                "nombre": "Comercio agropecuario",
                "keywords": ["productos agropecuarios", "exportación agropecuaria", "certificación agropecuaria", "precio de garantía",
                             # Leyes federales
                             "Ley de Desarrollo Sustentable de la Cafeticultura",
                             "Ley de Desarrollo Sustentable de la Caña de Azúcar",
                             "Ley de Fomento y Protección del Maíz Nativo",
                             "Ley de Fomento a la Industria Vitivinícola",
                             "Ley de Productos Orgánicos", "Ley de Producción y Comercio de Semillas",
                             "Ley Federal de Sanidad Vegetal", "Ley Federal de Variedades Vegetales"],
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
                             "cita consular", "sanciones", "lista negra", "OFAC",
                             # Leyes federales
                             "Ley del Servicio Exterior Mexicano", "Ley sobre la Celebración de Tratados",
                             "Ley sobre Tratados en Materia Económica", "Ley de Cooperación Internacional",
                             "Ley para Conservar la Neutralidad",
                             "Ley de Protección al Comercio e Inversión de Normas Extranjeras",
                             "Ley del Convenio del Banco Interamericano",
                             "Ley del Convenio de la Asociación Internacional de Fomento",
                             "Ley del Convenio del Banco de Desarrollo del Caribe"],
            },
            "america_latina": {
                "nombre": "América Latina",
                "keywords": ["América Latina", "integración regional", "CELAC", "alianza del pacífico",
                             "Centroamérica", "Guatemala", "Honduras"],
            },
            "soberania_fronteras": {
                "nombre": "Soberanía y fronteras",
                "keywords": ["soberanía nacional", "intervención extranjera", "extradición", "deportación", "frontera norte", "frontera sur",
                             "remesas", "paisano", "diáspora mexicana",
                             "custodia de ICE", "detenido por ICE", "mexicano en custodia",
                             "candidatura para la ONU", "Naciones Unidas"],
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
                             "carpeta de investigación", "Función Pública", "secretario de la función pública",
                             # Leyes federales
                             "Ley General del Sistema Nacional Anticorrupción",
                             "Ley General de Responsabilidades Administrativas",
                             "Ley General de Transparencia y Acceso a la Información"],
            },
            "transparencia_acceso": {
                "nombre": "Transparencia y acceso a información",
                "keywords": ["transparencia", "INAI", "acceso a la información", "rendición de cuentas", "datos abiertos",
                             "denuncia ciudadana", "denunciante",
                             # Leyes federales
                             "Ley de Fiscalización y Rendición de Cuentas",
                             "Ley de Contabilidad Gubernamental"],
            },
            "delitos_financieros": {
                "nombre": "Delitos financieros",
                "keywords": ["lavado de dinero", "UIF", "enriquecimiento ilícito", "extinción de dominio", "soborno",
                             "empresa fantasma", "facturera", "Pablo Gómez", "Santiago Nieto", "patrimonio inexplicable"],
            },
            "etica_publica": {
                "nombre": "Ética pública",
                "keywords": ["conflicto de interés", "nepotismo", "declaración patrimonial", "contrato público", "licitación",
                             "contratación directa", "adjudicación directa",
                             # Leyes federales
                             "Ley Federal de Austeridad Republicana",
                             "Ley de Remuneraciones de los Servidores Públicos",
                             "Ley de Responsabilidad Patrimonial del Estado",
                             "Ley de Responsabilidades de los Servidores Públicos"],
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
                "keywords": ["cambio climático", "calentamiento global", "emisiones de carbono", "gases de efecto invernadero", "Acuerdo de París",
                             # Leyes federales
                             "Ley General de Cambio Climático"],
            },
            "conservacion_biodiversidad": {
                "nombre": "Conservación y biodiversidad",
                "keywords": ["biodiversidad", "área natural protegida", "Semarnat", "Profepa", "deforestación", "especies en peligro",
                             "vida silvestre", "equilibrio ecológico", "ecosistema", "áreas naturales",
                             "jaguar", "tortuga marina", "manglar", "arrecife", "minería a cielo abierto",
                             "concesión minera", "ambientalista", "Greenpeace",
                             # Leyes federales
                             "Ley General del Equilibrio Ecológico y la Protección al Ambiente",
                             "Ley General de Vida Silvestre",
                             "Ley General de Desarrollo Forestal Sustentable",
                             "Ley de Vertimientos en Zonas Marinas", "Ley Federal del Mar"],
            },
            "contaminacion_residuos": {
                "nombre": "Contaminación y residuos",
                # Keywords de calidad del agua agregadas abr 2026 tras benchmark:
                # el caso "Contaminada, 59.1% del agua superficial de México"
                # caía en infraestructura por "Ley de Aguas Nacionales" en vez
                # de en medio_ambiente (donde sí pertenece).
                "keywords": ["contaminación ambiental", "residuos peligrosos", "contingencia ambiental", "calidad del aire",
                             "economía circular", "ley ambiental", "norma ambiental", "impacto ambiental", "política ambiental",
                             "gestión de residuos", "residuos sólidos", "Gestión Integral de los Residuos",
                             "agua contaminada", "contaminación del agua", "calidad del agua",
                             "crisis del agua", "agua superficial", "agua subterránea",
                             "mantos acuíferos", "río contaminado", "derrame petrolero",
                             "derrame químico", "derrame industrial", "derrame de hidrocarburos",
                             "basura", "relleno sanitario",
                             "tiradero", "smog", "mala calidad del aire", "contingencia",
                             # Leyes federales
                             "Ley General para la Prevención y Gestión de Residuos",
                             "Ley General de Economía Circular",
                             "Ley Federal de Responsabilidad Ambiental"],
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
                             "brecha digital", "inclusión digital", "startup", "emprendimiento tecnológico",
                             # Leyes federales
                             "Ley para Regular las Instituciones de Tecnología Financiera",
                             "Ley que crea la Agencia Espacial Mexicana"],
            },
            "ciberseguridad_datos": {
                "nombre": "Ciberseguridad y datos",
                "keywords": ["ley de ciberseguridad", "protección de datos personales", "ley de datos", "ciberataque", "privacidad digital",
                             "hackeo", "ransomware", "ataque informático", "reconocimiento facial",
                             "vigilancia masiva", "datos biométricos", "huella digital",
                             # Leyes federales
                             "Ley Federal de Protección de Datos Personales en Posesión de los Particulares",
                             "Ley General de Protección de Datos Personales en Posesión de Sujetos Obligados"],
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
                "keywords": ["adultos mayores", "personas con discapacidad", "vulnerabilidad", "asistencia social",
                             # Leyes federales
                             "Ley General de Desarrollo Social", "Ley de Asistencia Social"],
            },
            "bienestar_desarrollo": {
                "nombre": "Bienestar y desarrollo social",
                # NOTA: "bienestar" suelto se quitó porque generaba FPs en docs sobre
                # "IMSS-Bienestar" (sistema de salud, no política social). Se mantienen
                # compuestas seguras: "secretaría de bienestar", "tarjeta bienestar".
                "keywords": ["secretaría de bienestar", "secretaria de bienestar",
                             "tarjeta bienestar", "desarrollo social", "subsidio",
                             "ingreso mínimo", "transferencia directa",
                             # Leyes federales
                             "Ley de la Alimentación Adecuada y Sostenible"],
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
                             "Ley en Materia de Telecomunicaciones", "telecomunicaciones y radiodifusión",
                             # Leyes federales
                             "Ley en Materia de Telecomunicaciones y Radiodifusión"],
            },
            "television_radio": {
                "nombre": "Televisión y radio",
                "keywords": ["televisión abierta", "televisión pública", "medio radiofónico", "concesión de televisión", "radiodifusión",
                             "transmisiones deportivas", "transmisión televisiva", "señal abierta",
                             "televisión de paga", "contenido audiovisual", "canal de televisión",
                             # Leyes federales
                             "Ley del Sistema Público de Radiodifusión del Estado Mexicano"],
            },
            "libertad_prensa": {
                "nombre": "Libertad de prensa",
                "keywords": ["censura mediática", "derecho a la información", "libertad de prensa", "periodista amenazado", "regulación de contenidos",
                             # Leyes federales
                             "Ley Federal del Derecho de Autor", "Ley del Derecho de Réplica",
                             "Ley de Protección de Datos Personales",
                             "Ley de Protección a Defensores de DDHH y Periodistas"],
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
                "keywords": ["turismo", "Sectur", "política turística", "turismo sustentable",
                             # Leyes federales
                             "Ley General de Turismo"],
            },
            "infraestructura_turistica": {
                "nombre": "Infraestructura turística",
                "keywords": ["hotel", "hotelería", "industria hotelera", "infraestructura turística", "ocupación hotelera", "crucero turístico", "crucero marítimo", "buque crucero"],
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
                    # Agregadas v2 (eval set v1):
                    # - "violencia digital" + "ley olimpia" cubren reformas tipo art. 199 Octies CPF
                    # - "delito de violación", "violación sexual" cubren reformas al CPF tipo #83
                    "violencia digital", "ley olimpia",
                    "delito de violación", "violación sexual", "delitos sexuales",
                    # Leyes federales
                    "Ley General de Acceso de las Mujeres a una Vida Libre de Violencias",
                    "Ley para Prevenir Sancionar y Erradicar la Trata de Personas",
                ],
            },
            "igualdad_sustantiva": {
                "nombre": "Igualdad sustantiva",
                "keywords": [
                    "igualdad de género", "igualdad sustantiva", "brecha salarial",
                    "igualdad laboral", "discriminación de género", "equidad salarial",
                    # Agregadas v2 (eval set v1) — compuestas con "mujeres" como sujeto temático.
                    # NO se agrega "mujeres" suelta porque genera FPs en cualquier nota
                    # política que mencione mujeres en otro contexto.
                    "derechos de las mujeres", "derechos de la mujer",
                    "mujeres rurales", "mujeres indígenas", "mujer indígena",
                    "mujeres afromexicanas", "mujeres emprendedoras",
                    "empoderamiento de la mujer", "empoderamiento de las mujeres",
                    # Leyes federales
                    "Ley General para la Igualdad Sustantiva entre Mujeres y Hombres",
                ],
            },
            "participacion_politica_mujeres": {
                "nombre": "Participación política de mujeres",
                "keywords": [
                    "paridad de género", "paridad en todo",
                    "violencia política de género",
                    "participación política de mujeres", "cuotas de género",
                    # Agregadas v2 (eval set v1, caso #73): el clasificador no detectaba
                    # "comisiones paritarias" porque "paritaria" no se stemifica a "paridad".
                    "integración paritaria", "comisiones paritarias",
                    "comisión paritaria", "representación paritaria",
                ],
            },
            "derechos_reproductivos": {
                "nombre": "Derechos reproductivos",
                "keywords": [
                    "aborto", "derechos reproductivos",
                    "interrupción legal del embarazo",
                    "salud reproductiva", "maternidad",
                    # Agregadas v2 (eval set v1, caso #56): lactancia materna y salas
                    # de lactancia entran como derechos reproductivos / maternidad.
                    "lactancia materna", "salas de lactancia", "sala de lactancia",
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
            # Agregada v3 (abr 2026): el clasificador solo tenía "diversidad sexual"
            # bajo derechos_humanos.genero_diversidad, lo que dejaba fuera a
            # igualdad_genero en propuestas LGBT+. En MX INMUJERES y Conavim
            # también intervienen en diversidad, así que debe aparecer en ambas.
            # Caso testigo: propuesta INEGI/censo 2030 de Damaris Silva (abr 2026).
            "diversidad_sexual_genero": {
                "nombre": "Diversidad sexual y de género",
                "keywords": [
                    "diversidad sexual y de género", "diversidad sexual",
                    "diversidad de género", "identidad de género",
                    "orientación sexual", "expresión de género",
                    "matrimonio igualitario", "adopción homoparental",
                    "población de la diversidad sexual",
                    "personas LGBT", "personas LGBTI", "comunidad LGBT",
                    "LGBT", "LGBTI", "LGBTI+", "LGBTTTI", "LGBTTTIQ", "LGBTTTIQ+",
                    "homofobia", "transfobia", "lesbofobia", "bifobia",
                    "transgénero", "transexual", "personas trans",
                    "infancias trans", "adolescencias trans",
                ],
            },
        },
    },
    # ─────────────────────────────────────────────
    # 19. ADMINISTRACIÓN
    # Agregada v3 (post eval set v1). Cubre el gap estructural detectado en el
    # etiquetado manual: reformas constitucionales al art. 25/73, Ley Orgánica
    # de la APF, servicio profesional de carrera, trámites burocráticos,
    # planeación nacional, federalismo y desarrollo municipal.
    # Caso ancla: #21 "Reforma constitucional al art. 25 y 73".
    # ─────────────────────────────────────────────
    "administracion": {
        "nombre": "Administración",
        "comisiones": [
            # Diputados
            "Gobernación y Población",
            "Reforma del Estado",
            # Senado LXVI
            "Gobernación",
            "Puntos Constitucionales",
            "Federalismo",
            "Desarrollo Municipal",
        ],
        "trends_keywords": [
            "administración pública México",
            "reforma del Estado",
            "Plan Nacional de Desarrollo",
            "federalismo México",
            "simplificación administrativa",
        ],
        "subcategorias": {
            # NOTA v3: el clasificador es token-based + substring, así que keywords
            # como "reforma constitucional" o "servidores públicos" explotan porque
            # sus tokens ("reforma", "artículo", "constitucional", "servidor", "público")
            # son genéricos y aparecen en casi cualquier iniciativa. Las keywords aquí
            # deben ser frases específicas que capturan el dominio administrativo sin
            # desparramarse. La recuperación fina de reformas constitucionales
            # estructurales (casos tipo art. 25/73) se hace vía COMISION_A_CATEGORIA
            # con "puntos constitucionales" → administracion.
            "estructura_estado": {
                "nombre": "Estructura del Estado",
                "keywords": [
                    "rectoría del Estado", "rectoría económica del Estado",
                    "división de poderes",
                    "facultades del Congreso de la Unión",
                    "atribuciones del ejecutivo federal",
                    "competencias concurrentes", "órganos del Estado mexicano",
                ],
            },
            "administracion_publica_federal": {
                "nombre": "Administración pública federal",
                "keywords": [
                    "administración pública federal",
                    "Ley Orgánica de la Administración Pública Federal",
                    "secretarías de Estado", "Oficina de la Presidencia",
                    "gabinete presidencial", "dependencias del ejecutivo federal",
                    "entidades paraestatales",
                    "organismo descentralizado", "organismos descentralizados",
                    "empresa productiva del Estado", "empresas productivas del Estado",
                    "fideicomiso público federal", "fideicomisos públicos federales",
                    "órganos desconcentrados",
                    "consejería jurídica del ejecutivo",
                    "reestructuración administrativa",
                    "Ley de Entidades Paraestatales",
                ],
            },
            "servicio_civil": {
                "nombre": "Servicio civil de carrera",
                "keywords": [
                    "servicio profesional de carrera",
                    "servicio civil de carrera",
                    "mandos medios y superiores",
                    "Ley del Servicio Profesional de Carrera",
                    "Ley de Premios Estímulos y Recompensas Civiles",
                ],
            },
            "procedimiento_administrativo": {
                "nombre": "Procedimiento administrativo y trámites",
                "keywords": [
                    "procedimiento administrativo",
                    "trámites burocráticos", "simplificación administrativa",
                    "ventanilla única", "mejora regulatoria",
                    "firma electrónica avanzada", "gobierno digital",
                    "digitalización de trámites",
                    "Ley de Procedimiento Administrativo",
                    "Ley Nacional para Eliminar Trámites Burocráticos",
                    "Ley de Firma Electrónica Avanzada",
                    "Diario Oficial de la Federación",
                    "Ley del Diario Oficial",
                ],
            },
            "planeacion_federalismo": {
                "nombre": "Planeación y federalismo",
                "keywords": [
                    "Plan Nacional de Desarrollo",
                    "sistema nacional de planeación",
                    "planeación democrática",
                    "Ley de Planeación",
                    "pacto federal", "coordinación intergubernamental",
                    "descentralización administrativa",
                    "conferencia nacional de gobernadores",
                    "fortalecimiento municipal",
                    "autonomía municipal",
                ],
            },
        },
    },
}


# ─────────────────────────────────────────────
# HELPER: Obtener keywords planos por categoría
# ─────────────────────────────────────────────
def obtener_keywords_categoria(cat_clave):
    """Retorna la unión de keywords de todas las subcategorías + nombres de leyes federales.
    Backward-compatible: si la categoría aún tiene 'keywords' (legacy), los retorna directamente."""
    cat = CATEGORIAS[cat_clave]
    if "keywords" in cat:  # fallback legacy
        todos = set(cat["keywords"])
    else:
        todos = set()
        for sub in cat.get("subcategorias", {}).values():
            todos.update(sub["keywords"])
    # Agregar nombres de leyes federales mapeadas a esta categoría
    for ley_nombre, ley_cat in LEYES_FEDERALES.items():
        if ley_cat == cat_clave:
            todos.add(ley_nombre)
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

# SCORE = (0.20×Media) + (0.15×Trends) + (0.25×Congreso) + (0.10×Mañanera) + (0.15×Urgencia) + (0.15×Dominancia)

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
    "min_confianza": 0.3,          # Umbral mínimo (bajado abr 2026 tras fix word boundary
                                   # + match de tokens compuestos. Los FPs que antes pasaban
                                   # por tokens sueltos ya no existen, así que el umbral puede
                                   # ser más permisivo para rescatar casos con 1 keyword limpia.)
    "max_categorias": 3,           # Probado v3=4 contra eval set v1 → meseta (recupera 1 TP en derechos_humanos pero mete 4 FPs en categorías ruidosas). Volver a 3 hasta arreglar la precision de electoral_politico/relaciones_exteriores.
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
    # Administración (v3 — movidas desde electoral_politico)
    ("gobernación", "administracion"),
    ("gobernacion", "administracion"),
    ("reforma del estado", "administracion"),
    ("puntos constitucionales", "administracion"),
    ("federalismo", "administracion"),
    ("desarrollo municipal", "administracion"),
    # Electoral y Político
    ("reforma política-electoral", "electoral_politico"),
    ("reforma politica-electoral", "electoral_politico"),
    ("participación ciudadana", "electoral_politico"),
    ("participacion ciudadana", "electoral_politico"),
    ("reglamentos y prácticas parlamentarias", "electoral_politico"),
    ("medalla belisario", "electoral_politico"),
    ("estudios legislativos", "electoral_politico"),
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
# LEYES FEDERALES → CATEGORÍA (lookup rápido)
# Mapea nombre corto de cada ley federal a su categoría FIAT primaria.
# ─────────────────────────────────────────────
LEYES_FEDERALES = {
    # ── seguridad_justicia ──
    "Código Penal Federal": "seguridad_justicia",
    "Código Nacional de Procedimientos Penales": "seguridad_justicia",
    "Código de Justicia Militar": "seguridad_justicia",
    "Código Militar de Procedimientos Penales": "seguridad_justicia",
    "Código Civil Federal": "seguridad_justicia",
    "Código Federal de Procedimientos Civiles": "seguridad_justicia",
    "Código Nacional de Procedimientos Civiles y Familiares": "seguridad_justicia",
    "Ley de Amparo": "seguridad_justicia",
    "Ley de Carrera Judicial": "seguridad_justicia",
    "Ley Orgánica del Poder Judicial": "seguridad_justicia",
    "Ley Nacional de Ejecución Penal": "seguridad_justicia",
    "Ley Nacional de Mecanismos Alternativos en Materia Penal": "seguridad_justicia",
    "Ley Nacional del Sistema Integral de Justicia para Adolescentes": "seguridad_justicia",
    "Ley Federal contra la Delincuencia Organizada": "seguridad_justicia",
    "Ley Nacional de Extinción de Dominio": "seguridad_justicia",
    "Ley Federal de Armas de Fuego y Explosivos": "seguridad_justicia",
    "Ley de Extradición Internacional": "seguridad_justicia",
    "Ley para Prevenir y Sancionar Delitos de Secuestro": "seguridad_justicia",
    "Ley de Delitos de Extorsión": "seguridad_justicia",
    "Ley para Prevenir e Identificar Operaciones con Recursos Ilícitos": "seguridad_justicia",
    "Ley de la Guardia Nacional": "seguridad_justicia",
    "Ley de Seguridad Nacional": "seguridad_justicia",
    "Ley Orgánica del Ejército y Fuerza Aérea": "seguridad_justicia",
    "Ley Orgánica de la Armada": "seguridad_justicia",
    "Ley de Ascensos de la Armada": "seguridad_justicia",
    "Ley de Ascensos del Ejército": "seguridad_justicia",
    "Ley de Disciplina del Ejército": "seguridad_justicia",
    "Ley del Servicio Militar": "seguridad_justicia",
    "Ley del Instituto de Seguridad Social para las Fuerzas Armadas": "seguridad_justicia",
    "Ley de Protección del Espacio Aéreo": "seguridad_justicia",
    "Ley del Sistema Nacional de Investigación e Inteligencia": "seguridad_justicia",
    "Ley Nacional sobre el Uso de la Fuerza": "seguridad_justicia",
    "Ley de la Policía Federal": "seguridad_justicia",
    "Ley de Seguridad Privada": "seguridad_justicia",
    "Ley General de Víctimas": "seguridad_justicia",
    "Ley para Prevenir y Sancionar la Tortura": "seguridad_justicia",
    "Ley de Prevención Social de la Violencia": "seguridad_justicia",
    "Ley de Declaración Especial de Ausencia para Personas Desaparecidas": "seguridad_justicia",
    "Ley de Desaparición Forzada": "seguridad_justicia",
    "Ley de Protección a Personas que Intervienen en el Procedimiento Penal": "seguridad_justicia",
    # ── economia_hacienda ──
    "Código Fiscal de la Federación": "economia_hacienda",
    "Ley de Ingresos de la Federación": "economia_hacienda",
    "Ley del Impuesto al Valor Agregado": "economia_hacienda",
    "Ley del ISR": "economia_hacienda",
    "Ley del IEPS": "economia_hacienda",
    "Ley del Impuesto Especial sobre Producción y Servicios": "economia_hacienda",
    "Ley Federal de Derechos": "economia_hacienda",
    "Ley de Coordinación Fiscal": "economia_hacienda",
    "Ley Federal de Presupuesto y Responsabilidad Hacendaria": "economia_hacienda",
    "Presupuesto de Egresos de la Federación": "economia_hacienda",
    "Ley del SAT": "economia_hacienda",
    "Ley Federal de Deuda Pública": "economia_hacienda",
    "Ley de Disciplina Financiera": "economia_hacienda",
    "Ley Aduanera": "economia_hacienda",
    "Ley de los Impuestos Generales de Importación y Exportación": "economia_hacienda",
    "Ley del Impuesto sobre Automóviles Nuevos": "economia_hacienda",
    "Ley de Contribución de Mejoras": "economia_hacienda",
    "Ley de los Derechos del Contribuyente": "economia_hacienda",
    "Ley de Tesorería": "economia_hacienda",
    "Ley de Instituciones de Crédito": "economia_hacienda",
    "Ley del Mercado de Valores": "economia_hacienda",
    "Ley de la CNBV": "economia_hacienda",
    "Ley del Banco de México": "economia_hacienda",
    "Ley de Ahorro y Crédito Popular": "economia_hacienda",
    "Ley de Uniones de Crédito": "economia_hacienda",
    "Ley de Fondos de Inversión": "economia_hacienda",
    "Ley de Instituciones de Seguros y de Fianzas": "economia_hacienda",
    "Ley de Protección al Ahorro Bancario": "economia_hacienda",
    "Ley de Sistemas de Pagos": "economia_hacienda",
    "Ley de la Casa de Moneda": "economia_hacienda",
    "Ley Monetaria": "economia_hacienda",
    "Ley de CONDUSEF": "economia_hacienda",
    "Ley de Sociedades de Información Crediticia": "economia_hacienda",
    "Ley para Regular Agrupaciones Financieras": "economia_hacienda",
    "Ley de Tecnología Financiera": "economia_hacienda",
    "Ley de los Sistemas de Ahorro para el Retiro": "economia_hacienda",
    "Ley de Transparencia de Servicios Financieros": "economia_hacienda",
    "Ley de Transparencia en el Crédito Garantizado": "economia_hacienda",
    "Ley del Seguro": "economia_hacienda",
    "Ley de Organizaciones Auxiliares del Crédito": "economia_hacienda",
    "Ley de Cooperativas de Ahorro y Préstamo": "economia_hacienda",
    "Ley Orgánica de Nacional Financiera": "economia_hacienda",
    "Ley Orgánica de Sociedad Hipotecaria Federal": "economia_hacienda",
    "Ley Orgánica de Bancomext": "economia_hacienda",
    "Ley Orgánica de Banobras": "economia_hacienda",
    "Ley Orgánica de Banjercito": "economia_hacienda",
    "Ley Orgánica del Banco del Bienestar": "economia_hacienda",
    "Código de Comercio": "economia_hacienda",
    "Ley de Comercio Exterior": "economia_hacienda",
    "Ley de Concursos Mercantiles": "economia_hacienda",
    "Ley Federal de Competencia Económica": "economia_hacienda",
    "Ley Federal de Protección al Consumidor": "economia_hacienda",
    "Ley General de Sociedades Mercantiles": "economia_hacienda",
    "Ley General de Títulos y Operaciones de Crédito": "economia_hacienda",
    "Ley de Inversión Extranjera": "economia_hacienda",
    "Ley de Cámaras Empresariales": "economia_hacienda",
    "Ley de Fomento a la Microindustria": "economia_hacienda",
    "Ley de Infraestructura de la Calidad": "economia_hacienda",
    "Ley de Productividad y Competitividad": "economia_hacienda",
    "Ley de Economía Social y Solidaria": "economia_hacienda",
    "Ley de Propiedad Industrial": "economia_hacienda",
    "Ley de Sociedades Cooperativas": "economia_hacienda",
    "Ley de Adquisiciones del Sector Público": "economia_hacienda",
    "Ley de Zonas Económicas Especiales": "economia_hacienda",
    "Ley de Fomento a la Confianza Ciudadana": "economia_hacienda",
    "Ley de Sociedades de Responsabilidad Limitada": "economia_hacienda",
    "Ley de Sociedades de Solidaridad Social": "economia_hacienda",
    "Ley de Expropiación": "economia_hacienda",
    # ── energia ──
    "Ley del Sector Hidrocarburos": "energia",
    "Ley de la Empresa Pública Pemex": "energia",
    "Ley de Ingresos sobre Hidrocarburos": "energia",
    "Ley del Fondo Mexicano del Petróleo": "energia",
    "Ley de la ASEA": "energia",
    "Ley para Prevenir y Sancionar Delitos en Materia de Hidrocarburos": "energia",
    "Ley del Sector Eléctrico": "energia",
    "Ley de la Empresa Pública CFE": "energia",
    "Ley de la Comisión Nacional de Energía": "energia",
    "Ley de Planeación y Transición Energética": "energia",
    "Ley de Geotermia": "energia",
    "Ley de Biocombustibles": "energia",
    "Ley de Energía para el Campo": "energia",
    "Ley de Responsabilidad Civil por Daños Nucleares": "energia",
    # ── salud ──
    "Ley General de Salud": "salud",
    "Ley de los Institutos Nacionales de Salud": "salud",
    "Ley de Bioseguridad de Organismos Genéticamente Modificados": "salud",
    "Ley Federal para el Control de Precursores Químicos": "salud",
    "Ley Federal para el Control de Sustancias Químicas": "salud",
    "Ley General para el Control del Tabaco": "salud",
    "Ley General para la Detección Oportuna del Cáncer": "salud",
    "Ley General de la Alimentación Adecuada y Sostenible": "salud",
    "Ley de Ayuda Alimentaria para los Trabajadores": "salud",
    # ── educacion ──
    "Ley General de Educación": "educacion",
    "Ley General de Educación Superior": "educacion",
    "Ley de Mejora Continua de la Educación": "educacion",
    "Ley del Sistema para la Carrera de las Maestras y los Maestros": "educacion",
    "Ley General de Humanidades Ciencias Tecnologías e Innovación": "educacion",
    "Ley General de Cultura y Derechos Culturales": "educacion",
    "Ley General de Bibliotecas": "educacion",
    "Ley de Fomento para la Lectura y el Libro": "educacion",
    "Ley de Derechos Lingüísticos": "educacion",
    "Ley de Protección del Patrimonio Cultural de Pueblos Indígenas": "educacion",
    "Ley sobre Monumentos y Zonas Arqueológicos": "educacion",
    "Ley del INAH": "educacion",
    "Ley del INBAL": "educacion",
    "Ley Orgánica de la UNAM": "educacion",
    "Ley Orgánica de la UAM": "educacion",
    "Ley Orgánica del IPN": "educacion",
    "Ley de la Universidad Autónoma Chapingo": "educacion",
    "Ley del Seminario de Cultura Mexicana": "educacion",
    # ── trabajo ──
    "Ley Federal del Trabajo": "trabajo",
    "Ley Federal de los Trabajadores al Servicio del Estado": "trabajo",
    "Ley del Centro Federal de Conciliación y Registro Laboral": "trabajo",
    "Ley del Seguro Social": "trabajo",
    "Ley del ISSSTE": "trabajo",
    "Ley del FONACOT": "trabajo",
    "Ley del Infonavit": "trabajo",
    # ── electoral_politico ──
    "Ley General de Instituciones y Procedimientos Electorales": "electoral_politico",
    "Ley General de Partidos Políticos": "electoral_politico",
    "Ley General de Delitos Electorales": "electoral_politico",
    "Ley del Sistema de Medios de Impugnación Electoral": "electoral_politico",
    "Ley de Consulta Popular": "electoral_politico",
    "Ley de Revocación de Mandato": "electoral_politico",
    "Constitución Política": "electoral_politico",
    "Ley Orgánica del Congreso": "electoral_politico",
    "Reglamento de la Cámara de Diputados": "electoral_politico",
    "Reglamento del Senado": "electoral_politico",
    "Ley General de Comunicación Social": "electoral_politico",
    "Ley General de Población": "electoral_politico",
    "Ley Reglamentaria del Artículo 5o Constitucional": "electoral_politico",
    "Ley de los Husos Horarios": "electoral_politico",
    "Ley sobre el Escudo la Bandera y el Himno": "electoral_politico",
    # ── administracion (v3 — movidas desde electoral_politico) ──
    "Ley Orgánica de la Administración Pública Federal": "administracion",
    "Ley de Planeación": "administracion",
    "Ley de Entidades Paraestatales": "administracion",
    "Ley del Diario Oficial": "administracion",
    "Ley del Servicio Profesional de Carrera": "administracion",
    "Ley de Procedimiento Administrativo": "administracion",
    "Ley de Premios Estímulos y Recompensas Civiles": "administracion",
    "Ley Nacional para Eliminar Trámites Burocráticos": "administracion",
    "Ley de Firma Electrónica Avanzada": "administracion",
    "Estatuto de Gobierno del Distrito Federal": "administracion",
    # ── derechos_humanos ──
    "Ley de la CNDH": "derechos_humanos",
    "Ley de Amnistía": "derechos_humanos",
    "Ley Federal para Prevenir y Eliminar la Discriminación": "derechos_humanos",
    "Ley de Migración": "derechos_humanos",
    "Ley sobre Refugiados y Asilo Político": "derechos_humanos",
    "Ley de Nacionalidad": "derechos_humanos",
    "Ley del Instituto Nacional de los Pueblos Indígenas": "derechos_humanos",
    "Ley de Derechos Lingüísticos de los Pueblos Indígenas": "derechos_humanos",
    "Ley General de los Derechos de Niñas Niños y Adolescentes": "derechos_humanos",
    "Ley General de Prestación de Servicios para Cuidado Infantil": "derechos_humanos",
    "Ley de la Condición del Espectro Autista": "derechos_humanos",
    "Ley de Asociaciones Religiosas": "derechos_humanos",
    "Ley General para la Inclusión de Personas con Discapacidad": "derechos_humanos",
    "Ley de los Derechos de Personas Adultas Mayores": "derechos_humanos",
    # ── infraestructura ──
    "Ley de Aeropuertos": "infraestructura",
    "Ley de Aviación Civil": "infraestructura",
    "Ley de Caminos Puentes y Autotransporte Federal": "infraestructura",
    "Ley Reglamentaria del Servicio Ferroviario": "infraestructura",
    "Ley de Vías Generales de Comunicación": "infraestructura",
    "Ley General de Movilidad y Seguridad Vial": "infraestructura",
    "Ley de Navegación y Comercio Marítimos": "infraestructura",
    "Ley de Puertos": "infraestructura",
    "Ley de Obras Públicas": "infraestructura",
    "Ley de Asociaciones Público Privadas": "infraestructura",
    "Ley de Aguas Nacionales": "infraestructura",
    "Ley General de Aguas": "infraestructura",
    "Ley de Vivienda": "infraestructura",
    "Ley General de Asentamientos Humanos": "infraestructura",
    "Ley en Materia de Telecomunicaciones y Radiodifusión": "infraestructura",
    # ── agro_rural ──
    "Ley Agraria": "agro_rural",
    "Ley de Desarrollo Rural Sustentable": "agro_rural",
    "Ley de Capitalización del Procampo": "agro_rural",
    "Ley de Fondos de Aseguramiento Agropecuario": "agro_rural",
    "Ley de Organizaciones Ganaderas": "agro_rural",
    "Ley General de Pesca y Acuacultura": "agro_rural",
    "Ley Federal de Sanidad Animal": "agro_rural",
    "Ley de Fondo de Garantía para Agricultura Ganadería y Avicultura": "agro_rural",
    "Ley de Desarrollo Sustentable de la Cafeticultura": "agro_rural",
    "Ley de Desarrollo Sustentable de la Caña de Azúcar": "agro_rural",
    "Ley de Fomento y Protección del Maíz Nativo": "agro_rural",
    "Ley de Fomento a la Industria Vitivinícola": "agro_rural",
    "Ley de Productos Orgánicos": "agro_rural",
    "Ley de Producción y Comercio de Semillas": "agro_rural",
    "Ley Federal de Sanidad Vegetal": "agro_rural",
    "Ley Federal de Variedades Vegetales": "agro_rural",
    # ── relaciones_exteriores ──
    "Ley del Servicio Exterior Mexicano": "relaciones_exteriores",
    "Ley sobre la Celebración de Tratados": "relaciones_exteriores",
    "Ley sobre Tratados en Materia Económica": "relaciones_exteriores",
    "Ley de Cooperación Internacional": "relaciones_exteriores",
    "Ley para Conservar la Neutralidad": "relaciones_exteriores",
    "Ley de Protección al Comercio e Inversión de Normas Extranjeras": "relaciones_exteriores",
    "Ley del Convenio del Banco Interamericano": "relaciones_exteriores",
    "Ley del Convenio de la Asociación Internacional de Fomento": "relaciones_exteriores",
    "Ley del Convenio del Banco de Desarrollo del Caribe": "relaciones_exteriores",
    # ── anticorrupcion ──
    "Ley General del Sistema Nacional Anticorrupción": "anticorrupcion",
    "Ley General de Responsabilidades Administrativas": "anticorrupcion",
    "Ley General de Transparencia y Acceso a la Información": "anticorrupcion",
    "Ley de Fiscalización y Rendición de Cuentas": "anticorrupcion",
    "Ley de Contabilidad Gubernamental": "anticorrupcion",
    "Ley Federal de Austeridad Republicana": "anticorrupcion",
    "Ley de Remuneraciones de los Servidores Públicos": "anticorrupcion",
    "Ley de Responsabilidad Patrimonial del Estado": "anticorrupcion",
    "Ley de Responsabilidades de los Servidores Públicos": "anticorrupcion",
    # ── medio_ambiente ──
    "Ley General del Equilibrio Ecológico y la Protección al Ambiente": "medio_ambiente",
    "Ley General de Vida Silvestre": "medio_ambiente",
    "Ley General de Desarrollo Forestal Sustentable": "medio_ambiente",
    "Ley de Vertimientos en Zonas Marinas": "medio_ambiente",
    "Ley Federal del Mar": "medio_ambiente",
    "Ley General de Cambio Climático": "medio_ambiente",
    "Ley General para la Prevención y Gestión de Residuos": "medio_ambiente",
    "Ley General de Economía Circular": "medio_ambiente",
    "Ley Federal de Responsabilidad Ambiental": "medio_ambiente",
    "Ley de Minería": "medio_ambiente",
    # ── medios_comunicacion ──
    "Ley del Sistema Público de Radiodifusión del Estado Mexicano": "medios_comunicacion",
    "Ley Federal de Cinematografía": "medios_comunicacion",
    "Ley Federal del Derecho de Autor": "medios_comunicacion",
    "Ley del Derecho de Réplica": "medios_comunicacion",
    "Ley de Protección de Datos Personales": "medios_comunicacion",
    "Ley de Protección a Defensores de DDHH y Periodistas": "medios_comunicacion",
    # ── turismo ──
    "Ley General de Turismo": "turismo",
    # ── igualdad_genero ──
    "Ley General de Acceso de las Mujeres a una Vida Libre de Violencias": "igualdad_genero",
    "Ley para Prevenir Sancionar y Erradicar la Trata de Personas": "igualdad_genero",
    "Ley General para la Igualdad Sustantiva entre Mujeres y Hombres": "igualdad_genero",
    # ── politica_social ──
    "Ley General de Desarrollo Social": "politica_social",
    "Ley de Asistencia Social": "politica_social",
    "Ley de la Alimentación Adecuada y Sostenible": "politica_social",
    # ── inteligencia_artificial ──
    "Ley Federal de Protección de Datos Personales en Posesión de los Particulares": "inteligencia_artificial",
    "Ley General de Protección de Datos Personales en Posesión de Sujetos Obligados": "inteligencia_artificial",
    "Ley para Regular las Instituciones de Tecnología Financiera": "inteligencia_artificial",
    "Ley que crea la Agencia Espacial Mexicana": "inteligencia_artificial",
}

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
    # Noticias económicas/políticas/turísticas de otros países
    "dólar en Colombia", "peso colombiano", "peso argentino",
    "real brasileño", "sol peruano", "bolívar venezolano",
    "economía de Colombia", "economía de Argentina", "economía de Perú",
    "elecciones en Colombia", "elecciones en Argentina", "elecciones en Chile",
    "elecciones en Perú", "elecciones en Brasil",
    "desde Chile", "desde Colombia", "desde Argentina", "desde Perú",
    "desde Brasil", "desde Venezuela",
    "guía de viaje", "SoloCruceros",
    # Gentilicios como adjetivo (señal fuerte de noticia no-México)
    "argentino", "argentina", "colombiano", "colombiana",
    "peruano", "peruana", "chileno", "chilena",
    "brasileño", "brasileña", "venezolano", "venezolana",
    "ecuatoriano", "ecuatoriana", "uruguayo", "uruguaya",
    "paraguayo", "paraguaya", "boliviano", "boliviana",
    "costarricense", "panameño", "panameña",
    # Figuras y partidos internacionales no-México
    "Steffon Diggs", "Stefon Diggs", "Tom Brady", "LeBron James",
    "Taylor Swift", "Elon Musk", "Jeff Bezos",
    "Milei", "Boric", "Petro", "Lula",
    "Fujimori", "Keiko Fujimori", "Bukele", "Maduro",
    "Bolsonaro", "Castillo", "Arce", "Lacalle Pou",
    "parlamento de Perú", "congreso peruano",
    # Figuras económicas internacionales
    "Dujovne", "Sergio Massa", "Luis Caputo",
    "Paulo Guedes", "Haddad", "Roberto Campos Neto",
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

# ─────────────────────────────────────────────
# COMISIONES OFICIALES DEL SENADO (LXVI Legislatura)
# 68 ordinarias + 4 especiales = 72 total
# ─────────────────────────────────────────────
COMISIONES_SENADO = [
    "Administración",
    "Agricultura",
    "Análisis, Seguimiento y Evaluación sobre la aplicación y desarrollo de la Inteligencia Artificial en México",
    "Anticorrupción y Transparencia",
    "Asuntos de la Frontera Norte",
    "Asuntos de la Frontera Sur",
    "Asuntos Migratorios",
    "Bienestar",
    "Ciberseguridad",
    "Ciencia, Humanidades, Tecnología e Innovación",
    "Comunicaciones y Transportes",
    "Cultura",
    "Defensa de los Consumidores",
    "Defensa Nacional",
    "Deporte",
    "Derechos de la Niñez y de la Adolescencia",
    "Derechos Digitales",
    "Derechos Humanos",
    "Desarrollo Municipal",
    "Desarrollo Regional",
    "Desarrollo Rural",
    "Desarrollo Urbano y Ordenamiento Territorial",
    "Economía",
    "Educación",
    "Energía",
    "Estudios Legislativos",
    "Estudios Legislativos, Primera",
    "Estudios Legislativos, Segunda",
    "Federalismo",
    "Fomento Económico y al Emprendimiento",
    "Ganadería",
    "Gobernación",
    "Guardia Nacional",
    "Hacienda y Crédito Público",
    "Infraestructura Ferroviaria",
    "Jurisdiccional",
    "Justicia",
    "Juventud",
    "Marina",
    "Medalla Belisario Domínguez",
    "Medio Ambiente, Recursos Naturales y Cambio Climático",
    "Minería",
    "Organismos Internacionales",
    "Para el Seguimiento a la Implementación de la Agenda 2030",
    "Para la Igualdad de Género",
    "Participación Ciudadana",
    "Pesca y Acuacultura",
    "Pueblos Indígenas y Afromexicanos",
    "Puertos e Infraestructura Marítima",
    "Puntos Constitucionales",
    "Radio, Televisión y Cinematografía",
    "Recursos Hídricos e Infraestructura Hidráulica",
    "Reforma Agraria",
    "Reglamentos y Prácticas Parlamentarias",
    "Relaciones Exteriores",
    "Relaciones Exteriores, África",
    "Relaciones Exteriores, América del Norte",
    "Relaciones Exteriores, América Latina y el Caribe",
    "Relaciones Exteriores, Asia-Pacífico",
    "Relaciones Exteriores, Europa",
    "Reordenamiento Urbano y Vivienda",
    "Salud",
    "Seguimiento a la Implementación y Revisión del T-MEC",
    "Seguridad Pública",
    "Seguridad Social",
    "Trabajo y Previsión Social",
    "Turismo",
    "Zonas Metropolitanas y Movilidad",
    # Especiales
    "Comisión Especial de Ciudades Sostenibles",
    "Comisión Especial de Economía Circular y Desarrollo Empresarial",
    "Comisión Especial de Seguimiento e Impulso al Corredor Interoceánico del Istmo de Tehuantepec",
    "Comisión Especial para revisar y vigilar el proceso de quiebra de Altos Hornos de México, S. A. B. de C. V.",
]

# Índice invertido para normalización rápida (lowercase → nombre canónico)
_COMISIONES_SENADO_INDEX = {}
for _c in COMISIONES_SENADO:
    _COMISIONES_SENADO_INDEX[_c.lower()] = _c
    # Variantes sin prefijo "Comisión Especial de/para..."
    if _c.startswith("Comisión Especial de "):
        _COMISIONES_SENADO_INDEX[_c[21:].lower()] = _c
    elif _c.startswith("Comisión Especial para "):
        _COMISIONES_SENADO_INDEX[_c[23:].lower()] = _c
    # Variante "Asuntos Frontera Sur" sin "de la"
    if " de la " in _c:
        _COMISIONES_SENADO_INDEX[_c.replace(" de la ", " ").lower()] = _c
    if " de los " in _c:
        _COMISIONES_SENADO_INDEX[_c.replace(" de los ", " ").lower()] = _c
    # Variante con "y" en vez de coma: "Radio Televisión y Cinematografía"
    _COMISIONES_SENADO_INDEX[_c.lower().replace(", ", " y ")] = _c

# Variantes comunes con typos o formatos alternativos
_COMISIONES_SENADO_INDEX["radio televisión y cinematografía"] = "Radio, Televisión y Cinematografía"
_COMISIONES_SENADO_INDEX["radio television y cinematografía"] = "Radio, Televisión y Cinematografía"
_COMISIONES_SENADO_INDEX["medo ambiente, recursos naturales y cambio climático"] = "Medio Ambiente, Recursos Naturales y Cambio Climático"
_COMISIONES_SENADO_INDEX["medio ambiente recursos naturales y cambio climático"] = "Medio Ambiente, Recursos Naturales y Cambio Climático"
_COMISIONES_SENADO_INDEX["reglamentos y práctica parlamentarias"] = "Reglamentos y Prácticas Parlamentarias"
_COMISIONES_SENADO_INDEX["recursos e infraestructura hidráulica"] = "Recursos Hídricos e Infraestructura Hidráulica"
_COMISIONES_SENADO_INDEX["asuntos frontera sur"] = "Asuntos de la Frontera Sur"
_COMISIONES_SENADO_INDEX["asuntos frontera norte"] = "Asuntos de la Frontera Norte"
_COMISIONES_SENADO_INDEX["especial de seguimiento a la implementación de la agenda 2030"] = "Para el Seguimiento a la Implementación de la Agenda 2030"
_COMISIONES_SENADO_INDEX["comisión especial de seguimiento a la implementación de la agenda 2030"] = "Para el Seguimiento a la Implementación de la Agenda 2030"
_COMISIONES_SENADO_INDEX["especial para revisar y vigilar el proceso de quiebra de altos hornos de méxico"] = "Comisión Especial para revisar y vigilar el proceso de quiebra de Altos Hornos de México, S. A. B. de C. V."


def normalizar_comision_senado(nombre_raw):
    """
    Normaliza un nombre de comisión del Senado contra la lista oficial.
    Maneja nombres sucios como 'AGRICULTURAConvocatoria a la...'
    y comisiones unidas como 'Agricultura y de Estudios Legislativos, Primera'.
    Retorna el nombre canónico o None si no se reconoce.
    """
    if not nombre_raw or nombre_raw == "No especificada":
        return None

    nombre = nombre_raw.strip()

    # 1. Match exacto (case-insensitive)
    if nombre.lower() in _COMISIONES_SENADO_INDEX:
        return _COMISIONES_SENADO_INDEX[nombre.lower()]

    # 2. Comisiones unidas: "X y de Y" → tomar la primera
    if " y de " in nombre and nombre[0].isupper() and nombre[0:1] != nombre[0:1].upper():
        primera = nombre.split(" y de ")[0].strip()
        if primera.lower() in _COMISIONES_SENADO_INDEX:
            return _COMISIONES_SENADO_INDEX[primera.lower()]

    # 3. Nombre en MAYÚSCULAS pegado a texto (ej: "AGRICULTURAConvocatoria...")
    #    Buscar la comisión más larga cuyo nombre en mayúsculas sea prefijo.
    #    Se normaliza la puntuación para que "RELACIONES EXTERIORES ÁFRICA..."
    #    (sin coma) matchee "Relaciones Exteriores, África" (con coma).
    import re as _re_punct
    nombre_upper = nombre.upper()
    _strip_punct = lambda s: _re_punct.sub(r"[,\.]", "", s).upper()
    nombre_upper_norm = _strip_punct(nombre)
    mejor = None
    mejor_len = 0
    for canonica in COMISIONES_SENADO:
        prefijo = _strip_punct(canonica)
        if nombre_upper_norm.startswith(prefijo):
            if len(prefijo) > mejor_len:
                mejor = canonica
                mejor_len = len(prefijo)
    if mejor:
        return mejor

    # 4. Prefijo en title case (comisiones unidas con título normal)
    nombre_lower = nombre.lower()
    mejor = None
    for clave, canonica in _COMISIONES_SENADO_INDEX.items():
        if nombre_lower.startswith(clave) and (mejor is None or len(clave) > len(mejor[0])):
            mejor = (clave, canonica)
    if mejor and len(mejor[0]) >= 5:
        return mejor[1]

    # 5. Prefijo "ESPECIAL DE/PARA" (sin "Comisión"): intentar con prefijo completo
    if nombre_upper.startswith("ESPECIAL "):
        return normalizar_comision_senado("Comisión " + nombre)

    # 6. Variante "DE" prefijado: "DEDERECHOS..." o "DE DEPORTE..."
    if nombre_upper.startswith("DE ") or nombre_upper.startswith("DE"):
        sin_de = nombre[2:].strip() if nombre_upper.startswith("DE ") else nombre[2:].strip()
        return normalizar_comision_senado(sin_de)

    # 7. Variante "LA " prefijado
    if nombre_upper.startswith("LA "):
        return normalizar_comision_senado(nombre[3:].strip())

    # 8. Prefijo "LOS " → "Derechos de los Consumidores" etc
    if nombre_upper.startswith("LOS "):
        return normalizar_comision_senado(nombre[4:].strip())

    return None
