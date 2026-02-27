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
]

# Twitter/X API v2 — Bearer Token (PPU plan)
import os
TWITTER_BEARER_TOKEN = os.environ.get(
    "TWITTER_BEARER_TOKEN",
    "AAAAAAAAAAAAAAAAAAAAAFtE7wEAAAAAFJYjhootmF922JDbuebwMdpiGz8=lD9rhkUP4z6ni4rvVwc79hZlMOIKyJb1lhV4fHsknT39ncBloc"
)

# ─────────────────────────────────────────────
# 12 CATEGORÍAS LEGISLATIVAS (basadas en comisiones)
# ─────────────────────────────────────────────
CATEGORIAS = {
    "seguridad_justicia": {
        "nombre": "Seguridad y Justicia",
        "keywords": [
            "seguridad", "justicia", "penal", "policía", "guardia nacional",
            "crimen organizado", "narcotráfico", "homicidio", "violencia", "fiscalía",
            "ministerio público", "cárcel", "prisión", "delito", "impunidad",
            "extorsión", "secuestro", "feminicidio", "desaparición forzada",
        ],
        "comisiones": [
            "Justicia", "Seguridad Pública", "Defensa Nacional",
        ],
    },
    "economia_hacienda": {
        "nombre": "Economía y Hacienda",
        "keywords": [
            "economía", "hacienda", "presupuesto", "impuestos", "SAT",
            "inflación", "PIB", "deuda", "gasto público", "política fiscal",
            "reforma fiscal", "aranceles", "comercio exterior", "inversión extranjera",
            "inversión pública", "tipo de cambio", "peso mexicano",
            "recaudación", "IEPS", "IVA", "ISR", "déficit",
        ],
        "comisiones": [
            "Hacienda y Crédito Público", "Economía, Comercio y Competitividad",
            "Presupuesto y Cuenta Pública",
        ],
    },
    "energia": {
        "nombre": "Energía",
        "keywords": [
            "energía", "petróleo", "Pemex", "CFE", "electricidad",
            "hidrocarburos", "gasolina", "gas", "renovable", "solar",
            "eólica", "litio", "refinería", "subsidio energético",
            "tarifas eléctricas", "soberanía energética",
        ],
        "comisiones": [
            "Energía",
        ],
    },
    "salud": {
        "nombre": "Salud",
        "keywords": [
            "salud", "IMSS", "ISSSTE", "hospital", "medicamento",
            "vacuna", "epidemia", "pandemia", "médico", "enfermedad",
            "INSABI", "IMSS-Bienestar", "desabasto", "farmacia",
            "salud mental", "adicciones", "fentanilo",
        ],
        "comisiones": [
            "Salud",
        ],
    },
    "educacion": {
        "nombre": "Educación",
        "keywords": [
            "educación", "SEP", "escuela", "universidad", "maestro",
            "profesor", "becas", "libros de texto", "CONACYT", "Conahcyt",
            "investigación", "ciencia", "tecnología", "UNAM", "IPN",
            "rezago educativo", "deserción escolar",
        ],
        "comisiones": [
            "Educación", "Ciencia, Tecnología e Innovación",
        ],
    },
    "trabajo": {
        "nombre": "Trabajo",
        "keywords": [
            "trabajo", "empleo", "salario", "sindicato", "outsourcing",
            "subcontratación", "pensión", "AFORE", "desempleo", "informalidad",
            "salario mínimo", "jornada laboral", "huelga", "STPS",
            "prestaciones", "aguinaldo", "vacaciones dignas",
        ],
        "comisiones": [
            "Trabajo y Previsión Social",
        ],
    },
    "electoral_politico": {
        "nombre": "Electoral y Político",
        "keywords": [
            "elección", "INE", "TEPJF", "partido político", "voto", "campaña electoral",
            "reforma electoral", "democracia", "diputado", "senador",
            "congreso", "legislatura", "gobernador", "presidencia",
            "Morena", "PAN", "PRI", "coalición", "oposición",
            "revocación de mandato", "consulta popular",
        ],
        "comisiones": [
            "Gobernación y Población", "Reforma Política-Electoral",
        ],
    },
    "derechos_humanos": {
        "nombre": "Derechos Humanos",
        "keywords": [
            "derechos humanos", "CNDH", "discriminación", "igualdad",
            "género", "migración", "migrante", "refugiado", "indígena",
            "diversidad", "LGBT", "aborto", "violencia de género",
            "trata de personas", "libertad de expresión", "periodista",
            "defensor", "amnistía",
        ],
        "comisiones": [
            "Derechos Humanos", "Igualdad de Género",
            "Asuntos Migratorios",
        ],
    },
    "infraestructura": {
        "nombre": "Infraestructura",
        "keywords": [
            "infraestructura", "carretera", "tren", "aeropuerto", "AIFA",
            "Tren Maya", "transporte", "obra pública", "construcción",
            "vivienda", "agua", "Conagua", "drenaje", "puente",
            "corredor interoceánico", "telecomunicaciones",
            "terremoto", "sismo", "inundación", "huracán", "desastre natural",
            "protección civil", "reconstrucción", "declaratoria de emergencia",
            "damnificados", "derrumbe", "evacuación",
        ],
        "comisiones": [
            "Infraestructura", "Comunicaciones y Transportes",
            "Recursos Hidráulicos, Agua Potable y Saneamiento",
        ],
    },
    "agro_rural": {
        "nombre": "Agro y Desarrollo Rural",
        "keywords": [
            "agricultura", "campo", "campesino", "agro", "ganadería",
            "pesca", "maíz", "glifosato", "transgénico", "Segalmex",
            "Sembrando Vida", "fertilizante", "sequía", "cosecha",
            "ejido", "tierra", "reforma agraria", "soberanía alimentaria",
            "productos agropecuarios", "exportación agropecuaria",
            "certificación agropecuaria",
        ],
        "comisiones": [
            "Desarrollo y Conservación Rural, Agrícola y Autosuficiencia Alimentaria",
        ],
    },
    "relaciones_exteriores": {
        "nombre": "Relaciones Exteriores",
        "keywords": [
            "relaciones exteriores", "diplomacia", "embajada", "tratado",
            "T-MEC", "Estados Unidos", "frontera", "aranceles", "ONU",
            "cancillería", "SRE", "consulado", "extradición",
            "soberanía", "intervención", "deportación",
        ],
        "comisiones": [
            "Relaciones Exteriores",
        ],
    },
    "anticorrupcion": {
        "nombre": "Anticorrupción",
        "keywords": [
            "corrupción", "transparencia", "INAI", "ASF", "auditoría",
            "conflicto de interés", "enriquecimiento ilícito", "soborno",
            "licitación", "contrato", "nepotismo", "lavado de dinero",
            "declaración patrimonial", "Sistema Nacional Anticorrupción",
            "SNA", "UIF", "extinción de dominio",
        ],
        "comisiones": [
            "Transparencia y Anticorrupción",
        ],
    },
    "medio_ambiente": {
        "nombre": "Medio Ambiente y Cambio Climático",
        "keywords": [
            "medio ambiente", "cambio climático", "contaminación ambiental",
            "deforestación", "biodiversidad", "emisiones de carbono",
            "Semarnat", "Profepa", "Conafor", "área natural protegida",
            "calentamiento global", "gases de efecto invernadero",
            "economía circular", "contingencia ambiental",
            "ley ambiental", "norma ambiental", "impacto ambiental",
            "política ambiental", "residuos peligrosos", "Acuerdo de París",
            "sequía", "incendio forestal", "ola de calor", "fenómeno natural",
            "forestal", "desarrollo forestal", "reforestación",
            "certificación ambiental", "productos forestales",
            "tala", "silvicultura",
        ],
        "comisiones": [
            "Medio Ambiente, Sustentabilidad, Cambio Climático y Recursos Naturales",
            "Cambio Climático y Sostenibilidad",
        ],
    },
    "inteligencia_artificial": {
        "nombre": "Inteligencia Artificial",
        "keywords": [
            "inteligencia artificial", "regulación de inteligencia artificial",
            "ley de inteligencia artificial", "iniciativa inteligencia artificial",
            "regulación tecnológica", "regulación algorítmica",
            "deepfake", "ética de la inteligencia artificial",
            "sesgo algorítmico", "gobernanza digital",
            "ley de ciberseguridad", "regulación de plataformas digitales",
            "protección de datos personales", "ley de datos",
        ],
        "comisiones": [
            "Ciencia, Tecnología e Innovación",
        ],
    },
}

# ─────────────────────────────────────────────
# SCORING - Fórmula del Semáforo
# ─────────────────────────────────────────────
SCORING = {
    "pesos": {
        "media": 0.25,       # Cobertura mediática (volumen + concentración + diversidad)
        "trends": 0.15,      # Google Trends (atención pública, no intención política)
        "congreso": 0.30,    # Actividad en Gaceta Parlamentaria (señal institucional)
        "mananera": 0.15,    # Mención de la Presidenta en conferencia matutina
        "urgencia": 0.15,    # Factor de urgencia condicional (amplifica si convergen señales)
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
    "ventana_dias": 30,                 # Ventana de análisis
    "max_lag_dias": 14,                 # Lag máximo a evaluar
    "granger_max_lag": 7,               # Lags para test de Granger
    "p_value_threshold": 0.05,          # Significancia estadística
    "min_observaciones": 15,            # Mínimo de datos para análisis
    "cross_correlation_lags": 14,       # Lags para cross-correlation
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
        "México", "mexicano", "gobierno", "presidente", "federal",
        "estado", "nacional", "país", "república",
    ],
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
