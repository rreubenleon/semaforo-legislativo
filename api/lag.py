"""
Análisis de Correlación Temporal - Semáforo Legislativo
Hipótesis: evento mediático → presión sostenida → iniciativa en N días

Implementa:
- Test de Granger: ¿la cobertura mediática predice actividad legislativa?
- Cross-correlation: ¿cuál es el lag óptimo entre medios y congreso?
- Detección de picos mediáticos y su correlación con legislación
"""

import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from scipy import stats as scipy_stats
from scipy.signal import correlate

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CATEGORIAS, LAG_CONFIG, DATABASE
from scrapers.medios import contar_menciones_por_fecha
from scrapers.gaceta import contar_actividad_por_fecha
from scrapers.trends import obtener_serie_temporal

logger = logging.getLogger(__name__)


def init_db():
    """Crea tabla de correlaciones."""
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS correlaciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            categoria TEXT NOT NULL,
            tipo_analisis TEXT NOT NULL,
            lag_optimo INTEGER,
            coeficiente REAL,
            p_value REAL,
            significativo INTEGER,
            detalle TEXT,
            fecha_analisis TEXT NOT NULL,
            UNIQUE(categoria, tipo_analisis, fecha_analisis)
        )
    """)
    conn.commit()
    return conn


def alinear_series(serie_a, serie_b, dias=None):
    """
    Alinea dos series temporales (dict {fecha: valor}) por fechas comunes.
    Rellena días faltantes con 0.
    Retorna dos arrays numpy alineados.
    """
    if dias is None:
        dias = LAG_CONFIG["ventana_dias"]

    # Generar todas las fechas en el rango
    hoy = datetime.now()
    todas_fechas = [
        (hoy - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(dias - 1, -1, -1)
    ]

    arr_a = np.array([float(serie_a.get(f, 0)) for f in todas_fechas])
    arr_b = np.array([float(serie_b.get(f, 0)) for f in todas_fechas])

    return arr_a, arr_b, todas_fechas


def granger_test(x, y, max_lag=None):
    """
    Test de causalidad de Granger simplificado.
    Prueba si la serie X (medios) causa-Granger a Y (congreso).

    Implementación: compara dos regresiones OLS
    - Modelo restringido: Y_t = a + b1*Y_{t-1} + ... + bn*Y_{t-n}
    - Modelo no restringido: Y_t = a + b1*Y_{t-1} + ... + bn*Y_{t-n} + c1*X_{t-1} + ... + cn*X_{t-n}
    - F-test para significancia

    Retorna dict con resultados por lag.
    """
    if max_lag is None:
        max_lag = LAG_CONFIG["granger_max_lag"]

    n = len(y)
    if n < max_lag + LAG_CONFIG["min_observaciones"]:
        return {"error": "Insuficientes observaciones", "resultados": []}

    resultados = []

    for lag in range(1, max_lag + 1):
        # Construir matrices de regresión
        Y = y[lag:]  # Variable dependiente
        n_obs = len(Y)

        # Modelo restringido: solo lags de Y
        X_restringido = np.column_stack([
            y[lag - i - 1: n - i - 1] for i in range(lag)
        ])
        X_restringido = np.column_stack([np.ones(n_obs), X_restringido])

        # Modelo no restringido: lags de Y + lags de X
        X_no_restringido = np.column_stack([
            X_restringido,
            *[x[lag - i - 1: n - i - 1].reshape(-1, 1) for i in range(lag)]
        ])

        try:
            # OLS para modelo restringido
            beta_r, rss_r, _, _ = np.linalg.lstsq(X_restringido, Y, rcond=None)
            residuos_r = Y - X_restringido @ beta_r
            ssr_r = np.sum(residuos_r ** 2)

            # OLS para modelo no restringido
            beta_nr, rss_nr, _, _ = np.linalg.lstsq(X_no_restringido, Y, rcond=None)
            residuos_nr = Y - X_no_restringido @ beta_nr
            ssr_nr = np.sum(residuos_nr ** 2)

            # F-statistic
            df1 = lag  # restricciones adicionales
            df2 = n_obs - X_no_restringido.shape[1]

            if ssr_nr == 0 or df2 <= 0:
                continue

            f_stat = ((ssr_r - ssr_nr) / df1) / (ssr_nr / df2)
            p_value = 1 - scipy_stats.f.cdf(f_stat, df1, df2)

            resultados.append({
                "lag": lag,
                "f_statistic": round(f_stat, 4),
                "p_value": round(p_value, 6),
                "significativo": p_value < LAG_CONFIG["p_value_threshold"],
            })

        except (np.linalg.LinAlgError, ValueError) as e:
            logger.warning(f"Error en Granger lag={lag}: {e}")
            continue

    return {
        "max_lag_probado": max_lag,
        "n_observaciones": n,
        "resultados": resultados,
    }


def cross_correlation(x, y, max_lags=None):
    """
    Calcula la cross-correlation entre dos series.
    Lag positivo = X precede a Y (medios antes que congreso).

    Retorna:
    - correlaciones por lag
    - lag óptimo (donde correlación es máxima)
    - significancia estadística
    """
    if max_lags is None:
        max_lags = LAG_CONFIG["cross_correlation_lags"]

    n = len(x)
    if n < LAG_CONFIG["min_observaciones"]:
        return {"error": "Insuficientes observaciones"}

    # Normalizar series (z-score)
    x_norm = (x - np.mean(x)) / (np.std(x) + 1e-10)
    y_norm = (y - np.mean(y)) / (np.std(y) + 1e-10)

    correlaciones = []

    for lag in range(-max_lags, max_lags + 1):
        if lag > 0:
            # X precede a Y
            corr = np.corrcoef(x_norm[:n - lag], y_norm[lag:])[0, 1]
        elif lag < 0:
            # Y precede a X
            corr = np.corrcoef(x_norm[-lag:], y_norm[:n + lag])[0, 1]
        else:
            corr = np.corrcoef(x_norm, y_norm)[0, 1]

        # Test de significancia (bilateral)
        n_eff = n - abs(lag)
        if n_eff > 2:
            t_stat = corr * np.sqrt(n_eff - 2) / np.sqrt(1 - corr ** 2 + 1e-10)
            p_value = 2 * (1 - scipy_stats.t.cdf(abs(t_stat), n_eff - 2))
        else:
            p_value = 1.0

        correlaciones.append({
            "lag": lag,
            "correlacion": round(float(corr) if not np.isnan(corr) else 0, 4),
            "p_value": round(float(p_value), 6),
            "significativo": p_value < LAG_CONFIG["p_value_threshold"],
        })

    # Encontrar lag óptimo (solo lags positivos = medios antes que congreso)
    lags_positivos = [c for c in correlaciones if c["lag"] > 0]
    if lags_positivos:
        mejor = max(lags_positivos, key=lambda c: abs(c["correlacion"]))
    else:
        mejor = {"lag": 0, "correlacion": 0, "p_value": 1.0}

    return {
        "correlaciones": correlaciones,
        "lag_optimo": mejor["lag"],
        "correlacion_maxima": mejor["correlacion"],
        "p_value_optimo": mejor["p_value"],
        "interpretacion": interpretar_lag(mejor["lag"], mejor["correlacion"]),
    }


def interpretar_lag(lag, correlacion):
    """Genera interpretación legible del resultado."""
    if abs(correlacion) < 0.2:
        return f"Correlación débil (r={correlacion:.2f}). Sin relación temporal clara."

    direccion = "positiva" if correlacion > 0 else "negativa"

    if lag > 0:
        return (
            f"Correlación {direccion} (r={correlacion:.2f}) con lag de {lag} días. "
            f"La cobertura mediática PRECEDE a la actividad legislativa por ~{lag} días."
        )
    elif lag < 0:
        return (
            f"Correlación {direccion} (r={correlacion:.2f}) con lag de {abs(lag)} días. "
            f"La actividad legislativa PRECEDE a la cobertura mediática."
        )
    else:
        return f"Correlación {direccion} simultánea (r={correlacion:.2f}, lag=0)."


def detectar_picos(serie, umbral_zscore=2.0):
    """
    Detecta picos inusuales en una serie temporal.
    Un pico = valor que supera el umbral de z-score.
    """
    if len(serie) < 5:
        return []

    media = np.mean(serie)
    std = np.std(serie)

    if std == 0:
        return []

    picos = []
    for i, valor in enumerate(serie):
        z = (valor - media) / std
        if z >= umbral_zscore:
            picos.append({
                "indice": i,
                "valor": float(valor),
                "z_score": round(float(z), 2),
            })

    return picos


def analizar_categoria(categoria_clave, dias=None):
    """
    Análisis completo de correlación temporal para una categoría.
    Combina Granger + cross-correlation + detección de picos.
    """
    if dias is None:
        dias = LAG_CONFIG["ventana_dias"]

    cat_config = CATEGORIAS[categoria_clave]
    logger.info(f"Analizando correlación temporal: {cat_config['nombre']}")

    # Obtener series temporales
    # Serie 1: Menciones en medios (usar keyword principal)
    keyword_principal = cat_config["keywords"][0]
    menciones_medios = contar_menciones_por_fecha(keyword_principal, dias)

    # Serie 2: Actividad en Gaceta (general, no filtrada por keyword)
    actividad_congreso = contar_actividad_por_fecha(dias)

    # Serie 3: Google Trends
    trends = obtener_serie_temporal(categoria_clave, dias)

    # Alinear series
    medios_arr, congreso_arr, fechas = alinear_series(menciones_medios, actividad_congreso, dias)
    trends_arr, _, _ = alinear_series(trends, actividad_congreso, dias)

    resultado = {
        "categoria": categoria_clave,
        "nombre": cat_config["nombre"],
        "fecha_analisis": datetime.now().isoformat(),
        "n_dias": dias,
    }

    # 1. Granger: ¿Medios causan Congreso?
    if np.sum(medios_arr) > 0 and np.sum(congreso_arr) > 0:
        resultado["granger_medios_congreso"] = granger_test(medios_arr, congreso_arr)
    else:
        resultado["granger_medios_congreso"] = {"error": "Series vacías"}

    # 2. Cross-correlation: Medios vs Congreso
    if np.sum(medios_arr) > 0 and np.sum(congreso_arr) > 0:
        resultado["xcorr_medios_congreso"] = cross_correlation(medios_arr, congreso_arr)
    else:
        resultado["xcorr_medios_congreso"] = {"error": "Series vacías"}

    # 3. Cross-correlation: Trends vs Congreso
    if np.sum(trends_arr) > 0 and np.sum(congreso_arr) > 0:
        resultado["xcorr_trends_congreso"] = cross_correlation(trends_arr, congreso_arr)
    else:
        resultado["xcorr_trends_congreso"] = {"error": "Series vacías"}

    # 4. Detección de picos en medios
    resultado["picos_medios"] = detectar_picos(medios_arr)
    resultado["picos_congreso"] = detectar_picos(congreso_arr)

    return resultado


def analizar_todas_categorias():
    """Ejecuta análisis temporal para todas las categorías."""
    conn = init_db()
    resultados = []
    fecha_hoy = datetime.now().strftime("%Y-%m-%d")

    for cat_clave in CATEGORIAS:
        resultado = analizar_categoria(cat_clave)
        resultados.append(resultado)

        # Guardar resultado principal en BD
        xcorr = resultado.get("xcorr_medios_congreso", {})
        if "error" not in xcorr:
            try:
                conn.execute("""
                    INSERT INTO correlaciones
                        (categoria, tipo_analisis, lag_optimo, coeficiente,
                         p_value, significativo, detalle, fecha_analisis)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    cat_clave,
                    "xcorr_medios_congreso",
                    xcorr.get("lag_optimo", 0),
                    xcorr.get("correlacion_maxima", 0),
                    xcorr.get("p_value_optimo", 1),
                    1 if xcorr.get("p_value_optimo", 1) < LAG_CONFIG["p_value_threshold"] else 0,
                    xcorr.get("interpretacion", ""),
                    fecha_hoy,
                ))
            except sqlite3.IntegrityError:
                pass

        # Guardar Granger
        granger = resultado.get("granger_medios_congreso", {})
        if "error" not in granger and granger.get("resultados"):
            mejor_granger = min(
                granger["resultados"],
                key=lambda x: x["p_value"],
            )
            try:
                conn.execute("""
                    INSERT INTO correlaciones
                        (categoria, tipo_analisis, lag_optimo, coeficiente,
                         p_value, significativo, detalle, fecha_analisis)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    cat_clave,
                    "granger_medios_congreso",
                    mejor_granger["lag"],
                    mejor_granger["f_statistic"],
                    mejor_granger["p_value"],
                    1 if mejor_granger["significativo"] else 0,
                    f"F={mejor_granger['f_statistic']:.4f}",
                    fecha_hoy,
                ))
            except sqlite3.IntegrityError:
                pass

    conn.commit()
    conn.close()
    return resultados


def obtener_prediccion(categoria_clave):
    """
    Genera una predicción de actividad legislativa basada en el análisis temporal.
    Combina: lag histórico + picos actuales + tendencia.
    """
    db_path = Path(__file__).resolve().parent.parent / DATABASE["archivo"]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Obtener última correlación conocida
    row = conn.execute("""
        SELECT * FROM correlaciones
        WHERE categoria = ? AND tipo_analisis = 'xcorr_medios_congreso'
        ORDER BY fecha_analisis DESC LIMIT 1
    """, (categoria_clave,)).fetchone()

    conn.close()

    if not row:
        return {
            "categoria": categoria_clave,
            "prediccion": "Sin datos suficientes para predicción",
            "confianza": 0,
            "dias_estimados": None,
        }

    lag = row["lag_optimo"]
    coef = row["coeficiente"]
    sig = row["significativo"]

    confianza = min(abs(coef) * 100, 100) if sig else abs(coef) * 50

    return {
        "categoria": categoria_clave,
        "nombre": CATEGORIAS[categoria_clave]["nombre"],
        "lag_historico_dias": lag,
        "correlacion": coef,
        "significativo": bool(sig),
        "confianza": round(confianza, 1),
        "dias_estimados": lag if sig else None,
        "prediccion": (
            f"Basado en patrones históricos, la actividad legislativa "
            f"ocurre ~{lag} días después de picos mediáticos "
            f"(r={coef:.2f}, {'significativo' if sig else 'no significativo'})."
        ),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=== Análisis de Correlación Temporal ===\n")

    # Demo con datos sintéticos
    np.random.seed(42)
    n = 60

    # Simular: medios preceden a congreso por ~5 días
    ruido = np.random.normal(0, 0.3, n)
    medios = np.maximum(np.random.poisson(3, n).astype(float) + ruido, 0)
    congreso = np.zeros(n)
    for i in range(5, n):
        congreso[i] = 0.6 * medios[i - 5] + 0.3 * medios[i - 4] + np.random.normal(0, 0.5)
    congreso = np.maximum(congreso, 0)

    print("Granger test (medios → congreso):")
    granger = granger_test(medios, congreso, max_lag=7)
    for r in granger["resultados"]:
        sig = " ***" if r["significativo"] else ""
        print(f"  Lag {r['lag']}: F={r['f_statistic']:.4f}, p={r['p_value']:.6f}{sig}")

    print("\nCross-correlation:")
    xcorr = cross_correlation(medios, congreso)
    print(f"  Lag óptimo: {xcorr['lag_optimo']} días")
    print(f"  Correlación: {xcorr['correlacion_maxima']:.4f}")
    print(f"  {xcorr['interpretacion']}")

    print("\nPicos en medios:")
    picos = detectar_picos(medios)
    for p in picos:
        print(f"  Día {p['indice']}: valor={p['valor']:.1f}, z={p['z_score']}")
