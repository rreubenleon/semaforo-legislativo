"""
db.py — Conexión centralizada a base de datos para Semáforo Legislativo.

Soporta tres modos (seleccionado por variable de entorno SEMAFORO_DB_MODE):
  1. "turso"   — Embedded replica: cache local + sync con Turso remoto (para CI)
  2. "remote"  — Conexión directa a Turso sin cache local
  3. "local"   — SQLite local semaforo.db (default, para desarrollo)

Variables de entorno requeridas para modo turso/remote:
  TURSO_DATABASE_URL  — ej: libsql://semaforo-legislativo-rubenleon.turso.io
  TURSO_AUTH_TOKEN    — Token de autenticación de Turso
"""

import os
import re
import sqlite3
import logging
from pathlib import Path

logger = logging.getLogger("semaforo.db")
ROOT = Path(__file__).resolve().parent

_connection = None
_mode = None


# ─────────────────────────────────────────────
# Wrapper para compatibilidad libsql ↔ sqlite3
# ─────────────────────────────────────────────

class _DictRow(dict):
    """Row que funciona como dict Y como tuple (compatible con sqlite3.Row)."""

    def __init__(self, columns, values):
        super().__init__(zip(columns, values))
        self._values = tuple(values)

    def __getitem__(self, key):
        if isinstance(key, (int, slice)):
            return self._values[key]
        return super().__getitem__(key)

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)


class _CursorWrapper:
    """Wrapper de cursor que aplica row_factory a los resultados."""

    def __init__(self, cursor, columns=None):
        self._cursor = cursor
        self._columns = columns

    @property
    def description(self):
        return getattr(self._cursor, "description", None)

    @property
    def lastrowid(self):
        return getattr(self._cursor, "lastrowid", None)

    @property
    def rowcount(self):
        return getattr(self._cursor, "rowcount", -1)

    def _wrap_row(self, row):
        if row is None:
            return None
        if self._columns:
            return _DictRow(self._columns, row)
        return row

    def fetchone(self):
        row = self._cursor.fetchone()
        return self._wrap_row(row)

    def fetchall(self):
        rows = self._cursor.fetchall()
        if self._columns:
            return [_DictRow(self._columns, r) for r in rows]
        return rows

    def fetchmany(self, size=None):
        rows = self._cursor.fetchmany(size) if size else self._cursor.fetchmany()
        if self._columns:
            return [_DictRow(self._columns, r) for r in rows]
        return rows

    def __iter__(self):
        # libsql cursors no soportan iteración directa — usar fetchall()
        try:
            rows = self._cursor.fetchall()
        except Exception:
            return
        for row in rows:
            yield self._wrap_row(row)


class _CursorProxy:
    """Proxy de cursor para conn.cursor() — emula sqlite3.Cursor."""

    def __init__(self, conn_wrapper):
        self._conn_wrapper = conn_wrapper
        self._last_cursor = None

    def execute(self, sql, parameters=None):
        self._last_cursor = self._conn_wrapper.execute(sql, parameters)
        return self._last_cursor

    def fetchone(self):
        if self._last_cursor:
            return self._last_cursor.fetchone()
        return None

    def fetchall(self):
        if self._last_cursor:
            return self._last_cursor.fetchall()
        return []

    def fetchmany(self, size=None):
        if self._last_cursor:
            return self._last_cursor.fetchmany(size)
        return []

    def __iter__(self):
        if self._last_cursor:
            return iter(self._last_cursor)
        return iter([])


class _ConnectionWrapper:
    """
    Wrapper de conexión libsql que agrega compatibilidad con sqlite3:
    - conn.row_factory = sqlite3.Row  (soportado vía _DictRow)
    - conn.cursor() retorna proxy compatible
    - Parámetros dict (:nombre) se convierten a positional (?)
    - list se convierte a tuple
    - Reconexión automática cuando el stream de Turso expira
    """

    def __init__(self, conn, connect_fn=None):
        self._conn = conn
        self._use_dict_rows = False
        # Función para reconectar si el stream expira
        self._connect_fn = connect_fn

    @property
    def row_factory(self):
        return sqlite3.Row if self._use_dict_rows else None

    @row_factory.setter
    def row_factory(self, value):
        # Cualquier row_factory no-None activa el modo dict
        self._use_dict_rows = value is not None

    def cursor(self):
        """Retorna un proxy de cursor compatible con sqlite3.Cursor."""
        return _CursorProxy(self)

    def _reconnect(self):
        """Reconecta creando una nueva conexión libsql."""
        if self._connect_fn is None:
            return False
        try:
            logger.warning("Stream expirado — reconectando a Turso...")
            self._conn = self._connect_fn()
            logger.info("Reconexión exitosa")
            return True
        except Exception as e:
            logger.error(f"Error al reconectar: {e}")
            return False

    def _execute_raw(self, sql, parameters=None):
        """Ejecuta SQL con reconexión automática si el stream expiró o la conexión se perdió."""
        for attempt in range(3):
            try:
                if parameters is not None:
                    return self._conn.execute(sql, parameters)
                else:
                    return self._conn.execute(sql)
            except (ValueError, Exception) as e:
                err_str = str(e).lower()
                # Reconectar en errores de stream expirado o conexión perdida
                recoverable = (
                    "stream not found" in err_str
                    or "connection reset" in err_str
                    or "connection refused" in err_str
                    or "broken pipe" in err_str
                    or "http error" in err_str
                )
                if recoverable and attempt < 2:
                    import time as _time
                    _time.sleep(2 * (attempt + 1))  # Backoff: 2s, 4s
                    if self._reconnect():
                        continue
                raise
        # Fallback (no debería llegar aquí)
        if parameters is not None:
            return self._conn.execute(sql, parameters)
        return self._conn.execute(sql)

    @staticmethod
    def _convert_named_params(sql, params_dict):
        """Convierte SQL con :nombre + dict a SQL con ? + tuple.
        libsql no soporta parámetros nombrados vía dict."""
        names = re.findall(r':(\w+)', sql)
        sql_positional = re.sub(r':(\w+)', '?', sql)
        values = tuple(params_dict[name] for name in names)
        return sql_positional, values

    def execute(self, sql, parameters=None):
        if parameters is not None:
            if isinstance(parameters, dict):
                # libsql no soporta named params — convertir a positional
                sql, parameters = self._convert_named_params(sql, parameters)
            elif isinstance(parameters, list):
                # libsql requiere tuple, no list
                parameters = tuple(parameters)

        cursor = self._execute_raw(sql, parameters)

        # Extraer nombres de columnas del cursor
        columns = None
        if self._use_dict_rows and hasattr(cursor, "description") and cursor.description:
            columns = [desc[0] for desc in cursor.description]

        return _CursorWrapper(cursor, columns)

    def executemany(self, sql, parameters):
        # libsql requiere tuples dentro de la secuencia
        params_list = list(parameters)
        if params_list and isinstance(params_list[0], dict):
            # Convertir named params a positional
            converted = []
            sql_positional = None
            for p in params_list:
                s, vals = self._convert_named_params(sql, p)
                sql_positional = s
                converted.append(vals)
            sql = sql_positional
            params = converted
        else:
            params = [tuple(p) if isinstance(p, list) else p for p in params_list]
        for attempt in range(3):
            try:
                return self._conn.executemany(sql, params)
            except (ValueError, Exception) as e:
                err_str = str(e).lower()
                recoverable = (
                    "stream not found" in err_str
                    or "connection reset" in err_str
                    or "connection refused" in err_str
                    or "broken pipe" in err_str
                    or "http error" in err_str
                )
                if recoverable and attempt < 2:
                    import time as _time
                    _time.sleep(2 * (attempt + 1))
                    if self._reconnect():
                        continue
                raise

    def commit(self):
        return self._conn.commit()

    def close(self):
        return self._conn.close()

    def sync(self):
        """Sync embedded replica con remoto."""
        if hasattr(self._conn, "sync"):
            return self._conn.sync()


# ─────────────────────────────────────────────
# API pública
# ─────────────────────────────────────────────

def _create_turso_connection():
    """Crea una nueva conexión libsql a Turso. Usada internamente para reconexión."""
    import libsql_experimental as libsql

    url = os.environ.get("TURSO_DATABASE_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")

    if _mode == "turso":
        local_replica = str(ROOT / "local_replica.db")

        try:
            raw_conn = libsql.connect(
                local_replica,
                sync_url=url,
                auth_token=token,
            )
            raw_conn.sync()
        except Exception as e:
            # Si la réplica local está corrupta (ej: wal_insert_frame failed),
            # borrarla y empezar de cero
            logger.warning(f"Sync falló con réplica existente ({e}), recreando...")
            import glob as globmod
            for f in globmod.glob(local_replica + "*"):
                try:
                    os.remove(f)
                    logger.info(f"  Eliminado: {f}")
                except OSError:
                    pass
            raw_conn = libsql.connect(
                local_replica,
                sync_url=url,
                auth_token=token,
            )
            raw_conn.sync()
    else:
        raw_conn = libsql.connect(url, auth_token=token)

    return raw_conn


def get_connection():
    """
    Retorna una conexión a la base de datos. Reutiliza la misma conexión
    en todas las llamadas dentro de un proceso (una corrida del pipeline).

    La conexión retornada es compatible con sqlite3:
    - conn.execute(sql, params)
    - conn.executemany(sql, params_list)
    - conn.commit()
    - conn.row_factory = sqlite3.Row
    - cursor.fetchall(), cursor.fetchone()
    """
    global _connection, _mode

    if _connection is not None:
        return _connection

    _mode = os.environ.get("SEMAFORO_DB_MODE", "local")

    if _mode in ("turso", "remote"):
        try:
            import libsql_experimental as libsql  # noqa: F401
        except ImportError:
            logger.error(
                "libsql_experimental no está instalado. "
                "Ejecuta: pip install libsql-experimental"
            )
            raise

        url = os.environ.get("TURSO_DATABASE_URL")
        token = os.environ.get("TURSO_AUTH_TOKEN")

        if not url or not token:
            raise EnvironmentError(
                "TURSO_DATABASE_URL y TURSO_AUTH_TOKEN son requeridos "
                f"para SEMAFORO_DB_MODE={_mode}"
            )

        raw_conn = _create_turso_connection()
        logger.info(f"Conectando a Turso ({_mode})")
        logger.info("Sync inicial completado")

        # Envolver en wrapper con función de reconexión
        _connection = _ConnectionWrapper(raw_conn, connect_fn=_create_turso_connection)
    else:
        # SQLite local (modo desarrollo)
        from config import DATABASE
        db_path = str(ROOT / DATABASE["archivo"])
        logger.info(f"Conectando a SQLite local: {db_path}")
        _connection = sqlite3.connect(db_path)

    return _connection


def sync():
    """
    Sincroniza la replica embebida con el remoto.
    Llamar después de lotes de escritura para subir cambios a Turso.
    No-op si no estamos en modo embedded replica.
    """
    if _connection is not None and hasattr(_connection, "sync"):
        try:
            _connection.sync()
            logger.debug("Sync completado")
        except Exception as e:
            logger.warning(f"Error en sync (no fatal): {e}")


def close():
    """
    Sync final y cerrar la conexión.
    Llamar al final del pipeline.
    """
    global _connection, _mode
    if _connection is not None:
        # Sync final antes de cerrar (si es embedded replica)
        if hasattr(_connection, "sync"):
            try:
                _connection.sync()
                logger.info("Sync final completado")
            except Exception as e:
                logger.warning(f"Error en sync final: {e}")
        _connection.close()
        _connection = None
        _mode = None
        logger.info("Conexión cerrada")
