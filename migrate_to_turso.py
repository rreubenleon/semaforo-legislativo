#!/usr/bin/env python3
"""
migrate_to_turso.py — Migración one-time de semaforo.db local a Turso.

Uso:
  export TURSO_DATABASE_URL=libsql://semaforo-legislativo-<user>.turso.io
  export TURSO_AUTH_TOKEN=<token>
  python migrate_to_turso.py

Lee todas las tablas de semaforo.db local, las crea en Turso,
y copia todos los datos en batches.
"""

import os
import sys
import sqlite3
import time


def migrate():
    # ── Verificar variables de entorno ──
    url = os.environ.get("TURSO_DATABASE_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")

    if not url or not token:
        print("❌ Error: TURSO_DATABASE_URL y TURSO_AUTH_TOKEN son requeridos")
        print("   export TURSO_DATABASE_URL=libsql://tu-db.turso.io")
        print("   export TURSO_AUTH_TOKEN=tu-token")
        sys.exit(1)

    # ── Verificar que existe semaforo.db ──
    if not os.path.exists("semaforo.db"):
        print("❌ Error: semaforo.db no encontrado en el directorio actual")
        sys.exit(1)

    # ── Conectar a SQLite local ──
    print("📂 Conectando a semaforo.db local...")
    local = sqlite3.connect("semaforo.db")

    # ── Conectar a Turso ──
    print(f"☁️  Conectando a Turso: {url}")
    try:
        import libsql_experimental as libsql
    except ImportError:
        print("❌ Error: libsql_experimental no instalado")
        print("   pip install libsql-experimental")
        sys.exit(1)

    remote = libsql.connect(url, auth_token=token)

    # ── Obtener lista de tablas ──
    tables = [
        row[0]
        for row in local.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
    ]
    print(f"\n📋 Tablas encontradas: {len(tables)}")
    for t in tables:
        count = local.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
        print(f"   {t}: {count:,} rows")

    # ── Desactivar foreign keys para poder migrar en cualquier orden ──
    remote.execute("PRAGMA foreign_keys = OFF")
    remote.commit()
    print("\n🔧 Foreign keys desactivadas para migración")

    # ── Migrar cada tabla ──
    total_rows = 0
    t0 = time.time()

    for table in tables:
        print(f"\n{'='*60}")
        print(f"🔄 Migrando: {table}")

        # 1. Obtener schema (CREATE TABLE statement)
        schema_row = local.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()

        if not schema_row or not schema_row[0]:
            print(f"   ⚠️  Sin schema para {table}, saltando")
            continue

        schema_sql = schema_row[0]

        # 2. Crear tabla en Turso
        try:
            remote.execute(schema_sql)
            remote.commit()
            print(f"   ✓ Tabla creada")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"   ℹ️  Tabla ya existe, continuando")
            else:
                print(f"   ❌ Error creando tabla: {e}")
                continue

        # 3. Obtener columnas
        pragma = local.execute(f"PRAGMA table_info([{table}])").fetchall()
        col_names = [row[1] for row in pragma]
        col_count = len(col_names)
        cols_str = ", ".join(f"[{c}]" for c in col_names)
        placeholders = ", ".join(["?"] * col_count)

        # 4. Leer todos los datos
        rows = local.execute(f"SELECT {cols_str} FROM [{table}]").fetchall()

        if not rows:
            print(f"   ℹ️  Tabla vacía, nada que migrar")
            continue

        # 5. Insertar en batches (con retry para manejar stream timeouts)
        batch_size = 50
        inserted = 0
        skipped = 0

        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            for row in batch:
                try:
                    remote.execute(
                        f"INSERT OR IGNORE INTO [{table}] ({cols_str}) "
                        f"VALUES ({placeholders})",
                        tuple(row),
                    )
                    inserted += 1
                except Exception as e:
                    skipped += 1
                    if skipped <= 3:
                        print(f"   ⚠️  Skip row: {str(e)[:80]}")

            try:
                remote.commit()
            except Exception as e:
                # Stream timeout — reconectar y reintentar commit
                print(f"\n   ⚠️  Commit error, reconnecting: {str(e)[:60]}")
                remote = libsql.connect(url, auth_token=token)
                remote.execute("PRAGMA foreign_keys = OFF")
                remote.commit()
                time.sleep(1)

            # Progreso
            done = min(i + batch_size, len(rows))
            pct = done / len(rows) * 100
            print(f"   [{pct:5.1f}%] {done:,}/{len(rows):,} rows", end="\r")

        print(f"   ✓ {inserted:,} insertados, {skipped:,} saltados" + " " * 20)
        total_rows += inserted

        # 6. Copiar índices
        indexes = local.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' "
            "AND tbl_name=? AND sql IS NOT NULL",
            (table,),
        ).fetchall()

        for idx in indexes:
            try:
                remote.execute(idx[0])
                remote.commit()
            except Exception:
                pass  # Índice ya existe

        if indexes:
            print(f"   ✓ {len(indexes)} índices creados")

    # ── Resumen ──
    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"✅ Migración completada en {elapsed:.1f}s")
    print(f"   {len(tables)} tablas, {total_rows:,} rows totales")

    # ── Verificar conteos ──
    print(f"\n📊 Verificación de conteos:")
    all_match = True
    for table in tables:
        local_count = local.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
        try:
            remote_count = remote.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
        except Exception:
            remote_count = "ERROR"
        match = "✓" if local_count == remote_count else "✗"
        if local_count != remote_count:
            all_match = False
        print(f"   {match} {table}: local={local_count:,}  turso={remote_count}")

    if all_match:
        print("\n🎉 Todos los conteos coinciden. Migración exitosa!")
    else:
        print("\n⚠️  Algunos conteos no coinciden. Revisar manualmente.")

    local.close()
    remote.close()


if __name__ == "__main__":
    migrate()
