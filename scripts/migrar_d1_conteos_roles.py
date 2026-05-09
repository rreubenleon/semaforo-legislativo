"""
Migración idempotente: agrega columnas n_ini_iniciante / n_ini_adherente
/ etc a la tabla `legisladores` en D1.

Tolerante a "duplicate column name" (si ya existen las columnas).
Solo se corre una vez por columna; runs subsecuentes son no-ops.
"""
import subprocess
import sys

COLS = [
    ("n_ini_iniciante", "INTEGER DEFAULT 0"),
    ("n_ini_adherente", "INTEGER DEFAULT 0"),
    ("n_ini_de_grupo", "INTEGER DEFAULT 0"),
    ("n_prop_proponente", "INTEGER DEFAULT 0"),
    ("n_prop_adherente", "INTEGER DEFAULT 0"),
    ("n_prop_de_grupo", "INTEGER DEFAULT 0"),
    ("n_total_vinculadas", "INTEGER DEFAULT 0"),
]


def main():
    creadas = 0
    ya_existen = 0
    for col, ddl in COLS:
        sql = f"ALTER TABLE legisladores ADD COLUMN {col} {ddl};"
        r = subprocess.run(
            ["npx", "wrangler", "d1", "execute", "fiat-busqueda", "--remote",
             "--command", sql],
            capture_output=True, text=True, timeout=60,
        )
        out = (r.stdout + r.stderr).lower()
        if "duplicate column" in out or "already exists" in out:
            ya_existen += 1
            print(f"  · {col}: ya existe (skip)")
        elif r.returncode == 0:
            creadas += 1
            print(f"  ✅ {col}: creada")
        else:
            print(f"  ❌ {col}: ERROR — {r.stderr[:200]}")
    print()
    print(f"Resultado: {creadas} creadas, {ya_existen} ya existían")


if __name__ == "__main__":
    main()
