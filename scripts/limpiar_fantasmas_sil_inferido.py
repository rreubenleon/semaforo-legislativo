"""
Limpia los stubs FANTASMA de legisladores (origen='sil_inferido') que en
realidad son DUPLICADOS de un legislador del roster oficial.

Causa raíz (arreglada en completar_legisladores_desde_elo.py el 16-may):
ese script escribía camara='Cámara de Senadores' para los stubs de
senadores, pero el roster oficial usa camara='Senado'. El dedup filtraba
`WHERE camara = ?` → los senadores nunca se deduplicaban y se crearon
fantasmas:
  · Miguel Márquez Márquez   id=629  dup de  id=540 (real, 64 act)
  · Imelda Castro Castro     id=640  dup de  id=570 (real,  3 act)
    └─ este fantasma tenía el ELO 1505/1-partida → es el "100% 1/1"
       de efectividad que reportó el usuario.

Criterio de fantasma (autoritativo, usa utils.matcher):
  el stub resuelve, con cámara canónica, a un id DISTINTO que existe en
  el roster oficial (origen='sitl_oficial' o NULL).

Acción por fantasma:
  - borra filas en legisladores_elo / legisladores_h2h /
    actividad_legislador / legisladores_trayectoria / legisladores_perfil
    (si existe) del id fantasma
  - borra la fila legisladores
El dato real ya vive (o se recomputará) bajo el id oficial; el pipeline
recalcula ELO/actividad con utils.matcher robusto, que YA resuelve
estos nombres al id correcto (540/570) — verificado.

Además: normaliza camara de los stubs LEGÍTIMOS que sobreviven
('Cámara de Senadores' → 'Senado') para que perfil_sil / rebuild /
Radar empaten.

Uso:
    python scripts/limpiar_fantasmas_sil_inferido.py --dry-run
    python scripts/limpiar_fantasmas_sil_inferido.py
"""
import argparse
import logging
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.matcher import build_bd_index, encontrar_legislador_id, normalizar_nombre

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

# tablas con FK lógica legislador_id que hay que limpiar del fantasma
TABLAS_FK = [
    "legisladores_elo",
    "legisladores_h2h",
    "actividad_legislador",
    "legisladores_trayectoria",
    "legisladores_perfil",
]


def canon(camara):
    return "Senado" if "senad" in (camara or "").lower() else "Cámara de Diputados"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--d1", action="store_true",
                    help="además borra los fantasmas de D1 vía wrangler "
                         "(requiere CLOUDFLARE_API_TOKEN; el pipeline cada-4h "
                         "NO lo pasa, solo el workflow one-off)")
    args = ap.parse_args()

    conn = sqlite3.connect(str(ROOT / "semaforo.db"))
    conn.row_factory = sqlite3.Row

    real_ids = {
        r[0] for r in conn.execute(
            "SELECT id FROM legisladores "
            "WHERE origen='sitl_oficial' OR origen IS NULL"
        )
    }
    idx = build_bd_index(conn)

    stubs = conn.execute(
        "SELECT id, nombre, camara FROM legisladores "
        "WHERE origen='sil_inferido' ORDER BY id"
    ).fetchall()

    fantasmas = []   # (stub_id, nombre, real_id)
    norm_camara = []  # stubs legítimos cuyo camara hay que canonizar

    for s in stubs:
        sid, nom, cam = s["id"], s["nombre"], s["camara"]
        nn = normalizar_nombre(nom)
        c = canon(cam)
        mid = encontrar_legislador_id(nn, c, idx)
        if not mid or mid == sid:
            otra = "Cámara de Diputados" if c == "Senado" else "Senado"
            mid = encontrar_legislador_id(nn, otra, idx)
        if mid and mid != sid and mid in real_ids:
            fantasmas.append((sid, nom, mid))
        elif cam != c:
            norm_camara.append((sid, nom, cam, c))

    print(f"\n  Stubs sil_inferido: {len(stubs)}")
    print(f"  Fantasmas (dup de roster oficial): {len(fantasmas)}")
    for sid, nom, mid in fantasmas:
        rl = conn.execute(
            "SELECT nombre, camara FROM legisladores WHERE id=?", (mid,)
        ).fetchone()
        cnt = {}
        for t in TABLAS_FK:
            try:
                cnt[t] = conn.execute(
                    f"SELECT COUNT(*) FROM {t} WHERE legislador_id=?", (sid,)
                ).fetchone()[0]
            except sqlite3.OperationalError:
                cnt[t] = "—"
        print(f"   DEL id={sid} {nom!r}  →  real id={mid} "
              f"{rl['nombre']!r} [{rl['camara']}]")
        print(f"       filas a borrar: {cnt}")

    print(f"\n  Stubs legítimos a normalizar camara: {len(norm_camara)}")
    for sid, nom, viejo, nuevo in norm_camara[:5]:
        print(f"   id={sid} {nom!r}: {viejo!r} → {nuevo!r}")
    if len(norm_camara) > 5:
        print(f"   … y {len(norm_camara) - 5} más")

    if args.dry_run:
        print("\n  (dry-run: no se tocó la DB)")
        return 0

    borradas = 0
    for sid, nom, mid in fantasmas:
        for t in TABLAS_FK:
            try:
                conn.execute(f"DELETE FROM {t} WHERE legislador_id=?", (sid,))
            except sqlite3.OperationalError:
                pass
        conn.execute("DELETE FROM legisladores WHERE id=?", (sid,))
        borradas += 1
        logger.info(f"Fantasma id={sid} {nom!r} borrado (dup de {mid})")

    for sid, nom, viejo, nuevo in norm_camara:
        conn.execute(
            "UPDATE legisladores SET camara=? WHERE id=?", (nuevo, sid)
        )

    conn.commit()

    # Propagar el borrado a D1 (radar lee de ahí). Nada repuebla la tabla
    # D1 legisladores — sync_legisladores_cargo_d1.py solo hace UPDATE —
    # así que los fantasmas viven en D1 hasta que se borran explícitamente.
    if args.d1 and fantasmas:
        ids = [sid for sid, _, _ in fantasmas]
        lista = ",".join(str(i) for i in ids)
        sql_d1 = "\n".join([
            f"DELETE FROM legisladores_elo WHERE legislador_id IN ({lista});",
            f"DELETE FROM legisladores_stats WHERE legislador_id IN ({lista});",
            f"DELETE FROM legisladores_hit_rate WHERE legislador_id IN ({lista});",
            f"DELETE FROM legisladores_perfil WHERE legislador_id IN ({lista});",
            f"DELETE FROM legisladores WHERE id IN ({lista});",
        ])
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql",
                                         delete=False) as tmp:
            tmp.write(sql_d1)
            tmp_path = tmp.name
        try:
            r = subprocess.run(
                ["npx", "wrangler", "d1", "execute", "fiat-busqueda",
                 "--remote", "--file", tmp_path],
                cwd=ROOT / "worker", capture_output=True, text=True,
                timeout=120,
            )
            if r.returncode != 0:
                logger.warning(f"wrangler stderr: {r.stderr[:600]}")
            else:
                logger.info(f"✓ D1: fantasmas {ids} borrados")
        finally:
            Path(tmp_path).unlink()

    print(f"\n  ═══ Limpieza ═══")
    print(f"  Fantasmas borrados:        {borradas}")
    print(f"  Camara normalizada:        {len(norm_camara)}")
    total = conn.execute("SELECT COUNT(*) FROM legisladores").fetchone()[0]
    inf = conn.execute(
        "SELECT COUNT(*) FROM legisladores WHERE origen='sil_inferido'"
    ).fetchone()[0]
    print(f"  legisladores ahora:        {total} ({inf} sil_inferido)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
