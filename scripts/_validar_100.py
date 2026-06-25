"""Batería de validación pre-lanzamiento de FIAT contra el sitio EN VIVO.
100 simulaciones aleatorias (semilla fija) + salud de endpoints + integridad
global. Replica las fórmulas reales del frontend (Wilson efectividad) para
cotejar. NO afirma nada que no verifique. Exit 0 solo si todo pasa."""
import json, math, random, sys, unicodedata, urllib.request, urllib.parse

_INVIS = ''.join(chr(c) for c in (0x00AD, 0x200B, 0x200C, 0x200D, 0xFEFF))
def na(s):  # normaliza: minúsculas, sin acentos, sin caracteres invisibles
    s = (s or '').translate({ord(c): None for c in _INVIS})
    return ''.join(c for c in unicodedata.normalize('NFD', s.lower())
                   if unicodedata.category(c) not in ('Mn', 'Cf')).strip()

CARGOS_OK = {'presidente', 'presidenta', 'secretario', 'secretaria',
             'integrante', 'vocal'}
# Substrings que delatan CV / formación / trayectoria (no son comisiones).
# OJO: nada de 'municipal'/'senador'/'diputad' sueltos — chocan con comisiones
# reales ("Desarrollo Municipal") o ya los caza el cargo roto (':a', ':o').
JUNK_SUB = ('maestria', 'licenciatura', 'doctorado', 'bachillerato', 'preparatoria',
            'posgrado', 'diplomado', 'especialidad en', 'no proporcion', 'legislatura',
            'presidencia de la republica', 'gobierno del', 'oficialia mayor',
            'oficial mayor', 'director', 'coordinador', 'subsecretari', 'jefe de',
            'jefa de', 'secretario tecnico', 'secretario nacional', 'secretaria del',
            'titular de', 'delegad', 'regidor', 'presidente municipal',
            'presidencia municipal', 'ayuntamiento', 'gobernador', 'municipio de',
            'comite directivo', 'comision nacional', 'comisario', 'centro de',
            'instituto', 'contralor', 'consultor', 'a las que pertenece',
            'iniciativa', 'proposicion')
JUNK_EXACT = {'derecho', 'de derecho', 'medicina', 'ingenieria', 'arquitectura',
              'contaduria', 'psicologia', 'enfermeria', 'no proporciono',
              'independiente', 'presidenta', 'consejera'}

def comisiones_problemas(cc):
    """Devuelve lista de partes problemáticas de un comisiones_cargo."""
    probs = []
    for parte in (cc or '').split('|'):
        parte = parte.strip()
        if not parte:
            continue
        if ':' not in parte:
            probs.append(f"sin cargo: {parte[:40]}"); continue
        nom, cargo = parte.rsplit(':', 1)
        cn, nn = na(cargo), na(nom)
        if cn not in CARGOS_OK:
            probs.append(f"cargo inválido: {parte[:45]}")
        elif not any(c.isalpha() for c in nn):
            probs.append(f"no-comisión (año/núm): {parte[:45]}")
        elif any(j in nn for j in JUNK_SUB) or nn in JUNK_EXACT:
            probs.append(f"no-comisión (CV): {parte[:45]}")
    return probs

BASE = "https://fiat-busqueda.rreubenleon.workers.dev"
random.seed(1729)
fail = []   # errores duros
warn = []   # avisos no bloqueantes

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
def get(path):
    req = urllib.request.Request(BASE + path, headers={"User-Agent": UA,
                                                       "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=40) as r:
        return r.status, json.loads(r.read().decode())

def jsround(x):  # Math.round de JS (half-up), no banker's
    return math.floor(x + 0.5)

def wilson(aprob, desech):
    res = (aprob or 0) + (desech or 0)
    if res < 1: return None
    p = (aprob or 0)/res; n = res; z = 1.96
    denom = 1 + z*z/n
    center = p + z*z/(2*n)
    margin = z*math.sqrt(p*(1-p)/n + z*z/(4*n*n))
    return jsround(max(0, (center-margin)/denom)*100)

# ── 1. Salud de endpoints ────────────────────────────────────────────
print("== 1. ENDPOINTS ==")
ENDPOINTS = [
    ("/radar?limite=5", lambda d: len(d.get("legisladores", [])) > 0),
    ("/comisiones?camara=Senado", lambda d: len(d.get("comisiones", [])) > 0),
    ("/comisiones?camara=" + urllib.parse.quote("Cámara de Diputados"),
        lambda d: len(d.get("comisiones", [])) > 0),
    ("/buscar?q=" + urllib.parse.quote("seguridad"), lambda d: isinstance(d, dict)),
    ("/historicos?legislador_id=633", lambda d: isinstance(d, (dict, list))),
]
for path, ok in ENDPOINTS:
    try:
        st, d = get(path)
        good = st == 200 and ok(d)
        print(f"  {'OK ' if good else 'FAIL'} {path[:55]:55} {st}")
        if not good: fail.append(f"endpoint {path} -> {st}")
    except Exception as e:
        print(f"  FAIL {path[:55]:55} {e}")
        fail.append(f"endpoint {path}: {e}")

# ── 2. Universo completo ─────────────────────────────────────────────
print("\n== 2. UNIVERSO ==")
st, d = get("/radar?limite=2000")
legs = d.get("legisladores", [])
byid = {x["id"]: x for x in legs}
print(f"  total legisladores: {len(legs)}")
if len(legs) < 600:
    fail.append(f"universo chico: {len(legs)} (esperado ~681)")

CAMARAS = {"Senado", "Cámara de Diputados"}
GRADES = {"A", "B", "C", "D", "F", "", None}

def valida_leg(r, ctx):
    e = []
    if not isinstance(r.get("id"), int): e.append("id no int")
    if not (r.get("nombre") or "").strip(): e.append("nombre vacío")
    if r.get("camara") not in CAMARAS: e.append(f"camara rara: {r.get('camara')!r}")
    # efectividad Wilson coherente
    ef = wilson(r.get("elo_aprobados"), r.get("elo_desechados"))
    if ef is not None and not (0 <= ef <= 100): e.append(f"efectividad fuera de rango: {ef}")
    # reactividad
    rc = r.get("reactividad")
    if rc is not None and not (0 <= rc <= 100): e.append(f"reactividad fuera de rango: {rc}")
    # matchup
    if r.get("matchup_grade") not in GRADES: e.append(f"matchup raro: {r.get('matchup_grade')!r}")
    # comisiones_cargo: formato + sin basura de CV
    for p in comisiones_problemas(r.get("comisiones_cargo")):
        e.append(p)
    # valores basura
    for k, v in r.items():
        if isinstance(v, str) and v.strip().lower() in ("nan", "undefined", "[object object]"):
            e.append(f"valor basura {k}={v!r}")
    return e

# ── 3. 100 simulaciones aleatorias ───────────────────────────────────
print("\n== 3. 100 SIMULACIONES ALEATORIAS ==")
muestra = random.sample(legs, min(100, len(legs)))
n_sim_ok = 0
for i, r in enumerate(muestra):
    errs = valida_leg(r, i)
    if errs:
        fail.append(f"sim leg {r.get('id')} ({r.get('nombre','?')[:25]}): {'; '.join(errs)}")
    else:
        n_sim_ok += 1
print(f"  simulaciones OK: {n_sim_ok}/100")

# ── 4. Integridad sobre TODO el padrón ───────────────────────────────
print("\n== 4. INTEGRIDAD GLOBAL (todos) ==")
err_all = 0
for r in legs:
    if valida_leg(r, -1): err_all += 1
print(f"  legisladores con algún error: {err_all}/{len(legs)}")
if err_all: fail.append(f"{err_all} legisladores con errores de integridad")

# Desglose específico: comisiones contaminadas con CV
junk_legs = [(r["id"], r["nombre"], comisiones_problemas(r.get("comisiones_cargo")))
             for r in legs if comisiones_problemas(r.get("comisiones_cargo"))]
porcamara = {}
for lid, nm, _ in junk_legs:
    cam = byid[lid].get("camara", "?")
    porcamara[cam] = porcamara.get(cam, 0) + 1
print(f"  comisiones_cargo contaminado con CV: {len(junk_legs)} legisladores  {porcamara}")
for lid, nm, probs in junk_legs[:4]:
    print(f"    - {lid} {nm[:28]}: {probs[:3]}")

# foto_url cobertura
sin_foto = sum(1 for r in legs if not (r.get("foto_url") or "").startswith("http"))
print(f"  sin foto_url: {sin_foto}")
if sin_foto > len(legs)*0.15: warn.append(f"{sin_foto} sin foto (>15%)")

# reactividad: líder debe ser 100
react = [(r.get("reactividad") or 0, r.get("nombre"), r.get("id")) for r in legs]
mx = max(react)
print(f"  reactividad máx: {mx[0]} → {mx[1]} (id {mx[2]})")
if mx[0] != 100: fail.append(f"líder de reacción no llega a 100 (es {mx[0]})")
con_react = sum(1 for r in legs if (r.get("reactividad") or 0) > 0)
print(f"  con reacción > 0: {con_react}")

# efectividad: cuántos con número vs en revisión
con_ef = sum(1 for r in legs if wilson(r.get("elo_aprobados"), r.get("elo_desechados")) is not None)
print(f"  con efectividad calculable: {con_ef}")

# ── 5. Chequeos puntuales de esta sesión ─────────────────────────────
print("\n== 5. FIXES DE ESTA SESIÓN ==")
# Andrea comisiones
a = byid.get(571)
a_cc = (a or {}).get("comisiones_cargo", "")
ok_a = "Bienestar:Presidente" in a_cc and "Puntos Constitucionales" in a_cc
print(f"  {'OK ' if ok_a else 'FAIL'} Andrea (571) comisiones: {a_cc!r}")
if not ok_a: fail.append("Andrea sin comisiones correctas en D1")

# Efectividad de las fichas publicadas (PNG campaña) == sitio en vivo
PNG = {571: ("Andrea", 25), 610: ("Karen", 10), 532: ("Gino", 12), 162: ("Narro", 0)}
for lid, (nm, esperado) in PNG.items():
    r = byid.get(lid)
    if not r:
        print(f"  FAIL {nm} (id {lid}) ausente"); fail.append(f"{nm} ausente"); continue
    ef = wilson(r.get("elo_aprobados"), r.get("elo_desechados"))
    good = ef == esperado
    print(f"  {'OK ' if good else 'FAIL'} {nm} ef={ef} (PNG={esperado}) "
          f"[{r.get('elo_aprobados')} aprob / {r.get('elo_desechados')} desech]")
    if not good: fail.append(f"{nm}: efectividad sitio {ef} != PNG {esperado}")

# ── Veredicto ────────────────────────────────────────────────────────
print("\n" + "="*60)
if warn:
    print(f"AVISOS ({len(warn)}):")
    for w in warn: print("  ⚠ ", w)
if fail:
    print(f"\n❌ FALLOS ({len(fail)}):")
    for f in fail[:40]: print("  ✗", f)
    print("\nNO está al 100%.")
    sys.exit(1)
else:
    print("\n✅ TODO AL 100% — 100/100 simulaciones, integridad global y fixes verificados.")
    sys.exit(0)
