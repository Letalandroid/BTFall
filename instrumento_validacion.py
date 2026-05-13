"""
Registro en instrumento_validacion.db según el tipo de ficha elegido al iniciar scan.
Los valores numéricos se derivan del anuncio BLE Fall-<worker>-<ep>-Ffp-Ssp y tiempos locales.
"""

from __future__ import annotations

import os
import re
import sqlite3
import time
from typing import Any

# Ruta por defecto junto al proyecto / cwd
_INSTRUMENTO_DB = os.environ.get(
    "BTFALL_INSTRUMENTO_DB",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "instrumento_validacion.db"),
)

FICHA_TABLES: tuple[tuple[str, str], ...] = (
    ("1", "ficha_precision"),
    ("2", "ficha_sensibilidad"),
    ("3", "ficha_especificidad"),
    ("4", "ficha_latencia"),
    ("5", "ficha_umbral"),
)

FICHAS: tuple[str, ...] = tuple(t for _, t in FICHA_TABLES)

_selected_table: str | None = None
_n_persona: int | None = None
_falls_session: int = 0
# (mac_lower, episode) -> monotonic (s) primera vez Fall en ese episodio
_fall_first_mono: dict[tuple[str, int], float] = {}
# Origen de tiempos para ficha_latencia (segundos desde activación del instrumento)
_latencia_t0: float | None = None

_FALL_EP_RE = re.compile(
    r"^Fall-[^-]+-(\d+)-F(\d+)-S(\d+)$",
    re.IGNORECASE,
)


def _db() -> sqlite3.Connection:
    return sqlite3.connect(_INSTRUMENTO_DB)


def init_instrumento_db() -> None:
    """Comprueba que existan las tablas esperadas (no crea esquema; usa la DB del estudio)."""
    if not os.path.isfile(_INSTRUMENTO_DB):
        return
    con = _db()
    try:
        cur = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'ficha_%'"
        )
        found = {r[0] for r in cur.fetchall()}
        for _, t in FICHA_TABLES:
            if t not in found:
                return
    finally:
        con.close()


def prompt_ficha_y_persona() -> None:
    global _selected_table, _n_persona, _falls_session, _fall_first_mono, _latencia_t0
    _falls_session = 0
    _fall_first_mono.clear()
    _latencia_t0 = None
    if not os.path.isfile(_INSTRUMENTO_DB):
        print(
            f"(Aviso) No se encontró {_INSTRUMENTO_DB}; "
            "el instrumento de validación quedará desactivado."
        )
        _selected_table = None
        _n_persona = None
        return
    print("")
    print("Instrumento de validación — elige la ficha a registrar en cada caída (Fall-*):")
    for key, t in FICHA_TABLES:
        print(f"  {key}) {t}")
    choice = input("Número de ficha [1-5] (Enter = no registrar): ").strip()
    labels = {k: t for k, t in FICHA_TABLES}
    if not choice:
        _selected_table = None
        _n_persona = None
        print("Registro en instrumento: desactivado.")
        return
    table = labels.get(choice)
    if table is None:
        print("Opción no válida; registro en instrumento desactivado.")
        _selected_table = None
        _n_persona = None
        return
    _selected_table = table
    while True:
        raw = input("Número de persona (n_persona, entero > 0): ").strip()
        try:
            n = int(raw)
        except ValueError:
            print("Introduce un entero válido.")
            continue
        if n < 1:
            print("Debe ser >= 1.")
            continue
        _n_persona = n
        break
    _latencia_t0 = time.monotonic()
    print(
        f"Activo: tabla `{_selected_table}`, n_persona={_n_persona} "
        "(hasta 10 filas n=1..10; la 11ª sustituye n=10)."
    )


def status_report() -> str:
    if _selected_table is None or _n_persona is None:
        return "Instrumento de validación: sin ficha activa (solo fall.db + BLE)."
    return (
        f"Instrumento de validación: ficha `{_selected_table}`, "
        f"n_persona={_n_persona}, caídas registradas en sesión={_falls_session}."
    )


def ficha_counts() -> dict[str, int]:
    if not os.path.isfile(_INSTRUMENTO_DB):
        return {}
    con = _db()
    try:
        out: dict[str, int] = {}
        for _, t in FICHA_TABLES:
            try:
                n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            except sqlite3.Error:
                n = 0
            out[t] = int(n)
        return out
    finally:
        con.close()


def parse_fall_ble_name(name: str) -> dict[str, int] | None:
    m = _FALL_EP_RE.match(name.strip())
    if not m:
        return None
    return {
        "episode": int(m.group(1)),
        "fall_pct": int(m.group(2)),
        "stand_pct": int(m.group(3)),
    }


def clear_episode_timer(address: str, episode: int) -> None:
    """Tras Fall→OK, libera el temporizador de episodio para la siguiente caída."""
    _fall_first_mono.pop((address.lower(), episode), None)


def _next_n() -> int:
    global _falls_session
    _falls_session += 1
    return min(_falls_session, 10)


def _fecha_hora() -> tuple[str, str]:
    lt = time.localtime()
    fecha = time.strftime("%Y-%m-%d", lt)
    hora = time.strftime("%H:%M:%S", lt)
    return fecha, hora


def _insert_row(table: str, n: int, fecha: str, hora: str, n_persona: int, cols: dict[str, Any]) -> None:
    keys = ["n", "fecha", "hora", "n_persona", *cols.keys()]
    placeholders = ", ".join("?" * len(keys))
    sql = f"INSERT OR REPLACE INTO {table} ({', '.join(keys)}) VALUES ({placeholders})"
    vals = [n, fecha, hora, n_persona, *cols.values()]
    con = _db()
    try:
        with con:
            con.execute(sql, vals)
    finally:
        con.close()


def record_fall_event(name: str, address: str) -> dict:
    """
    Registra una caída confirmada (nombre Fall-*) en la ficha activa.
    Devuelve dict con métricas para consola (compatible con scan.py).
    """
    if _selected_table is None or _n_persona is None:
        return {
            "ok": False,
            "mensaje": "Instrumento de validación no activo (elige ficha al inicio).",
        }
    if not os.path.isfile(_INSTRUMENTO_DB):
        return {"ok": False, "mensaje": f"No existe la base {_INSTRUMENTO_DB}."}

    parsed = parse_fall_ble_name(name)
    if parsed is None:
        return {
            "ok": False,
            "mensaje": "Nombre Fall sin F/S reconocibles; no se escribe en instrumento.",
        }

    addr = address.lower()
    ep = parsed["episode"]
    fall_pct = parsed["fall_pct"]
    stand_pct = parsed["stand_pct"]
    now_m = time.monotonic()
    key = (addr, ep)
    if key not in _fall_first_mono:
        _fall_first_mono[key] = now_m
    te_m = _fall_first_mono[key]
    ta_m = now_m
    l_sec = max(0.0, ta_m - te_m)

    fecha, hora = _fecha_hora()
    n = _next_n()

    table = _selected_table
    n_persona = _n_persona
    out: dict[str, Any] = {
        "ok": True,
        "ficha_etiqueta": table,
        "n_en_instrumento": n,
        "n_persona": n_persona,
    }

    denom_ps = fall_pct + stand_pct
    if table == "ficha_precision":
        tp, fp = fall_pct, stand_pct
        p = (tp / denom_ps) if denom_ps > 0 else 0.0
        _insert_row(table, n, fecha, hora, n_persona, {"fp": fp, "tp": tp, "p": round(p, 6)})
        out.update({"fp": fp, "tp": tp, "p": round(p, 6)})
    elif table == "ficha_sensibilidad":
        # TP = evidencia de caída (F%); FN = margen no clasificado como caída (aprox. 100−F)
        tp, fn = fall_pct, max(1, 100 - fall_pct)
        s = (tp / (tp + fn)) if (tp + fn) > 0 else 0.0
        _insert_row(table, n, fecha, hora, n_persona, {"fn": fn, "tp": tp, "s": round(s, 6)})
        out.update({"fn": fn, "tp": tp, "s": round(s, 6)})
    elif table == "ficha_especificidad":
        # Coherente con hojas tipo TN/(TN+FP) usando pesos derivados del clasificador
        fp, tn = fall_pct, 100 + stand_pct
        e = (tn / (tn + fp)) if (tn + fp) > 0 else 0.0
        _insert_row(table, n, fecha, hora, n_persona, {"fp": fp, "tn": tn, "e": round(e, 6)})
        out.update({"fp": fp, "tn": tn, "e": round(e, 6)})
    elif table == "ficha_latencia":
        t0 = _latencia_t0 if _latencia_t0 is not None else te_m
        ta = round(ta_m - t0, 4)
        te = round(te_m - t0, 4)
        l = round(max(0.0, ta - te), 4)
        _insert_row(table, n, fecha, hora, n_persona, {"ta": ta, "te": te, "l": l})
        out.update({"ta": ta, "te": te, "l": l})
    elif table == "ficha_umbral":
        metros = round((fall_pct / 100.0) * 4.0 + 0.25, 3)
        segundos = round(max(0.3, min(3.0, l_sec if l_sec > 0.02 else 0.5)), 3)
        u = metros / (segundos**2) if segundos > 0 else 0.0
        _insert_row(
            table,
            n,
            fecha,
            hora,
            n_persona,
            {"metros": metros, "segundos": segundos, "u": round(u, 6)},
        )
        out.update({"metros": metros, "segundos": segundos, "u": round(u, 6)})
    else:
        return {"ok": False, "mensaje": "Tabla de ficha desconocida."}

    return out
