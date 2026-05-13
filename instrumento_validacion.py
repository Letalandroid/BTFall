"""
Registro en instrumento_validacion.db según la ficha elegida al iniciar scan.
Fecha/hora en UTC−5. Cada fila n (1–10) admite hasta M personas (n_persona=1..M);
al completar M, el escaneo debe detenerse y volver a ejecutar scan.py para la siguiente fila.
tp/fp/fn/tn se derivan de forma coherente entre fichas a partir de F y S del anuncio BLE.
"""

from __future__ import annotations

import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Any

# Ruta por defecto junto al proyecto / cwd
_INSTRUMENTO_DB = os.environ.get(
    "BTFALL_INSTRUMENTO_DB",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "instrumento_validacion.db"),
)

# Hora de registro fija UTC−5 (sin DST en este offset).
UTC_MINUS_5 = timezone(timedelta(hours=-5))

FICHA_TABLES: tuple[tuple[str, str], ...] = (
    ("1", "ficha_precision"),
    ("2", "ficha_sensibilidad"),
    ("3", "ficha_especificidad"),
    ("4", "ficha_latencia"),
    ("5", "ficha_umbral"),
)

FICHAS: tuple[str, ...] = tuple(t for _, t in FICHA_TABLES)

_selected_table: str | None = None
_max_personas_por_fila: int | None = None
_session_fila_n: int | None = None
# Último n_persona escrito en esta sesión para la fila actual (0 = aún ninguno)
_person_counter: int = 0
# (mac_lower, episode) -> monotonic (s) primera vez Fall en ese episodio
_fall_first_mono: dict[tuple[str, int], float] = {}
# Origen de tiempos para ficha_latencia (segundos desde activación del instrumento)
_latencia_t0: float | None = None

_FALL_EP_RE = re.compile(
    r"^Fall-[^-]+-(\d+)-F(\d+)-S(\d+)$",
    re.IGNORECASE,
)

_CREATE_FICHA_TABLES_SQL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS ficha_precision (
      n INTEGER NOT NULL CHECK (n BETWEEN 1 AND 10),
      n_persona INTEGER NOT NULL CHECK (n_persona >= 1),
      fecha TEXT NOT NULL,
      hora TEXT NOT NULL,
      fp INTEGER NOT NULL,
      tp INTEGER NOT NULL,
      p REAL NOT NULL,
      PRIMARY KEY (n, n_persona)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ficha_sensibilidad (
      n INTEGER NOT NULL CHECK (n BETWEEN 1 AND 10),
      n_persona INTEGER NOT NULL CHECK (n_persona >= 1),
      fecha TEXT NOT NULL,
      hora TEXT NOT NULL,
      fn INTEGER NOT NULL,
      tp INTEGER NOT NULL,
      s REAL NOT NULL,
      PRIMARY KEY (n, n_persona)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ficha_especificidad (
      n INTEGER NOT NULL CHECK (n BETWEEN 1 AND 10),
      n_persona INTEGER NOT NULL CHECK (n_persona >= 1),
      fecha TEXT NOT NULL,
      hora TEXT NOT NULL,
      fp INTEGER NOT NULL,
      tn INTEGER NOT NULL,
      e REAL NOT NULL,
      PRIMARY KEY (n, n_persona)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ficha_latencia (
      n INTEGER NOT NULL CHECK (n BETWEEN 1 AND 10),
      n_persona INTEGER NOT NULL CHECK (n_persona >= 1),
      fecha TEXT NOT NULL,
      hora TEXT NOT NULL,
      ta REAL NOT NULL,
      te REAL NOT NULL,
      l REAL NOT NULL,
      PRIMARY KEY (n, n_persona)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ficha_umbral (
      n INTEGER NOT NULL CHECK (n BETWEEN 1 AND 10),
      n_persona INTEGER NOT NULL CHECK (n_persona >= 1),
      fecha TEXT NOT NULL,
      hora TEXT NOT NULL,
      metros REAL NOT NULL,
      segundos REAL NOT NULL,
      u REAL NOT NULL,
      PRIMARY KEY (n, n_persona)
    )
    """,
)


def _db() -> sqlite3.Connection:
    return sqlite3.connect(_INSTRUMENTO_DB)


def _pk_columns(con: sqlite3.Connection, table: str) -> list[str]:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(r[1]) for r in rows if r[5]]


def _legacy_primary_key_only_n(con: sqlite3.Connection, table: str) -> bool:
    pk = _pk_columns(con, table)
    return pk == ["n"]


def _migrate_table_to_composite_pk(con: sqlite3.Connection, table: str) -> None:
    tmp = f"{table}__migr"
    if table == "ficha_precision":
        new_sql = _CREATE_FICHA_TABLES_SQL[0]
        copy_sql = (
            f"INSERT INTO {tmp} (n, n_persona, fecha, hora, fp, tp, p) "
            f"SELECT n, n_persona, fecha, hora, fp, tp, p FROM {table}"
        )
    elif table == "ficha_sensibilidad":
        new_sql = _CREATE_FICHA_TABLES_SQL[1]
        copy_sql = (
            f"INSERT INTO {tmp} (n, n_persona, fecha, hora, fn, tp, s) "
            f"SELECT n, n_persona, fecha, hora, fn, tp, s FROM {table}"
        )
    elif table == "ficha_especificidad":
        new_sql = _CREATE_FICHA_TABLES_SQL[2]
        copy_sql = (
            f"INSERT INTO {tmp} (n, n_persona, fecha, hora, fp, tn, e) "
            f"SELECT n, n_persona, fecha, hora, fp, tn, e FROM {table}"
        )
    elif table == "ficha_latencia":
        new_sql = _CREATE_FICHA_TABLES_SQL[3]
        copy_sql = (
            f"INSERT INTO {tmp} (n, n_persona, fecha, hora, ta, te, l) "
            f"SELECT n, n_persona, fecha, hora, ta, te, l FROM {table}"
        )
    elif table == "ficha_umbral":
        new_sql = _CREATE_FICHA_TABLES_SQL[4]
        copy_sql = (
            f"INSERT INTO {tmp} (n, n_persona, fecha, hora, metros, segundos, u) "
            f"SELECT n, n_persona, fecha, hora, metros, segundos, u FROM {table}"
        )
    else:
        return
    con.execute(new_sql.replace(f"CREATE TABLE IF NOT EXISTS {table}", f"CREATE TABLE {tmp}"))
    con.execute(copy_sql)
    con.execute(f"DROP TABLE {table}")
    con.execute(f"ALTER TABLE {tmp} RENAME TO {table}")


def _ensure_instrumento_db() -> None:
    """Crea la base si no existe, tablas con PK (n, n_persona) y migra esquema antiguo (solo n)."""
    con = _db()
    try:
        for sql, (_, table) in zip(_CREATE_FICHA_TABLES_SQL, FICHA_TABLES, strict=True):
            cur = con.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,),
            )
            exists = cur.fetchone() is not None
            if not exists:
                con.execute(sql)
            elif _legacy_primary_key_only_n(con, table):
                _migrate_table_to_composite_pk(con, table)
        con.commit()
    finally:
        con.close()


def init_instrumento_db() -> None:
    """Asegura archivo y esquema del instrumento de validación."""
    _ensure_instrumento_db()


def _count_rows_for_n(con: sqlite3.Connection, table: str, n: int) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table} WHERE n = ?", (n,)).fetchone()[0])


def _next_available_fila(con: sqlite3.Connection, table: str, max_personas: int) -> int | None:
    """Primera fila n en 1..10 con menos de max_personas registros; None si todas están llenas."""
    for n in range(1, 11):
        if _count_rows_for_n(con, table, n) < max_personas:
            return n
    return None


def prompt_ficha_y_persona() -> None:
    global _selected_table, _max_personas_por_fila, _session_fila_n, _person_counter
    global _fall_first_mono, _latencia_t0
    _fall_first_mono.clear()
    _latencia_t0 = None
    _session_fila_n = None
    _person_counter = 0
    _max_personas_por_fila = None
    _ensure_instrumento_db()
    print("")
    print("Instrumento de validación — elige la ficha a registrar en cada caída (Fall-*):")
    for key, t in FICHA_TABLES:
        print(f"  {key}) {t}")
    choice = input("Número de ficha [1-5] (Enter = no registrar): ").strip()
    labels = {k: t for k, t in FICHA_TABLES}
    if not choice:
        _selected_table = None
        print("Registro en instrumento: desactivado.")
        return
    table = labels.get(choice)
    if table is None:
        print("Opción no válida; registro en instrumento desactivado.")
        _selected_table = None
        return
    _selected_table = table
    while True:
        raw = input(
            "Máximo de personas a registrar en esta fila (1..100). "
            "Al llegar a ese número el escaneo se detiene; vuelve a ejecutar scan.py para la siguiente fila: "
        ).strip()
        try:
            m = int(raw)
        except ValueError:
            print("Introduce un entero válido.")
            continue
        if m < 1 or m > 100:
            print("Debe estar entre 1 y 100.")
            continue
        _max_personas_por_fila = m
        break

    con = _db()
    try:
        fila = _next_available_fila(con, table, _max_personas_por_fila)
        if fila is None:
            print(
                "No queda ninguna fila n=1..10 con hueco para ese máximo de personas. "
                "Exporta o vacía las tablas de instrumento y vuelve a intentarlo."
            )
            _selected_table = None
            _max_personas_por_fila = None
            return
        _session_fila_n = fila
        _person_counter = _count_rows_for_n(con, table, fila)
    finally:
        con.close()

    _latencia_t0 = time.monotonic()
    pend = _max_personas_por_fila - _person_counter
    print(
        f"Activo: tabla `{_selected_table}`, fila n={_session_fila_n}, "
        f"máximo {_max_personas_por_fila} persona(s) por fila "
        f"({pend} hueco(s) en esta fila). Fecha/hora en UTC−5."
    )


def status_report() -> str:
    if _selected_table is None or _max_personas_por_fila is None or _session_fila_n is None:
        return "Instrumento de validación: sin ficha activa (solo fall.db + BLE)."
    pend = max(0, _max_personas_por_fila - _person_counter)
    return (
        f"Instrumento de validación: ficha `{_selected_table}`, fila n={_session_fila_n}, "
        f"personas {_person_counter}/{_max_personas_por_fila} en esta fila "
        f"({pend} restante(s) antes de detener)."
    )


def ficha_counts() -> dict[str, int]:
    _ensure_instrumento_db()
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


def _fecha_hora_utc5() -> tuple[str, str]:
    now = datetime.now(UTC_MINUS_5)
    return now.strftime("%Y-%m-%d"), now.strftime("%H:%M:%S")


def _confusion_from_scores(fall_pct: int, stand_pct: int) -> dict[str, int]:
    """
    Pesos enteros coherentes entre fichas:
    tp = F, fp = S (salida del modelo), fn = 100−tp, tn = 100−fp (complementos en escala 0..100).
    """
    tp = int(max(0, min(100, fall_pct)))
    fp = int(max(0, min(100, stand_pct)))
    fn = max(0, 100 - tp)
    tn = max(0, 100 - fp)
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn}


def _insert_row(
    table: str,
    n: int,
    n_persona: int,
    fecha: str,
    hora: str,
    cols: dict[str, Any],
) -> None:
    keys = ["n", "n_persona", "fecha", "hora", *cols.keys()]
    placeholders = ", ".join("?" * len(keys))
    sql = f"INSERT OR REPLACE INTO {table} ({', '.join(keys)}) VALUES ({placeholders})"
    vals = [n, n_persona, fecha, hora, *cols.values()]
    con = _db()
    try:
        with con:
            con.execute(sql, vals)
    finally:
        con.close()


def record_fall_event(name: str, address: str) -> dict:
    """
    Registra una caída confirmada (nombre Fall-*) en la ficha activa.
    Devuelve dict con métricas para consola; stop_scan=True al completar el máximo de personas en la fila.
    """
    global _person_counter

    if (
        _selected_table is None
        or _max_personas_por_fila is None
        or _session_fila_n is None
    ):
        return {
            "ok": False,
            "mensaje": "Instrumento de validación no activo (elige ficha al inicio).",
        }
    _ensure_instrumento_db()

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

    next_persona = _person_counter + 1
    if next_persona > _max_personas_por_fila:
        return {
            "ok": False,
            "mensaje": (
                "Esta fila ya tiene el máximo de personas registradas; "
                "reinicia scan.py para usar la siguiente fila."
            ),
        }

    fecha, hora = _fecha_hora_utc5()
    fila_n = _session_fila_n
    n_persona_col = next_persona

    cm = _confusion_from_scores(fall_pct, stand_pct)
    tp = cm["tp"]
    fp = cm["fp"]
    fn = cm["fn"]
    tn = cm["tn"]

    table = _selected_table
    out: dict[str, Any] = {
        "ok": True,
        "ficha_etiqueta": table,
        "n_en_instrumento": fila_n,
        "n_persona": n_persona_col,
        "max_personas_fila": _max_personas_por_fila,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
    }

    denom_ps = tp + fp
    if table == "ficha_precision":
        p = (tp / denom_ps) if denom_ps > 0 else 0.0
        _insert_row(table, fila_n, n_persona_col, fecha, hora, {"fp": fp, "tp": tp, "p": round(p, 6)})
        out.update({"p": round(p, 6)})
    elif table == "ficha_sensibilidad":
        s = (tp / (tp + fn)) if (tp + fn) > 0 else 0.0
        _insert_row(table, fila_n, n_persona_col, fecha, hora, {"fn": fn, "tp": tp, "s": round(s, 6)})
        out.update({"s": round(s, 6)})
    elif table == "ficha_especificidad":
        e = (tn / (tn + fp)) if (tn + fp) > 0 else 0.0
        _insert_row(table, fila_n, n_persona_col, fecha, hora, {"fp": fp, "tn": tn, "e": round(e, 6)})
        out.update({"e": round(e, 6)})
    elif table == "ficha_latencia":
        t0 = _latencia_t0 if _latencia_t0 is not None else te_m
        ta = round(ta_m - t0, 4)
        te = round(te_m - t0, 4)
        l = round(max(0.0, ta - te), 4)
        _insert_row(table, fila_n, n_persona_col, fecha, hora, {"ta": ta, "te": te, "l": l})
        out.update({"ta": ta, "te": te, "l": l})
    elif table == "ficha_umbral":
        metros = round((fall_pct / 100.0) * 4.0 + 0.25, 3)
        segundos = round(max(0.3, min(3.0, l_sec if l_sec > 0.02 else 0.5)), 3)
        u = metros / (segundos**2) if segundos > 0 else 0.0
        _insert_row(
            table,
            fila_n,
            n_persona_col,
            fecha,
            hora,
            {"metros": metros, "segundos": segundos, "u": round(u, 6)},
        )
        out.update({"metros": metros, "segundos": segundos, "u": round(u, 6)})
    else:
        return {"ok": False, "mensaje": "Tabla de ficha desconocida."}

    _person_counter = next_persona
    if _person_counter >= _max_personas_por_fila:
        out["stop_scan"] = True
        out["mensaje_fin"] = (
            "Límite de personas para esta fila alcanzado. "
            "El escaneo se detendrá; ejecuta de nuevo scan.py para la siguiente fila."
        )

    return out
