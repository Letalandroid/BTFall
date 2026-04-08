"""
Instrumentos Anexo 2 — recolección alineada con detecciones BLE.

Cada nueva alerta Fall-* (no duplicada por MAC+nombre) añade UNA fila en la
ficha activa: la primera con menos de 10 registros (precisión → sensibilidad →
especificidad → latencia → umbral). Si pausas el escáner, al reiniciar sigue
por el conteo actual en instrumento_validacion.db.

Indicadores (se recalculan al completar campos vía CLI o UPDATE):
  P = TP / (TP + FP)
  S = TP / (TP + FN)
  E = TN / (TN + FP)
  L = TA - TE  (ta_seg, te_seg en segundos; mismo origen temporal)
  U = M / S^2  (metros, segundos)
"""

from __future__ import annotations

import argparse
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any

DEFAULT_DB = Path(__file__).resolve().parent / "instrumento_validacion.db"

FICHAS: tuple[str, ...] = (
    "ficha_precision",
    "ficha_sensibilidad",
    "ficha_especificidad",
    "ficha_latencia",
    "ficha_umbral",
)

FICHA_LABELS: tuple[str, ...] = (
    "Precisión (P = TP/(TP+FP))",
    "Sensibilidad (S = TP/(TP+FN))",
    "Especificidad (E = TN/(TN+FP))",
    "Latencia (L = TA - TE)",
    "Umbral de activación (U = M/S²)",
)


def _connect(path: Path | str = DEFAULT_DB) -> sqlite3.Connection:
    return sqlite3.connect(str(path))


def init_instrumento_db(path: Path | str = DEFAULT_DB) -> None:
    path = Path(path)
    con = _connect(path)
    try:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS estudio_meta (
              id INTEGER PRIMARY KEY CHECK (id = 1),
              investigadores TEXT NOT NULL,
              institucion TEXT NOT NULL,
              tipo_prueba TEXT NOT NULL,
              dimension_estudio TEXT NOT NULL,
              fecha_inicio TEXT,
              fecha_final TEXT,
              variable TEXT NOT NULL,
              medida_resumen TEXT NOT NULL
            );

            INSERT OR IGNORE INTO estudio_meta (
              id, investigadores, institucion, tipo_prueba, dimension_estudio,
              fecha_inicio, fecha_final, variable, medida_resumen
            ) VALUES (
              1,
              'Acaro Cornejo Nhaisa Jhamily; Jimenez Arevalo Luis Guillermo',
              'Universidad César Vallejo',
              'Descriptivo',
              'Exactitud del diagnóstico / Rendimiento (según ficha)',
              '__/__/2025',
              '__/__/2025',
              'Riesgo de caídas',
              'Medición con sistema portátil (BLE + Edge Impulse) — Piura 2025'
            );

            CREATE TABLE IF NOT EXISTS ficha_precision (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              n_en_instrumento INTEGER NOT NULL,
              fecha TEXT NOT NULL,
              hora TEXT NOT NULL,
              n_persona INTEGER NOT NULL,
              fp INTEGER,
              tp INTEGER,
              p REAL,
              ble_name TEXT,
              ble_address TEXT,
              UNIQUE(n_en_instrumento)
            );

            CREATE TABLE IF NOT EXISTS ficha_sensibilidad (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              n_en_instrumento INTEGER NOT NULL,
              fecha TEXT NOT NULL,
              hora TEXT NOT NULL,
              n_persona INTEGER NOT NULL,
              fn INTEGER,
              tp INTEGER,
              s REAL,
              ble_name TEXT,
              ble_address TEXT,
              UNIQUE(n_en_instrumento)
            );

            CREATE TABLE IF NOT EXISTS ficha_especificidad (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              n_en_instrumento INTEGER NOT NULL,
              fecha TEXT NOT NULL,
              hora TEXT NOT NULL,
              n_persona INTEGER NOT NULL,
              fp INTEGER,
              tn INTEGER,
              e REAL,
              ble_name TEXT,
              ble_address TEXT,
              UNIQUE(n_en_instrumento)
            );

            CREATE TABLE IF NOT EXISTS ficha_latencia (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              n_en_instrumento INTEGER NOT NULL,
              fecha TEXT NOT NULL,
              hora TEXT NOT NULL,
              n_persona INTEGER NOT NULL,
              ta_seg REAL,
              te_seg REAL,
              l REAL,
              ble_name TEXT,
              ble_address TEXT,
              UNIQUE(n_en_instrumento)
            );

            CREATE TABLE IF NOT EXISTS ficha_umbral (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              n_en_instrumento INTEGER NOT NULL,
              fecha TEXT NOT NULL,
              hora TEXT NOT NULL,
              n_persona INTEGER NOT NULL,
              metros REAL,
              segundos REAL,
              u REAL,
              ble_name TEXT,
              ble_address TEXT,
              UNIQUE(n_en_instrumento)
            );

            CREATE TABLE IF NOT EXISTS app_meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );

            INSERT OR IGNORE INTO app_meta (key, value) VALUES ('next_n_persona', '1');
            """
        )
        con.commit()
    finally:
        con.close()


def _next_persona(con: sqlite3.Connection) -> int:
    cur = con.execute("SELECT value FROM app_meta WHERE key = 'next_n_persona'")
    row = cur.fetchone()
    n = int(row[0]) if row else 1
    con.execute(
        "UPDATE app_meta SET value = ? WHERE key = 'next_n_persona'",
        (str(n + 1),),
    )
    return n


def ficha_activa(con: sqlite3.Connection) -> tuple[int, str, int] | None:
    """Índice 0..4, nombre tabla, filas ya guardadas."""
    for i, table in enumerate(FICHAS):
        (cnt,) = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        if cnt < 10:
            return i, table, cnt
    return None


def _safe_div(num: float, den: float) -> float | None:
    if den == 0:
        return None
    return num / den


def recalc_indicators(con: sqlite3.Connection, table: str, row_id: int) -> None:
    if table == "ficha_precision":
        fp, tp = con.execute(
            "SELECT fp, tp FROM ficha_precision WHERE id = ?", (row_id,)
        ).fetchone()
        if fp is None or tp is None:
            return
        den = tp + fp
        p = _safe_div(float(tp), float(den))
        con.execute("UPDATE ficha_precision SET p = ? WHERE id = ?", (p, row_id))
    elif table == "ficha_sensibilidad":
        fn, tp = con.execute(
            "SELECT fn, tp FROM ficha_sensibilidad WHERE id = ?", (row_id,)
        ).fetchone()
        if fn is None or tp is None:
            return
        den = tp + fn
        s = _safe_div(float(tp), float(den))
        con.execute("UPDATE ficha_sensibilidad SET s = ? WHERE id = ?", (s, row_id))
    elif table == "ficha_especificidad":
        fp, tn = con.execute(
            "SELECT fp, tn FROM ficha_especificidad WHERE id = ?", (row_id,)
        ).fetchone()
        if fp is None or tn is None:
            return
        den = tn + fp
        e = _safe_div(float(tn), float(den))
        con.execute("UPDATE ficha_especificidad SET e = ? WHERE id = ?", (e, row_id))
    elif table == "ficha_latencia":
        ta, te = con.execute(
            "SELECT ta_seg, te_seg FROM ficha_latencia WHERE id = ?", (row_id,)
        ).fetchone()
        if ta is None or te is None:
            return
        l = float(ta) - float(te)
        con.execute("UPDATE ficha_latencia SET l = ? WHERE id = ?", (l, row_id))
    elif table == "ficha_umbral":
        m, s = con.execute(
            "SELECT metros, segundos FROM ficha_umbral WHERE id = ?", (row_id,)
        ).fetchone()
        if m is None or s is None or float(s) == 0.0:
            return
        u = float(m) / (float(s) ** 2)
        con.execute("UPDATE ficha_umbral SET u = ? WHERE id = ?", (u, row_id))


def record_fall_event(
    ble_name: str,
    ble_address: str,
    path: Path | str = DEFAULT_DB,
) -> dict[str, Any]:
    """
    Registra un evento de detección en la ficha que corresponda (10 filas por ficha).
    Devuelve un dict con etiqueta humana o error si las 5 fichas están llenas.
    """
    path = Path(path)
    init_instrumento_db(path)
    con = _connect(path)
    try:
        active = ficha_activa(con)
        if active is None:
            return {
                "ok": False,
                "mensaje": "Las 5 fichas (50 celdas de estudio) están completas.",
            }
        idx, table, count = active
        n_slot = count + 1
        now = datetime.now()
        fecha = now.strftime("%Y-%m-%d")
        hora = now.strftime("%H:%M:%S")
        n_persona = _next_persona(con)

        ta = time.time()
        if table == "ficha_precision":
            con.execute(
                """
                INSERT INTO ficha_precision (
                  n_en_instrumento, fecha, hora, n_persona, fp, tp, p,
                  ble_name, ble_address
                ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
                """,
                (n_slot, fecha, hora, n_persona, ble_name, ble_address),
            )
        elif table == "ficha_sensibilidad":
            con.execute(
                """
                INSERT INTO ficha_sensibilidad (
                  n_en_instrumento, fecha, hora, n_persona, fn, tp, s,
                  ble_name, ble_address
                ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
                """,
                (n_slot, fecha, hora, n_persona, ble_name, ble_address),
            )
        elif table == "ficha_especificidad":
            con.execute(
                """
                INSERT INTO ficha_especificidad (
                  n_en_instrumento, fecha, hora, n_persona, fp, tn, e,
                  ble_name, ble_address
                ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
                """,
                (n_slot, fecha, hora, n_persona, ble_name, ble_address),
            )
        elif table == "ficha_latencia":
            con.execute(
                """
                INSERT INTO ficha_latencia (
                  n_en_instrumento, fecha, hora, n_persona, ta_seg, te_seg, l,
                  ble_name, ble_address
                ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?)
                """,
                (n_slot, fecha, hora, n_persona, ta, ble_name, ble_address),
            )
        else:
            con.execute(
                """
                INSERT INTO ficha_umbral (
                  n_en_instrumento, fecha, hora, n_persona, metros, segundos, u,
                  ble_name, ble_address
                ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
                """,
                (n_slot, fecha, hora, n_persona, ble_name, ble_address),
            )

        row_id = con.execute("SELECT last_insert_rowid()").fetchone()[0]
        con.commit()
        return {
            "ok": True,
            "ficha_index": idx,
            "ficha_tabla": table,
            "ficha_etiqueta": FICHA_LABELS[idx],
            "n_en_instrumento": n_slot,
            "n_persona": n_persona,
            "row_id": row_id,
            "fecha": fecha,
            "hora": hora,
            "ble_name": ble_name,
            "ble_address": ble_address,
        }
    finally:
        con.close()


def ficha_counts(path: Path | str = DEFAULT_DB) -> dict[str, int]:
    """Conteo de filas por tabla (útil para comprobar que SQLite se actualiza)."""
    path = Path(path)
    if not path.is_file():
        return {t: 0 for t in FICHAS}
    init_instrumento_db(path)
    con = _connect(path)
    try:
        return {
            t: int(con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])
            for t in FICHAS
        }
    finally:
        con.close()


def status_report(path: Path | str = DEFAULT_DB) -> str:
    path = Path(path)
    if not path.is_file():
        return f"No existe aún la base: {path} (se crea al primer evento)."
    init_instrumento_db(path)
    con = _connect(path)
    try:
        lines = [f"Base: {path}", ""]
        active = ficha_activa(con)
        if active is None:
            lines.append("Estado: todas las fichas completas (5×10).")
        else:
            idx, table, cnt = active
            lines.append(
                f"Ficha activa: [{idx + 1}/5] {FICHA_LABELS[idx]} — tabla `{table}` "
                f"({cnt}/10 filas)."
            )
        lines.append("")
        for i, table in enumerate(FICHAS):
            (c,) = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            lines.append(f"  {FICHA_LABELS[i]}: {c}/10")
        return "\n".join(lines)
    finally:
        con.close()


def _cli_status(args: argparse.Namespace) -> None:
    print(status_report(args.db))


def _cli_set_precision(args: argparse.Namespace) -> None:
    init_instrumento_db(args.db)
    con = _connect(args.db)
    try:
        con.execute(
            "UPDATE ficha_precision SET fp = ?, tp = ? WHERE id = ?",
            (args.fp, args.tp, args.id),
        )
        recalc_indicators(con, "ficha_precision", args.id)
        con.commit()
        print("Actualizado ficha_precision id=", args.id)
    finally:
        con.close()


def _cli_set_sensibilidad(args: argparse.Namespace) -> None:
    init_instrumento_db(args.db)
    con = _connect(args.db)
    try:
        con.execute(
            "UPDATE ficha_sensibilidad SET fn = ?, tp = ? WHERE id = ?",
            (args.fn, args.tp, args.id),
        )
        recalc_indicators(con, "ficha_sensibilidad", args.id)
        con.commit()
        print("Actualizado ficha_sensibilidad id=", args.id)
    finally:
        con.close()


def _cli_set_especificidad(args: argparse.Namespace) -> None:
    init_instrumento_db(args.db)
    con = _connect(args.db)
    try:
        con.execute(
            "UPDATE ficha_especificidad SET fp = ?, tn = ? WHERE id = ?",
            (args.fp, args.tn, args.id),
        )
        recalc_indicators(con, "ficha_especificidad", args.id)
        con.commit()
        print("Actualizado ficha_especificidad id=", args.id)
    finally:
        con.close()


def _cli_set_latencia(args: argparse.Namespace) -> None:
    init_instrumento_db(args.db)
    con = _connect(args.db)
    try:
        con.execute(
            "UPDATE ficha_latencia SET te_seg = ? WHERE id = ?",
            (args.te, args.id),
        )
        recalc_indicators(con, "ficha_latencia", args.id)
        con.commit()
        print("Actualizado ficha_latencia id=", args.id)
    finally:
        con.close()


def _cli_set_umbral(args: argparse.Namespace) -> None:
    init_instrumento_db(args.db)
    con = _connect(args.db)
    try:
        con.execute(
            "UPDATE ficha_umbral SET metros = ?, segundos = ? WHERE id = ?",
            (args.metros, args.segundos, args.id),
        )
        recalc_indicators(con, "ficha_umbral", args.id)
        con.commit()
        print("Actualizado ficha_umbral id=", args.id)
    finally:
        con.close()


def _cli_list(args: argparse.Namespace) -> None:
    init_instrumento_db(args.db)
    con = _connect(args.db)
    try:
        for table in FICHAS:
            print(f"\n=== {table} ===")
            for row in con.execute(f"SELECT * FROM {table} ORDER BY id"):
                print(row)
    finally:
        con.close()


def main() -> None:
    p = argparse.ArgumentParser(description="Instrumentos Anexo 2 — validación")
    p.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="Ruta a instrumento_validacion.db",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status", help="Resumen de fichas y ficha activa")

    sp = sub.add_parser("set-precision", help="FP, TP y recalcular P")
    sp.add_argument("--id", type=int, required=True)
    sp.add_argument("--fp", type=int, required=True)
    sp.add_argument("--tp", type=int, required=True)

    ss = sub.add_parser("set-sensibilidad", help="FN, TP y recalcular S")
    ss.add_argument("--id", type=int, required=True)
    ss.add_argument("--fn", type=int, required=True)
    ss.add_argument("--tp", type=int, required=True)

    se = sub.add_parser("set-especificidad", help="FP, TN y recalcular E")
    se.add_argument("--id", type=int, required=True)
    se.add_argument("--fp", type=int, required=True)
    se.add_argument("--tn", type=int, required=True)

    sl = sub.add_parser("set-latencia", help="TE (seg); TA ya guardado al alertar")
    sl.add_argument("--id", type=int, required=True)
    sl.add_argument("--te", type=float, required=True, help="Tiempo del evento (s, mismo origen que TA)")

    su = sub.add_parser("set-umbral", help="Metros y segundos; recalcula U=M/S²")
    su.add_argument("--id", type=int, required=True)
    su.add_argument("--metros", type=float, required=True)
    su.add_argument("--segundos", type=float, required=True)

    sub.add_parser("list", help="Volcar tablas (depuración)")

    args = p.parse_args()
    if args.cmd == "status":
        _cli_status(args)
    elif args.cmd == "set-precision":
        _cli_set_precision(args)
    elif args.cmd == "set-sensibilidad":
        _cli_set_sensibilidad(args)
    elif args.cmd == "set-especificidad":
        _cli_set_especificidad(args)
    elif args.cmd == "set-latencia":
        _cli_set_latencia(args)
    elif args.cmd == "set-umbral":
        _cli_set_umbral(args)
    elif args.cmd == "list":
        _cli_list(args)


if __name__ == "__main__":
    main()
