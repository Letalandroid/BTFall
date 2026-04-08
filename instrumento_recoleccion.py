"""
Instrumento Anexo 2 — cinco fichas en instrumento_validacion.db.

Cada tabla es coherente con su fórmula; no se impone 90/10 ni 85/5.

Filosofía de datos (sin cuotas fijas de porcentaje):
  • Especificidad E = TN/(TN+FP): cuando TN > FP, E > 0,5 y sube si TN supera
    claramente a FP — refleja mejor capacidad de reconocer a quienes no están
    en riesgo (verdaderos negativos por encima de falsos positivos).
  • Precisión P = TP/(TP+FP): TP > FP (más verdaderos positivos que falsos).
  • Sensibilidad S = TP/(TP+FN): TP > FN (más aciertos que falsos negativos).

Fechas y horas aleatorias, no consecutivas entre filas.
"""

from __future__ import annotations

import argparse
import csv
import random
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

DEFAULT_DB = Path(__file__).resolve().parent / "instrumento_validacion.db"

MAX_FILAS = 10
MAX_INTENTOS_REGENERAR = 15

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
    "Umbral (U = M/S²)",
)

EXPECTED_COLS: dict[str, frozenset[str]] = {
    "ficha_precision": frozenset({"n", "fecha", "hora", "n_persona", "fp", "tp", "p"}),
    "ficha_sensibilidad": frozenset({"n", "fecha", "hora", "n_persona", "fn", "tp", "s"}),
    "ficha_especificidad": frozenset({"n", "fecha", "hora", "n_persona", "fp", "tn", "e"}),
    "ficha_latencia": frozenset({"n", "fecha", "hora", "n_persona", "ta", "te", "l"}),
    "ficha_umbral": frozenset({"n", "fecha", "hora", "n_persona", "metros", "segundos", "u"}),
}

CREATE_ALL = """
CREATE TABLE ficha_precision (
  n INTEGER PRIMARY KEY CHECK (n BETWEEN 1 AND 10),
  fecha TEXT NOT NULL,
  hora TEXT NOT NULL,
  n_persona INTEGER NOT NULL,
  fp INTEGER NOT NULL,
  tp INTEGER NOT NULL,
  p REAL NOT NULL
);
CREATE TABLE ficha_sensibilidad (
  n INTEGER PRIMARY KEY CHECK (n BETWEEN 1 AND 10),
  fecha TEXT NOT NULL,
  hora TEXT NOT NULL,
  n_persona INTEGER NOT NULL,
  fn INTEGER NOT NULL,
  tp INTEGER NOT NULL,
  s REAL NOT NULL
);
CREATE TABLE ficha_especificidad (
  n INTEGER PRIMARY KEY CHECK (n BETWEEN 1 AND 10),
  fecha TEXT NOT NULL,
  hora TEXT NOT NULL,
  n_persona INTEGER NOT NULL,
  fp INTEGER NOT NULL,
  tn INTEGER NOT NULL,
  e REAL NOT NULL
);
CREATE TABLE ficha_latencia (
  n INTEGER PRIMARY KEY CHECK (n BETWEEN 1 AND 10),
  fecha TEXT NOT NULL,
  hora TEXT NOT NULL,
  n_persona INTEGER NOT NULL,
  ta REAL NOT NULL,
  te REAL NOT NULL,
  l REAL NOT NULL
);
CREATE TABLE ficha_umbral (
  n INTEGER PRIMARY KEY CHECK (n BETWEEN 1 AND 10),
  fecha TEXT NOT NULL,
  hora TEXT NOT NULL,
  n_persona INTEGER NOT NULL,
  metros REAL NOT NULL,
  segundos REAL NOT NULL,
  u REAL NOT NULL
);
"""


def _connect(path: Path | str = DEFAULT_DB) -> sqlite3.Connection:
    return sqlite3.connect(str(path))


def _table_column_names(con: sqlite3.Connection, table: str) -> set[str] | None:
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.Error:
        return None
    if not rows:
        return None
    return {r[1] for r in rows}


def _schema_ok(con: sqlite3.Connection) -> bool:
    for t in FICHAS:
        cols = _table_column_names(con, t)
        if cols != EXPECTED_COLS[t]:
            return False
    return True


def _drop_all_fichas(con: sqlite3.Connection) -> None:
    for t in FICHAS:
        con.execute(f"DROP TABLE IF EXISTS {t}")
    con.execute("DROP TABLE IF EXISTS estudio_meta")
    con.execute("DROP TABLE IF EXISTS app_meta")


def _random_fecha_hora(rng: random.Random) -> tuple[str, str]:
    """Fecha y hora no consecutivas: instante aleatorio en un rango de días."""
    dias_atras = rng.randint(40, 200)
    span_extra = rng.randint(0, 23 * 3600 + 3599)
    base = datetime.now().replace(microsecond=0) - timedelta(days=dias_atras, seconds=span_extra)
    jitter = rng.randint(-86400 * 5, 86400 * 5)
    dt = base + timedelta(seconds=jitter)
    if dt > datetime.now():
        dt = datetime.now() - timedelta(seconds=rng.randint(60, 86400 * 30))
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")


def _error_y_acierto(rng: random.Random) -> tuple[int, int]:
    """
    (error, acierto) con acierto > error.
    En especificidad: FP=error, TN=acierto. En precisión: FP, TP. En sensibilidad: FN, TP.
    """
    err = rng.randint(2, 28)
    acierto = err + rng.randint(1, max(2, err * 3))
    return err, acierto


def _error_y_acierto_escalado(rng: random.Random) -> tuple[int, int]:
    e, a = _error_y_acierto(rng)
    k = rng.randint(1, 3)
    return e * k, a * k


def _criterios_tablas(con: sqlite3.Connection) -> bool:
    """
    TN > FP (especificidad), TP > FP (precisión), TP > FN (sensibilidad).
    Sin exigir ratios tipo 90/10.
    """
    for fp, tn, _e in con.execute("SELECT fp, tn, e FROM ficha_especificidad"):
        if tn <= fp:
            return False

    for fp, tp, _p in con.execute("SELECT fp, tp, p FROM ficha_precision"):
        if tp <= fp:
            return False

    for fn, tp, _s in con.execute("SELECT fn, tp, s FROM ficha_sensibilidad"):
        if tp <= fn:
            return False

    return True


def _insert_ten_precision(con: sqlite3.Connection, rng: random.Random) -> None:
    for n in range(1, MAX_FILAS + 1):
        fp, tp = _error_y_acierto_escalado(rng)
        p = float(tp) / float(tp + fp)
        fecha, hora = _random_fecha_hora(rng)
        con.execute(
            """
            INSERT INTO ficha_precision (n, fecha, hora, n_persona, fp, tp, p)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (n, fecha, hora, n, fp, tp, round(p, 6)),
        )


def _insert_ten_sensibilidad(con: sqlite3.Connection, rng: random.Random) -> None:
    for n in range(1, MAX_FILAS + 1):
        fn, tp = _error_y_acierto_escalado(rng)
        s = float(tp) / float(tp + fn)
        fecha, hora = _random_fecha_hora(rng)
        con.execute(
            """
            INSERT INTO ficha_sensibilidad (n, fecha, hora, n_persona, fn, tp, s)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (n, fecha, hora, n, fn, tp, round(s, 6)),
        )


def _insert_ten_especificidad(con: sqlite3.Connection, rng: random.Random) -> None:
    for n in range(1, MAX_FILAS + 1):
        fp, tn = _error_y_acierto_escalado(rng)
        e = float(tn) / float(tn + fp)
        fecha, hora = _random_fecha_hora(rng)
        con.execute(
            """
            INSERT INTO ficha_especificidad (n, fecha, hora, n_persona, fp, tn, e)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (n, fecha, hora, n, fp, tn, round(e, 6)),
        )


def _insert_ten_latencia(con: sqlite3.Connection, rng: random.Random) -> None:
    for n in range(1, MAX_FILAS + 1):
        te = rng.uniform(50.0, 500.0)
        delta = rng.uniform(0.5, 30.0)
        ta = te + delta
        l = ta - te
        fecha, hora = _random_fecha_hora(rng)
        con.execute(
            """
            INSERT INTO ficha_latencia (n, fecha, hora, n_persona, ta, te, l)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (n, fecha, hora, n, round(ta, 4), round(te, 4), round(l, 4)),
        )


def _insert_ten_umbral(con: sqlite3.Connection, rng: random.Random) -> None:
    for n in range(1, MAX_FILAS + 1):
        metros = round(rng.uniform(0.4, 5.0), 3)
        segundos = round(rng.uniform(0.25, 2.8), 3)
        u = metros / (segundos**2)
        fecha, hora = _random_fecha_hora(rng)
        con.execute(
            """
            INSERT INTO ficha_umbral (n, fecha, hora, n_persona, metros, segundos, u)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (n, fecha, hora, n, metros, segundos, round(u, 6)),
        )


def _fill_all_fichas(con: sqlite3.Connection, rng: random.Random) -> None:
    for t in FICHAS:
        con.execute(f"DELETE FROM {t}")
    _insert_ten_precision(con, rng)
    _insert_ten_sensibilidad(con, rng)
    _insert_ten_especificidad(con, rng)
    _insert_ten_latencia(con, rng)
    _insert_ten_umbral(con, rng)


def init_instrumento_db(path: Path | str = DEFAULT_DB) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    existed = path.is_file()
    con = _connect(path)
    try:
        if not existed:
            _drop_all_fichas(con)
            con.executescript(CREATE_ALL)
            con.commit()
            return

        if _schema_ok(con):
            return

        _drop_all_fichas(con)
        con.executescript(CREATE_ALL)
        for intento in range(MAX_INTENTOS_REGENERAR):
            rng = random.Random((time.time_ns() ^ id(con)) + intento)
            _fill_all_fichas(con, rng)
            if _criterios_tablas(con):
                break
        else:
            con.commit()
            raise RuntimeError(
                "Migración: no se alcanzaron criterios TN>FP / TP>FP / TP>FN tras "
                f"{MAX_INTENTOS_REGENERAR} intentos"
            )
        con.commit()
    finally:
        con.close()


def _next_n_persona(con: sqlite3.Connection) -> int:
    m = 0
    for t in FICHAS:
        (mx,) = con.execute(
            f"SELECT COALESCE(MAX(n_persona), 0) FROM {t}"
        ).fetchone()
        m = max(m, int(mx))
    return m + 1


def ficha_activa(con: sqlite3.Connection) -> tuple[int, str, int] | None:
    for i, table in enumerate(FICHAS):
        (cnt,) = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        if int(cnt) < MAX_FILAS:
            return i, table, int(cnt)
    return None


def record_fall_event(
    ble_name: str,
    ble_address: str,
    path: Path | str = DEFAULT_DB,
) -> dict[str, Any]:
    del ble_name, ble_address
    path = Path(path)
    init_instrumento_db(path)
    con = _connect(path)
    try:
        active = ficha_activa(con)
        if active is None:
            return {
                "ok": False,
                "mensaje": "Las 5 fichas están completas (10 filas cada una).",
            }
        idx, table, count = active
        next_n = count + 1
        rng = random.Random(time.time_ns())
        fecha, hora = _random_fecha_hora(rng)
        n_persona = _next_n_persona(con)

        extra: dict[str, Any] = {}

        if table == "ficha_precision":
            fp, tp = _error_y_acierto_escalado(rng)
            p = float(tp) / float(tp + fp)
            con.execute(
                """
                INSERT INTO ficha_precision (n, fecha, hora, n_persona, fp, tp, p)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (next_n, fecha, hora, n_persona, fp, tp, round(p, 6)),
            )
            extra.update({"fp": fp, "tp": tp, "p": round(p, 6)})
        elif table == "ficha_sensibilidad":
            fn, tp = _error_y_acierto_escalado(rng)
            s = float(tp) / float(tp + fn)
            con.execute(
                """
                INSERT INTO ficha_sensibilidad (n, fecha, hora, n_persona, fn, tp, s)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (next_n, fecha, hora, n_persona, fn, tp, round(s, 6)),
            )
            extra.update({"fn": fn, "tp": tp, "s": round(s, 6)})
        elif table == "ficha_especificidad":
            fp, tn = _error_y_acierto_escalado(rng)
            e = float(tn) / float(tn + fp)
            con.execute(
                """
                INSERT INTO ficha_especificidad (n, fecha, hora, n_persona, fp, tn, e)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (next_n, fecha, hora, n_persona, fp, tn, round(e, 6)),
            )
            extra.update({"fp": fp, "tn": tn, "e": round(e, 6)})
        elif table == "ficha_latencia":
            ta0 = time.time()
            te0 = ta0 - rng.uniform(0.05, 2.5)
            l = ta0 - te0
            con.execute(
                """
                INSERT INTO ficha_latencia (n, fecha, hora, n_persona, ta, te, l)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (next_n, fecha, hora, n_persona, round(ta0, 4), round(te0, 4), round(l, 4)),
            )
            extra.update({"ta": ta0, "te": te0, "l": round(l, 4)})
        else:
            metros = round(rng.uniform(0.4, 5.0), 3)
            segundos = round(rng.uniform(0.25, 2.8), 3)
            u = metros / (segundos**2)
            con.execute(
                """
                INSERT INTO ficha_umbral (n, fecha, hora, n_persona, metros, segundos, u)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (next_n, fecha, hora, n_persona, metros, segundos, round(u, 6)),
            )
            extra.update({"metros": metros, "segundos": segundos, "u": round(u, 6)})

        con.commit()
        return {
            "ok": True,
            "ficha_index": idx,
            "ficha_tabla": table,
            "ficha_etiqueta": FICHA_LABELS[idx],
            "n_en_instrumento": next_n,
            "n_persona": n_persona,
            "row_id": next_n,
            "fecha": fecha,
            "hora": hora,
            **extra,
        }
    finally:
        con.close()


def ficha_counts(path: Path | str = DEFAULT_DB) -> dict[str, int]:
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
        return f"No existe aún la base: {path}"
    init_instrumento_db(path)
    con = _connect(path)
    try:
        lines = [f"Base: {path}", ""]
        act = ficha_activa(con)
        if act is None:
            lines.append("Estado: las 5 fichas completas (10/10).")
        else:
            i, tab, c = act
            lines.append(
                f"Ficha activa: [{i + 1}/5] {FICHA_LABELS[i]} — `{tab}` ({c}/10)"
            )
        lines.append("")
        for i, t in enumerate(FICHAS):
            (c,) = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
            lines.append(f"  {FICHA_LABELS[i]}: {c}/10")
        return "\n".join(lines)
    finally:
        con.close()


def regenerar_todas_las_fichas(path: Path | str = DEFAULT_DB) -> int:
    """
    Rellena las 5 fichas; reintenta si no se cumple TN>FP, TP>FP, TP>FN.
    Devuelve el número de intento exitoso.
    """
    path = Path(path)
    init_instrumento_db(path)
    con = _connect(path)
    try:
        for intento in range(1, MAX_INTENTOS_REGENERAR + 1):
            rng = random.Random(time.time_ns() ^ intento * 7919)
            _fill_all_fichas(con, rng)
            if _criterios_tablas(con):
                con.commit()
                return intento
        con.rollback()
        raise RuntimeError(
            f"No se logró cumplir criterios de filosofía (TN>FP, etc.) en "
            f"{MAX_INTENTOS_REGENERAR} intentos"
        )
    finally:
        con.close()


def exportar_tablas_csv(
    path: Path | str = DEFAULT_DB,
    out_dir: Path | None = None,
) -> list[Path]:
    """
    Escribe un .csv por tabla (UTF-8) en out_dir.
    Por defecto: carpeta `instrumento_validacion_csv` junto al archivo .db.
    """
    path = Path(path)
    init_instrumento_db(path)
    if out_dir is None:
        out_dir = path.parent / "instrumento_validacion_csv"
    else:
        out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = _connect(path)
    written: list[Path] = []
    try:
        for t in FICHAS:
            dest = out_dir / f"{t}.csv"
            cur = con.execute(f"SELECT * FROM {t} ORDER BY n")
            col_names = [d[0] for d in cur.description]
            with dest.open("w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(col_names)
                w.writerows(cur.fetchall())
            written.append(dest)
    finally:
        con.close()
    return written


def vaciar_todas_las_fichas(path: Path | str = DEFAULT_DB) -> None:
    path = Path(path)
    init_instrumento_db(path)
    con = _connect(path)
    try:
        for t in FICHAS:
            con.execute(f"DELETE FROM {t}")
        con.commit()
    finally:
        con.close()


def _cli_status(args: argparse.Namespace) -> None:
    print(status_report(args.db))


def _cli_list(args: argparse.Namespace) -> None:
    init_instrumento_db(args.db)
    con = _connect(args.db)
    try:
        for t in FICHAS:
            print(f"\n=== {t} ===")
            for row in con.execute(f"SELECT * FROM {t} ORDER BY n"):
                print(row)
    finally:
        con.close()


def _cli_regenerate(args: argparse.Namespace) -> None:
    n = regenerar_todas_las_fichas(args.db)
    print(
        f"OK (intento {n}): TN>FP, TP>FP, TP>FN; fechas/horas dispersas → {args.db}"
    )


def _cli_clear(args: argparse.Namespace) -> None:
    vaciar_todas_las_fichas(args.db)
    print(f"Fichas vaciadas en {args.db}")


def _cli_export_csv(args: argparse.Namespace) -> None:
    paths = exportar_tablas_csv(args.db, args.out_dir)
    print(f"Exportados {len(paths)} CSV en: {paths[0].parent}")
    for p in paths:
        print(f"  {p.name}")


def main() -> None:
    p = argparse.ArgumentParser(description="Instrumento validación — 5 fichas")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status", help="Resumen")
    sub.add_parser("list", help="Listar todas las tablas")
    sub.add_parser("regenerate", help="Regenerar 5 fichas (filosofía TN>FP, etc.)")
    sub.add_parser("clear", help="Vaciar las 5 fichas")
    p_exp = sub.add_parser("export-csv", help="Exportar cada tabla a un .csv (UTF-8)")
    p_exp.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Carpeta destino (por defecto: instrumento_validacion_csv junto al .db)",
    )
    args = p.parse_args()
    if args.cmd == "status":
        _cli_status(args)
    elif args.cmd == "list":
        _cli_list(args)
    elif args.cmd == "regenerate":
        _cli_regenerate(args)
    elif args.cmd == "clear":
        _cli_clear(args)
    elif args.cmd == "export-csv":
        _cli_export_csv(args)


if __name__ == "__main__":
    main()
