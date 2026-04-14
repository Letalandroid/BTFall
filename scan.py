# Worker Fall Detection v0.2 (BLE modern)
# Adapted for Raspberry Pi Python 3.13
#
# El nombre local (OK-Smith / Fall-Smith-N) suele ir en la *scan response*.
# BleakScanner.discover() no siempre refleja ese cambio; usamos escaneo continuo
# con detection_callback y advertisement_data.local_name (recomendación Bleak).
#
# Criterio de estudio: OK-<worker> = caída parcial. Se registra una vez por
# ciclo impreso (#N) y por MAC si en ese ciclo llega al menos un anuncio OK-*
# (no se omite por “mismo episodio” entre ciclos). Fall-* sigue con deduplicación
# lógica de episodio + reinicio si la MAC no se oyó en el ciclo anterior.

import asyncio
import os
import time

from bleak import BleakScanner
from termcolor import colored
import sqlite3 as sl

# El módulo `instrumento_recoleccion` ya no está en el repo (commit de limpieza).
# Stubs mínimos para que el escáner BLE y SQLite sigan funcionando.
FICHAS: tuple[str, ...] = ()


def init_instrumento_db() -> None:
    pass


def status_report() -> str:
    return "Instrumento de recolección: no disponible (solo BLE + fall.db)."


def ficha_counts() -> dict[str, int]:
    return {}


def record_fall_event(name: str, address: str) -> dict:
    return {
        "ok": False,
        "mensaje": "Registro solo en fall.db (instrumento de estudio no cargado).",
    }

con = sl.connect("fall.db")
cursor = con.cursor()

iterations = 0
# Último nombre BLE visto por MAC.
last_seen_name_by_address: dict[str, str] = {}
last_printed_name_by_address: dict[str, str] = {}
last_skip_log_mono_by_address: dict[str, float] = {}
SKIP_LOG_INTERVAL_SEC = 12.0
# Último #N en el que ya se registró OK parcial por MAC (solo evita duplicar
# dentro del mismo ciclo de 2 s, no entre #119 y #120).
ok_partial_registered_cycle_by_address: dict[str, int] = {}

os.system("clear")

print(colored("Worker Fall Detection v0.2", "green"))
print("Roni Bandini - Argentina - Powered by Edge Impulse")
print("")
init_instrumento_db()
print(colored(status_report(), "cyan"))
print("")
print(colored(
    "Escaneo continuo: Fall-* = caída confirmada; OK-* = caída parcial (1 registro "
    "por ciclo #N y dispositivo si hay anuncios OK). Fall reutiliza episodio hasta "
    "cambio de nombre o corte de radio.",
    "cyan",
))
print("Scanning...")
print("")


def _print_name_if_changed(address: str, name: str) -> None:
    if last_printed_name_by_address.get(address) == name:
        return
    last_printed_name_by_address[address] = name
    print(colored(f"    {name}, {address}", "yellow"))


def _maybe_skip_log(address: str) -> None:
    now = time.monotonic()
    if now - last_skip_log_mono_by_address.get(address, 0) < SKIP_LOG_INTERVAL_SEC:
        return
    last_skip_log_mono_by_address[address] = now
    msg = "    (mismo episodio Fall: sin OK intermedio ni cambio de nombre; omito)"
    print(colored(msg, "cyan"))


async def _register_detection_event(name: str, address: str, title: str) -> None:
    print(colored(title, "red"))

    inst = record_fall_event(name, address)
    if inst.get("ok"):
        counts = ficha_counts()
        metric_keys = (
            "fp", "tp", "p", "fn", "s", "tn", "e", "ta", "te", "l",
            "metros", "segundos", "u",
        )
        detalles = ", ".join(
            f"{k}={inst[k]}"
            for k in metric_keys
            if k in inst and inst[k] is not None
        )
        print(
            colored(
                "    Instrumento: "
                f"{inst.get('ficha_etiqueta', '')} — fila {inst['n_en_instrumento']}/10 — "
                f"N° persona {inst['n_persona']}"
                + (f" — {detalles}" if detalles else ""),
                "magenta",
            )
        )
        print(
            colored(
                "    SQLite instrumento: "
                + ", ".join(f"{t}={counts[t]}" for t in FICHAS),
                "magenta",
            )
        )
    else:
        print(colored("    " + inst["mensaje"], "yellow"))

    sql = "SELECT * from FALL WHERE name='" + name + "'"
    print(sql)

    cursor.execute(sql)
    records = cursor.fetchall()

    if len(records) == 0:
        print("Adding record: " + name)
        parts = name.split("-")
        worker = parts[1] if len(parts) >= 2 else "?"
        sql_ins = (
            "INSERT INTO FALL (name, worker) values ('"
            + name
            + "','"
            + worker
            + "')"
        )
        with con:
            con.execute(sql_ins)
        await asyncio.sleep(5)
    else:
        print(colored("This fall was already in the database", "green"))


async def process_adv_packet(address: str, name: str, scan_cycle: int) -> int:
    """
    Devuelve 1 si en este paquete hubo un evento registrable (Fall u OK parcial).
    scan_cycle es el número del #N actual (no incrementa hasta terminar la ventana).
    """
    _print_name_if_changed(address, name)

    if "Fall" in name:
        prev = last_seen_name_by_address.get(address)
        last_seen_name_by_address[address] = name
        new_event = prev is None or ("Fall" not in prev) or (prev != name)
        if not new_event:
            _maybe_skip_log(address)
            return 1
        await _register_detection_event(name, address, "Fall detected")
        return 1

    if name.startswith("OK-"):
        last_seen_name_by_address[address] = name
        if ok_partial_registered_cycle_by_address.get(address) == scan_cycle:
            return 0
        ok_partial_registered_cycle_by_address[address] = scan_cycle
        await _register_detection_event(
            name,
            address,
            "Caída parcial detectada (OK-…, wearable sin Fall suavizado)",
        )
        return 1

    last_seen_name_by_address[address] = name
    return 0


async def scan_loop() -> None:
    global iterations

    loop = asyncio.get_running_loop()
    adv_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue(maxsize=1000)
    heard_previous: set[str] | None = None

    def detection_callback(device, advertisement_data) -> None:
        name = advertisement_data.local_name or device.name or ""
        name = name.strip()
        if not name:
            return
        address = device.address.lower()

        def _enqueue() -> None:
            try:
                adv_queue.put_nowait((address, name))
            except asyncio.QueueFull:
                pass

        loop.call_soon_threadsafe(_enqueue)

    async with BleakScanner(
        detection_callback=detection_callback,
        scanning_mode="active",
    ):
        while True:
            print("#" + str(iterations))

            if heard_previous is not None:
                for addr in list(last_seen_name_by_address.keys()):
                    if addr not in heard_previous:
                        last_seen_name_by_address.pop(addr, None)
                        last_printed_name_by_address.pop(addr, None)
                        last_skip_log_mono_by_address.pop(addr, None)
                        ok_partial_registered_cycle_by_address.pop(addr, None)

            found = 0
            heard_this: set[str] = set()
            window_end = time.monotonic() + 2.0

            while time.monotonic() < window_end:
                try:
                    addr, pkt_name = await asyncio.wait_for(
                        adv_queue.get(),
                        timeout=0.35,
                    )
                except asyncio.TimeoutError:
                    continue
                heard_this.add(addr)
                hit = await process_adv_packet(addr, pkt_name, iterations)
                found = max(found, hit)

            heard_previous = set(heard_this)

            if found == 0:
                print("")
                print("No falls detected")

            iterations += 1
            await asyncio.sleep(1)


asyncio.run(scan_loop())
