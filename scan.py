# Worker Fall Detection v0.2 (BLE modern)
# Adapted for Raspberry Pi Python 3.13
#
# El nombre local (OK-Smith / Fall-Smith-N) suele ir en la *scan response*.
# BleakScanner.discover() no siempre refleja ese cambio; usamos escaneo continuo
# con detection_callback y advertisement_data.local_name (recomendación Bleak).

import asyncio
import os
import time

from bleak import BleakScanner
from termcolor import colored
import sqlite3 as sl

from instrumento_recoleccion import (
    FICHAS,
    ficha_counts,
    init_instrumento_db,
    record_fall_event,
    status_report,
)

con = sl.connect("fall.db")
cursor = con.cursor()

iterations = 0
# Último nombre BLE visto por MAC (OK-… o Fall-…). Nuevo evento de caída =
# pasar de “no Fall” a Fall, o cambiar el texto Fall-* (nuevo contador en el Arduino).
last_seen_name_by_address: dict[str, str] = {}
# Evita spamear la misma línea en cada anuncio BLE.
last_printed_name_by_address: dict[str, str] = {}
# Mensaje “omitido” como mucho cada SKIP_LOG_INTERVAL s por dispositivo.
last_skip_log_mono_by_address: dict[str, float] = {}
SKIP_LOG_INTERVAL_SEC = 12.0

os.system("clear")

print(colored("Worker Fall Detection v0.2", "green"))
print("Roni Bandini - Argentina - Powered by Edge Impulse")
print("")
init_instrumento_db()
print(colored(status_report(), "cyan"))
print("")
print(colored(
    "Escaneo continuo (active + callback). Si solo ves OK-… al caerte, el Arduino "
    "no está llegando a prediction==Fall suavizado o revisa el monitor serie.",
    "cyan",
))
print("Scanning...")
print("")


def _print_name_if_changed(address: str, name: str) -> None:
    if last_printed_name_by_address.get(address) == name:
        return
    last_printed_name_by_address[address] = name
    print(colored(f"    {name}, {address}", "yellow"))


async def process_adv_packet(address: str, name: str) -> int:
    """
    Procesa un anuncio con nombre conocido. Devuelve 1 si hubo detección Fall
    relevante en este paquete, 0 si no.
    """
    _print_name_if_changed(address, name)

    if "Fall" not in name:
        last_seen_name_by_address[address] = name
        return 0

    prev = last_seen_name_by_address.get(address)
    last_seen_name_by_address[address] = name

    new_fall_event = prev is None or ("Fall" not in prev) or (prev != name)

    if not new_fall_event:
        now = time.monotonic()
        if now - last_skip_log_mono_by_address.get(address, 0) >= SKIP_LOG_INTERVAL_SEC:
            last_skip_log_mono_by_address[address] = now
            print(
                colored(
                    "    (mismo episodio Fall: sin OK intermedio ni cambio de nombre; omito)",
                    "cyan",
                )
            )
        return 1

    print(colored("Fall detected", "red"))

    inst = record_fall_event(name, address)
    if inst.get("ok"):
        counts = ficha_counts()
        print(
            colored(
                "    Instrumento Anexo 2: fila "
                f"{inst['n_en_instrumento']}/10 — {inst['ficha_etiqueta']} — "
                f"N° persona {inst['n_persona']} (row id {inst['row_id']})",
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

    name_db = name

    sql = "SELECT * from FALL WHERE name='" + name_db + "'"
    print(sql)

    cursor.execute(sql)
    records = cursor.fetchall()

    if len(records) == 0:

        print("Adding record: " + name)

        field_array = name.split("-")

        sql = (
            "INSERT INTO FALL (name, worker) values ('"
            + name
            + "','"
            + field_array[1]
            + "')"
        )

        with con:
            con.execute(sql)

        await asyncio.sleep(5)

    else:
        print(colored("This fall was already in the database", "green"))

    return 1


async def scan_loop() -> None:
    global iterations

    loop = asyncio.get_running_loop()
    adv_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue(maxsize=1000)

    def detection_callback(device, advertisement_data) -> None:
        # Scan response → local_name; sin esto Bleak a menudo deja el nombre viejo.
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

            found = 0
            window_end = time.monotonic() + 2.0

            while time.monotonic() < window_end:
                try:
                    addr, pkt_name = await asyncio.wait_for(
                        adv_queue.get(),
                        timeout=0.35,
                    )
                except asyncio.TimeoutError:
                    continue
                hit = await process_adv_packet(addr, pkt_name)
                found = max(found, hit)

            if found == 0:
                print("")
                print("No falls detected")

            iterations += 1
            await asyncio.sleep(1)


asyncio.run(scan_loop())
