# Worker Fall Detection v0.2 (BLE modern)
# Adapted for Raspberry Pi Python 3.13

import asyncio
from bleak import BleakScanner
from termcolor import colored
import sqlite3 as sl
import os
import time

from instrumento_recoleccion import init_instrumento_db, record_fall_event, status_report

con = sl.connect('fall.db')
cursor = con.cursor()

found = 0
iterations = 0
# Último nombre BLE visto por MAC (OK-… o Fall-…). Nuevo evento de caída =
# pasar de “no Fall” a Fall, o cambiar el texto Fall-* (nuevo contador en el Arduino).
last_seen_name_by_address = {}

os.system('clear')

print(colored('Worker Fall Detection v0.2', 'green'))
print("Roni Bandini - Argentina - Powered by Edge Impulse")
print("")
init_instrumento_db()
print(colored(status_report(), "cyan"))
print("")
print("Scanning...")

async def scan_loop():
    global found
    global iterations

    while True:

        print("#" + str(iterations))

        devices = await BleakScanner.discover(timeout=2)
        found = 0

        for d in devices:

            name = d.name
            address = d.address

            if not name:
                continue

            print(colored(f"    {name}, {address}", 'yellow'))

            if "Fall" not in name:
                last_seen_name_by_address[address] = name
                continue

            prev = last_seen_name_by_address.get(address)
            last_seen_name_by_address[address] = name

            new_fall_event = (
                prev is None
                or ("Fall" not in prev)
                or (prev != name)
            )

            if not new_fall_event:
                print(colored(
                    "    (mismo episodio Fall: sin OK intermedio ni cambio de nombre; omito)",
                    "cyan",
                ))
                found = 1
                continue

            found = 1
            print(colored('Fall detected', 'red'))

            nameDb = name

            inst = record_fall_event(nameDb, address)
            if inst.get("ok"):
                print(
                    colored(
                        "    Instrumento Anexo 2: fila "
                        f"{inst['n_en_instrumento']}/10 — {inst['ficha_etiqueta']} — "
                        f"N° persona {inst['n_persona']} (row id {inst['row_id']})",
                        "magenta",
                    )
                )
            else:
                print(colored("    " + inst["mensaje"], "yellow"))

            sql = "SELECT * from FALL WHERE name='" + nameDb + "'"
            print(sql)

            cursor.execute(sql)
            records = cursor.fetchall()

            if len(records) == 0:

                print('Adding record: ' + name)

                fieldArray = name.split("-")

                sql = "INSERT INTO FALL (name, worker) values ('" + name + "','" + fieldArray[1] + "')"

                with con:
                    con.execute(sql)

                await asyncio.sleep(5)

            else:
                print(colored('This fall was already in the database', 'green'))

        if found == 0:
            print("")
            print("No falls detected")

        iterations += 1

        await asyncio.sleep(1)


asyncio.run(scan_loop())
