import asyncio
import os

from bleak import BleakScanner
from termcolor import colored

iterations = 0
# Solo alertar una vez por cada anuncio Fall-* distinto por dispositivo (MAC).
last_fall_name_by_address: dict[str, str] = {}

os.system("clear")

print(colored("Worker Fall Detection v0.2", "green"))
print("Roni Bandini - Argentina - Powered by Edge Impulse")
print("")
print("Escaneando...")
print("")


async def scan_loop() -> None:
    global iterations
    while True:
        print("#" + str(iterations))

        devices = await BleakScanner.discover(timeout=2)
        found = 0

        for d in devices:
            name = (d.name or "").strip()
            address = d.address.lower()

            if not name:
                continue

            print(colored(f"    {name}, {address}", "yellow"))

            if not name.startswith("Fall-"):
                # Si deja de anunciar Fall- para esta MAC, permitimos un nuevo evento futuro.
                last_fall_name_by_address.pop(address, None)
                continue

            if last_fall_name_by_address.get(address) == name:
                print(colored("    (mismo anuncio Fall ya notificado para esta MAC; omito)", "cyan"))
                found = 1
                continue

            last_fall_name_by_address[address] = name
            found = 1
            print(colored("Caída detectada", "red"))

        if found == 0:
            print("")
            print(colored("No se detectó caída.", "magenta"))

        iterations += 1
        await asyncio.sleep(1)


asyncio.run(scan_loop())
