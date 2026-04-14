import asyncio
import os
import time

from bleak import BleakScanner
from termcolor import colored

iterations = 0

# Filtros para reducir falsos positivos por ruido de RSSI / anuncios intermitentes.
MIN_RSSI = -75
CONFIRMATIONS_REQUIRED = 0
ALERT_COOLDOWN_SECONDS = 8
TARGET_NAME_TOKEN = "Smith"
TARGET_ADDRESS: str | None = None

# Estado por MAC para deduplicar y confirmar eventos.
consecutive_fall_seen: dict[str, int] = {}
last_fall_name_by_address: dict[str, str] = {}
last_alert_ts_by_address: dict[str, float] = {}
known_target_address: str | None = None

os.system("clear")

print(colored("Worker Fall Detection v0.2", "green"))
print("Roni Bandini - Argentina - Powered by Edge Impulse")
print("")
print("Escaneando...")
print("")


async def scan_loop() -> None:
    global iterations
    global known_target_address
    while True:
        print("#" + str(iterations))

        devices = await BleakScanner.discover(timeout=2)
        found = 0

        for d in devices:
            name = (d.name or "").strip()
            address = d.address.lower()
            # Compatibilidad entre versiones de bleak: no siempre BLEDevice trae .rssi
            rssi = getattr(d, "rssi", None)

            if not name:
                continue

            print(colored(f"    {name}, {address}, RSSI {rssi}", "yellow"))

            # 1) Si se configuro una MAC, solo escuchamos esa.
            if TARGET_ADDRESS and address != TARGET_ADDRESS.lower():
                continue

            # 2) Si no hay MAC fija, aprendemos la MAC del wearable por su nombre (OK-Smith/Fall-Smith).
            if known_target_address is None:
                if TARGET_NAME_TOKEN and TARGET_NAME_TOKEN not in name:
                    continue
                known_target_address = address
                print(colored(f"    (dispositivo objetivo fijado en {known_target_address})", "cyan"))

            # 3) Ignorar otros dispositivos para evitar falsos positivos.
            if known_target_address and address != known_target_address:
                continue

            if rssi is not None and rssi < MIN_RSSI:
                # Señal muy débil: suele traer anuncios ruidosos de otros equipos lejanos.
                consecutive_fall_seen.pop(address, None)
                continue

            if not name.startswith("Fall-"):
                # Si deja de anunciar Fall- para esta MAC, permitimos un nuevo evento futuro.
                consecutive_fall_seen.pop(address, None)
                last_fall_name_by_address.pop(address, None)
                continue

            consecutive_fall_seen[address] = consecutive_fall_seen.get(address, 0) + 1
            if consecutive_fall_seen[address] < CONFIRMATIONS_REQUIRED:
                print(colored("    (Fall visto, esperando confirmacion...)", "cyan"))
                continue

            now = time.time()
            if (now - last_alert_ts_by_address.get(address, 0)) < ALERT_COOLDOWN_SECONDS:
                print(colored("    (en cooldown; omito alerta repetida)", "cyan"))
                found = 1
                continue

            if last_fall_name_by_address.get(address) == name:
                print(colored("    (mismo anuncio Fall ya notificado para esta MAC; omito)", "cyan"))
                found = 1
                continue

            last_fall_name_by_address[address] = name
            last_alert_ts_by_address[address] = now
            found = 1
            print(colored("Caída detectada", "red"))

        if found == 0:
            print("")
            print(colored("No se detectó caída.", "magenta"))

        iterations += 1
        await asyncio.sleep(1)


asyncio.run(scan_loop())
