import asyncio
import json
import os
import sqlite3
import time
import urllib.error
import urllib.request

from bleak import BleakScanner
from termcolor import colored

iterations = 0

# Filtros para reducir falsos positivos por ruido de RSSI / anuncios intermitentes.
MIN_RSSI = -75
CONFIRMATIONS_REQUIRED = 1
ALERT_COOLDOWN_SECONDS = 2
ALLOW_REPEAT_SAME_NAME_AFTER_SECONDS = 2
TARGET_NAME_TOKEN = "Smith"
TARGET_ADDRESS: str | None = None
DB_PATH = "fall_events.db"
VERBOSE_DEVICE_LOGS = False
SUMMARY_INTERVAL_SECONDS = 60
WEBHOOK_URL = "https://n8n.federico-system-inventary.space/webhook-test/detectar-caidas"

# Estado por MAC para deduplicar y confirmar eventos.
consecutive_fall_seen: dict[str, int] = {}
last_fall_name_by_address: dict[str, str] = {}
last_alert_ts_by_address: dict[str, float] = {}
known_target_address: str | None = None
stats = {
    "loops": 0,
    "seen_target_packets": 0,
    "neutral_packets": 0,
    "fall_packets": 0,
    "ignored_weak_signal": 0,
    "ignored_cooldown": 0,
    "ignored_duplicate": 0,
    "saved_events": 0,
    "rssi_sum": 0,
    "rssi_count": 0,
}
last_summary_ts = time.time()


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fall_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                address TEXT NOT NULL,
                name TEXT NOT NULL,
                rssi INTEGER
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def save_fall_event(address: str, name: str, rssi: int | None) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT INTO fall_events (ts, address, name, rssi) VALUES (?, ?, ?, ?)",
            (int(time.time()), address, name, rssi),
        )
        conn.commit()
    finally:
        conn.close()


def send_fall_webhook(address: str, name: str, rssi: int | None) -> None:
    # Texto corto y no tecnico para disparar mensajes de WhatsApp.
    payload = {
        "event": "fall_detected",
        "source": "raspberry_pi_btfall",
        "ts": int(time.time()),
        "device": {
            "name": name,
            "address": address,
            "rssi": rssi,
        },
        "details": {
            "mensaje": "Se detecto una posible caida del trabajador. Revisar su estado de inmediato.",
            "tipo": "Alerta de seguridad",
            "causa_probable": "Movimiento brusco compatible con caida",
            "accion_sugerida": "Contactar al trabajador y validar si requiere asistencia",
            "url_revisada": "Panel de monitoreo BTFall",
        },
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        WEBHOOK_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            status = getattr(resp, "status", None)
            print(colored(f"    (webhook enviado: {status})", "green"))
    except urllib.error.URLError as exc:
        print(colored(f"    (error enviando webhook: {exc})", "red"))

os.system("clear")

print(colored("Worker Fall Detection v0.2", "green"))
print("Roni Bandini - Argentina - Powered by Edge Impulse")
print("")
print("Escaneando...")
print("")


async def scan_loop() -> None:
    global iterations
    global known_target_address
    global last_summary_ts
    while True:
        print("#" + str(iterations))
        stats["loops"] += 1

        devices = await BleakScanner.discover(timeout=2, return_adv=True)
        found = 0

        for _addr, (device, adv) in devices.items():
            # En Linux/Bleak suele venir mejor por AdvertisementData que por BLEDevice.
            name = ((adv.local_name or device.name) or "").strip()
            address = device.address.lower()
            rssi = getattr(adv, "rssi", None)

            if not name:
                continue

            if VERBOSE_DEVICE_LOGS:
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

            stats["seen_target_packets"] += 1
            if rssi is not None:
                stats["rssi_sum"] += rssi
                stats["rssi_count"] += 1

            if rssi is not None and rssi < MIN_RSSI:
                # Señal muy débil: suele traer anuncios ruidosos de otros equipos lejanos.
                consecutive_fall_seen.pop(address, None)
                stats["ignored_weak_signal"] += 1
                continue

            if not name.startswith("Fall-"):
                # Si deja de anunciar Fall- para esta MAC, permitimos un nuevo evento futuro.
                consecutive_fall_seen.pop(address, None)
                last_fall_name_by_address.pop(address, None)
                stats["neutral_packets"] += 1
                continue

            stats["fall_packets"] += 1
            consecutive_fall_seen[address] = consecutive_fall_seen.get(address, 0) + 1
            if consecutive_fall_seen[address] < CONFIRMATIONS_REQUIRED:
                print(colored("    (Fall visto, esperando confirmacion...)", "cyan"))
                continue

            now = time.time()
            if (now - last_alert_ts_by_address.get(address, 0)) < ALERT_COOLDOWN_SECONDS:
                print(colored("    (en cooldown; omito alerta repetida)", "cyan"))
                stats["ignored_cooldown"] += 1
                found = 1
                continue

            if last_fall_name_by_address.get(address) == name:
                same_name_age = now - last_alert_ts_by_address.get(address, 0)
                if same_name_age < ALLOW_REPEAT_SAME_NAME_AFTER_SECONDS:
                    print(colored("    (mismo anuncio Fall muy seguido; omito)", "cyan"))
                    stats["ignored_duplicate"] += 1
                    found = 1
                    continue
                print(colored("    (mismo anuncio Fall pero pasado el umbral; lo registro)", "cyan"))

            last_fall_name_by_address[address] = name
            last_alert_ts_by_address[address] = now
            found = 1
            print(colored(f"Caída detectada -> {name} ({address}) RSSI {rssi}", "red"))
            save_fall_event(address, name, rssi)
            send_fall_webhook(address, name, rssi)
            stats["saved_events"] += 1
            print(colored(f"    (guardado en DB: {DB_PATH})", "green"))

        if found == 0:
            print("")
            print(colored("No se detectó caída.", "magenta"))

        now = time.time()
        if (now - last_summary_ts) >= SUMMARY_INTERVAL_SECONDS:
            avg_rssi = None
            if stats["rssi_count"] > 0:
                avg_rssi = round(stats["rssi_sum"] / stats["rssi_count"], 1)
            print("")
            print(
                colored(
                    (
                        "[RESUMEN] "
                        f"loops={stats['loops']} "
                        f"target_pkts={stats['seen_target_packets']} "
                        f"ok={stats['neutral_packets']} "
                        f"fall={stats['fall_packets']} "
                        f"guardados={stats['saved_events']} "
                        f"ign_weak={stats['ignored_weak_signal']} "
                        f"ign_cd={stats['ignored_cooldown']} "
                        f"ign_dup={stats['ignored_duplicate']} "
                        f"rssi_avg={avg_rssi}"
                    ),
                    "blue",
                )
            )
            last_summary_ts = now

        iterations += 1
        await asyncio.sleep(1)


init_db()
asyncio.run(scan_loop())
