# Worker Fall Detection v0.2 (BLE modern)
# Adapted for Raspberry Pi Python 3.13
#
# El nombre local (OK-Smith / Fall-Smith-N) suele ir en la *scan response*.
# BleakScanner.discover() no siempre refleja ese cambio; usamos escaneo continuo
# con detection_callback y advertisement_data.local_name (recomendación Bleak).
#
# OK-<worker> en el wearable = reposo / sin Fall suavizado (no es alerta por sí solo).
# Solo se registra “caída parcial / recuperación” cuando el nombre pasa de Fall-* a OK-*.
# Fall-* = caída confirmada; deduplicación de episodio + reinicio si la MAC no se oyó antes.

import asyncio
import json
import os
import re
import time
import urllib.error
import urllib.request

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


def init_fall_db() -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS FALL (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            worker TEXT
        )
        """
    )
    con.commit()


con = sl.connect("fall.db")
cursor = con.cursor()
init_fall_db()

iterations = 0
# Último nombre BLE visto por MAC.
last_seen_name_by_address: dict[str, str] = {}
last_printed_name_by_address: dict[str, str] = {}
last_skip_log_mono_by_address: dict[str, float] = {}
# Última vez que vimos un ADV de esa MAC (no borrar estado por una ventana sin paquetes).
last_heard_mono_by_address: dict[str, float] = {}
SKIP_LOG_INTERVAL_SEC = 12.0
# Tras N s sin ningún paquete de esa MAC, olvidar estado (evita crecer sin límite).
BTFALL_STALE_MAC_SEC = float(os.environ.get("BTFALL_STALE_MAC_SEC", "300").strip() or "300")

# Debe coincidir con `worker` en fall1.ino (nombre sintético desde manufacturer data).
BTFALL_WORKER = os.environ.get("BTFALL_WORKER", "Smith").strip() or "Smith"
# 1 = imprimir ln vs device.name vs manufacturer_data por paquete
BTFALL_DEBUG_BLE = os.environ.get("BTFALL_DEBUG_BLE", "").strip() in ("1", "true", "yes")

os.system("clear")

print(colored("Worker Fall Detection v0.2", "green"))
print("Roni Bandini - Argentina - Powered by Edge Impulse")
print("")
init_instrumento_db()
print(colored(status_report(), "cyan"))
print("")
print(colored(
    "Escaneo continuo: Fall-* = caída confirmada. OK-* en reposo = ignorado. "
    "Solo Fall→OK (misma MAC) = recuperación / aviso parcial + n8n.",
    "cyan",
))
print(colored(
    f"Nombre worker (MFG BTFall / debe coincidir con fall1.ino): {BTFALL_WORKER}",
    "cyan",
))
print("Scanning...")
print("")

N8N_WEBHOOK_URL = (
    "http://127.0.0.1:5678/webhook/detectar-caidas"
)

FALL_NAME_RE = re.compile(r"-F(\d+)-S(\d+)$")

# Máx. caracteres del cuerpo HTTP al registrar errores (p. ej. 403 de Cloudflare/nginx/n8n).
N8N_ERROR_BODY_MAX_CHARS = 4000

BTFALL_MFG_MAGIC = b"BT"


def _name_from_btfall_mfg(advertisement_data) -> str | None:
    """
    Company 0xFFFF (Arduino): BT + state + ep(lo,hi) + fp + sp — alineado con Fall-<worker>-<ep>-Ffp-Ssp.
    """
    md = advertisement_data.manufacturer_data or {}
    for _cid, blob in md.items():
        if len(blob) >= 7 and blob[:2] == BTFALL_MFG_MAGIC:
            state = blob[2]
            ep = blob[3] | (blob[4] << 8)
            fp, sp = blob[5], blob[6]
            if state == 1:
                return f"Fall-{BTFALL_WORKER}-{ep}-F{fp}-S{sp}"
            return f"OK-{BTFALL_WORKER}"
    return None


def _resolve_ble_name(device, advertisement_data) -> str:
    mfg_name = _name_from_btfall_mfg(advertisement_data)
    if mfg_name:
        return mfg_name
    return _adv_visible_name(device, advertisement_data).strip()


def _adv_visible_name(device, advertisement_data) -> str:
    """
    Nombre BLE para este evento. En Linux/BlueZ, device.name a menudo queda en OK-* viejo
    mientras local_name ya trae Fall-* (o al revés). Reglas:
    - Si local_name trae Fall, usarlo.
    - Si local_name es OK-*, confiar en eso (no dejar que un Fall cacheado en device.name gane).
    - Si no hay local_name pero device.name tiene Fall, usarlo (evento sin nombre en este AD).
    """
    ln = (advertisement_data.local_name or "").strip()
    dn = (device.name or "").strip()
    if ln and "Fall" in ln:
        return ln
    if ln and ln.startswith("OK-"):
        return ln
    if ln:
        return ln
    if dn and "Fall" in dn:
        return dn
    return dn


def _print_n8n_http_error(exc: BaseException, label: str) -> None:
    """Imprime el fallo completo: HTTPError incluye código, URL y cuerpo de respuesta."""
    print(colored(f"    (error n8n webhook {label}: {exc!r})", "red"))
    if isinstance(exc, urllib.error.HTTPError):
        url = getattr(exc, "url", "") or "(url desconocida)"
        print(colored(f"    HTTP {exc.code} {exc.reason} — URL: {url}", "red"))
        for hdr in ("Content-Type", "WWW-Authenticate", "Server", "NEL", "cf-mitigated"):
            if exc.headers and hdr in exc.headers:
                print(colored(f"    {hdr}: {exc.headers[hdr]}", "red"))
        try:
            raw = exc.read()
            body = raw.decode("utf-8", errors="replace").strip()
        except Exception as read_err:  # noqa: BLE001 — queremos ver cualquier fallo al leer
            print(colored(f"    (no se pudo leer el body: {read_err!r})", "red"))
            return
        if not body:
            print(colored("    body: (vacío)", "red"))
            return
        if len(body) > N8N_ERROR_BODY_MAX_CHARS:
            body = body[: N8N_ERROR_BODY_MAX_CHARS] + "…"
        # Una línea por prefijo para que no se pierda en terminales estrechos
        print(colored("    body completo:", "red"))
        for line in body.splitlines():
            print(colored(f"    | {line}", "red"))
    elif isinstance(exc, urllib.error.URLError) and exc.reason is not None:
        print(colored(f"    motivo: {exc.reason!r}", "red"))


def _extract_scores(name: str) -> tuple[int | None, int | None]:
    m = FALL_NAME_RE.search(name)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def send_n8n_fall_webhook(
    name: str,
    address: str,
    fall_pct: int | None,
    stand_pct: int | None,
) -> None:
    """Envía JSON a n8n para WhatsApp / automatización (mensaje no técnico)."""
    if fall_pct is not None and stand_pct is not None:
        mensaje = (
            "Se detectó una posible caída. El modelo indica "
            f"{fall_pct}% de probabilidad de caída y {stand_pct}% de estar de pie."
        )
    else:
        mensaje = (
            "Se detectó una posible caída según el wearable. "
            "Revisa el estado de la persona de inmediato."
        )

    payload = {
        "event": "fall_detected",
        "source": "raspberry_pi_btfall",
        "ts": int(time.time()),
        "device": {
            "name": name,
            "address": address,
            "fall_pct": fall_pct,
            "stand_pct": stand_pct,
        },
        "details": {
            "mensaje": mensaje,
            "tipo": "Alerta de seguridad",
            "causa_probable": "Señal compatible con caída o impacto brusco (sensor de movimiento).",
            "accion_sugerida": "Contactar al trabajador y verificar si necesita ayuda.",
            "url_revisada": "Panel de monitoreo BTFall (Raspberry)",
        },
    }

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        N8N_WEBHOOK_URL,
        data=data,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            code = getattr(resp, "status", None) or resp.getcode()
            print(colored(f"    (n8n webhook OK: HTTP {code})", "green"))
    except urllib.error.URLError as exc:
        _print_n8n_http_error(exc, "fall")


def send_n8n_ok_partial_webhook(name: str, address: str) -> None:
    """Webhook tras transición Fall*→OK-* (recuperación / fin de alarma en el wearable)."""
    payload = {
        "event": "partial_fall_detected",
        "source": "raspberry_pi_btfall",
        "ts": int(time.time()),
        "device": {
            "name": name,
            "address": address,
            "fall_pct": None,
            "stand_pct": None,
        },
        "details": {
            "mensaje": (
                "El wearable pasó de señal Fall a OK (recuperación o fin de episodio). "
                "Conviene verificar el estado de la persona."
            ),
            "tipo": "Seguimiento post-alerta",
            "causa_probable": "Transición BLE Fall→OK tras clasificación en el dispositivo.",
            "accion_sugerida": "Confirmar con el trabajador que se encuentra bien.",
            "url_revisada": "Panel de monitoreo BTFall (Raspberry)",
        },
    }

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        N8N_WEBHOOK_URL,
        data=data,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            code = getattr(resp, "status", None) or resp.getcode()
            print(colored(f"    (n8n webhook OK parcial: HTTP {code})", "green"))
    except urllib.error.URLError as exc:
        _print_n8n_http_error(exc, "parcial")


def _print_name_if_changed(address: str, name: str) -> None:
    if last_printed_name_by_address.get(address) == name:
        return
    last_printed_name_by_address[address] = name
    fall_pct, stand_pct = _extract_scores(name)
    if fall_pct is not None and stand_pct is not None:
        print(colored(f"    {name}, {address} (F:{fall_pct}% S:{stand_pct}%)", "yellow"))
    else:
        print(colored(f"    {name}, {address}", "yellow"))


def _purge_stale_ble_addresses() -> None:
    """No confundir con ventanas de 2 s sin ADV: antes se borraba el estado y se repetían alertas."""
    now = time.monotonic()
    for addr in list(last_seen_name_by_address.keys()):
        if now - last_heard_mono_by_address.get(addr, 0) > BTFALL_STALE_MAC_SEC:
            last_seen_name_by_address.pop(addr, None)
            last_printed_name_by_address.pop(addr, None)
            last_skip_log_mono_by_address.pop(addr, None)
            last_heard_mono_by_address.pop(addr, None)


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
    inserted = len(records) == 0

    if inserted:
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
    else:
        print(colored("This fall was already in the database", "green"))

    # n8n: caída solo al insertar fila nueva (evita spam si el mismo Fall-* se re-registra por error).
    if name.startswith("Fall"):
        if inserted:
            f_pct, s_pct = _extract_scores(name)
            await asyncio.to_thread(
                send_n8n_fall_webhook,
                name,
                address,
                f_pct,
                s_pct,
            )
    elif name.startswith("OK-"):
        await asyncio.to_thread(
            send_n8n_ok_partial_webhook,
            name,
            address,
        )

    if inserted:
        await asyncio.sleep(5)


async def process_adv_packet(address: str, name: str) -> int:
    """
    Devuelve 1 si en este paquete hubo un evento registrable (Fall u OK parcial).
    """
    last_heard_mono_by_address[address] = time.monotonic()
    _print_name_if_changed(address, name)

    if "Fall" in name:
        prev = last_seen_name_by_address.get(address)
        last_seen_name_by_address[address] = name
        new_event = prev is None or ("Fall" not in prev) or (prev != name)
        if not new_event:
            _maybe_skip_log(address)
            return 1
        fall_pct, stand_pct = _extract_scores(name)
        if fall_pct is not None and stand_pct is not None:
            title = f"Fall detected (modelo: F={fall_pct}% S={stand_pct}%)"
        else:
            title = "Fall detected"
        await _register_detection_event(name, address, title)
        return 1

    if name.startswith("OK-"):
        prev = last_seen_name_by_address.get(address)
        last_seen_name_by_address[address] = name
        # OK-* solo anuncia “neutral” en el Arduino; no es caída mientras no venga tras Fall-*.
        if prev is not None and ("Fall" in prev):
            await _register_detection_event(
                name,
                address,
                "Recuperación / posible caída parcial (transición Fall→OK en el wearable)",
            )
            return 1
        return 0

    last_seen_name_by_address[address] = name
    return 0


async def scan_loop() -> None:
    global iterations

    loop = asyncio.get_running_loop()
    adv_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue(maxsize=1000)

    def detection_callback(device, advertisement_data) -> None:
        ln_raw = (advertisement_data.local_name or "").strip()
        dn_raw = (device.name or "").strip()
        mfg_raw = dict(advertisement_data.manufacturer_data or {})
        name = _resolve_ble_name(device, advertisement_data)
        if BTFALL_DEBUG_BLE and (ln_raw or dn_raw or mfg_raw or name):
            print(
                colored(
                    f"    [BLE dbg] mfg={mfg_raw!r} ln={ln_raw!r} dn={dn_raw!r} → usado={name!r}",
                    "yellow",
                )
            )
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

            _purge_stale_ble_addresses()

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
                hit = await process_adv_packet(addr, pkt_name)
                found = max(found, hit)

            if found == 0:
                print("")
                print("No falls detected")

            iterations += 1
            await asyncio.sleep(1)


asyncio.run(scan_loop())
