#!/Users/KriKri/gf-venv/bin/python
"""Konsistente Grainfather-Firmware für dein Lobster-Lager-Rezept.

Das Script verbindet sich per BLE, folgt Schritt für Schritt deinem Prozess,
spielt akustische Hinweise (Mac Glass.aiff) und wartet bei kritischen
Zeitpunkten auf Tastaturbestätigungen. Während jeder Phase kannst du
'pause/continue', 'skip' oder 'stop' über die Konsole eingeben.
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import json
import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict

try:
    from bleak import BleakClient, BleakScanner
    from bleak.exc import BleakDeviceNotFoundError, BleakError
except ImportError:
    BleakClient = None  # type: ignore
    BleakScanner = None  # type: ignore
    BleakDeviceNotFoundError = BleakError = Exception  # type: ignore

ADDRESS = "4B94A369-C146-CBE4-35BB-258575D08512"
NOTIFY_UUID = "0003cdd1-0000-1000-8000-00805f9b0131"
WRITE_UUID = "0003cdd2-0000-1000-8000-00805f9b0131"
SOUND = "/System/Library/Sounds/Glass.aiff"
TEMP_TOLERANCE = 0.5
POLL_INTERVAL = 1.0
DEFAULT_DEVICE_NAME = "Grainfather"
SCAN_TIMEOUT = 6.0
CONNECT_RETRIES = 3
USE_DEVICE_TIMER = os.getenv("GF_USE_DEVICE_TIMER", "0").strip() == "1"

state: Dict[str, list[str] | None] = {"X": None, "T": None, "Y": None, "W": None}

COMMAND_QUEUE: "InputQueue"
CONTROL_STATE: Dict[str, bool]

STATUS_DIR = Path("status")
STATUS_PATH = STATUS_DIR / "brew_status.json"
STATUS: Dict[str, Any] = {
    "step": None,
    "next_step": None,
    "current_temp": None,
    "target_temp": None,
    "timer_label": None,
    "timer_remaining": None,
    "timer_duration": None,
    "timer_active": False,
    "pump": None,
    "heat": None,
    "power_pct": None,
    "state": "idle",
    "hop_event": None,
    "next_hop": None,
    "next_hop_remaining": None,
    "last_message": None,
    "recipe": None,
    "x_raw": [],
    "ble_connected": False,
    "hop_acks": {},
    "hop_acks_updated_at": None,
}

PUMP_STATE = False
HEAT_STATE = False
DESIRED_PUMP = False
DESIRED_HEAT = False
ACK_WAITING = False
LAST_BLE_ACK_AT = 0.0
LAST_BLE_ACK_PAYLOAD = ""


def choose_temperatures(values: list[float | None]) -> tuple[float | None, float | None]:
    prev_current = STATUS.get("current_temp")
    prev_target = STATUS.get("target_temp")
    if not values:
        return None, None
    combos: list[tuple[float | None, float | None]] = []
    if len(values) == 1:
        combos = [(values[0], None), (None, values[0])]
    else:
        combos = [
            (values[1], values[0]),
            (values[0], values[1]),
        ]
    best: tuple[float | None, float | None] = (None, None)
    best_score = float("inf")

    def score_pair(current: float | None, target: float | None) -> float:
        score = 0.0
        if prev_current is not None:
            if current is not None:
                score += abs(current - prev_current)
            else:
                score += 2.5
        if prev_target is not None:
            if target is not None:
                score += abs(target - prev_target)
            else:
                score += 2.5
        if current is not None and target is not None and current > target:
            score += 0.25
        return score

    for current, target in combos:
        pair_score = score_pair(current, target)
        if pair_score < best_score:
            best_score = pair_score
            best = (current, target)
    return best



HOP_SCHEDULE: list[Dict[str, Any]] = []
COOK_START_TIME: float | None = None

SELECTED_RECIPE_FILE = Path("selected_recipe.json")
DEFAULT_RECIPE_FILE = Path("recipes/test_sud_nr202.json")


class StopExecution(Exception):
    pass


class BackStep(Exception):
    pass


class InputQueue:
    def __init__(self) -> None:
        self._deque = collections.deque()
        self._lock = threading.Lock()

    def put(self, line: str) -> None:
        with self._lock:
            self._deque.append(line)

    def get_nowait(self) -> str:
        with self._lock:
            if not self._deque:
                raise queue.Empty
            return self._deque.popleft()


def start_command_listener() -> None:
    def reader() -> None:
        while True:
            line = sys.stdin.readline()
            if not line:
                break
            COMMAND_QUEUE.put(line.rstrip("\n"))

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()


def print_command_help() -> None:
    print("Interaktive Kommandos:")
    print("  pause (p)   → aktuellen Schritt anhalten")
    print("  resume (r)  → Pause aufheben")
    print("  skip (n)    → Schritt überspringen")
    print("  back (b)    → vorherigen Schritt erneut starten")
    print("  stop (q)    → gesamten Ablauf beenden")
    print("Zusätzlich: Bei Hinweisen einfach ENTER drücken, um sie zu quittieren.")
    print()


def handle(sender: int, data: bytearray) -> None:
    payload = data.decode("ascii", errors="ignore").strip()
    if not payload:
        return
    _log_ble_payload(payload)
    _maybe_ack_from_ble(payload)
    key = payload[0]
    state[key] = payload[1:].split(",")


def _ble_ack_matches() -> list[str]:
    raw = os.getenv("GF_ACK_NOTIFY", "")
    return [item.strip().lower() for item in raw.split(",") if item.strip()]


def _log_ble_payload(payload: str) -> None:
    if os.getenv("GF_NOTIFY_LOG", "").strip().lower() not in ("1", "true", "yes"):
        return
    try:
        log_path = Path("logs/ble_notify.log")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(f"{stamp} | {payload}\n")
    except Exception:
        pass


def _maybe_ack_from_ble(payload: str) -> None:
    global LAST_BLE_ACK_AT, LAST_BLE_ACK_PAYLOAD
    if not ACK_WAITING:
        return
    matches = _ble_ack_matches()
    if not matches:
        return
    lowered = payload.lower()
    if not any(match in lowered for match in matches):
        return
    now = time.monotonic()
    if payload == LAST_BLE_ACK_PAYLOAD and now - LAST_BLE_ACK_AT < 1.0:
        return
    LAST_BLE_ACK_AT = now
    LAST_BLE_ACK_PAYLOAD = payload
    COMMAND_QUEUE.put("")


def safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def summarize() -> Dict[str, Any]:
    x = state.get("X") or []
    t = state.get("T") or []
    y = state.get("Y") or []
    w = state.get("W") or []

    temps = [safe_float(value) for value in x[:2]]
    current, target = choose_temperatures(temps)
    pump = (y[1] == "1") if len(y) > 1 else None
    heat = (y[0] == "1") if len(y) > 0 else None
    power = safe_float(w[0]) if len(w) > 0 else None

    timer_sec = None
    timer_active = False
    if len(t) > 3 and t[3].isdigit():
        timer_sec = int(t[3])
    if len(t) > 2 and any(t[i] == "1" for i in range(3)):
        timer_active = True

    return {
        "target_temp": target,
        "current_temp": current,
        "pump": pump,
        "heat": heat,
        "power_pct": power,
        "timer_active": timer_active,
        "timer_sec": timer_sec,
        "x_raw": x,
    }


def ensure_status_dir() -> None:
    STATUS_DIR.mkdir(parents=True, exist_ok=True)


def status_update(**kwargs: Any) -> None:
    ensure_status_dir()
    if "hop_acks" not in kwargs:
        try:
            if STATUS_PATH.exists():
                existing = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
                hop_acks = existing.get("hop_acks")
                if isinstance(hop_acks, dict):
                    STATUS["hop_acks"] = hop_acks
                if "hop_acks_updated_at" in existing:
                    STATUS["hop_acks_updated_at"] = existing.get("hop_acks_updated_at")
        except Exception:
            pass
    STATUS.update(kwargs)
    STATUS["timestamp"] = time.time()
    STATUS_PATH.write_text(json.dumps(STATUS, ensure_ascii=False, indent=2))


async def discover_address(name_hint: str | None = None) -> str | None:
    if BleakScanner is None:
        return None
    devices = await BleakScanner.discover(timeout=SCAN_TIMEOUT)
    if not devices:
        return None
    if name_hint:
        hint = name_hint.lower()
        for device in devices:
            if device.name and hint in device.name.lower():
                return device.address
    if len(devices) == 1:
        return devices[0].address
    return None


async def resolve_ble_address() -> str | None:
    env_address = os.getenv("GF_BLE_ADDRESS")
    if env_address:
        return env_address.strip()
    return ADDRESS


def register_hop_schedule(entries: list[Dict[str, Any]], boil_duration_min: float | None = None) -> None:
    global HOP_SCHEDULE, COOK_START_TIME
    COOK_START_TIME = time.monotonic()
    HOP_SCHEDULE = []
    for hop in entries:
        amount = hop.get("amount_g") or hop.get("amount")
        offset = hop.get("offset_min") or hop.get("offset")
        if amount is None or offset is None:
            continue
        try:
            offset_val = float(offset)
            amount_val = float(amount)
        except (TypeError, ValueError):
            continue
        if boil_duration_min and boil_duration_min > 0:
            scheduled_offset = max(0.0, boil_duration_min - offset_val)
        else:
            scheduled_offset = offset_val
        scheduled = COOK_START_TIME + scheduled_offset * 60
        HOP_SCHEDULE.append(
            {
                "name": hop["name"],
                "amount": amount_val,
                "offset": offset_val,
                "scheduled": scheduled,
                "triggered": False,
            }
        )
    update_next_hop_status()


def mark_hop_triggered(name: str) -> None:
    for hop in HOP_SCHEDULE:
        if hop["name"] == name and not hop["triggered"]:
            hop["triggered"] = True
            break
    update_next_hop_status()


def update_next_hop_status() -> None:
    if not HOP_SCHEDULE or COOK_START_TIME is None:
        status_update(next_hop=None, next_hop_remaining=None)
        return
    now = time.monotonic()
    candidates = [hop for hop in HOP_SCHEDULE if not hop["triggered"]]
    if not candidates:
        status_update(next_hop=None, next_hop_remaining=None)
        return
    next_hop = min(candidates, key=lambda hop: hop["scheduled"])
    remaining = max(0, int(next_hop["scheduled"] - now))
    status_update(next_hop=next_hop["name"], next_hop_remaining=remaining)


def is_front_hop(hop: dict[str, Any]) -> bool:
    name = str(hop.get("name") or "").lower()
    if "vorderwürze" in name or "vorderwuerze" in name or "front" in name:
        return True
    offset_val = safe_float(hop.get("offset_min") or hop.get("offset"))
    return offset_val is not None and offset_val <= 0


def ensure_selection_file() -> None:
    if SELECTED_RECIPE_FILE.exists():
        return
    SELECTED_RECIPE_FILE.parent.mkdir(parents=True, exist_ok=True)
    selection = {"path": str(DEFAULT_RECIPE_FILE)}
    SELECTED_RECIPE_FILE.write_text(json.dumps(selection, ensure_ascii=False, indent=2))


def resolve_recipe_path(cli_path: Path | None) -> Path:
    if cli_path:
        return cli_path
    ensure_selection_file()
    try:
        data = json.loads(SELECTED_RECIPE_FILE.read_text(encoding="utf-8"))
        candidate = Path(data.get("path", ""))
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        if candidate.exists():
            return candidate
    except Exception:
        pass
    return DEFAULT_RECIPE_FILE


def load_recipe(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def temperature_command(target: float) -> str:
    value = int(round(target))
    return f"${value},"


def timer_command(duration_min: float) -> str:
    if duration_min <= 0:
        return ""
    minutes = max(1, int(round(duration_min)))
    return f"S{minutes}"


def pad19(cmd: str) -> bytes:
    payload = (cmd[:19]).ljust(19, " ")
    return payload.encode("ascii")


async def ensure_heat_state(client: BleakClient, desired: bool) -> None:
    global HEAT_STATE, DESIRED_HEAT
    DESIRED_HEAT = desired
    if desired != HEAT_STATE:
        await client.write_gatt_char(WRITE_UUID, pad19("H"), response=False)
        HEAT_STATE = desired

async def ensure_pump_state(client: BleakClient, desired: bool) -> None:
    global PUMP_STATE, DESIRED_PUMP
    DESIRED_PUMP = desired
    if desired != PUMP_STATE:
        await client.write_gatt_char(WRITE_UUID, pad19("P"), response=False)
        PUMP_STATE = desired

def play_sound() -> None:
    try:
        subprocess.run(["afplay", SOUND], check=False)
    except Exception:
        pass


def print_status(
    current: float | None, target: float | None, label: str, next_step: str | None, state_name: str = "heating"
) -> None:
    st = summarize()
    x_values = st.get("x_raw") or []
    heat = st["heat"]
    power = st["power_pct"]
    target_label = f"{target:.1f}" if target is not None else "—"
    line = (
        f"{time.strftime('%H:%M:%S')} {label} | IST={current if current is not None else '??'} °C "
        f"| Ziel={target_label} °C | Heat={heat} | Power={power if power is not None else '??'}"
    )
    print("\r" + line.ljust(120), end="", flush=True)
    status_update(
        step=label,
        next_step=None,
        current_temp=current,
        target_temp=target,
        pump=st["pump"],
        heat=heat,
        power_pct=power,
        state=state_name,
        timer_label=None,
        timer_remaining=None,
        timer_duration=None,
        timer_active=False,
        x_raw=x_values,
    )


def handle_command(line: str, context: str) -> str | None:
    normalized = line.strip().lower()
    if not normalized:
        return None
    if normalized != "stop":
        CONTROL_STATE["stop_confirm_pending"] = False
    if normalized in ("pause", "p"):
        if not CONTROL_STATE["paused"]:
            CONTROL_STATE["paused"] = True
            print(f"⏸️  '{context}' pausiert – 'resume' zum Weiterfahren.")
        else:
            print("⏸️  Bereits pausiert.")
        return None
    if normalized in ("resume", "r"):
        if CONTROL_STATE["paused"]:
            CONTROL_STATE["paused"] = False
            print(f"▶️  '{context}' fortsetzen.")
        else:
            print("▶️  Bereits aktiv.")
        return None
    if normalized in ("skip", "next", "n"):
        print(f"↷ '{context}' wird übersprungen...")
        return "skip"
    if normalized in ("back", "prev", "b"):
        print(f"↩ '{context}' wird zurückgesetzt – vorheriger Schritt wird erneut gestartet.")
        return "back"
    if normalized in ("stop", "quit", "exit", "q"):
        if context == "Ack":
            now = time.monotonic()
            if (
                not CONTROL_STATE.get("stop_confirm_pending")
                or now - CONTROL_STATE.get("stop_confirm_at", 0.0) > 3.0
            ):
                CONTROL_STATE["stop_confirm_pending"] = True
                CONTROL_STATE["stop_confirm_at"] = now
                print("⛔ Stop bestätigen: 'stop' erneut senden.")
                return None
        CONTROL_STATE["stop_confirm_pending"] = False
        CONTROL_STATE["stop"] = True
        print(f"⛔ Gesamtablauf wird gestoppt (aus '{context}').")
        return "stop"
    if normalized in ("help", "h"):
        print_command_help()
        return None
    print(f"Unbekanntes Kommando: '{line}'.")
    return None


def process_pending_commands(step_label: str) -> str | None:
    action: str | None = None
    while True:
        try:
            line = COMMAND_QUEUE.get_nowait()
        except queue.Empty:
            break
        result = handle_command(line, step_label)
        if result == "stop":
            raise StopExecution
        if result == "skip":
            action = "skip"
        if result == "back":
            raise BackStep
    return action


async def apply_pause(client: BleakClient) -> None:
    if CONTROL_STATE.get("pause_applied"):
        return
    CONTROL_STATE["pause_applied"] = True
    CONTROL_STATE["pause_state"] = STATUS.get("state")
    status_update(state="paused", last_message="Pausiert – Resume zum Fortsetzen.")
    await ensure_heat_state(client, False)
    await ensure_pump_state(client, False)


async def clear_pause(client: BleakClient) -> None:
    if not CONTROL_STATE.get("pause_applied"):
        return
    CONTROL_STATE["pause_applied"] = False
    restore_state = CONTROL_STATE.get("pause_state") or STATUS.get("state") or "idle"
    status_update(state=restore_state, last_message="Fortgesetzt.")
    await ensure_heat_state(client, DESIRED_HEAT)
    await ensure_pump_state(client, DESIRED_PUMP)


async def wait_for_ack(prompt: str, *, finish_state: str | None = "idle", finish_message: str | None = None) -> None:
    global ACK_WAITING
    print(prompt)
    status_update(state="waiting_ack", last_message=prompt)
    ACK_WAITING = True
    try:
        while True:
            if CONTROL_STATE["stop"]:
                raise StopExecution
            try:
                line = COMMAND_QUEUE.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.1)
                continue
            if not line:
                print()
                post_state = finish_state if finish_state is not None else STATUS.get("state") or "idle"
                post_message = finish_message or prompt
                CONTROL_STATE["stop_confirm_pending"] = False
                status_update(state=post_state, last_message=post_message)
                return
            result = handle_command(line, "Ack")
            if result == "stop":
                raise StopExecution
            if result == "skip":
                print()
                post_state = finish_state if finish_state is not None else STATUS.get("state") or "idle"
                post_message = finish_message or prompt
                CONTROL_STATE["stop_confirm_pending"] = False
                status_update(state=post_state, last_message=post_message)
                return
            # Ack ignores pause/resume while waiting
    finally:
        ACK_WAITING = False


async def wait_for_start_signal() -> None:
    message = "⚙️  Warten auf Startsignal (Brau starten)."
    print(message)
    status_update(state="starting_wait", last_message=message)
    while True:
        if CONTROL_STATE["stop"]:
            raise StopExecution
        try:
            line = COMMAND_QUEUE.get_nowait()
        except queue.Empty:
            await asyncio.sleep(0.1)
            continue
        normalized = line.strip().lower()
        if not normalized:
            continue
        if normalized == "start":
            print("▶️  Startsignal erhalten – der Ablauf beginnt jetzt.")
            status_update(state="starting", last_message="Startsignal empfangen – Vorheizen startet.")
            return
        result = handle_command(line, "Startsignal")
        if result == "stop":
            raise StopExecution


async def wait_for_temperature(
    client: BleakClient,
    target: float,
    label: str,
    next_step: str | None,
    state_name: str,
    boost_target: float | None = None,
    allow_skip_without_next: bool = False,
) -> bool:
    print(f"\n→ Heize auf {target:.1f} °C ({label})")
    if boost_target is not None and boost_target > target:
        await client.write_gatt_char(
            WRITE_UUID, pad19(temperature_command(boost_target)), response=False
        )
    reached = False
    while True:
        if CONTROL_STATE["stop"]:
            raise StopExecution
        action = process_pending_commands(label)
        if action == "skip":
            status_update(state="skipped", last_message=f"{label} übersprungen")
            if allow_skip_without_next:
                return True
            if not next_step:
                raise StopExecution
            return False
        if CONTROL_STATE["paused"]:
            await apply_pause(client)
            await asyncio.sleep(0.25)
            continue
        else:
            await clear_pause(client)
        st = summarize()
        current = st["current_temp"]
        print_status(current, target, label, next_step, state_name)
        if current is not None and current >= target - TEMP_TOLERANCE:
            reached = True
            if not CONTROL_STATE["paused"]:
                print()
                play_sound()
                print(f"✨ {label}: Zieltemperatur {target:.1f} °C erreicht.")
                if boost_target is not None and boost_target > target:
                    await client.write_gatt_char(
                        WRITE_UUID, pad19(temperature_command(target)), response=False
                    )
                break
        await asyncio.sleep(POLL_INTERVAL)
    await asyncio.sleep(0.2)
    return reached


async def run_hold(
    client: BleakClient,
    duration_min: float,
    label: str,
    target_temp: float | None,
    next_step: str | None,
    timer_cmd: str,
    state_name: str,
) -> bool:
    seconds = int(duration_min * 60)
    if seconds <= 0:
        return True
    target_display = f"{target_temp:.1f}" if target_temp is not None else "—"
    print(f"→ Timer '{label}' ({duration_min:.1f} min) läuft...")
    if USE_DEVICE_TIMER and timer_cmd:
        await client.write_gatt_char(WRITE_UUID, pad19(timer_cmd), response=False)
    end = time.monotonic() + seconds
    pause_started: float | None = None
    while True:
        if CONTROL_STATE["stop"]:
            raise StopExecution
        action = process_pending_commands(label)
        if action == "skip":
            status_update(state="skipped", last_message=f"{label} übersprungen")
            if not next_step:
                raise StopExecution
            return False
        if CONTROL_STATE["paused"]:
            if pause_started is None:
                pause_started = time.monotonic()
            await apply_pause(client)
            await asyncio.sleep(0.25)
            continue
        if pause_started is not None:
            end += time.monotonic() - pause_started
            pause_started = None
        await clear_pause(client)
        remaining = max(0, int(end - time.monotonic()))
        mins = remaining // 60
        secs = remaining % 60
        st = summarize()
        current = st["current_temp"]
        heat = st["heat"]
        power = st["power_pct"]
        status_line = (
            f"{time.strftime('%H:%M:%S')} {label} | IST={current if current is not None else '??'} °C "
            f"| Ziel={target_display} °C | Heat={heat} | Power={power if power is not None else '??'} "
            f"| Verbleibend: {mins:02d}:{secs:02d}"
        )
        print("\r" + status_line.ljust(140), end="", flush=True)
        status_update(
            step=label,
            next_step=None,
            current_temp=current,
            target_temp=target_temp,
            pump=st["pump"],
            heat=heat,
            power_pct=power,
            timer_label=label,
            timer_remaining=remaining,
            timer_duration=seconds,
            timer_active=True,
            state=state_name,
        )
        if remaining <= 0:
            break
        await asyncio.sleep(1)
    print()
    play_sound()
    print(f"✔ {label} abgeschlossen")
    await asyncio.sleep(0.2)
    status_update(
        step=None,
        next_step=None,
        timer_label=None,
        timer_remaining=None,
        timer_duration=None,
        timer_active=False,
        state=state_name,
    )
    return True

def is_boil_step(step: dict[str, Any]) -> bool:
    name = str(step.get("name") or "").lower()
    target_value = safe_float(step.get("target_temp"))
    if target_value is not None and target_value >= 100:
        return True
    markers = ("koch", "boil", "würze")
    return any(marker in name for marker in markers)

def split_mash_and_boil_steps(steps: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    mash = []
    boil = []
    boil_started = False
    for step in steps:
        if boil_started:
            boil.append(step)
            continue
        if is_boil_step(step):
            boil_started = True
            boil.append(step)
            continue
        mash.append(step)
        if str(step.get("pump_action") or "").strip().lower() == "stop":
            boil_started = True
    return mash, boil

async def run_mash_steps(
    client: BleakClient, mash_steps: list[dict[str, Any]], next_boil_name: str | None
) -> None:
    if not mash_steps:
        return
    idx = 0
    while idx < len(mash_steps):
        step = mash_steps[idx]
        label = step.get("name") or f"Schritt {idx + 1}"
        target_temp = safe_float(step.get("target_temp"))
        duration = safe_float(step.get("duration_min")) or 0.0
        if idx == 0:
            duration = 0.0
        next_step = (
            mash_steps[idx + 1].get("name")
            if idx + 1 < len(mash_steps)
            else next_boil_name
        )
        try:
            status_update(
                step=label,
                next_step=None,
                last_message=f"Maische: {label}",
                state="mashing",
            )
            if target_temp is not None:
                print(f"→ Setze Zieltemperatur {target_temp:.1f} °C ({label})")
                await client.write_gatt_char(
                    WRITE_UUID, pad19(temperature_command(target_temp)), response=False
                )
                reached = await wait_for_temperature(
                    client, target_temp, label, next_step, state_name="mashing"
                )
            else:
                reached = True
            if duration > 0 and reached:
                timer_cmd = timer_command(duration)
                await run_hold(client, duration, label, target_temp, next_step, timer_cmd, state_name="mashing")
            idx += 1
        except BackStep:
            if idx > 0:
                idx -= 1
                status_update(state="mashing", last_message="Vorheriger Schritt wird erneut gestartet.")
            else:
                status_update(state="mashing", last_message="Bereits beim ersten Schritt.")

async def run_lautern_phase(has_boil: bool) -> None:
    status_update(
        state="lautern_ready",
        last_message="Pumpen aus – Läutern vorbereiten (78 °C werden gehalten).",
        timer_active=False,
        timer_label=None,
        timer_remaining=None,
        timer_duration=None,
    )
    play_sound()
    await wait_for_ack(
        "🛟 Läutern durchführen – ENTER zum Start.",
        finish_state="lautern",
        finish_message="Läutern läuft – Temperatur wird gehalten.",
    )
    if has_boil:
        play_sound()
        await wait_for_ack(
            "🛟 Läutern beendet? ENTER zum Start der Kochphase.",
            finish_state="boil",
            finish_message="Kochphase startet.",
        )
    else:
        status_update(
            state="lautern",
            last_message="Läutern läuft – keine Kochschritte definiert.",
        )

async def run_boil_phase(
    client: BleakClient, boil_steps: list[dict[str, Any]], hop_schedule: list[dict[str, Any]]
) -> None:
    if not boil_steps:
        status_update(
            state="boil",
            last_message="Keine Kochschritte im Rezept.",
            timer_active=False,
            timer_label=None,
            timer_remaining=None,
            timer_duration=None,
        )
        return
    status_update(state="boil", last_message="Kochphase startet.")
    hop_tasks: list[asyncio.Task] = []
    pending_hops: list[dict[str, Any]] = []
    hop_tasks_started = False
    front_hop: dict[str, Any] | None = None
    boil_duration_min = None
    for step in boil_steps:
        dur = safe_float(step.get("duration_min"))
        if dur is not None and dur > 0:
            boil_duration_min = dur
            break
    if hop_schedule:
        for hop in hop_schedule:
            if is_front_hop(hop):
                continue
            offset_val = safe_float(hop.get("offset_min") or hop.get("offset")) or 0.0
            if boil_duration_min and boil_duration_min > 0:
                delay_min = max(0.0, boil_duration_min - offset_val)
            else:
                delay_min = offset_val
            pending_hops.append(
                {
                    "name": hop["name"],
                    "amount": float(hop.get("amount_g") or hop.get("amount", 0)),
                    "delay_min": delay_min,
                }
            )
        for hop in hop_schedule:
            offset_val = safe_float(hop.get("offset_min") or hop.get("offset"))
            if is_front_hop(hop) or (offset_val is not None and offset_val <= 0):
                front_hop = hop
                break
    idx = 0
    while idx < len(boil_steps):
        step = boil_steps[idx]
        label = step.get("name") or f"Boil-Schritt {idx + 1}"
        target_temp = safe_float(step.get("target_temp"))
        duration = safe_float(step.get("duration_min")) or 0.0
        next_step = boil_steps[idx + 1].get("name") if idx + 1 < len(boil_steps) else None
        try:
            status_update(
                step=label,
                next_step=None,
                last_message=f"Kochphase: {label}",
                state="boil",
            )
            if target_temp is not None:
                print(f"→ Setze Zieltemperatur {target_temp:.1f} °C ({label})")
                await client.write_gatt_char(
                    WRITE_UUID, pad19(temperature_command(target_temp)), response=False
                )
                if idx == 0 and front_hop:
                    play_sound()
                    await wait_for_ack(
                        f"🍺 Vorderwürzehopfung durchführen – {front_hop.get('name', 'Vorderwürze')} hinzufügen. ENTER zum Bestätigen.",
                        finish_state="boil",
                        finish_message="Vorderwürzehopfung bestätigt.",
                    )
                    mark_hop_triggered(front_hop.get("name", "Vorderwürze"))
                boost_target = None
                if target_temp is not None and target_temp >= 95 and target_temp < 100:
                    boost_target = 100.0
                reached = await wait_for_temperature(
                    client,
                    target_temp,
                    label,
                    next_step,
                    state_name="boil",
                    boost_target=boost_target,
                    allow_skip_without_next=True,
                )
                if idx == 0 and reached:
                    play_sound()
                    await wait_for_ack(
                        "⚡ Kochtemperatur erreicht – ENTER zum Start des Kochens.",
                        finish_state="boil",
                        finish_message="Kochtemperatur bestätigt – Timer startet.",
                    )
                    if pending_hops and not hop_tasks_started:
                        register_hop_schedule(hop_schedule, boil_duration_min)
                        hop_tasks = [
                            asyncio.create_task(
                                hop_notification(hop["name"], hop["amount"], hop["delay_min"])
                            )
                            for hop in pending_hops
                        ]
                        hop_tasks_started = True
            else:
                reached = True
            if idx == 0 and reached and pending_hops and not hop_tasks_started:
                register_hop_schedule(hop_schedule, boil_duration_min)
                hop_tasks = [
                    asyncio.create_task(hop_notification(hop["name"], hop["amount"], hop["delay_min"]))
                    for hop in pending_hops
                ]
                hop_tasks_started = True
            if duration > 0 and reached:
                timer_cmd = timer_command(duration)
                await run_hold(client, duration, label, target_temp, next_step, timer_cmd, state_name="boil")
            idx += 1
        except BackStep:
            if idx > 0:
                idx -= 1
                status_update(state="boil", last_message="Vorheriger Kochschritt wird erneut gestartet.")
            else:
                status_update(state="boil", last_message="Bereits beim ersten Kochschritt.")
    if hop_tasks:
        await asyncio.gather(*hop_tasks)
    status_update(
        step=None,
        next_step=None,
        last_message="Kochphase abgeschlossen.",
        state="idle",
        timer_active=False,
        timer_label=None,
        timer_remaining=None,
        timer_duration=None,
        next_hop=None,
        next_hop_remaining=None,
    )


async def hop_notification(name: str, amount: float, delay_min: float) -> None:
    await asyncio.sleep(delay_min * 60)
    play_sound()
    status_update(hop_event=f"Hopfengabe: {name} ({amount} g)", state="hop_event")
    await wait_for_ack(f"🍺 Hopfengabe: {name} ({amount} g) – ENTER zum Bestätigen.")
    mark_hop_triggered(name)
    status_update(hop_event=None)


async def final_ack_and_shutdown(client: BleakClient, recipe_label: str) -> None:
    await ensure_heat_state(client, False)
    await ensure_pump_state(client, False)
    play_sound()
    await wait_for_ack(
        "🔚 Kochende – ENTER zum Schließen des Prozesses.",
        finish_state="finalizing",
        finish_message="Heizung aus – Prozess wird abgeschlossen.",
    )
    await cancel_device(client)
    status_update(
        state="finished",
        last_message=f"{recipe_label} abgeschlossen.",
        timer_active=False,
        timer_label=None,
        timer_remaining=0,
    )


async def cancel_device(client: BleakClient) -> None:
    try:
        await ensure_pump_state(client, False)
        await ensure_heat_state(client, False)
        await client.write_gatt_char(WRITE_UUID, pad19("C0"), response=False)
        print("🔥 Gerät heruntergefahren.")
    except Exception:
        pass


async def run_recipe_steps(client: BleakClient, recipe: dict[str, Any]) -> None:
    steps = recipe.get("steps", [])
    if not steps:
        status_update(state="idle", last_message="Rezept enthält keine Schritte.")
        return
    first_step = steps[0]
    first_target = safe_float(first_step.get("target_temp"))
    if first_target is not None:
        preheat_target = first_target
        print(f"→ Vorheize auf {preheat_target:.1f} °C (erste Maischestufe).")
        status_update(
            state="preheating",
            last_message=f"Vorheizen auf {preheat_target:.1f} °C.",
            target_temp=preheat_target,
            timer_active=False,
            timer_label=None,
            timer_remaining=None,
            timer_duration=None,
        )
        await client.write_gatt_char(
            WRITE_UUID, pad19(temperature_command(preheat_target)), response=False
        )
        await wait_for_temperature(
            client,
            preheat_target,
            "Vorheizen",
            first_step.get("name"),
            state_name="preheating",
        )
        play_sound()
        await wait_for_ack(
            "🔔 Bereit zum Einmaischen – ENTER zum Start der Maische.",
            finish_state="mashing_ready",
            finish_message="Bereit zum Einmaischen – bitte Malz einfüllen und bestätigen.",
        )
        play_sound()
        await wait_for_ack(
            "🔔 Einmaischen beendet? ENTER zum Start der Maischerasten.",
            finish_state="mashing",
            finish_message="Einmaischen abgeschlossen – Rasten starten.",
        )
    else:
        print("⚠️ Kein Zielwert für die erste Maischestufe definiert.")
        status_update(state="preheating", last_message="Vorheizen ohne Zieltemperatur.")
        play_sound()
        await wait_for_ack(
            "🔔 Bereit zum Einmaischen – ENTER zum Start der Maische.",
            finish_state="mashing_ready",
            finish_message="Bereit zum Einmaischen – bitte Malz einfüllen und bestätigen.",
        )
    print("→ Pumpe startet – Einmaischung läuft.")
    mash_steps, boil_steps = split_mash_and_boil_steps(steps)
    first_boil_name = boil_steps[0].get("name") if boil_steps else None
    if mash_steps:
        await ensure_pump_state(client, True)
        status_update(state="mashing", pump=True, last_message="Pumpe läuft – Maischen aktiv.")
        try:
            await run_mash_steps(client, mash_steps, first_boil_name)
        finally:
            await ensure_pump_state(client, False)
    else:
        status_update(last_message="Keine Maischschritte definiert.", state="mashing")
    status_update(
        state="lautern_ready",
        last_message="Pumpen aus – Läutern vorbereiten.",
        timer_active=False,
        timer_label=None,
        timer_remaining=None,
        timer_duration=None,
    )
    await run_lautern_phase(bool(boil_steps))
    await run_boil_phase(client, boil_steps, recipe.get("hop_schedule", []))


async def main() -> None:
    global COMMAND_QUEUE, CONTROL_STATE
    COMMAND_QUEUE = InputQueue()
    CONTROL_STATE = {
        "paused": False,
        "stop": False,
        "pause_applied": False,
        "pause_state": None,
        "stop_confirm_pending": False,
        "stop_confirm_at": 0.0,
        }

    if BleakClient is None:
        raise RuntimeError("Die bleak-Bibliothek wird für den Brewflow benötigt.")

    parser = argparse.ArgumentParser(description="Grainfather-Brauflow")
    parser.add_argument("--tolerance", type=float, default=0.5, help="Temperaturtoleranz in °C")
    parser.add_argument("--recipe", type=Path, help="Pfad zu einer JSON-Rezeptdatei")
    args = parser.parse_args()
    global TEMP_TOLERANCE
    TEMP_TOLERANCE = args.tolerance

    recipe_path = resolve_recipe_path(args.recipe)
    recipe_data = load_recipe(recipe_path)
    recipe_name = recipe_data.get("name") or recipe_path.stem
    status_update(last_message=f"Rezept geladen: {recipe_name}", recipe=recipe_name)

    start_command_listener()
    print_command_help()

    address = await resolve_ble_address()
    if not address:
        raise RuntimeError("Keine BLE-Adresse verfügbar. Setze GF_BLE_ADDRESS oder aktiviere das Gerät.")

    name_hint = os.getenv("GF_BLE_NAME", DEFAULT_DEVICE_NAME)
    last_error: Exception | None = None
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            status_update(
                state="connecting",
                last_message=f"Verbinde per BLE… (Versuch {attempt}/{CONNECT_RETRIES})",
                recipe=recipe_name,
            )
            async with BleakClient(address, timeout=30.0) as client:
                await client.start_notify(NOTIFY_UUID, handle)
                if hasattr(client, "get_services"):
                    await client.get_services()
                status_update(
                    ble_connected=True,
                    state="starting",
                    last_message="BLE verbunden, starte Rezept",
                    recipe=recipe_name,
                )
                try:
                    await client.write_gatt_char(WRITE_UUID, pad19("C0"), response=False)
                    status_update(state="starting", last_message="Verbindung aufbauen", recipe=recipe_name)
                    print("Connected:", client.is_connected)
                    await wait_for_start_signal()
                    await ensure_heat_state(client, True)

                    await run_recipe_steps(client, recipe_data)

                    await ensure_pump_state(client, False)
                    print("→ Pumpe ausgeschaltet")
                    await final_ack_and_shutdown(client, recipe_name)
                except StopExecution:
                    status_update(state="stopped", last_message="Abbruch durch Benutzer", recipe=recipe_name)
                    await cancel_device(client)
                    raise
                finally:
                    status_update(ble_connected=False)
                    await client.stop_notify(NOTIFY_UUID)
            return
        except StopExecution:
            raise
        except BleakDeviceNotFoundError as exc:
            last_error = exc
            status_update(
                state="connecting",
                last_message="BLE-Gerät nicht gefunden – suche erneut…",
                recipe=recipe_name,
            )
            found = await discover_address(name_hint)
            if found:
                address = found
            await asyncio.sleep(2.0)
        except BleakError as exc:
            last_error = exc
            status_update(
                state="connecting",
                last_message=f"BLE-Fehler: {exc}",
                recipe=recipe_name,
            )
            await asyncio.sleep(2.0)

    if last_error:
        raise last_error
    raise RuntimeError("BLE-Verbindung fehlgeschlagen.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except StopExecution:
        status_update(state="stopped", last_message="Gestoppt durch Benutzer")
        print("Script wurde gestoppt.")
