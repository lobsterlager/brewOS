#!/usr/bin/env python3
"""Brauablauf-Manager für deine Lobster-Lager-Rezepte.

Das Script arbeitet die einzelnen Schritte sequentiell ab, sorgt für die
Zieltemperaturen, protokolliert Timer-Events (z. B. Hopfengaben) und zeigt
laufend den aktuellen Schritt samt Temperatur und Pumpenstatus an.

Die Bluetooth-Kommandos sind Platzhalter (send_ble_command). Ersetze sie
durch deine funktionierenden Codex-/BLE-Funktionen.
"""

from __future__ import annotations

import argparse
import json
import queue
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

POLL_INTERVAL = 5  # Sekunden zwischen Status-Updates
COMMAND_QUEUE: queue.Queue[str] = queue.Queue()
pump_active = False


class StopExecution(Exception):
    pass


def send_ble_command(command: str, **kwargs: Any) -> None:
    """Platzhalter für deine BLE-Befehle.

    Ersetze diese Funktion mit dem Code, der dein Grainfather G30 über BLE
    steuert (z. B. Temperatur setzen, Pumpen starten, Timer setzen).
    """

    print(f"[BLE] {command} -> {kwargs}")


def read_temperature() -> float:
    """Lesen der aktuellen Temperatur vom Sensor.

    Hier kannst du deinen Code ergänzen, der den aktuellen Ist-Wert
    abruft. Aktuell wird der zuletzt gesetzte Sollwert zurückgegeben.
    """

    return read_temperature.last_target


read_temperature.last_target = 20.0  # Default-Wert


def set_temperature(target: float) -> None:
    """Setzt die Solltemperatur und informiert die BLE-Bridge."""

    read_temperature.last_target = target
    send_ble_command("set_temperature", temperature=target)


def format_duration(seconds: float) -> str:
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes:02d}:{secs:02d}"


def load_recipe(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Rezeptdatei {path} existiert nicht")
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def start_command_listener() -> None:
    def reader() -> None:
        while True:
            try:
                line = input()
            except EOFError:
                break
            COMMAND_QUEUE.put(line.strip().lower())

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()


def print_command_help() -> None:
    print("Interaktive Kommandos verfügbar:")
    print("  pause  → aktuellen Schritt anhalten")
    print("  resume → Pausierung aufheben")
    print("  skip   → Schritt überspringen, nächster Schritt startet")
    print("  stop   → gesamten Ablauf anhalten")
    print()


def handle_command(line: str, control: Dict[str, bool]) -> None:
    if line in ("pause", "p"):
        if not control["paused"]:
            control["paused"] = True
            print("⏸️  Schritt pausiert (resume zum Fortfahren)")
        else:
            print("⏸️  Bereits pausiert")
    elif line in ("resume", "r"):
        if control["paused"]:
            control["paused"] = False
            print("▶️  Schritt fortsetzen")
        else:
            print("▶️  Bereits aktiv")
    elif line in ("skip", "next", "n"):
        control["skip_step"] = True
    elif line in ("stop", "quit", "exit", "q"):
        control["stop"] = True
    elif line in ("help", "h"):
        print_command_help()
    else:
        print(f"Unbekanntes Kommando: {line}")


def poll_commands(control: Dict[str, bool], step_name: str) -> None:
    while True:
        try:
            line = COMMAND_QUEUE.get_nowait()
        except queue.Empty:
            break
        handle_command(line, control)
        if control["skip_step"]:
            print(f"↷ '{step_name}' wird übersprungen...")
        if control["stop"]:
            print("⛔ Gesamtablauf wird gestoppt...")


def display_step(
    step_name: str,
    current_temp: float,
    target_temp: Optional[float],
    remaining: Optional[float],
    pump_on: bool,
    elapsed: float,
) -> None:
    line = (
        f"[{datetime.now().strftime('%H:%M:%S')}] Schritt: {step_name}"
        f" | Ist: {current_temp:.1f}°C"
    )
    if target_temp is not None:
        line += f" | Ziel: {target_temp:.1f}°C"
    if remaining is not None:
        line += f" | Verbleibend: {format_duration(remaining)}"
    line += f" | Pump: {'On' if pump_on else 'Off'}"
    line += f" | Gesamt: {format_duration(elapsed)}"
    print(line)


def handle_hop_schedule(hop_schedule: List[Dict[str, Any]], elapsed_seconds: float) -> None:
    for hop in hop_schedule:
        if hop.get("triggered"):
            continue
        offset = hop.get("offset_min", 0) * 60
        if elapsed_seconds >= offset:
            send_ble_command(
                "hop_add",
                name=hop.get("name"),
                amount_g=hop.get("amount_g"),
                alpha_percent=hop.get("alpha") if hop.get("alpha") else "?",
            )
            hop["triggered"] = True
            print(f" ➤ Hopfengabe: {hop.get('name')} ({hop.get('amount_g')} g) bei {hop.get('offset_min')} min")


def apply_pump_action(action: Optional[str]) -> None:
    global pump_active
    if action == "start" and not pump_active:
        send_ble_command("pump", action="start")
        pump_active = True
        print("→ Pumpe gestartet")
    elif action == "stop" and pump_active:
        send_ble_command("pump", action="stop")
        pump_active = False
        print("→ Pumpe gestoppt")


def ensure_pump_off() -> None:
    if pump_active:
        apply_pump_action("stop")


def stop_requested(control: Dict[str, bool]) -> bool:
    return control.get("stop", False)


def execute_step(
    step: Dict[str, Any],
    recipe: Dict[str, Any],
    global_elapsed: float,
    control: Dict[str, bool],
) -> float:
    target_temp = step.get("target_temp")
    duration_seconds = step.get("duration_min", 0) * 60

    if target_temp is not None:
        print(f"\n→ Setze Temperatur auf {target_temp}°C für Schritt '{step['name']}'")
        set_temperature(target_temp)

    if duration_seconds <= 0:
        display_step(
            step["name"],
            read_temperature(),
            target_temp,
            0,
            pump_active,
            global_elapsed,
        )
        return global_elapsed

    remaining = duration_seconds
    last_time = time.time()
    next_display = last_time

    while remaining > 0:
        poll_commands(control, step["name"])
        if stop_requested(control):
            raise StopExecution("Abbruch durch Benutzer")
        if control["skip_step"]:
            control["skip_step"] = False
            print(f"↷ '{step['name']}' übersprungen")
            break

        now = time.time()
        delta = now - last_time
        last_time = now

        if control["paused"]:
            time.sleep(0.3)
            continue

        remaining = max(0.0, remaining - delta)
        elapsed_for_recipe = global_elapsed + (duration_seconds - remaining)

        if now >= next_display:
            current_temp = read_temperature()
            display_step(
                step["name"],
                current_temp,
                target_temp,
                remaining,
                pump_active,
                elapsed_for_recipe,
            )
            next_display = now + POLL_INTERVAL

        handle_hop_schedule(recipe.get("hop_schedule", []), elapsed_for_recipe)
        time.sleep(0.5)

    executed = duration_seconds - remaining
    if remaining == 0:
        print(f"✔ Schritt '{step['name']}' abgeschlossen")
    return global_elapsed + executed


def run_recipe(recipe: Dict[str, Any]) -> None:
    start_command_listener()
    print_command_help()
    step_control = {"paused": False, "skip_step": False, "stop": False}

    print(f"Starte Rezept: {recipe.get('name')}")
    print(f"Gesamtschüttung: {recipe.get('grain_bill')} kg")
    print("------------------------------")

    global_elapsed = 0.0
    try:
        for step in recipe.get("steps", []):
            if stop_requested(step_control):
                raise StopExecution()
            global_elapsed = execute_step(step, recipe, global_elapsed, step_control)
            apply_pump_action(step.get("pump_action"))

    except StopExecution:
        print("\nRezeptlauf wurde gestoppt.")
    finally:
        ensure_pump_off()

    print("\nAlle Schritte durchlaufen oder gestoppt.")
    print("Hopfengaben-Status:")
    for hop in recipe.get("hop_schedule", []):
        status = "✅" if hop.get("triggered") else "⏱️"
        print(f"  {status} {hop['name']} ({hop['amount_g']} g) bei {hop['offset_min']} min")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Brauablauf automatisch abarbeiten")
    parser.add_argument(
        "recipe",
        nargs="?",
        default="recipes/lobsterlager_recipe.json",
        help="Pfad zur Rezeptdatei (JSON)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    recipe_path = Path(args.recipe)
    recipe = load_recipe(recipe_path)
    run_recipe(recipe)


if __name__ == "__main__":
    main()
