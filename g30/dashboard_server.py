#!/usr/bin/env python3
"""Server für das Brew Dashboard und die Statusdaten."""

from __future__ import annotations

import http.server
import io
import json
import os
import socketserver
import subprocess
import threading
import time
import webbrowser
from functools import partial
from pathlib import Path
from urllib.parse import unquote

PORT = 8002
ROOT = os.path.dirname(os.path.abspath(__file__))
STATUS_DIR = os.path.join(ROOT, "status")
STATUS_FILE = "brew_status.json"
RECIPES_DIR = os.path.join(ROOT, "recipes")
SELECTION_FILE = os.path.join(ROOT, "selected_recipe.json")
DEFAULT_RECIPE_PATH = os.path.join(RECIPES_DIR, "test_sud_nr202.json")
LOG_DIR = os.path.join(ROOT, "logs")
BREW_PY = os.path.join(ROOT, "gf_brew_flow.py")
PYTHON = os.getenv("GF_PYTHON", "python3")
TELEGRAM_NOTIFY_CONFIG = os.path.join(ROOT, "telegram_triggers.json")
NOTIFIER_CMD = [
    PYTHON,
    os.path.join(ROOT, "telegram_brew_notify.py"),
    "--config",
    TELEGRAM_NOTIFY_CONFIG,
    "--auto-stop",
]
NOTIFIER_PROCESS: subprocess.Popen | None = None
NOTIFIER_LOG_FILE: io.BufferedWriter | None = None

BREW_LOCK = threading.Lock()
BREW_PROCESS: subprocess.Popen | None = None
BREW_LOG_FILE: "io.BufferedWriter" | None = None
BREW_STARTED_AT: float | None = None
BREW_LAST_COMMAND: str | None = None
START_FEEDBACK: dict[str, object | None] = {
    "success": None,
    "message": "Noch kein Startversuch",
    "timestamp": None,
}


def run_cancel_script() -> None:
    cmd = [PYTHON, "gf_all_off.py"]
    subprocess.Popen(cmd, cwd=ROOT)


def ensure_selection_file() -> None:
    if os.path.isfile(SELECTION_FILE):
        return
    os.makedirs(os.path.dirname(SELECTION_FILE), exist_ok=True)
    selection = {
        "path": os.path.relpath(DEFAULT_RECIPE_PATH, ROOT),
        "selected_at": time.time(),
    }
    with open(SELECTION_FILE, "w", encoding="utf-8") as fh:
        json.dump(selection, fh, ensure_ascii=False, indent=2)


def ensure_log_dir() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)


def stop_telegram_notifier() -> None:
    global NOTIFIER_PROCESS, NOTIFIER_LOG_FILE
    if NOTIFIER_PROCESS:
        if NOTIFIER_PROCESS.poll() is None:
            try:
                NOTIFIER_PROCESS.terminate()
                NOTIFIER_PROCESS.wait(timeout=5)
            except subprocess.TimeoutExpired:
                NOTIFIER_PROCESS.kill()
                NOTIFIER_PROCESS.wait(timeout=5)
        NOTIFIER_PROCESS = None
    if NOTIFIER_LOG_FILE:
        try:
            NOTIFIER_LOG_FILE.close()
        except Exception:
            pass
        NOTIFIER_LOG_FILE = None


def start_telegram_notifier() -> None:
    global NOTIFIER_PROCESS, NOTIFIER_LOG_FILE
    stop_telegram_notifier()
    ensure_log_dir()
    log_path = Path(LOG_DIR) / "telegram_notifier.log"
    log_handle = log_path.open("ab")
    try:
        NOTIFIER_PROCESS = subprocess.Popen(
            NOTIFIER_CMD,
            cwd=ROOT,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
        )
        NOTIFIER_LOG_FILE = log_handle
    except Exception:
        log_handle.close()
        raise


def ensure_status_file() -> None:
    os.makedirs(STATUS_DIR, exist_ok=True)
    status_path = os.path.join(STATUS_DIR, STATUS_FILE)
    if os.path.isfile(status_path):
        return
    placeholder = {
        "state": "idle",
        "last_message": "Noch kein Status geschrieben.",
        "ble_connected": False,
        "timestamp": time.time(),
    }
    with open(status_path, "w", encoding="utf-8") as fh:
        json.dump(placeholder, fh, ensure_ascii=False, indent=2)


def read_status_file() -> dict[str, object]:
    ensure_status_file()
    status_path = os.path.join(STATUS_DIR, STATUS_FILE)
    try:
        return json.loads(Path(status_path).read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_status_file(payload: dict[str, object]) -> None:
    ensure_status_file()
    status_path = os.path.join(STATUS_DIR, STATUS_FILE)
    with open(status_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def reset_status_file(message: str = "Status zurückgesetzt.") -> dict[str, object]:
    payload: dict[str, object] = {
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
        "last_message": message,
        "recipe": None,
        "x_raw": [],
        "ble_connected": False,
        "hop_acks": {},
        "hop_acks_updated_at": time.time(),
        "timestamp": time.time(),
    }
    write_status_file(payload)
    return payload


def update_hop_ack(payload: dict[str, object]) -> dict[str, object]:
    key = str(payload.get("key") or "").strip()
    if not key:
        raise ValueError("Hopfen-Key fehlt")
    acked = bool(payload.get("acked"))
    status = read_status_file()
    hop_acks = status.get("hop_acks")
    if not isinstance(hop_acks, dict):
        hop_acks = {}
    if acked:
        hop_acks[key] = {
            "name": payload.get("hop_name"),
            "amount": payload.get("amount"),
            "recipe": payload.get("recipe"),
            "brew_since": payload.get("brew_since"),
            "acked": True,
            "timestamp": time.time(),
        }
    else:
        hop_acks.pop(key, None)
    status["hop_acks"] = hop_acks
    status["hop_acks_updated_at"] = time.time()
    write_status_file(status)
    return status

def set_start_feedback(success: bool, message: str) -> dict[str, object | None]:
    global START_FEEDBACK
    START_FEEDBACK = {
        "success": success,
        "message": message,
        "timestamp": time.time(),
    }
    return START_FEEDBACK


def get_start_feedback() -> dict[str, object | None]:
    return START_FEEDBACK


def list_recipes() -> list[dict[str, str]]:
    if not os.path.isdir(RECIPES_DIR):
        return []
    recipes = []
    for name in sorted(os.listdir(RECIPES_DIR)):
        if not name.endswith(".json"):
            continue
        full = os.path.join(RECIPES_DIR, name)
        try:
            data = json.loads(Path(full).read_text(encoding="utf-8"))
        except Exception:
            continue
        if data.get("archived"):
            continue
        recipes.append(
            {
                "path": os.path.relpath(full, ROOT),
                "name": data.get("name") or Path(full).stem,
                "description": data.get("description", ""),
            }
        )
    return recipes


def read_selection() -> dict[str, str | float]:
    ensure_selection_file()
    try:
        data = json.loads(Path(SELECTION_FILE).read_text(encoding="utf-8"))
    except Exception:
        data = {}
    rel = data.get("path") or os.path.relpath(DEFAULT_RECIPE_PATH, ROOT)
    candidate = os.path.normpath(os.path.join(ROOT, rel))
    if not candidate.startswith(ROOT) or not os.path.isfile(candidate):
        candidate = DEFAULT_RECIPE_PATH
    selection = {
        "path": os.path.relpath(candidate, ROOT),
        "name": Path(candidate).stem,
        "selected_at": time.time(),
    }
    return selection


def write_selection(path: str) -> dict[str, str | float]:
    normalized = os.path.normpath(os.path.join(ROOT, path))
    if not normalized.startswith(ROOT) or not os.path.isfile(normalized):
        raise ValueError("Ungültiger Rezeptpfad")
    selection = {
        "path": os.path.relpath(normalized, ROOT),
        "name": Path(normalized).stem,
        "selected_at": time.time(),
    }
    with open(SELECTION_FILE, "w", encoding="utf-8") as fh:
        json.dump(selection, fh, ensure_ascii=False, indent=2)
    return selection


def update_process_state() -> None:
    global BREW_PROCESS, BREW_LOG_FILE, BREW_STARTED_AT, BREW_LAST_COMMAND
    if BREW_PROCESS and BREW_PROCESS.poll() is not None:
        try:
            BREW_PROCESS.stdin and BREW_PROCESS.stdin.close()
        except Exception:
            pass
        BREW_PROCESS = None
        BREW_STARTED_AT = None
        BREW_LAST_COMMAND = None
        if BREW_LOG_FILE:
            try:
                BREW_LOG_FILE.close()
            except Exception:
                pass
            BREW_LOG_FILE = None


def is_brew_running() -> bool:
    update_process_state()
    return BREW_PROCESS is not None


def read_start_error(log_path: Path) -> str | None:
    try:
        content = log_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None
    lines = content.strip().splitlines()
    tail = "\n".join(lines[-40:])
    for marker in (
        "BleakDeviceNotFoundError",
        "BleakError",
        "Traceback",
    ):
        if marker in tail:
            return tail
    return None


def start_brew() -> dict[str, str | float]:
    global BREW_PROCESS, BREW_LOG_FILE, BREW_STARTED_AT, BREW_LAST_COMMAND
    stop_telegram_notifier()
    with BREW_LOCK:
        if is_brew_running():
            raise RuntimeError("Ein Brauvorgang läuft bereits")
        ensure_log_dir()
        timestamp = int(time.time())
        log_path = Path(LOG_DIR) / f"brew_{timestamp}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_file = open(log_path, "ab")
        BREW_LOG_FILE = log_file
        env = os.environ.copy()
        BREW_PROCESS = subprocess.Popen(
            [PYTHON, BREW_PY],
            cwd=ROOT,
            stdin=subprocess.PIPE,
            stdout=log_file,
            stderr=log_file,
            env=env,
        )
        if BREW_PROCESS.stdin:
            BREW_PROCESS.stdin.write(b"start\n")
            BREW_PROCESS.stdin.flush()
        time.sleep(2.0)
        if BREW_PROCESS.poll() is not None:
            error_tail = read_start_error(log_path)
            update_process_state()
            if error_tail:
                raise RuntimeError(f"Start fehlgeschlagen (siehe Log):\n{error_tail}")
            raise RuntimeError("Start fehlgeschlagen: Prozess wurde sofort beendet (BLE nicht verbunden?)")
        BREW_STARTED_AT = time.time()
        BREW_LAST_COMMAND = None
        payload = brew_status_payload()
        try:
            start_telegram_notifier()
        except Exception as exc:
            print(f"[WARN] Telegram-Notifier konnte nicht gestartet werden: {exc}")
        return payload


def send_brew_command(command: str) -> dict[str, str | float]:
    global BREW_LAST_COMMAND
    with BREW_LOCK:
        if not is_brew_running():
            raise RuntimeError("Kein laufender Brauvorgang")
        if not BREW_PROCESS.stdin:
            raise RuntimeError("Keine Kommando-Schnittstelle verfügbar")
        BREW_PROCESS.stdin.write((command.strip() + "\n").encode("utf-8"))
        BREW_PROCESS.stdin.flush()
        BREW_LAST_COMMAND = command.strip()
        return brew_status_payload()


def stop_brew() -> dict[str, str | float]:
    if not is_brew_running():
        raise RuntimeError("Kein laufender Brauvorgang")
    payload = send_brew_command("stop")
    stop_telegram_notifier()
    return payload

def abort_brew() -> dict[str, str | float]:
    if not is_brew_running():
        raise RuntimeError("Kein laufender Brauvorgang")
    try:
        send_brew_command("stop")
    except RuntimeError:
        pass
    with BREW_LOCK:
        process = BREW_PROCESS
    if process:
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
    run_cancel_script()
    update_process_state()
    stop_telegram_notifier()
    return brew_status_payload()

def hardware_off() -> dict[str, str | float | None]:
    run_cancel_script()
    update_process_state()
    stop_telegram_notifier()
    return brew_status_payload()


def brew_status_payload() -> dict[str, str | float | None]:
    update_process_state()
    running = bool(BREW_PROCESS)
    return {
        "running": running,
        "pid": BREW_PROCESS.pid if running and BREW_PROCESS else None,
        "since": BREW_STARTED_AT,
        "last_command": BREW_LAST_COMMAND,
    }





def send_brew_ack() -> dict[str, str | float]:
    with BREW_LOCK:
        if not is_brew_running():
            raise RuntimeError("Kein laufender Brauvorgang")
        if not BREW_PROCESS.stdin:
            raise RuntimeError("Keine Kommando-Schnittstelle verfügbar")
        BREW_PROCESS.stdin.write(b'\n')
        BREW_PROCESS.stdin.flush()
        return brew_status_payload()

ALLOWED_COMMANDS = {"pause", "resume", "skip", "next", "stop", "back"}


def send_json(handler: http.server.BaseHTTPRequestHandler, payload: object, status_code: int = 200) -> None:
    handler.send_response(status_code)
    handler.send_header("Content-Type", "application/json")
    handler.end_headers()
    handler.wfile.write(json.dumps(payload, ensure_ascii=False).encode("utf-8"))


def brew_status_with_recipe() -> dict[str, object | str | float | None]:
    status = brew_status_payload()
    current = read_selection()
    status["recipe"] = current.get("name")
    status["recipe_path"] = current.get("path")
    return status


def list_log_files() -> list[dict[str, object]]:
    ensure_log_dir()
    root = Path(LOG_DIR)
    logs = []
    for entry in sorted(root.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if not entry.is_file() or not entry.name.lower().endswith('.log'):
            continue
        stats = entry.stat()
        logs.append({
            'name': entry.name,
            'path': os.path.relpath(entry, ROOT).replace(os.sep, '/'),
            'modified_at': stats.st_mtime,
            'size': stats.st_size,
        })
    return logs


def read_log_file(name: str) -> str:
    if not name:
        raise FileNotFoundError('Logname fehlt')
    safe_name = os.path.basename(name)
    candidate = os.path.join(LOG_DIR, safe_name)
    if not os.path.isfile(candidate):
        raise FileNotFoundError('Logdatei nicht gefunden')
    with open(candidate, 'r', encoding='utf-8', errors='replace') as fh:
        return fh.read()


def ble_config_payload() -> dict[str, str | None]:
    return {
        "ble_name": os.getenv("GF_BLE_NAME"),
        "ble_address": os.getenv("GF_BLE_ADDRESS"),
    }




class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def translate_path(self, path: str) -> str:
        path = path.split("?", 1)[0].split("#", 1)[0]
        if path.startswith("/status/"):
            full = os.path.join(ROOT, path.lstrip("/"))
            if os.path.isfile(full):
                return full
        return os.path.join(ROOT, path.lstrip("/"))

    def do_GET(self):
        if self.path.startswith("/status/"):
            path = self.path.split("?", 1)[0].split("#", 1)[0]
            full = os.path.join(ROOT, path.lstrip("/"))
            if os.path.isfile(full):
                try:
                    payload = json.loads(Path(full).read_text(encoding="utf-8"))
                except Exception:
                    payload = {}
                send_json(self, payload)
                return
        if self.path == "/api/status":
            status_path = os.path.join(STATUS_DIR, STATUS_FILE)
            if os.path.isfile(status_path):
                send_json(self, json.loads(Path(status_path).read_text(encoding="utf-8")))
                return
        if self.path == "/api/recipes":
            send_json(self, list_recipes())
            return
        if self.path == "/api/current":
            send_json(self, read_selection())
            return
        if self.path == "/api/brew":
            send_json(self, brew_status_with_recipe())
            return
        if self.path == "/api/start-feedback":
            send_json(self, get_start_feedback())
            return
        if self.path == "/api/logs":
            send_json(self, {"logs": list_log_files()})
            return
        if self.path == "/api/ble-config":
            send_json(self, ble_config_payload())
            return
        if self.path.startswith("/api/logs/"):
            name = unquote(self.path[len("/api/logs/"):])
            name = name.strip()
            if name:
                try:
                    content = read_log_file(name)
                except (FileNotFoundError, ValueError):
                    send_json(self, {"error": "Log nicht gefunden"}, status_code=404)
                else:
                    send_json(self, {"name": os.path.basename(name), "content": content})
                return
        return super().do_GET()

    def do_POST(self):
        if self.path == "/api/ack":
            length = int(self.headers.get("Content-Length", 0))
            if length:
                self.rfile.read(length)
            try:
                send_json(self, send_brew_ack())
            except Exception as exc:
                send_json(self, {"error": str(exc)}, status_code=400)
            return
        if self.path == "/api/hop-ack":
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b""
            try:
                payload = json.loads(raw.decode("utf-8")) if raw else {}
            except json.JSONDecodeError:
                send_json(self, {"error": "Ungültiges JSON"}, status_code=400)
                return
            try:
                send_json(self, update_hop_ack(payload))
            except Exception as exc:
                send_json(self, {"error": str(exc)}, status_code=400)
            return
        if self.path == "/api/stop":
            length = int(self.headers.get("Content-Length", 0))
            if length:
                self.rfile.read(length)
            try:
                send_json(self, stop_brew())
            except Exception as exc:
                send_json(self, {"error": str(exc)}, status_code=400)
            return
        if self.path == "/api/start":
            length = int(self.headers.get("Content-Length", 0))
            if length:
                self.rfile.read(length)
            try:
                payload = start_brew()
            except Exception as exc:
                set_start_feedback(False, str(exc))
                send_json(self, {"error": str(exc)}, status_code=400)
            else:
                set_start_feedback(True, "Brauvorgang gestartet")
                send_json(self, payload)
            return
        if self.path == "/api/command":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length) if length else b"{}"
            try:
                payload = json.loads(body or b"{}")
                command = (payload.get("command") or "").strip().lower()
                if not command or command not in ALLOWED_COMMANDS:
                    raise ValueError("Ungültiges Kommando")
                send_json(self, send_brew_command(command))
            except Exception as exc:
                send_json(self, {"error": str(exc)}, status_code=400)
            return
        if self.path == "/api/abort":
            length = int(self.headers.get("Content-Length", 0))
            if length:
                self.rfile.read(length)
            try:
                send_json(self, abort_brew())
            except Exception as exc:
                send_json(self, {"error": str(exc)}, status_code=400)
            return
        if self.path == "/api/hardware-off":
            length = int(self.headers.get("Content-Length", 0))
            if length:
                self.rfile.read(length)
            try:
                send_json(self, hardware_off())
            except Exception as exc:
                send_json(self, {"error": str(exc)}, status_code=400)
            return
        if self.path == "/api/select":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length) if length else b"{}"
            try:
                payload = json.loads(body or b"{}")
                target = payload.get("path")
                if not target:
                    raise ValueError
                if is_brew_running():
                    send_json(self, {"error": "Brauvorgang läuft, Auswahl nicht möglich."}, status_code=400)
                    return
                selection = write_selection(target)
                reset_status_file(message=f"Rezept gewechselt: {selection.get('name')}")
                send_json(self, selection)
            except Exception:
                send_json(self, {"error": "Ungültiger Rezeptpfad"}, status_code=400)
            return
        self.send_error(404)


def cleanup_previous_runs() -> None:
    try:
        subprocess.run(["pkill", "-f", "gf_brew_flow"], check=False)
    except Exception:
        pass


def main() -> None:
    socketserver.TCPServer.allow_reuse_address = True
    os.chdir(ROOT)
    ensure_selection_file()
    ensure_status_file()
    cleanup_previous_runs()
    handler = partial(DashboardHandler, directory=ROOT)
    bind_address = os.getenv("GF_BIND_ADDRESS", "127.0.0.1")
    with socketserver.TCPServer((bind_address, PORT), handler) as httpd:
        url = f"http://localhost:{PORT}/dashboard/index.html"
        print(f"Dashboard läuft unter {url}")
        webbrowser.open(url)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("Server beendet.")


if __name__ == "__main__":
    main()
