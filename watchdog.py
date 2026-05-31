import subprocess
import time
import os
import sys
import psutil
from datetime import datetime

# Настройки
PROCESSES = {
    "Rust Core": ["powershell.exe", "-Command", "cd rust_core; $env:RUST_LOG='info'; cargo run --release"],
    "Signal Engine": ["uv", "run", "signal_engine/main.py"],
    "Telegram Bot": ["uv", "run", "telegram_bot/main.py"],
    "Volume Monitor": ["uv", "run", "signal_engine/volume_monitor.py"]
}

PROJECT_ROOT = os.getcwd()
INCIDENTS_FILE = os.path.join(PROJECT_ROOT, "INCIDENTS.md")

def log_incident(component, event_type, description):
    timestamp = datetime.now().strftime("%d.%m.%Y %H:%M")
    line = f"| {timestamp} | {component} | {event_type} | {description} |\n"
    
    if not os.path.exists(INCIDENTS_FILE):
        with open(INCIDENTS_FILE, "w", encoding="utf-8") as f:
            f.write("# Журнал инцидентов CryptoSpread\n\n| Время (UTC) | Компонент | Тип события | Описание и статус |\n| :--- | :--- | :--- | :--- |\n")
    
    with open(INCIDENTS_FILE, "a", encoding="utf-8") as f:
        f.write(line)

def is_process_running(name_part):
    """Проверяет, запущен ли процесс по фрагменту командной строки."""
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = " ".join(proc.info['cmdline'] or [])
            if name_part in cmdline and proc.pid != os.getpid():
                return proc
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None

def start_process(name, command):
    print(f"[*] Starting {name}...")
    # Используем CREATE_NEW_CONSOLE для Windows, чтобы видеть окна (опционально)
    # Здесь запускаем в фоне
    return subprocess.Popen(command, cwd=PROJECT_ROOT, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)

def monitor():
    print(f"[!] CryptoSpread Watchdog started at {datetime.now()}")
    active_procs = {}

    while True:
        for name, cmd in PROCESSES.items():
            # Ищем процесс по уникальному фрагменту команды
            search_str = cmd[-1] if len(cmd) > 1 else cmd[0]
            proc = is_process_running(search_str)
            
            if proc is None:
                print(f"[!] {name} is NOT running. Restarting...")
                log_incident(name, "CRASH", "Process disappeared from system. Restarting...")
                new_proc = start_process(name, cmd)
                active_procs[name] = new_proc
            else:
                # Если процесс есть, проверяем не "зомби" ли он
                if not proc.is_running():
                    print(f"[!] {name} (PID {proc.pid}) is stuck. Killing and restarting...")
                    log_incident(name, "STUCK", f"Process {proc.pid} is unresponsive. Restarting...")
                    proc.kill()
                    start_process(name, cmd)

        time.sleep(30) # Проверка каждые 30 секунд

if __name__ == "__main__":
    try:
        monitor()
    except KeyboardInterrupt:
        print("\n[!] Watchdog stopped.")
