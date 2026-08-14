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
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
INCIDENTS_FILE = os.path.join(PROJECT_ROOT, "INCIDENTS.md")
STABILITY_FILE = os.path.join(PROJECT_ROOT, "STABILITY_REPORT.md")

if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

LAST_AUDITED_POS = {}
LAST_INCIDENTS = {}

def log_incident(component, event_type, description):
    now_ts = time.time()
    key = (component, event_type, description[:50])
    last_record = LAST_INCIDENTS.get(key)
    if last_record and (now_ts - last_record) < 300:
        return
    LAST_INCIDENTS[key] = now_ts

    timestamp = datetime.now().strftime("%d.%m.%Y %H:%M")
    line = f"| {timestamp} | {component} | {event_type} | {description} |\n"
    
    if not os.path.exists(INCIDENTS_FILE):
        with open(INCIDENTS_FILE, "w", encoding="utf-8") as f:
            f.write("# Журнал инцидентов CryptoSpread\n\n| Время (UTC) | Компонент | Тип события | Описание и статус |\n| :--- | :--- | :--- | :--- |\n")
    
    with open(INCIDENTS_FILE, "a", encoding="utf-8") as f:
        f.write(line)

def update_stability_report():
    timestamp = datetime.now().strftime("%d.%m.%Y %H:%M")
    report = f"\n## Hourly Status Update: {timestamp}\n"
    
    for name, cmd in PROCESSES.items():
        search_str = cmd[-1] if len(cmd) > 1 else cmd[0]
        proc = is_process_running(search_str)
        status = f"ALIVE (PID {proc.pid})" if proc else "DEAD"
        report += f"- **{name}**: {status}\n"
    
    with open(STABILITY_FILE, "a", encoding="utf-8") as f:
        f.write(report)

def is_process_running(name_part):
    search_norm = name_part.replace('\\', '/')
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = " ".join(proc.info['cmdline'] or []).replace('\\', '/')
            if search_norm in cmdline and proc.pid != os.getpid():
                return proc
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None

def start_process(name, command):
    log_file_name = f"{name.lower().replace(' ', '_')}.log"
    log_path = os.path.join(LOGS_DIR, log_file_name)
    
    # Открываем файл в режиме добавления с UTF-8
    log_file = open(log_path, "a", encoding="utf-8")
    try:
        print(f"[*] Starting {name}, logging to {log_path}...")
        proc = subprocess.Popen(
            command, 
            cwd=PROJECT_ROOT, 
            stdout=log_file, 
            stderr=subprocess.STDOUT, 
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
        )
        return proc
    finally:
        log_file.close()

def audit_logs():
    """Сканирует только новые строки логов на наличие ERROR без считывания всего файла."""
    for name in PROCESSES.keys():
        log_file_name = f"{name.lower().replace(' ', '_')}.log"
        log_path = os.path.join(LOGS_DIR, log_file_name)
        
        if os.path.exists(log_path):
            try:
                file_size = os.path.getsize(log_path)
                last_pos = LAST_AUDITED_POS.get(log_file_name, max(0, file_size - 8192))
                if file_size < last_pos:
                    last_pos = max(0, file_size - 8192)
                
                with open(log_path, "rb") as f:
                    f.seek(last_pos)
                    chunk = f.read()
                    LAST_AUDITED_POS[log_file_name] = f.tell()
                    
                    if chunk:
                        text = chunk.decode("utf-8", errors="ignore")
                        lines = text.splitlines()[-50:]
                        for line in lines:
                            if "ERROR" in line.upper() and "[AUDIT IGNORE]" not in line:
                                if "ClientConnectorError" in line or "Flood control" in line:
                                    continue
                                log_incident(name, "LOG_ERROR", line.strip()[:100])
            except Exception:
                pass

def monitor():
    print(f"[!] CryptoSpread Intelligent Watchdog started at {datetime.now()}")
    last_stability_update = 0

    while True:
        current_time = time.time()
        
        # Раз в час обновляем отчет о стабильности
        if current_time - last_stability_update > 3600:
            update_stability_report()
            last_stability_update = current_time

        for name, cmd in PROCESSES.items():
            search_str = cmd[-1] if len(cmd) > 1 else cmd[0]
            proc = is_process_running(search_str)
            
            if proc is None:
                print(f"[!] {name} is NOT running. Restarting...")
                log_incident(name, "CRASH", "Process disappeared. Restarting...")
                start_process(name, cmd)
            else:
                if not proc.is_running():
                    print(f"[!] {name} (PID {proc.pid}) is stuck. Restarting...")
                    log_incident(name, "STUCK", f"Process {proc.pid} unresponsive. Restarting...")
                    try:
                        proc.kill()
                    except:
                        pass
                    start_process(name, cmd)

        audit_logs()
        time.sleep(30)

if __name__ == "__main__":
    try:
        monitor()
    except KeyboardInterrupt:
        print("\n[!] Watchdog stopped.")
