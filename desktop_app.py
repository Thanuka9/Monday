import os
import sys
import time
import socket
import shutil
import subprocess
import ctypes
import tempfile
import psutil

from PyQt5.QtCore import QUrl, QTimer
from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox
from PyQt5.QtWebEngineWidgets import QWebEngineView

PORT = 8501
MUTEX_NAME = "MondayAuditorDesktopAppMutex"
LOCK_FILE = os.path.join(tempfile.gettempdir(), "monday_app.lock")


# =============================
# FORCE KILL OLD INSTANCES
# =============================
def kill_existing_instances():
    current_pid = os.getpid()
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if proc.info['pid'] != current_pid:
                if "desktop_app" in proc.info['name'].lower():
                    proc.kill()
        except:
            pass


# =============================
# NETWORK
# =============================
def is_port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def wait_for_server(port: int, timeout: int = 30) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        if is_port_open(port):
            return True
        time.sleep(0.5)
    return False


# =============================
# PATH
# =============================
def get_base_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def get_app_path(base_dir: str) -> str:
    if getattr(sys, "frozen", False):
        paths = [
            os.path.join(base_dir, "monday_auditor.py"),
            os.path.join(sys._MEIPASS, "monday_auditor.py"),
        ]
        for p in paths:
            if os.path.exists(p):
                return p
    else:
        return os.path.join(base_dir, "monday_auditor.py")
    return None


# =============================
# PYTHON FINDER
# =============================
def find_python():
    # Always use current Python (fixes embedded interpreter error)
    return sys.executable


# =============================
# SINGLE INSTANCE LOCK (HARD)
# =============================
def acquire_lock():
    if os.path.exists(LOCK_FILE):
        return False
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    return True


def release_lock():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)


# =============================
# UI
# =============================
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Monday Auditor")
        self.resize(1280, 840)

        self.browser = QWebEngineView()
        self.setCentralWidget(self.browser)

    def load_app(self):
        self.browser.setUrl(QUrl(f"http://127.0.0.1:{PORT}"))


# =============================
# MAIN
# =============================
def main():
    kill_existing_instances()

    if not acquire_lock():
        app = QApplication(sys.argv)
        QMessageBox.information(None, "Monday Auditor", "Already running")
        return 0

    base_dir = get_base_dir()
    app_path = get_app_path(base_dir)

    if not app_path or not os.path.exists(app_path):
        app = QApplication(sys.argv)
        QMessageBox.critical(None, "Error", "monday_auditor.py not found")
        return 1

    streamlit_proc = None

    # =============================
    # START STREAMLIT
    # =============================
    if not is_port_open(PORT):
        python_exe = find_python()

        flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0

        streamlit_proc = subprocess.Popen(
            [
                python_exe,
                "-m",
                "streamlit",
                "run",
                app_path,
                "--server.port",
                str(PORT),
                "--server.headless",
                "true",
                "--browser.serverAddress",
                "127.0.0.1",
            ],
            cwd=base_dir,
            creationflags=flags,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        if not wait_for_server(PORT, 30):
            app = QApplication(sys.argv)
            QMessageBox.critical(None, "Error", "Streamlit failed to start")
            return 1

    # =============================
    # UI
    # =============================
    qt_app = QApplication(sys.argv)
    window = MainWindow()
    window.show()

    QTimer.singleShot(1500, window.load_app)

    exit_code = qt_app.exec_()

    # =============================
    # CLEANUP (CRITICAL FIX)
    # =============================
    if streamlit_proc:
        try:
            parent = psutil.Process(streamlit_proc.pid)
            for child in parent.children(recursive=True):
                child.kill()
            parent.kill()
        except:
            pass

    release_lock()

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())