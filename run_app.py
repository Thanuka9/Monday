import sys
import subprocess
import socket
import time
import os

from PyQt5.QtWidgets import QApplication, QMainWindow
from PyQt5.QtWebEngineWidgets import QWebEngineView

PORT = 8501


def is_port_open(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


def start_streamlit():
    if is_port_open(PORT):
        return None  # already running

    current_dir = os.path.dirname(os.path.abspath(__file__))
    app_path = os.path.join(current_dir, "monday_auditor.py")

    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            app_path,
            "--server.port",
            str(PORT),
            "--server.headless",
            "true",
        ],
    )


def wait_for_server():
    timeout = 20
    start = time.time()

    while time.time() - start < timeout:
        if is_port_open(PORT):
            return True
        time.sleep(1)

    return False


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Monday Auditor")
        self.setGeometry(100, 100, 1200, 800)

        self.browser = QWebEngineView()
        self.setCentralWidget(self.browser)

        self.browser.setUrl(f"http://localhost:{PORT}")


if __name__ == "__main__":
    process = start_streamlit()

    if not wait_for_server():
        print("Streamlit failed to start")
        sys.exit()

    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()

    exit_code = app.exec_()

    if process:
        process.terminate()

    sys.exit(exit_code)