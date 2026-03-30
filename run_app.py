"""
run_app.py — Monday Auditor (Clean Native Architecture)
"""
import sys
import os
import socket
import time
import webbrowser
import multiprocessing

def get_base_path():
    """Locate the bundled files whether running as script or frozen exe."""
    if getattr(sys, "frozen", False):
        return sys._MEIPASS  # type: ignore
    return os.path.dirname(os.path.abspath(__file__))

def is_port_open(port):
    """Check if the Streamlit server has booted up yet."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0

def run_server(app_path, port):
    """The isolated Streamlit worker process."""
    # 1. Clean environment
    os.environ["STREAMLIT_SERVER_PORT"] = str(port)
    os.environ["STREAMLIT_SERVER_HEADLESS"] = "true"
    os.environ["STREAMLIT_SERVER_FILE_WATCHER_TYPE"] = "none"
    os.environ["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"
    
    # 🛑 THE FIX: Force Streamlit out of Development Mode so we can set the port
    os.environ["STREAMLIT_GLOBAL_DEVELOPMENT_MODE"] = "false"

    # 2. Prevent void output crashes in PyInstaller
    if sys.stdout is None: sys.stdout = open(os.devnull, "w")
    if sys.stderr is None: sys.stderr = open(os.devnull, "w")

    # 3. Launch Streamlit
    sys.argv = ["streamlit", "run", app_path]
    from streamlit.web import cli
    cli.main()

if __name__ == "__main__":
    # MUST be the very first thing for Windows multiprocessing safety
    multiprocessing.freeze_support()

    print("Starting Monday Auditor server...")
    app_path = os.path.join(get_base_path(), "monday_auditor.py")
    
    if not os.path.exists(app_path):
        print(f"Error: Could not find {app_path}")
        input("Press Enter to exit...")
        sys.exit(1)

    # Find an open port
    port = 8501
    while is_port_open(port):
        port += 1

    # Start the Streamlit server in a background process
    server_process = multiprocessing.Process(
        target=run_server, 
        args=(app_path, port), 
        daemon=True
    )
    server_process.start()

    # Wait for the server to initialize
    print("Waiting for server to boot...")
    while not is_port_open(port):
        time.sleep(0.5)

    # Open the user's default web browser
    print(f"Server up! Opening browser at http://localhost:{port}")
    webbrowser.open(f"http://localhost:{port}")

    print("\n" + "="*60)
    print(" ✅ Monday Auditor is running in your browser!")
    print("    Keep this console window open. Closing it will stop the app.")
    print("="*60 + "\n")

    try:
        # Keep the main process alive so the background server doesn't die
        server_process.join()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server_process.terminate()
        sys.exit(0)