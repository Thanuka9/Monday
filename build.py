"""
build.py — Packages Monday Auditor into a standalone Windows desktop app.

Usage (run once from your project root with the venv active):
    python build.py

Output:
    dist/MondayAuditor/          ← the distributable folder
    dist/MondayAuditor/MondayAuditor.exe

Requirements (install first if missing):
    pip install pyinstaller
"""

import subprocess
import sys
import os
import shutil


# ── Clean up old build files ──────────────────────────────────────────────────
def cleanup_old_builds(project_root: str):
    """Removes old build, dist, and .spec files to ensure a clean build."""
    print("Cleaning up old build files...")
    paths_to_remove = [
        os.path.join(project_root, "build"),
        os.path.join(project_root, "dist"),
        os.path.join(project_root, "MondayAuditor.spec"),
    ]
    
    for path in paths_to_remove:
        if os.path.exists(path):
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                    print(f"  Deleted directory: {path}")
                else:
                    os.remove(path)
                    print(f"  Deleted file: {path}")
            except Exception as e:
                print(f"  Warning: Could not delete {path} ({e})")
        else:
            print(f"  Already clean: {path}")
    print("Cleanup complete.\n")


# ── Locate Streamlit's package directory ─────────────────────────────────────
def find_package_dir(package: str) -> str:
    import importlib.util
    spec = importlib.util.find_spec(package)
    if spec is None:
        raise RuntimeError(f"Package '{package}' not found in current environment.")
    # spec.submodule_search_locations gives the folder
    return list(spec.submodule_search_locations)[0]


def main():
    print("=" * 60)
    print("  Monday Auditor — PyInstaller Build")
    print("=" * 60)

    # ── Resolve paths ─────────────────────────────────────────────────────────
    project_root   = os.path.dirname(os.path.abspath(__file__))
    entry_point    = os.path.join(project_root, "run_app.py")
    auditor_script = os.path.join(project_root, "monday_auditor.py")

    if not os.path.exists(entry_point):
        sys.exit(f"ERROR: run_app.py not found at {entry_point}")
    if not os.path.exists(auditor_script):
        sys.exit(f"ERROR: monday_auditor.py not found at {auditor_script}")

    # ── 1. Wipe out old builds first! ─────────────────────────────────────────
    cleanup_old_builds(project_root)

    # ── Collect Streamlit static/component data ───────────────────────────────
    try:
        st_dir = find_package_dir("streamlit")
    except RuntimeError as e:
        sys.exit(f"ERROR: {e}\nMake sure your venv is active and streamlit is installed.")

    # Streamlit needs its entire package tree bundled (static files, config, etc.)
    datas = [
        # (source_path, dest_folder_inside_bundle)
        (st_dir, "streamlit"),
        # Bundle monday_auditor.py so run_app.py can find it at runtime
        (auditor_script, "."),
    ]

    # ── Hidden imports needed for Streamlit + pandas + requests ───────────────
    # Note: PyQt5 has been removed to drastically reduce file size and stop crashes
    hidden_imports = [
        # Streamlit internals
        "streamlit",
        "streamlit.web.cli",
        "streamlit.web.server",
        "streamlit.runtime",
        "streamlit.runtime.scriptrunner",
        "streamlit.runtime.caching",
        "streamlit.components.v1",
        "streamlit.delta_generator",
        # Data / HTTP
        "pandas",
        "pandas._libs.tslibs.np_datetime",
        "pandas._libs.tslibs.nattype",
        "pandas._libs.tslibs.timedeltas",
        "requests",
        "urllib3",
        "certifi",
        "charset_normalizer",
        "idna",
        # Streamlit optional deps that are auto-discovered at runtime
        "altair",
        "pyarrow",
        "packaging",
        "pydeck",
        "validators",
        "click",
        "toml",
        "tornado",
        "watchdog",
        "gitpython",
    ]

    # ── Build the PyInstaller command ─────────────────────────────────────────
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--noconfirm",          # overwrite dist/ without asking
        "--onedir",             # folder bundle (faster startup than --onefile)
        # "--windowed",         # REMOVED: We need the console to keep our background server alive
        "--name", "MondayAuditor",
        "--icon", "NONE",       # replace NONE with path to a .ico file if you have one
        "--clean",
    ]

    # Add --add-data entries  (Windows uses ; as separator)
    sep = ";" if sys.platform == "win32" else ":"
    for src, dst in datas:
        cmd += ["--add-data", f"{src}{sep}{dst}"]

    # Add hidden imports
    for imp in hidden_imports:
        cmd += ["--hidden-import", imp]

    # Collect entire packages (ensures submodules & data are included)
    for pkg in ["streamlit", "altair", "pandas", "pydeck"]:
        cmd += ["--collect-all", pkg]

    cmd.append(entry_point)

    # ── Run PyInstaller ───────────────────────────────────────────────────────
    print("\nRunning PyInstaller...\n")
    print(" ".join(cmd))
    print()

    result = subprocess.run(cmd, cwd=project_root)

    if result.returncode != 0:
        sys.exit("\nBuild FAILED. See errors above.")

    # ── Post-build: copy monday_auditor.py next to the .exe (safety net) ──────
    dist_dir = os.path.join(project_root, "dist", "MondayAuditor")
    auditor_dest = os.path.join(dist_dir, "monday_auditor.py")
    if not os.path.exists(auditor_dest):
        shutil.copy2(auditor_script, auditor_dest)
        print(f"Copied monday_auditor.py → {auditor_dest}")

    print("\n" + "=" * 60)
    print("  BUILD SUCCESSFUL")
    print(f"  App folder: {dist_dir}")
    print(f"  Executable: {os.path.join(dist_dir, 'MondayAuditor.exe')}")
    print("=" * 60)
    print("\nTo distribute: zip the entire dist/MondayAuditor/ folder.")


if __name__ == "__main__":
    main()