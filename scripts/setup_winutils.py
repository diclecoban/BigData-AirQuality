"""Download winutils.exe and hadoop.dll for Windows PySpark.

Sets up C:\\hadoop\\bin\\ so Spark can write to the local filesystem on Windows.

Run ONCE before anything else:
  python scripts/setup_winutils.py
"""

import os
import sys
import urllib.request
from pathlib import Path

HADOOP_HOME = Path("C:/hadoop")
BIN_DIR     = HADOOP_HOME / "bin"

# Hadoop 3.3.6 winutils (matches PySpark 3.5.x bundled Hadoop version)
FILES = {
    "winutils.exe": "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe",
    "hadoop.dll":   "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll",
}


def download(url: str, dest: Path) -> None:
    print(f"  Downloading {dest.name} ...", end=" ", flush=True)
    urllib.request.urlretrieve(url, dest)
    size_kb = dest.stat().st_size // 1024
    print(f"OK ({size_kb} KB)")


def set_env_permanently() -> None:
    """Write HADOOP_HOME to user environment via reg (Windows only)."""
    import subprocess
    hadoop_str = str(HADOOP_HOME)
    subprocess.run(
        ["setx", "HADOOP_HOME", hadoop_str],
        check=True, capture_output=True,
    )
    print(f"  HADOOP_HOME set permanently to {hadoop_str}")
    print("  (Restart your terminal for the permanent setting to take effect.)")


def main() -> None:
    print("Setting up winutils for Windows PySpark")
    print(f"  Target: {HADOOP_HOME}")

    BIN_DIR.mkdir(parents=True, exist_ok=True)

    for filename, url in FILES.items():
        dest = BIN_DIR / filename
        if dest.exists():
            print(f"  {filename} already present — skipping.")
            continue
        download(url, dest)

    # Set for the current process (takes effect immediately for child processes)
    os.environ["HADOOP_HOME"]       = str(HADOOP_HOME)
    os.environ["hadoop.home.dir"]   = str(HADOOP_HOME)
    os.environ["PATH"]              = str(BIN_DIR) + ";" + os.environ.get("PATH", "")

    # Persist to user environment
    try:
        set_env_permanently()
    except Exception as e:
        print(f"  Could not set permanently ({e}). Use the env-var block in run_pipeline.py instead.")

    print("\nDone. winutils is ready.")
    print("Next step: python scripts/run_pipeline.py")


if __name__ == "__main__":
    if sys.platform != "win32":
        print("This script is only needed on Windows.")
        sys.exit(0)
    main()
