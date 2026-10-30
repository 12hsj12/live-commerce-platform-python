from __future__ import annotations

import argparse
import subprocess
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description="Syntax/build check for the Python backend.")
    parser.add_argument("--check-dependencies", action="store_true", help="also run pip check")
    args = parser.parse_args()
    subprocess.run([sys.executable, "-m", "compileall", "-q", "app", "tools", "tests"], check=True)
    if args.check_dependencies:
        subprocess.run([sys.executable, "-m", "pip", "check"], check=True)
    print("Python syntax compilation succeeded")


if __name__ == "__main__":
    main()
