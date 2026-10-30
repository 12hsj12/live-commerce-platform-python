from __future__ import annotations

import argparse
import subprocess
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Python test suite.")
    parser.add_argument("pytest_args", nargs=argparse.REMAINDER,
                        help="optional pytest arguments, for example -k inventory")
    args = parser.parse_args()
    command = [sys.executable, "-m", "pytest", *(args.pytest_args or ["-q"])]
    raise SystemExit(subprocess.call(command))


if __name__ == "__main__":
    main()
