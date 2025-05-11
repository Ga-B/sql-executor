# Standard library imports
import argparse
import os
import pathlib
import sys
from datetime import datetime

# Custom library imports
from sql_executor import collect_sorted_file_paths

timestamp = datetime.now().strftime("%Y-%m-%d__%H-%M-%S")
LOG_DIR = pathlib.Path(f"./logs/{timestamp}")

try:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
except OSError as e:
    print(
        f"WARNING: Could not create log directory '{LOG_DIR}'. "
        f"File logging disabled. Error: {e}",
        file=sys.stderr,
    )
    LOG_DIR = None

SQL_BASE_DIR = pathlib.Path("../")  # Can be overridden by argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "List SQL scripts in a specified directory sorted naturally. "
            "Listing is recursive using the 'natsort' module."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-d",
        "--sql-dir",
        type=pathlib.Path,
        default=SQL_BASE_DIR,
        help=(
            "Path to the directory containing '*.sql' files "
            f"(default: {SQL_BASE_DIR})"
        ),
    )

    try:
        args = parser.parse_args()
        directory = args.sql_dir
    except Exception as e:
        print(
            f"Halting execution. Error parsing command-line arguments: {e}",
            file=sys.stderr
        )
        sys.exit(2)

    paths_found = collect_sorted_file_paths(base_path=directory, ext=".sql")
    files_found_count = len(paths_found.get('files_found', []))
    anomalies_count = len(paths_found.get('anomalies', []))

    try:
        for prefix in ["files_found", "anomalies"]:
            file_header = f"Listing: '{prefix}' | Date: {timestamp}"
            log_file = LOG_DIR / f"{prefix}.log"
            with open(log_file, "w", encoding="utf-8") as f:
                f.write(file_header)
                f.write(f"\n{'-'*len(file_header)}\n")
                f.write(
                    "\n".join(map(str, paths_found.get(prefix)))
                    if paths_found.get(prefix)
                    else "None"
                )
        print(f"Found paths logged to './{LOG_DIR}'")
    except Exception as report_err:
            print(
                f"Failed to write report files: {report_err}", exc_info=True
            )