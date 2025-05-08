# Standard library imports
import argparse
from datetime import datetime
import logging
import os
import pathlib
import sys
import time

# Third-party imports
import psycopg2
from dotenv import load_dotenv
from natsort import natsorted

# Required for transaction status check before rollback
from psycopg2.extensions import TRANSACTION_STATUS_INTRANS


# ===== 1. LOGGING SETUP =====

# --- Create log directory if it doesn't exist ---
# Reports if it cannot be created, disabling file logging
timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
LOG_DIR = pathlib.Path(f"./logs/{timestamp}")
LOG_FILE = LOG_DIR / f"sql_executor_{timestamp}.log"

try:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
except OSError as e:
    print(
        f"WARNING: Could not create log directory '{LOG_DIR}'. "
        f"File logging disabled. Error: {e}",
        file=sys.stderr,
    )
    LOG_FILE = None

# --- Configure root logger ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_format = (
    "%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s"
)
date_format = "%Y-%m-%d %H:%M:%S"
formatter = logging.Formatter(log_format, datefmt=date_format)

# --- Console logging handler ---
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# --- File logging handler ---
if LOG_FILE:
    try:
        file_handler = logging.FileHandler(
            LOG_FILE, mode="a", encoding="utf-8"
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.info(f"Logging to console and to file {LOG_FILE}")
    except Exception as e:
        logger.error(
            f"Failed to set up logging to file {LOG_FILE}: {e}", exc_info=True
        )


# ===== 2. DATABASE CONFIGURATION =====

# --- Database parameters ---
# An .env file can be used to define DB_PARAMS. It won't override
# environmental variables already defined. If no .env or environmental
# variables are defined, DB_PARAMS defaults to the Postgres official
# Docker image parameters.
load_dotenv()
DB_PARAMS = {
    "dbname": os.getenv("DB_NAME", default="postgres"),
    "user": os.getenv("DB_USER", default="postgres"),
    "password": os.getenv("DB_PASS", default="mysecretpassword"),
    "host": os.getenv("DB_HOST", default="localhost"),
    "port": os.getenv("DB_PORT", default=5432),
}

# --- Scripts directory ---
# All *.sql files in SQL_BASE_DIR will be executed
# in natural order (e.g. 1, 2, 10)
SQL_BASE_DIR = pathlib.Path("../")  # Can be overridden by argparse


# ===== 3. DATABASE CONNECTION =====


def connect_db(params, attempt_limit=5, delay=3):
    """
    Attempts to establish a connection to the PostgreSQL database, 
    with retries. Defaults to 5 attempts with 3 seconds of delay.
    """
    for attempt in range(1, attempt_limit + 1):
        try:
            logging.info(
                f"Attempting database connection "
                f"(attempt {attempt}/{attempt_limit})..."
            )
            connection = psycopg2.connect(**params)
            logging.info("Database connection established successfully.")
            return connection
        except psycopg2.OperationalError as op_err:
            logging.warning(f"Connection attempt {attempt} failed: {op_err}")
            if attempt < attempt_limit:
                logging.info(f"Retrying connection in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.critical("Maximum connection attempts reached.")
        except Exception as e:
            logging.critical(
                f"Unexpected error during connection attempt {attempt}: {e}",
                exc_info=True,
            )
            break

    # Return None if DB connection fails in the loop
    logging.error(
        f"Failed to establish database connection after {attempt_limit} attempts."
    )
    return None


# ===== 4. FILE RETRIEVAL =====


def collect_file_paths(dir_path, ext):
    """
    Finds files in the specified directory matching the given extension,
    recursively, and checks for file access/integrity.

    Returns a dictionary with two lists: 'files_found' (potentially valid
    paths) and 'anomalies' (broken/irregular/inaccessible paths).
    """
    logging.info(
        f"--- Searching recursively in '{dir_path}' (following symlinks)... ---"
    )
    files_found = []
    anomalies = []
    for root, _, files in os.walk(dir_path, followlinks=True):
        current_dir = pathlib.Path(root)
        for filename in files:
            if filename.endswith(ext):
                full_path = current_dir / filename
                try:
                    if full_path.is_file():
                        files_found.append(full_path)
                    else:
                        link_status = (
                            "a broken symlink?"
                            if full_path.is_symlink()
                            else "not a regular file:"
                        )
                        logging.warning(
                            f"Path matching *{ext} is {link_status} "
                            f"{full_path}. Skipping scan."
                        )
                        anomalies.append(full_path)
                except OSError as e:
                    logging.error(
                        f"Scan Error: Could not access '{full_path}'. "
                        f"Check permissions. Error: {e}"
                    )
                    anomalies.append(full_path)
                except Exception as e:
                    logging.error(
                        f"Unexpected Scan Error checking path {full_path}. "
                        f"Skipping. Error: {e}",
                        exc_info=True,
                    )
                    anomalies.append(full_path)
    return {"files_found": files_found, "anomalies": anomalies}


# ===== 5. HELPER FUNCTIONS for Script Processing =====


def _ensure_connection(current_conn, params):
    """
    Checks connection, and attempts reconnect if needed.
    Returns a valid connection or None if it fails.
    """
    if current_conn and not current_conn.closed:
        return current_conn  # Connection is okay

    logging.warning(
        "Connection lost or not established. Attempting to (re)connect..."
    )
    new_conn = connect_db(params)  # Implements the main retry logic
    if not new_conn:
        logging.critical(
            "Halting processing: Unable to establish database connection."
        )
        return None
    return new_conn


def _read_sql_file(sql_file_path):
    """Reads SQL file, returns content or None if empty/whitespace."""
    try:
        sql_script = sql_file_path.read_text(encoding="utf-8")
        if not sql_script.strip():
            logging.warning(
                f"EMPTY FILE: SQL script {sql_file_path} is empty. Skipping."
            )
            return None
        return sql_script
    except (OSError, UnicodeDecodeError) as file_err:
        log_prefix = (
            "FILE ERROR"
            if isinstance(file_err, OSError)
            else "ENCODING ERROR (Need UTF-8)"
        )
        logging.error(
            f"{log_prefix}: Cannot read/decode {sql_file_path}: {file_err}."
        )
        raise  # Re-raise to be handled by the caller


def _execute_sql(cursor, sql_script, sql_file_path):
    """Executes SQL script using the provided cursor."""
    try:
        logging.info(f"Executing SQL script from '{sql_file_path.name}'...")
        cursor.execute(sql_script)
        logging.info(f"Script '{sql_file_path.name}' executed successfully.")
    except psycopg2.Error as db_err:
        logging.error(
            f"DATABASE EXECUTION ERROR for script '{sql_file_path}': {db_err}"
        )
        logging.error(
            f"SQLSTATE: {db_err.pgcode}. Error details: {db_err.pgerror}"
        )
        raise  # Re-raise to be handled by the caller


def _attempt_commit(conn, sql_file_path, mode):
    """
    Attempts to commit the transaction for each file in 'per-file' and
    'per-file-until-error' modes.
    """
    try:
        conn.commit()
        logging.info(
            f"Transaction committed for '{sql_file_path.name}' ({mode} mode)."
        )
    except psycopg2.Error as commit_err:
        logging.error(
            f"DATABASE COMMIT ERROR for script '{sql_file_path}': {commit_err}"
        )
        logging.error(
            f"SQLSTATE: {commit_err.pgcode}. Error details: {commit_err.pgerror}"
        )
        raise  # Re-raise to be handled by the caller


def _attempt_rollback(conn, context_msg=""):
    """Attempts to rollback the current transaction, logs outcome."""
    try:
        # Check if rollback is possible/needed
        if conn and not conn.closed:
            status = conn.get_transaction_status()
            if status == TRANSACTION_STATUS_INTRANS or status == 3:
                logging.warning(f"Attempting rollback ({context_msg}). "
                                f"Current status: {status}")
                conn.rollback()
                logging.info(f"Rollback successful ({context_msg}).")
            else:
                logging.info(
                    f"No active or error transaction to rollback ({context_msg}). Status: {status}"
                )
        else:
            logging.warning(f"Cannot rollback, connection closed or None ({context_msg}).")
    except psycopg2.Error as rb_e:
        logging.error(f"Rollback failed ({context_msg}): {rb_e}")
    except Exception as e:
        logging.error(
            f"Unexpected error during rollback ({context_msg}): {e}",
            exc_info=True,
        )


# ===== 6. MODE-SPECIFIC PROCESSING WORKFLOWS =====


def _process_per_file(conn, db_params, sorted_sql_files):
    """Workflow for 'per-file' transaction mode."""
    processed = []  # Files successfully committed
    errors = []
    empty_files = []
    fatal_error_occurred = False  # Indicates if connection died permanently
    cursor = None

    for sql_file_path in sorted_sql_files:
        logging.info(
            f"***** Processing file: {sql_file_path} (per-file mode) *****"
        )

        conn = _ensure_connection(conn, db_params)
        if not conn:
            fatal_error_occurred = True  # Permanent connection failure is fatal
            logging.error("Skipping remaining files due to lost connection.")
            break  # Stop processing loop

        try:
            sql_script = _read_sql_file(sql_file_path)
            if sql_script is None:
                empty_files.append(sql_file_path)
                continue  # Skip to next file

            # Get a cursor for this file's transaction
            if cursor and not cursor.closed:
                try:
                    cursor.close()
                except Exception:
                    pass  # Ignore cursor close error
            cursor = conn.cursor()

            _execute_sql(cursor, sql_script, sql_file_path)
            _attempt_commit(conn, sql_file_path, "per-file")
            processed.append(sql_file_path)

        except (OSError, UnicodeDecodeError) as file_err:  # File read errors
            errors.append(sql_file_path)
            log_prefix = (
                "FILE ERROR"
                if isinstance(file_err, OSError)
                else "ENCODING ERROR (Need UTF-8)"
            )
            logging.error(
                f"{log_prefix}: Cannot read/decode {sql_file_path}: "
                f"{file_err}. Skipping."
            )
            continue  # Continue to next file

        except psycopg2.Error as db_err:  # Execution or Commit errors
            errors.append(sql_file_path)
            logging.error(
                f"DATABASE EXECUTION ERROR for script '{sql_file_path}': {db_err}"
            )
            if hasattr(db_err, 'pgcode') and hasattr(db_err, 'pgerror'):
                logging.error(f"SQLSTATE: {db_err.pgcode}. Error details: {db_err.pgerror}")
            _attempt_rollback(conn, f"error processing {sql_file_path.name}")
            continue  # Continue to next file

        except Exception as unexpected_err:  # Other errors
            errors.append(sql_file_path)
            logging.error(
                f"UNEXPECTED ERROR processing {sql_file_path}: {unexpected_err}",
                exc_info=True,
            )
            _attempt_rollback(conn, f"unexpected error for {sql_file_path.name}")
            continue  # Continue to next file
        finally:
            # Close cursor after each file in per-file mode
            if cursor and not cursor.closed:
                try:
                    cursor.close()
                except Exception:
                    pass

    return {
        "processed": processed,
        "errors": errors,
        "empty_files": empty_files,
        "fatal_error_occurred": fatal_error_occurred,
        "failed_all_or_nothing": False,
        "connection": conn,
    }


def _process_per_file_until_error(conn, db_params, sorted_sql_files):
    """Workflow for 'per-file-until-error' transaction mode."""
    processed = []  # Files successfully committed
    errors = []
    empty_files = []
    fatal_error_occurred = False  # Set to True on first error
    cursor = None

    for sql_file_path in sorted_sql_files:
        logging.info(
            f"***** Processing file: {sql_file_path} "
            f"(per-file-until-error mode) *****"
        )

        conn = _ensure_connection(conn, db_params)
        if not conn:
            fatal_error_occurred = True
            errors.append(sql_file_path)
            break

        try:
            sql_script = _read_sql_file(sql_file_path)
            if sql_script is None:
                empty_files.append(sql_file_path)
                continue

            # Get cursor
            if cursor and not cursor.closed:
                try:
                    cursor.close()
                except Exception:
                    pass
            cursor = conn.cursor()

            _execute_sql(cursor, sql_script, sql_file_path)
            _attempt_commit(conn, sql_file_path, "per-file-until-error")
            processed.append(sql_file_path)

        except (OSError, UnicodeDecodeError) as file_err: 
            # File read errors are fatal
            errors.append(sql_file_path)
            log_prefix = (
                "FILE ERROR"
                if isinstance(file_err, OSError)
                else "ENCODING ERROR (Need UTF-8)"
            )
            logging.critical(
                f"{log_prefix}: Cannot read/decode {sql_file_path}: "
                f"{file_err}. Skipping. Halting processing"
            )
            fatal_error_occurred = True
            _attempt_rollback(conn, f"file error {sql_file_path.name}")
            break  # Stop processing

        except psycopg2.Error:  # Execution or Commit errors are fatal
            errors.append(sql_file_path)
            logging.critical(
                f"Halting processing due to DB/commit error in "
                f"'{sql_file_path.name}'."
            )
            fatal_error_occurred = True
            _attempt_rollback(conn, f"DB/commit error {sql_file_path.name}")
            break  # Stop processing

        except Exception as unexpected_err:  # Other errors are fatal
            errors.append(sql_file_path)
            logging.error(
                f"UNEXPECTED ERROR processing {sql_file_path}: {unexpected_err}",
                exc_info=True,
            )
            logging.critical(
                "Halting processing due to unexpected error in "
                f"'{sql_file_path.name}'."
            )
            fatal_error_occurred = True
            _attempt_rollback(conn, f"unexpected error {sql_file_path.name}")
            break  # Stop processing
        finally:
            # Close cursor
            if cursor and not cursor.closed:
                try:
                    cursor.close()
                except Exception:
                    pass

    return {
        "processed": processed,
        "errors": errors,
        "empty_files": empty_files,
        "fatal_error_occurred": fatal_error_occurred,
        "failed_all_or_nothing": False,
        "connection": conn,
    }


def _process_all_or_nothing(conn, db_params, sorted_sql_files):
    """Workflow for 'all-or-nothing' transaction mode."""
    processed = []  # Files successfully executed
    errors = []
    empty_files = []
    fatal_error_occurred = False
    failed_all_or_nothing = False  # Specific flag for this mode
    cursor = None  # Single cursor for the whole transaction

    try:
        cursor = conn.cursor()  # Get cursor once at the beginning

        for sql_file_path in sorted_sql_files:
            logging.info(
                f"***** Processing file: {sql_file_path} (all-or-nothing mode) *****"
            )

            # Check connection before attempting execution
            conn = _ensure_connection(conn, db_params)
            if not conn:
                fatal_error_occurred = True
                failed_all_or_nothing = True
                errors.append(sql_file_path)
                _attempt_rollback(conn, "connection lost mid-transaction")
                break

            try:
                sql_script = _read_sql_file(sql_file_path)
                if sql_script is None:
                    empty_files.append(sql_file_path)
                    continue

                _execute_sql(cursor, sql_script, sql_file_path)
                processed.append(sql_file_path)  # Add to executed list
                logging.info(
                    f"Script '{sql_file_path.name}' added to transaction "
                    f"(all-or-nothing)."
                )

            except (OSError, UnicodeDecodeError) as file_err: 
                # File read errors are fatal
                errors.append(sql_file_path)
                log_prefix = (
                    "FILE ERROR"
                    if isinstance(file_err, OSError)
                    else "ENCODING ERROR (Need UTF-8)"
                )
                logging.critical(
                    f"{log_prefix}: Cannot read/decode {sql_file_path}: "
                    f"{file_err}. Halting processing"
                )
                fatal_error_occurred = True
                failed_all_or_nothing = True
                _attempt_rollback(conn, f"file error {sql_file_path.name}")
                break  # Stop processing

            except psycopg2.Error:  # Execution errors are fatal
                errors.append(sql_file_path)
                logging.critical(
                    "Halting transaction due to DB error in "
                    f"'{sql_file_path.name}'."
                )
                fatal_error_occurred = True
                failed_all_or_nothing = True
                _attempt_rollback(conn, f"DB error {sql_file_path.name}")
                break

            except Exception as unexpected_err:  # Other errors are fatal
                errors.append(sql_file_path)
                logging.error(
                    f"UNEXPECTED ERROR processing {sql_file_path}: {unexpected_err}",
                    exc_info=True,
                )
                logging.critical(
                    "Halting transaction due to unexpected error in "
                    f"'{sql_file_path.name}'."
                )
                fatal_error_occurred = True
                failed_all_or_nothing = True
                _attempt_rollback(conn, f"unexpected error {sql_file_path.name}")
                break

    except Exception as outer_err:
        # Error setting up cursor maybe?
        logging.error(
            "Unexpected error setting up all-or-nothing transaction: "
            f"{outer_err}",
            exc_info=True,
        )
        fatal_error_occurred = True
        failed_all_or_nothing = True
        _attempt_rollback(conn, "transaction setup error")

    finally:
        # Keep cursor open until final commit/rollback handled by caller
        pass

    return {
        "processed": processed,
        "errors": errors,
        "empty_files": empty_files,
        "fatal_error_occurred": fatal_error_occurred,
        "failed_all_or_nothing": failed_all_or_nothing,
        "connection": conn,
        "cursor": cursor,  # Return cursor for final handling
    }


# ===== 7. MAIN SCRIPT EXECUTION ORCHESTRATOR =====


def execute_sql_scripts_in_dir(conn, db_params, sql_dir, transaction_mode):
    """
    Orchestrates finding and executing '.sql' files based on transaction mode.

    Delegates processing to mode-specific functions. Handles final
    commit/rollback for 'all-or-nothing' and generates the summary report.
    """
    if not conn:
        logging.error("Initial database connection not available. Aborting.")
        return False

    results = {
        "processed": [],
        "file_anomalies": [],
        "errors": [],
        "empty_files": [],
        "fatal_error_occurred": False,
        "failed_all_or_nothing": False,
        "connection": conn,
        "cursor": None,
    }
    anomalies_count = 0
    cursor = None  # Keep track of cursor for final closing
    sorted_sql_files = []

    try:
        logging.info("===== Starting processing of SQL script files =====")
        logging.info(f"Transaction Mode: {transaction_mode}")

        if not sql_dir.is_dir():
            logging.error(f"SQL script directory not found: {sql_dir}. Halting.")
            return False

        # --- Scan for SQL files ---
        paths_found = collect_file_paths(dir_path=sql_dir, ext=".sql")
        sql_files_found = paths_found["files_found"]
        anomalies = paths_found["anomalies"]
        anomalies_count = len(anomalies)
        results["file_anomalies"] = natsorted(anomalies)

        # --- Check for Fatal Scanning Errors ---
        if anomalies_count > 0:
            logging.warning(
                f"Found {anomalies_count} unreadable files during path scanning."
            )
            if transaction_mode in ["all-or-nothing", "per-file-until-error"]:
                logging.warning(
                    "File anomalies found (check logs for details). "
                    "Halting execution before transaction start."
                )
                for item in natsorted(anomalies):
                    logging.warning(f" - File anomaly: {item}")
                results["fatal_error_occurred"] = True
                # results["file_anomalies"] is already populated before this block
                # If anomalies are fatal, add them to the "errors" list for reporting consistency
                # as the script didn't get to process them.
                results["errors"].extend(results["file_anomalies"]) # Or keep them separate in reporting
                return results

        # --- Sort Found Files ---
        logging.info(
            f"Found {len(sql_files_found)} potential *.sql file paths. "
            "Sorting naturally..."
        )
        sorted_sql_files = natsorted(sql_files_found, key=lambda p: p.as_posix())
        logging.info("--- Search and sorting complete. ---")

        if not sorted_sql_files:
            logging.warning(
                f"No valid SQL files found in {sql_dir}. Nothing to execute."
            )
            results["connection"] = conn
            return True

        # --- Delegate to Mode-Specific Workflow ---
        logging.info(
            f"--- Starting execution of {len(sorted_sql_files)} *.sql files. ---"
        )

        if transaction_mode == "per-file":
            # update results with the dictionary returned by the function
            results.update(
                _process_per_file(
                    results["connection"], db_params, sorted_sql_files
                )
            )
        elif transaction_mode == "per-file-until-error":
            results.update(
                _process_per_file_until_error(
                    results["connection"], db_params, sorted_sql_files
                )
            )
        elif transaction_mode == "all-or-nothing":
            results.update(
                _process_all_or_nothing(
                    results["connection"], db_params, sorted_sql_files
                )
            )
        else:
            logging.error(f"Unknown transaction mode: {transaction_mode}")
            results["fatal_error_occurred"] = True
            return False

        # Update main connection/cursor variables from results
        conn = results["connection"]
        cursor = results.get("cursor")  # Only all-or-nothing returns cursor

        logging.info("===== Finished iterating through SQL script files =====")

        # --- Final commit/rollback for 'all-or-nothing' ---
        if transaction_mode == "all-or-nothing":
            if not results["fatal_error_occurred"]:
                logging.info(
                    "Attempting final commit for 'all-or-nothing' transaction..."
                )
                try:
                    if conn and not conn.closed:
                        conn.commit()
                        logging.info(
                            "Final commit successful for 'all-or-nothing' mode."
                        )
                    elif conn is None or conn.closed:
                        raise psycopg2.OperationalError(
                            "Connection closed before final commit could occur."
                        )
                except psycopg2.Error as final_commit_e:
                    logging.critical(
                        f"CRITICAL: Final commit FAILED: {final_commit_e}"
                    )
                    logging.error(
                        f"SQLSTATE: {final_commit_e.pgcode}. "
                        f"Error details: {final_commit_e.pgerror}"
                    )
                    results["fatal_error_occurred"] = True
                    results["failed_all_or_nothing"] = True
                    _attempt_rollback(conn, "final commit failure")
                except Exception as final_err:
                    logging.critical(
                        "CRITICAL: Unexpected error during final commit "
                        f"attempt: {final_err}",
                        exc_info=True,
                    )
                    results["fatal_error_occurred"] = True
                    results["failed_all_or_nothing"] = True
                    _attempt_rollback(conn, "unexpected final commit error")

            elif results["fatal_error_occurred"]:
                logging.warning(
                    "Commit skipped for 'all-or-nothing' mode due to "
                    "earlier fatal errors."
                )
                results["failed_all_or_nothing"] = True

        # --- Final Summary ---
        logging.info("===== Execution Summary =====")
        logging.info(f"Mode: {transaction_mode}")
        if results["failed_all_or_nothing"]:
            description = "Files executed before fatal error/rollback:"
        else:
            description = "Successfully committed scripts:"
        logging.info(f"{description} {len(results['processed'])}")
        logging.info(f"Empty files found: {len(results['empty_files'])}")
        logging.info(f"Files skipped due to errors: {len(results['errors'])}")
        logging.info(f"Path scanning anomalies: {anomalies_count}")
        logging.info(f"Check log directory {LOG_DIR} for more details.")
        logging.info("=============================")

        return results

    except Exception as e:
        logging.error(
            f"Unexpected error during script execution orchestration: {e}",
            exc_info=True
        )
        results["fatal_error_occurred"] = True
        _attempt_rollback(conn, "unexpected orchestration error")
        results["connection"] = conn
        return results
    finally:
        # --- Cleanup: Close orchestrator-level cursor (if any) ---
        # The cursor in results is from _process_all_or_nothing.
        # Mode-specific functions should handle their own cursors if not returned.
        orchestrator_cursor = results.get("cursor")
        if orchestrator_cursor and not orchestrator_cursor.closed:
            try:
                orchestrator_cursor.close()
                logging.info("Cursor closed.")
            except Exception as e:
                logging.error(f"Error closing cursor: {e}")

        # --- Write Report Files ---
        log_prefix = f"{timestamp}_{transaction_mode.replace("-", "_")}"

        # If failed_all_or_nothing, no scripts were committed
        processed_type = ("executable_files" if results["failed_all_or_nothing"]
                          else "committed_files")
        viewed = set(results["processed"]
                     + results["errors"]
                     + results["empty_files"])
        results["unprocessed_files"] = [x for x in sorted_sql_files
                                        if x not in viewed]

        try:
            # Write logs listing empty files, file anomalies, etc.
            common_header = f"Mode: '{transaction_mode}' | Run: {timestamp}"
            for suffix in [
                "processed",
                "file_anomalies",
                "errors",
                "empty_files",
                "unprocessed_files"
            ]:                    
                if suffix == "processed":
                    log_file = LOG_DIR / f"{log_prefix}_{processed_type}.txt"
                else:
                    log_file = LOG_DIR / f"{log_prefix}_{suffix}.txt"
                with open(log_file, "w", encoding="utf-8") as f:
                    file_header = f"Listing: '{suffix}' | " + common_header
                    f.write(file_header)
                    f.write(f"\n{'-'*len(file_header)}\n")
                    f.write(
                        "\n".join(map(str, results.get(suffix)))
                        if results.get(suffix)
                        else "None"
                    )

            logging.info(
                f"Report files created in '{LOG_DIR}'"
            )
        except Exception as report_err:
            logging.error(
                f"Failed to write report files: {report_err}", exc_info=True
            )


# ===== 8. SCRIPT ENTRY POINT =====


if __name__ == "__main__":
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(
        description=(
            "Execute SQL scripts in a specified directory against a "
            "PostgreSQL database."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-t",
        "--transaction-mode",
        required=True,
        choices=["per-file", "all-or-nothing", "per-file-until-error"],
        help=(
            "Specify the database transaction behavior:\n"
            "  'per-file': Commit after each script. On DB error, rollback\n"
            "              that script & continue. File errors skip the file.\n"
            "  'all-or-nothing': Single transaction. Commit only if ALL scripts\n"
            "                    succeed. Halts and rolls back ENTIRE transaction\n"
            "                    on ANY error (scan, file, DB).\n"
            "  'per-file-until-error': Commit after each script. Halts processing\n"
            "                          and attempts rollback on the FIRST error\n"
            "                          encountered (scan, file, or DB)."
        ),
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
        sql_directory_to_use = args.sql_dir
    except Exception as e:
        print(
            f"Halting execution. Error parsing command-line arguments: {e}",
            file=sys.stderr
        )
        sys.exit(2)

    # --- Main Execution Flow ---
    logging.info("===== SQL script executor started =====")
    active_connection = connect_db(DB_PARAMS)  # Initial connection
    execution_successful = False
    final_results_dict = None

    if active_connection:
        try:
            final_results_dict = execute_sql_scripts_in_dir(
                conn=active_connection,
                db_params=DB_PARAMS,
                sql_dir=sql_directory_to_use,
                transaction_mode=args.transaction_mode
            )
            if final_results_dict:
                execution_successful = not final_results_dict.get("fatal_error_occurred", True)
                # Update active_connection to the one actually
                # used/returned by the function
                active_connection = final_results_dict.get("connection", active_connection)
            else:
                # This case should ideally not be reached if
                # execute_sql_scripts_in_dir always returns a dict
                logging.error("Function 'execute_sql_scripts_in_dir' "
                              "did not return results.")
                execution_successful = False

        except Exception as main_exec_err:
            logging.critical(
                f"Critical error during main execution flow: {main_exec_err}",
                exc_info=True
            )
            execution_successful = False
            # Active_connection here is the one before execute_sql_scripts_in_dir
            # was called, or potentially None if connect_db failed.
            # If connect_db succeeded, _attempt_rollback can be tried.
            if active_connection:
                _attempt_rollback(active_connection, "critical main execution error")
        finally:
            # Close the connection that was last known to be active
            if active_connection and not active_connection.closed:
                try:
                    active_connection.close()
                    logging.info("Database connection closed.")
                except Exception as e:
                    logging.error(f"Error closing connection: {e}")
    else:
        logging.error(
            "SQL script execution cannot proceed without an initial "
            "database connection."
        )
        execution_successful = False

    logging.info("===== SQL script execution finished =====")

    sys.exit(0 if execution_successful else 1)