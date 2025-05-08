# SQL Script Executor for PostgreSQL

This Python script automates the execution of SQL scripts from a specified directory against a PostgreSQL database. It offers robust logging, comprehensive error handling, and flexible transaction control, making it suitable for database migrations, setup procedures, or batch SQL operations.

## Table of Contents

1.  [Overview](#overview)
2.  [Features](#features)
3.  [Prerequisites](#prerequisites)
4.  [Installation](#installation)
5.  [Configuration](#configuration)
    * [Database Connection](#database-connection)
    * [SQL Script Directory](#sql-script-directory)
6.  [Usage](#usage)
    * [Command-Line Arguments](#command-line-arguments)
    * [Examples](#examples)
7.  [Transaction Modes Explained](#transaction-modes-explained)
8.  [Logging and Reporting](#logging-and-reporting)
9.  [Error Handling](#error-handling)
10. [Expected Directory Structure](#expected-directory-structure)

## Overview

This script connects to a PostgreSQL database and executes all `.sql` files found within a designated directory. Files are processed in a natural sort order (e.g., `1_script.sql`, `2_script.sql`, `10_script.sql`). The script provides multiple transaction handling modes to accommodate various deployment scenarios, from simple script sequences to complex, atomic database updates.

## Features

* **Automated SQL Execution:** Executes multiple `.sql` files in a defined, naturally sorted order.
* **PostgreSQL Compatibility:** Specifically designed for PostgreSQL database interactions.
* **Flexible Transaction Control:**
    * `per-file`: Each script is executed in a separate transaction.
    * `per-file-until-error`: Each script is executed in a separate transaction, halting on the first encountered error.
    * `all-or-nothing`: All scripts are executed within a single transaction; any failure results in a complete rollback.
* **Robust Database Connection:**
    * Implements retry logic for initial database connection attempts.
    * Operates with `autocommit` disabled to ensure explicit transaction management.
* **Intelligent File Discovery:**
    * Recursively locates `.sql` files within the target directory.
    * Supports symbolic links.
    * Identifies and reports file access issues or path anomalies (e.g., broken symbolic links, non-regular files).
    * Utilizes natural sort order for predictable script execution sequence.
* **Comprehensive Logging:**
    * Outputs logs to both the console and a dedicated log file (`./logs/sql_executor.log`).
    * Includes detailed timestamps, log levels, and source module information.
* **Detailed Execution Reports:**
    * Generates timestamped report files in the `./logs` directory, detailing:
        * Successfully committed/executed scripts.
        * Scripts that caused errors.
        * Empty script files.
        * File scanning anomalies.
        * Unprocessed scripts (if execution halted early).
* **Configuration via Environment Variables or `.env` File:** Facilitates setting up database connection parameters.
* **Command-Line Interface:** Provides a clear CLI for script operation.

## Prerequisites

* **Python 3.x:** The script is developed for current Python 3 versions.
* **PostgreSQL Server:** A running PostgreSQL instance is required for database operations.
* **Python Libraries:**
    * `psycopg2-binary`: PostgreSQL adapter for Python.
    * `python-dotenv`: For loading environment variables from `.env` files.
    * `natsort`: For natural sorting of filenames.

## Installation

1.  **Obtain the Script:**
    In your project directory, clone the repository containing the script or save the script file (e.g., `sql_executor.py`).

2.  **Establish a Virtual Environment (Recommended):**
    Navigate to your project directory and create a dedicated virtual environment (isolating project dependencies is a standard best practice).

    * With Python's standard `venv`:
    ```bash
    cd <path_to_project_directory>
    python -m venv <environment_name>
    source <environment_name>/bin/activate  # On Windows: <environment_name>\Scripts\activate
    ```
    * With Conda:
    ```bash
    conda create -n <environment_name>
    conda activate <environment_name>
    ```

3.  **Install Dependencies:**
    Use the provided `requirements.txt`, or create the file in your project root with the following content:
    ```txt
    psycopg2-binary
    python-dotenv
    natsort
    ```
    Install dependencies using a package manager of your choice:
    ```bash
    pip install -r requirements.txt
    ```
    ```bash
    conda install --file requirements.txt
    ```

## Configuration

### Database Connection

The script utilizes environment variables for PostgreSQL connection parameters. These can be set in the operating system environment or defined in a `.env` file located in the script's execution directory. **Note:** Environment variables will override values specified in a `.env` file. The defaults given below are the same used in the official PostgreSQL Docker image.

* `DB_NAME`: The target database name (default: `postgres`).
* `DB_USER`: The database username (default: `postgres`).
* `DB_PASS`: The database user's password (default: `mysecretpassword` - **it is strongly recommended to change this for any non-development environment**).
* `DB_HOST`: The database server hostname or IP address (default: `localhost`).
* `DB_PORT`: The port number for the database server (default: `5432`).

**Example `.env` file:**
```env
DB_NAME="production_db"
DB_USER="deploy_user"
DB_PASS="not a weak passphrase"
DB_HOST="db.example.com"
DB_PORT=5432
```


### SQL Script Directory

By default, the script targets `../` as the source for SQL files. For instance, if the script's path is `~/dir_1/dir_2/dir_3/sql_executor.py`, then it will process all SQL scripts inside `dir_2`, recursively. That way, to process your SQL scripts you can simply drop this project's directory inside the top SQL scripts' directory.

Alternatively, the path to the SQL scripts' top directory can be passed to this script via the `--sql-dir` command-line argument.

## Usage

Execute the script from the command line.

### Command-Line Arguments

```bash
# Long version
python sql_executor.py --transaction-mode <MODE> [--sql-dir <PATH_TO_SQL_FILES>]

# Short version
python sql_executor.py -t <MODE> [-d <PATH_TO_SQL_FILES>]
```

* **`--transaction-mode` (short version `-t`) (Required):** Defines the transaction handling strategy.
    * `per-file`: Each script is committed individually. Database errors lead to a rollback for that specific script, and execution proceeds to the next. File read errors result in skipping the problematic file.
    * `all-or-nothing`: All scripts are processed within a single transaction. This transaction is committed only if all scripts (and pre-execution file checks) complete without error. Any failure at any stage triggers a full rollback and halts processing.
    * `per-file-until-error`: Each script is committed individually. However, processing halts upon the first error encountered (scan, file, or database). A rollback is attempted for the script that caused the error.
* **`--sql-dir` (short version `-d`) (Optional):** Specifies the path to the directory containing `.sql` files.
    * Default: `../`

### Examples

1.  **Execute scripts in `per-file` mode from `../sql_files`:**
    ```bash
    python sql_executor.py --transaction-mode per-file --sql-dir ../sql_files
    ```

2.  **Execute scripts in `all-or-nothing` mode from `./database/migrations`:**
    ```bash
    python sql_executor.py -t all-or-nothing -d ./database/migrations
    ```

3.  **Execute scripts in `per-file-until-error` mode from a directory named `deployment_scripts`:**
    ```bash
    python sql_executor.py -t per-file-until-error --sql-dir deployment_scripts
    ```

## Transaction Modes Explained

Selecting the appropriate transaction mode is critical for ensuring data integrity and predictable deployment outcomes.

* **`per-file`**:
    * **Use Case:** Suitable when SQL scripts are largely independent, and the failure of one should not impede the execution of others (e.g., applying multiple, discrete patches or minor updates).
    * **Advantages:** Maximizes the number of scripts applied when some contain errors.
    * **Considerations:** May result in a partially modified database state if errors occur. Requires diligent log review.

* **`per-file-until-error`**:
    * **Use Case:** Appropriate for incremental updates where each step is a prerequisite for the next, but successful preceding steps should be committed. Useful during development or for quickly identifying the initial point of failure in a sequence.
    * **Advantages:** Commits completed work up to the point of failure. Facilitates pinpointing the problematic script.
    * **Considerations:** An error occurring mid-sequence will result in a partially updated database.

* **`all-or-nothing`**:
    * **Use Case:** Essential for critical deployments where atomicity and database consistency are paramount (e.g., complex schema migrations, large-scale data transformations that must fully complete or not at all).
    * **Advantages:** Guarantees the database is either fully updated or remains unchanged if any error occurs. Simplifies rollback scenarios.
    * **Considerations:** A single error in any script, or even a pre-execution file access issue, will prevent all changes. May have longer execution times for extensive script sets as the commit occurs only at the end.

## Logging and Reporting

The script provides extensive logging for traceability and audit purposes.

* **Console Logging:** Provides real-time operational feedback.
* **File Logging (`./logs/sql_executor.log`):** Maintains a persistent record of all operations. The `logs` directory is automatically created if it does not exist.
    * Log Format: `YYYY-MM-DD HH:MM:SS - LEVEL - [module:lineno] - message`
* **Execution Report Files:** Post-execution, timestamped text files are generated in the `./logs` directory, categorized by the outcome:
    * `*_committed_files.txt` (or `*_executable_files.txt` if `all-or-nothing` failed): Lists scripts successfully included in a commit (or those that would have been, prior to a global rollback).
    * `*_errors.txt`: Details scripts that encountered errors during processing or execution.
    * `*_empty_files.txt`: Identifies `.sql` files that were found but contained no executable content.
    * `*_file_anomalies.txt`: Reports paths identified as `.sql` files but were inaccessible, broken links, or not regular files.
    * `*_unprocessed_files.txt`: Lists scripts found but not attempted due to an earlier fatal error halting execution (primarily relevant for `all-or-nothing` and `per-file-until-error` modes).

These reports serve as valuable resources for auditing deployment processes and troubleshooting issues.

## Error Handling

The script incorporates mechanisms to manage various error conditions:

* **Connection Errors:** Attempts to reconnect to the database with a retry mechanism. Persistent failure to connect will terminate execution.
* **File System Errors:**
    * Failure to create the log directory disables file logging but permits console logging.
    * Issues accessing or reading `.sql` files are logged.
    * In `all-or-nothing` and `per-file-until-error` modes, file scanning anomalies or read errors are considered fatal and will halt execution. In `per-file` mode, such files are skipped.
* **SQL Execution Errors:**
    * `psycopg2.Error` exceptions are caught and handled.
    * Error details, including `SQLSTATE` and `pgerror`, are logged.
    * Rollback procedures are initiated according to the selected transaction mode.
* **Commit Errors:** Treated similarly to SQL execution errors, with appropriate rollback attempts.
* **Unexpected Errors:** A global exception handler logs other unforeseen issues to aid in diagnostics.

The script endeavors to ensure the database connection is properly closed and transactions are appropriately managed (committed or rolled back) upon completion or error.

## Expected Directory Structure

A typical project layout might be as follows:

```
your_database_project/
├── sql_executor.py         # This script
├── requirements.txt        # Python dependencies
├── .env                    # Optional: for DB credentials (ensure this is in .gitignore)
├── sql_scripts/            # Directory for SQL files
│   ├── 01_create_schemas.sql
│   ├── 02_create_tables.sql
│   ├── ...
│   └── 10_seed_initial_data.sql
└── logs/                   # Automatically created for log and report files
    ├── sql_executor.log
    ├── <timestamp>_<mode>_committed_files.txt
    └── ... (other report files)


├── sql_scripts/            # Directory for SQL files
│   ├── 01_create_schemas.sql
│   ├── 02_create_tables.sql
│   ├── ...
│   └── 10_seed_initial_data.sql
├── script_directory/
    ├── sql_executor.py         # This script
    ├── requirements.txt        # Python dependencies
    ├── .env                    # Optional: for DB credentials (ensure this is in .gitignore)
    └── logs/                   # Automatically created for log and report files
        ├── sql_executor.log
        ├── <timestamp>_<mode>_committed_files.txt
        └── ... (other report files)

sql_scripts/            # Directory for SQL files
│   ├── 01_create_schemas.sql
│   ├── 02_create_tables.sql
|   ├── ...
│   ├── 10_seed_initial_data.sql
│   └── sub_directory/
|         ├── 11_create_sub_schemas.sql
│         ├── 12_update_tables.sql
│         └── 13_clean_up.sql
| 
├── executor_directory/
    ├── sql_executor.py         # This script
    ├── requirements.txt        # Python dependencies
    ├── .env                    # Optional: for DB credentials (ensure this is in .gitignore)
    └── logs/                   # Automatically created for log and report files
        ├── sql_executor.log
        ├── <timestamp>__<mode>__committed_files.txt
        └── ... (other report files)
```