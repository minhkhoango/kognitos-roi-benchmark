# src/database.py
# This module handles all interactions with the SQLite database.
# It's responsible for initializing the DB from a schema file and
# logging the results of each benchmark run.

import sqlite3
from pathlib import Path
from typing import Optional

# --- Configuration ---
DB_PATH: Path = Path("results.db")
SCHEMA_PATH: Path = Path("db_schema.sql")
# --- End Configuration ---

def get_db_connection() -> sqlite3.Connection:
    """Establishes and returns a connection to the SQLite database."""
    try:
        con: sqlite3.Connection = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        return con
    except sqlite3.Error as e:
        print(f"Database connection error: {e}")
        raise

def init_db() -> None:
    """
    Initializes the database by executing the schema script.
    This is idempotent - it won't fail if the table already exists.
    """
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Database schema not found at {SCHEMA_PATH}")

    with get_db_connection() as con:
        try:
            with open(SCHEMA_PATH, "r") as f:
                schema_sql = f.read()
            con.executescript(schema_sql)
            con.commit()
            print(f"Database initialized at {DB_PATH}")
        except sqlite3.Error as e:
            print(f"Error initializing database: {e}")
            raise

def log_run(
    run_id: str,
    run_type: str,
    invoice_id: str,
    ts_start: float,
    ts_end: float,
    cycle_time_s: float,
    cost_usd: float,
    status: str,
    error_details: Optional[str],
    merkle_root: Optional[str],
) -> None:
    """
    Logs the results of a single processing run to the database.
    """
    sql = """
        INSERT INTO runs (
            run_id, run_type, invoice_id, ts_start, ts_end, 
            cycle_time_s, cost_usd, status, error_details, merkle_root
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    with get_db_connection() as con:
        try:
            con.execute(
                sql,
                (
                    run_id,
                    run_type,
                    invoice_id,
                    ts_start,
                    ts_end,
                    cycle_time_s,
                    cost_usd,
                    status,
                    error_details,
                    merkle_root,
                ),
            )
            con.commit()
        except sqlite3.Error as e:
            print(f"Error logging run {run_id}: {e}")
