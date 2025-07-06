-- This schema is designed to capture all relevant metrics for later analysis.

CREATE TABLE IF NOT EXISTS runs (
    -- A unique identifier for this specific run (UUID).
    run_id TEXT PRIMARY KEY,

    -- The type of process being run: 'baseline' or 'kognitos'.
    run_type TEXT NOT NULL,

    -- The ID of the invoice being processed.
    invoice_id TEXT NOT NULL,

    -- The start and end timestamps for the run (Unix epoch).
    ts_start REAL NOT NULL,
    ts_end REAL NOT NULL,

    -- The calculated duration of the run in seconds.
    cycle_time_s REAL NOT NULL,

    -- The calculated cost of the run in USD.
    cost_usd REAL NOT NULL,

    -- The final status of the run: 'SUCCESS' or 'FAILURE'.
    status TEXT NOT NULL,

    -- If status is 'FAILURE', this field contains the error message.
    error_details TEXT,

    -- For 'kognitos' runs, this stores the immutable SHA-256 audit seal.
    merkle_root TEXT
);