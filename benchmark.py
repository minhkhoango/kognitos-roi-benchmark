# benchmark.py
# This is the main executable script for the demo.
# It orchestrates the entire process:
# 1. Sets up the database.
# 2. Runs both 'baseline' and 'kognitos' workflows against all invoices.
# 3. Logs every result to the database.
# 4. Queries the database to calculate aggregate KPIs.
# 5. Prints a final, formatted Markdown report to the console.

import time
import uuid
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

from src.database import init_db, log_run, get_db_connection
from src.processing import ProcessingResult, run_baseline_process, run_kognitos_process

# --- Configuration ---
DATA_DIR: Path = Path("data")
# Simple cost model: cost per second for human vs machine time
COST_PER_HOUR_HUMAN: float = 45.0  # Blended rate for an AP clerk
COST_PER_HOUR_MACHINE: float = 0.50  # Generous cost for compute
KOGNITOS_FIXED_FEE_PER_RUN: float = 0.001 # A small API fee
# --- End Configuration ---

def calculate_cost(run_type: str, cycle_time_s: float) -> float:
    """Calculates the cost of a run based on its type and duration."""
    if run_type == "baseline":
        cost_per_second = COST_PER_HOUR_HUMAN / 3600
        return cycle_time_s * cost_per_second
    else:  # kognitos
        cost_per_second = COST_PER_HOUR_MACHINE / 3600
        return (cycle_time_s * cost_per_second) + KOGNITOS_FIXED_FEE_PER_RUN

def print_results(df: pd.DataFrame) -> None:
    """Calculates aggregate metrics and prints a formatted Markdown table."""
    if df.empty:
        print("No data to report.")
        return

    summary: Dict[str, Dict[str, Any]] = {}
    for run_type, group in df.groupby("run_type"): # type: ignore
        if not isinstance(run_type, str):
            continue

        total_runs: int = len(group)
        successful_runs: int = len(group[group["status"] == "SUCCESS"])
        error_rate: float = ((total_runs - successful_runs) / total_runs) * 100 if total_runs > 0 else 0
        
        summary[run_type] = {
            "Avg Cycle Time (s)": group["cycle_time_s"].mean(),
            "Avg Cost ($)": group["cost_usd"].mean(),
            "Error Rate (%)": error_rate,
            "Total Runs": total_runs,
            "Successful Runs": successful_runs,
        }

        baseline: Dict[str, Any] = summary.get("baseline", {})
        kognitos: Dict[str, Any] = summary.get("kognitos", {})

        # Calculate percentage change (delta)
        # Handle potential division by zero if baseline values are 0
        baseline_time = baseline.get("Avg Cycle Time (s)", 1)
        if baseline_time == 0: baseline_time = 1
        time_delta = ((kognitos.get("Avg Cycle Time (s)", 0) - baseline_time) / baseline_time) * 100

        baseline_cost = baseline.get("Avg Cost ($)", 1)
        if baseline_cost == 0: baseline_cost = 1
        cost_delta = ((kognitos.get("Avg Cost ($)", 0) - baseline_cost) / baseline_cost) * 100

        baseline_error = baseline.get("Error Rate (%)", 0)
        if baseline_error > 0:
            error_delta = ((kognitos.get("Error Rate (%)", 0) - baseline_error) / baseline_error) * 100
        else:
            error_delta = -100.0 if kognitos.get("Error Rate (%)", 0) == 0 else 0.0
        
        # --- Print Markdown Table ---
        print("\n--- Kognitos Battering Ram: Final Report ---\n")
        header = "| Metric              | Baseline   | Kognitos   | Delta      |"
        separator = "|---------------------|------------|------------|------------|"
        print(header)
        print(separator)

        print(f"| Avg Cycle Time (s)  | {baseline.get('Avg Cycle Time (s)', 0):<10.2f} | {kognitos.get('Avg Cycle Time (s)', 0):<10.2f} | {time_delta:<9.2f}% |")
        print(f"| Avg Cost ($)        | {baseline.get('Avg Cost ($)', 0):<10.4f} | {kognitos.get('Avg Cost ($)', 0):<10.4f} | {cost_delta:<9.2f}% |")
        print(f"| Error Rate (%)      | {baseline.get('Error Rate (%)', 0):<10.2f} | {kognitos.get('Error Rate (%)', 0):<10.2f} | {error_delta:<9.2f}% |")
        print(f"| Total Runs          | {baseline.get('Total Runs', 0):<10} | {kognitos.get('Total Runs', 0):<10} |            |")
        print(f"| Successful Runs     | {baseline.get('Successful Runs', 0):<10} | {kognitos.get('Successful Runs', 0):<10} |            |")
        print("\n--- End of Report ---\n")

def main() -> None:
    """Main orchestration function."""
    init_db()

    invoice_paths: List[Path] = sorted(list(DATA_DIR.glob("*.csv")))
    if not invoice_paths:
        print(f"Error: No invoices found in {DATA_DIR}. Did you run 'make setup'?")
        return
    
    # --- Run Baseline ---
    print(f"\nRunning BASELINE process for {len(invoice_paths)} invoices...")
    for path in invoice_paths:
        ts_start: float = time.perf_counter()
        result: ProcessingResult = run_baseline_process(path)
        ts_end: float = time.perf_counter()

        cycle_time: float = ts_end - ts_start
        cost: float = calculate_cost("baseline", cycle_time)
        
        log_run(
            run_id=str(uuid.uuid4()),
            run_type="baseline",
            invoice_id=result["invoice_id"],
            ts_start=ts_start,
            ts_end=ts_end,
            cycle_time_s=cycle_time,
            cost_usd=cost,
            status=result["status"],
            error_details=result["error_details"],
            merkle_root=result["merkle_root"],
        )
    
    # --- Run Kognitos ---
    print(f"\nRunning KOGNITOS process for {len(invoice_paths)} invoices...")
    for path in invoice_paths:
        ts_start = time.perf_counter()
        result = run_kognitos_process(path)
        ts_end = time.perf_counter()

        cycle_time = ts_end - ts_start
        cost = calculate_cost("kognitos", cycle_time)

        log_run(
            run_id=str(uuid.uuid4()),
            run_type="kognitos",
            invoice_id=result["invoice_id"],
            ts_start=ts_start,
            ts_end=ts_end,
            cycle_time_s=cycle_time,
            cost_usd=cost,
            status=result["status"],
            error_details=result["error_details"],
            merkle_root=result["merkle_root"],
        )

    # --- Analyze and Report ---
    print("\nBenchmark complete. Generating report...")
    with get_db_connection() as con:
        df: pd.DataFrame = pd.read_sql_query("SELECT * FROM runs", con) # type: ignore
    
    print_results(df)

if __name__ == "__main__":
    main()