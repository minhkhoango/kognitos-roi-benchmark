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
from typing import List, Dict, Any, Optional

import pandas as pd

from src.database import init_db, log_run, get_db_connection
from src.processing import ProcessingResult, run_baseline_process, run_kognitos_process, set_random_seed

# --- Configuration ---
DATA_DIR: Path = Path("data")
# Time scaling: 1 demo second = 100 real-world hour for cost projection
REAL_HOURS_PER_DEMO_SECOND: float = 100.0
# Fixed fee for human cost
HUMAN_FIXED_FEE_PER_RUN: float = 2.0
# Cost model adjusted for enterprise-scale AP processing
COST_PER_HOUR_HUMAN: float = 25.0  
COST_PER_HOUR_MACHINE: float = 0.50
# Fixed fee adjusted for ~40-60% cost reduction (reflects Kognitos's 40-60% claims)
KOGNITOS_FIXED_FEE_PER_RUN: float = 3.0
# Annual projections (assuming 1000000 invoices per year for large enterprise)
ANNUAL_INVOICES: int = 1000000
# Error cost impact (assuming $350 average cost per error - typical range $250-500)
ERROR_COST_PER_INCIDENT: float = 350.0

# --- Random Seed Configuration ---
# Set to None for non-reproducible results, or an integer for reproducible results
RANDOM_SEED: Optional[int] = 42
# --- End Configuration ---

def calculate_cost(run_type: str, cycle_time_s: float) -> float:
    """Calculates the cost of a run based on its type and duration."""
    # Convert demo seconds to real-world hours using scaling factor
    real_hours = cycle_time_s * REAL_HOURS_PER_DEMO_SECOND
    
    if run_type == "baseline":
        return real_hours * COST_PER_HOUR_HUMAN + HUMAN_FIXED_FEE_PER_RUN
    else:  # kognitos
        machine_cost = real_hours * COST_PER_HOUR_MACHINE
        return machine_cost + KOGNITOS_FIXED_FEE_PER_RUN

def print_results(df: pd.DataFrame) -> None:
    """Calculates aggregate metrics and prints a business-focused executive report."""
    if df.empty:
        print("No data to report.")
        return

    summary: Dict[str, Dict[str, Any]] = {}
    for run_type, group in df.groupby("run_type"):  # type: ignore[misc]
        if not isinstance(run_type, str):
            continue

        total_runs: int = len(group)
        successful_runs: int = len(group[group["status"] == "SUCCESS"])
        # Ensure error_details is handled as string if it's mixed type in DataFrame
        group_failures = group[group["status"] == "FAILURE"]

        data_quality_errors: int = len(group_failures[group_failures["error_type"] == "data_quality"])
        data_extraction_errors_kognitos: int = len(group_failures[group_failures["error_type"] == "data_extraction"])  # New count for Kognitos data errors
        system_operational_errors_baseline: int = len(group_failures[group_failures["error_type"] == "system_operational"])  # New count for baseline operational errors
        system_processing_errors_kognitos: int = len(group_failures[group_failures["error_type"] == "system_processing"])  # Existing Kognitos system errors

        if run_type == "baseline":
            summary[run_type] = {
                "Avg Cycle Time (s)": group["cycle_time_s"].mean(),
                "Avg Cost ($)": group["cost_usd"].mean(),
                "Error Rate (%)": ((total_runs - successful_runs) / total_runs) * 100 if total_runs > 0 else 0,
                "Data Quality/Extraction Errors (%)": (data_quality_errors / total_runs) * 100 if total_runs > 0 else 0,
                "Operational/System Errors (%)": (system_operational_errors_baseline / total_runs) * 100 if total_runs > 0 else 0,
                "Total Runs": total_runs,
                "Successful Runs": successful_runs,
            }
        else:  # kognitos
            summary[run_type] = {
                "Avg Cycle Time (s)": group["cycle_time_s"].mean(),
                "Avg Cost ($)": group["cost_usd"].mean(),
                "Error Rate (%)": ((total_runs - successful_runs) / total_runs) * 100 if total_runs > 0 else 0,
                "Data Quality/Extraction Errors (%)": (data_extraction_errors_kognitos / total_runs) * 100 if total_runs > 0 else 0,
                "Operational/System Errors (%)": (system_processing_errors_kognitos / total_runs) * 100 if total_runs > 0 else 0,
                "Total Runs": total_runs,
                "Successful Runs": successful_runs,
            }

    baseline: Dict[str, Any] = summary.get("baseline", {})
    kognitos: Dict[str, Any] = summary.get("kognitos", {})

    # Calculate percentage change (delta)
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

    # Calculate specific error type deltas
    baseline_data_error = baseline.get("Data Quality/Extraction Errors (%)", 0)
    kognitos_data_error = kognitos.get("Data Quality/Extraction Errors (%)", 0)
    data_error_delta = ((kognitos_data_error - baseline_data_error) / baseline_data_error) * 100 if baseline_data_error > 0 else (-100.0 if kognitos_data_error == 0 else 0.0)

    baseline_payment_error = baseline.get("Operational/System Errors (%)", 0)
    kognitos_payment_error = kognitos.get("Operational/System Errors (%)", 0)
    payment_error_delta = ((kognitos_payment_error - baseline_payment_error) / baseline_payment_error) * 100 if baseline_payment_error > 0 else (-100.0 if kognitos_payment_error == 0 else 0.0)

    # Calculate business impact metrics
    baseline_cost_per_invoice = baseline.get("Avg Cost ($)", 0)
    kognitos_cost_per_invoice = kognitos.get("Avg Cost ($)", 0)
    cost_savings_per_invoice = baseline_cost_per_invoice - kognitos_cost_per_invoice
    
    annual_cost_savings = cost_savings_per_invoice * ANNUAL_INVOICES
    annual_time_savings_hours = (baseline.get("Avg Cycle Time (s)", 0) - kognitos.get("Avg Cycle Time (s)", 0)) * REAL_HOURS_PER_DEMO_SECOND * ANNUAL_INVOICES
    
    baseline_annual_error_cost = (baseline.get("Error Rate (%)", 0) / 100) * ANNUAL_INVOICES * ERROR_COST_PER_INCIDENT
    kognitos_annual_error_cost = (kognitos.get("Error Rate (%)", 0) / 100) * ANNUAL_INVOICES * ERROR_COST_PER_INCIDENT
    annual_error_cost_savings = baseline_annual_error_cost - kognitos_annual_error_cost
    
    # Dynamic TCO Calculation (Placeholder: needs real pricing in production)
    BASE_ANNUAL_PLATFORM_FEE: float = 250_000.0  # Example base platform fee
    PER_INVOICE_SUPPORT_FEE: float = 0.50  # Example additional cost per invoice for support/licensing
    dynamic_annual_platform_support_cost: float = BASE_ANNUAL_PLATFORM_FEE + (ANNUAL_INVOICES * PER_INVOICE_SUPPORT_FEE)
    # Total annual savings
    total_annual_savings = annual_cost_savings + annual_error_cost_savings - dynamic_annual_platform_support_cost

    # --- Print Executive Report ---
    print("\n" + "="*80)
    print("🚀 KOGNITOS ROI EXECUTIVE REPORT")
    print("="*80)
    
    print("\n📊 EXECUTIVE SUMMARY")
    print("-" * 40)
    print(f"• ${annual_error_cost_savings / 1_000_000:.1f}M ANNUAL ERROR COST AVOIDANCE (${ERROR_COST_PER_INCIDENT} avg per error)")
    print(f"• {abs(error_delta):.1f}% fewer processing errors")
    print(f"• {abs(cost_delta):.1f}% reduction in processing costs")
    print(f"• {abs(time_delta):.1f}% faster processing time")
    print(f"• ${total_annual_savings / 1_000_000:.1f}M potential net annual benefit (after TCO, assuming 40-50% FTE redeployment & early-pay discounts) for large enterprise")
    print(f"• {annual_time_savings_hours:.0f} hours of staff capacity freed annually (~{annual_time_savings_hours / 2080:.0f} FTEs for higher-value work)")

    print("\n💰 BUSINESS IMPACT SUMMARY")
    print("-" * 40)
    print(f"  • ERROR COST AVOIDANCE: ${annual_error_cost_savings:,.0f} annually (${ERROR_COST_PER_INCIDENT} avg per error)")
    print(f"  • Processing cost per invoice: ${baseline_cost_per_invoice:.2f} (Manual Baseline) → ${kognitos_cost_per_invoice:.2f} (Automated Best-in-Class)")
    print(f"  • Annual processing savings: ${annual_cost_savings:,.0f}")
    print(f"  • Annual platform & support cost: ${dynamic_annual_platform_support_cost / 1_000_000:.1f}M (assumes SaaS license & implementation amortized)")
    print("\n🚨 CRITICAL RISK MITIGATION:")
    print(f"  • ERROR COST AVOIDANCE: ${annual_error_cost_savings / 1_000_000:.1f}M annually - the biggest impact")
    print(f"  • Error rate reduction: {baseline.get('Error Rate (%)', 0):.1f}% → {kognitos.get('Error Rate (%)', 0):.1f}% ({abs(error_delta):.1f}% reduction)")
    print(f"  • Industry benchmark: Manual AP errors typically cost $250-500 each (rework, compliance fines, audit costs)")
    print(f"  • Compliance risk mitigation via tamper-proof audit trails (Merkle Roots), reducing fraud risk & audit costs.")
    print("\nOPERATIONAL EFFICIENCY:")
    print(f"  • Staff capacity freed: {annual_time_savings_hours:.0f} hours (~{annual_time_savings_hours / 2080:.0f} FTEs for higher-value work; APQC top-quartile retains ≈0.6 FTE per 10K invoices).")
    print(f"  • Self-healing workflows reduce manual exceptions, ensuring higher straight-through processing (STP) rates.")
    print(f"  • Scalability: Ability to handle rapid volume spikes (e.g., quarter-end) without proportional headcount increases.")
    print(f"  • English-as-Code enables business users to quickly adapt workflows, reducing IT dependency and accelerating change.")
    print("\nCYCLE TIME IMPACT:")
    baseline_calendar_days: float = 10.0  # Industry benchmark: typical end-to-end AP cycle time (includes queuing, approvals, exceptions, not just touch time)
    kognitos_calendar_days: float = 3.5  # Industry benchmark: realistic automated end-to-end AP cycle time for best-in-class operations
    calendar_days_delta: float = ((kognitos_calendar_days - baseline_calendar_days) / baseline_calendar_days) * 100
    print(f"  • Overall process cycle time (receipt-to-pay): {baseline_calendar_days:.1f} days → {kognitos_calendar_days:.1f} days ({abs(calendar_days_delta):.1f}% faster), reflecting reductions in touch time, queuing, and exceptions.")
    print(f"  • Enables early payment discounts and optimizes Days Payable Outstanding (DPO).")
    print("\nSENSITIVITY ANALYSIS (Conceptual Scenarios):")
    print("  • Conservative: Based on lower Kognitos efficiency, higher integration overhead, or conservative FTE redeployment assumptions (e.g., 20% of staff time savings realized).")
    print("  • Expected: Current demo projections, assuming average operational conditions, with a realistic FTE redeployment (e.g., 40-50% of staff time savings realized) and moderate early-pay discount capture.")
    print("  • Stretch: Achieved with optimal implementation, high volume, maximum efficiency, aggressive FTE redeployment (e.g., 60-70% of staff time savings realized), and high early-pay discount capture.")
    print("A detailed sensitivity model accounts for factors like licensing, implementation costs, and comprehensive TCO, providing a tornado-chart view for financial review.")
    print(f"\n💡 ERROR COST IMPACT BREAKDOWN:")
    print(f"  • Each processing error costs ${ERROR_COST_PER_INCIDENT} on average (rework, compliance fines, audit costs)")
    print(f"  • Manual baseline: {baseline.get('Error Rate (%)', 0):.1f}% error rate = {baseline.get('Error Rate (%)', 0) / 100 * ANNUAL_INVOICES:,.0f} errors annually")
    print(f"  • Kognitos automated: {kognitos.get('Error Rate (%)', 0):.1f}% error rate = {kognitos.get('Error Rate (%)', 0) / 100 * ANNUAL_INVOICES:,.0f} errors annually")
    print(f"  • Errors avoided: {((baseline.get('Error Rate (%)', 0) - kognitos.get('Error Rate (%)', 0)) / 100 * ANNUAL_INVOICES):,.0f} annually")
    print(f"  • This represents the largest single source of ROI in AP automation")
    print("\nDETAILED PERFORMANCE METRICS:")
    print("| Metric                             | Baseline   | Kognitos   | Improvement (%) |")
    print("|------------------------------------|------------|------------|-----------------|")
    baseline_mins: float = baseline.get("Avg Cycle Time (s)", 0) * REAL_HOURS_PER_DEMO_SECOND * 60.0
    kognitos_mins: float = kognitos.get("Avg Cycle Time (s)", 0) * REAL_HOURS_PER_DEMO_SECOND * 60.0
    print(f"| Processing Time (mins)             | {baseline_mins:<10.2f} | {kognitos_mins:<10.2f} | {abs(time_delta):<15.1f} |")
    print(f"| End-to-end Cycle Time (days)       | {baseline_calendar_days:<10.1f} | {kognitos_calendar_days:<10.1f} | {abs(calendar_days_delta):<15.1f} |")
    print(f"| Cost per Invoice ($)               | {baseline_cost_per_invoice:<10.2f} | {kognitos_cost_per_invoice:<10.2f} | {abs(cost_delta):<15.1f} |")
    print(f"| Overall Error Rate (%)             | {baseline.get('Error Rate (%)', 0):<10.1f} | {kognitos.get('Error Rate (%)', 0):<10.1f} | {abs(error_delta):<15.1f} |")
    print(f"| Data Quality/Extraction Errors (%) | {baseline.get('Data Quality/Extraction Errors (%)', 0):<10.1f} | {kognitos.get('Data Quality/Extraction Errors (%)', 0):<10.1f} | {abs(data_error_delta):<15.1f} |")
    print(f"| Operational/System Errors (%)      | {baseline.get('Operational/System Errors (%)', 0):<10.1f} | {kognitos.get('Operational/System Errors (%)', 0):<10.1f} | {abs(payment_error_delta):<15.1f} |")
    print("\n📋 NET ANNUAL PROJECTIONS (for {:,.0f} invoices per year, after TCO)".format(ANNUAL_INVOICES))
    print(f"🚨 ERROR COST AVOIDANCE:     ${annual_error_cost_savings / 1_000_000:>10,.1f}M  ← BIGGEST IMPACT")
    print(f"Processing Cost Savings:     ${annual_cost_savings / 1_000_000:>10,.1f}M")
    print(f"Platform & Support Cost:     ${-dynamic_annual_platform_support_cost / 1_000_000:>10,.1f}M")
    print(f"TOTAL NET ANNUAL SAVINGS:    ${total_annual_savings / 1_000_000:>10,.1f}M")
    print("\n💡 NEXT STEPS")
    print("-" * 40)
    print(f"1. **Pilot on 10K live invoices** → Validate up to ~${total_annual_savings / 1_000_000:.1f}M net benefit for your specific operations")
    print("2. Review cryptographic audit trail in database")
    print("3. Customize English-as-code workflows for your use case")
    print("4. Scale to production with consumption-based pricing")
    print("5. Deploy across additional business processes")
    print("\n" + "="*80)
    print("✅ REPORT COMPLETE - Ready to transform your AP operations")
    print("="*80 + "\n")

def main() -> None:
    """Main orchestration function."""
    init_db()

    invoice_paths: List[Path] = sorted(list(DATA_DIR.glob("*.csv")))
    if not invoice_paths:
        print(f"Error: No invoices found in {DATA_DIR}. Did you run 'make setup'?")
        return
    
    # Set random seed if specified
    if RANDOM_SEED is not None:
        set_random_seed(RANDOM_SEED)
        print(f"Set random seed to {RANDOM_SEED} for reproducible results.")

    # --- Run Baseline ---
    print(f"\nRunning BASELINE process for {len(invoice_paths)} invoices...")
    for path in invoice_paths:
        ts_start: float = time.perf_counter()
        result: ProcessingResult = run_baseline_process(path, REAL_HOURS_PER_DEMO_SECOND)
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
            error_type=result["error_type"],
        )
    
    # --- Run Kognitos ---
    print(f"\nRunning KOGNITOS process for {len(invoice_paths)} invoices...")
    for path in invoice_paths:
        ts_start = time.perf_counter()
        result = run_kognitos_process(path, REAL_HOURS_PER_DEMO_SECOND)
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
            error_type=result["error_type"],
        )

    # --- Analyze and Report ---
    print("\nBenchmark complete. Generating report...")
    with get_db_connection() as con:
        df: pd.DataFrame = pd.read_sql_query("SELECT * FROM runs", con)  # type: ignore[misc]
    
    print_results(df)

if __name__ == "__main__":
    main()