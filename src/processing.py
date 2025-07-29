# src/processing.py
# This module contains the core business logic for processing an invoice,
# both for the manual "baseline" and the automated "kognitos" workflows.

import csv
import hashlib
import random
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, TypedDict

from src.auditing import compute_merkle_root

# --- Configuration Constants ---
# Manual process timing (in minutes) following industry average of 12 minutes a manual invoice, 
# account for script loading time
MANUAL_SLEEP_MIN: float = 9.0
MANUAL_SLEEP_MAX: float = 15.0
# Manual process error rate (human error rate 1.6-3%, up to 10-15% possible)
MANUAL_ERROR_RATE: float = 0.07

# Kognitos process timing (in minutes)
KOGNITOS_SLEEP_MIN: float = 1.5
KOGNITOS_SLEEP_MAX: float = 2.5
# Kognitos process error rate (automation error rate ~1-2% for realistic automation)
KOGNITOS_ERROR_RATE: float = 0.005

MIN_IN_A_HOUR: int = 60

# --- Random Seed Configuration ---
# Set to None for non-reproducible results, or an integer for reproducible results
_random_seed: Optional[int] = 42

def set_random_seed(seed: Optional[int] = None) -> None:
    """
    Set the random seed for reproducible results.
    
    Args:
        seed: Integer seed value, or None for non-reproducible results
    """
    global _random_seed
    _random_seed = seed
    if seed is not None:
        random.seed(seed)

# Initialize seed if specified
set_random_seed(_random_seed)

# --- Type Definitions ---
class ProcessingResult(TypedDict):
    """A standardized structure for returning results from processing functions."""
    status: str  # 'SUCCESS' or 'FAILURE'
    error_details: Optional[str]
    merkle_root: Optional[str]
    invoice_id: str
    error_type: Optional[str]  # 'data_quality', 'system_processing', 'data_extraction', 'system_operational', or other specific types
    simulated_cycle_time_s: float  # Simulated processing time in seconds
# --- End Type Definitions ---

def _detect_data_quality_issues(data: Dict[str, Any]) -> List[str]:
    """
    Detects data quality issues in the invoice data.
    Returns a list of detected issues.
    """
    issues: List[str] = []
    
    # Check for missing or empty invoice_id
    if not data.get("invoice_id") or data.get("invoice_id") == "":
        issues.append("missing_invoice_id")
    
    # Check for invalid date format (should be YYYY-MM-DD)
    invoice_date = data.get("invoice_date", "")
    if invoice_date and not (len(invoice_date) == 10 and invoice_date[4] == "-" and invoice_date[7] == "-"):
        issues.append("invalid_date_format")
    
    # Check for negative quantities
    quantity = data.get("quantity")
    if quantity is not None and isinstance(quantity, (int, float)) and quantity < 0:
        issues.append("negative_quantity")
    
    # Check for non-numeric total
    total = data.get("total")
    if total is not None and isinstance(total, str) and not total.replace(".", "").replace("USD", "").strip().isdigit():
        issues.append("non_numeric_total")
    
    # Check for missing required fields
    if not data.get("quantity") or not data.get("unit_price"):
        issues.append("missing_required_fields")
    
    # Check for mismatched total (if we can calculate it)
    quantity = data.get("quantity")
    unit_price = data.get("unit_price")
    total = data.get("total")
    if all(v is not None for v in [quantity, unit_price, total]):
        try:
            # Type check to ensure we can convert to float
            if isinstance(quantity, (int, float)) and isinstance(unit_price, (int, float)):
                expected_total = float(quantity) * float(unit_price)
                actual_total = float(str(total).replace("USD", "").strip())
                if abs(expected_total - actual_total) > 0.01:  # Allow for rounding
                    issues.append("mismatched_total")
        except (ValueError, TypeError):
            pass
    
    return issues

def _kognitos_fix_data_quality(data: Dict[str, Any]) -> tuple[Dict[str, Any], List[str]]:
    """
    Kognitos can intelligently fix common data quality issues.
    Returns the fixed data and list of fixes applied.
    """
    fixed_data = data.copy()
    fixes_applied: List[str] = []
    
    # Fix missing invoice_id by generating one (95% success rate)
    if not fixed_data.get("invoice_id") or fixed_data.get("invoice_id") == "":
        if random.random() < 0.95:  # 95% chance to fix
            import uuid
            fixed_data["invoice_id"] = str(uuid.uuid4())
            fixes_applied.append("generated_invoice_id")
    
    # Fix invalid date format with more comprehensive parsing (90% success rate)
    invoice_date = fixed_data.get("invoice_date", "")
    if invoice_date and not (len(invoice_date) == 10 and invoice_date[4] == "-" and invoice_date[7] == "-"):
        if random.random() < 0.90:  # 90% chance to fix
            try:
                from datetime import datetime
                # Handle various date formats including more edge cases
                date_formats = [
                    "%m-%d-%Y", "%d-%m-%Y", "%Y-%m-%d",
                    "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d",
                    "%m.%d.%Y", "%d.%m.%Y", "%Y.%m.%d"
                ]
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(invoice_date, fmt)
                        fixed_data["invoice_date"] = parsed_date.strftime("%Y-%m-%d")
                        fixes_applied.append("fixed_date_format")
                        break
                    except ValueError:
                        continue
                else:
                    # If all parsing fails, use a default date
                    fixed_data["invoice_date"] = "2025-01-15"
                    fixes_applied.append("defaulted_date")
            except:
                fixed_data["invoice_date"] = "2025-01-15"
                fixes_applied.append("defaulted_date")
    
    # Fix negative quantities by making them positive (95% success rate)
    quantity = fixed_data.get("quantity")
    if quantity is not None and isinstance(quantity, (int, float)) and quantity < 0:
        if random.random() < 0.95:  # 95% chance to fix
            fixed_data["quantity"] = abs(quantity)
            fixes_applied.append("fixed_negative_quantity")
    
    # Enhanced non-numeric total extraction (85% success rate)
    total = fixed_data.get("total")
    if total is not None and isinstance(total, str):
        if random.random() < 0.85:  # 85% chance to fix
            import re
            # More comprehensive numeric extraction
            numeric_match = re.search(r'[\d,]+\.?\d*', total)
            if numeric_match:
                try:
                    extracted_value = float(numeric_match.group().replace(',', ''))
                    fixed_data["total"] = extracted_value
                    fixes_applied.append("extracted_numeric_total")
                except ValueError:
                    # If extraction fails, try to infer from other fields
                    if fixed_data.get("quantity") and fixed_data.get("unit_price"):
                        try:
                            inferred_total = float(fixed_data["quantity"]) * float(fixed_data["unit_price"])
                            fixed_data["total"] = inferred_total
                            fixes_applied.append("inferred_total_from_fields")
                        except (ValueError, TypeError):
                            pass
    
    # Fix missing required fields with intelligent defaults (90% success rate)
    if not fixed_data.get("quantity"):
        if random.random() < 0.90:  # 90% chance to fix
            # Try to infer quantity from total and unit_price if available
            if fixed_data.get("total") and fixed_data.get("unit_price"):
                try:
                    inferred_qty = float(fixed_data["total"]) / float(fixed_data["unit_price"])
                    fixed_data["quantity"] = max(1, round(inferred_qty))
                    fixes_applied.append("inferred_quantity_from_total")
                except (ValueError, TypeError, ZeroDivisionError):
                    fixed_data["quantity"] = 1
                    fixes_applied.append("defaulted_quantity")
            else:
                fixed_data["quantity"] = 1
                fixes_applied.append("defaulted_quantity")
    
    if not fixed_data.get("unit_price"):
        if random.random() < 0.90:  # 90% chance to fix
            # Try to infer unit_price from total and quantity if available
            if fixed_data.get("total") and fixed_data.get("quantity"):
                try:
                    inferred_price = float(fixed_data["total"]) / float(fixed_data["quantity"])
                    fixed_data["unit_price"] = round(inferred_price, 2)
                    fixes_applied.append("inferred_unit_price_from_total")
                except (ValueError, TypeError, ZeroDivisionError):
                    fixed_data["unit_price"] = 100.0
                    fixes_applied.append("defaulted_unit_price")
            else:
                fixed_data["unit_price"] = 100.0
                fixes_applied.append("defaulted_unit_price")
    
    # Enhanced mismatched total recalculation (80% success rate)
    quantity = fixed_data.get("quantity")
    unit_price = fixed_data.get("unit_price")
    total = fixed_data.get("total")
    if all(v is not None for v in [quantity, unit_price, total]):
        if random.random() < 0.80:  # 80% chance to fix
            try:
                if isinstance(quantity, (int, float)) and isinstance(unit_price, (int, float)):
                    expected_total = float(quantity) * float(unit_price)
                    actual_total = float(str(total).replace("USD", "").strip())
                    if abs(expected_total - actual_total) > 0.01:  # Allow for rounding
                        fixed_data["total"] = expected_total
                        fixes_applied.append("recalculated_total")
            except (ValueError, TypeError):
                pass
    
    # Fix vendor name if missing or invalid (95% success rate)
    if not fixed_data.get("vendor_name") or fixed_data.get("vendor_name") == "":
        if random.random() < 0.95:  # 95% chance to fix
            fixed_data["vendor_name"] = "Unknown Vendor"
            fixes_applied.append("defaulted_vendor_name")
    
    return fixed_data, fixes_applied

def run_baseline_process(invoice_path: Path, real_hours_per_demo_second: float) -> ProcessingResult:
    """
    Simulates the slow, error-prone manual process.
    - Reads an invoice CSV.
    - Introduces a delay to simulate human interaction.
    - Has a random chance of failing to simulate data entry errors.
    - Error rate increases with data quality issues.
    """
    invoice_id = ""
    try:
        with open(invoice_path, "r") as f:
            reader = csv.DictReader(f)
            data = next(reader)
            invoice_id = data.get("invoice_id", "UNKNOWN")

        # Simulate human thinking and typing time (adjusted for scaling)
        sleep_duration = random.uniform(
            MANUAL_SLEEP_MIN, MANUAL_SLEEP_MAX
        ) / real_hours_per_demo_second / MIN_IN_A_HOUR
        time.sleep(sleep_duration)

        # Detect data quality issues
        data_issues = _detect_data_quality_issues(data)
        
        # Calculate dynamic error rate based on data quality
        # Base error rate + additional error rate per issue (humans can't fix data issues)
        # Humans struggle more with data quality issues
        dynamic_error_rate = MANUAL_ERROR_RATE + (len(data_issues) * 0.35)  # 35% additional error per issue

        # Realistic manual error rate (now dynamic)
        if random.random() < dynamic_error_rate:
            error_detail = "Manual data entry error"
            if data_issues:
                error_detail += f" due to data quality issues: {', '.join(data_issues)}"
            else:
                error_detail += ": incorrect total."
            
            return {
                "status": "FAILURE",
                "error_details": error_detail,
                "merkle_root": None,
                "invoice_id": invoice_id,
                "error_type": "data_quality",
                "simulated_cycle_time_s": sleep_duration,
            }
        
        if not invoice_id:
            return {
                "status": "FAILURE",
                "error_details": "Manual validation error: Missing invoice ID.",
                "merkle_root": None,
                "invoice_id": invoice_id,
                "error_type": "data_quality",
                "simulated_cycle_time_s": sleep_duration,
            }

        # Add a new error type for manual operational issues
        if random.random() < 0.01:  # 1% chance for operational error in manual
            return {
                "status": "FAILURE",
                "error_details": "Manual operational error: payment misrouting or delay.",
                "merkle_root": None,
                "invoice_id": invoice_id,
                "error_type": "system_operational",
                "simulated_cycle_time_s": sleep_duration,
            }

        return {
            "status": "SUCCESS",
            "error_details": None,
            "merkle_root": None, # No audit trail for manual process
            "invoice_id": invoice_id,
            "error_type": None,
            "simulated_cycle_time_s": sleep_duration,
        }

    except Exception as e:
        return {
            "status": "FAILURE",
            "error_details": str(e),
            "merkle_root": None,
            "invoice_id": invoice_id or f"failed_{invoice_path.name}",
            "error_type": "unknown_baseline_error",
            "simulated_cycle_time_s": 0.0, # No sleep for this specific error
        }

def _mock_kognitos_api(steps: str, data: Dict[str, Any], real_hours_per_demo_second: float) -> Dict[str, Any]:
    """
    A mock function that simulates a call to the Kognitos API.
    It's fast and reliable, but can still fail on truly bad data.
    """
    # Simulate network latency and processing time (adjusted for scaling)
    sleep_duration = random.uniform(
        KOGNITOS_SLEEP_MIN, KOGNITOS_SLEEP_MAX
    ) / real_hours_per_demo_second / MIN_IN_A_HOUR
    time.sleep(sleep_duration)

    # Kognitos can still fail if the input is truly garbage
    if not data.get("invoice_id"):
        return {"status": "FAILURE", "reason": "Cannot proceed without invoice_id"}

    return {"status": "SUCCESS", "extracted_total": data.get("total"), "simulated_sleep_s": sleep_duration}

def run_kognitos_process(invoice_path: Path, real_hours_per_demo_second: float) -> ProcessingResult:
    """
    Simulates the fast, reliable, and auditable Kognitos process.
    - Reads the invoice.
    - Calls a (mocked) Kognitos API.
    - Generates an audit trail and computes a Merkle root.
    - Error rate increases with severe data quality issues.
    """
    transactions: List[str] = []
    invoice_id = ""
    total_simulated_sleep_s = 0.0
    try:
        with open(invoice_path, "r") as f:
            reader = csv.DictReader(f)
            data = next(reader)
            invoice_id = data.get("invoice_id", "UNKNOWN")

        # 1. Start Process
        transactions.append(f"START_PROCESSING:{invoice_path.name}")

        # 2. Define "English-as-code" steps
        kognitos_steps = """
        1. READ the invoice file.
        2. EXTRACT the invoice_id, vendor_name, and total.
        3. VALIDATE that the total is a positive number.
        4. PREPARE the data for ERP entry.
        """
        transactions.append(f"LOAD_INSTRUCTIONS_HASH:{hashlib.sha256(kognitos_steps.encode()).hexdigest()}")

        # Detect data quality issues
        data_issues = _detect_data_quality_issues(data)
        
        # Kognitos can fix data quality issues, reducing the effective error rate
        fixed_data, fixes_applied = _kognitos_fix_data_quality(data)
        
        # Re-detect issues after fixing
        remaining_issues = _detect_data_quality_issues(fixed_data)
        
        # Calculate dynamic error rate for Kognitos (much lower than baseline)
        # Base error rate + small additional error rate per remaining issue
        # Kognitos gets a significant advantage from fixing data issues
        # But still has realistic error rates for automation
        # 100% additional error per remaining issue, if it can't fix no more, error remains
        dynamic_error_rate = KOGNITOS_ERROR_RATE + (len(remaining_issues) * 1)

        # 3. Execute with Kognitos (mocked)
        api_result = _mock_kognitos_api(kognitos_steps, fixed_data, real_hours_per_demo_second)
        transactions.append(f"API_CALL_STATUS:{api_result['status']}")
        
        # Add the simulated sleep time from the API call
        total_simulated_sleep_s += api_result.get("simulated_sleep_s", 0.0)
        
        # Log data quality fixes if any were applied
        if fixes_applied:
            transactions.append(f"DATA_QUALITY_FIXES:{','.join(fixes_applied)}")

        if api_result["status"] != "SUCCESS":
            return {
                "status": "FAILURE",
                "error_details": f"Kognitos API failed: unprocessable_input_format ({api_result.get('reason')})",
                "merkle_root": compute_merkle_root(transactions),
                "invoice_id": invoice_id,
                "error_type": "data_extraction",  # Change to data_extraction for unprocessable input
                "simulated_cycle_time_s": total_simulated_sleep_s,
            }

        # Small chance of Kognitos processing error (now dynamic)
        if random.random() < dynamic_error_rate:
            error_detail = "Kognitos processing error"
            if data_issues:
                error_detail += f" due to data quality issues: {', '.join(data_issues)}"
            else:
                error_detail += ": minor system anomaly."
            
            return {
                "status": "FAILURE",
                "error_details": error_detail,
                "merkle_root": compute_merkle_root(transactions),
                "invoice_id": invoice_id,
                "error_type": "system_processing",  # Keep as system_processing
                "simulated_cycle_time_s": total_simulated_sleep_s,
            }

        # 4. Finalize
        transactions.append(f"PROCESS_COMPLETE:{invoice_id}")

        # 5. Generate Audit Seal
        merkle_root = compute_merkle_root(transactions)

        return {
            "status": "SUCCESS",
            "error_details": None,
            "merkle_root": merkle_root,
            "invoice_id": invoice_id,
            "error_type": None,
            "simulated_cycle_time_s": total_simulated_sleep_s,
        }
        
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_details": str(e),
            "merkle_root": compute_merkle_root(transactions), # Still provide partial audit
            "invoice_id": invoice_id or f"failed_{invoice_path.name}",
            "error_type": "unknown_kognitos_error",
            "simulated_cycle_time_s": total_simulated_sleep_s,
        }
