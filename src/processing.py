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
MANUAL_SLEEP_MIN: float = 6.0
MANUAL_SLEEP_MAX: float = 14.0
# Manual process error rate (human error rate 1.6-3%, up to 10-15% possible)
MANUAL_ERROR_RATE: float = 0.07

# Kognitos process timing (in minutes)
KOGNITOS_SLEEP_MIN: float = 0.6
KOGNITOS_SLEEP_MAX: float = 1.5
# Kognitos process error rate (automation error rate ~0.5% or less)
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
# --- End Type Definitions ---

def run_baseline_process(invoice_path: Path, real_hours_per_demo_second: float) -> ProcessingResult:
    """
    Simulates the slow, error-prone manual process.
    - Reads an invoice CSV.
    - Introduces a delay to simulate human interaction.
    - Has a random chance of failing to simulate data entry errors.
    """
    invoice_id = ""
    try:
        with open(invoice_path, "r") as f:
            reader = csv.DictReader(f)
            data = next(reader)
            invoice_id = data.get("invoice_id", "UNKNOWN")

        # Simulate human thinking and typing time (adjusted for scaling)
        time.sleep(random.uniform(
            MANUAL_SLEEP_MIN, MANUAL_SLEEP_MAX
        ) / real_hours_per_demo_second / MIN_IN_A_HOUR)

        # Realistic manual error rate
        if random.random() < MANUAL_ERROR_RATE:
            return {
                "status": "FAILURE",
                "error_details": "Manual data entry error: incorrect total.",
                "merkle_root": None,
                "invoice_id": invoice_id,
                "error_type": "data_quality",
            }
        
        if not invoice_id:
            return {
                "status": "FAILURE",
                "error_details": "Manual validation error: Missing invoice ID.",
                "merkle_root": None,
                "invoice_id": invoice_id,
                "error_type": "data_quality",
            }

        # Add a new error type for manual operational issues
        if random.random() < 0.01:  # 1% chance for operational error in manual
            return {
                "status": "FAILURE",
                "error_details": "Manual operational error: payment misrouting or delay.",
                "merkle_root": None,
                "invoice_id": invoice_id,
                "error_type": "system_operational",
            }

        return {
            "status": "SUCCESS",
            "error_details": None,
            "merkle_root": None, # No audit trail for manual process
            "invoice_id": invoice_id,
            "error_type": None,
        }

    except Exception as e:
        return {
            "status": "FAILURE",
            "error_details": str(e),
            "merkle_root": None,
            "invoice_id": invoice_id or f"failed_{invoice_path.name}",
            "error_type": "unknown_baseline_error",
        }

def _mock_kognitos_api(steps: str, data: Dict[str, Any], real_hours_per_demo_second: float) -> Dict[str, Any]:
    """
    A mock function that simulates a call to the Kognitos API.
    It's fast and reliable.
    """
    # Simulate network latency and processing time (adjusted for scaling)
    time.sleep(random.uniform(
        KOGNITOS_SLEEP_MIN, KOGNITOS_SLEEP_MAX
    ) / real_hours_per_demo_second / MIN_IN_A_HOUR)

    # Kognitos can still fail if the input is truly garbage
    if not data.get("invoice_id"):
        return {"status": "FAILURE", "reason": "Cannot proceed without invoice_id"}

    return {"status": "SUCCESS", "extracted_total": data.get("total")}

def run_kognitos_process(invoice_path: Path, real_hours_per_demo_second: float) -> ProcessingResult:
    """
    Simulates the fast, reliable, and auditable Kognitos process.
    - Reads the invoice.
    - Calls a (mocked) Kognitos API.
    - Generates an audit trail and computes a Merkle root.
    """
    transactions: List[str] = []
    invoice_id = ""
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

        # 3. Execute with Kognitos (mocked)
        api_result = _mock_kognitos_api(kognitos_steps, data, real_hours_per_demo_second)
        transactions.append(f"API_CALL_STATUS:{api_result['status']}")

        if api_result["status"] != "SUCCESS":
            return {
                "status": "FAILURE",
                "error_details": f"Kognitos API failed: unprocessable_input_format ({api_result.get('reason')})",
                "merkle_root": compute_merkle_root(transactions),
                "invoice_id": invoice_id,
                "error_type": "data_extraction",  # Change to data_extraction for unprocessable input
            }

        # Small chance of Kognitos processing error
        if random.random() < KOGNITOS_ERROR_RATE:
            return {
                "status": "FAILURE",
                "error_details": "Kognitos processing error: minor system anomaly.",
                "merkle_root": compute_merkle_root(transactions),
                "invoice_id": invoice_id,
                "error_type": "system_processing",  # Keep as system_processing
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
        }
        
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_details": str(e),
            "merkle_root": compute_merkle_root(transactions), # Still provide partial audit
            "invoice_id": invoice_id or f"failed_{invoice_path.name}",
            "error_type": "unknown_kognitos_error",
        }
