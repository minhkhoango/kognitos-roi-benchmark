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

# --- Type Definitions ---
class ProcessingResult(TypedDict):
    """A standardized structure for returning results from processing functions."""
    status: str  # 'SUCCESS' or 'FAILURE'
    error_details: Optional[str]
    merkle_root: Optional[str]
    invoice_id: str
# --- End Type Definitions ---

def run_baseline_process(invoice_path: Path) -> ProcessingResult:
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

        # Simulate human thinking and typing time
        time.sleep(random.uniform(1.5, 2.5))

        # Hard-coded 10% chance of "manual error"
        if random.random() < 0.10:
            raise ValueError("Manual data entry error: incorrect total.")
        
        if not invoice_id:
            raise ValueError("Manual validation error: Missing invoice ID.")

        return {
            "status": "SUCCESS",
            "error_details": None,
            "merkle_root": None, # No audit trail for manual process
            "invoice_id": invoice_id,
        }

    except Exception as e:
        return {
            "status": "FAILURE",
            "error_details": str(e),
            "merkle_root": None,
            "invoice_id": invoice_id or f"failed_{invoice_path.name}",
        }

def _mock_kognitos_api(steps: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    A mock function that simulates a call to the Kognitos API.
    It's fast and reliable.
    """
    # Simulate network latency and processing time
    time.sleep(random.uniform(0.05, 0.15))

    # Kognitos can still fail if the input is truly garbage
    if not data.get("invoice_id"):
        return {"status": "FAILURE", "reason": "Cannot proceed without invoice_id"}

    return {"status": "SUCCESS", "extracted_total": data.get("total")}

def run_kognitos_process(invoice_path: Path) -> ProcessingResult:
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
        api_result = _mock_kognitos_api(kognitos_steps, data)
        transactions.append(f"API_CALL_STATUS:{api_result['status']}")

        if api_result["status"] != "SUCCESS":
            raise ConnectionError(f"Kognitos API failed: {api_result.get('reason')}")

        # 4. Finalize
        transactions.append(f"PROCESS_COMPLETE:{invoice_id}")

        # 5. Generate Audit Seal
        merkle_root = compute_merkle_root(transactions)

        return {
            "status": "SUCCESS",
            "error_details": None,
            "merkle_root": merkle_root,
            "invoice_id": invoice_id,
        }
        
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_details": str(e),
            "merkle_root": compute_merkle_root(transactions), # Still provide partial audit
            "invoice_id": invoice_id or f"failed_{invoice_path.name}",
        }
