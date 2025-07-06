# generate_invoices.py
# This script creates the synthetic data needed for the benchmark.
# It's designed to produce both "clean" invoices and "monster" invoices
# to test the robustness of the processing logic.

import csv 
import random
import uuid
from pathlib import Path
from typing import Dict, Any, List

# --- Configuration ---
TOTAL_INVOICES: int = 50
MONSTER_INVOICE_PERCENTAGE: float = 0.20  # 20% of invoices will be monsters
DATA_DIR: Path = Path("data")
VENDORS: List[str] = ["Stark Industries", "Wayne Enterprises", "Cyberdyne Systems", "Acme Corp", "Soylent Corp"]
# --- End Configuration ---

def create_invoice_data(is_monster: bool) -> Dict[str, Any]:
    """
    Generates a dictionary representing a single invoice's data.
    If `is_monster` is True, it introduces realistic errors.
    """
    invoice_id = str(uuid.uuid4())
    vendor: str = random.choice(VENDORS)
    quantity: int = random.randint(1, 10)
    unit_price: float = round(random.uniform(20.0, 500.0), 2)
    total: float = round(quantity * unit_price, 2)

    # Base, clean data structure
    data: Dict[str, Any] = {
        "invoice_id": invoice_id,
        "vendor_name": vendor,
        "invoice_date": f"2025-07-{random.randint(10, 25)}",
        "quantity": quantity,
        "unit_price": unit_price,
        "total": total,
    }

    if is_monster:
        # Introduce chaos
        chaos_type = random.choice(["missing_id", "bad_date", "negative_qty", "extra_col"])
        
        if chaos_type == "missing_id":
            data["invoice_id"] = ""  # Missing ID
        elif chaos_type == "bad_date":
            data["invoice_date"] = f"{random.randint(1,12)}-{random.randint(1,28)}-2025" # Bad format
        elif chaos_type == "negative_qty":
            data["quantity"] = -abs(quantity) # Negative value
            data["total"] = -abs(total)
        elif chaos_type == "extra_col":
            data["notes"] = "Urgent payment required" # Extra column

    return data

def main() -> None:
    """
    Main function to generate and write all invoice files.
    """
    if not DATA_DIR.exists():
        print(f"Creating data directory: {DATA_DIR}")
        DATA_DIR.mkdir()

    num_monsters = int(TOTAL_INVOICES * MONSTER_INVOICE_PERCENTAGE)
    invoice_types: List[bool] = [True] * num_monsters + [False] * (TOTAL_INVOICES - num_monsters)
    random.shuffle(invoice_types)

    all_headers: set[str] = {"invoice_id", "vendor_name", "invoice_date", "quantity", "unit_price", "total", "notes"}

    for i, is_monster in enumerate(invoice_types):
        invoice_data: Dict[str, Any] = create_invoice_data(is_monster)
        file_path: Path = DATA_DIR / f"invoice_{i+1:03d}.csv"

        try:
            with open(file_path, "w", newline="") as csvfile:
                # Ensure all possible headers are written for consistency, even if empty
                writer: csv.DictWriter[str] = csv.DictWriter(csvfile, fieldnames=list(all_headers))
                writer.writeheader()
                writer.writerow(invoice_data)
        except IOError as e:
            print(f"Error writing file {file_path}: {e}")
            continue
    
    print(f"Generated {TOTAL_INVOICES} invoices ({num_monsters} monsters) in '{DATA_DIR}/'")


if __name__ == "__main__":
    main()