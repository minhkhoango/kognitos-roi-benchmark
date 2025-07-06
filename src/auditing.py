# src/auditing.py
# Contains the logic for creating an immutable, tamper-proof audit trail
# using a Merkle Tree. This is a core part of the "Trust/Governance" solution.

import hashlib
from typing import List, Optional

def compute_merkle_root(transaction_ids: List[str]) -> Optional[str]:
    """
    Computes the Merkle root for a list of transaction IDs (strings).

    A Merkle root provides a single, verifiable hash for a set of data.
    If any transaction changes, the root hash will change completely,
    making tampering immediately obvious.

    Args:
        transaction_ids: A list of strings, where each string is a
                         unique identifier for an action in a workflow.

    Returns:
        The final SHA-256 Merkle root as a hex digest string, or None if the
        input list is empty.
    """
    if not transaction_ids:
        return None

    # Start with the initial list of hashes (the "leaves" of the tree).
    # We hash the initial IDs to ensure uniform format.
    hashed_transactions: List[str] = [
        hashlib.sha256(tx_id.encode()).hexdigest() for tx_id in transaction_ids
    ]

    # The tree is built by repeatedly hashing pairs of nodes.
    # If there's an odd number of nodes at any level, we duplicate the last one
    # to ensure there's always a pair.
    while len(hashed_transactions) > 1:
        if len(hashed_transactions) % 2 != 0:
            hashed_transactions.append(hashed_transactions[-1])

        next_level: List[str] = []
        # Process pairs of hashes from the current level.
        for i in range(0, len(hashed_transactions), 2):
            # Concatenate the pair.
            pair_concatenated: str = hashed_transactions[i] + hashed_transactions[i + 1]
            # Hash the concatenated string to create the parent node.
            new_hash: str = hashlib.sha256(pair_concatenated.encode()).hexdigest()
            next_level.append(new_hash)
        
        # The new level of hashes becomes the current level for the next iteration.
        hashed_transactions = next_level

    # The loop continues until only one hash remains: the Merkle root.
    return hashed_transactions[0]
