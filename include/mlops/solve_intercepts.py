from __future__ import annotations

import math
from collections import Counter

from include.mlops import shipment_features as sf


def _balance() -> dict[str, float]:
    from include.demo_setup import _catalogue_rows
    from include.support_systems import CUSTOMERS

    _, _, labels = sf.generate_shipment_history(
        products=_catalogue_rows(),
        customer_ids=[c["customer_id"] for c in CUSTOMERS],
    )
    counts = Counter(row["delivery_outcome"] for row in labels)
    total = sum(counts.values())
    return {label: counts.get(label, 0) / total for label in sf.CLASS_LABELS}


def solve(iterations: int = 60, rate: float = 0.7) -> dict[str, float]:
    offsets = dict.fromkeys(sf.CLASS_LABELS, 0.0)
    best, best_error = dict(offsets), float("inf")
    for _ in range(iterations):
        sf.INTERCEPTS.update(offsets)
        observed = _balance()
        error = sum(abs(observed[c] - sf.TARGET_BALANCE[c]) for c in sf.CLASS_LABELS)
        if error < best_error:
            best, best_error = dict(offsets), error
        for label in sf.CLASS_LABELS:
            share = max(observed[label], 1e-4)
            offsets[label] += rate * math.log(sf.TARGET_BALANCE[label] / share)
    sf.INTERCEPTS.update(best)
    return best, best_error, _balance()


if __name__ == "__main__":
    offsets, error, observed = solve()
    print("INTERCEPTS = {")
    for label in sf.CLASS_LABELS:
        print(f'    "{label}": {offsets[label]:.2f},')
    print("}")
    print()
    print(f"total absolute error {error:.4f}")
    for label in sf.CLASS_LABELS:
        print(f"  {label:<18} target {sf.TARGET_BALANCE[label]:.0%}  got {observed[label]:.1%}")
