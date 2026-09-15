from __future__ import annotations

import logging
from collections import Counter

from include.metrics_store import write_metrics
from include.support_systems import HELPDESK_REPLIES

log = logging.getLogger(__name__)

CATEGORIES = (
    "billing",
    "hardware_failure",
    "order_change",
    "product_question",
    "product_request",
)

CLASSIFY_TICKET_SYSTEM_PROMPT = (
    "You categorise incoming CosMarket support tickets. Pick the single "
    "category that best fits what the customer is asking for, based only "
    "on their message."
)

ATTEMPTS = 5

AMBIGUOUS_TICKETS = [
    {
        "ticket_id": "TKT-4101",
        "customer_ask": (
            "Subject: Cancelled last week, still charged\n\n"
            "I cancelled order ORD-90211 before it shipped and got the "
            "confirmation, but 1,800 cr came off my account this morning "
            "anyway. Can you sort this out?"
        ),
    },
    {
        "ticket_id": "TKT-4102",
        "customer_ask": (
            "Subject: Grinding noise from the air scrubber\n\n"
            "The Air Scrubber X-Series has started making a grinding noise on "
            "the intake side. Is that normal at this age, or should I be "
            "worried?"
        ),
    },
    {
        "ticket_id": "TKT-4103",
        "customer_ask": (
            "Subject: EVA suit visor cracked in transit\n\n"
            "The EVA Suit Mark VII arrived with a hairline crack in the visor. "
            "While you sort that out, I have realised I ordered the wrong size, "
            "so I would rather have the next size up."
        ),
    },
    {
        "ticket_id": "TKT-4104",
        "customer_ask": (
            "Subject: Filter subscription cost\n\n"
            "What does the quarterly filter subscription cost, and can you add "
            "it to the invoice we already have open rather than a new one?"
        ),
    },
]

GOLDEN_LABELS = {
    "TKT-4001": "billing",
    "TKT-4002": "hardware_failure",
    "TKT-4003": "product_question",
    "TKT-4004": "order_change",
    "TKT-4005": "product_request",
    "TKT-4101": "billing",
    "TKT-4102": "hardware_failure",
    "TKT-4103": "order_change",
    "TKT-4104": "billing",
}


def labelled_tickets() -> list[dict]:
    tickets = [
        {"ticket_id": t["ticket_id"], "customer_ask": t["customer_ask"]}
        for t in HELPDESK_REPLIES
    ] + AMBIGUOUS_TICKETS
    return [
        {**ticket, "expected_category": GOLDEN_LABELS[ticket["ticket_id"]]}
        for ticket in tickets
        if ticket["ticket_id"] in GOLDEN_LABELS
    ]


def sampling_cases(tickets: list[dict], attempts: int = ATTEMPTS) -> list[dict]:
    return [
        {**ticket, "attempt": attempt}
        for ticket in tickets
        for attempt in range(1, attempts + 1)
    ]


def score_pass_at_k(cases: list[dict], classifications: list[dict]) -> dict:
    by_ticket: dict[str, dict] = {}
    for case, result in zip(cases, classifications):
        entry = by_ticket.setdefault(
            case["ticket_id"], {"expected": case["expected_category"], "labels": []}
        )
        entry["labels"].append(result["category"])

    rows = []
    for ticket_id, entry in sorted(by_ticket.items()):
        labels, expected = entry["labels"], entry["expected"]
        counts = Counter(labels)
        majority, majority_count = counts.most_common(1)[0]
        rows.append(
            {
                "ticket_id": ticket_id,
                "expected": expected,
                "labels": labels,
                "majority": majority,
                "pass_at_1": labels[0] == expected,
                "pass_at_k": expected in labels,
                "majority_correct": majority == expected,
                "agreement": majority_count / len(labels),
            }
        )
        log.info(
            "%s expected=%s samples=%s majority=%s",
            ticket_id,
            expected,
            labels,
            majority,
        )

    total = len(rows)
    if not total:
        log.warning("no classifications to score")
        return {}

    metrics = {
        "tickets": total,
        "attempts_per_ticket": len(rows[0]["labels"]),
        "pass_at_1": sum(r["pass_at_1"] for r in rows) / total,
        "pass_at_k": sum(r["pass_at_k"] for r in rows) / total,
        "majority_vote_accuracy": sum(r["majority_correct"] for r in rows) / total,
        "mean_agreement": sum(r["agreement"] for r in rows) / total,
    }
    write_metrics(**metrics)
    return {**metrics, "per_ticket": rows}
