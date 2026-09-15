from __future__ import annotations

import re

CSAT_DIMENSIONS = ("resolution", "speed", "friendliness")

SUPPORT_ADDRESS = "support@cosmarket.example"

SCORE_SUPPORT_REPLY_SYSTEM_PROMPT = (
    "You score customer support responses against the evidence "
    "provided. Judge only what the evidence supports. Do not "
    "rewrite the response."
)

SCORE_CUSTOMER_REPLY_SYSTEM_PROMPT = (
    "You score how a customer reacted to a support response. Judge "
    "only the customer's reply, using the earlier messages as "
    "context."
)

PRODUCTS = [
    {
        "product_sku": "ION-DRIVE-MK3",
        "product_name": "Ion Drive Mark III",
        "description": "Efficient ion propulsion engine",
        "base_price_credits": 45000.00,
    },
    {
        "product_sku": "OXY-GEN-5000",
        "product_name": "Oxygen Generator Mark V",
        "description": "High-efficiency oxygen generation unit for medium spacecraft",
        "base_price_credits": 2500.00,
    },
    {
        "product_sku": "AIR-SCRUB-X",
        "product_name": "Air Scrubber X-Series",
        "description": "Industrial grade CO2 scrubber",
        "base_price_credits": 1800.00,
    },
    {
        "product_sku": "EVA-SUIT-MK7",
        "product_name": "EVA Suit Mark VII",
        "description": "Latest generation spacewalk suit",
        "base_price_credits": 28000.00,
    },
    {
        "product_sku": "SELDON-CRYO-ICE",
        "product_name": "Seldon's Psychohistory Swirl",
        "description": (
            "Freeze-dried Trantorian ice cream with probability-mapped flavor "
            "layers, tastes different every time but always delicious"
        ),
        "base_price_credits": 45.00,
    },
]

CUSTOMERS = [
    {
        "customer_id": "CUS-118",
        "full_name": "James T. Kirk",
        "home_port": "USS Enterprise",
        "customer_tier": "Standard",
    },
    {
        "customer_id": "CUS-204",
        "full_name": "Kathryn Janeway",
        "home_port": "USS Voyager",
        "customer_tier": "Priority",
    },
    {
        "customer_id": "CUS-091",
        "full_name": "Jean-Luc Picard",
        "home_port": "USS Enterprise-D",
        "customer_tier": "Standard",
    },
    {
        "customer_id": "CUS-457",
        "full_name": "Benjamin Sisko",
        "home_port": "Deep Space 9",
        "customer_tier": "Priority",
    },
    {
        "customer_id": "CUS-330",
        "full_name": "Jonathan Archer",
        "home_port": "Enterprise NX-01",
        "customer_tier": "Standard",
    },
]

HELPDESK_REPLIES = [
    {
        "ticket_id": "TKT-4001",
        "customer_id": "CUS-118",
        "product_sku": "ION-DRIVE-MK3",
        "customer_ask": (
            "Subject: Charged twice for order ORD-88412\n\n"
            "My account was billed twice for the Ion Drive Mark III that shipped "
            "to the Enterprise. Please refund the duplicate charge."
        ),
        "order_record": (
            "customer_id: CUS-118\n"
            "customer_tier: Standard\n"
            "order: ORD-88412, 1x ION-DRIVE-MK3 Ion Drive Mark III, "
            "destination USS Enterprise\n"
            "charges: 2026-08-01 45,000 cr captured, 2026-08-01 45,000 cr "
            "captured (duplicate, flagged by payments)\n"
            "refunds: 2026-08-14 45,000 cr issued to account ending 4412\n"
            "refund_settlement: 5-7 standard days"
        ),
        "support_response": (
            "Hi James,\n\nYou're right, ORD-88412 was charged twice on August 1. "
            "We refunded the duplicate 45,000 cr to the account ending 4412 on "
            "August 14, and it should settle back within 5-7 standard days.\n\n"
            "Sorry for the extra step on your end.\n\nBest,\nCosMarket Support"
        ),
        "agent": "Chris",
        "responded_at": "2026-08-14T09:12:00Z",
    },
    {
        "ticket_id": "TKT-4002",
        "customer_id": "CUS-204",
        "product_sku": "OXY-GEN-5000",
        "customer_ask": (
            "Subject: Oxygen generator failed, habitat on backup\n\n"
            "The Oxygen Generator Mark V we bought from you failed 20 minutes "
            "ago and our habitat is running on backup canisters. We have six "
            "people here. What do we do?"
        ),
        "order_record": (
            "customer_id: CUS-204\n"
            "customer_tier: Priority\n"
            "order: ORD-88907, 1x OXY-GEN-5000 Oxygen Generator Mark V, "
            "destination USS Voyager, delivered 2026-06-02\n"
            "warranty: active, life support class, 4 hour replacement SLA\n"
            "recall: RCL-19 open on this batch, faulty intake valve\n"
            "remedy: replacement unit in stock at Voyager resupply depot, "
            "emergency dispatch available\n"
            "escalation_path: life support tickets page the on-call engineer"
        ),
        "support_response": (
            "Yeah, we're aware of an issue with that batch. There's a recall "
            "open (RCL-19) on the intake valve. Watch the recall page for "
            "updates, no need to keep writing in.\n\nCosMarket Support"
        ),
        "agent": "Chris",
        "responded_at": "2026-08-14T14:26:00Z",
    },
    {
        "ticket_id": "TKT-4003",
        "customer_id": "CUS-091",
        "product_sku": "AIR-SCRUB-X",
        "customer_ask": (
            "Subject: Replacement filters for the Air Scrubber\n\n"
            "Where do I order replacement filters for the Air Scrubber "
            "X-Series, and how often should they be changed?"
        ),
        "order_record": (
            "customer_id: CUS-091\n"
            "customer_tier: Standard\n"
            "order: ORD-87220, 1x AIR-SCRUB-X Air Scrubber X-Series, "
            "delivered 2026-04-18\n"
            "consumables: filter cartridge AIR-SCRUB-FLT, 90 cr each, "
            "recommended change every 400 hours\n"
            "subscription_available: yes, quarterly filter delivery\n"
            "docs: /docs/products/air-scrub-x/maintenance"
        ),
        "support_response": (
            "Hi Jean-Luc,\n\nHappy to help. Filter cartridges are AIR-SCRUB-FLT at "
            "90 cr each, and you'll want to change them every 400 hours of "
            "runtime. The maintenance guide is at "
            "/docs/products/air-scrub-x/maintenance. Since you're on the "
            "Standard tier your filters ship free for the lifetime of the unit, "
            "so just claim them under Warranty whenever you need a new one."
            "\n\nBest,\nCosMarket Support"
        ),
        "agent": "Chris",
        "responded_at": "2026-08-14T11:03:00Z",
    },
    {
        "ticket_id": "TKT-4004",
        "customer_id": "CUS-457",
        "product_sku": "EVA-SUIT-MK7",
        "customer_ask": (
            "Subject: Cancel order before it ships\n\n"
            "I ordered an EVA Suit Mark VII yesterday and need to cancel it. Can "
            "you confirm the cancellation went through and that I won't be "
            "charged?"
        ),
        "order_record": (
            "customer_id: CUS-457\n"
            "customer_tier: Priority\n"
            "order: ORD-89104, 1x EVA-SUIT-MK7 EVA Suit Mark VII, 28,000 cr, "
            "status: awaiting dispatch\n"
            "cancellation_request: received 2026-08-14\n"
            "cancellation_window: until dispatch, dispatch scheduled 2026-08-16\n"
            "status: cancellation accepted, authorisation released, "
            "no charge will settle"
        ),
        "support_response": (
            "Hi Benjamin,\n\nThanks for the heads up. The EVA Suit Mark VII is a "
            "great unit and it stays available on your account if you change "
            "your mind. Returns are accepted for 30 standard days after "
            "delivery if you'd rather send it back later.\n\nBest,\n"
            "CosMarket Support"
        ),
        "agent": "Chris",
        "responded_at": "2026-08-14T16:41:00Z",
    },
    {
        "ticket_id": "TKT-4005",
        "customer_id": "CUS-330",
        "product_sku": "SELDON-CRYO-ICE",
        "customer_ask": (
            "Subject: Bigger tubs of the Psychohistory Swirl?\n\n"
            "The half litre tubs of Seldon's Psychohistory Swirl are gone in two "
            "days flat up here. Any chance of a family size?"
        ),
        "order_record": (
            "customer_id: CUS-330\n"
            "customer_tier: Standard\n"
            "order: ORD-88650, 6x SELDON-CRYO-ICE Seldon's Psychohistory Swirl, "
            "45 cr each, cold chain\n"
            "product_request: PR-1188 family size tub, status: under consideration\n"
            "roadmap_commitment: none\n"
            "vote_count: 412"
        ),
        "support_response": (
            "Hi Jonathan,\n\nGreat news, the family size tub ships in the next "
            "catalogue update at the end of the quarter and will be stocked at "
            "every depot. Thanks for being patient with us.\n\nBest,\n"
            "CosMarket Support"
        ),
        "agent": "Chris",
        "responded_at": "2026-08-14T13:55:00Z",
    },
]

INBOX_REPLIES = [
    {
        "ticket_id": "TKT-4001",
        "customer_response": (
            "Perfect, I can see the refund pending on the account already. "
            "Thanks for turning that around so fast."
        ),
        "received_at": "2026-08-14T10:47:00Z",
    },
    {
        "ticket_id": "TKT-4002",
        "customer_response": (
            "We are on backup air with six people aboard and \"no need to keep "
            "writing in\" is not an acceptable answer. There is a replacement "
            "unit sitting in your depot on this station. I want it dispatched "
            "now and I want someone on a call with me today."
        ),
        "received_at": "2026-08-14T14:33:00Z",
    },
    {
        "ticket_id": "TKT-4003",
        "customer_response": (
            "Thanks for the part number and the schedule. I went to claim the "
            "free filters under Warranty though and there is no such option, and "
            "the depot says they are 90 cr each. Is the lifetime filter thing "
            "something you have to enable, or does it not exist? I had already "
            "told the crew we were covered."
        ),
        "received_at": "2026-08-14T15:20:00Z",
    },
    {
        "ticket_id": "TKT-4004",
        "customer_response": (
            "That doesn't answer what I asked. I need written confirmation that "
            "the cancellation went through and that the 28,000 cr will not be "
            "charged. I am not looking for a returns policy."
        ),
        "received_at": "2026-08-14T17:08:00Z",
    },
    {
        "ticket_id": "TKT-4005",
        "customer_response": (
            "Amazing, that made my day. I'll let the whole crew know it's "
            "coming at the end of the quarter."
        ),
        "received_at": "2026-08-14T14:12:00Z",
    },
]

SURVEY_RESPONSES = [
    {
        "ticket_id": "TKT-4001",
        "ratings": {"resolution": 5, "speed": 5, "friendliness": 5},
        "submitted_at": "2026-08-15T08:02:00Z",
    },
    {
        "ticket_id": "TKT-4002",
        "ratings": {"resolution": 3, "speed": 2, "friendliness": 1},
        "submitted_at": "2026-08-15T07:44:00Z",
    },
    {
        "ticket_id": "TKT-4003",
        "ratings": {"resolution": 3, "speed": 4, "friendliness": 5},
        "submitted_at": "2026-08-15T09:31:00Z",
    },
    {
        "ticket_id": "TKT-4004",
        "ratings": {"resolution": 2, "speed": 4, "friendliness": 4},
        "submitted_at": "2026-08-15T08:57:00Z",
    },
    {
        "ticket_id": "TKT-4005",
        "ratings": {"resolution": 5, "speed": 5, "friendliness": 5},
        "submitted_at": "2026-08-15T10:15:00Z",
    },
]

GOLDEN_REPLIES = {
    "TKT-4001": (
        "Hi James,\n\nYou're right, ORD-88412 was charged twice on August 1. We "
        "refunded the duplicate 45,000 cr to the account ending 4412 on August "
        "14, and it settles back within 5-7 standard days.\n\nSorry for the "
        "extra step on your end.\n\nBest,\nCosMarket Support"
    ),
    "TKT-4002": (
        "Hi Kathryn,\n\nThis is a life support fault, so I have paged our on-call "
        "engineer and they will contact you directly.\n\nYour unit is covered by "
        "an active warranty with a 4 hour replacement SLA, and it is affected by "
        "recall RCL-19, a faulty intake valve. There is a replacement unit in "
        "stock at the Voyager resupply depot and I have requested emergency dispatch "
        "to you now.\n\nBest,\nCosMarket Support"
    ),
    "TKT-4003": (
        "Hi Jean-Luc,\n\nThe filter you need is cartridge AIR-SCRUB-FLT at 90 cr "
        "each, and we recommend changing it every 400 hours of runtime. The "
        "maintenance guide is at /docs/products/air-scrub-x/maintenance.\n\nIf "
        "you would rather not track it, we offer a quarterly filter "
        "subscription that ships them to you automatically.\n\nBest,\n"
        "CosMarket Support"
    ),
    "TKT-4004": (
        "Hi Benjamin,\n\nConfirmed: the cancellation of ORD-89104 was accepted on "
        "August 14, before the dispatch date of August 16. The authorisation has "
        "been released and the 28,000 cr will not be charged.\n\nBest,\n"
        "CosMarket Support"
    ),
    "TKT-4005": (
        "Hi Jonathan,\n\nThanks for asking. A family size tub is logged as PR-1188 "
        "and it is under consideration with 412 votes, but there is no committed "
        "release date yet, so I do not want to promise one.\n\nI have added your "
        "note to the request.\n\nBest,\nCosMarket Support"
    ),
}

_TICKET_ID_PATTERN = re.compile(r"\b(TKT-\d{4})\b")


def find_ticket_id(text: str | None) -> str | None:
    if not text:
        return None
    match = _TICKET_ID_PATTERN.search(str(text))
    return match.group(1) if match else None


def ticket_subject(ticket: dict) -> str:
    first_line = ticket["customer_ask"].splitlines()[0]
    return first_line.removeprefix("Subject:").strip()


def thread_id_for(ticket_id: str) -> str:
    return ticket_id.replace("TKT-", "THR-")


def ticket_by_id(ticket_id: str) -> dict | None:
    return next((t for t in HELPDESK_REPLIES if t["ticket_id"] == ticket_id), None)


def product_by_sku(product_sku: str) -> dict | None:
    return next((p for p in PRODUCTS if p["product_sku"] == product_sku), None)


def fetch_tickets() -> list[dict]:
    return HELPDESK_REPLIES


def fetch_customers() -> list[dict]:
    return CUSTOMERS


def fetch_support_replies() -> list[dict]:
    return HELPDESK_REPLIES


def fetch_customer_replies() -> list[dict]:
    return INBOX_REPLIES


def fetch_satisfaction_scores() -> list[dict]:
    return SURVEY_RESPONSES
