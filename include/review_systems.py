from __future__ import annotations

import logging

log = logging.getLogger(__name__)

TRIAGE_REVIEW_SYSTEM_PROMPT = (
    "You triage product reviews for CosMarket, an ecommerce marketplace "
    "serving ships, stations, and colonies across the system. Each "
    "review is routed to one team on the strength of your "
    "classification, so pick the team's problem, not the loudest "
    "sentence.\n\n"
    "Categories:\n"
    "- safety_hazard: a failure that puts people or a habitat at risk, "
    "life support included.\n"
    "- product_defect: the product breaks, underperforms, or misses its "
    "specification, with no immediate risk to people.\n"
    "- delivery_damage: the fault is in transport, packaging, the depot, "
    "or the cold chain rather than in the product.\n"
    "- feature_request: the customer asks for something the product does "
    "not do yet.\n"
    "- praise: the customer is satisfied and names nothing to fix.\n"
    "- other: anything that does not clearly belong to one of the "
    "categories above.\n\n"
    "Use other rather than forcing a review into a category that only "
    "nearly fits. Rate a star count on its own as weak evidence: judge "
    "what the text describes. Lower your confidence when the review "
    "carries more than one problem or when two categories fit equally "
    "well, and summarise every problem it raises either way."
)

PRODUCT_REVIEWS = [
    {
        "review_id": "REV-2001",
        "customer_id": "CUS-204",
        "product_sku": "OXY-GEN-5000",
        "rating": 1,
        "title": "Cut out with six people aboard",
        "body": (
            "The generator dropped offline mid-shift and the habitat went to "
            "backup canisters. Six of us up here and about four hours of air in "
            "reserve. The intake valve is making a grinding noise when it tries "
            "to restart. We are not using this unit again until someone looks at "
            "it."
        ),
        "submitted_at": "2026-08-14T15:02:00Z",
    },
    {
        "review_id": "REV-2002",
        "customer_id": "CUS-118",
        "product_sku": "ION-DRIVE-MK3",
        "rating": 3,
        "title": "Thrust vector drifts after long burns",
        "body": (
            "Efficiency is as advertised, no complaints there. But past roughly "
            "40 hours of continuous burn the thrust vector drifts about half a "
            "degree off axis and we have to recalibrate by hand. Did that twice "
            "on the last transit. Nothing dangerous, just annoying on a long "
            "haul."
        ),
        "submitted_at": "2026-08-15T08:20:00Z",
    },
    {
        "review_id": "REV-2003",
        "customer_id": "CUS-330",
        "product_sku": "SELDON-CRYO-ICE",
        "rating": 2,
        "title": "Arrived as soup",
        "body": (
            "Six tubs ordered, all six arrived slushy and refrozen into one "
            "block. The depot clerk said the cold chain container sat on the pad "
            "for nine hours waiting for a slot. The product itself is great when "
            "it arrives intact, which is why this is two stars and not one."
        ),
        "submitted_at": "2026-08-15T09:41:00Z",
    },
    {
        "review_id": "REV-2004",
        "customer_id": "CUS-091",
        "product_sku": "AIR-SCRUB-X",
        "rating": 4,
        "title": "Please add a runtime counter",
        "body": (
            "Solid scrubber, does what it says. The filters are rated for 400 "
            "hours but the unit gives you no way to know how many hours it has "
            "actually run, so we track it on a clipboard like it is 2150. An "
            "hour meter on the front panel, or a reading over the maintenance "
            "port, would make this a five star unit."
        ),
        "submitted_at": "2026-08-15T11:07:00Z",
    },
    {
        "review_id": "REV-2005",
        "customer_id": "CUS-457",
        "product_sku": "EVA-SUIT-MK7",
        "rating": 5,
        "title": "Best suit I have worn",
        "body": (
            "Nine hours outside on a hull repair and I never thought about the "
            "suit once. Glove articulation is a real step up from the Mark VI "
            "and the visor stayed clear the whole time. Station is ordering four "
            "more."
        ),
        "submitted_at": "2026-08-15T12:33:00Z",
    },
    {
        "review_id": "REV-2006",
        "customer_id": "CUS-118",
        "product_sku": "SELDON-CRYO-ICE",
        "rating": 5,
        "title": "Five stars, also a question",
        "body": (
            "Ice cream is excellent, the crew fought over the last tub. Docking "
            "at the depot was a mess and the clerk was short with my nephew, "
            "though that is not really your product. Unrelated, does the Ion "
            "Drive Mark III come in any finish other than grey? Asking for the "
            "hangar deck."
        ),
        "submitted_at": "2026-08-15T13:58:00Z",
    },
    {
        "review_id": "REV-2007",
        "customer_id": "CUS-091",
        "product_sku": "OXY-GEN-5000",
        "rating": 3,
        "title": "Manual only ships in one language",
        "body": (
            "Unit runs fine. The maintenance manual only came in Standard "
            "Vulcan and half my engineering crew cannot read it, so we are "
            "working off a translation someone typed up. Three stars until there "
            "is a Federation Standard edition."
        ),
        "submitted_at": "2026-08-15T16:12:00Z",
    },
]


def fetch_reviews() -> list[dict]:
    return PRODUCT_REVIEWS


def file_to_queue(queue: str, reviews: list[dict]) -> int:
    for review in reviews:
        model_category = review["category"]
        routed_to = review.get("routed_to", model_category)
        overridden = "" if routed_to == model_category else f", overridden from {model_category}"
        log.info(
            "%s <- %s (%s, confidence %.2f%s): %s",
            queue,
            review["review_id"],
            routed_to,
            review["confidence"],
            overridden,
            review["summary"],
        )
    log.info("filed %s reviews to %s", len(reviews), queue)
    return len(reviews)
