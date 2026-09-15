from __future__ import annotations

import csv
import logging
import re
from pathlib import Path

from include.cosmarket_db import SCHEMA_FILE, create_schema, get_conn, upsert
from include.otel_traces import SPANS_FILE
from include.support_systems import (
    CUSTOMERS,
    HELPDESK_REPLIES,
    PRODUCTS,
    thread_id_for,
    ticket_subject,
)

log = logging.getLogger(__name__)

CATALOGUE_FILE = Path(__file__).parent / "csvs" / "products.csv"

_CREATE_TABLE_PATTERN = re.compile(
    r"CREATE TABLE IF NOT EXISTS\s+(\w+)", re.IGNORECASE
)
_CREATE_SEQUENCE_PATTERN = re.compile(
    r"CREATE SEQUENCE IF NOT EXISTS\s+(\w+)", re.IGNORECASE
)


def schema_tables() -> list[str]:
    return _CREATE_TABLE_PATTERN.findall(SCHEMA_FILE.read_text())


def schema_sequences() -> list[str]:
    return _CREATE_SEQUENCE_PATTERN.findall(SCHEMA_FILE.read_text())


def drop_all_tables() -> list[str]:
    tables = schema_tables()
    with get_conn() as conn:
        for sequence in schema_sequences():
            conn.execute(f"DROP SEQUENCE IF EXISTS {sequence}")
        for table in reversed(tables):
            conn.execute(f"DROP TABLE IF EXISTS {table}")
    log.info("dropped %s tables: %s", len(tables), tables)
    return tables


def clear_exported_spans() -> int:
    if not SPANS_FILE.exists():
        log.info("no span file at %s, nothing to clear", SPANS_FILE)
        return 0
    size = SPANS_FILE.stat().st_size
    SPANS_FILE.write_text("")
    log.info("cleared %s bytes of exported spans from %s", size, SPANS_FILE)
    return size


def seed_reference_data() -> int:
    upsert("customers", "customer_id", CUSTOMERS)
    upsert("products", "product_sku", _catalogue_rows(full=True))
    upsert("products", "product_sku", PRODUCTS)

    threads = []
    messages = []
    for ticket in HELPDESK_REPLIES:
        ticket_id = ticket["ticket_id"]
        thread_id = thread_id_for(ticket_id)
        subject = ticket_subject(ticket)
        threads.append(
            {
                "thread_id": thread_id,
                "ticket_id": ticket_id,
                "customer_id": ticket["customer_id"],
                "product_sku": ticket["product_sku"],
                "subject": subject,
                "resolved": None,
                "reopened": None,
                "csat_average": None,
                "detractor": None,
            }
        )
        messages.append(
            {
                "message_id": f"MSG-{ticket_id}-1",
                "thread_id": thread_id,
                "turn": 1,
                "direction": "inbound",
                "sender": ticket["customer_id"],
                "subject": subject,
                "body": ticket["customer_ask"],
            }
        )

    upsert("support_threads", "thread_id", threads)
    upsert("support_messages", "message_id", messages)
    log.info("seeded %s customers, %s products, %s tickets", len(CUSTOMERS), len(PRODUCTS), len(threads))
    return len(threads)


def _number(value: str | None, cast):
    text = (value or "").strip()
    if not text:
        return None
    try:
        return cast(text)
    except ValueError:
        log.warning("could not read %r as %s, treating it as missing", value, cast.__name__)
        return None


def _flag(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"true", "yes", "1"}


def _catalogue_rows(full: bool = False) -> list[dict]:
    with CATALOGUE_FILE.open(newline="") as handle:
        rows = list(csv.DictReader(handle))

    products = []
    for row in rows:
        if not _flag(row.get("is_active")):
            continue
        product = {
            "product_sku": row["product_sku"],
            "category_id": _number(row.get("category_id"), int),
            "weight_kg": _number(row.get("weight_kg"), float),
            "dimensions_cm": (row.get("dimensions_cm") or "").strip() or None,
            "base_price_credits": _number(row.get("base_price_credits"), float),
            "is_hazardous": _flag(row.get("is_hazardous")),
            "requires_cold_chain": _flag(row.get("requires_cold_chain")),
        }
        if full:
            product |= {
                "product_id": _number(row.get("product_id"), int),
                "product_name": (row.get("product_name") or "").strip() or None,
                "description": (row.get("description") or "").strip() or None,
                "is_active": True,
            }
        products.append(product)
    return products


def seed_shipment_history() -> int:
    from include.mlops.shipment_features import generate_shipment_history

    shipments, features, labels = generate_shipment_history(
        products=_catalogue_rows(),
        customer_ids=[c["customer_id"] for c in CUSTOMERS],
    )

    upsert("shipments", "shipment_id", shipments)
    upsert("shipment_features", "shipment_id", features)
    upsert("shipment_labels", "shipment_id", labels)

    outcomes: dict[str, int] = {}
    for label in labels:
        outcomes[label["delivery_outcome"]] = outcomes.get(label["delivery_outcome"], 0) + 1
    log.info(
        "seeded %s shipments (%s labelled, %s in transit), outcomes: %s",
        len(shipments),
        len(labels),
        len(shipments) - len(labels),
        outcomes,
    )
    return len(shipments)


def rebuild_schema() -> None:
    create_schema()
