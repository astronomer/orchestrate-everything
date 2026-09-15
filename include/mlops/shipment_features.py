from __future__ import annotations

import math
import random
from datetime import datetime, timedelta

DELIVERED_COUNT = 2500
IN_TRANSIT_COUNT = 200
SEED = 1969
CLASS_LABELS = ["on_time", "delayed", "damaged", "lost_in_transit"]


TARGET_BALANCE = {
    "on_time": 0.46,
    "delayed": 0.30,
    "damaged": 0.16,
    "lost_in_transit": 0.08,
}
INTERCEPTS = {
    "on_time": 1.28,
    "delayed": -1.64,
    "damaged": 1.60,
    "lost_in_transit": 1.49,
}

STATIONS = {
    "Luna Gateway": 0.38,
    "Ceres Hub": 2.6,
    "Mars Olympus": 78.0,
    "Belt Waystation 7": 240.0,
    "Europa Deep": 628.0,
    "Titan Outpost": 1275.0,
}
CARRIER_TIERS = {"economy": 1.0, "standard": 0.45, "express": 0.1}
PRIORITY_CLASSES = {"standard": 1.0, "priority": 0.5, "critical": 0.2}

CATEGORICAL_COLUMNS = [
    "destination_station",
    "carrier_tier",
    "priority_class",
    "category_id",
]
NUMERIC_COLUMNS = [
    "weight_kg",
    "volume_l",
    "base_price_credits",
    "is_hazardous",
    "requires_cold_chain",
    "transfer_distance_gm",
    "solar_storm_index",
    "warehouse_queue_depth",
]


FEATURE_COLUMNS = ["shipment_id", *NUMERIC_COLUMNS, *CATEGORICAL_COLUMNS]


def parse_volume_l(dimensions_cm: str | None) -> float:
    if not dimensions_cm:
        return 0.0
    try:
        length, width, height = (
            float(part) for part in dimensions_cm.lower().split("x")
        )
    except ValueError:
        return 0.0
    return round(length * width * height / 1000, 2)


def _outcome_logits(row: dict) -> dict[str, float]:
    distance = math.log1p(row["transfer_distance_gm"]) / math.log1p(1275.0)
    storm = row["solar_storm_index"] / 9.0
    queue = row["warehouse_queue_depth"] / 40.0
    bulk = min(row["weight_kg"] / 60.0, 1.5)
    carrier_drag = CARRIER_TIERS[row["carrier_tier"]]
    priority_drag = PRIORITY_CLASSES[row["priority_class"]]

    return {
        "on_time": INTERCEPTS["on_time"]
        + 2.4
        - 1.5 * distance
        - 1.1 * queue
        - 0.9 * carrier_drag
        - 0.5 * storm,
        "delayed": INTERCEPTS["delayed"]
        - 0.6
        + 2.2 * distance
        + 2.0 * queue
        + 1.4 * carrier_drag
        + 0.8 * priority_drag,
        "damaged": (
            INTERCEPTS["damaged"]
            - 2.1
            + 1.9 * row["is_hazardous"]
            + 1.6 * row["requires_cold_chain"]
            + 2.3 * storm
            + 0.7 * bulk
        ),
        "lost_in_transit": INTERCEPTS["lost_in_transit"]
        - 4.4
        + 2.6 * distance
        + 2.2 * storm
        + 0.9 * carrier_drag,
    }


def _sample_outcome(row: dict, rng: random.Random) -> str:
    logits = _outcome_logits(row)
    noisy = {label: value + rng.gauss(0, 0.85) for label, value in logits.items()}
    peak = max(noisy.values())
    weights = {label: math.exp(value - peak) for label, value in noisy.items()}
    total = sum(weights.values())
    draw = rng.random() * total
    for label in CLASS_LABELS:
        draw -= weights[label]
        if draw <= 0:
            return label
    return CLASS_LABELS[0]


def _shipment(
    index: int,
    product: dict,
    customer_ids: list[str],
    rng: random.Random,
    now: datetime,
) -> dict:
    station = rng.choice(list(STATIONS))
    return {
        "shipment_id": f"SHP-{index:05d}",
        "product_sku": product["product_sku"],
        "customer_id": rng.choice(customer_ids) if customer_ids else None,
        "destination_station": station,
        "carrier_tier": rng.choices(list(CARRIER_TIERS), weights=[0.35, 0.45, 0.20])[0],
        "transfer_distance_gm": round(STATIONS[station] * rng.uniform(0.85, 1.15), 2),
        "solar_storm_index": round(min(9.0, abs(rng.gauss(2.2, 1.9))), 2),
        "warehouse_queue_depth": max(0, int(rng.gauss(14, 9))),
        "priority_class": rng.choices(list(PRIORITY_CLASSES), weights=[0.6, 0.3, 0.1])[
            0
        ],
        "shipped_at": now - timedelta(hours=rng.randint(1, 24 * 400)),
        "status": "delivered",
    }


def generate_shipment_history(
    products: list[dict],
    customer_ids: list[str],
    delivered: int = DELIVERED_COUNT,
    in_transit: int = IN_TRANSIT_COUNT,
) -> tuple[list[dict], list[dict], list[dict]]:
    if not products:
        raise ValueError(
            "no products to build shipments from, run load_product_catalogue first"
        )

    rng = random.Random(SEED)
    now = datetime(2026, 8, 1, 12, 0, 0)
    by_sku = {p["product_sku"]: p for p in products}

    shipments, features, labels = [], [], []
    for index in range(delivered + in_transit):
        product = by_sku[rng.choice(list(by_sku))]
        shipment = _shipment(index + 1, product, customer_ids, rng, now)

        feature_row = {
            "shipment_id": shipment["shipment_id"],
            "weight_kg": float(product["weight_kg"] or 0.0),
            "volume_l": parse_volume_l(product["dimensions_cm"]),
            "base_price_credits": float(product["base_price_credits"] or 0.0),
            "category_id": int(product["category_id"] or 0),
            "is_hazardous": bool(product["is_hazardous"]),
            "requires_cold_chain": bool(product["requires_cold_chain"]),
            "transfer_distance_gm": shipment["transfer_distance_gm"],
            "solar_storm_index": shipment["solar_storm_index"],
            "warehouse_queue_depth": shipment["warehouse_queue_depth"],
            "destination_station": shipment["destination_station"],
            "carrier_tier": shipment["carrier_tier"],
            "priority_class": shipment["priority_class"],
        }

        if index < delivered:
            labels.append(
                {
                    "shipment_id": shipment["shipment_id"],
                    "delivery_outcome": _sample_outcome(feature_row, rng),
                }
            )
        else:
            shipment["status"] = "in_transit"
            shipment["shipped_at"] = now - timedelta(hours=rng.randint(1, 72))

        shipments.append(shipment)
        features.append(feature_row)

    return shipments, features, labels


def one_hot_encode(df, columns: list[str]):
    import polars as pl

    present = [column for column in columns if column in df.columns]
    if not present:
        return df
    df = df.with_columns([pl.col(column).cast(pl.Utf8) for column in present])
    return df.to_dummies(present)


def align_to_training_columns(df, feature_names: list[str]):
    import polars as pl

    missing = [name for name in feature_names if name not in df.columns]
    if missing:
        df = df.with_columns([pl.lit(0).cast(pl.Int8).alias(name) for name in missing])
    return df.select(feature_names)
