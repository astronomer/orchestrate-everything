from airflow.sdk import chain, dag, task

from include.dag_defaults import DB_TASK_ARGS, DEFAULT_ARGS
from include.mlops.assets import MODEL_REGISTERED, SHIPMENTS_SCORED

_MODEL_STAGE = "production"
_AT_RISK_CLASSES = ("delayed", "damaged", "lost_in_transit")


@dag(
    schedule=[MODEL_REGISTERED],
    tags=["MLOps"],
    max_active_tasks=1,
    default_args=DEFAULT_ARGS,
)
def score_open_shipments():

    @task(**DB_TASK_ARGS)
    def collect_open_shipments() -> list[dict]:
        from include.cosmarket_db import get_conn
        from include.mlops.shipment_features import FEATURE_COLUMNS

        selected = ", ".join(f"f.{column}" for column in FEATURE_COLUMNS)
        with get_conn(read_only=True) as conn:
            result = conn.execute(
                f"SELECT {selected} FROM shipment_features f"
                " JOIN shipments s ON f.shipment_id = s.shipment_id"
                " WHERE s.status = 'in_transit'"
                " ORDER BY s.shipped_at"
            )
            columns = [c[0] for c in result.description]
            return [dict(zip(columns, row)) for row in result.fetchall()]

    @task(**DB_TASK_ARGS)
    def score(open_shipments: list[dict]) -> list[dict]:
        import json

        import numpy as np
        import polars as pl

        from include.mlops.shipment_features import (
            CATEGORICAL_COLUMNS,
            align_to_training_columns,
            one_hot_encode,
        )
        from include.mlops_tracking import MlopsTracker

        if not open_shipments:
            raise ValueError(
                "no in-transit shipments to score, run setup or reset_demo first"
            )

        tracker = MlopsTracker()
        registered = tracker.load_model(stage=_MODEL_STAGE)
        model = registered["model"]
        feature_names = tracker.run_params(registered["run_id"])["features"]

        df = pl.DataFrame(open_shipments)
        shipment_ids = df["shipment_id"].to_list()
        encoded = align_to_training_columns(
            one_hot_encode(df.drop("shipment_id"), CATEGORICAL_COLUMNS), feature_names
        )

        X = np.array(encoded.to_numpy().tolist(), dtype=float)
        predictions = model.predict(X)
        probabilities = model.predict_proba(X)
        classes = list(model.classes_)

        return [
            {
                "prediction_id": (
                    f"PRD-{shipment_id}-{registered['model_name']}"
                    f"-v{registered['model_version']}"
                ),
                "shipment_id": shipment_id,
                "model_name": registered["model_name"],
                "model_version": registered["model_version"],
                "predicted_outcome": str(predicted),
                "confidence": float(max(row)),
                "class_probabilities": json.dumps(
                    {label: round(float(p), 4) for label, p in zip(classes, row)}
                ),
            }
            for shipment_id, predicted, row in zip(
                shipment_ids, predictions, probabilities
            )
        ]

    @task(outlets=[SHIPMENTS_SCORED], **DB_TASK_ARGS)
    def load_predictions(predictions: list[dict]) -> int:
        from include.cosmarket_db import upsert

        return upsert("shipment_predictions", "prediction_id", predictions)

    @task
    def report_at_risk(predictions: list[dict]) -> dict:
        import logging
        from collections import Counter

        log = logging.getLogger(__name__)

        breakdown = Counter(p["predicted_outcome"] for p in predictions)
        at_risk = sorted(
            (p for p in predictions if p["predicted_outcome"] in _AT_RISK_CLASSES),
            key=lambda p: p["confidence"],
            reverse=True,
        )

        log.info(
            "scored %s in-transit shipments: %s", len(predictions), dict(breakdown)
        )
        for prediction in at_risk[:10]:
            log.info(
                "at risk: %s -> %s (%.0f%% confidence)",
                prediction["shipment_id"],
                prediction["predicted_outcome"],
                prediction["confidence"] * 100,
            )

        return {
            "scored": len(predictions),
            "at_risk": len(at_risk),
            "breakdown": dict(breakdown),
            "top_at_risk": [p["shipment_id"] for p in at_risk[:10]],
        }

    _open = collect_open_shipments()
    _scored = score(_open)
    _loaded = load_predictions(_scored)
    _report = report_at_risk(_scored)
    chain(_open, _scored, _loaded, _report)


score_open_shipments()
