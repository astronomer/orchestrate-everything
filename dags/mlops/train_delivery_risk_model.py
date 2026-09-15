from airflow.sdk import chain, dag, task

from include.dag_defaults import DB_TASK_ARGS, DEFAULT_ARGS, TRAINING_TASK_ARGS
from include.mlops.assets import MODEL_REGISTERED, SHIPMENT_HISTORY

_EXPERIMENT = "delivery_outcome_classification"
_MODEL_NAME = "delivery_risk_model"

_N_ESTIMATORS = [200]
_MAX_DEPTH = [8, 14]

_LR_C = [0.3, 1.0]
_LR_PENALTY = ["l2"]


@dag(
    schedule=[SHIPMENT_HISTORY],
    tags=["MLOps"],
    max_active_tasks=1,
    default_args=DEFAULT_ARGS,
)
def train_delivery_risk_model():

    @task(**DB_TASK_ARGS)
    def assemble_training_set() -> dict:
        import polars as pl

        from include.cosmarket_db import records
        from include.mlops.shipment_features import CATEGORICAL_COLUMNS, one_hot_encode

        df = pl.DataFrame(records("shipment_features")).join(
            pl.DataFrame(records("shipment_labels")), on="shipment_id", how="inner"
        )
        if df.is_empty():
            raise ValueError("no labelled shipments, run setup or reset_demo first")

        df = one_hot_encode(df, CATEGORICAL_COLUMNS)

        y = df["delivery_outcome"].to_list()
        X = df.drop("shipment_id", "delivery_outcome")
        return {
            "feature_names": X.columns,
            "X": X.to_numpy().tolist(),
            "y": y,
            "class_labels": sorted(set(y)),
        }

    @task(
        max_active_tis_per_dagrun=1,
        map_index_template="{{ custom_map_index }}",
        **TRAINING_TASK_ARGS,
    )
    def train_random_forest(
        n_estimators: int, max_depth: int, training_set: dict
    ) -> dict:
        from sklearn.ensemble import RandomForestClassifier

        from include.mlops.training import train_and_track

        return train_and_track(
            RandomForestClassifier(
                n_estimators=n_estimators,
                max_depth=max_depth,
                class_weight="balanced",
                random_state=42,
            ),
            training_set=training_set,
            model_name=f"{_MODEL_NAME}_random_forest_classifier",
            model_type="RandomForestClassifier",
            experiment=_EXPERIMENT,
            dag_id="train_delivery_risk_model",
            task_id="train_random_forest",
            extra_params={"n_estimators": n_estimators, "max_depth": max_depth},
            tags={"n_estimators": n_estimators, "max_depth": max_depth},
            importances_of=lambda model, _: model.feature_importances_.tolist(),
            importance_label="Importance",
            map_index=f"Trees: {n_estimators} | Depth: {max_depth}",
        )

    @task(
        max_active_tis_per_dagrun=1,
        map_index_template="{{ custom_map_index }}",
        **TRAINING_TASK_ARGS,
    )
    def train_logistic_regression(C: float, penalty: str, training_set: dict) -> dict:
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler

        from include.mlops.training import train_and_track

        def importances_of(model, _feature_names):
            import numpy as np

            return np.abs(model.named_steps["logreg"].coef_).max(axis=0).tolist()

        return train_and_track(
            Pipeline(
                [
                    ("scaler", StandardScaler()),
                    (
                        "logreg",
                        LogisticRegression(
                            C=C,
                            penalty=penalty,
                            class_weight="balanced",
                            max_iter=2000,
                            random_state=42,
                        ),
                    ),
                ]
            ),
            training_set=training_set,
            model_name=f"{_MODEL_NAME}_logistic_regression",
            model_type="LogisticRegression",
            experiment=_EXPERIMENT,
            dag_id="train_delivery_risk_model",
            task_id="train_logistic_regression",
            extra_params={"C": C, "penalty": penalty, "max_iter": 2000},
            tags={"C": C, "penalty": penalty},
            importances_of=importances_of,
            importance_label="Max absolute coefficient",
            map_index=f"C: {C} | Penalty: {penalty}",
        )

    @task
    def select_best(forest_variants: list[dict], linear_variants: list[dict]) -> dict:
        return max([*forest_variants, *linear_variants], key=lambda v: v["macro_f1"])

    @task(**DB_TASK_ARGS)
    def visualize(best: dict) -> dict:
        from include.ml_plots import plot_classification
        from include.mlops.training import describe_model
        from include.mlops_tracking import MlopsTracker

        title = (
            f"Delivery Outcome | {describe_model(best)} | "
            f"Macro F1 = {best['metrics']['macro_f1']:.3f}, "
            f"Accuracy = {best['metrics']['accuracy']:.3f}"
        )
        return plot_classification({**best, "plot_title": title}, MlopsTracker())

    @task(outlets=[MODEL_REGISTERED], **DB_TASK_ARGS)
    def register_model(best: dict) -> None:
        from include.mlops_tracking import MlopsTracker

        MlopsTracker().promote_model(
            best["model_name"], best["model_version"], stage="production"
        )

    _set = assemble_training_set()
    _forest = train_random_forest.partial(training_set=_set).expand(
        n_estimators=_N_ESTIMATORS, max_depth=_MAX_DEPTH
    )
    _linear = train_logistic_regression.partial(training_set=_set).expand(
        C=_LR_C, penalty=_LR_PENALTY
    )
    _best = select_best(_forest, _linear)
    chain(_set, [_forest, _linear], _best, [visualize(_best), register_model(_best)])


train_delivery_risk_model()
