from __future__ import annotations


def evaluate_classification(y_true: list, y_pred: list, labels: list[str]) -> dict:
    from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

    per_class = f1_score(y_true, y_pred, labels=labels, average=None, zero_division=0)
    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "macro_f1": float(f1_score(y_true, y_pred, labels=labels, average="macro", zero_division=0)),
        "weighted_f1": float(
            f1_score(y_true, y_pred, labels=labels, average="weighted", zero_division=0)
        ),
        "macro_precision": float(
            precision_score(y_true, y_pred, labels=labels, average="macro", zero_division=0)
        ),
        "macro_recall": float(
            recall_score(y_true, y_pred, labels=labels, average="macro", zero_division=0)
        ),
        "per_class_f1": {label: float(score) for label, score in zip(labels, per_class)},
    }


def describe_model(variant: dict) -> str:
    if variant["model_type"] == "RandomForestClassifier":
        return f"Random Forest ({variant['n_estimators']} trees, depth {variant['max_depth']})"
    return f"Logistic Regression (C={variant['C']}, {variant['penalty']})"


def train_and_track(
    estimator,
    *,
    training_set: dict,
    model_name: str,
    model_type: str,
    experiment: str,
    dag_id: str,
    task_id: str,
    extra_params: dict,
    tags: dict,
    importances_of,
    importance_label: str,
    map_index: str,
    test_size: float = 0.2,
    random_state: int = 42,
) -> dict:
    import numpy as np
    from sklearn.model_selection import train_test_split

    from include.mlops_tracking import MlopsTracker

    X = np.array(training_set["X"], dtype=float)
    y = np.array(training_set["y"])
    labels = training_set["class_labels"]
    feature_names = training_set["feature_names"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )
    estimator.fit(X_train, y_train)
    preds = estimator.predict(X_test)

    params = {
        "model_type": model_type,
        "class_weight": "balanced",
        "features": feature_names,
        "class_labels": labels,
        "test_size": test_size,
        "random_state": random_state,
        **extra_params,
    }
    metrics = {
        **evaluate_classification(y_test.tolist(), preds.tolist(), labels),
        "train_size": len(X_train),
        "test_size": len(X_test),
    }

    tracker = MlopsTracker()
    run_id = tracker.start_run(
        experiment_name=experiment, dag_id=dag_id, task_id=task_id, tags=tags
    )
    tracker.log_params(run_id, params)
    tracker.log_metrics(run_id, metrics)
    model_version = tracker.log_model(run_id, model_name, model_type, estimator)
    tracker.end_run(run_id)

    from airflow.sdk import get_current_context

    get_current_context()["custom_map_index"] = (
        f"{map_index} | Macro F1: {metrics['macro_f1']:.4f} | "
        f"Acc: {metrics['accuracy']:.4f}"
    )

    return {
        "run_id": run_id,
        "model_name": model_name,
        "model_version": model_version,
        "model_type": model_type,
        "macro_f1": metrics["macro_f1"],
        "accuracy": metrics["accuracy"],
        "metrics": metrics,
        "params": params,
        "feature_cols": feature_names,
        "class_labels": labels,
        "importances": importances_of(estimator, feature_names),
        "importance_label": importance_label,
        "y_test": y_test.tolist(),
        "y_pred": preds.tolist(),
        **extra_params,
    }
