from __future__ import annotations

import base64
import json
import logging
import pickle
from typing import Any

from include.cosmarket_db import get_conn

log = logging.getLogger(__name__)

STAGES = ("none", "staging", "production", "archived")


def _next_id(conn, sequence: str) -> int:
    return conn.execute(f"SELECT nextval('{sequence}')").fetchone()[0]


class MlopsTracker:

    def experiment_id(self, experiment_name: str, description: str | None = None) -> int:
        with get_conn() as conn:
            row = conn.execute(
                "SELECT experiment_id FROM ml_experiments WHERE experiment_name = ?",
                (experiment_name,),
            ).fetchone()
            if row:
                return row[0]

            experiment_id = _next_id(conn, "seq_ml_experiment_id")
            conn.execute(
                "INSERT INTO ml_experiments (experiment_id, experiment_name, description)"
                " VALUES (?, ?, ?)",
                (experiment_id, experiment_name, description or experiment_name),
            )
            return experiment_id

    def start_run(
        self,
        experiment_name: str,
        dag_id: str,
        task_id: str,
        tags: dict | None = None,
        description: str | None = None,
    ) -> int:
        experiment_id = self.experiment_id(experiment_name, description)
        with get_conn() as conn:
            run_id = _next_id(conn, "seq_ml_run_id")
            run_number = (
                conn.execute(
                    "SELECT count(*) + 1 FROM ml_runs WHERE experiment_id = ?",
                    (experiment_id,),
                ).fetchone()
            )[0]
            conn.execute(
                "INSERT INTO ml_runs"
                " (run_id, experiment_id, dag_id, task_id, status, hyperparameters,"
                "  metrics, tags, run_number)"
                " VALUES (?, ?, ?, ?, 'running', '{}', '{}', ?, ?)",
                (run_id, experiment_id, dag_id, task_id, json.dumps(tags or {}, default=str), run_number),
            )
        log.info("started run %s (#%s) in experiment %s", run_id, run_number, experiment_name)
        return run_id

    def log_params(self, run_id: int, params: dict) -> None:
        with get_conn() as conn:
            conn.execute(
                "UPDATE ml_runs SET hyperparameters = ? WHERE run_id = ?",
                (json.dumps(params, default=str), run_id),
            )

    def log_metrics(self, run_id: int, metrics: dict) -> None:
        with get_conn() as conn:
            conn.execute(
                "UPDATE ml_runs SET metrics = ? WHERE run_id = ?",
                (json.dumps(metrics, default=str), run_id),
            )

    def end_run(self, run_id: int, status: str = "finished") -> None:
        with get_conn() as conn:
            conn.execute("UPDATE ml_runs SET status = ? WHERE run_id = ?", (status, run_id))

    def log_model(self, run_id: int, model_name: str, model_type: str, model: Any) -> int:
        blob = base64.b64encode(pickle.dumps(model)).decode("ascii")
        with get_conn() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                model_version = (
                    conn.execute(
                        "SELECT coalesce(max(model_version), 0) + 1 FROM ml_models WHERE model_name = ?",
                        (model_name,),
                    ).fetchone()
                )[0]
                conn.execute(
                    "INSERT INTO ml_models"
                    " (model_name, model_version, run_id, model_type, stage, model_blob)"
                    " VALUES (?, ?, ?, ?, 'none', ?)",
                    (model_name, model_version, run_id, model_type, blob),
                )
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        log.info("logged %s v%s (%s)", model_name, model_version, model_type)
        return model_version

    def log_plot(self, run_id: int, plot_name: str, plot_type: str, plot_data: str) -> int:
        with get_conn() as conn:
            plot_id = _next_id(conn, "seq_ml_plot_id")
            conn.execute(
                "INSERT INTO ml_plots (plot_id, run_id, plot_name, plot_type, plot_data)"
                " VALUES (?, ?, ?, ?, ?)",
                (plot_id, run_id, plot_name, plot_type, plot_data),
            )
        return plot_id

    def promote_model(self, model_name: str, model_version: int, stage: str = "production") -> None:
        if stage not in STAGES:
            raise ValueError(f"stage must be one of {STAGES}, got {stage!r}")
        with get_conn() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                experiment_id = conn.execute(
                    "SELECT r.experiment_id FROM ml_models m"
                    " JOIN ml_runs r ON m.run_id = r.run_id"
                    " WHERE m.model_name = ? AND m.model_version = ?",
                    (model_name, model_version),
                ).fetchone()
                if experiment_id is None:
                    raise ValueError(
                        f"no model {model_name!r} at version {model_version} to promote"
                    )

                demoted = conn.execute(
                    "UPDATE ml_models SET stage = 'archived', staged_at = current_timestamp"
                    " WHERE stage = ?"
                    "   AND NOT (model_name = ? AND model_version = ?)"
                    "   AND run_id IN (SELECT run_id FROM ml_runs WHERE experiment_id = ?)"
                    " RETURNING model_name, model_version",
                    (stage, model_name, model_version, experiment_id[0]),
                ).fetchall()

                conn.execute(
                    "UPDATE ml_models SET stage = ?, staged_at = current_timestamp"
                    " WHERE model_name = ? AND model_version = ?",
                    (stage, model_name, model_version),
                )
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        if demoted:
            log.info("archived %s previously in %s: %s", len(demoted), stage, demoted)
        log.info("promoted %s v%s to %s", model_name, model_version, stage)

    def load_model(self, model_name: str | None = None, stage: str = "production") -> dict:
        with get_conn(read_only=True) as conn:
            if model_name:
                row = conn.execute(
                    "SELECT model_name, model_version, model_type, run_id, model_blob"
                    " FROM ml_models WHERE model_name = ? AND stage = ?"
                    " ORDER BY model_version DESC LIMIT 1",
                    (model_name, stage),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT model_name, model_version, model_type, run_id, model_blob"
                    " FROM ml_models WHERE stage = ?"
                    " ORDER BY staged_at DESC NULLS LAST, run_id DESC, model_version DESC"
                    " LIMIT 1",
                    (stage,),
                ).fetchone()

        if not row:
            raise ValueError(
                f"no model in stage {stage!r}"
                + (f" for {model_name!r}" if model_name else "")
                + " -- run train_delivery_risk_model first"
            )

        return {
            "model_name": row[0],
            "model_version": row[1],
            "model_type": row[2],
            "run_id": row[3],
            "model": pickle.loads(base64.b64decode(row[4])),
        }

    def run_params(self, run_id: int) -> dict:
        with get_conn(read_only=True) as conn:
            row = conn.execute(
                "SELECT hyperparameters FROM ml_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        return json.loads(row[0]) if row and row[0] else {}
