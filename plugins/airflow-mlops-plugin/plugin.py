from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import threading
import time
from contextlib import closing
from datetime import timezone
from pathlib import Path

import duckdb
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from airflow.api_fastapi.core_api.security import requires_authenticated
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
DB_PATH = os.environ.get("COSMARKET_DB_PATH") or os.path.join(
    os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"),
    "include",
    "cosmarket.duckdb",
)

_ICON_DATA_URI = "data:image/svg+xml;base64," + base64.b64encode(
    (BASE_DIR / "assets" / "icon.svg").read_bytes()
).decode("ascii")

_require_view = Depends(requires_authenticated())

app = FastAPI(title="MLOps Plugin", dependencies=[_require_view])
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
app.mount("/assets", StaticFiles(directory=BASE_DIR / "assets"), name="assets")


_DB_LOCK = threading.Lock()

_LOCK_RETRIES = 6
_LOCK_BACKOFF = 0.25


def _connect(read_only: bool):
    last: Exception | None = None
    for attempt in range(_LOCK_RETRIES):
        try:
            return duckdb.connect(DB_PATH, read_only=read_only)
        except (duckdb.IOException, duckdb.ConnectionException) as exc:
            last = exc
            time.sleep(_LOCK_BACKOFF * (attempt + 1))
    log.warning("gave up waiting for the DuckDB file lock: %s", last)
    raise HTTPException(
        status_code=503,
        detail=(
            "The CosMarket database is being written by a running task. "
            "DuckDB allows one writer at a time on a local file, so this view "
            "is unavailable for a moment. Refresh shortly."
        ),
    )


def _db_records(sql: str, params: tuple = ()) -> list[tuple]:
    with _DB_LOCK, closing(_connect(read_only=True)) as conn:
        return conn.execute(sql, params).fetchall()


def _db_first(sql: str, params: tuple = ()) -> tuple | None:
    rows = _db_records(sql, params)
    return rows[0] if rows else None


def _db_run(sql: str, params: tuple = ()) -> int:
    with _DB_LOCK, closing(_connect(read_only=False)) as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            changed = len(conn.execute(sql, params).fetchall())
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
        return changed


def _iso(value) -> str | None:
    if value is None:
        return None
    isoformat = getattr(value, "isoformat", None)
    if isoformat is None:
        return str(value)
    if getattr(value, "tzinfo", None) is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _safe_json(val) -> dict:
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


@app.get("/ui", response_class=FileResponse)
async def serve_ui():
    return FileResponse(BASE_DIR / "static" / "index.html")


@app.get("/api/summary")
async def get_summary():
    def _fetch():
        exp_count = (_db_first("SELECT count(*) FROM ml_experiments") or (0,))[0]
        run_count = (_db_first("SELECT count(*) FROM ml_runs") or (0,))[0]
        model_count = (_db_first("SELECT count(*) FROM ml_models") or (0,))[0]
        plot_count = (_db_first("SELECT count(*) FROM ml_plots") or (0,))[0]
        return {
            "experiments": exp_count,
            "runs": run_count,
            "models": model_count,
            "plots": plot_count,
        }

    return await asyncio.to_thread(_fetch)


@app.get("/api/experiments")
async def list_experiments():
    def _fetch():
        rows = _db_records(
            "SELECT experiment_id, experiment_name, description"
            " FROM ml_experiments ORDER BY experiment_id"
        )
        return [
            {"experiment_id": r[0], "experiment_name": r[1],
             "description": r[2], "source": "db"}
            for r in rows
        ]

    return await asyncio.to_thread(_fetch)


@app.get("/api/runs")
async def list_runs(experiment: str | None = Query(default=None)):
    def _fetch():
        cols = ("SELECT r.run_id, e.experiment_name, r.dag_id, r.task_id,"
                " r.status, r.hyperparameters, r.metrics, r.tags, r.run_ts,"
                " r.run_number")
        if experiment:
            rows = _db_records(
                cols + " FROM ml_runs r"
                " JOIN ml_experiments e ON r.experiment_id = e.experiment_id"
                " WHERE e.experiment_name = ?"
                " ORDER BY r.run_ts, r.run_id",
                (experiment,),
            )
        else:
            rows = _db_records(
                cols + " FROM ml_runs r"
                " JOIN ml_experiments e ON r.experiment_id = e.experiment_id"
                " ORDER BY r.run_ts DESC, r.run_id DESC"
            )
        return [
            {
                "run_id": r[0], "experiment_name": r[1], "dag_id": r[2],
                "task_id": r[3], "status": r[4],
                "params": _safe_json(r[5]), "metrics": _safe_json(r[6]),
                "tags": _safe_json(r[7]),
                "run_ts": _iso(r[8]),
                "run_number": r[9],
                "source": "db",
            }
            for r in rows
        ]

    return await asyncio.to_thread(_fetch)


@app.get("/api/runs/{run_id}")
async def get_run(run_id: int):
    def _fetch():
        row = _db_first(
            "SELECT r.run_id, e.experiment_name, r.dag_id, r.task_id,"
            " r.status, r.hyperparameters, r.metrics, r.tags, r.run_number"
            " FROM ml_runs r"
            " JOIN ml_experiments e ON r.experiment_id = e.experiment_id"
            " WHERE r.run_id = ?",
            (run_id,),
        )
        if row:
            return {
                "run_id": row[0], "experiment_name": row[1], "dag_id": row[2],
                "task_id": row[3], "status": row[4],
                "params": _safe_json(row[5]), "metrics": _safe_json(row[6]),
                "tags": _safe_json(row[7]), "run_number": row[8],
                "source": "db",
            }
        return None

    result = await asyncio.to_thread(_fetch)
    if not result:
        raise HTTPException(status_code=404, detail="Run not found")
    return result


@app.get("/api/runs/{run_id}/plots")
async def get_plots(run_id: int):
    def _fetch():
        rows = _db_records(
            "SELECT plot_id, plot_name, plot_type, plot_data"
            " FROM ml_plots WHERE run_id = ?",
            (run_id,),
        )
        return [
            {"plot_id": r[0], "plot_name": r[1], "plot_type": r[2],
             "plot_data": r[3], "source": "db"}
            for r in rows
        ]

    return await asyncio.to_thread(_fetch)


@app.get("/api/plots")
async def list_all_plots():
    def _fetch():
        rows = _db_records(
            "SELECT p.plot_id, p.run_id, p.plot_name, p.plot_type, p.plot_data,"
            " e.experiment_name"
            " FROM ml_plots p"
            " JOIN ml_runs r ON p.run_id = r.run_id"
            " JOIN ml_experiments e ON r.experiment_id = e.experiment_id"
            " ORDER BY p.plot_id DESC"
        )
        return [
            {"plot_id": r[0], "run_id": r[1], "plot_name": r[2],
             "plot_type": r[3], "plot_data": r[4],
             "experiment_name": r[5], "source": "db"}
            for r in rows
        ]

    return await asyncio.to_thread(_fetch)


@app.get("/api/models")
async def list_models():
    def _fetch():
        rows = _db_records(
            "SELECT model_name, model_version, run_id, model_type, stage, staged_at"
            " FROM ml_models ORDER BY model_name, model_version DESC"
        )
        return [
            {"model_name": r[0], "model_version": r[1], "run_id": r[2],
             "model_type": r[3], "stage": r[4], "staged_at": _iso(r[5]),
             "source": "db"}
            for r in rows
        ]

    return await asyncio.to_thread(_fetch)


STAGES = ("none", "staging", "production", "archived")


@app.patch("/api/models/{model_name}/{model_version}/stage")
async def update_model_stage(model_name: str, model_version: int, stage: str = Query(...)):
    if stage not in STAGES:
        raise HTTPException(
            status_code=422,
            detail=f"stage must be one of {list(STAGES)}, got {stage!r}",
        )

    def _update():
        with _DB_LOCK, closing(_connect(read_only=False)) as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                experiment = conn.execute(
                    "SELECT r.experiment_id FROM ml_models m"
                    " JOIN ml_runs r ON m.run_id = r.run_id"
                    " WHERE m.model_name = ? AND m.model_version = ?",
                    (model_name, model_version),
                ).fetchone()
                if experiment is None:
                    conn.execute("ROLLBACK")
                    return 0
                if stage in ("production", "staging"):
                    conn.execute(
                        "UPDATE ml_models SET stage = 'archived',"
                        " staged_at = current_timestamp"
                        " WHERE stage = ?"
                        "   AND NOT (model_name = ? AND model_version = ?)"
                        "   AND run_id IN"
                        "       (SELECT run_id FROM ml_runs WHERE experiment_id = ?)",
                        (stage, model_name, model_version, experiment[0]),
                    )
                changed = len(
                    conn.execute(
                        "UPDATE ml_models SET stage = ?, staged_at = current_timestamp"
                        " WHERE model_name = ? AND model_version = ? RETURNING 1",
                        (stage, model_name, model_version),
                    ).fetchall()
                )
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
            return changed

    changed = await asyncio.to_thread(_update)
    if not changed:
        raise HTTPException(
            status_code=404,
            detail=f"no model {model_name!r} at version {model_version}",
        )
    return {"model_name": model_name, "model_version": model_version, "stage": stage}


@app.get("/api/models/{model_name}/{model_version}/artifact")
async def get_model_artifact(model_name: str, model_version: int):
    def _fetch():
        row = _db_first(
            "SELECT model_blob FROM ml_models WHERE model_name = ? AND model_version = ?",
            (model_name, model_version),
        )
        if row:
            return {"model_b64": row[0], "model_name": model_name, "source": "db"}
        return None

    result = await asyncio.to_thread(_fetch)
    if not result:
        raise HTTPException(status_code=404, detail="Model artifact not found")
    return result


class MlopsPlugin(AirflowPlugin):
    name = "mlops_plugin"

    fastapi_apps = [
        {
            "app": app,
            "url_prefix": "/mlops",
            "name": "MLOps Plugin",
        }
    ]

    external_views = [
        {
            "name": "MLOps",
            "href": "mlops/ui",
            "destination": "nav",
            "url_route": "mlops",
            "nav_top_level": True,
            "icon": _ICON_DATA_URI,
        }
    ]
