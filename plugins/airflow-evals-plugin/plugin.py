from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from contextlib import closing
from datetime import timezone
from pathlib import Path

import duckdb
from fastapi import Depends, FastAPI, HTTPException
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

app = FastAPI(title="CosMarket AI Evals", dependencies=[_require_view])
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
app.mount("/assets", StaticFiles(directory=BASE_DIR / "assets"), name="assets")


_LOCK_RETRIES = 6
_LOCK_BACKOFF = 0.25


def _connect():
    last = None
    for attempt in range(_LOCK_RETRIES):
        try:
            return duckdb.connect(DB_PATH, read_only=True)
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


def _records(table: str) -> list[dict]:
    if not os.path.exists(DB_PATH):
        return []
    with closing(_connect()) as conn:
        try:
            result = conn.execute(f"SELECT * FROM {table}")
            columns = [c[0] for c in result.description]
            return [dict(zip(columns, row)) for row in result.fetchall()]
        except duckdb.CatalogException:
            return []


def _coerce_json(value):
    if isinstance(value, (dict, list)) or value is None:
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return value
    return value


def _ts(value) -> str | None:
    if value is None:
        return None
    isoformat = getattr(value, "isoformat", None)
    if isoformat is None:
        return str(value)
    if getattr(value, "tzinfo", None) is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _is_dimension(value) -> bool:
    return isinstance(value, dict) and "score" in value


def load_evals() -> list[dict]:
    threads = {t.get("thread_id"): t for t in _records("support_threads")}
    customers = {c.get("customer_id"): c for c in _records("customers")}
    products = {p.get("product_sku"): p for p in _records("products")}
    messages = _records("support_messages")

    replies = {}
    for message in messages:
        if message.get("direction") == "outbound":
            replies[message.get("thread_id")] = message.get("body")

    out = []
    for record in _records("support_thread_evals"):
        thread = threads.get(record.get("thread_id"), {})
        customer = customers.get(thread.get("customer_id"), {})
        product = products.get(thread.get("product_sku"), {})
        scores = _coerce_json(record.get("scores")) or {}
        dimensions = {k: v for k, v in scores.items() if _is_dimension(v)}
        flags = {k: v for k, v in scores.items() if not _is_dimension(v)}
        out.append(
            {
                "eval_id": record.get("eval_id"),
                "thread_id": record.get("thread_id"),
                "ticket_id": thread.get("ticket_id"),
                "subject": thread.get("subject"),
                "customer_name": customer.get("full_name"),
                "customer_tier": customer.get("customer_tier"),
                "product_name": product.get("product_name"),
                "reply": record.get("scored_reply") or replies.get(record.get("thread_id")),
                "reply_is_current_thread_message": record.get("scored_reply") is None,
                "trace_id": record.get("trace_id"),
                "span_id": record.get("span_id"),
                "model": record.get("model"),
                "input_tokens": record.get("input_tokens"),
                "output_tokens": record.get("output_tokens"),
                "reasoning_tokens": record.get("reasoning_tokens"),
                "cost": record.get("cost"),
                "duration_ms": record.get("duration_ms"),
                "tool_calls": _coerce_json(record.get("tool_calls")) or [],
                "tools_available": _coerce_json(record.get("tools_available")) or [],
                "dimensions": dimensions,
                "flags": flags,
                "scored_at": _ts(record.get("scored_at")),
            }
        )

    out.sort(key=lambda r: (r.get("scored_at") or "", str(r.get("eval_id") or "")), reverse=True)

    latest, superseded = [], 0
    seen: set = set()
    for record in out:
        key = record.get("thread_id") or record.get("eval_id")
        if key in seen:
            superseded += 1
            continue
        seen.add(key)
        latest.append(record)
    if superseded:
        log.info("hiding %s superseded eval rows, showing the newest per thread", superseded)
    return latest


def load_summary() -> dict:
    evals = load_evals()
    total = len(evals)
    if not total:
        return {
            "scored": 0,
            "dimensions": [],
            "avg_reasoning_tokens": None,
            "avg_duration_ms": None,
            "avg_tool_calls": None,
        }

    names = []
    for record in evals:
        for name in record["dimensions"]:
            if name not in names:
                names.append(name)

    dimensions = []
    for name in names:
        present = [r["dimensions"][name] for r in evals if name in r["dimensions"]]
        dimensions.append(
            {
                "name": name,
                "counted": len(present),
                "good": sum(1 for d in present if d.get("score") == "good"),
                "acceptable": sum(1 for d in present if d.get("score") == "acceptable"),
                "poor": sum(1 for d in present if d.get("score") == "poor"),
                "low_confidence": sum(1 for d in present if d.get("confidence") == "low"),
            }
        )

    reasoned = [r["reasoning_tokens"] for r in evals if r["reasoning_tokens"] is not None]
    timed = [r["duration_ms"] for r in evals if r["duration_ms"] is not None]
    tools = [len(r["tool_calls"]) for r in evals]

    return {
        "scored": total,
        "dimensions": dimensions,
        "avg_reasoning_tokens": round(sum(reasoned) / len(reasoned)) if reasoned else None,
        "avg_duration_ms": round(sum(timed) / len(timed), 1) if timed else None,
        "avg_tool_calls": round(sum(tools) / total, 1) if total else None,
    }


@app.get("/ui", response_class=FileResponse)
async def serve_ui():
    return FileResponse(BASE_DIR / "static" / "index.html")


@app.get("/api/summary")
async def get_summary():
    return await asyncio.to_thread(load_summary)


@app.get("/api/evals")
async def get_evals():
    return await asyncio.to_thread(load_evals)


class EvalsPlugin(AirflowPlugin):
    name = "evals_plugin"

    fastapi_apps = [
        {
            "app": app,
            "url_prefix": "/evals",
            "name": "CosMarket AI Evals",
        }
    ]

    external_views = [
        {
            "name": "AI Evals",
            "href": "evals/ui",
            "destination": "nav",
            "url_route": "evals",
            "nav_top_level": True,
            "icon": _ICON_DATA_URI,
        }
    ]
