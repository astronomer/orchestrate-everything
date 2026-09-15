from __future__ import annotations

import json
import logging
import re

from include.cosmarket_db import upsert
from include.metrics_store import write_metrics
from include.support_systems import (
    GOLDEN_REPLIES,
    find_ticket_id,
    thread_id_for,
    ticket_by_id,
)

log = logging.getLogger(__name__)

STRUCTURE_CHECKS = ("has_content", "within_length", "no_placeholders", "signed_off")
PLACEHOLDER_PATTERN = re.compile(r"\[[A-Za-z_ ]+\]|\{\{.+?\}\}")
SIGN_OFF = "CosMarket Support"

SCORE_RUBRIC_SYSTEM_PROMPT = (
    "You score CosMarket support replies against a fixed rubric. "
    "Score each dimension independently, on the reply as written. "
    "Confidence applies to the single dimension it scores, not to "
    "the reply as a whole. Do not rewrite the reply."
)

COMPARE_WITH_REFERENCE_SYSTEM_PROMPT = (
    "You compare a generated support reply against a reference reply "
    "written by the support team. The reference is the source of "
    "truth: judge correctness against it, not against what sounds "
    "plausible. Wording differences are not differences in substance."
)


def _reply_text(run: dict) -> str | None:
    output = run.get("output")
    if isinstance(output, dict):
        return output.get("body")
    return output if isinstance(output, str) else None


DRAFTING_TASK_ID = "draft_reply"


def resolve_scored_runs(
    agent_runs: list[dict], task_id: str = DRAFTING_TASK_ID
) -> list[dict]:
    runs = []
    for call in agent_runs:
        if call.get("task_id") != task_id:
            continue
        ticket_id = find_ticket_id(call["prompt"])
        ticket = ticket_by_id(ticket_id) if ticket_id else None
        if ticket is None or ticket_id not in GOLDEN_REPLIES:
            continue
        runs.append(
            {
                "ticket_id": ticket_id,
                "thread_id": thread_id_for(ticket_id),
                "customer_ask": ticket["customer_ask"],
                "order_record": ticket["order_record"],
                "reference_reply": GOLDEN_REPLIES[ticket_id],
                "generated_reply": _reply_text(call),
                "start_unix_nano": call.get("start_unix_nano"),
                **{
                    key: call.get(key)
                    for key in ("dag_id", "task_id", "run_id", "map_index", "try_number")
                },
                **{
                    key: call[key]
                    for key in (
                        "trace_id",
                        "span_id",
                        "model",
                        "input_tokens",
                        "output_tokens",
                        "reasoning_tokens",
                        "cost",
                        "duration_ms",
                        "tool_calls",
                        "tools_available",
                    )
                },
            }
        )
    latest: dict[str, dict] = {}
    for run in runs:
        current = latest.get(run["ticket_id"])
        if current is None or (run.get("start_unix_nano") or 0) > (
            current.get("start_unix_nano") or 0
        ):
            latest[run["ticket_id"]] = run

    newest = sorted(latest.values(), key=lambda r: r["ticket_id"])
    log.info(
        "%s of %s agent runs are scoreable, %s after keeping the newest per ticket",
        len(runs),
        len(agent_runs),
        len(newest),
    )
    return newest


def _dimensions(rubric: dict) -> list[str]:
    return [
        name for name, value in rubric.items()
        if isinstance(value, dict) and "score" in value
    ]


def _scored_dimensions(record: dict) -> list[str]:
    return sorted(key[: -len("_score")] for key in record if key.endswith("_score"))


def check_reply_structure(reply: str | None) -> dict:
    text = str(reply or "")
    words = len(text.split())
    return {
        "has_content": bool(text.strip()),
        "within_length": 20 <= words <= 300,
        "no_placeholders": not PLACEHOLDER_PATTERN.search(text),
        "signed_off": SIGN_OFF in text,
        "word_count": words,
    }


def build_eval_record(
    run: dict, structure: dict, rubric: dict, reference: dict
) -> dict:
    record = {
        "ticket_id": run["ticket_id"],
        "thread_id": run["thread_id"],
        "scored_reply": run["generated_reply"],
        "structurally_valid": all(structure[check] for check in STRUCTURE_CHECKS),
        "word_count": structure["word_count"],
        "reference_equivalence": reference["equivalence"],
        "reference_confidence": reference["confidence"],
        "reference_missing_points": reference["missing_points"],
        "reference_reasoning": reference["reasoning"],
        **{
            key: run[key]
            for key in (
                "trace_id",
                "span_id",
                "model",
                "input_tokens",
                "output_tokens",
                "reasoning_tokens",
                "cost",
                "duration_ms",
                "tool_calls",
                "tools_available",
            )
        },
    }
    for judged in (rubric, reference):
        for dimension in _dimensions(judged):
            for field in ("score", "confidence", "reasoning"):
                record[f"{dimension}_{field}"] = judged[dimension][field]
    return record


def _rate(records: list[dict], predicate) -> float:
    return sum(1 for r in records if predicate(r)) / len(records)


def _mean(values: list) -> float:
    present = [v for v in values if v is not None]
    return sum(present) / len(present) if present else 0


def load_eval_results(records: list[dict]) -> None:
    if not records:
        log.warning("nothing to load, no model call was scoreable")
        return

    dimensions = _scored_dimensions(records[0])

    upsert(
        "support_thread_evals",
        "eval_id",
        [
            {
                "eval_id": f"EVAL-{r['ticket_id']}-{r['span_id']}",
                "thread_id": r["thread_id"],
                "turn": 2,
                "trace_id": r["trace_id"],
                "span_id": r["span_id"],
                "model": r["model"],
                "input_tokens": r["input_tokens"],
                "output_tokens": r["output_tokens"],
                "reasoning_tokens": r["reasoning_tokens"],
                "cost": r["cost"],
                "duration_ms": r["duration_ms"],
                "tool_calls": json.dumps(r["tool_calls"]),
                "tools_available": json.dumps(r["tools_available"]),
                "scored_reply": r["scored_reply"],
                "scores": json.dumps(
                    {
                        "structurally_valid": r["structurally_valid"],
                        "reference_equivalence": r["reference_equivalence"],
                        **{
                            d: {
                                field: r[f"{d}_{field}"]
                                for field in ("score", "confidence", "reasoning")
                            }
                            for d in dimensions
                        },
                    }
                ),
            }
            for r in records
        ],
    )

    write_metrics(
        replies_scored=len(records),
        structurally_valid_rate=_rate(records, lambda r: r["structurally_valid"]),
        equivalent_rate=_rate(records, lambda r: r["reference_equivalence"] == "equivalent"),
        different_rate=_rate(records, lambda r: r["reference_equivalence"] == "different"),
        avg_reasoning_tokens=_mean([r["reasoning_tokens"] for r in records]),
        avg_duration_ms=_mean([r["duration_ms"] for r in records]),
        avg_tool_calls=_mean([len(r["tool_calls"]) for r in records]),
        **{
            f"{d}_good_rate": _rate(records, lambda r, d=d: r[f"{d}_score"] == "good")
            for d in dimensions
        },
        **{
            f"{d}_low_confidence_rate": _rate(
                records, lambda r, d=d: r[f"{d}_confidence"] == "low"
            )
            for d in dimensions
        },
    )
