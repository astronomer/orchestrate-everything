from __future__ import annotations

import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)

SPANS_FILE = Path("/traces/spans.jsonl")

_INPUT_TOKEN_KEYS = (
    "gen_ai.usage.input_tokens",
    "gen_ai.aggregated_usage.input_tokens",
    "gen_ai.usage.prompt_tokens",
)
_OUTPUT_TOKEN_KEYS = (
    "gen_ai.usage.output_tokens",
    "gen_ai.aggregated_usage.output_tokens",
    "gen_ai.usage.completion_tokens",
)
_REASONING_TOKEN_KEYS = (
    "gen_ai.usage.details.reasoning_tokens",
    "gen_ai.aggregated_usage.details.reasoning_tokens",
)
_COST_KEYS = ("operation.cost",)
_AGENT_MODEL_KEYS = ("model_name", "gen_ai.request.model")
_FINAL_RESULT_KEYS = ("final_result",)
_MESSAGES_KEYS = ("pydantic_ai.all_messages",)
_TOOL_DEFINITIONS_KEYS = ("gen_ai.tool.definitions",)


def _attr_value(value: dict):
    if "stringValue" in value or "string_value" in value:
        return value.get("stringValue", value.get("string_value"))
    if "intValue" in value or "int_value" in value:
        return int(value.get("intValue", value.get("int_value")))
    if "doubleValue" in value or "double_value" in value:
        return float(value.get("doubleValue", value.get("double_value")))
    if "boolValue" in value or "bool_value" in value:
        return value.get("boolValue", value.get("bool_value"))
    array = value.get("arrayValue", value.get("array_value"))
    if array is not None:
        return [_attr_value(v) for v in array.get("values", [])]
    kvlist = value.get("kvlistValue", value.get("kvlist_value"))
    if kvlist is not None:
        return _attributes(kvlist.get("values", []))
    return None


def _attributes(raw: list[dict]) -> dict:
    return {a["key"]: _attr_value(a.get("value") or {}) for a in raw or []}


def _get(obj: dict, *names, default=None):
    for name in names:
        if name in obj:
            return obj[name]
    return default


def _first(attributes: dict, keys: tuple[str, ...]):
    for key in keys:
        if attributes.get(key) is not None:
            return attributes[key]
    return None


def read_spans(path: Path = SPANS_FILE) -> list[dict]:
    if not path.exists():
        log.warning(
            "no spans exported to %s yet, check that the otel-collector container "
            "is running and that AIRFLOW__TRACES__OTEL_ON is set",
            path,
        )
        return []

    spans: list[dict] = []
    with path.open() as handle:
        for number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                log.warning("line %s of %s is not valid JSON, skipping", number, path)
                continue

            for resource_spans in _get(payload, "resourceSpans", "resource_spans", default=[]):
                resource = _get(resource_spans, "resource", default={})
                resource_attributes = _attributes(_get(resource, "attributes", default=[]))
                for scope_spans in _get(resource_spans, "scopeSpans", "scope_spans", default=[]):
                    for span in _get(scope_spans, "spans", default=[]):
                        start = int(_get(span, "startTimeUnixNano", "start_time_unix_nano", default=0) or 0)
                        end = int(_get(span, "endTimeUnixNano", "end_time_unix_nano", default=0) or 0)
                        spans.append(
                            {
                                "trace_id": _get(span, "traceId", "trace_id"),
                                "span_id": _get(span, "spanId", "span_id"),
                                "parent_span_id": _get(span, "parentSpanId", "parent_span_id"),
                                "name": span.get("name"),
                                "start_unix_nano": start,
                                "duration_ms": round((end - start) / 1_000_000, 1) if end and start else None,
                                "service_name": resource_attributes.get("service.name"),
                                "attributes": _attributes(_get(span, "attributes", default=[])),
                            }
                        )

    log.info("read %s spans from %s", len(spans), path)
    return spans


def _is_genai(span: dict) -> bool:
    return any(key.startswith("gen_ai.") for key in span["attributes"])


def _is_model_call(span: dict) -> bool:
    operation = span["attributes"].get("gen_ai.operation.name")
    if operation in {"chat", "generate_content", "text_completion"}:
        return True
    return operation is None and (span["name"] or "").startswith("chat")


def _is_agent_run(span: dict) -> bool:
    operation = span["attributes"].get("gen_ai.operation.name")
    if operation == "invoke_agent":
        return True
    return operation is None and (span["name"] or "").startswith("invoke_agent")


def _is_tool_call(span: dict) -> bool:
    operation = span["attributes"].get("gen_ai.operation.name")
    if operation == "execute_tool":
        return True
    return operation is None and (span["name"] or "").startswith("execute_tool")


def _tool_names(attributes: dict) -> list[str]:
    raw = _first(attributes, _TOOL_DEFINITIONS_KEYS)
    if not raw:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    if not isinstance(raw, list):
        return []
    return [t.get("name") for t in raw if isinstance(t, dict) and t.get("name")]


def _airflow_context(span: dict, by_span_id: dict) -> dict:
    seen: set = set()
    current: str | None = span.get("span_id")
    while current and current not in seen:
        node = by_span_id.get(current) or {}
        attributes = node.get("attributes") or {}
        if "airflow.task_id" in attributes:
            return {
                "dag_id": attributes.get("airflow.dag_id"),
                "task_id": attributes.get("airflow.task_id"),
                "run_id": attributes.get("airflow.dag_run.run_id"),
                "map_index": attributes.get("airflow.task_instance.map_index"),
                "try_number": attributes.get("airflow.task_instance.try_number"),
            }
        seen.add(current)
        current = node.get("parent_span_id")
    return {"dag_id": None, "task_id": None, "run_id": None, "map_index": None, "try_number": None}


def _ancestor(span: dict, by_span_id: dict, roots: set) -> str | None:
    seen = set()
    current = span.get("parent_span_id")
    while current and current not in seen:
        if current in roots:
            return current
        seen.add(current)
        current = (by_span_id.get(current) or {}).get("parent_span_id")
    return None


def read_agent_runs(path: Path = SPANS_FILE) -> list[dict]:
    spans = read_spans(path)
    if not spans:
        return []

    by_span_id = {s["span_id"]: s for s in spans if s.get("span_id")}
    agent_spans = [s for s in spans if _is_genai(s) and _is_agent_run(s)]
    if not agent_spans:
        log.warning("no invoke_agent spans found among %s spans", len(spans))
        return []

    roots = {s["span_id"] for s in agent_spans}

    tools_by_run: dict[str, list[dict]] = {}
    tools_offered: dict[str, list[str]] = {}
    for span in spans:
        if not _is_genai(span):
            continue
        run_id = _ancestor(span, by_span_id, roots)
        if run_id is None:
            continue
        if _is_tool_call(span):
            tools_by_run.setdefault(run_id, []).append(
                {
                    "name": span["attributes"].get("gen_ai.tool.name") or span["name"],
                    "duration_ms": span["duration_ms"],
                }
            )
        elif _is_model_call(span):
            for name in _tool_names(span["attributes"]):
                tools_offered.setdefault(run_id, [])
                if name not in tools_offered[run_id]:
                    tools_offered[run_id].append(name)

    runs = []
    for span in agent_spans:
        attributes = span["attributes"]
        raw_output = _first(attributes, _FINAL_RESULT_KEYS)
        output = raw_output
        if isinstance(raw_output, str):
            try:
                output = json.loads(raw_output)
            except json.JSONDecodeError:
                output = raw_output
        runs.append(
            {
                "trace_id": span["trace_id"],
                "span_id": span["span_id"],
                "service_name": span["service_name"],
                "start_unix_nano": span["start_unix_nano"],
                "model": _first(attributes, _AGENT_MODEL_KEYS),
                "input_tokens": _first(attributes, _INPUT_TOKEN_KEYS),
                "output_tokens": _first(attributes, _OUTPUT_TOKEN_KEYS),
                "reasoning_tokens": _first(attributes, _REASONING_TOKEN_KEYS),
                "cost": _first(attributes, _COST_KEYS),
                "duration_ms": span["duration_ms"],
                "prompt": _first(attributes, _MESSAGES_KEYS),
                "output": output,
                "tool_calls": tools_by_run.get(span["span_id"], []),
                "tools_available": tools_offered.get(span["span_id"], []),
                **_airflow_context(span, by_span_id),
            }
        )

    by_task: dict = {}
    for run in runs:
        by_task[run["task_id"]] = by_task.get(run["task_id"], 0) + 1
    log.info("%s agent runs found, by task: %s", len(runs), by_task)
    return runs
