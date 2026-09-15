from __future__ import annotations

from pendulum import duration

from airflow.providers.common.ai.policies.retry import LLMRetryPolicy
from airflow.sdk.definitions.retry_policy import (
    ExceptionRetryPolicy,
    RetryAction,
    RetryRule,
)

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": duration(seconds=30),
}


_LOCAL_DB_RULES = [
    RetryRule(
        exception=[
            "duckdb.IOException",
            "duckdb.ConnectionException",
            "duckdb.TransactionException",
        ],
        action=RetryAction.RETRY,
        retry_delay=duration(seconds=10),
        reason="another process holds the DuckDB file, it only needs to let go",
    ),
    RetryRule(
        exception=[
            "duckdb.BinderException",
            "duckdb.CatalogException",
            "duckdb.ConversionException",
            "duckdb.ConstraintException",
            "duckdb.InvalidInputException",
        ],
        action=RetryAction.FAIL,
        reason="schema or data error, a retry writes the same bad rows again",
    ),
    RetryRule(
        exception=[KeyError, IndexError, TypeError, AttributeError],
        action=RetryAction.FAIL,
        reason="the data is not the shape the code expects, identical on every attempt",
    ),
    RetryRule(
        exception=ValueError,
        action=RetryAction.FAIL,
        reason="a precondition this Dag checks itself is not met, run setup first",
    ),
]

_MODEL_API_RULES = [
    RetryRule(
        exception="openai.RateLimitError",
        action=RetryAction.RETRY,
        retry_delay=duration(minutes=2),
        reason="rate limited, the quota window has to roll over",
    ),
    RetryRule(
        exception=[
            "openai.APITimeoutError",
            "openai.APIConnectionError",
            "openai.InternalServerError",
            "pydantic_ai.exceptions.ModelHTTPError",
        ],
        action=RetryAction.RETRY,
        retry_delay=duration(seconds=30),
        reason="provider side or network, usually gone by the next attempt",
    ),
    RetryRule(
        exception=[
            "openai.AuthenticationError",
            "openai.PermissionDeniedError",
            "openai.BadRequestError",
            "openai.NotFoundError",
        ],
        action=RetryAction.FAIL,
        reason="credentials, model id, or request shape, no retry fixes those",
    ),
    RetryRule(
        exception="pydantic_ai.exceptions.UsageLimitExceeded",
        action=RetryAction.FAIL,
        reason="the run hit its usage limit, retrying just spends more",
    ),
    RetryRule(
        exception=[
            "pydantic_ai.exceptions.UnexpectedModelBehavior",
            "pydantic_core.ValidationError",
        ],
        action=RetryAction.DEFAULT,
        reason="output did not match the schema, another sample often does",
    ),
]

_TRACE_RULES = [
    RetryRule(
        exception=[FileNotFoundError, "json.JSONDecodeError"],
        action=RetryAction.RETRY,
        retry_delay=duration(seconds=15),
        reason="the collector batches every 5s, the span may not be on disk yet",
    ),
    RetryRule(
        exception=[KeyError, IndexError, TypeError, AttributeError],
        action=RetryAction.FAIL,
        reason="the span payload changed shape, that needs a code change not a retry",
    ),
]

_TRAINING_RULES = [
    RetryRule(
        exception=MemoryError,
        action=RetryAction.FAIL,
        reason="the training set does not fit, a retry meets the same rows",
    ),
    RetryRule(
        exception=ValueError,
        action=RetryAction.FAIL,
        reason="usually a stratified split with under two rows in a class, reseed instead",
    ),
]


DB_TASK_ARGS = {
    "retries": 4,
    "retry_delay": duration(seconds=10),
    "retry_policy": ExceptionRetryPolicy(rules=_LOCAL_DB_RULES),
}

CHECK_TASK_ARGS = {
    "retries": 0,
    "retry_policy": ExceptionRetryPolicy(
        rules=[
            RetryRule(
                exception=Exception,
                action=RetryAction.FAIL,
                reason="a quality gate failed, and the data it read has not changed",
            )
        ]
    ),
}

MODEL_TASK_ARGS = {
    "retries": 5,
    "retry_delay": duration(minutes=1),
    "execution_timeout": duration(minutes=15),
    "retry_policy": ExceptionRetryPolicy(rules=_MODEL_API_RULES),
}

TRAINING_TASK_ARGS = {
    "retries": 1,
    "retry_delay": duration(seconds=30),
    "execution_timeout": duration(minutes=30),
    "retry_policy": ExceptionRetryPolicy(rules=[*_TRAINING_RULES, *_LOCAL_DB_RULES]),
}


AGENT_TASK_ARGS = {
    "retries": 5,
    "retry_delay": duration(minutes=10),
    "execution_timeout": duration(minutes=15),
    "retry_policy": LLMRetryPolicy(
        llm_conn_id="pydanticai_default",
        timeout=30.0,
        fallback_rules=[*_MODEL_API_RULES, *_LOCAL_DB_RULES],
    ),
}


TRACE_TASK_ARGS = {
    "retries": 3,
    "retry_delay": duration(seconds=30),
    "execution_timeout": duration(minutes=10),
    "retry_policy": LLMRetryPolicy(
        llm_conn_id="pydanticai_default",
        timeout=30.0,
        fallback_rules=[*_TRACE_RULES, *_LOCAL_DB_RULES],
    ),
}
