from typing import Literal

from airflow.sdk import dag, task, task_group
from pydantic import BaseModel, Field

from include.dag_defaults import (
    DB_TASK_ARGS,
    DEFAULT_ARGS,
    MODEL_TASK_ARGS,
    TRACE_TASK_ARGS,
)
from include.eval_records import (
    COMPARE_WITH_REFERENCE_SYSTEM_PROMPT,
    SCORE_RUBRIC_SYSTEM_PROMPT,
    build_eval_record,
    check_reply_structure,
    load_eval_results,
    resolve_scored_runs,
)
from include.mapped_results import as_records
from include.otel_traces import read_agent_runs


class DimensionScore(BaseModel):
    score: Literal["good", "acceptable", "poor"] = Field(
        description=(
            "good: fully meets the criterion. acceptable: meets it with a flaw "
            "a reviewer would let through. poor: fails the criterion."
        )
    )
    confidence: Literal["low", "medium", "high"] = Field(
        description=(
            "How certain you are about the score you just gave for this "
            "dimension only, not about the reply overall."
        )
    )
    reasoning: str = Field(
        description="One sentence supporting the score for this dimension only."
    )


class SupportReplyScore(BaseModel):

    relevance: DimensionScore = Field(
        description="Whether the reply addresses what the customer actually asked."
    )
    helpfulness: DimensionScore = Field(
        description="Whether the reply gives the customer something they can act on."
    )
    conciseness: DimensionScore = Field(
        description="Whether the reply says what it needs to without padding or repetition."
    )


class ReferenceComparison(BaseModel):

    correctness: DimensionScore = Field(
        description=(
            "Whether the reply's factual claims agree with the reference reply. "
            "A claim the reference contradicts, or a promise the reference "
            "deliberately withholds, is not correct."
        )
    )
    equivalence: Literal["equivalent", "close", "different"] = Field(
        description=(
            "equivalent: makes the same points as the reference. close: same "
            "intent, misses or adds a point. different: a different reply."
        )
    )
    confidence: Literal["low", "medium", "high"] = Field(
        description="How certain you are about the equivalence judgement."
    )
    missing_points: list[str] = Field(
        description="Points the reference makes that the generated reply does not."
    )
    reasoning: str = Field(description="One sentence supporting the judgement.")


@dag(tags=["AI evals", "AI model evals"], default_args=DEFAULT_ARGS)
def evaluate_support_model_responses():

    @task(**TRACE_TASK_ARGS)
    def fetch_traces() -> list[dict]:
        return read_agent_runs()

    @task(**TRACE_TASK_ARGS)
    def resolve_tickets(agent_runs: list[dict]) -> list[dict]:
        return resolve_scored_runs(agent_runs)

    @task_group
    def evaluate_reply(run: dict):
        @task
        def check_structure(run: dict) -> dict:
            return check_reply_structure(run["generated_reply"])

        @task.llm(
            llm_conn_id="pydanticai_default",
            system_prompt=SCORE_RUBRIC_SYSTEM_PROMPT,
            output_type=SupportReplyScore,
            serialize_output=True,
            **MODEL_TASK_ARGS,
        )
        def score_rubric(run: dict) -> str:
            return (
                f"Customer message:\n{run['customer_ask']}\n\n"
                f"Generated reply:\n{run['generated_reply']}"
            )

        @task.llm(
            llm_conn_id="pydanticai_default",
            system_prompt=COMPARE_WITH_REFERENCE_SYSTEM_PROMPT,
            output_type=ReferenceComparison,
            serialize_output=True,
            **MODEL_TASK_ARGS,
        )
        def compare_with_reference(run: dict) -> str:
            return (
                f"Reference reply:\n{run['reference_reply']}\n\n"
                f"Generated reply:\n{run['generated_reply']}"
            )

        @task
        def build_record(
            run: dict, structure: dict, rubric: dict, reference: dict
        ) -> dict:
            return build_eval_record(run, structure, rubric, reference)

        return build_record(
            run=run,
            structure=check_structure(run),
            rubric=score_rubric(run),
            reference=compare_with_reference(run),
        )

    @task(**DB_TASK_ARGS)
    def load_metrics(records) -> None:
        load_eval_results(as_records(records))

    _runs = resolve_tickets(fetch_traces())

    load_metrics(evaluate_reply.expand(run=_runs))


evaluate_support_model_responses()
