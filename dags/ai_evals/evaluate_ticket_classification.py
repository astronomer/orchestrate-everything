from typing import Literal

from airflow.sdk import Param, dag, get_current_context, task
from pydantic import BaseModel, Field

from include.dag_defaults import DEFAULT_ARGS, MODEL_TASK_ARGS
from include.ticket_classification import (
    CATEGORIES,
    CLASSIFY_TICKET_SYSTEM_PROMPT,
    labelled_tickets,
    sampling_cases,
    score_pass_at_k,
)


class TicketClassification(BaseModel):
    category: Literal[CATEGORIES] = Field(
        description="The single category that best fits what the customer is asking for."
    )
    reasoning: str = Field(description="One sentence supporting the category.")


@dag(
    tags=["AI evals", "AI model evals"],
    default_args=DEFAULT_ARGS,
    params={
        "k": Param(
            5,
            type="integer",
            minimum=1,
            maximum=20,
            description="How many times each ticket is classified in total.",
        )
    },
)
def evaluate_ticket_classification():

    @task
    def fetch_labelled_tickets() -> list[dict]:
        return labelled_tickets()

    @task
    def build_sampling_cases(tickets: list[dict]) -> list[dict]:
        return sampling_cases(tickets, attempts=get_current_context()["params"]["k"])

    @task.llm(
        llm_conn_id="pydanticai_default",
        model_id="openai:gpt-4.1-nano",
        system_prompt=CLASSIFY_TICKET_SYSTEM_PROMPT,
        output_type=TicketClassification,
        serialize_output=True,
        agent_params={"model_settings": {"temperature": 1.0}},
        map_index_template="{{ task.op_kwargs.case.ticket_id }} #{{ task.op_kwargs.case.attempt }}",
        **MODEL_TASK_ARGS,
    )
    def classify_ticket(case: dict) -> str:
        return case["customer_ask"]

    @task
    def report_pass_at_k(cases: list[dict], classifications: list[dict]) -> dict:
        return score_pass_at_k(cases, classifications)

    _cases = build_sampling_cases(fetch_labelled_tickets())

    report_pass_at_k(
        cases=_cases,
        classifications=classify_ticket.expand(case=_cases),
    )


evaluate_ticket_classification()
