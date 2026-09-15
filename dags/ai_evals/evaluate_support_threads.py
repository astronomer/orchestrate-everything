import logging
from typing import Literal

from airflow.sdk import dag, task, task_group
from pydantic import BaseModel, Field

from include.dag_defaults import DEFAULT_ARGS, MODEL_TASK_ARGS
from include.mapped_results import as_records
from include.metrics_store import write_eval_records, write_metrics
from include.support_systems import (
    CSAT_DIMENSIONS,
    SCORE_CUSTOMER_REPLY_SYSTEM_PROMPT,
    SCORE_SUPPORT_REPLY_SYSTEM_PROMPT,
    fetch_customer_replies,
    fetch_satisfaction_scores,
    fetch_support_replies,
)

log = logging.getLogger(__name__)


class SupportReplyScore(BaseModel):
    accurate: bool = Field(
        description="True if every claim in the support response is supported by the ticket or the order record."
    )
    addresses_question: bool = Field(
        description="True if the support response answers what the customer asked."
    )
    tone: Literal["good", "acceptable", "poor"] = Field(
        description="Fit of the tone for a customer on this account tier."
    )
    reasoning: str = Field(description="One sentence supporting the scores.")


class CustomerReplyScore(BaseModel):
    sentiment: Literal["positive", "neutral", "negative"] = Field(
        description="Overall sentiment the customer expresses in their reply."
    )
    satisfied: bool = Field(
        description="True if the customer considers their request handled."
    )
    escalation_risk: Literal["low", "medium", "high"] = Field(
        description="Risk that this customer escalates or churns without follow-up."
    )
    reasoning: str = Field(description="One sentence supporting the scores.")


@dag(tags=["AI evals", "AI product evals"], default_args=DEFAULT_ARGS)
def evaluate_support_threads():

    @task
    def fetch_replies_from_helpdesk() -> list[dict]:
        return fetch_support_replies()

    @task
    def fetch_replies_from_inbox() -> list[dict]:
        return fetch_customer_replies()

    @task
    def fetch_ratings_from_survey_tool() -> list[dict]:
        return fetch_satisfaction_scores()

    @task
    def join_by_ticket(
        support_replies: list[dict],
        customer_replies: list[dict],
        satisfaction_scores: list[dict],
    ) -> list[dict]:
        customer_by_ticket = {r["ticket_id"]: r for r in customer_replies}
        survey_by_ticket = {s["ticket_id"]: s for s in satisfaction_scores}

        dropped = [
            support["ticket_id"]
            for support in support_replies
            if support["ticket_id"] not in customer_by_ticket
            or support["ticket_id"] not in survey_by_ticket
        ]
        if dropped:
            log.warning(
                "%s of %s tickets have no customer reply or no survey score and "
                "are not scored: %s",
                len(dropped),
                len(support_replies),
                sorted(dropped),
            )

        return [
            {
                "ticket_id": support["ticket_id"],
                "customer_ask": support["customer_ask"],
                "order_record": support["order_record"],
                "support_response": support["support_response"],
                "customer_response": customer_by_ticket[support["ticket_id"]][
                    "customer_response"
                ],
                "ratings": survey_by_ticket[support["ticket_id"]]["ratings"],
            }
            for support in support_replies
            if support["ticket_id"] in customer_by_ticket
            and support["ticket_id"] in survey_by_ticket
        ]

    @task_group
    def evaluate_thread(thread: dict):
        @task.llm(
            llm_conn_id="pydanticai_default",
            system_prompt=SCORE_SUPPORT_REPLY_SYSTEM_PROMPT,
            output_type=SupportReplyScore,
            serialize_output=True,
            **MODEL_TASK_ARGS,
        )
        def score_support_reply(thread: dict) -> str:
            return (
                f"Customer ask:\n{thread['customer_ask']}\n\n"
                f"Order record:\n{thread['order_record']}\n\n"
                f"Support response:\n{thread['support_response']}"
            )

        @task.llm(
            llm_conn_id="pydanticai_default",
            system_prompt=SCORE_CUSTOMER_REPLY_SYSTEM_PROMPT,
            output_type=CustomerReplyScore,
            serialize_output=True,
            **MODEL_TASK_ARGS,
        )
        def score_customer_reply(thread: dict) -> str:
            return (
                f"Customer ask:\n{thread['customer_ask']}\n\n"
                f"Support response:\n{thread['support_response']}\n\n"
                f"Customer reply:\n{thread['customer_response']}"
            )

        @task
        def score_satisfaction(thread: dict) -> dict:
            ratings = thread["ratings"]
            return {
                **{f"csat_{d}": ratings[d] for d in CSAT_DIMENSIONS},
                "csat_average": sum(ratings[d] for d in CSAT_DIMENSIONS)
                / len(CSAT_DIMENSIONS),
                "detractor": any(ratings[d] <= 2 for d in CSAT_DIMENSIONS),
            }

        @task
        def build_record(
            thread: dict,
            reply_score: dict,
            customer_score: dict,
            satisfaction_score: dict,
        ) -> dict:
            return {
                "ticket_id": thread["ticket_id"],
                "reply_accurate": reply_score["accurate"],
                "reply_addresses_question": reply_score["addresses_question"],
                "reply_tone": reply_score["tone"],
                "reply_reasoning": reply_score["reasoning"],
                "customer_sentiment": customer_score["sentiment"],
                "customer_satisfied": customer_score["satisfied"],
                "escalation_risk": customer_score["escalation_risk"],
                "customer_reasoning": customer_score["reasoning"],
                **satisfaction_score,
            }

        return build_record(
            thread=thread,
            reply_score=score_support_reply(thread),
            customer_score=score_customer_reply(thread),
            satisfaction_score=score_satisfaction(thread),
        )

    @task
    def load_metrics(mapped_records) -> None:
        records = as_records(mapped_records)
        write_eval_records(records)

        total = len(records)
        if not total:
            log.warning("no scored threads to report on")
            return

        write_metrics(
            tickets_scored=total,
            accuracy_rate=sum(r["reply_accurate"] for r in records) / total,
            relevance_rate=sum(r["reply_addresses_question"] for r in records) / total,
            poor_tone_rate=sum(r["reply_tone"] == "poor" for r in records) / total,
            positive_sentiment_rate=sum(
                r["customer_sentiment"] == "positive" for r in records
            )
            / total,
            satisfied_rate=sum(r["customer_satisfied"] for r in records) / total,
            high_escalation_risk_rate=sum(
                r["escalation_risk"] == "high" for r in records
            )
            / total,
            detractor_rate=sum(r["detractor"] for r in records) / total,
            **{
                f"avg_csat_{d}": sum(r[f"csat_{d}"] for r in records) / total
                for d in CSAT_DIMENSIONS
            },
        )

    _threads = join_by_ticket(
        support_replies=fetch_replies_from_helpdesk(),
        customer_replies=fetch_replies_from_inbox(),
        satisfaction_scores=fetch_ratings_from_survey_tool(),
    )

    load_metrics(evaluate_thread.expand(thread=_threads))


evaluate_support_threads()
