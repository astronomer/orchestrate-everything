from collections import Counter
from typing import Literal

from airflow.sdk import chain, dag, task, task_group
from pydantic import BaseModel, Field

from include.dag_defaults import DEFAULT_ARGS, MODEL_TASK_ARGS
from include.mapped_results import as_records
from include.review_systems import (
    TRIAGE_REVIEW_SYSTEM_PROMPT,
    fetch_reviews,
    file_to_queue,
)

CATEGORY_QUEUES = {
    "safety_hazard": ("escalate_to_life_support", "life-support-escalation"),
    "product_defect": ("notify_quality_engineering", "quality-engineering"),
    "delivery_damage": ("notify_depot_ops", "depot-and-cold-chain-ops"),
    "feature_request": ("notify_product_team", "product-management"),
    "praise": ("notify_community_team", "community-and-marketing"),
    "other": ("queue_for_human_triage", "support-triage"),
}

MIN_CONFIDENCE = 0.7

TRIAGE_GROUP_ID = "triage_and_route"


class ReviewTriage(BaseModel):
    summary: str = Field(
        description=(
            "One or two sentences covering every complaint or request the review "
            "makes, written for the team that has to act on it."
        )
    )
    category: Literal[
        "safety_hazard",
        "product_defect",
        "delivery_damage",
        "feature_request",
        "praise",
        "other",
    ] = Field(description="The single category that best fits the review.")
    confidence: float = Field(
        ge=0.0,
        le=1.0,
        description=(
            "How well the chosen category fits, from 0.0 for a guess to 1.0 for "
            "an unambiguous fit."
        ),
    )
    reasoning: str = Field(description="One sentence supporting the category.")


@dag(tags=["AI orchestration", "LLM orchestration"], default_args=DEFAULT_ARGS)
def triage_product_reviews():

    @task
    def fetch_new_reviews() -> list[dict]:
        return fetch_reviews()

    @task_group(group_id=TRIAGE_GROUP_ID)
    def triage_and_route(review: dict):

        @task.llm(
            llm_conn_id="pydanticai_default",
            system_prompt=TRIAGE_REVIEW_SYSTEM_PROMPT,
            output_type=ReviewTriage,
            serialize_output=True,
            **MODEL_TASK_ARGS,
        )
        def triage_review(review: dict) -> str:
            return (
                f"review_id: {review['review_id']}\n"
                f"product_sku: {review['product_sku']}\n"
                f"rating: {review['rating']} of 5\n\n"
                f"Title: {review['title']}\n\n"
                f"{review['body']}"
            )

        @task
        def assign_queue(review: dict, triage: dict) -> dict:
            return {
                "review_id": review["review_id"],
                "product_sku": review["product_sku"],
                "rating": review["rating"],
                "summary": triage["summary"],
                "category": triage["category"],
                "confidence": triage["confidence"],
                "reasoning": triage["reasoning"],
                "routed_to": (
                    triage["category"]
                    if triage["confidence"] >= MIN_CONFIDENCE
                    else "other"
                ),
            }

        @task.branch
        def route_by_category(routed: dict) -> str:
            task_id, _ = CATEGORY_QUEUES[routed["routed_to"]]
            return f"{TRIAGE_GROUP_ID}.{task_id}"

        def notify_queue(task_id: str, queue: str):
            @task(task_id=task_id)
            def notify_team(routed: dict) -> int:
                return file_to_queue(queue, [routed])

            return notify_team

        @task(trigger_rule="none_failed_min_one_success")
        def record_outcome(routed: dict) -> dict:
            return routed

        _routed = assign_queue(review=review, triage=triage_review(review))
        _recorded = record_outcome(_routed)

        chain(
            route_by_category(_routed),
            [
                notify_queue(task_id, queue)(_routed)
                for task_id, queue in CATEGORY_QUEUES.values()
            ],
            _recorded,
        )

        return _recorded

    @task
    def report_routing(mapped_routed) -> dict:
        routed = as_records(mapped_routed)
        counts = Counter(review["routed_to"] for review in routed)
        return {
            "reviews_triaged": len(routed),
            "routed_to_human_triage": counts["other"],
            "per_queue": {
                queue: counts[category]
                for category, (_, queue) in CATEGORY_QUEUES.items()
            },
        }

    report_routing(triage_and_route.expand(review=fetch_new_reviews()))


triage_product_reviews()
