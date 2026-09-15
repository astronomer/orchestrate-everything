import logging
from datetime import timedelta

from airflow.providers.standard.operators.hitl import (
    HITLBranchOperator,
    HITLEntryOperator,
)
from airflow.sdk import Param, chain, dag, task
from pydantic import BaseModel, Field

from include.dag_defaults import AGENT_TASK_ARGS, DB_TASK_ARGS, DEFAULT_ARGS
from include.draft_store import send_reply
from include.support_systems import fetch_tickets, ticket_subject
from include.support_toolset import (
    DRAFT_SUPPORT_REPLY_SYSTEM_PROMPT,
    cosmarket_sql,
    support_toolset,
)

log = logging.getLogger(__name__)


class SupportReply(BaseModel):
    body: str = Field(
        description="The reply sent to the customer, signed off as CosMarket Support."
    )
    resolves_ticket: bool = Field(
        description="True if this reply answers what the customer asked."
    )
    evidence_used: str = Field(
        description="The facts from the order record the reply relies on."
    )
    needs_human: bool = Field(
        description="True if a person has to act before this reply is sent."
    )
    context_used: list[str] = Field(
        description=(
            "The chunk_ids of the policy passages this reply relied on, exactly "
            "as they appeared in square brackets in the search_policy results. "
            "Empty if no policy passage was used."
        )
    )


@dag(
    tags=["AI orchestration", "Agent orchestration", "HITL"], default_args=DEFAULT_ARGS
)
def draft_support_replies():

    @task
    def fetch_open_ticket() -> dict:
        return fetch_tickets()[0]

    _fetch_open_ticket = fetch_open_ticket()

    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt=DRAFT_SUPPORT_REPLY_SYSTEM_PROMPT,
        output_type=SupportReply,
        toolsets=[support_toolset, cosmarket_sql],
        serialize_output=True,
        durable=True,  # Durable Execution with the task state store
        **AGENT_TASK_ARGS,
    )
    def draft_reply(ticket: dict) -> str:
        return (
            f"ticket_id: {ticket['ticket_id']}\n"
            f"customer_id: {ticket['customer_id']}\n"
            f"product_sku: {ticket['product_sku']}\n\n"
            f"Customer message:\n{ticket['customer_ask']}"
        )

    _draft_reply = draft_reply(_fetch_open_ticket)

    @task
    def format_review_request(
        drafted_reply: SupportReply | dict, original_ticket: dict
    ) -> dict:
        if not isinstance(drafted_reply, dict):
            drafted_reply = drafted_reply.model_dump()
        return {
            "ticket_info": (
                f"**Ticket:** {original_ticket['ticket_id']}\n"
                f"**Customer:** {original_ticket['customer_id']}\n"
                f"**Product:** {original_ticket['product_sku']}\n"
                f"**Subject:** {ticket_subject(original_ticket)}"
            ),
            "draft_body": drafted_reply["body"],
            "evidence_used": drafted_reply["evidence_used"],
            "resolves_ticket": drafted_reply["resolves_ticket"],
            "needs_human": drafted_reply["needs_human"],
            "context_used": drafted_reply["context_used"],
            "metadata": drafted_reply,
            "original_ticket": original_ticket,
        }

    _format_review_request = format_review_request(
        drafted_reply=_draft_reply, original_ticket=_fetch_open_ticket
    )

    _review_drafted_reply = HITLBranchOperator(
        task_id="review_drafted_reply",
        subject="AI Drafted Support Reply Ready For Review",
        body="""**Please review the drafted reply below before it reaches the customer:**

{{ ti.xcom_pull(task_ids='format_review_request')['ticket_info'] }}

**Agent Flagged This For A Human:** {{ 'yes' if ti.xcom_pull(task_ids='format_review_request')['needs_human'] else 'no' }}
**Agent Says This Resolves The Ticket:** {{ 'yes' if ti.xcom_pull(task_ids='format_review_request')['resolves_ticket'] else 'no' }}
**Policy Passages Used:** {{ ti.xcom_pull(task_ids='format_review_request')['context_used'] | join(', ') or 'none' }}

**The Customer Wrote:**
```
{{ ti.xcom_pull(task_ids='format_review_request')['original_ticket']['customer_ask'] }}
```

**Evidence The Agent Relied On:**
```
{{ ti.xcom_pull(task_ids='format_review_request')['evidence_used'] }}
```

**Drafted Reply:**
```
{{ ti.xcom_pull(task_ids='format_review_request')['draft_body'] }}
```

**Instructions:**
- **Send AI Reply**: send this reply to the customer as drafted
- **Respond Manually**: write the reply yourself instead
- **Escalate To On-Call Engineer**: life support or safety issue
- **Escalate To Account Manager**: commercial or relationship issue

Check every promise in the reply against the evidence before you send it.""",
        options=[
            "Send AI Reply",
            "Respond Manually",
            "Escalate To On-Call Engineer",
            "Escalate To Account Manager",
        ],
        options_mapping={
            "Send AI Reply": "send_ai_reply",
            "Respond Manually": "respond_manually",
            "Escalate To On-Call Engineer": "escalate_to_on_call_engineer",
            "Escalate To Account Manager": "escalate_to_account_manager",
        },
        defaults=["Escalate To On-Call Engineer"],
        multiple=False,
        response_timeout=timedelta(minutes=5),
    )

    @task(**DB_TASK_ARGS)
    def send_ai_reply(original_ticket: dict, review_request: dict) -> str:
        log.info("processing ticket %s", original_ticket["ticket_id"])
        log.info("sending approved AI reply: %s", review_request["draft_body"])
        return send_reply(
            body=review_request["draft_body"],
            ticket=original_ticket,
            context_used=review_request["context_used"],
        )

    _send_ai_reply = send_ai_reply(
        original_ticket=_fetch_open_ticket,
        review_request=_format_review_request,
    )

    _respond_manually = HITLEntryOperator(
        task_id="respond_manually",
        subject="Manual Reply",
        body="""**Please enter the reply that should go to the customer:**
```
{{ ti.xcom_pull(task_ids='format_review_request')['original_ticket']['customer_ask'] }}
```
""",
        params={
            "manual_reply": Param(
                "",
                type="string",
                minLength=1,
                title="Reply to the customer",
                description="Sent verbatim, signed off as CosMarket Support.",
            ),
        },
    )

    @task(**DB_TASK_ARGS)
    def send_manual_reply(original_ticket: dict, manual_reply: dict) -> str:
        body = manual_reply["params_input"]["manual_reply"]
        log.info("processing ticket %s", original_ticket["ticket_id"])
        log.info("sending manual reply: %s", body)
        return send_reply(body=body, ticket=original_ticket)

    _send_manual_reply = send_manual_reply(
        original_ticket=_fetch_open_ticket,
        manual_reply=_respond_manually.output,
    )

    @task
    def escalate_to_on_call_engineer(original_ticket: dict):
        log.info("processing ticket %s", original_ticket["ticket_id"])
        log.warning("paging the on-call engineer, no reply sent")

    _escalate_to_on_call_engineer = escalate_to_on_call_engineer(
        original_ticket=_fetch_open_ticket,
    )

    @task
    def escalate_to_account_manager(original_ticket: dict):
        log.info("processing ticket %s", original_ticket["ticket_id"])
        log.warning("escalating to the account manager, no reply sent")

    _escalate_to_account_manager = escalate_to_account_manager(
        original_ticket=_fetch_open_ticket,
    )

    chain(
        _format_review_request,
        _review_drafted_reply,
        [
            _send_ai_reply,
            _respond_manually,
            _escalate_to_on_call_engineer,
            _escalate_to_account_manager,
        ],
    )
    chain(
        _respond_manually,
        _send_manual_reply,
    )


draft_support_replies()
