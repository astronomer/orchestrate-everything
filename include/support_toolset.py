from __future__ import annotations

import logging

from airflow.providers.common.ai.toolsets import SQLToolset
from pydantic_ai.toolsets import FunctionToolset

from include.support_systems import ticket_by_id

log = logging.getLogger(__name__)

DRAFT_SUPPORT_REPLY_SYSTEM_PROMPT = (
    "You draft customer support replies for CosMarket, an ecommerce "
    "marketplace serving ships, stations, and colonies across the "
    "system.\n\n"
    "Always call get_order_record for the ticket before you write "
    "anything. Use the SQL tools to look up the customer and the "
    "product when the reply depends on who they are or what they "
    "bought: list_tables and get_schema show what is available, and "
    "query runs read-only SQL. Call "
    "search_policy before you state what CosMarket will or will not do, "
    "and record the chunk_ids you relied on in context_used.\n\n"
    "Use only what those tools return. Never promise a refund, a "
    "replacement, a discount, a policy, or a release date that the order "
    "record or a policy passage does not confirm. If the record shows "
    "the issue affects life support, follow the escalation path it "
    "names. Sign off as CosMarket Support."
)

cosmarket_sql = SQLToolset(
    db_conn_id="duckdb_default",
    allowed_tables=["products", "customers", "support_threads", "support_messages"],
    allow_writes=False,
    max_rows=20,
)


def get_order_record(ticket_id: str) -> str:
    ticket = ticket_by_id(ticket_id)
    if ticket is None:
        return f"No ticket found with id {ticket_id}."
    return ticket["order_record"]


def search_policy(query: str) -> str:
    from pydantic_ai.exceptions import ModelRetry

    from include.context_units import search_context_units

    try:
        results = search_context_units(query)
    except Exception as exc:
        log.warning("policy search failed: %s", exc)
        raise ModelRetry(
            "The policy corpus could not be searched "
            f"({type(exc).__name__}). Do not promise anything policy would have "
            "to confirm; say a human will follow up instead."
        ) from exc

    if not results:
        return (
            "No policy passages found. The context_units table may be empty, "
            "run the context_engineering Dag."
        )
    return "\n\n".join(
        f"[{r['chunk_id']}] {r['document_title']} / {r['title']}\n{r['body']}"
        for r in results
    )


support_toolset = FunctionToolset([get_order_record, search_policy])
