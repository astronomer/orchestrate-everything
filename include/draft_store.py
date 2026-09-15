from __future__ import annotations

import json
import logging

from include.cosmarket_db import upsert
from include.support_systems import SUPPORT_ADDRESS, thread_id_for, ticket_subject

log = logging.getLogger(__name__)

REPLY_TURN = 2


def _outbound_message(body: str, ticket: dict, context_used: list[str] | None) -> dict:
    ticket_id = ticket["ticket_id"]
    return {
        "message_id": f"MSG-{ticket_id}-{REPLY_TURN}",
        "thread_id": thread_id_for(ticket_id),
        "turn": REPLY_TURN,
        "direction": "outbound",
        "sender": f"CosMarket Support <{SUPPORT_ADDRESS}>",
        "subject": f"Re: {ticket_subject(ticket)}",
        "body": body,
        "context_used": json.dumps(context_used or []),
    }


def send_reply(
    body: str, ticket: dict, context_used: list[str] | None = None
) -> str:
    message = _outbound_message(body, ticket, context_used)
    upsert("support_messages", "message_id", [message])
    log.info(
        "sent reply %s to the customer on %s",
        message["message_id"],
        ticket["ticket_id"],
    )
    return message["message_id"]
