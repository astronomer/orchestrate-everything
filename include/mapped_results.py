from __future__ import annotations

from collections.abc import Mapping


def as_records(mapped_group_output) -> list[dict]:
    if isinstance(mapped_group_output, Mapping):
        return [dict(mapped_group_output)]
    return list(mapped_group_output)
