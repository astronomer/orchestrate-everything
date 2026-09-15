from __future__ import annotations

import hashlib
import logging
import os
import re
import uuid
from pathlib import Path

from include.cosmarket_db import get_conn

log = logging.getLogger(__name__)

SEED_DIR = Path(os.getenv("AIRFLOW_HOME", "/usr/local/airflow")) / "include" / "seed_context"
EMBEDDING_MODEL = "text-embedding-3-small"


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def chunk_markdown(path: Path) -> list[dict]:
    raw = path.read_text(encoding="utf-8")
    document_title = ""
    sections: list[dict] = []

    for line in raw.splitlines():
        if line.startswith("# ") and not document_title:
            document_title = line[2:].strip()
        elif line.startswith("## "):
            sections.append({"title": line[3:].strip(), "lines": []})
        elif sections:
            sections[-1]["lines"].append(line)

    chunks = []
    for ordinal, section in enumerate(sections):
        body = "\n".join(section["lines"]).strip()
        if not body:
            continue
        source_uri = f"{path.name}#{_slug(section['title'])}"
        chunks.append(
            {
                "chunk_id": str(uuid.uuid5(uuid.NAMESPACE_URL, source_uri)),
                "source_uri": source_uri,
                "document_title": document_title,
                "title": section["title"],
                "body": body,
                "ordinal": ordinal,
                "chars": len(body),
                "checksum": hashlib.sha256(body.encode()).hexdigest()[:16],
            }
        )
    return chunks


def embedding_text(chunk: dict) -> str:
    return f"{chunk['document_title']}: {chunk['title']}\n{chunk['body']}"


def embed_texts(texts: list[str], model: str = EMBEDDING_MODEL) -> list[list[float]]:
    from openai import OpenAI

    response = OpenAI().embeddings.create(model=model, input=texts)
    return [item.embedding for item in response.data]


def search_context_units(query: str, limit: int = 3) -> list[dict]:
    vector = embed_texts([query])[0]
    with get_conn(read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT chunk_id, title, document_title, source_uri, body,
                   list_cosine_similarity(embedding, ?::FLOAT[]) AS similarity
            FROM context_units
            WHERE embedding IS NOT NULL
            ORDER BY similarity DESC
            LIMIT ?
            """,
            [vector, limit],
        ).fetchall()
    return [
        {
            "chunk_id": r[0],
            "title": r[1],
            "document_title": r[2],
            "source_uri": r[3],
            "body": r[4],
            "similarity": r[5],
        }
        for r in rows
    ]
