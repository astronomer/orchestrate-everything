import logging
from collections import defaultdict
from pathlib import Path

from airflow.sdk import Asset, dag, get_current_context, task

from include.context_units import (
    EMBEDDING_MODEL,
    SEED_DIR,
    chunk_markdown,
    embed_texts,
    embedding_text,
)
from include.cosmarket_db import upsert
from include.dag_defaults import DB_TASK_ARGS, DEFAULT_ARGS, MODEL_TASK_ARGS

log = logging.getLogger(__name__)

CONTEXT_UNITS = Asset("cosmarket_context_units")
CHECKPOINT_PREFIX = "embedding"


@dag(tags=["Context engineering"], max_active_tasks=1, default_args=DEFAULT_ARGS)
def rag_policy_documents():

    @task
    def list_policy_documents() -> list[str]:
        return sorted(str(path) for path in SEED_DIR.glob("*.md"))

    @task
    def chunk_documents(paths: list[str]) -> list[dict]:
        chunks = []
        for path in paths:
            chunks.extend(chunk_markdown(Path(path)))
        return chunks

    @task(**MODEL_TASK_ARGS)
    def embed_chunks(chunks: list[dict]) -> list[dict]:
        store = get_current_context()["task_state_store"]

        by_document = defaultdict(list)
        for chunk in chunks:
            by_document[chunk["source_uri"].split("#")[0]].append(chunk)

        vectors: dict[str, list[float]] = {}
        replayed, embedded, calls = 0, 0, 0

        for document, document_chunks in sorted(by_document.items()):
            pending = []
            for chunk in document_chunks:
                cached = store.get(f"{CHECKPOINT_PREFIX}:{chunk['chunk_id']}")
                if cached is None:
                    pending.append(chunk)
                else:
                    vectors[chunk["chunk_id"]] = cached
                    replayed += 1

            if not pending:
                log.info("%s already embedded, replayed from the task state store", document)
                continue

            log.info("embedding %s (%s chunks)", document, len(pending))
            calls += 1
            for chunk, vector in zip(pending, embed_texts([embedding_text(c) for c in pending])):
                store.set(f"{CHECKPOINT_PREFIX}:{chunk['chunk_id']}", vector)
                vectors[chunk["chunk_id"]] = vector
                embedded += 1

        log.info(
            "%s chunks embedded in %s api calls, %s replayed from the previous attempt",
            embedded,
            calls,
            replayed,
        )
        return [
            {
                **chunk,
                "embedding": vectors[chunk["chunk_id"]],
                "embedding_model": EMBEDDING_MODEL,
            }
            for chunk in chunks
        ]

    @task(outlets=[CONTEXT_UNITS], **DB_TASK_ARGS)
    def load_context_units(units: list[dict]) -> int:
        return upsert("context_units", "chunk_id", units)

    load_context_units(embed_chunks(chunk_documents(list_policy_documents())))


rag_policy_documents()
