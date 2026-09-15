"""
WARNING: Drops every table, recreates the schema, reseeds the reference data, and empties
the exported span file.
"""

from airflow.sdk import chain, dag, task

from include.dag_defaults import DEFAULT_ARGS, DB_TASK_ARGS
from include.demo_setup import (
    clear_exported_spans,
    drop_all_tables,
    rebuild_schema,
    seed_reference_data,
    seed_shipment_history,
)
from include.mlops.assets import SHIPMENT_HISTORY


@dag(tags=["Setup"], max_active_tasks=1, doc_md=__doc__, default_args=DEFAULT_ARGS)
def reset_demo():

    @task
    def clear_traces() -> int:
        return clear_exported_spans()

    @task(**DB_TASK_ARGS)
    def drop_tables() -> list[str]:
        return drop_all_tables()

    @task(**DB_TASK_ARGS)
    def create_tables() -> None:
        rebuild_schema()

    @task(**DB_TASK_ARGS)
    def seed_tickets() -> int:
        return seed_reference_data()

    @task(outlets=[SHIPMENT_HISTORY], **DB_TASK_ARGS)
    def seed_shipments() -> int:
        return seed_shipment_history()

    chain(
        clear_traces(), drop_tables(), create_tables(), seed_tickets(), seed_shipments()
    )


reset_demo()
