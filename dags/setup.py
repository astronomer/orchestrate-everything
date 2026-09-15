from airflow.sdk import chain, dag, task

from include.dag_defaults import DEFAULT_ARGS, DB_TASK_ARGS
from include.demo_setup import (
    rebuild_schema,
    seed_reference_data,
    seed_shipment_history,
)
from include.mlops.assets import SHIPMENT_HISTORY


@dag(tags=["Setup"], max_active_tasks=1, default_args=DEFAULT_ARGS)
def setup():

    @task(**DB_TASK_ARGS)
    def create_tables() -> None:
        rebuild_schema()

    @task(**DB_TASK_ARGS)
    def seed_tickets() -> int:
        return seed_reference_data()

    @task(outlets=[SHIPMENT_HISTORY], **DB_TASK_ARGS)
    def seed_shipments() -> int:
        return seed_shipment_history()

    chain(create_tables(), seed_tickets(), seed_shipments())


setup()
