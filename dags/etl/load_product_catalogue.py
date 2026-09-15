import csv
from pathlib import Path

from airflow.sdk import Asset, dag, task

from include.cosmarket_db import upsert
from include.dag_defaults import CHECK_TASK_ARGS, DB_TASK_ARGS, DEFAULT_ARGS

CATALOGUE_FILE = Path(__file__).parents[2] / "include" / "csvs" / "products.csv"
PRODUCT_CATALOGUE = Asset("cosmarket_product_catalogue")

NUMERIC_COLUMNS = {
    "product_id": int,
    "category_id": int,
    "weight_kg": float,
    "base_price_credits": float,
}
BOOLEAN_COLUMNS = ("is_hazardous", "requires_cold_chain", "is_active")


@dag(tags=["ETL"], max_active_tasks=1, default_args=DEFAULT_ARGS)
def load_product_catalogue():

    @task
    def extract_catalogue() -> list[dict]:
        with CATALOGUE_FILE.open(newline="") as handle:
            return list(csv.DictReader(handle))

    @task
    def transform_catalogue(rows: list[dict]) -> list[dict]:
        products = []
        for row in rows:
            product = {key: (value.strip() or None) for key, value in row.items()}
            for column, cast in NUMERIC_COLUMNS.items():
                value = product.get(column)
                product[column] = cast(value) if value is not None else None
            for column in BOOLEAN_COLUMNS:
                product[column] = str(product.get(column)).lower() == "true"
            if product["is_active"]:
                products.append(product)
        return products

    @task(**CHECK_TASK_ARGS)
    def check_catalogue(products: list[dict]) -> int:
        if not products:
            raise ValueError("the catalogue is empty, nothing would be loaded")

        skus = [p["product_sku"] for p in products]
        duplicates = {sku for sku in skus if skus.count(sku) > 1}
        if duplicates:
            raise ValueError(f"duplicate product_sku values: {sorted(duplicates)}")

        unpriced = [p["product_sku"] for p in products if not p["base_price_credits"]]
        if unpriced:
            raise ValueError(f"products with no price: {unpriced}")

        return len(products)

    @task(outlets=[PRODUCT_CATALOGUE], **DB_TASK_ARGS)
    def load_catalogue(products: list[dict]) -> int:
        return upsert("products", "product_sku", products)

    _rows = extract_catalogue()
    _products = transform_catalogue(_rows)

    check_catalogue(_products) >> load_catalogue(_products)


load_product_catalogue()
