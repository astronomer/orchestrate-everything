__version__ = "0.1.0"


def get_provider_info() -> dict:
    return {
        "package-name": "cosmarket-duckdb",
        "name": "CosMarket DuckDB",
        "description": "DuckDB DbApiHook for local development.",
        "connection-types": [
            {
                "connection-type": "duckdb",
                "hook-class-name": "cosmarket_duckdb.hooks.DuckDBHook",
            }
        ],
        "versions": [__version__],
    }
