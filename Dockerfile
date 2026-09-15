FROM astrocrpublic.azurecr.io/runtime:3.3-4

RUN pip install --no-cache-dir /usr/local/airflow/include/cosmarket_duckdb
