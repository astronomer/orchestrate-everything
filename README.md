# A Practical Guide to Orchestrate Everything

Welcome! This is the companion repository to the *A Practical Guide to Orchestrate Everything* talk at the [2026 Orchestrate Everything conference](https://www.astronomer.io/events/orchestrate-everything/).

It shows how to use [Apache Airflow®](https://airflow.apache.org/) for

- [ETL](/dags/etl/): loading a product catalogue
- [Context engineering](/dags/context_engineering/): chunking and embedding policy documents for retrieval
- [AI orchestration](/dags/ai_orchestration/): an LLM classifier that routes product reviews, and a tool-calling support agent with a human-in-the-loop review step
- [AI evals](/dags/ai_evals/): OTel trace ingestion, LLM-as-judge scoring, pass@k, and evaluating support outcomes against CSAT
- [MLOps](/dags/mlops/): training a delivery-risk classifier and scoring shipments against it

All of these Dags run **CosMarket**, a fictional ecommerce marketplace serving ships, stations, and colonies across the solar system. The data is stored in a [DuckDB](https://duckdb.org/) file. The only external service needed is an AI model provider, for which you need to provide the key in `.env`.

![Airflow UI Dags list: all 11 Dags in this repo](source/dags_list_overview.png)

## Requirements

- [Docker](https://docs.docker.com/)
- The [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- An OpenAI API key or credentials for another [Pydantic AI compatible model provider](https://pydantic.dev/docs/ai/models/overview/)

## Setup

```bash
cp .env_example .env      # add your OPENAI_API_KEY or credential to another Pydantic AI compatible model provider
astro dev start
```

`astro dev start` starts six containers:

- `postgres`: Airflow's metadata database
- `scheduler`
- `dag-processor`
- `api-server`
- `triggerer`
- `otel-collector`: receives OTLP spans over gRPC/HTTP and appends them to `traces/spans.jsonl`

The `include/cosmarket.duckdb` file is the database Dags interact with. It is created by the `setup` Dag on first run.

`.env_example` defines:

- `AIRFLOW_CONN_DUCKDB_DEFAULT`: `{"conn_type": "duckdb", "host": "/usr/local/airflow/include/cosmarket.duckdb"}`
- `AIRFLOW_CONN_PYDANTICAI_DEFAULT`: `{"conn_type": "pydanticai", "extra": {"model": "openai:gpt-5-mini"}}`, the connection id `@task.llm`/`@task.agent` references. Note that if you want to use a different Pydantic AI compatible model provider, you'll need to set your credential as `password` and change the model. 
- `AIRFLOW__TRACES__OTEL_ON`, `AIRFLOW__COMMON_AI__OTEL_EXPORT_ENABLED`, `AIRFLOW__COMMON_AI__CAPTURE_CONTENT`: all three have to be `True` for a GenAI span to be emitted with its prompt and completion attached
- `OTEL_TRACES_EXPORTER`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_SERVICE_NAME`: send the exporter's spans to the `otel-collector` container over OTLP/HTTP

Add your own `OPENAI_API_KEY` to `.env` after copying it or provide credentials to another [Pydantic AI compatible model provider](https://pydantic.dev/docs/ai/models/overview/). Note that if you choose a different provider, you'll need to change the model specified in the `AIRFLOW_CONN_PYDANTICAI_DEFAULT` and the extra installed for the Common AI provider in the [`requirements.txt`](/requirements.txt) file.

## How to run the example

| # | Step | Notes |
|---|---|---|
| 0 | Unpause all Dags in the UI, or `astro dev run dags unpause --treat-dag-id-as-regex -y ".*"` | - |
| 1 | Run `setup` (or `reset_demo` for a clean slate) | Seeds customers, products, open support tickets, and shipment history |
| 2 | `train_delivery_risk_model` runs on its own | Scheduled on the `cosmarket_shipment_history` asset that step 1 updates |
| 3 | `score_open_shipments` runs on its own | Scheduled on the `cosmarket_delivery_risk_model` asset that step 2 updates |
| 4 | Run `load_product_catalogue`, `rag_policy_documents`, `draft_support_replies`, `triage_product_reviews` in any order | Each is a standalone pattern. Note that `draft_support_replies` has a human-in-the-loop step that will wait 5min for your decision. |
| 5 | Run `evaluate_support_model_responses` after `draft_support_replies` has at least one run | Scores that Dag's OTel traces against golden replies |
| 6 | Run `evaluate_ticket_classification` and `evaluate_support_threads` | Both run idependently of other Dags |

## The Dags

### Setup (`dags/`)

| Dag | What it does |
|---|---|
| `setup` | Creates the schema and seeds customers, products, open support tickets, and shipment history. |
| `reset_demo` | WARINING. Drops every table, recreates the schema, reseeds, and empties the exported span file. |

### ETL (`dags/etl/`)

| Dag | What it does |
|---|---|
| `load_product_catalogue` | Reads `include/csvs/products.csv`, filters to active products, checks for duplicate SKUs and missing prices, and upserts into `products` |

![load_product_catalogue task graph: extract_catalogue, transform_catalogue, check_catalogue, load_catalogue](source/load_product_catalogue_task_graph.png)

### Context engineering (`dags/context_engineering/`)

| Dag | What it does |
|---|---|
| `rag_policy_documents` | Chunks the five markdown policy documents in `include/seed_context/`, embeds each chunk with `text-embedding-3-small`, and upserts into `context_units`. |

![rag_policy_documents task graph: list_policy_documents, chunk_documents, embed_chunks, load_context_units](source/rag_policy_documents_task_graph.png)

### AI orchestration (`dags/ai_orchestration/`)

| Dag | What it does |
|---|---|
| `draft_support_replies` | An `@task.agent` drafts a reply to one open ticket, with a read-only `SQLToolset` with access to to four tables, a tool that looks up the order record behind the ticket, and a tool that searches the policy corpus. The `durable=True` setting enables durable execution with caching of model call and tool responses. `HITLBranchOperator` routes the draft to send-as-drafted, respond-manually, escalate-to-on-call, or escalate-to-account-manager, defaulting to on-call escalation if no one responds within five minutes. |
| `triage_product_reviews` | An `@task.llm` classifies each review into one of six categories with a confidence score, dynamically mapped one task group per review. |

![draft_support_replies task graph: fetch_open_ticket, draft_reply, format_review_request, then the HITL branch to send_ai_reply, respond_manually, escalate_to_on_call_engineer, or escalate_to_account_manager](source/draft_support_replies_task_graph.png)

![triage_product_reviews task graph: fetch_new_reviews, then a mapped triage_and_route task group (triage_review, assign_queue, route_by_category, one notify task per queue, record_outcome), then report_routing](source/triage_product_reviews_task_graph.png)

### AI evals (`dags/ai_evals/`)

| Dag | What it does | 
|---|---|
| `evaluate_support_model_responses` | Reads `draft_support_replies` OTel traces, scores a rubric plus a comparison against a golden reply |
| `evaluate_ticket_classification` | Classifies each labelled ticket `k` times at temperature 1.0, reports pass@1, pass@k, and majority-vote accuracy | 
| `evaluate_support_threads` | Joins already-sent support replies, the customer's actual reply, and CSAT survey scores by ticket, and judges accuracy, tone, sentiment, and escalation risk. |

![evaluate_support_model_responses task graph: fetch_traces, resolve_tickets, then a mapped evaluate_reply task group scoring each reply, then load_metrics](source/evaluate_support_model_responses_task_graph.png)

![evaluate_support_threads task graph: fetch_replies_from_inbox, fetch_replies_from_helpdesk, fetch_ratings_from_survey_tool, join_by_ticket, then a mapped evaluate_thread task group, then load_metrics](source/evaluate_support_threads_task_graph.png)

![evaluate_ticket_classification task graph: fetch_labelled_tickets, build_sampling_cases, a mapped classify_ticket, then report_pass_at_k](source/evaluate_ticket_classification_task_graph.png)

### MLOps (`dags/mlops/`)

| Dag | What it does |
|---|---|
| `train_delivery_risk_model` | Scheduled on the `cosmarket_shipment_history` asset. Trains a random forest and a logistic regression across a small hyperparameter grid in parallel, dynamically mapped tasks, tracks every variant as its own run through `MlopsTracker`, and picks the best model by macro F1 |
| `score_open_shipments` | Scheduled by the `cosmarket_delivery_risk_model` asset. Loads the production model, scores every in-transit shipment, upserts `shipment_predictions`, and logs the highest-confidence at-risk shipments. |

![train_delivery_risk_model task graph: assemble_training_set, mapped train_random_forest and train_logistic_regression, select_best, visualize, register_model](source/train_delivery_risk_model_task_graph.png)

![score_open_shipments task graph: collect_open_shipments, score, load_predictions, report_at_risk](source/score_open_shipments_task_graph.png)

## Plugins

### AI Evals (`plugins/airflow-evals-plugin`)

Accessible at `/evals/ui`. Joins `support_thread_evals` with `support_threads`, `support_messages`, `customers`, and `products` to show each reply: dimension scores, confidence, token usage, cost, duration, tool calls, and a summary rate per dimension across good/acceptable/poor and low-confidence. Populated by `evaluate_support_model_responses`.

![AI Evals plugin dashboard: replies scored, average reasoning tokens, latency, and tool calls, a rate-per-dimension table, and every scored reply](source/ai_evals_plugin_dashboard.png)

### MLOps (`plugins/airflow-mlops-plugin`)

Accessible at `/mlops/ui`. An experiment tracker, run history, and model registry over `ml_experiments`, `ml_runs`, `ml_models`, and `ml_plots`, populated by `train_delivery_risk_model`. 

![MLOps plugin Experiments tab: the delivery_outcome_classification experiment's four runs, a metrics-over-time chart, and feature evolution across runs](source/mlops_plugin_registry.png)

## Schema

Schema of the duckdb after running all Dags:

| Tables | Used by |
|---|---|
| `customers`, `products` | Reference data seeded by `setup` |
| `support_threads`, `support_messages`, `support_thread_evals` | The support agent and its eval |
| `context_units` | context engineering pattern: chunk text, embedding, and source document |
| `shipments`, `shipment_features`, `shipment_labels`, `shipment_predictions` | MLOps pattern: training data, labels, and scored predictions |
| `ml_experiments`, `ml_runs`, `ml_models`, `ml_plots` | The tracker and registry the MLOps plugin reads. |

## Resources

### ETL

- [Apache Airflow 3 Best Practices for ETL/ELT Pipelines](https://www.astronomer.io/ebooks/apache-airflow-3-best-practices-etl-elt-pipelines/)
- [Orchestrating dbt with Airflow Using Cosmos](https://www.astronomer.io/ebooks/orchestrating-dbt-with-airflow-using-cosmos/)
- [Quick Notes: Data Quality](https://www.astronomer.io/ebooks/quick-notes-data-quality/)
- [ETL Workshop](https://github.com/astronomer/devrel-public-workshops/tree/workshops/astrotrips/etl)

### Context engineering

- [AI Context Engineering with Apache Airflow](https://www.astronomer.io/ebooks/ai-context-engineering-with-apache-airflow/)

### AI orchestration

- [AI Orchestration with Apache Airflow](https://www.astronomer.io/ebooks/ai-orchestration-with-apache-airflow/)
- [AI Orchestration Overview](https://www.astronomer.io/docs/learn/ai-orchestration-overview)
- [The Common AI Provider](https://www.astronomer.io/docs/learn/airflow-common-ai-provider)
- [Human-in-the-Loop Workflows](https://www.astronomer.io/docs/learn/airflow-human-in-the-loop)
- [Self-Healing Pipelines with Otto](https://www.astronomer.io/docs/learn/airflow-otto-rca-auto-fix)
- [AI Workshop](https://github.com/astronomer/devrel-public-workshops/tree/workshops/astrotrips/ai)

### AI evals

- [AI Orchestration: Model Evals](https://www.astronomer.io/docs/learn/ai-orchestration-model-evals)
- [AI Orchestration: Product Evals](https://www.astronomer.io/docs/learn/ai-orchestration-product-evals)

### MLOps

- [MLOps 101 Workshop](https://github.com/astronomer/devrel-public-workshops/tree/workshops/astrotrips/mlops)
- [MLOps and AI Workshop](https://github.com/astronomer/devrel-public-workshops/tree/workshops/astrotrips/mlops-and-ai)

### General

- [Practical Guide to Apache Airflow 3](https://www.astronomer.io/ebooks/practical-guide-to-apache-airflow-3/)
- [Dynamic Task Mapping](https://www.astronomer.io/docs/learn/dynamic-tasks)
- [Durable Execution with the Task State Store](https://www.astronomer.io/docs/learn/airflow-task-state-store)
- [Best Practices for Testing Apache Airflow DAGs](https://www.astronomer.io/ebooks/best-practices-for-testing-apache-airflow-dags/)
- [Retry Policies](https://www.astronomer.io/docs/learn/rerunning-dags#retry-policies)
- [DAG Writing Workshop](https://github.com/astronomer/devrel-public-workshops/tree/workshops/astrotrips/dag-writing)
