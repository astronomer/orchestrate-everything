CREATE TABLE IF NOT EXISTS customers (
  customer_id   VARCHAR PRIMARY KEY,
  full_name     VARCHAR,
  home_port     VARCHAR,
  customer_tier VARCHAR
);

CREATE TABLE IF NOT EXISTS products (
  product_sku         VARCHAR PRIMARY KEY,
  product_name        VARCHAR,
  description         VARCHAR,
  base_price_credits  DOUBLE,
  product_id          INTEGER,
  category_id         INTEGER,
  weight_kg           DOUBLE,
  dimensions_cm       VARCHAR,
  is_hazardous        BOOLEAN,
  requires_cold_chain BOOLEAN,
  is_active           BOOLEAN
);

CREATE TABLE IF NOT EXISTS support_threads (
  thread_id    VARCHAR PRIMARY KEY,
  ticket_id    VARCHAR,
  customer_id  VARCHAR,
  product_sku  VARCHAR,
  subject      VARCHAR,
  resolved     BOOLEAN,
  reopened     BOOLEAN,
  csat_average DOUBLE,
  detractor    BOOLEAN,
  created_at   TIMESTAMP DEFAULT current_timestamp,
  updated_at   TIMESTAMP DEFAULT current_timestamp,
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  FOREIGN KEY (product_sku) REFERENCES products(product_sku)
);

CREATE TABLE IF NOT EXISTS support_messages (
  message_id VARCHAR PRIMARY KEY,
  thread_id  VARCHAR,
  turn       INTEGER,
  direction  VARCHAR,
  sender     VARCHAR,
  subject    VARCHAR,
  body       VARCHAR,
  context_used JSON,
  created_at TIMESTAMP DEFAULT current_timestamp,
  updated_at TIMESTAMP DEFAULT current_timestamp,
  FOREIGN KEY (thread_id) REFERENCES support_threads(thread_id)
);

CREATE TABLE IF NOT EXISTS support_thread_evals (
  eval_id       VARCHAR PRIMARY KEY,
  thread_id     VARCHAR,
  turn          INTEGER,
  trace_id      VARCHAR,
  span_id       VARCHAR,
  model         VARCHAR,
  input_tokens      INTEGER,
  output_tokens     INTEGER,
  reasoning_tokens  INTEGER,
  cost              DOUBLE,
  duration_ms       DOUBLE,
  tool_calls        JSON,
  tools_available   JSON,
  scores        JSON,
  scored_reply  VARCHAR,
  scored_at     TIMESTAMP DEFAULT current_timestamp,
  FOREIGN KEY (thread_id) REFERENCES support_threads(thread_id)
);

CREATE TABLE IF NOT EXISTS context_units (
  chunk_id        VARCHAR PRIMARY KEY,
  source_uri      VARCHAR,
  document_title  VARCHAR,
  title           VARCHAR,
  body            VARCHAR,
  ordinal         INTEGER,
  chars           INTEGER,
  checksum        VARCHAR,
  embedding       FLOAT[],
  embedding_model VARCHAR,
  created_at      TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS shipments (
  shipment_id           VARCHAR PRIMARY KEY,
  product_sku           VARCHAR,
  customer_id           VARCHAR,
  destination_station   VARCHAR,
  carrier_tier          VARCHAR,
  transfer_distance_gm  DOUBLE,
  solar_storm_index     DOUBLE,
  warehouse_queue_depth INTEGER,
  priority_class        VARCHAR,
  shipped_at            TIMESTAMP,
  status                VARCHAR,
  FOREIGN KEY (product_sku) REFERENCES products(product_sku),
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE IF NOT EXISTS shipment_features (
  shipment_id           VARCHAR PRIMARY KEY,
  weight_kg             DOUBLE,
  volume_l              DOUBLE,
  base_price_credits    DOUBLE,
  category_id           INTEGER,
  is_hazardous          BOOLEAN,
  requires_cold_chain   BOOLEAN,
  transfer_distance_gm  DOUBLE,
  solar_storm_index     DOUBLE,
  warehouse_queue_depth INTEGER,
  destination_station   VARCHAR,
  carrier_tier          VARCHAR,
  priority_class        VARCHAR,
  FOREIGN KEY (shipment_id) REFERENCES shipments(shipment_id)
);

CREATE TABLE IF NOT EXISTS shipment_labels (
  shipment_id      VARCHAR PRIMARY KEY,
  delivery_outcome VARCHAR,
  FOREIGN KEY (shipment_id) REFERENCES shipments(shipment_id)
);

CREATE TABLE IF NOT EXISTS shipment_predictions (
  prediction_id       VARCHAR PRIMARY KEY,
  shipment_id         VARCHAR,
  model_name          VARCHAR,
  model_version       INTEGER,
  predicted_outcome   VARCHAR,
  confidence          DOUBLE,
  class_probabilities JSON,
  scored_at           TIMESTAMP DEFAULT current_timestamp,
  FOREIGN KEY (shipment_id) REFERENCES shipments(shipment_id)
);

CREATE TABLE IF NOT EXISTS ml_experiments (
  experiment_id   INTEGER PRIMARY KEY,
  experiment_name VARCHAR UNIQUE,
  description     VARCHAR
);

CREATE TABLE IF NOT EXISTS ml_runs (
  run_id          INTEGER PRIMARY KEY,
  experiment_id   INTEGER,
  dag_id          VARCHAR,
  task_id         VARCHAR,
  status          VARCHAR,
  hyperparameters JSON,
  metrics         JSON,
  tags            JSON,
  run_ts          TIMESTAMP DEFAULT current_timestamp,
  run_number      INTEGER,
  FOREIGN KEY (experiment_id) REFERENCES ml_experiments(experiment_id)
);

CREATE TABLE IF NOT EXISTS ml_models (
  model_name    VARCHAR,
  model_version INTEGER,
  run_id        INTEGER,
  model_type    VARCHAR,
  stage         VARCHAR,
  model_blob    VARCHAR,
  staged_at     TIMESTAMP,
  PRIMARY KEY (model_name, model_version),
  FOREIGN KEY (run_id) REFERENCES ml_runs(run_id)
);

CREATE TABLE IF NOT EXISTS ml_plots (
  plot_id   INTEGER PRIMARY KEY,
  run_id    INTEGER,
  plot_name VARCHAR,
  plot_type VARCHAR,
  plot_data VARCHAR,
  FOREIGN KEY (run_id) REFERENCES ml_runs(run_id)
);

CREATE SEQUENCE IF NOT EXISTS seq_ml_experiment_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_ml_run_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_ml_plot_id START 1;
