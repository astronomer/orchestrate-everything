from airflow.sdk import Asset

SHIPMENT_HISTORY = Asset("cosmarket_shipment_history")
MODEL_REGISTERED = Asset("cosmarket_delivery_risk_model")
SHIPMENTS_SCORED = Asset("cosmarket_shipment_predictions")
