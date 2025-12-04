

"""
ML Anomaly Detection Pipeline
---------------------------------------

What it does:
- Consumes `gold_applications_validated` and `gold_transaction_features` as
  streaming inputs.
- Applies simple anomaly rules:
    • Applications: req_income_ratio > 0.6
    • Transactions: txn_limit_ratio > 0.75
- Flags anomalous records (is_ml_anomaly = 1).
- Performs daily aggregations with watermarking to handle late data.
- Produces two DLT-managed Gold tables:
    • gold_app_ml_anomaly_agg     -- daily application anomaly counts
    • gold_txn_ml_anomaly_agg     -- daily transaction anomaly counts


It provides daily anomaly signals for dashboards, alerting, and trend monitoring.
"""

import dlt
from pyspark.sql.functions import *

# Anomaly threshold 
ANOMALY_THRESHOLD = 0.05 

# --- Intermediate Table: Application Anomaly Aggregation ---

@dlt.table(
    name="gold_app_ml_anomaly_agg",
    comment="Daily aggregated counts of application anomalies (Internal streaming table)."
)
def gold_app_anomaly_agg():
    return (
        dlt.read_stream("gold_applications_validated")
        .withWatermark("timestamp", "2 hours") 
        .withColumn("is_ml_anomaly", when(col("req_income_ratio") > 0.6, 1).otherwise(0))
        .filter(col("is_ml_anomaly") == 1)
        .groupBy(
            window(col("timestamp"), "1 day").alias("date_window")
        ) 
        .agg(count("*").alias("app_anomalies_count"))
        .withColumn("date", to_date(col("date_window.start"))) 
        .select("date", "app_anomalies_count")
    )


# --- Intermediate Table: Transaction Anomaly Aggregation ---

@dlt.table(
    name="gold_txn_ml_anomaly_agg",
    comment="Daily aggregated counts of transaction anomalies (Internal streaming table)."
)
def gold_txn_anomaly_agg():
    return (
        dlt.read_stream("gold_transaction_features")
        .withWatermark("timestamp", "2 hours") 
        .withColumn("is_ml_anomaly", when(col("txn_limit_ratio") > 0.75, 1).otherwise(0))
        .filter(col("is_ml_anomaly") == 1)
        .withColumn("date", to_date(col("timestamp"))) 
        .groupBy("date")
        .agg(count("*").alias("txn_anomalies_count"))
        .select("date", "txn_anomalies_count")
    )



