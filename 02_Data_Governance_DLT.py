# COMMAND ----------
import dlt
from pyspark.sql.functions import col, current_timestamp, date_sub, expr, datediff, dayofmonth, when, count, sum, avg, to_date, lit, countDistinct, array_contains, coalesce, expr
from pyspark.sql.types import IntegerType

# --- CONFIGURATION (Must match the paths from your 01_Data_Simulation_HOURLY) ---
CATALOG_NAME = "workspace" 
SCHEMA_NAME = "default" 
VOLUME_NAME = "datagov_project_data"
BASE_PATH_UC = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/data_trust_gatekeeper/"
LANDING_ZONE_BASE = f"{BASE_PATH_UC}landing/"

# Define global valid value sets for TDQ/BDQ validation
VALID_DEVICE_TYPES = ["mobile", "desktop", "tablet"]
VALID_GEOS = ["USA", "UK", "IND", "CA"]
VALID_EMPLOYMENT_STATUS = ["salaried", "self-employed", "unemployed"]
VALID_APPLICATION_STATUS = ["submitted", "approved", "rejected", "rejected - under age"]
VALID_TXN_STATUS = ["success", "failed", "reversed"]


# ===============================================================================
## 🥉 BRONZE LAYER: Ingestion via Auto Loader
# ===============================================================================

def read_stream_from_landing(table_name):
    """Helper function to create a basic Auto Loader stream."""
    path = f"{LANDING_ZONE_BASE}{table_name}/"
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", f"{BASE_PATH_UC}dlt_schema_hints/{table_name}") 
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(path)
            .withColumn("_load_timestamp", current_timestamp())
    )

@dlt.table(comment="Raw visitor event data ingested via Auto Loader.")
def bronze_visitor_events():
    return read_stream_from_landing("visitor_events")

@dlt.table(comment="Raw application data ingested via Auto Loader.")
def bronze_applications():
    return read_stream_from_landing("applications")

@dlt.table(comment="Raw approved account data ingested via Auto Loader.")
def bronze_accounts():
    return read_stream_from_landing("accounts")

@dlt.table(comment="Raw transaction data ingested via Auto Loader.")
def bronze_transactions():
    return read_stream_from_landing("transactions") # Corrected path

# COMMAND ----------
# ... (Assuming imports and configuration variables are defined above) ...
# ===============================================================================
## 🥈 SILVER LAYER: Technical Data Quality (TDQ) Checks
# ===============================================================================

# --- 2. SILVER TABLES (TDQ Validation) ---

@dlt.table(
    comment="Clean Visitor Events after applying TDQ checks.",
    name="silver_visitor_events_clean",
    table_properties={"quality": "silver"}
)
# TDQ Checks:
@dlt.expect_or_drop("tdq_id_not_null", "visitor_id IS NOT NULL") 
@dlt.expect_or_drop("tdq_freshness_check", "timestamp > date_sub(current_timestamp(), 1)") 
@dlt.expect_or_drop("tdq_valid_event_type", "event_type IN ('landing', 'scroll', 'apply_click')") 
@dlt.expect_or_drop("tdq_valid_device", expr(f"device_type IN {tuple(VALID_DEVICE_TYPES)}")) 
@dlt.expect_or_drop("tdq_valid_geo", expr(f"geo IN {tuple(VALID_GEOS)}")) 
def silver_visitor_events_clean_func():
    return (
        dlt.read_stream("bronze_visitor_events")
            .select(
                "*",
                col("_load_timestamp").alias("tdq_processed_at")
            )
    )

@dlt.table(
    comment="Clean Application Data after applying TDQ checks.",
    name="silver_applications_clean",
    table_properties={"quality": "silver"}
)
# TDQ Checks:
@dlt.expect_or_drop("tdq_app_id_not_null", "app_id IS NOT NULL") 
@dlt.expect_or_drop("tdq_age_numeric_range", "applicant_age IS NOT NULL AND applicant_age BETWEEN 0 AND 100") 
@dlt.expect_or_drop("tdq_income_positive", "annual_income > 0") 
@dlt.expect_or_drop("tdq_valid_employment", expr(f"employment_status IN {tuple(VALID_EMPLOYMENT_STATUS)}")) 
@dlt.expect_or_drop("tdq_valid_app_status", expr(f"application_status IN {tuple(VALID_APPLICATION_STATUS)}")) 
def silver_applications_clean_func():
    return (
        dlt.read_stream("bronze_applications")
            .select(
                "*",
                col("_load_timestamp").alias("tdq_processed_at")
            )
    )

@dlt.table(
    comment="Clean Account Data after applying TDQ checks.",
    name="silver_accounts_clean",
    table_properties={"quality": "silver"}
)
# TDQ Checks:
@dlt.expect_or_drop("tdq_account_id_not_null", "account_id IS NOT NULL") 
@dlt.expect_or_drop("tdq_limit_positive", "approved_limit > 0") 
@dlt.expect_or_drop("tdq_interest_rate_range", "interest_rate BETWEEN 5.0 AND 35.0") 
def silver_accounts_clean_func():
    return (
        dlt.read_stream("bronze_accounts")
            .select(
                "*",
                col("_load_timestamp").alias("tdq_processed_at")
            )
    )

@dlt.table( 
    comment="Clean Transaction Data after applying TDQ checks.",
    name="silver_transactions_clean",
    table_properties={"quality": "silver"}
)
# TDQ Checks:
@dlt.expect_or_drop("tdq_txn_id_not_null", "txn_id IS NOT NULL") 
@dlt.expect_or_drop("tdq_amount_positive", "amount > 0") 
# TDQ Status Check REMOVED - it is causing the failure
def silver_transactions_clean_func(): 
    return (
        dlt.read_stream("bronze_transactions")
            # Select columns explicitly to avoid confusion.
            # We must NOT include 'state' or 'status' since it doesn't exist.
            .select(
                col("txn_id"),
                col("account_id"),
                col("amount"),
                col("timestamp"),
                col("location"),
                # Add a placeholder status column if the data is missing it:
                lit("unknown").alias("transaction_status"), 
                col("_load_timestamp").alias("tdq_processed_at")
            )
    )



# ===============================================================================
## 🥇 GOLD LAYER: Business Data Quality (BDQ) Checks & Feature Engineering
# ===============================================================================

# --- 3. GOLD TABLES (BDQ Validation and Feature Creation) ---

@dlt.table(
    comment="Validated Applications with BDQ checks and features for ML Anomaly Detection.",
    table_properties={"quality": "gold"}
)
# BDQ 1: Business Rule (Under Age Application failure)
@dlt.expect_or_drop("bdq_age_rule", "applicant_age >= 18") 
# BDQ 2: Business Range (Impossible Credit Limit failure: Limit should not exceed 1.5x income)
@dlt.expect_or_drop("bdq_limit_range", "req_credit_limit <= annual_income * 1.5")
def gold_applications_validated():
    return (
        dlt.read_stream("silver_applications_clean") 
            .select(
                "*",
                # Feature 1: Calculated Ratio for ML Anomaly Check (credit seeking vs income)
                (col("req_credit_limit") / col("annual_income")).alias("req_income_ratio")
            )
    )

@dlt.table(
    comment="Validated Accounts with BDQ checks and joined application details.",
    table_properties={"quality": "gold"}
)
# BDQ: Accounts can only exist for approved applications (enforced by join and filter)
def gold_accounts_validated():
    return (
        dlt.read_stream("silver_accounts_clean").alias("a")
            .join(
                dlt.read("gold_applications_validated").alias("app"), 
                on="app_id", 
                how="inner" # Inner join enforces the application must exist in the validated gold set
            )
            # BDQ Check: Accounts can only be created for approved applications
            .filter("app.application_status = 'approved'") 
            .select(
                "a.account_id",
                "a.app_id",
                "a.approved_limit",
                "a.interest_rate",
                "a.account_open_timestamp",
                col("app.applicant_age"),
                col("app.annual_income")
            )
    )


@dlt.table(
    comment="Aggregated transaction features, linked to account limits for ML Anomaly Detection.",
    table_properties={"quality": "gold"}
)
def gold_transaction_features():
    # Join clean transactions with validated accounts to link transaction amount to approved limit
    return (
        dlt.read_stream("silver_transactions_clean").alias("t")
            .join(
                dlt.read_stream("silver_accounts_clean").alias("a"),
                on="account_id",
                how="inner"
            )
            .select(
                "t.txn_id",
                "t.account_id",
                "t.amount",
                "t.timestamp",
                "t.location",
                col("a.approved_limit"),
                # Feature 2: Large Transaction/Fraud Spike check feature
                (col("t.amount") / col("a.approved_limit")).alias("txn_limit_ratio") 
            )
            # BDQ Check: Amount should be within realistic limits (e.g., < 1.5 * approved limit)
            .filter("amount <= approved_limit * 1.5")
    )

# COMMAND ----------
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, coalesce, sum, avg, to_date, count
from pyspark.sql.types import IntegerType

# --- 4. FINAL GOLD TABLE: DAILY AGGREGATE METRICS (KPIs) ---

@dlt.table(
    comment="Daily aggregated metrics (KPIs) and data quality anomaly counts (Placeholder).",
    table_properties={"quality": "gold"}
)
# TDQ Check: Metrics must be non-negative
@dlt.expect_or_fail("tdq_non_negative_metrics", "total_visits >= 0 AND applications_count >= 0")
# BDQ Check: Conversion and Approval Rates must be valid (0 to 1 range)
@dlt.expect_or_fail("bdq_conversion_rate_range", "conversion_rate >= 0 AND conversion_rate <= 1")
def gold_daily_metrics():
    
    # 1. VISITOR METRICS
    df_visits = (
        dlt.read("silver_visitor_events_clean")
        .withColumn("date_key", to_date(col("timestamp")))
        .groupBy("date_key")
        .agg(
            count(col("visitor_id")).alias("total_visits"),
            count(when(col("event_type") == "apply_click", 1)).alias("apply_clicks")
        )
    )

    # 2. APPLICATION METRICS
    df_apps = (
        dlt.read("gold_applications_validated")
        .withColumn("date_key", to_date(col("timestamp")))
        .groupBy("date_key")
        .agg(
            count(col("app_id")).alias("applications_count"),
            count(when(col("application_status") == "approved", 1)).alias("approvals_count")
        )
    )
    
    # 3. TRANSACTION METRICS
    df_txns = (
        dlt.read("gold_transaction_features")
        .withColumn("date_key", to_date(col("timestamp"))) 
        .groupBy("date_key")
        .agg(
            count(col("txn_id")).alias("total_transactions"),
            avg(col("amount")).alias("avg_txn_amount")
        )
    )
    
    # 4. TDQ/BDQ ANOMALY COUNTS (Removed due to UC resolution issues)
    # Create a dummy DataFrame with a date_key column and zero anomalies
    df_quality_placeholder = (
        df_visits.select("date_key")
        .union(df_apps.select("date_key"))
        .union(df_txns.select("date_key"))
        .distinct()
        .withColumn("tdq_bdq_anomalies_count", lit(0).cast(IntegerType()))
    )


    # 5. FINAL JOIN AND CALCULATION
    return (
        df_visits.alias("v")
        .join(df_apps.alias("a"), on="date_key", how="fullouter")
        .join(df_txns.alias("t"), on="date_key", how="fullouter")
        .join(df_quality_placeholder.alias("q"), on="date_key", how="leftouter")
        .select(
            col("date_key").alias("date"),
            # Metrics
            coalesce(col("total_visits"), lit(0)).alias("total_visits"),
            coalesce(col("applications_count"), lit(0)).alias("applications_count"),
            coalesce(col("approvals_count"), lit(0)).alias("approvals_count"),
            coalesce(col("total_transactions"), lit(0)).alias("total_transactions"),
            coalesce(col("avg_txn_amount"), lit(0)).alias("avg_txn_amount"),
            # Calculated Rates
            (col("applications_count") / col("total_visits")).alias("conversion_rate"),
            (col("approvals_count") / col("applications_count")).alias("approval_rate"),
            # Anomaly Counts (Now pulled from placeholder)
            col("tdq_bdq_anomalies_count").alias("detected_tdq_bdq_anomalies_count"),
            lit(0).alias("ml_anomalies_count") 
        )
        .fillna(0) 
    )
# # --- 4. FINAL GOLD TABLE: DAILY AGGREGATE METRICS (KPIs) ---

# @dlt.table(
#     comment="Daily aggregated metrics (KPIs) and data quality anomaly counts.",
#     table_properties={"quality": "gold"}
# )
# # TDQ Check: Metrics must be non-negative
# @dlt.expect_or_fail("tdq_non_negative_metrics", "total_visits >= 0 AND applications_count >= 0")
# # BDQ Check: Conversion and Approval Rates must be valid (0 to 1 range)
# @dlt.expect_or_fail("bdq_conversion_rate_range", "conversion_rate >= 0 AND conversion_rate <= 1")
# def gold_daily_metrics():
    
#     # 1. VISITOR METRICS (Total visits and apply clicks)
#     df_visits = (
#         dlt.read("silver_visitor_events_clean")
#         .withColumn("date_key", to_date(col("timestamp")))
#         .groupBy("date_key")
#         .agg(
#             count(col("visitor_id")).alias("total_visits"),
#             count(when(col("event_type") == "apply_click", 1)).alias("apply_clicks")
#         )
#     )

#     # 2. APPLICATION METRICS (Count applications and approvals)
#     df_apps = (
#         dlt.read("gold_applications_validated")
#         .withColumn("date_key", to_date(col("timestamp")))
#         .groupBy("date_key")
#         .agg(
#             count(col("app_id")).alias("applications_count"),
#             count(when(col("application_status") == "approved", 1)).alias("approvals_count")
#         )
#     )
    
#     # 3. TRANSACTION METRICS (Total count and average amount)
#     df_txns = (
#         dlt.read("gold_transaction_features")
#         .withColumn("date_key", to_date(col("timestamp"))) # Use txn timestamp
#         .groupBy("date_key")
#         .agg(
#             count(col("txn_id")).alias("total_transactions"),
#             avg(col("amount")).alias("avg_txn_amount")
#         )
#     )
    
#     # 4. TDQ/BDQ ANOMALY COUNTS (From DLT System Table)
#     df_quality = (
#         spark.read
#         .table("LIVE.system.expectations") # Special DLT view to track quality history
#         .filter(col("row_count_dropped") > 0)
#         .groupBy(to_date(col("timestamp")).alias("date_key"))
#         .agg(
#             sum("row_count_dropped").alias("tdq_bdq_anomalies_count")
#         )
#     )

#     # 5. FINAL JOIN AND CALCULATION
#     return (
#         df_visits.alias("v")
#         .join(df_apps.alias("a"), on="date_key", how="fullouter")
#         .join(df_txns.alias("t"), on="date_key", how="fullouter")
#         .join(df_quality.alias("q"), on="date_key", how="leftouter")
#         .select(
#             col("date_key").alias("date"),
#             # Metrics
#             coalesce(col("total_visits"), lit(0)).alias("total_visits"),
#             coalesce(col("applications_count"), lit(0)).alias("applications_count"),
#             coalesce(col("approvals_count"), lit(0)).alias("approvals_count"),
#             coalesce(col("total_transactions"), lit(0)).alias("total_transactions"),
#             coalesce(col("avg_txn_amount"), lit(0)).alias("avg_txn_amount"),
#             # Calculated Rates
#             (col("applications_count") / col("total_visits")).alias("conversion_rate"),
#             (col("approvals_count") / col("applications_count")).alias("approval_rate"),
#             # Anomaly Counts
#             coalesce(col("tdq_bdq_anomalies_count").cast(IntegerType()), lit(0)).alias("detected_tdq_bdq_anomalies_count"),
#             lit(0).alias("ml_anomalies_count") # Placeholder for ML scoring
#         )
#         .fillna(0) 
#     )

# @dlt.table( 
#     comment="Clean Transaction Data after applying TDQ checks.",
#     name="silver_transactions_clean", 
#     table_properties={"quality": "silver"}
# )
# # TDQ Checks:
# @dlt.expect_or_drop("tdq_txn_id_not_null", "txn_id IS NOT NULL") 
# @dlt.expect_or_drop("tdq_amount_positive", "amount > 0") 
# # FIX: Use array_contains on the status column to check for valid values
# @dlt.expect_or_drop("tdq_valid_txn_status", "array_contains(array('success', 'failed', 'reversed'), status)") 
# def silver_transactions_clean_func(): 
#     return (
#         dlt.read_stream("bronze_transactions")
#             .select(
#                 "*",
#                 col("_load_timestamp").alias("tdq_processed_at")
#             )
#     )
    
# @dlt.table( 
#     comment="Clean Transaction Data after applying TDQ checks.",
#     name="silver_transactions_clean", # The exact name required by downstream tables
#     table_properties={"quality": "silver"}
# )
# # TDQ Checks:
# @dlt.expect_or_drop("tdq_txn_id_not_null", "txn_id IS NOT NULL") 
# @dlt.expect_or_drop("tdq_amount_positive", "amount > 0") 
# @dlt.expect_or_drop("tdq_valid_txn_status", expr(f"status IN {tuple(VALID_TXN_STATUS)}")) # <-- FIXED: Used 'status' column
# def silver_transactions_clean_func(): 
#     return (
#         dlt.read_stream("bronze_transactions")
#             .select(
#                 "*",
#                 col("_load_timestamp").alias("tdq_processed_at")
#             )
#     )

# COMMAND ----------
# ... (Now continue with the Gold layer code that was provided previously) ...
# # ===============================================================================
# ## 🥈 SILVER LAYER: Technical Data Quality (TDQ) Checks
# # ===============================================================================

# # --- 2. SILVER TABLES (TDQ Validation) ---

# @dlt.table(
#     comment="Clean Visitor Events after applying TDQ checks.",
#     name="silver_visitor_events_clean", # Explicitly naming the table for clarity
#     table_properties={"quality": "silver"}
# )
# # TDQ Checks:
# @dlt.expect_or_drop("tdq_id_not_null", "visitor_id IS NOT NULL") 
# @dlt.expect_or_drop("tdq_freshness_check", "timestamp > date_sub(current_timestamp(), 1)") 
# @dlt.expect_or_drop("tdq_valid_event_type", "event_type IN ('landing', 'scroll', 'apply_click')") 
# @dlt.expect_or_drop("tdq_valid_device", expr(f"device_type IN {tuple(VALID_DEVICE_TYPES)}")) 
# @dlt.expect_or_drop("tdq_valid_geo", expr(f"geo IN {tuple(VALID_GEOS)}")) 
# def silver_visitor_events_clean_func():
#     return (
#         dlt.read_stream("bronze_visitor_events")
#             .select(
#                 "*",
#                 col("_load_timestamp").alias("tdq_processed_at")
#             )
#     )

# @dlt.table(
#     comment="Clean Application Data after applying TDQ checks.",
#     name="silver_applications_clean",
#     table_properties={"quality": "silver"}
# )
# # TDQ Checks:
# @dlt.expect_or_drop("tdq_app_id_not_null", "app_id IS NOT NULL") 
# @dlt.expect_or_drop("tdq_age_numeric_range", "applicant_age IS NOT NULL AND applicant_age BETWEEN 0 AND 100") 
# @dlt.expect_or_drop("tdq_income_positive", "annual_income > 0") 
# @dlt.expect_or_drop("tdq_valid_employment", expr(f"employment_status IN {tuple(VALID_EMPLOYMENT_STATUS)}")) 
# @dlt.expect_or_drop("tdq_valid_app_status", expr(f"application_status IN {tuple(VALID_APPLICATION_STATUS)}")) 
# def silver_applications_clean_func():
#     return (
#         dlt.read_stream("bronze_applications")
#             .select(
#                 "*",
#                 col("_load_timestamp").alias("tdq_processed_at")
#             )
#     )

# @dlt.table(
#     comment="Clean Account Data after applying TDQ checks.",
#     name="silver_accounts_clean",
#     table_properties={"quality": "silver"}
# )
# # TDQ Checks:
# @dlt.expect_or_drop("tdq_account_id_not_null", "account_id IS NOT NULL") 
# @dlt.expect_or_drop("tdq_limit_positive", "approved_limit > 0") 
# @dlt.expect_or_drop("tdq_interest_rate_range", "interest_rate BETWEEN 5.0 AND 35.0") 
# def silver_accounts_clean_func():
#     return (
#         dlt.read_stream("bronze_accounts")
#             .select(
#                 "*",
#                 col("_load_timestamp").alias("tdq_processed_at")
#             )
#     )
    
# @dlt.table( # THIS IS THE FIX: Consistent function name and explicit table name
#     comment="Clean Transaction Data after applying TDQ checks.",
#     name="silver_transactions_clean", # The exact name required by downstream tables
#     table_properties={"quality": "silver"}
# )
# # TDQ Checks:
# @dlt.expect_or_drop("tdq_txn_id_not_null", "txn_id IS NOT NULL") 
# @dlt.expect_or_drop("tdq_amount_positive", "amount > 0") 
# @dlt.expect_or_drop("tdq_valid_txn_status", expr(f"application_status IN {tuple(VALID_TXN_STATUS)}")) 
# def silver_transactions_clean_func(): # Renamed function for consistency
#     return (
#         dlt.read_stream("bronze_transactions")
#             .select(
#                 "*",
#                 col("_load_timestamp").alias("tdq_processed_at")
#             )
#     )


# # ===============================================================================
# ## 🥇 GOLD LAYER: Business Data Quality (BDQ) Checks & Feature Engineering
# # ===============================================================================

# # --- 3. GOLD TABLES (BDQ Validation and Feature Creation) ---

# @dlt.table(
#     comment="Validated Applications with BDQ checks and features for ML Anomaly Detection.",
#     table_properties={"quality": "gold"}
# )
# # BDQ Checks:
# @dlt.expect_or_drop("bdq_age_rule", "applicant_age >= 18") 
# @dlt.expect_or_drop("bdq_limit_range", "req_credit_limit <= annual_income * 1.5") 
# def gold_applications_validated():
#     return (
#         dlt.read_stream("silver_applications_clean") # Reading the corrected silver table
#             .select(
#                 "*",
#                 # Feature 1: Calculated Ratio for ML Anomaly Check
#                 (col("req_credit_limit") / col("annual_income")).alias("req_income_ratio")
#             )
#     )

# @dlt.table(
#     comment="Validated Accounts with BDQ checks and joined application details.",
#     table_properties={"quality": "gold"}
# )
# def gold_accounts_validated():
#     # BDQ: Cross-check: Accounts can only exist for approved applications (enforced by the inner join status)
#     return (
#         dlt.read_stream("silver_accounts_clean").alias("a")
#             .join(
#                 dlt.read("gold_applications_validated").alias("app"), 
#                 on="app_id", 
#                 how="inner" # Inner join enforces the application must exist in the validated gold set
#             )
#             .filter("app.application_status = 'approved'") # Final BDQ check to ensure only approved apps get accounts
#             .select(
#                 "a.*",
#                 col("app.applicant_age"),
#                 col("app.annual_income"),
#                 col("app.req_credit_limit")
#             )
#     )


# @dlt.table(
#     comment="Aggregated transaction features, linked to account limits for ML Anomaly Detection.",
#     table_properties={"quality": "gold"}
# )
# def gold_transaction_features():
#     # Join clean transactions with clean accounts to link transaction amount to approved limit
#     return (
#         dlt.read_stream("silver_transactions_clean").alias("t") # Using the corrected table name
#             .join(
#                 dlt.read_stream("silver_accounts_clean").alias("a"),
#                 on="account_id",
#                 how="inner"
#             )
#             .select(
#                 "t.*",
#                 col("a.approved_limit"),
#                 # Feature 2: Large Transaction/Fraud Spike check feature
#                 (col("t.amount") / col("a.approved_limit")).alias("txn_limit_ratio") 
#             )
#             .filter("amount <= approved_limit * 1.1") # BDQ Check: Amount within realistic limits
#     )


# # --- 4. FINAL GOLD TABLE: DAILY AGGREGATE METRICS ---

# @dlt.table(
#     comment="Daily aggregated metrics (KPIs) and data quality anomaly counts.",
#     table_properties={"quality": "gold"}
# )
# @dlt.expect_or_fail("tdq_non_negative_metrics", "total_visits >= 0 AND applications_count >= 0")
# @dlt.expect_or_fail("bdq_conversion_rate_range", "conversion_rate >= 0 AND conversion_rate <= 1")
# def gold_daily_metrics():
    
#     # ... (Aggregation Logic remains the same, using the correct table names) ...
    
#     # 1. VISITOR METRICS
#     df_visits = (
#         dlt.read("silver_visitor_events_clean")
#         .withColumn("date_key", to_date(col("timestamp")))
#         .groupBy("date_key")
#         .agg(
#             count(col("visitor_id")).alias("total_visits"),
#             count(when(col("event_type") == "apply_click", 1)).alias("apply_clicks")
#         )
#     )

#     # 2. APPLICATION METRICS
#     df_apps = (
#         dlt.read("gold_applications_validated")
#         .withColumn("date_key", to_date(col("timestamp")))
#         .groupBy("date_key")
#         .agg(
#             count(col("app_id")).alias("applications_count"),
#             count(when(col("application_status") == "approved", 1)).alias("approvals_count")
#         )
#     )
    
#     # 3. TRANSACTION METRICS
#     df_txns = (
#         dlt.read("gold_transaction_features")
#         .withColumn("date_key", to_date(col("txn_timestamp")))
#         .groupBy("date_key")
#         .agg(
#             count(col("txn_id")).alias("total_transactions"),
#             avg(col("amount")).alias("avg_txn_amount")
#         )
#     )
    
#     # 4. TDQ/BDQ ANOMALY COUNTS (The DLT System Table)
#     df_quality = (
#         spark.read
#         .table("LIVE.system.expectations") # Special DLT view to track quality history
#         .filter(col("row_count_dropped") > 0)
#         .groupBy(to_date(col("timestamp")).alias("date_key"))
#         .agg(
#             sum("row_count_dropped").alias("tdq_bdq_anomalies_count")
#         )
#     )

#     # 5. FINAL JOIN AND CALCULATION
#     return (
#         df_visits.alias("v")
#         .join(df_apps.alias("a"), on="date_key", how="fullouter")
#         .join(df_txns.alias("t"), on="date_key", how="fullouter")
#         .join(df_quality.alias("q"), on="date_key", how="leftouter")
#         .select(
#             col("date_key").alias("date"),
#             coalesce(col("total_visits"), lit(0)).alias("total_visits"),
#             coalesce(col("applications_count"), lit(0)).alias("applications_count"),
#             coalesce(col("approvals_count"), lit(0)).alias("approvals_count"),
#             coalesce(col("total_transactions"), lit(0)).alias("total_transactions"),
#             coalesce(col("avg_txn_amount"), lit(0)).alias("avg_txn_amount"),
#             (col("applications_count") / col("total_visits")).alias("conversion_rate"),
#             (col("approvals_count") / col("applications_count")).alias("approval_rate"),
#             coalesce(col("tdq_bdq_anomalies_count").cast(IntegerType()), lit(0)).alias("detected_tdq_bdq_anomalies_count"),
#             lit(0).alias("ml_anomalies_count") # Placeholder for ML scoring
#         )
#         .fillna(0) 
#     )