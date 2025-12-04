
# Data Trust Gatekeeper — End-to-End Data Quality & Anomaly Detection

## Overview
This project implements a **real-time Data Quality and Anomaly Detection pipeline** for the customer acquisition lifecycle, tracking the end-to-end journey from **visitor → application → account → transactions**. It ensures that mission-critical data published by Data Engineering is **accurate, complete, and reliable** for downstream Product and analytics teams.  

The pipeline includes:  
- **TDQ (Technical Data Quality) Checks** – schema compliance, completeness, freshness, uniqueness.  
- **BDQ (Business Data Quality) Checks** – business rule validation, cross-table consistency, plausibility checks.  
- **ML-based Anomaly Detection** – detects unusual patterns in application and transaction data using time-series and statistical methods.  
- **Gold Metrics & Dashboard** – aggregates daily metrics and anomalies into dashboards for leadership and alerting.

---

## Project Structure

| Notebook / Module | Purpose |
|------------------|---------|
| **01_Data_Simulation_and_Ingestion** | Generates simulated end-to-end customer data with injected TDQ, BDQ, and ML anomalies for testing. Writes to landing zones in Unity Catalog. |
| **Data_Governance_DLT_V0.py** | Implements the **Bronze → Silver → Gold** DLT pipeline with TDQ/BDQ checks, ML feature extraction, and KPI aggregation. |
| **03_ML_Anomaly_Detection.py** | Applies simplified ML rules to identify anomalies in applications and transactions. Produces daily anomaly scores. |
| **Dashboard Notebook** | Visualizes pipeline metrics, including TDQ/BDQ failure trends, ML anomalies, conversion rates, freshness, and uniqueness violations. |

---

## Pipeline Layers

### Bronze Layer
- Ingests raw Parquet streams from landing zones using **Databricks Autoloader**.  
- Preserves all original fields with ingestion timestamps.  
- Ensures raw data can be replayed or debugged.

### Silver Layer
- Performs **TDQ and BDQ checks**:  
  - Null and schema violations  
  - Freshness lag  
  - Business rules (age, income, credit limit)  
  - Cross-table consistency (visitor → application → approval → account)  
- Cleans and transforms data for downstream analysis.

### Gold Layer
- Aggregates metrics for **daily dashboards**:  
  - Total visits, applications, approvals, transactions  
  - Conversion and approval rates  
  - Average transaction amounts  
  - Freshness lag and schema compliance  
  - TDQ/BDQ failures and ML anomaly counts  

---

## Key Features
- **End-to-End Monitoring:** Tracks pipeline health from ingestion to reporting.  
- **Governance Metrics:** Measures compliance and completeness in near real-time.  
- **ML Anomaly Detection:** Flags unusual behaviors or potential fraud in applications and transactions.  
- **Dashboards:** Provides clear metrics for Data Engineering and leadership.  

