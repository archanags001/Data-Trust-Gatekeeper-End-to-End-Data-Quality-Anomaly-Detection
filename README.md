
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


## Project Structure: `data_trust_gatekeeper`

data_trust_gatekeeper/
│
├── Data_Simulation_and_Ingestion.ipynb
├── Data_Governance_Pipeline/
│ └── Data_Governance_DLT_V0.py (Implements the Bronze → Silver → Gold DLT pipeline)
├── ML_Anomaly_Detection_pipeline/
│ └── ML_Anomaly_Detection.py (Applies simplified ML rules to detect anomalies in applications and transactions.)
│ 
│
└── Data_Trust_Gatekeeper_Dashboard.ipynb

## How to Run

1. **Setup Workspace**
   - Create a folder in your Databricks workspace for the project.

2. **Upload Notebooks**
   - Upload the following notebooks to the folder:
     - `Data_Simulation_and_Ingestion.ipynb`
     - `Data_Trust_Gatekeeper_Dashboard.ipynb`

3. **Run Data Simulation**
   - Execute the entire `Data_Simulation_and_Ingestion.ipynb`.
   - **Note:** This may take a few minutes as it generates simulated datasets.

4. **Create and Run ETL Pipelines** Creating ETL pipelines automates the end-to-end data processing, ensuring consistent ingestion, validation, and anomaly detection in production.
   1. Navigate to your main folder in Databricks.
   2. Click **Create → ETL Pipeline**.
   3. Select an empty file and the folder , then give your pipeline a name.
   4. **Import and run the first pipeline:then in pipeline settings go to Advanced settings → select Edit Advanced settings then -- choose similar to the image below --save, and run the first pipeline:**
      - `Data_Governance_DLT_V0.py`
      - <img width="524" height="345" alt="image" src="https://github.com/user-attachments/assets/c927f926-a651-43ec-b108-9620b596d1f2" />

   5. **Import and run the second pipeline:**
      - `ML_Anomaly_Detection.py`
      - **Ensure the `Data_Governance_DLT_V0` pipeline completes successfully before running this pipeline.**

5. **Run the Dashboard**
   - Once both pipelines are completed, open and run `Data_Trust_Gatekeeper_Dashboard.ipynb` to visualize the KPIs, anomalies, and data governance metrics.

