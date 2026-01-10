# Retail Sales ETL Pipeline with Apache Airflow, GCS & BigQuery  
*A Production-Grade Cloud Data Engineering Pipeline for Automotive Sales Data*

This project is a fully automated **ETL pipeline** that extracts daily automotive sales CSV files, stores the raw files in **Google Cloud Storage (GCS)**, cleans and processes the data, and loads it into **BigQuery** for analytics-ready consumption.

The pipeline is orchestrated end-to-end using **Cloud Composer (Managed Apache Airflow)** and can be extended to run **event-driven** on new CSV uploads.

---

## Architecture 


### **Architecture**
![DAG](Assets/architecture.png)

         ┌─────────────────────┐
         │     CSV Upload      │
         │   (raw/autos.csv)  │
         └─────────┬───────────┘
                   │
                   ▼
         ┌─────────────────────┐
         │  Google Cloud Storage│
         │  (Raw Landing Zone)  │
         └─────────┬───────────┘
                   │  Sensor + PythonOperator
                   ▼
         ┌─────────────────────┐
         │  Cleaned CSV File    │
         │ processed/autos_cleaned.csv │
         └─────────┬───────────┘
                   │
                   ▼
         ┌─────────────────────┐
         │   BigQuery Raw Table │
         │   sales_raw.raw_sales│
         └─────────────────────┘
                   │
                   ▼
         ┌─────────────────────┐
         │ Analytics & Reports │
         │ (Looker Studio / BI) │
         └─────────────────────┘

---

## Key Features

### Automated CSV Ingestion  
- Waits for new CSV files in the GCS `raw/` bucket using **GCSObjectExistenceSensor**.
- Supports automated daily ingestion or can be extended for **event-driven triggers**.

### Cloud-Orchestrated ETL with Airflow  
- Managed using **Cloud Composer**.
- Handles scheduling, retries, logging, and task orchestration.
- Tasks include **download → clean → append → upload → BigQuery load**.

### Data Cleaning & Processing  
- Cleans column names to be **BigQuery-compatible**:
  - Removes special characters
  - Converts to lowercase
  - Replaces spaces and symbols with underscores
- Appends new CSV data to the **existing cleaned CSV** in `processed/`.

### BigQuery Integration  
- Loads cleaned CSV into BigQuery in **append mode**.
- Schema is **autodetected** from the CSV.
- Enables analytics-ready queries and dashboards.

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Orchestration** | Apache Airflow (Cloud Composer) | ETL automation & scheduling |
| **Data Lake** | Google Cloud Storage | Raw and processed CSV storage |
| **Warehouse** | Google BigQuery | Querying, transformations & analytics |
| **Language** | Python | ETL logic (Pandas, GCS Client, BigQuery Client) |
| **Libraries** | Pandas, io, Google Cloud SDK | Data processing and GCS/BigQuery integration |

---

## Output Tables (BigQuery)

### **sales_raw.raw_sales**  
Stores cleaned, processed sales data with daily appends.

| Column | Description |
|--------|-------------|
| column_name | Cleaned column from raw CSV |
| ... | Other sales-specific metrics (autodetected) |

> The table schema is automatically detected from the cleaned CSV. Each new CSV uploaded **appends** to this table.

---

## Project Screenshots





### **1. Airflow DAG**
![DAG](Assets/dagster.png)


### **2. GCS Bucket**
![](Assets/bucket1.png)
![](Assets/bucket2.png)

### **3. BigQuery**
![BigQuery Raw Table](Assets/bigquery.png)


### **4. GCP Airflow Environment**
![](Assets/env1.png)
![](Assets/env2.png)


---

## ⚡ Workflow Summary

1. **Sensor Task** waits for the raw CSV to appear in `raw/autos.csv`.
2. **PythonOperator** downloads the CSV, cleans the columns, and appends new rows to `processed/autos_cleaned.csv`.
3. **PythonOperator** loads the cleaned CSV into BigQuery in **append mode**.
4. Data is ready for **analytics dashboards** or further transformations.

---



---


