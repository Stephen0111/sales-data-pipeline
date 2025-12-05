from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from google.cloud import storage, bigquery
import pandas as pd
from io import StringIO

# -----------------------------
# CONFIG
# -----------------------------
PROJECT_ID = "sales-data-pipeline-480101"
BUCKET_NAME = "sales-data-pipeline-bucket"
DATASET = "sales_raw"
TABLE = "raw_sales"

RAW_FILE_PATH = "raw/autos.csv"
CLEANED_FILE_PATH = "processed/autos_cleaned.csv"


# ----------------------------------
# 1. Clean column names function
# ----------------------------------
def clean_column_names(df):
    df.columns = (
        df.columns
        .str.replace(r"[^0-9a-zA-Z_]+", "_", regex=True)
        .str.lower()
    )
    return df


# ----------------------------------
# 2. Task: Download CSV, clean it, append to cleaned CSV
# ----------------------------------
def upload_cleaned_csv(**context):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    raw_blob = bucket.blob(RAW_FILE_PATH)
    cleaned_blob = bucket.blob(CLEANED_FILE_PATH)

    # Read the new CSV into Pandas
    csv_data = raw_blob.download_as_text()
    df_new = pd.read_csv(StringIO(csv_data))
    df_new = clean_column_names(df_new)

    # Check if a cleaned CSV already exists
    if cleaned_blob.exists():
        existing_csv = cleaned_blob.download_as_text()
        df_existing = pd.read_csv(StringIO(existing_csv))
        # Append new data to existing data
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new

    # Upload the combined cleaned CSV to GCS
    cleaned_blob.upload_from_string(df_combined.to_csv(index=False), content_type="text/csv")

    return CLEANED_FILE_PATH


# ----------------------------------
# 3. Task: Load cleaned file into BigQuery
# ----------------------------------
def load_file_to_bq(**context):
    client = bigquery.Client()

    # Pull the cleaned CSV path from the previous task
    cleaned_path = context["ti"].xcom_pull(task_ids="upload_cleaned_csv")
    if not cleaned_path:
        raise ValueError("No cleaned file path found in XCom from upload_cleaned_csv task.")

    uri = f"gs://{BUCKET_NAME}/{cleaned_path}"
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()  # Waits for job to complete


# ----------------------------------
# DAG DEFINITION
# ----------------------------------
with DAG(
    "retail_sales_scheduled_ingestion",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Sensor: Wait until raw CSV exists in GCS
    wait_for_raw_file = GCSObjectExistenceSensor(
        task_id="wait_for_raw_file",
        bucket=BUCKET_NAME,
        object=RAW_FILE_PATH,
        poke_interval=60,      # check every 60 seconds
        timeout=3600,          # fail after 1 hour if file doesn't appear
        mode="poke",
    )

    upload_clean = PythonOperator(
        task_id="upload_cleaned_csv",
        python_callable=upload_cleaned_csv,
        provide_context=True,
    )

    load_to_bq = PythonOperator(
        task_id="load_file_to_bq",
        python_callable=load_file_to_bq,
        provide_context=True,
    )

    # DAG sequence
    wait_for_raw_file >> upload_clean >> load_to_bq
