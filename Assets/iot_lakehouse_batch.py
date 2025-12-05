from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.utils.dates import days_ago
import json
import random
import time
import boto3

# -------------------------------
# CONFIGURATION
# -------------------------------
REGION = "us-east-1"
S3_BUCKET = "my-iot-lakehouse"
RAW_PREFIX = "raw/"
KINESIS_STREAM = "IoTSensorStream"

# AWS Clients
s3_client = boto3.client("s3", region_name=REGION)
kinesis_client = boto3.client("kinesis", region_name=REGION)

# -------------------------------
# FUNCTIONS
# -------------------------------

def simulate_iot_sensor_data(**kwargs):
    """
    Generate fake IoT sensor data and publish to Kinesis.
    """
    sensor_data = {
        "equipment_id": f"EQ-{random.randint(1,10)}",
        "temperature": round(random.uniform(20, 100), 2),
        "pressure": round(random.uniform(1, 5), 2),
        "vibration": round(random.uniform(0.1, 5), 2),
        "timestamp": int(time.time())
    }

    # Publish to Kinesis Data Stream
    kinesis_client.put_record(
        StreamName=KINESIS_STREAM,
        Data=json.dumps(sensor_data),
        PartitionKey=sensor_data["equipment_id"]
    )
    print(f"Published to Kinesis: {sensor_data}")

    # Save raw data to S3 for backup
    key = f"{RAW_PREFIX}sensor_{sensor_data['timestamp']}.json"
    s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(sensor_data))
    print(f"Saved raw data to s3://{S3_BUCKET}/{key}")

    return sensor_data

# -------------------------------
# DAG DEFINITION
# -------------------------------

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    "iot_full_pipeline_kinesis_delta",
    default_args=default_args,
    description="End-to-end IoT Lakehouse pipeline with Kinesis and Delta Lake",
    schedule_interval="@hourly",  # simulate hourly streaming
    start_date=days_ago(0),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task 1: Simulate IoT sensor data and publish to Kinesis
    generate_sensor_data = PythonOperator(
        task_id="generate_sensor_data",
        python_callable=simulate_iot_sensor_data,
    )

    # Task 2: Trigger Glue job to read from Kinesis and write to Delta Lake
    kinesis_to_delta = AwsGlueJobOperator(
        task_id="kinesis_to_delta_glue_job",
        job_name="Kinesis_to_DeltaLake_Job",  # Your actual Glue streaming job
        region_name=REGION,
        wait_for_completion=True,  # wait for Glue job to finish
    )

    # DAG Flow
    generate_sensor_data >> kinesis_to_delta
