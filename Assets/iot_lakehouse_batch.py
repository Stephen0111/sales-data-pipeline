from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
import pendulum
import json
import random
import time
import boto3
import logging
import os


# CONFIGURATION


REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET = "my-iot-lakehouse"
RAW_PREFIX = "raw/"
KINESIS_STREAM = "IoTSensorStream"
BATCH_SIZE = 5  # number of sensor records per DAG run


# FUNCTIONS


def simulate_iot_sensor_data(**kwargs):
    """Generate sensor data, send to Kinesis, and backup to S3"""
    logging.basicConfig(level=logging.INFO)

    s3_client = boto3.client("s3", region_name=REGION)
    kinesis_client = boto3.client("kinesis", region_name=REGION)

    sensor_batch = []

    for _ in range(BATCH_SIZE):
        sensor_data = {
            "equipment_id": f"EQ-{random.randint(1,10)}",
            "temperature": round(random.uniform(20, 100), 2),
            "pressure": round(random.uniform(1, 5), 2),
            "vibration": round(random.uniform(0.1, 5), 2),
            "timestamp": int(time.time())
        }

        # Send to Kinesis
        try:
            kinesis_client.put_record(
                StreamName=KINESIS_STREAM,
                Data=json.dumps(sensor_data),
                PartitionKey=sensor_data["equipment_id"]
            )
            logging.info(f"Published to Kinesis: {sensor_data}")
        except Exception as e:
            logging.error(f"Kinesis publish failed: {e}")

        # Backup to S3
        key = f"{RAW_PREFIX}sensor_{sensor_data['timestamp']}_{random.randint(1000,9999)}.json"
        try:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(sensor_data)
            )
            logging.info(f"Saved raw data to s3://{S3_BUCKET}/{key}")
        except Exception as e:
            logging.error(f"S3 upload failed: {e}")

        sensor_batch.append(sensor_data)
        time.sleep(0.1)

    return sensor_batch

def check_s3_files(**kwargs):
    """Ensure S3 has files before triggering Glue"""
    s3_client = boto3.client("s3", region_name=REGION)
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=RAW_PREFIX)
    if response.get("KeyCount", 0) == 0:
        raise ValueError(f"No files found in s3://{S3_BUCKET}/{RAW_PREFIX}. Cannot run Glue job.")
    logging.info(f"Found {response['KeyCount']} file(s) in S3. Proceeding to Glue job.")


# DAG DEFINITION


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
    schedule="@hourly",
    start_date=pendulum.now().subtract(days=1),
    catchup=False,
    max_active_runs=1,
) as dag:

    generate_sensor_data = PythonOperator(
        task_id="generate_sensor_data",
        python_callable=simulate_iot_sensor_data,
    )

    verify_s3_files = PythonOperator(
        task_id="verify_s3_files",
        python_callable=check_s3_files,
    )

   
    kinesis_to_delta = GlueJobOperator(
    task_id="s3_to_delta_batch_job",
    job_name="S3_to_DeltaLake_Batch_Job",  
    region_name=REGION,
    wait_for_completion=True,  
    )

    # DAG flow
    generate_sensor_data >> verify_s3_files >> kinesis_to_delta
