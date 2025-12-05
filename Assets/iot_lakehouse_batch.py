from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago
import json
import random
import time
import boto3


REGION = "us-east-1"
S3_BUCKET = "my-iot-lakehouse"
RAW_PREFIX = "raw/"
KINESIS_STREAM = "IoTSensorStream"


def simulate_iot_sensor_data(**kwargs):
    s3_client = boto3.client("s3", region_name=REGION)
    kinesis_client = boto3.client("kinesis", region_name=REGION)

    sensor_data = {
        "equipment_id": f"EQ-{random.randint(1,10)}",
        "temperature": round(random.uniform(20, 100), 2),
        "pressure": round(random.uniform(1, 5), 2),
        "vibration": round(random.uniform(0.1, 5), 2),
        "timestamp": int(time.time())
    }

    kinesis_client.put_record(
        StreamName=KINESIS_STREAM,
        Data=json.dumps(sensor_data),
        PartitionKey=sensor_data["equipment_id"]
    )

    key = f"{RAW_PREFIX}sensor_{sensor_data['timestamp']}.json"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(sensor_data)
    )

    return sensor_data


default_args = {
    "owner": "data-engineer",
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    "iot_full_pipeline_kinesis_delta",
    default_args=default_args,
    description="IoT Lakehouse pipeline using Kinesis + Glue + Delta Lake",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
) as dag:

    generate_sensor_data = PythonOperator(
        task_id="generate_sensor_data",
        python_callable=simulate_iot_sensor_data,
    )

    kinesis_to_delta = GlueJobOperator(
        task_id="kinesis_to_delta_glue_job",
        job_name="Kinesis_to_DeltaLake_Job",
        region_name=REGION,
        wait_for_completion=True,
    )

    generate_sensor_data >> kinesis_to_delta
