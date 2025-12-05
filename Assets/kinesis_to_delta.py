import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, avg, from_unixtime, window, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Configuration
REGION = "eu-north-1"
S3_INPUT_PATH = "s3://my-iot-lakehouse/raw/"
S3_OUTPUT_PATH = "s3://my-iot-lakehouse/processed/iot_data_delta/"

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Configure Delta Lake
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define schema
schema = StructType([
    StructField("equipment_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# Read JSON files from S3
df_raw = spark.read.schema(schema).json(S3_INPUT_PATH)

# Convert timestamp to datetime
df_parsed = df_raw.withColumn("event_time", from_unixtime(col("timestamp")))

# Compute aggregations (simulating streaming window)
df_avg = df_parsed.groupBy("equipment_id").agg(
    avg("temperature").alias("avg_temperature"),
    avg("pressure").alias("avg_pressure"),
    avg("vibration").alias("avg_vibration")
)

# Add processing timestamp
df_avg = df_avg.withColumn("processed_at", current_timestamp())

# Detect anomalies
df_anomalies = df_avg.withColumn("temp_anomaly", col("avg_temperature") > 80) \
    .withColumn("pressure_anomaly", col("avg_pressure") > 4) \
    .withColumn("vibration_anomaly", col("avg_vibration") > 4.5)

# Write to Delta Lake (batch mode)
df_anomalies.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("equipment_id") \
    .save(S3_OUTPUT_PATH)

job.commit()
