import sys
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window, to_timestamp, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


# CONFIGURATION

REGION = "eu-north-1"  # correct region for S3/Kinesis/Glue
S3_OUTPUT_PATH = "s3://my-iot-lakehouse/processed/"
CHECKPOINT_PATH = "s3://my-iot-lakehouse/checkpoints/"
KINESIS_STREAM = "IoTSensorStream"


# SPARK SESSION

spark = SparkSession.builder \
    .appName("Kinesis_to_DeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


# KINESIS STREAM SCHEMA

schema = StructType([
    StructField("equipment_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])


# READ STREAM FROM KINESIS

df_raw = spark.readStream \
    .format("kinesis") \
    .option("streamName", KINESIS_STREAM) \
    .option("region", REGION) \
    .option("initialPosition", "TRIM_HORIZON") \
    .load()

# Convert Kinesis binary data to string and parse JSON
from pyspark.sql.functions import from_json

df_parsed = df_raw.selectExpr("CAST(data AS STRING) as json_data") \
                  .select(from_json(col("json_data"), schema).alias("data")) \
                  .select("data.*")

# -------------------------------
# CONVERT TIMESTAMP AND ADD WATERMARK
# -------------------------------
# Kinesis timestamp is in seconds since epoch
df_parsed = df_parsed.withColumn("event_time", to_timestamp(col("timestamp")))

df_with_watermark = df_parsed.withWatermark("event_time", "5 minutes")


# COMPUTE 5-MINUTE ROLLING AVERAGES

df_avg = df_with_watermark.groupBy(
    col("equipment_id"),
    window(col("event_time"), "5 minutes")
).agg(
    avg("temperature").alias("avg_temperature"),
    avg("pressure").alias("avg_pressure"),
    avg("vibration").alias("avg_vibration")
)

# DETECT ANOMALIES

# Simple threshold-based anomaly detection
df_anomalies = df_avg.withColumn("temp_anomaly", col("avg_temperature") > 80) \
                     .withColumn("pressure_anomaly", col("avg_pressure") > 4) \
                     .withColumn("vibration_anomaly", col("avg_vibration") > 4.5)


# WRITE STREAM TO DELTA LAKE

query = df_anomalies.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .partitionBy("equipment_id") \
    .start(S3_OUTPUT_PATH)

query.awaitTermination()
