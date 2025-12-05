import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# -------------------------------
# CONFIGURATION
# -------------------------------
s3_output_path = "s3://my-iot-lakehouse/processed/"
kinesis_stream_name = "IoTSensorStream"
region_name = "us-east-1"

# -------------------------------
# SPARK SESSION
# -------------------------------
spark = SparkSession.builder \
    .appName("Kinesis_to_DeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# -------------------------------
# KINESIS STREAM SCHEMA
# -------------------------------
schema = StructType([
    StructField("equipment_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# -------------------------------
# READ STREAM FROM KINESIS
# -------------------------------
df = spark.readStream \
    .format("kinesis") \
    .option("streamName", kinesis_stream_name) \
    .option("region", region_name) \
    .option("initialPosition", "TRIM_HORIZON") \
    .load()

# Kinesis sends data as binary, convert to string and parse JSON
from pyspark.sql.functions import from_json, col

df_parsed = df.selectExpr("CAST(data AS STRING) as json_data") \
              .select(from_json(col("json_data"), schema).alias("data")) \
              .select("data.*")

# -------------------------------
# COMPUTE 5-MINUTE ROLLING AVERAGES
# -------------------------------
df_avg = df_parsed.withColumn("event_time", from_unixtime(col("timestamp"))) \
                  .groupBy(
                      col("equipment_id"),
                      window(col("event_time"), "5 minutes")
                  ).agg(
                      avg("temperature").alias("avg_temperature"),
                      avg("pressure").alias("avg_pressure"),
                      avg("vibration").alias("avg_vibration")
                  )

# -------------------------------
# DETECT ANOMALIES
# -------------------------------
# Example: simple threshold anomaly detection
df_anomalies = df_avg.withColumn("temp_anomaly", col("avg_temperature") > 80) \
                     .withColumn("pressure_anomaly", col("avg_pressure") > 4) \
                     .withColumn("vibration_anomaly", col("avg_vibration") > 4.5)

# -------------------------------
# WRITE TO DELTA LAKE
# -------------------------------
query = df_anomalies.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://my-iot-lakehouse/checkpoints/") \
    .partitionBy("equipment_id") \
    .start(s3_output_path)

query.awaitTermination()
