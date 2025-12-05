import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window, to_timestamp, from_json, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# GET JOB PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'stream_name', 'output_path', 'TempDir'])

# CONFIGURATION
REGION = "eu-north-1"
KINESIS_STREAM = args['stream_name']  # From job parameters
S3_OUTPUT_PATH = args['output_path']   # From job parameters
CHECKPOINT_PATH = f"{args['TempDir']}checkpoints/"

# INITIALIZE GLUE CONTEXT
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Configure Delta Lake
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# KINESIS STREAM SCHEMA
schema = StructType([
    StructField("equipment_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# READ FROM KINESIS USING GLUE STREAMING
kinesis_options = {
    "typeOfData": "kinesis",
    "streamARN": f"arn:aws:kinesis:{REGION}:317968578062:stream/{KINESIS_STREAM}",
    "classification": "json",
    "startingPosition": "TRIM_HORIZON",
    "inferSchema": "false",
}

# Create streaming DynamicFrame from Kinesis
data_frame_kinesis = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options=kinesis_options
)

# Convert to Spark DataFrame and parse JSON
df_raw = data_frame_kinesis.toDF()

# Parse JSON data
df_parsed = df_raw.selectExpr("CAST(data AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

# CONVERT TIMESTAMP AND ADD WATERMARK
df_parsed = df_parsed.withColumn("event_time", from_unixtime(col("timestamp")))
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

job.commit()
