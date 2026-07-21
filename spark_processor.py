#1 - Initialize spark sesssion
import os
import sys

# Set Hadoop Home explicitly in Python runtime
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += ";C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, expr, from_json, sum, window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

spark = (
    SparkSession.builder
    .appName("StockMarketProcessor")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

# Optional: Set log level to WARN to reduce console verbosity
# spark.sparkContext.setLogLevel("WARN")

#2 - Read stream from kakfa topic

rawStream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stock_prices")
    .option("startingOffsets", "latest")
    .load()
)

#3 - Deserialize the JSON data from Kafka messages into structured columns

# Define schema for the JSON payload received from Kafka
schema = StructType(
    [
        StructField("ticker", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("volume", IntegerType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

# Deserialize and extract fields

# Cast raw value bytes to string, parse JSON using schema, and select individual fields
parsedStream = (
    rawStream
    .selectExpr("CAST(value AS STRING) as json_string")
    .select(from_json(col("json_string"), schema).alias("data"))
    .select("data.*")
)

#4 - Compute metrrics

# 1. Cast timestamp string to proper TimestampType
# 2. Group by ticker and 5-second sliding window
# 3. Compute the average price per window
movingAvgStream = (
    parsedStream
    .withColumn("timestamp", col("timestamp").cast("timestamp"))
    .groupBy(
        col("ticker"),
        window(col("timestamp"), "5 seconds")
    )
    .agg(
        avg("price").alias("moving_avg_price")
    )
)

# VWAP

vwapStream = (
    parsedStream
    .withColumn("timestamp", col("timestamp").cast("timestamp"))
    .groupBy(
        col("ticker"),
        window(col("timestamp"), "5 seconds")
    )
    .agg(
        expr("sum(price * volume) / sum(volume)").alias("vwap")
    )
)

#5 - Output both streaming metrics to console

# Write Moving Average stream to console
query_moving_avg = (
    movingAvgStream
    .writeStream
    .format("console")
    .outputMode("complete")
    .queryName("moving_average")
    .start()
)

# Write VWAP stream to console
query_vwap = (
    vwapStream
    .writeStream
    .format("console")
    .outputMode("complete")
    .queryName("vwap")
    .start()
)

# Block execution to keep the streaming queries running
spark.streams.awaitAnyTermination()