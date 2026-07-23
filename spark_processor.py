# 1 - Initialize spark session
import os
import sys

# Set Hadoop Home explicitly in Python runtime
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += ";C:\\hadoop\\bin"

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, expr, from_json, row_number, sum, window
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window

spark = (
    SparkSession.builder.appName("StockMarketProcessor")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3",
    )
    .getOrCreate()
)

# Optional: Set log level to WARN to reduce console verbosity
# spark.sparkContext.setLogLevel("WARN")

# 2 - Read stream from kafka topic

rawStream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stock_prices")
    .option("startingOffsets", "latest")
    .load()
)

# 3 - Deserialize the JSON data from Kafka messages into structured columns

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
parsedStream = (
    rawStream.selectExpr("CAST(value AS STRING) as json_string")
    .select(from_json(col("json_string"), schema).alias("data"))
    .select("data.*")
)

# 4 - Compute metrics

# Moving Average Stream
movingAvgStream = (
    parsedStream.withColumn("timestamp", col("timestamp").cast("timestamp"))
    .groupBy(col("ticker"), window(col("timestamp"), "5 seconds"))
    .agg(avg("price").alias("moving_avg_price"))
)

# VWAP Stream
vwapStream = (
    parsedStream.withColumn("timestamp", col("timestamp").cast("timestamp"))
    .groupBy(col("ticker"), window(col("timestamp"), "5 seconds"))
    .agg(expr("sum(price * volume) / sum(volume)").alias("vwap"))
)


# PostgreSQL Write Function for foreachBatch
def write_to_postgres(batch_df, batch_id):
    # Deduplicate batch: keep newest window per ticker
    window_spec = Window.partitionBy("ticker").orderBy(col("window.end").desc())

    deduped_df = (
        batch_df.withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    rows = deduped_df.collect()

    if not rows:
        return

    db_config = {
    "dbname": "stockmarket",
    "user": "stockuser",
    "password": "stockpass",
    "host": "localhost",
    "port": 5433,
}

    upsert_sql = """
        INSERT INTO latest_prices (ticker, vwap, moving_avg_price, window_start, window_end, updated_at)
        VALUES (%s, %s, NULL, %s, %s, NOW())
        ON CONFLICT (ticker) DO UPDATE SET
            vwap = EXCLUDED.vwap,
            window_start = EXCLUDED.window_start,
            window_end = EXCLUDED.window_end,
            updated_at = NOW()
        WHERE latest_prices.window_end < EXCLUDED.window_end;
    """

    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        with conn.cursor() as cursor:
            for row in rows:
                cursor.execute(
                    upsert_sql,
                    (
                        row["ticker"],
                        row["vwap"],
                        row["window"]["start"],
                        row["window"]["end"],
                    ),
                )
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error writing batch {batch_id} to PostgreSQL: {e}")
        raise e
    finally:
        if conn:
            conn.close()


# 5 - Write VWAP stream to PostgreSQL

query_vwap = (
    vwapStream.writeStream.foreachBatch(write_to_postgres)
    .outputMode("complete")
    .queryName("vwap_to_postgres")
    .start()
)

spark.streams.awaitAnyTermination()