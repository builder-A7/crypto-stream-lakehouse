import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev, sum as _sum, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# --- Configuration ---
KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC_NAME = "crypto_ticks"
DELTA_LAKE_PATH = "./data/crypto_stats_delta"
CHECKPOINT_PATH = "./data/checkpoints/crypto_stats"

def create_spark_session():
    """
    Creates a Spark Session with Delta Lake and Kafka support.
    It will automatically download the necessary JARs from Maven Central.
    """
    return SparkSession.builder \
        .appName("CryptoLakehouse") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "2") \
        .master("local[*]") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN") # Reduce noise in terminal

    # 1. Define Schema
    # We must match the JSON structure sent by producer.py
    schema = StructType([
        StructField("pair", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("volume", FloatType(), True),
        StructField("timestamp", FloatType(), True), # Unix timestamp from source
        StructField("side", StringType(), True),
        StructField("ingest_ts", StringType(), True)
    ])

    # 2. Read Stream from Kafka
    # Resulting DF has key, value, topic, partition, offset, timestamp
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()

    # 3. ETL: Parse JSON and Handle Time
    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select(
        "data.*"
    ).withColumn(
        # Convert Unix Float timestamp to Spark TimestampType for Windowing
        "event_time", col("timestamp").cast(TimestampType())
    )

    # 4. Analytics: Calculate 1-Minute Moving Averages & Volatility
    # Watermark: Handle data arriving up to 10 seconds late
    aggregated_stream = parsed_stream \
        .withWatermark("event_time", "10 seconds") \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("pair")
        ) \
        .agg(
            avg("price").alias("avg_price"),
            stddev("price").alias("volatility"),
            _sum("volume").alias("total_volume"),
            expr("count(*)").alias("trade_count")
        )

    # 5. Write Stream to Delta Lake
    # We use 'append' mode to add new window calculations as they finalize
    query = aggregated_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .option("path", DELTA_LAKE_PATH) \
        .start()

    print(f"🚀 Streaming to Delta Lake at {DELTA_LAKE_PATH}...")
    
    # 5b. (Optional Debug) Uncomment to print to console instead of Delta
    # query = aggregated_stream.writeStream \
    #    .format("console") \
    #    .outputMode("update") \
    #    .option("truncate", "false") \
    #    .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()