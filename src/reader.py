from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# --- Configuration ---
DELTA_LAKE_PATH = "./data/crypto_stats_delta"

def create_spark_session():
    """
    Creates a Spark Session with Delta Lake support.
    Must match the configuration used in the processor to read the transaction logs correctly.
    """
    # We need the same Delta Lake JARs to read the format
    return SparkSession.builder \
        .appName("CryptoLakehouseReader") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local[*]") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR") # Hide the noise, show me the data

    print(f"📂 Reading Delta Lake from: {DELTA_LAKE_PATH}")

    try:
        # 1. Load the Delta Table
        # This reads the _delta_log to find the latest valid parquet files
        df = spark.read.format("delta").load(DELTA_LAKE_PATH)

        # 2. Analytics Query
        # Let's see the most recent calculations first
        print("\n📊 --- Latest Crypto Metrics (1-Minute Windows) ---")
        
        result = df.orderBy(desc("window.end"))
        
        # Display up to 20 rows, do not truncate long strings
        result.show(20, truncate=False)

        # 3. Print Schema Verification
        print("\n📋 --- Data Schema ---")
        df.printSchema()
        
        row_count = df.count()
        print(f"\n✅ Total Aggregated Windows Stored: {row_count}")

    except Exception as e:
        print(f"\n⚠️ Error reading data: {e}")
        print("Tip: Wait a minute for the stream to write the first batch of data.")

if __name__ == "__main__":
    main()