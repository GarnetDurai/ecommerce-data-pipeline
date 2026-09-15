import sys
import os
import subprocess

# macOS helper: Ensure Spark uses Java 17 LTS if Java 26+ is default on the system
if "JAVA_HOME" not in os.environ or "26" in os.environ.get("JAVA_HOME", ""):
    try:
        java17_path = subprocess.check_output(["/usr/libexec/java_home", "-v", "17"]).decode("utf-8").strip()
        os.environ["JAVA_HOME"] = java17_path
    except Exception:
        pass

# Ensure the root directory is in the Python path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    CHECKPOINT_DIR,
    S3_PROCESSED_DATA_PATH
)

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType
)
from pyspark.sql.functions import (
    col,
    from_json,
    upper,
    trim,
    when,
    round as _round,
    sum as _sum,
    count
)


def create_spark_session():
    """
    Creates SparkSession with the Kafka SQL connector package.
    In Java: SparkSession.builder().appName(...).getOrCreate();
    """
    spark = SparkSession.builder \
        .appName("EcommerceStreamPipeline") \
        .master("local[*]") \
        .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,"
        "org.apache.hadoop:hadoop-aws:3.5.0"
    ) \
        .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    ) \
        .getOrCreate()

    # Set log level to WARN to keep console output clean
    spark.sparkContext.setLogLevel("WARN")
    return spark


def define_order_schema():
    """
    Defines the explicit schema for incoming order JSON events.
    """
    return StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_timestamp", StringType(), True)
    ])


def clean_orders(df):
    """
    DATA CLEANING:
    Normalizes data format without discarding records.
    - Trims whitespace from order_status and converts to UPPERCASE.
    - In Java: order.setStatus(order.getStatus().trim().toUpperCase())
    """
    cleaned_df = df.withColumn(
        "order_status", upper(trim(col("order_status")))
    )
    return cleaned_df


def validate_orders(df):
    """
    DATA VALIDATION:
    Enforces business rules and flags invalid records.
    Valid criteria:
      1. order_id is not null
      2. customer_id is not null
      3. product_id is not null
      4. quantity > 0
      5. amount > 0
      6. order_status in ('PENDING', 'COMPLETED', 'CANCELLED')

    Returns:
      valid_df, invalid_df
    """
    # Define boolean condition for valid records
    is_valid_condition = (
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("product_id").isNotNull() &
        (col("quantity") > 0) &
        (col("amount") > 0) &
        col("order_status").isin("PENDING", "COMPLETED", "CANCELLED")
    )

    # Mark validation flag and explain invalid reason
    tagged_df = df.withColumn(
        "is_valid", when(is_valid_condition, True).otherwise(False)
    ).withColumn(
        "validation_note",
        when(col("order_id").isNull(), "Missing order_id")
        .when(col("customer_id").isNull(), "Missing customer_id")
        .when(col("product_id").isNull(), "Missing product_id")
        .when(col("quantity") <= 0, "Quantity must be > 0")
        .when(col("amount") <= 0, "Amount must be > 0")
        .when(~col("order_status").isin("PENDING", "COMPLETED", "CANCELLED"), "Invalid order_status")
        .otherwise("VALID")
    )

    # Separate streams: valid records continue, invalid records are quarantined
    valid_df = tagged_df.filter(col("is_valid") == True).drop("is_valid", "validation_note")
    invalid_df = tagged_df.filter(col("is_valid") == False)

    return valid_df, invalid_df


def transform_orders(valid_df):
    """
    DATA TRANSFORMATION:
    Adds business derived fields.
    - total_value = quantity * amount
    """
    transformed_df = valid_df.withColumn(
        "total_value", _round(col("quantity") * col("amount"), 2)
    )
    return transformed_df


def aggregate_orders(transformed_df):
    """
    DATA AGGREGATION:
    Groups valid orders by customer_id to calculate real-time business KPIs:
    - total_sales (sum of total_value)
    - order_count (number of orders)
    
    Note: In Spark, groupBy causes a network SHUFFLE across partitions.
    """
    aggregated_df = transformed_df.groupBy("customer_id").agg(
        _round(_sum("total_value"), 2).alias("total_sales"),
        count("order_id").alias("order_count")
    )
    return aggregated_df


def run_pipeline():
    """
    Main pipeline entry point:
    Kafka -> ReadStream -> Clean -> Validate -> Transform -> Aggregate -> Console
    """
    spark = create_spark_session()
    print("SparkSession created successfully.")

    # 1. Read real-time stream from Kafka
    kafka_raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Deserialize Kafka binary value to string and parse JSON
    kafka_string_df = kafka_raw_df.selectExpr("CAST(value AS STRING) AS json_value")
    order_schema = define_order_schema()
    raw_orders_df = kafka_string_df.select(
        from_json(col("json_value"), order_schema).alias("data")
    ).select("data.*")

    # 3. Clean records
    cleaned_df = clean_orders(raw_orders_df)

    # 4. Validate records (split into valid and invalid)
    valid_df, invalid_df = validate_orders(cleaned_df)

    # 5. Transform valid records
    transformed_df = transform_orders(valid_df)

    # 6. Aggregate valid records by customer
    aggregated_df = aggregate_orders(transformed_df)

    print("\nStarting Structured Streaming Queries...")
    print(f"1. Parquet Sink: Writing valid processed orders to '{S3_PROCESSED_DATA_PATH}'.")
    print("2. Table 'CustomerAggregations' displays real-time aggregated metrics.")
    print("3. Table 'InvalidOrders' displays any quarantined invalid records.")
    print("Press Ctrl+C to stop.\n")

    # 7A. Write valid processed orders to Parquet files
    # Streaming file sinks require 'append' output mode
    parquet_query = transformed_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", S3_PROCESSED_DATA_PATH) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/parquet") \
        .queryName("ParquetWriter") \
        .start()

    # 7B. Output aggregated KPI table to console
    # Mode 'complete' outputs the full updated state table on every micro-batch
    agg_query = aggregated_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", "false") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/aggregated") \
        .queryName("CustomerAggregations") \
        .start()

    # 7C. Output invalid quarantined records to console
    # Mode 'append' prints invalid records as they arrive
    invalid_query = invalid_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/invalid") \
        .queryName("InvalidOrders") \
        .start()

    # Wait for either stream to terminate
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_pipeline()
