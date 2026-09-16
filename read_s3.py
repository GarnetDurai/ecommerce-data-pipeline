import os
import sys

# Ensure root directory is in the Python path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))
from config.config import S3_PROCESSED_DATA_PATH

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadS3Parquet") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.5.0"
    ) \
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    ) \
    .getOrCreate()

print(f"Reading Parquet data from configured S3 path: {S3_PROCESSED_DATA_PATH}")
df = spark.read.parquet(S3_PROCESSED_DATA_PATH)
df.printSchema()
df.show(10, truncate=False)

spark.stop()