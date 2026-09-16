import os
import sys
import subprocess

# macOS helper: Ensure Spark uses Java 17 LTS if Java 26+ is default on the system
if "JAVA_HOME" not in os.environ or "26" in os.environ.get("JAVA_HOME", ""):
    try:
        java17_path = subprocess.check_output(["/usr/libexec/java_home", "-v", "17"]).decode("utf-8").strip()
        os.environ["JAVA_HOME"] = java17_path
    except Exception:
        pass

# Ensure root directory is in the Python path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.config import S3_PROCESSED_DATA_PATH

from pyspark.sql import SparkSession


def create_spark_session():
    """
    Initializes a SparkSession configured with AWS S3 credentials provider.
    """
    spark = SparkSession.builder \
        .appName("EcommerceSQLAnalytics") \
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

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def run_analytics():
    """
    Reads S3 Parquet data into a Spark DataFrame, registers a temporary view 'orders',
    and executes all 10 analytical SQL queries.
    """
    print("Initializing Spark session for SQL Analytics...")
    spark = create_spark_session()

    print(f"Reading Parquet dataset from configured S3 path:\n  -> {S3_PROCESSED_DATA_PATH}\n")
    orders_df = spark.read.parquet(S3_PROCESSED_DATA_PATH)

    # Register temporary view for Spark SQL execution
    orders_df.createOrReplaceTempView("orders")

    # Define the 10 analytical queries
    queries = [
        (
            "1. Total Sales and Total Order Count",
            """
            SELECT
                ROUND(SUM(total_value), 2) AS total_sales,
                COUNT(order_id)            AS total_orders
            FROM orders
            """
        ),
        (
            "2. Average Order Value (AOV)",
            """
            SELECT
                ROUND(AVG(total_value), 2) AS average_order_value
            FROM orders
            """
        ),
        (
            "3. Sales by Customer",
            """
            SELECT
                customer_id,
                COUNT(order_id)            AS order_count,
                ROUND(SUM(total_value), 2) AS total_sales
            FROM orders
            GROUP BY customer_id
            ORDER BY total_sales DESC
            """
        ),
        (
            "4. Top Customers by Total Sales",
            """
            SELECT
                customer_id,
                ROUND(SUM(total_value), 2) AS total_sales
            FROM orders
            GROUP BY customer_id
            ORDER BY total_sales DESC
            LIMIT 5
            """
        ),
        (
            "5. Sales by Product",
            """
            SELECT
                product_id,
                SUM(quantity)              AS total_quantity_sold,
                ROUND(SUM(total_value), 2) AS total_sales
            FROM orders
            GROUP BY product_id
            ORDER BY total_sales DESC
            """
        ),
        (
            "6. Daily Sales Breakdown",
            """
            SELECT
                TO_DATE(order_timestamp)   AS order_date,
                COUNT(order_id)            AS total_orders,
                ROUND(SUM(total_value), 2) AS daily_sales
            FROM orders
            GROUP BY TO_DATE(order_timestamp)
            ORDER BY order_date ASC
            """
        ),
        (
            "7. Monthly Sales Breakdown",
            """
            SELECT
                DATE_FORMAT(TO_TIMESTAMP(order_timestamp), 'yyyy-MM') AS order_month,
                COUNT(order_id)                                       AS total_orders,
                ROUND(SUM(total_value), 2)                            AS monthly_sales
            FROM orders
            GROUP BY DATE_FORMAT(TO_TIMESTAMP(order_timestamp), 'yyyy-MM')
            ORDER BY order_month ASC
            """
        ),
        (
            "8. Running Total of Sales Over Time",
            """
            WITH daily_sales_summary AS (
                SELECT
                    TO_DATE(order_timestamp)   AS order_date,
                    ROUND(SUM(total_value), 2) AS daily_sales
                FROM orders
                GROUP BY TO_DATE(order_timestamp)
            )
            SELECT
                order_date,
                daily_sales,
                ROUND(
                    SUM(daily_sales) OVER (
                        ORDER BY order_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ),
                    2
                ) AS running_total_sales
            FROM daily_sales_summary
            ORDER BY order_date ASC
            """
        ),
        (
            "9. Month-Over-Month (MoM) Sales Change Using LAG()",
            """
            WITH monthly_sales_summary AS (
                SELECT
                    DATE_FORMAT(TO_TIMESTAMP(order_timestamp), 'yyyy-MM') AS order_month,
                    ROUND(SUM(total_value), 2)                            AS current_month_sales
                FROM orders
                GROUP BY DATE_FORMAT(TO_TIMESTAMP(order_timestamp), 'yyyy-MM')
            )
            SELECT
                order_month,
                current_month_sales,
                LAG(current_month_sales, 1) OVER (
                    ORDER BY order_month ASC
                ) AS previous_month_sales,
                ROUND(
                    current_month_sales - LAG(current_month_sales, 1) OVER (ORDER BY order_month ASC),
                    2
                ) AS sales_change
            FROM monthly_sales_summary
            ORDER BY order_month ASC
            """
        ),
        (
            "10. Top 2 Customers Per Month Using ROW_NUMBER()",
            """
            WITH customer_monthly_sales AS (
                SELECT
                    DATE_FORMAT(TO_TIMESTAMP(order_timestamp), 'yyyy-MM') AS order_month,
                    customer_id,
                    ROUND(SUM(total_value), 2)                            AS total_sales
                FROM orders
                GROUP BY
                    DATE_FORMAT(TO_TIMESTAMP(order_timestamp), 'yyyy-MM'),
                    customer_id
            ),
            ranked_customers AS (
                SELECT
                    order_month,
                    customer_id,
                    total_sales,
                    ROW_NUMBER() OVER (
                        PARTITION BY order_month
                        ORDER BY total_sales DESC
                    ) AS rank_in_month
                FROM customer_monthly_sales
            )
            SELECT
                order_month,
                customer_id,
                total_sales,
                rank_in_month
            FROM ranked_customers
            WHERE rank_in_month <= 2
            ORDER BY order_month ASC, rank_in_month ASC
            """
        )
    ]

    for title, sql_query in queries:
        print("=" * 80)
        print(f"  {title}")
        print("=" * 80)
        spark.sql(sql_query).show(truncate=False)
        print()

    print("All 10 analytical SQL queries executed successfully.")
    spark.stop()


if __name__ == "__main__":
    run_analytics()
