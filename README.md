# E-Commerce Real-Time Data Pipeline

A beginner-friendly, end-to-end Data Engineering pipeline built for learning streaming architectures, distributed processing, cloud storage, and interview preparation (e.g., Tiger Analytics).

---

## End-to-End Architecture

```text
E-Commerce Event Generator
        ↓
  Kafka Producer (producer/producer.py)
        ↓
  Kafka Topic (ecommerce-orders)
        ↓
  PySpark Structured Streaming (spark/pipeline.py)
        ↓
  1. Data Cleaning: normalize status (trim, uppercase)
  2. Data Validation: check business contracts (valid vs. quarantined invalid)
  3. Transformation: total_value = quantity * amount
  4. Real-Time Aggregation: group by customer_id (total_sales, order_count) -> Console
  5. Cloud Storage Sink: Snappy-compressed Parquet -> Amazon S3 (s3a://)
        ↓
  Validation / SQL Analytics (read_s3.py)
```

---

## Project Structure

```text
ecommerce-data-pipeline/
│
├── docker/
│   └── docker-compose.yml       # Apache Kafka in KRaft mode (port 9092)
│
├── config/
│   └── config.py                # Dynamic configuration loader (.env & os.environ)
│
├── producer/
│   └── producer.py              # Synthetic order generator and Kafka publisher
│
├── consumer/
│   └── consumer.py              # Simple Kafka consumer for testing ingestion
│
├── spark/
│   └── pipeline.py              # PySpark Structured Streaming pipeline (Kafka -> S3)
│
├── data/
│   ├── checkpoints/             # Streaming state checkpoint directory
│   └── processed/               # Local Parquet output placeholder
│
├── sql/
│   ├── analytics_queries.sql    # 10 Spark SQL analytical queries
│   └── run_analytics.py         # Runner script to execute SQL queries on S3 Parquet
│
├── read_s3.py                   # Quick script to inspect Parquet files in S3
├── .env.example                 # Environment configuration template
├── requirements.txt             # Minimal project dependencies
└── README.md                    # Setup and execution guide
```

---

## Setup Guide

### 1. Prerequisites
- **Python 3.10+**
- **Java 17 LTS** (required for Apache Spark)
- **Docker & Docker Compose** (for running local Kafka)
- **AWS CLI** configured with an active AWS profile (`aws configure`)

---

### 2. Environment & S3 Configuration

1. **Clone the repository and create virtual environment:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Create your `.env` file from the template:**
   ```bash
   cp .env.example .env
   ```

3. **Configure your S3 bucket in `.env`:**
   Open `.env` and set your bucket name and region:
   ```env
   S3_BUCKET=your-unique-bucket-name
   S3_PREFIX=ecommerce/processed_v2
   AWS_REGION=your-aws-region
   ```
   > **Note:** Never commit `.env` or real AWS secret keys to GitHub. `.gitignore` automatically prevents `.env` from being committed while keeping `.env.example` tracked.

---

### 3. AWS Credentials

This pipeline uses the AWS SDK `ProfileCredentialsProvider` through `hadoop-aws`. It automatically reads credentials from your local AWS CLI configuration:
```bash
aws configure
```
Ensure your AWS IAM user or role has `s3:PutObject`, `s3:GetObject`, and `s3:ListBucket` permissions on your target bucket.

---

## How to Run the Pipeline

Open **3 terminal windows**:

### Terminal 1: Start Apache Kafka
```bash
docker compose -f docker/docker-compose.yml up
```
*(Wait a few seconds until Kafka starts and listens on `localhost:9092`)*

---

### Terminal 2: Start PySpark Streaming Pipeline
```bash
source .venv/bin/activate
python spark/pipeline.py
```
*The pipeline connects to Kafka, loads configuration dynamically from `.env`, runs cleaning/validation, outputs customer KPIs to the console, and writes Parquet files directly to your configured S3 bucket (`s3a://your-bucket/ecommerce/processed_v2/`).*

---

### Terminal 3: Start Order Event Producer
```bash
source .venv/bin/activate
python producer/producer.py
```
*Generates and sends order events to the `ecommerce-orders` topic every 2 seconds.*

---

## Verifying S3 Data Output

To verify that Parquet files are landing in your S3 bucket without opening the AWS Console:

```bash
source .venv/bin/activate
python read_s3.py
```
This reads and prints the schema and top rows directly from your configured S3 path.

---

## Running Phase 7: SQL Analytics

To run the complete suite of 10 analytical Spark SQL queries on your live S3 Parquet data:

```bash
source .venv/bin/activate
python sql/run_analytics.py
```
This registers the S3 dataset as a temporary view `orders` and prints formatted tables for:
1. Total sales and order count
2. Average Order Value (AOV)
3. Sales by customer
4. Top 5 customers by sales
5. Sales by product
6. Daily sales breakdown
7. Monthly sales breakdown
8. Cumulative running total of daily sales
9. Month-over-Month (MoM) sales change using `LAG()`
10. Top 2 customers per month using `ROW_NUMBER()`
