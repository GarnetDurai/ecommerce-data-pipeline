import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ------------------------------------------------------------------------------
# Kafka Broker & Topic Configuration
# ------------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce-orders")

# ------------------------------------------------------------------------------
# Local Storage & Checkpoint Directories
# ------------------------------------------------------------------------------
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "data/checkpoints/orders_v2")
PROCESSED_DATA_PATH = os.getenv("PROCESSED_DATA_PATH", "data/processed")

# ------------------------------------------------------------------------------
# AWS S3 Storage Configuration
# ------------------------------------------------------------------------------
AWS_REGION = os.getenv("AWS_REGION")
if not AWS_REGION:
    raise ValueError("AWS_REGION is required")

S3_BUCKET = os.getenv("S3_BUCKET")
if not S3_BUCKET:
    raise ValueError("S3_BUCKET is required")

S3_PREFIX = os.getenv("S3_PREFIX", "ecommerce/processed_v2")

# Construct dynamic S3A URL (e.g., s3a://your-bucket-name/ecommerce/processed_v2/)
S3_PROCESSED_DATA_PATH = (
    f"s3a://{S3_BUCKET.strip('/')}/{S3_PREFIX.strip('/')}/"
)