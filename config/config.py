# Configuration constants for the pipeline
# In Java, this is equivalent to a public class AppConfig with static final fields.

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ecommerce-orders"
CHECKPOINT_DIR = "data/checkpoints/orders_v2"
PROCESSED_DATA_PATH = "data/processed"
S3_PROCESSED_DATA_PATH = "s3a://garnetdurai-ecommerce-data-pipeline/ecommerce/processed_v2/"