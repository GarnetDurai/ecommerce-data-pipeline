import json
import sys
import os

# Ensure the root directory is in the Python path so we can import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

from kafka import KafkaConsumer


def run_consumer():
    """
    Connects to Kafka and continuously listens for incoming order events.
    In Java, this is like:
    Consumer<String, byte[]> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(topic));
    """
    print(f"Connecting consumer to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")

    # auto_offset_reset='earliest' means read all messages from the beginning if no offset is saved
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="test-order-consumer-group"
    )

    print(f"Connected! Subscribed to topic: '{KAFKA_TOPIC}'")
    print("Waiting for messages... (Press Ctrl+C to stop)\n")

    try:
        # Standard loop over consumer records
        # In Java, this is like: ConsumerRecords<String, byte[]> records = consumer.poll(...);
        for message in consumer:
            # 1. message.value contains raw bytes from the network
            raw_bytes = message.value

            # 2. Decode bytes to a UTF-8 string
            json_string = raw_bytes.decode("utf-8")

            # 3. Parse JSON string into a Python dictionary
            order = json.loads(json_string)

            # 4. Print the received order details
            print(f"[CONSUMER RECEIVED] Order ID: {order['order_id']} | "
                  f"Customer: {order['customer_id']} | "
                  f"Product: {order['product_id']} | "
                  f"Qty: {order['quantity']} | "
                  f"Amount: ${order['amount']} | "
                  f"Status: {order['order_status']} | "
                  f"Timestamp: {order['order_timestamp']}")

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    run_consumer()
