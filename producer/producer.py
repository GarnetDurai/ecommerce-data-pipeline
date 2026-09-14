import json
import random
import time
from datetime import datetime
import sys
import os

# Ensure the root directory is in the Python path so we can import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

from kafka import KafkaProducer


def create_order_event(order_id):
    """
    Generates a single e-commerce order event as a simple dictionary.
    In Java, this is like creating a new Order(orderId, customerId, ...) instance.
    """
    customers = [101, 102, 103, 104, 105]
    products = [501, 502, 503, 504, 505]
    statuses = ["COMPLETED", "COMPLETED", "COMPLETED", "PENDING", "CANCELLED"]

    # Pick random values for realistic order events
    customer_id = random.choice(customers)
    product_id = random.choice(products)
    quantity = random.randint(1, 5)
    unit_price = random.choice([299.00, 499.00, 999.00, 1499.00, 2499.00])
    amount = round(quantity * unit_price, 2)
    order_status = random.choice(statuses)
    order_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # Testing feature: Every 7th order is intentionally invalid to verify the validation and quarantine logic
    if order_id % 7 == 0:
        amount = -99.00
        order_status = "INVALID_STATUS"

    order = {
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": quantity,
        "amount": amount,
        "order_status": order_status,
        "order_timestamp": order_timestamp
    }
    return order


def run_producer():
    """
    Connects to Kafka and continuously publishes order events in a loop.
    """
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")

    # KafkaProducer sends bytes over the network.
    # value_serializer converts our Python dictionary -> JSON string -> UTF-8 bytes.
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda event: json.dumps(event).encode("utf-8")
    )

    print(f"Connected successfully! Sending order events to topic: '{KAFKA_TOPIC}'")
    print("Press Ctrl+C to stop.\n")

    current_order_id = 1001

    try:
        while True:
            # 1. Generate an order event
            order_event = create_order_event(current_order_id)

            # 2. Send the event to the Kafka topic
            # Like producer.send(new ProducerRecord<>(topic, key, value)) in Java
            producer.send(KAFKA_TOPIC, value=order_event)

            # 3. Print the event so we can observe what was sent
            print(f"[PRODUCER SENT] Order ID: {order_event['order_id']} | "
                  f"Customer: {order_event['customer_id']} | "
                  f"Amount: ${order_event['amount']} | "
                  f"Status: {order_event['order_status']} | "
                  f"Time: {order_event['order_timestamp']}")

            current_order_id = current_order_id + 1

            # Sleep for 2 seconds before generating the next order
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        # Flush ensures all buffered messages are sent before closing
        producer.flush()
        producer.close()
        print("Producer closed.")


if __name__ == "__main__":
    run_producer()
