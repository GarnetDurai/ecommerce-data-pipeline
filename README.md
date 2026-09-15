# E-Commerce Real-Time Data Pipeline

A beginner-friendly, end-to-end Data Engineering project built for learning core streaming architecture and interview preparation (e.g., Tiger Analytics).

---

## Architecture (Phase 4)

```text
E-Commerce Event Generator
        ↓
  Kafka Producer (producer.py)
        ↓
  Kafka Topic (ecommerce-orders)
        ↓
  PySpark Structured Streaming (spark/pipeline.py)
        ↓
  1. Cleaning: normalize order_status (trim, uppercase)
  2. Validation: enforce contracts (split into valid & invalid quarantine)
  3. Transformation: total_value = quantity * amount
  4. Storage Sink: Write valid processed orders to Parquet (data/processed/)
  5. Aggregation & Console: Group by customer_id (total_sales, order_count)
```

---

## Project Structure

```text
ecommerce-data-pipeline/
│
├── docker/
│   └── docker-compose.yml       # Starts Apache Kafka in KRaft mode (port 9092)
│
├── config/
│   └── config.py                # Broker URL, topic name, checkpoint path, data path
│
├── producer/
│   └── producer.py              # Generates order events and publishes to Kafka
│
├── consumer/
│   └── consumer.py              # Simple Kafka test consumer (Phase 1)
│
├── spark/
│   └── pipeline.py              # PySpark Structured Streaming pipeline (Phases 2 - 4)
│
├── data/
│   ├── checkpoints/             # Streaming state checkpoint directories
│   └── processed/               # Snappy-compressed Parquet files (Phase 4)
│
├── requirements.txt             # Project dependencies
└── README.md                    # Setup and guide
```

---

## Event Schema

Every event published to the `ecommerce-orders` topic has the following structure:

```json
{
    "order_id": 1001,
    "customer_id": 101,
    "product_id": 501,
    "quantity": 2,
    "amount": 1499.00,
    "order_status": "COMPLETED",
    "order_timestamp": "2026-09-13T10:30:00"
}
```

---

## How to Run Phase 1

### Step 1: Install Python Dependencies
```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install requirements
pip install -r requirements.txt
```

### Step 2: Start Kafka
In **Terminal 1**:
```bash
docker compose -f docker/docker-compose.yml up
```
*(Wait until Kafka initializes and is listening on port 9092)*

### Step 3: Start the Consumer
In **Terminal 2**:
```bash
source .venv/bin/activate
python consumer/consumer.py
```
*(The consumer will connect and wait for incoming messages)*

### Step 4: Start the Producer
In **Terminal 3**:
```bash
source .venv/bin/activate
python producer/producer.py
```
*(The producer will generate an order every 2 seconds and send it to Kafka)*

---

## Verification

When both the producer and consumer are running, you will see real-time output in both terminals:

- **Terminal 3 (Producer):**
  ```text
  [PRODUCER SENT] Order ID: 1001 | Customer: 102 | Amount: $1499.0 | Status: COMPLETED | Time: 2026-09-13T17:30:00
  [PRODUCER SENT] Order ID: 1002 | Customer: 105 | Amount: $2998.0 | Status: PENDING | Time: 2026-09-13T17:30:02
  ```

- **Terminal 2 (Consumer):**
  ```text
  [CONSUMER RECEIVED] Order ID: 1001 | Customer: 102 | Product: 503 | Qty: 1 | Amount: $1499.0 | Status: COMPLETED | Timestamp: 2026-09-13T17:30:00
  [CONSUMER RECEIVED] Order ID: 1002 | Customer: 105 | Product: 501 | Qty: 2 | Amount: $2998.0 | Status: PENDING | Timestamp: 2026-09-13T17:30:02
  ```
