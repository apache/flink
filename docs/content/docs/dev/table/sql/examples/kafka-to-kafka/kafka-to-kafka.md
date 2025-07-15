## Kafka to Kafka Example with Flink SQL

This example demonstrates how to use **Apache Flink SQL** to consume JSON messages from a Kafka topic, **transform and filter** the data, and then produce the output to another Kafka topic.

---

## Set Up Kafka Topics

You can use the Kafka Docker container to create the required topics.

> ℹ️ This example assumes you're running Kafka via Docker. If not, adjust the paths accordingly.
> 
> The Kafka Docker image can be pulled from [Docker Hub](https://hub.docker.com/r/bitnami/kafka) or [Confluent](https://hub.docker.com/r/confluentinc/cp-kafka).

```bash
# Create input topic
docker exec -it docker-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic input-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Create output topic
docker exec -it docker-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic output-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
````
---

## Producer Script

Run the following Python script to send JSON messages to the `input-topic`.

```bash
python producer.py
```

Sample output:

```
Sending: {'name': 'Sheila Grant', 'email': 'sylvia83@example.net', 'role': 'developer'}
Sending: {'name': 'Crystal Osborne', 'email': 'travis02@example.net', 'role': 'architect'}
...
✅ All messages sent.
```
---

## Define Flink SQL Tables

Use the **Flink SQL CLI** to define the source and sink tables:

```sql
-- Source: Input Table (Kafka Consumer)
CREATE TABLE kafka_input (
  name STRING,
  email STRING,
  role STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'input-topic',
  'properties.bootstrap.servers' = 'kafka:9093',
  'properties.group.id' = 'flink-i-consumer',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- Sink: Output Table (Kafka Producer)
CREATE TABLE kafka_output (
  name STRING,
  email STRING,
  role STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'output-topic',
  'properties.bootstrap.servers' = 'kafka:9093',
  'format' = 'json'
);
```
---

## Submit SQL Job

Run the following SQL query:

```sql
INSERT INTO kafka_output
SELECT
  UPPER(name) AS name,    -- transformation: convert name to uppercase
  email,
  role
FROM kafka_input
WHERE role = 'developer'; -- filter: only include developers
```


**Sample output:**

```
[INFO] SQL update statement has been successfully submitted to the cluster:
Job ID: 2d034349fb5fa2af873658e7abdb0579
```
---
## Consumer Script

Run the consumer to read messages from the `output-topic`.

```bash
python consumer.py
```

**Sample output:**

```
🔹 Received message: {'name': 'SHEILA GRANT', 'email': 'sylvia83@example.net', 'role': 'developer'}
🔹 Received message: {'name': 'ROBYN MORROW', 'email': 'katelynjackson@example.net', 'role': 'developer'}
🔹 Received message: {'name': 'KAYLA ROBINSON', 'email': 'amygreer@example.net', 'role': 'developer'}
```

## Summary

This example demonstrates how **Apache Flink SQL** can be used to build an end-to-end Kafka streaming pipeline. It highlights how to:

* **Transform** data using SQL functions like `UPPER()`
* **Filter** records using a `WHERE` clause
* Integrate Kafka as both **source and sink** with Flink SQL in real time.

---
