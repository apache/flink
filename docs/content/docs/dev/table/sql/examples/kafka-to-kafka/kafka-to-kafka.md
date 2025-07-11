This example demonstrates how to use **Apache Flink SQL** to consume JSON messages from a Kafka topic, process the data by transforming and filtering it, and then produce the output to another Kafka topic.

## Set Up Kafka Topics

```bash
# Create input topic
docker exec -it docker-kafka-1 kafka-topics.sh \
  --create --topic i-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

# Create output topic
docker exec -it docker-kafka-1 kafka-topics.sh \
  --create --topic o-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
````

## Producer Script

Run the following Python script to send JSON messages to the `i-topic`.

```bash
C:\tmp\flink-sql\kafka-scripts>python producer.py
```

Sample output:

```
Sending: {'name': 'Sheila Grant', 'email': 'sylvia83@example.net', 'role': 'developer'}
Sending: {'name': 'Crystal Osborne', 'email': 'travis02@example.net', 'role': 'architect'}
...
✅ All messages sent.
```

## Define Flink SQL Tables

In Flink SQL CLI:

```sql
-- Input Table
CREATE TABLE kafka_input (
  name STRING,
  email STRING,
  role STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'i-topic',
  'properties.bootstrap.servers' = 'kafka:9093',
  'properties.group.id' = 'flink-i-consumer',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- Output Table
CREATE TABLE kafka_output (
  name STRING,
  email STRING,
  role STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'o-topic',
  'properties.bootstrap.servers' = 'kafka:9093',
  'format' = 'json'
);
```

## Submit SQL Job

```sql
INSERT INTO kafka_output
SELECT
  UPPER(name) AS name,
  email,
  role
FROM kafka_input
WHERE role = 'developer';
```

Output:

```
[INFO] SQL update statement has been successfully submitted to the cluster:
Job ID: 2d034349fb5fa2af873658e7abdb0579
```

## Consumer Script

Run the consumer to listen on the `o-topic`.

```bash
C:\tmp\flink-sql\kafka-scripts>python consumer.py
```

Sample output:

```
🔹 Received message: {'name': 'SHEILA GRANT', 'email': 'sylvia83@example.net', 'role': 'developer'}
🔹 Received message: {'name': 'ROBYN MORROW', 'email': 'katelynjackson@example.net', 'role': 'developer'}
🔹 Received message: {'name': 'KAYLA ROBINSON', 'email': 'amygreer@example.net', 'role': 'developer'}
```

## Summary

This example shows how Flink SQL can be used to build an end-to-end Kafka pipeline with transformation and filtering logic in real time.
