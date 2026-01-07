################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""
Example demonstrating RowFieldExtractorSchema usage with Kafka.

This example shows how to use RowFieldExtractorSchema to serialize specific
Row fields as Kafka message keys and values. The schema requires fields to
be byte arrays, giving you full control over serialization.

Requirements:
    - Kafka running on localhost:9092
    - Topic 'row-extractor-example' created
    - Kafka connector JAR in classpath

Usage:
    python kafka_row_field_extractor_example.py
"""

import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee
)
from pyflink.common.serialization import RowFieldExtractorSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Row


def serialize_to_json_bytes(data):
    """Helper function to serialize data to JSON byte array."""
    return json.dumps(data).encode('utf-8')


def serialize_key_bytes(key):
    """Helper function to serialize a key to byte array."""
    return key.encode('utf-8')


def create_sample_data():
    """
    Create sample e-commerce events.
    Each event has: user_id (key) and event_data (value).
    """
    events = [
        {
            "user_id": "user-001",
            "event": {"type": "purchase", "item": "laptop", "price": 999.99}
        },
        {
            "user_id": "user-002",
            "event": {"type": "view", "item": "phone", "timestamp": "2024-01-07T10:30:00"}
        },
        {
            "user_id": "user-001",
            "event": {"type": "add_to_cart", "item": "mouse", "quantity": 2}
        },
        {
            "user_id": "user-003",
            "event": {"type": "purchase", "item": "keyboard", "price": 79.99}
        },
    ]

    # Convert to Rows with byte array fields
    rows = []
    for event in events:
        row = Row(
            serialize_key_bytes(event["user_id"]),      # Field 0: user_id as bytes (Kafka key)
            serialize_to_json_bytes(event["event"])     # Field 1: event as JSON bytes (Kafka value)
        )
        rows.append(row)

    return rows


def kafka_row_field_extractor_example():
    """
    Demonstrate RowFieldExtractorSchema with Kafka Sink.

    This example:
    1. Creates sample e-commerce events
    2. Converts user_id to bytes (for Kafka key)
    3. Converts event data to JSON bytes (for Kafka value)
    4. Uses RowFieldExtractorSchema to extract and send to Kafka
    """

    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Generate sample data
    data = create_sample_data()

    # Create DataStream with proper type information
    # Row has 2 fields: both are byte arrays
    ds = env.from_collection(
        data,
        type_info=Types.ROW([
            Types.PRIMITIVE_ARRAY(Types.BYTE()),  # Field 0: user_id (key)
            Types.PRIMITIVE_ARRAY(Types.BYTE())   # Field 1: event_data (value)
        ])
    )

    # Optional: Print what we're sending (for debugging)
    def print_event(row):
        user_id = row[0].decode('utf-8')
        event_data = json.loads(row[1].decode('utf-8'))
        print(f"Sending: User={user_id}, Event={event_data}")
        return row

    ds = ds.map(
        print_event,
        output_type=Types.ROW([
            Types.PRIMITIVE_ARRAY(Types.BYTE()),
            Types.PRIMITIVE_ARRAY(Types.BYTE())
        ])
    )

    # Create Kafka Sink with RowFieldExtractorSchema
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
        .set_topic("row-extractor-example")
        # Extract field 0 (user_id) as Kafka key for partitioning
        .set_key_serialization_schema(RowFieldExtractorSchema(0))
        # Extract field 1 (event_data) as Kafka value
        .set_value_serialization_schema(RowFieldExtractorSchema(1))
        .build()
    ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    # Send to Kafka
    ds.sink_to(kafka_sink)

    # Execute
    env.execute("Kafka RowFieldExtractorSchema Example")


if __name__ == '__main__':
    print("=" * 70)
    print("Kafka RowFieldExtractorSchema Example")
    print("=" * 70)
    print("\nMake sure:")
    print("  1. Kafka is running on localhost:9092")
    print("  2. Topic 'row-extractor-example' exists")
    print("  3. Kafka connector JAR is in classpath")
    print("\nTo create topic:")
    print("  kafka-topics.sh --create --topic row-extractor-example \\")
    print("    --bootstrap-server localhost:9092 --partitions 3")
    print("\nTo consume messages:")
    print("  kafka-console-consumer.sh --bootstrap-server localhost:9092 \\")
    print("    --topic row-extractor-example --from-beginning \\")
    print("    --property print.key=true --property key.separator=' => '")
    print("\n" + "=" * 70 + "\n")

    # Run the example
    kafka_row_field_extractor_example()
