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
import logging
import sys

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (KafkaRecordSerializationSchema, KafkaSink,
                                                 KafkaSource, KafkaOffsetsInitializer)
from pyflink.datastream.formats.avro import AvroRowSerializationSchema, AvroRowDeserializationSchema


# Make sure that the Kafka cluster is started and the topic 'test_avro_topic' is
# created before executing this job.
def write_to_kafka(env):
    ds = env.from_collection([
        (1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello'), (6, 'hello')],
        type_info=Types.ROW([Types.INT(), Types.STRING()]))

    serialization_schema = AvroRowSerializationSchema(
        avro_schema_string="""
            {
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"}
                ]
            }"""
    )

    record_serializer = KafkaRecordSerializationSchema.builder() \
        .set_topic('test_avro_topic') \
        .set_value_serialization_schema(serialization_schema) \
        .build()
    kafka_sink = (
        KafkaSink.builder()
        .set_record_serializer(record_serializer)
        .set_bootstrap_servers('localhost:9092')
        .set_property("group.id", "test_group")
        .build()
    )

    # note that the output type of ds must be RowTypeInfo
    ds.sink_to(kafka_sink)
    env.execute()


def read_from_kafka(env):
    deserialization_schema = AvroRowDeserializationSchema(
        avro_schema_string="""
            {
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"}
                ]
            }"""
    )

    kafka_source = (
        KafkaSource.builder()
        .set_topics('test_avro_topic')
        .set_value_only_deserializer(deserialization_schema)
        .set_properties({'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group_1'})
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .build()
    )

    ds = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka source"
    )

    ds.print()
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///path/to/flink-sql-avro-1.15.0.jar",
                 "file:///path/to/flink-sql-connector-kafka-1.15.0.jar")

    print("start writing data to kafka")
    write_to_kafka(env)

    print("start reading data from kafka")
    read_from_kafka(env)
