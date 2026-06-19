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
import json
import logging
import sys

from pyflink.common import Types, ByteArraySchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, \
    KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema


# This example works since Flink 2.0 since ByteArraySchema was introduced in Flink 2.0

# Make sure that the Kafka cluster is started and the topic 'test_json_topic' is
# created before executing this job.
def write_to_kafka(env):
    data = [
        (json.dumps({
            "id": 1,
            "country": "USA"
        }).encode("utf-8"),),
        (json.dumps({
            "id": 2,
            "country": "Canada"
        }).encode("utf-8"),),
        (json.dumps({
            "id": 3,
            "country": "Germany"
        }).encode("utf-8"),)
    ]
    type_info = Types.ROW([Types.PRIMITIVE_ARRAY(Types.BYTE())])
    ds = env.from_collection(data, type_info=type_info)

    # declare the output type as Types.PRIMITIVE_ARRAY(Types.BYTE()),
    # otherwise, Types.PICKLED_BYTE_ARRAY() will be used by default, it will
    # use pickler to serialize the result byte array which is unnecessary
    ds = ds.map(lambda x: x[0], output_type=Types.PRIMITIVE_ARRAY(Types.BYTE()))

    record_serializer = KafkaRecordSerializationSchema.builder() \
        .set_topic('test_bytearray_topic') \
        .set_value_serialization_schema(ByteArraySchema()) \
        .build()
    kafka_sink = (
        KafkaSink.builder()
        .set_record_serializer(record_serializer)
        .set_bootstrap_servers('localhost:9092')
        .set_property("group.id", "test_group")
        .build()
    )

    ds.sink_to(kafka_sink)
    env.execute()


def read_from_kafka(env):
    kafka_source = (
        KafkaSource.builder()
        .set_topics('test_bytearray_topic')
        .set_value_only_deserializer(ByteArraySchema())
        .set_properties({'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group_1'})
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .build()
    )

    ds = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka source"
    )

    # the data read out from the source is byte array, decode it as a string
    ds.map(lambda data: data.decode("utf-8")).print()
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///path/to/flink-sql-connector-kafka-1.15.0.jar")

    print("start writing data to kafka")
    write_to_kafka(env)

    print("start reading data from kafka")
    read_from_kafka(env)
