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

from pyflink.common.serialization import JsonRowSerializationSchema, \
    JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaProducer, FlinkKafkaConsumer

from functions import m_flat_map, add_one


def python_data_stream_example():
    env = StreamExecutionEnvironment.get_execution_environment()

    source_type_info = Types.ROW([Types.STRING(), Types.INT()])
    json_row_deserialization_schema = JsonRowDeserializationSchema.builder()\
        .type_info(source_type_info).build()
    source_topic = 'test-python-data-stream-source'
    consumer_props = {'bootstrap.servers': 'localhost:9092', 'group.id': 'pyflink-e2e-source'}
    kafka_consumer_1 = FlinkKafkaConsumer(source_topic, json_row_deserialization_schema,
                                          consumer_props)
    kafka_consumer_1.set_start_from_earliest()
    source_stream_1 = env.add_source(kafka_consumer_1).name('kafka source 1')
    mapped_type_info = Types.ROW([Types.STRING(), Types.INT(), Types.INT()])

    keyed_stream = source_stream_1.map(add_one, output_type=mapped_type_info) \
        .key_by(lambda x: x[2])

    flat_mapped_stream = keyed_stream.flat_map(m_flat_map, result_type=mapped_type_info)
    flat_mapped_stream.name("flat-map").set_parallelism(3)

    sink_topic = 'test-python-data-stream-sink'
    producer_props = {'bootstrap.servers': 'localhost:9092', 'group.id': 'pyflink-e2e-1'}
    json_row_serialization_schema = JsonRowSerializationSchema.builder()\
        .with_type_info(mapped_type_info).build()
    kafka_producer = FlinkKafkaProducer(topic=sink_topic, producer_config=producer_props,
                                        serialization_schema=json_row_serialization_schema)
    flat_mapped_stream.add_sink(kafka_producer)
    env.execute_async("test data stream to kafka")


if __name__ == '__main__':
    python_data_stream_example()
