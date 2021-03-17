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

from typing import Any

from pyflink.common import Duration
from pyflink.common.serialization import SimpleStringSchema, JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.datastream.functions import KeyedProcessFunction

from functions import MyKeySelector


def python_data_stream_example():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Set the parallelism to be one to make sure that all data including fired timer and normal data
    # are processed by the same worker and the collected result would be in order which is good for
    # assertion.
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    type_info = Types.ROW_NAMED(['createTime', 'orderId', 'payAmount', 'payPlatform', 'provinceId'],
                                [Types.LONG(), Types.LONG(), Types.DOUBLE(), Types.INT(),
                                 Types.INT()])
    json_row_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()
    kafka_props = {'bootstrap.servers': 'localhost:9092', 'group.id': 'pyflink-e2e-source'}

    kafka_consumer = FlinkKafkaConsumer("timer-stream-source", json_row_schema, kafka_props)
    kafka_producer = FlinkKafkaProducer("timer-stream-sink", SimpleStringSchema(), kafka_props)

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))\
        .with_timestamp_assigner(KafkaRowTimestampAssigner())

    kafka_consumer.set_start_from_earliest()
    ds = env.add_source(kafka_consumer).assign_timestamps_and_watermarks(watermark_strategy)
    ds.key_by(MyKeySelector(), key_type_info=Types.LONG()) \
        .process(MyProcessFunction(), output_type=Types.STRING()) \
        .add_sink(kafka_producer)
    env.execute_async("test data stream timer")


class MyProcessFunction(KeyedProcessFunction):

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        result = "Current key: {}, orderId: {}, payAmount: {}, timestamp: {}".format(
            str(ctx.get_current_key()), str(value[1]), str(value[2]), str(ctx.timestamp()))
        yield result
        current_watermark = ctx.timer_service().current_watermark()
        ctx.timer_service().register_event_time_timer(current_watermark + 1500)

    def on_timer(self, timestamp, ctx: 'KeyedProcessFunction.OnTimerContext'):
        yield "On timer timestamp: " + str(timestamp)


class KafkaRowTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return int(value[0])


if __name__ == '__main__':
    python_data_stream_example()
