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

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaProducer
from pyflink.datastream.functions import ProcessFunction, Collector
from pyflink.table import StreamTableEnvironment

from functions import MyKeySelector


def python_data_stream_example():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Set the parallelism to be one to make sure that all data including fired timer and normal data
    # are processed by the same worker and the collected result would be in order which is good for
    # assertion.
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    create_kafka_source_ddl = """
                CREATE TABLE payment_msg(
                    createTime VARCHAR,
                    rt as TO_TIMESTAMP(createTime),
                    orderId BIGINT,
                    payAmount DOUBLE,
                    payPlatform INT,
                    provinceId INT,
                    WATERMARK FOR rt as rt - INTERVAL '2' SECOND
                ) WITH (
                  'connector.type' = 'kafka',
                  'connector.version' = 'universal',
                  'connector.topic' = 'timer-stream-source',
                  'connector.properties.bootstrap.servers' = 'localhost:9092',
                  'connector.properties.group.id' = 'test_3',
                  'connector.startup-mode' = 'earliest-offset',
                  'format.type' = 'json'
                )
                """
    t_env.execute_sql(create_kafka_source_ddl)
    t = t_env.from_path("payment_msg").select("createTime, orderId, payAmount, payPlatform,"
                                              " provinceId")
    source_type_info = Types.ROW([
        Types.STRING(),
        Types.LONG(),
        Types.DOUBLE(),
        Types.INT(),
        Types.INT()])
    ds = t_env.to_append_stream(table=t, type_info=source_type_info)
    producer_props = {'bootstrap.servers': 'localhost:9092', 'group.id': 'pyflink-e2e-source'}
    kafka_producer = FlinkKafkaProducer("timer-stream-sink", SimpleStringSchema(),
                                        producer_props)
    ds.key_by(MyKeySelector(), key_type_info=Types.LONG()) \
        .process(MyProcessFunction(), output_type=Types.STRING()) \
        .add_sink(kafka_producer)
    env.execute_async("test data stream timer")


class MyProcessFunction(ProcessFunction):

    def process_element(self, value, ctx: 'ProcessFunction.Context', out: Collector):
        result = "Current orderId: " + str(value[1]) + " payAmount: " + str(value[2])
        out.collect(result)
        current_watermark = ctx.timer_service().current_watermark()
        ctx.timer_service().register_event_time_timer(current_watermark + 1500)

    def on_timer(self, timestamp, ctx: 'ProcessFunction.OnTimerContext', out: 'Collector'):
        out.collect("On timer timestamp: " + str(timestamp))


if __name__ == '__main__':
    python_data_stream_example()
