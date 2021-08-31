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

from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


class Sum(KeyedProcessFunction):

    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ValueStateDescriptor("state", Types.PICKLED_BYTE_ARRAY())
        self.state = runtime_context.get_state(state_descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # retrieve the current count
        current = self.state.value()
        if current is None:
            current = 0

        # update the state's count
        current += value[2]
        self.state.update(current)

        # register an event time timer 2 seconds later
        ctx.timer_service().register_event_time_timer(ctx.timestamp() + 2000)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        yield ctx.get_current_key(), self.state.value()


class MyTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return int(value[0])


def event_timer_timer_demo():
    env = StreamExecutionEnvironment.get_execution_environment()

    ds = env.from_collection(
        collection=[
            (1000, 'Alice', 110.1),
            (4000, 'Bob', 30.2),
            (3000, 'Alice', 20.0),
            (2000, 'Bob', 53.1),
            (5000, 'Alice', 13.1),
            (3000, 'Bob', 3.1),
            (7000, 'Bob', 16.1),
            (10000, 'Alice', 20.1)
        ])

    ds = ds.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(2))
                         .with_timestamp_assigner(MyTimestampAssigner()))

    # apply the process function onto a keyed stream
    ds.key_by(lambda value: value[1]) \
      .process(Sum()) \
      .print()

    # submit for execution
    env.execute()


if __name__ == '__main__':
    event_timer_timer_demo()
