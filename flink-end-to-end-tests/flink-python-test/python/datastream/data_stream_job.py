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
import sys
from typing import Any

from pyflink.common import Duration, Encoder, Row
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import (FileSink, FileSource, RollingPolicy,
                                                       StreamFormat)
from pyflink.datastream.functions import KeyedProcessFunction

from functions import MyKeySelector


def python_data_stream_example(input_path: str, output_path: str):
    env = StreamExecutionEnvironment.get_execution_environment()
    # Process everything on one worker so the timer behavior is deterministic and the output
    # lands in a single part file.
    env.set_parallelism(1)
    # No periodic watermarks: current_watermark() stays at Long.MIN_VALUE until the bounded
    # source emits MAX_WATERMARK at end of input, making the fired-timer output deterministic.
    env.get_config().set_auto_watermark_interval(0)

    type_info = Types.ROW_NAMED(['createTime', 'orderId', 'payAmount', 'payPlatform', 'provinceId'],
                                [Types.LONG(), Types.LONG(), Types.DOUBLE(), Types.INT(),
                                 Types.INT()])

    source = FileSource.for_record_stream_format(StreamFormat.text_line_format(), input_path) \
        .process_static_file_set().build()

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(PaymentTimestampAssigner())

    sink = FileSink.for_row_format(output_path, Encoder.simple_string_encoder()) \
        .with_rolling_policy(RollingPolicy.default_rolling_policy()).build()

    # Watermarks are assigned after parsing because the timestamp field is not available on the
    # raw text lines emitted by the source.
    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), 'payment_msg_source')
    ds.map(parse_payment_msg, output_type=type_info) \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(MyKeySelector(), key_type=Types.LONG()) \
        .process(MyProcessFunction(), output_type=Types.STRING()) \
        .sink_to(sink)
    # The committer commits all pending files at end of input when checkpointing is disabled,
    # so no checkpoint config is needed for the output to materialize.
    env.execute('test data stream timer')


def parse_payment_msg(line: str) -> Row:
    msg = json.loads(line)
    return Row(msg['createTime'], msg['orderId'], msg['payAmount'], msg['payPlatform'],
               msg['provinceId'])


class MyProcessFunction(KeyedProcessFunction):

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        result = "Current key: {}, orderId: {}, payAmount: {}, timestamp: {}".format(
            str(ctx.get_current_key()), str(value[1]), str(value[2]), str(ctx.timestamp()))
        yield result
        current_watermark = ctx.timer_service().current_watermark()
        ctx.timer_service().register_event_time_timer(current_watermark + 1500)

    def on_timer(self, timestamp, ctx: 'KeyedProcessFunction.OnTimerContext'):
        yield "On timer timestamp: " + str(timestamp)


class PaymentTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return int(value[0])


if __name__ == '__main__':
    python_data_stream_example(sys.argv[1], sys.argv[2])
