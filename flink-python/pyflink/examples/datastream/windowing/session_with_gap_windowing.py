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

import argparse
from typing import Iterable

from pyflink.common import Types, WatermarkStrategy, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.window import CountWindow, EventTimeSessionWindows,\
    SessionWindowTimeGapExtractor


class TimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value[1])


class MySessionWindowTimeGapExtractor(SessionWindowTimeGapExtractor):

    def extract(self, element: tuple) -> int:
        return element[1]


class CountWindowProcessFunction(ProcessWindowFunction[tuple, tuple, str, CountWindow]):
    def process(self,
                key: str,
                content: ProcessWindowFunction.Context,
                elements: Iterable[tuple]) -> Iterable[tuple]:
        print(elements)
        return [(key, len([e for e in elements]))]

    def clear(self, context: ProcessWindowFunction.Context) -> None:
        pass


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    data_stream = env.from_collection([
        ('hi', 1), ('hi', 2), ('hi', 3), ('hi', 4), ('hi', 8), ('hi', 9), ('hi', 15)],
        type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
        .with_timestamp_assigner(TimestampAssigner())

    data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda x: x[0], key_type=Types.STRING()) \
        .window(EventTimeSessionWindows.with_gap(Time.seconds(5))) \
        .process(CountWindowProcessFunction(), Types.TUPLE([Types.STRING(), Types.INT()])) \
        .print()

    env.execute()

