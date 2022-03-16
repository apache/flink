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

from typing import Iterable

from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.data_stream import DataStream
from pyflink.datastream.functions import (ProcessWindowFunction, WindowFunction)
from pyflink.datastream.window import (TumblingEventTimeWindows,
                                       SlidingEventTimeWindows, EventTimeSessionWindows,
                                       CountSlidingWindowAssigner, SessionWindowTimeGapExtractor,
                                       CountWindow)
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase


class WindowTests(PyFlinkStreamingTestCase):

    def setUp(self) -> None:
        super(WindowTests, self).setUp()
        self.test_sink = DataStreamTestSinkFunction()

    def tearDown(self) -> None:
        self.test_sink.clear()

    def assert_equals_sorted(self, expected, actual):
        expected.sort()
        actual.sort()
        self.assertEqual(expected, actual)

    def test_event_time_tumbling_window(self):
        data_stream = self.env.from_collection([
            ('hi', 1), ('hi', 2), ('hi', 3), ('hi', 4), ('hi', 5), ('hi', 8), ('hi', 9),
            ('hi', 15)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())
        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .key_by(lambda x: x[0], key_type=Types.STRING()) \
            .window(TumblingEventTimeWindows.of(Time.milliseconds(5))) \
            .process(CountWindowProcessFunction(), Types.TUPLE([Types.STRING(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_event_time_tumbling_window')
        results = self.test_sink.get_results()
        expected = ['(hi,4)', '(hi,3)', '(hi,1)']
        self.assert_equals_sorted(expected, results)

    def test_count_tumbling_window(self):
        data_stream = self.env.from_collection([
            (1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello'),
            (6, 'hello')],
            type_info=Types.TUPLE([Types.INT(), Types.STRING()]))  # type: DataStream
        data_stream.key_by(lambda x: x[1], key_type=Types.STRING()) \
            .count_window(3) \
            .apply(SumWindowFunction(), Types.TUPLE([Types.STRING(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_count_tumbling_window')
        results = self.test_sink.get_results()
        expected = ['(hi,9)', '(hello,12)']
        self.assert_equals_sorted(expected, results)

    def test_event_time_sliding_window(self):
        data_stream = self.env.from_collection([
            ('hi', 1), ('hi', 2), ('hi', 3), ('hi', 4), ('hi', 5), ('hi', 8), ('hi', 9),
            ('hi', 15)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())

        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .key_by(lambda x: x[0], key_type=Types.STRING()) \
            .window(SlidingEventTimeWindows.of(Time.milliseconds(5), Time.milliseconds(2))) \
            .process(CountWindowProcessFunction(), Types.TUPLE([Types.STRING(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_event_time_sliding_window')
        results = self.test_sink.get_results()
        expected = ['(hi,2)', '(hi,4)', '(hi,4)', '(hi,3)', '(hi,2)', '(hi,2)', '(hi,1)', '(hi,1)']
        self.assert_equals_sorted(expected, results)

    def test_count_sliding_window(self):
        data_stream = self.env.from_collection([
            (1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello')],
            type_info=Types.TUPLE([Types.INT(), Types.STRING()]))  # type: DataStream
        data_stream.key_by(lambda x: x[1], key_type=Types.STRING()) \
            .window(CountSlidingWindowAssigner(2, 1)) \
            .apply(SumWindowFunction(), Types.TUPLE([Types.STRING(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_count_sliding_window')
        results = self.test_sink.get_results()
        expected = ['(hello,6)', '(hi,8)', '(hi,4)', '(hello,10)']
        self.assert_equals_sorted(expected, results)

    def test_event_time_session_window(self):
        data_stream = self.env.from_collection([
            ('hi', 1), ('hi', 2), ('hi', 3), ('hi', 4), ('hi', 8), ('hi', 9), ('hi', 15)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())

        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .key_by(lambda x: x[0], key_type=Types.STRING()) \
            .window(EventTimeSessionWindows.with_gap(Time.milliseconds(5))) \
            .process(CountWindowProcessFunction(), Types.TUPLE([Types.STRING(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_event_time_session_window')
        results = self.test_sink.get_results()
        expected = ['(hi,1)', '(hi,6)']
        self.assert_equals_sorted(expected, results)

    def test_event_time_dynamic_gap_session_window(self):
        self.env.set_parallelism(1)
        data_stream = self.env.from_collection([
            ('hi', 1), ('hi', 2), ('hi', 3), ('hi', 4), ('hi', 9), ('hi', 9), ('hi', 15)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())

        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .key_by(lambda x: x[0], key_type=Types.STRING()) \
            .window(EventTimeSessionWindows.with_dynamic_gap(MySessionWindowTimeGapExtractor())) \
            .process(CountWindowProcessFunction(), Types.TUPLE([Types.STRING(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_event_time_dynamic_gap_session_window')
        results = self.test_sink.get_results()
        expected = ['(hi,3)', '(hi,4)']
        self.assert_equals_sorted(expected, results)


class SecondColumnTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value[1])


class MySessionWindowTimeGapExtractor(SessionWindowTimeGapExtractor):

    def extract(self, element: tuple) -> int:
        return element[1]


class SumWindowFunction(WindowFunction[tuple, tuple, str, CountWindow]):

    def apply(self, key: str, window: CountWindow, inputs: Iterable[tuple]):
        result = 0
        for i in inputs:
            result += i[0]
        return [(key, result)]


class CountWindowProcessFunction(ProcessWindowFunction[tuple, tuple, str, CountWindow]):

    def process(self,
                key: str,
                content: ProcessWindowFunction.Context,
                elements: Iterable[tuple]) -> Iterable[tuple]:
        return [(key, len([e for e in elements]))]

    def clear(self, context: ProcessWindowFunction.Context) -> None:
        pass
