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
from typing import Iterable, Tuple, Dict

from pyflink.common import Configuration
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.data_stream import DataStream
from pyflink.datastream.functions import (ProcessWindowFunction, WindowFunction, AggregateFunction,
                                          ProcessAllWindowFunction)
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.window import (TumblingEventTimeWindows,
                                       SlidingEventTimeWindows, EventTimeSessionWindows,
                                       CountSlidingWindowAssigner, SessionWindowTimeGapExtractor,
                                       CountWindow, PurgingTrigger, EventTimeTrigger, TimeWindow,
                                       GlobalWindows, CountTrigger)
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase
from pyflink.util.java_utils import get_j_env_configuration


class WindowTests(object):

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
            .process(CountWindowProcessFunction(),
                     Types.TUPLE([Types.STRING(), Types.LONG(), Types.LONG(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_event_time_tumbling_window')
        results = self.test_sink.get_results()
        expected = ['(hi,0,5,4)', '(hi,5,10,3)', '(hi,15,20,1)']
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
            .process(CountWindowProcessFunction(),
                     Types.TUPLE([Types.STRING(), Types.LONG(), Types.LONG(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_event_time_sliding_window')
        results = self.test_sink.get_results()
        expected = ['(hi,-2,3,2)', '(hi,0,5,4)', '(hi,2,7,4)', '(hi,4,9,3)', '(hi,6,11,2)',
                    '(hi,8,13,2)', '(hi,12,17,1)', '(hi,14,19,1)']
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
            .process(CountWindowProcessFunction(),
                     Types.TUPLE([Types.STRING(), Types.LONG(), Types.LONG(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_event_time_session_window')
        results = self.test_sink.get_results()
        expected = ['(hi,1,14,6)', '(hi,15,20,1)']
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
            .process(CountWindowProcessFunction(),
                     Types.TUPLE([Types.STRING(), Types.LONG(), Types.LONG(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_event_time_dynamic_gap_session_window')
        results = self.test_sink.get_results()
        expected = ['(hi,1,8,4)', '(hi,9,30,3)']
        self.assert_equals_sorted(expected, results)

    def test_window_reduce_passthrough(self):
        data_stream = self.env.from_collection([
            ('a', 1), ('a', 2), ('b', 3), ('a', 6), ('b', 8), ('b', 9), ('a', 15)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())
        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .key_by(lambda x: x[0], key_type=Types.STRING()) \
            .window(EventTimeSessionWindows.with_gap(Time.milliseconds(2))) \
            .reduce(lambda a, b: (b[0], a[1] + b[1]),
                    output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_window_reduce_passthrough')
        results = self.test_sink.get_results()
        expected = ['(a,3)', '(a,6)', '(a,15)', '(b,3)', '(b,17)']
        self.assert_equals_sorted(expected, results)

    def test_window_reduce_process(self):
        data_stream = self.env.from_collection([
            ('a', 1), ('a', 2), ('b', 3), ('a', 6), ('b', 8), ('b', 9), ('a', 15)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())

        class MyProcessFunction(ProcessWindowFunction):

            def process(self, key, context: ProcessWindowFunction.Context,
                        elements: Iterable[Tuple[str, int]]) -> Iterable[str]:
                yield "current window start at {}, reduce result {}".format(
                    context.window().start,
                    next(iter(elements)),
                )

        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .key_by(lambda x: x[0], key_type=Types.STRING()) \
            .window(EventTimeSessionWindows.with_gap(Time.milliseconds(2))) \
            .reduce(lambda a, b: (b[0], a[1] + b[1]),
                    window_function=MyProcessFunction(),
                    output_type=Types.STRING()) \
            .add_sink(self.test_sink)

        self.env.execute('test_window_reduce_process')
        results = self.test_sink.get_results()
        expected = ["current window start at 1, reduce result ('a', 3)",
                    "current window start at 15, reduce result ('a', 15)",
                    "current window start at 3, reduce result ('b', 3)",
                    "current window start at 6, reduce result ('a', 6)",
                    "current window start at 8, reduce result ('b', 17)"]
        self.assert_equals_sorted(expected, results)

    def test_window_aggregate_passthrough(self):
        data_stream = self.env.from_collection([
            ('a', 1), ('a', 2), ('b', 3), ('a', 6), ('b', 8), ('b', 9), ('a', 15)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())

        class MyAggregateFunction(AggregateFunction):

            def create_accumulator(self) -> Tuple[str, Dict[int, int]]:
                return '', {0: 0, 1: 0}

            def add(self, value: Tuple[str, int], accumulator: Tuple[str, Dict[int, int]]
                    ) -> Tuple[str, Dict[int, int]]:
                number_map = accumulator[1]
                number_map[value[1] % 2] += 1
                return value[0], number_map

            def get_result(self, accumulator: Tuple[str, Dict[int, int]]) -> Tuple[str, int]:
                number_map = accumulator[1]
                return accumulator[0], number_map[0] - number_map[1]

            def merge(self, acc_a: Tuple[str, Dict[int, int]], acc_b: Tuple[str, Dict[int, int]]
                      ) -> Tuple[str, Dict[int, int]]:
                number_map_a = acc_a[1]
                number_map_b = acc_b[1]
                new_number_map = {
                    0: number_map_a[0] + number_map_b[0],
                    1: number_map_a[1] + number_map_b[1]
                }
                return acc_a[0], new_number_map

        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .key_by(lambda x: x[0], key_type=Types.STRING()) \
            .window(EventTimeSessionWindows.with_gap(Time.milliseconds(2))) \
            .aggregate(MyAggregateFunction(),
                       output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_window_aggregate_passthrough')
        results = self.test_sink.get_results()
        expected = ['(a,-1)', '(a,0)', '(a,1)', '(b,-1)', '(b,0)']
        self.assert_equals_sorted(expected, results)

    def test_window_aggregate_accumulator_type(self):
        data_stream = self.env.from_collection([
            ('a', 1), ('a', 2), ('b', 3), ('a', 6), ('b', 8), ('b', 9), ('a', 15)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())

        class MyAggregateFunction(AggregateFunction):

            def create_accumulator(self) -> Tuple[int, str]:
                return 0, ''

            def add(self, value: Tuple[str, int], accumulator: Tuple[int, str]) -> Tuple[int, str]:
                return value[1] + accumulator[0], value[0]

            def get_result(self, accumulator: Tuple[str, int]):
                return accumulator[1], accumulator[0]

            def merge(self, acc_a: Tuple[int, str], acc_b: Tuple[int, str]):
                return acc_a[0] + acc_b[0], acc_a[1]

        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .key_by(lambda x: x[0], key_type=Types.STRING()) \
            .window(EventTimeSessionWindows.with_gap(Time.milliseconds(2))) \
            .aggregate(MyAggregateFunction(),
                       accumulator_type=Types.TUPLE([Types.INT(), Types.STRING()]),
                       output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_window_aggregate_accumulator_type')
        results = self.test_sink.get_results()
        expected = ['(a,15)', '(a,3)', '(a,6)', '(b,17)', '(b,3)']
        self.assert_equals_sorted(expected, results)

    def test_window_aggregate_process(self):
        data_stream = self.env.from_collection([
            ('a', 1), ('a', 2), ('b', 3), ('a', 6), ('b', 8), ('b', 9), ('a', 15)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())

        class MyAggregateFunction(AggregateFunction):

            def create_accumulator(self) -> Tuple[int, str]:
                return 0, ''

            def add(self, value: Tuple[str, int], accumulator: Tuple[int, str]) -> Tuple[int, str]:
                return value[1] + accumulator[0], value[0]

            def get_result(self, accumulator: Tuple[str, int]):
                return accumulator[1], accumulator[0]

            def merge(self, acc_a: Tuple[int, str], acc_b: Tuple[int, str]):
                return acc_a[0] + acc_b[0], acc_a[1]

        class MyProcessWindowFunction(ProcessWindowFunction):

            def process(self, key: str, context: ProcessWindowFunction.Context,
                        elements: Iterable[Tuple[str, int]]) -> Iterable[str]:
                agg_result = next(iter(elements))
                yield "key {} timestamp sum {}".format(agg_result[0], agg_result[1])

        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .key_by(lambda x: x[0], key_type=Types.STRING()) \
            .window(EventTimeSessionWindows.with_gap(Time.milliseconds(2))) \
            .aggregate(MyAggregateFunction(),
                       window_function=MyProcessWindowFunction(),
                       accumulator_type=Types.TUPLE([Types.INT(), Types.STRING()]),
                       output_type=Types.STRING()) \
            .add_sink(self.test_sink)

        self.env.execute('test_window_aggregate_process')
        results = self.test_sink.get_results()
        expected = ['key a timestamp sum 15',
                    'key a timestamp sum 3',
                    'key a timestamp sum 6',
                    'key b timestamp sum 17',
                    'key b timestamp sum 3']
        self.assert_equals_sorted(expected, results)

    def test_session_window_late_merge(self):
        data_stream = self.env.from_collection([
            ('hi', 0), ('hi', 8), ('hi', 4)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())
        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .key_by(lambda x: x[0], key_type=Types.STRING()) \
            .window(EventTimeSessionWindows.with_gap(Time.milliseconds(5))) \
            .process(CountWindowProcessFunction(),
                     Types.TUPLE([Types.STRING(), Types.LONG(), Types.LONG(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_session_window_late_merge')
        results = self.test_sink.get_results()
        expected = ['(hi,0,13,3)']
        self.assert_equals_sorted(expected, results)

    def test_event_time_session_window_with_purging_trigger(self):
        data_stream = self.env.from_collection([
            ('hi', 1), ('hi', 2), ('hi', 3), ('hi', 4), ('hi', 8), ('hi', 9), ('hi', 15)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())

        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .key_by(lambda x: x[0], key_type=Types.STRING()) \
            .window(EventTimeSessionWindows.with_gap(Time.milliseconds(3))) \
            .trigger(PurgingTrigger.of(EventTimeTrigger.create())) \
            .process(CountWindowProcessFunction(),
                     Types.TUPLE([Types.STRING(), Types.LONG(), Types.LONG(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_event_time_session_window_with_purging_trigger')
        results = self.test_sink.get_results()
        expected = ['(hi,1,7,4)', '(hi,8,12,2)', '(hi,15,18,1)']
        self.assert_equals_sorted(expected, results)

    def test_global_window_with_purging_trigger(self):
        self.env.set_parallelism(1)
        data_stream = self.env.from_collection([
            ('hi', 1), ('hi', 1), ('hi', 1), ('hi', 1), ('hi', 1), ('hi', 1), ('hi', 1)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream

        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())

        class MyProcessFunction(ProcessWindowFunction):

            def process(self, key, context: ProcessWindowFunction.Context,
                        elements: Iterable[Tuple[str, int]]) -> Iterable[tuple]:
                return [(key, len([e for e in elements]))]

        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .key_by(lambda x: x[0], key_type=Types.STRING()) \
            .window(GlobalWindows.create()) \
            .trigger(PurgingTrigger.of(CountTrigger.of(2))) \
            .process(MyProcessFunction(), Types.TUPLE([Types.STRING(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_global_window_with_purging_trigger')
        results = self.test_sink.get_results()
        expected = ['(hi,2)', '(hi,2)', '(hi,2)']
        self.assert_equals_sorted(expected, results)

    def test_event_time_tumbling_window_all(self):
        data_stream = self.env.from_collection([
            ('hi', 1), ('hello', 2), ('hi', 3), ('hello', 4), ('hello', 5), ('hi', 8), ('hi', 9),
            ('hi', 15)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream

        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())

        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .window_all(TumblingEventTimeWindows.of(Time.milliseconds(5))) \
            .process(CountAllWindowProcessFunction(),
                     Types.TUPLE([Types.LONG(), Types.LONG(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_event_time_tumbling_window_all')
        results = self.test_sink.get_results()
        expected = ['(0,5,4)', '(15,20,1)', '(5,10,3)']
        self.assert_equals_sorted(expected, results)

    def test_window_all_reduce(self):
        self.env.set_parallelism(1)
        data_stream = self.env.from_collection([
            ('a', 1), ('a', 2), ('b', 3), ('a', 6), ('b', 8), ('b', 9), ('a', 15)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())
        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .window_all(EventTimeSessionWindows.with_gap(Time.milliseconds(2))) \
            .reduce(lambda a, b: (a[0], a[1] + b[1]),
                    output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_window_all_reduce')
        results = self.test_sink.get_results()
        expected = ['(a,15)', '(a,6)', '(a,23)']
        self.assert_equals_sorted(expected, results)

    def test_window_all_reduce_process(self):
        self.env.set_parallelism(1)
        data_stream = self.env.from_collection([
            ('a', 1), ('a', 2), ('b', 3), ('a', 6), ('b', 8), ('b', 9), ('a', 15)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())

        class MyProcessFunction(ProcessAllWindowFunction):

            def process(self, context: 'ProcessAllWindowFunction.Context',
                        elements: Iterable[Tuple[str, int]]) -> Iterable[str]:
                yield "current window start at {}, reduce result {}".format(
                    context.window().start,
                    next(iter(elements)),
                )

        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .window_all(EventTimeSessionWindows.with_gap(Time.milliseconds(2))) \
            .reduce(lambda a, b: (a[0], a[1] + b[1]),
                    window_function=MyProcessFunction(),
                    output_type=Types.STRING()) \
            .add_sink(self.test_sink)

        self.env.execute('test_window_all_reduce_process')
        results = self.test_sink.get_results()
        expected = ["current window start at 1, reduce result ('a', 6)",
                    "current window start at 6, reduce result ('a', 23)",
                    "current window start at 15, reduce result ('a', 15)"]
        self.assert_equals_sorted(expected, results)

    def test_window_all_aggregate(self):
        self.env.set_parallelism(1)
        data_stream = self.env.from_collection([
            ('a', 1), ('a', 2), ('b', 3), ('a', 6), ('b', 8), ('b', 9), ('a', 15)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())

        class MyAggregateFunction(AggregateFunction):

            def create_accumulator(self) -> Tuple[str, Dict[int, int]]:
                return '', {0: 0, 1: 0}

            def add(self, value: Tuple[str, int], accumulator: Tuple[str, Dict[int, int]]
                    ) -> Tuple[str, Dict[int, int]]:
                number_map = accumulator[1]
                number_map[value[1] % 2] += 1
                return value[0], number_map

            def get_result(self, accumulator: Tuple[str, Dict[int, int]]) -> Tuple[str, int]:
                number_map = accumulator[1]
                return accumulator[0], number_map[0] - number_map[1]

            def merge(self, acc_a: Tuple[str, Dict[int, int]], acc_b: Tuple[str, Dict[int, int]]
                      ) -> Tuple[str, Dict[int, int]]:
                number_map_a = acc_a[1]
                number_map_b = acc_b[1]
                new_number_map = {
                    0: number_map_a[0] + number_map_b[0],
                    1: number_map_a[1] + number_map_b[1]
                }
                return acc_a[0], new_number_map

        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .window_all(EventTimeSessionWindows.with_gap(Time.milliseconds(2))) \
            .aggregate(MyAggregateFunction(),
                       output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
            .add_sink(self.test_sink)

        self.env.execute('test_window_all_aggregate')
        results = self.test_sink.get_results()
        expected = ['(a,-1)', '(b,-1)', '(b,1)']
        self.assert_equals_sorted(expected, results)

    def test_window_all_aggregate_process(self):
        self.env.set_parallelism(1)
        data_stream = self.env.from_collection([
            ('a', 1), ('a', 2), ('b', 3), ('a', 6), ('b', 8), ('b', 9), ('a', 15)],
            type_info=Types.TUPLE([Types.STRING(), Types.INT()]))  # type: DataStream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(SecondColumnTimestampAssigner())

        class MyAggregateFunction(AggregateFunction):
            def create_accumulator(self) -> Tuple[int, str]:
                return 0, ''

            def add(self, value: Tuple[str, int], accumulator: Tuple[int, str]) -> Tuple[int, str]:
                return value[1] + accumulator[0], value[0]

            def get_result(self, accumulator: Tuple[str, int]):
                return accumulator[1], accumulator[0]

            def merge(self, acc_a: Tuple[int, str], acc_b: Tuple[int, str]):
                return acc_a[0] + acc_b[0], acc_a[1]

        class MyProcessWindowFunction(ProcessAllWindowFunction):

            def process(self, context: ProcessAllWindowFunction.Context,
                        elements: Iterable[Tuple[str, int]]) -> Iterable[str]:
                agg_result = next(iter(elements))
                yield "key {} timestamp sum {}".format(agg_result[0], agg_result[1])

        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .window_all(EventTimeSessionWindows.with_gap(Time.milliseconds(2))) \
            .aggregate(MyAggregateFunction(),
                       window_function=MyProcessWindowFunction(),
                       accumulator_type=Types.TUPLE([Types.INT(), Types.STRING()]),
                       output_type=Types.STRING()) \
            .add_sink(self.test_sink)

        self.env.execute('test_window_all_aggregate_process')
        results = self.test_sink.get_results()
        expected = ['key b timestamp sum 6',
                    'key b timestamp sum 23',
                    'key a timestamp sum 15']
        self.assert_equals_sorted(expected, results)

    def test_side_output_late_data(self):
        self.env.set_parallelism(1)
        config = Configuration(
            j_configuration=get_j_env_configuration(self.env._j_stream_execution_environment)
        )
        config.set_integer('python.fn-execution.bundle.size', 1)
        jvm = get_gateway().jvm
        watermark_strategy = WatermarkStrategy(
            jvm.org.apache.flink.api.common.eventtime.WatermarkStrategy.forGenerator(
                jvm.org.apache.flink.streaming.api.functions.python.eventtime.
                PerElementWatermarkGenerator.getSupplier()
            )
        ).with_timestamp_assigner(SecondColumnTimestampAssigner())

        tag = OutputTag('late-data', type_info=Types.ROW([Types.STRING(), Types.INT()]))
        ds1 = self.env.from_collection([('a', 0), ('a', 8), ('a', 4), ('a', 6)],
                                       type_info=Types.ROW([Types.STRING(), Types.INT()]))
        ds2 = ds1.assign_timestamps_and_watermarks(watermark_strategy) \
            .key_by(lambda e: e[0]) \
            .window(TumblingEventTimeWindows.of(Time.milliseconds(5))) \
            .allowed_lateness(0) \
            .side_output_late_data(tag) \
            .process(CountWindowProcessFunction(),
                     Types.TUPLE([Types.STRING(), Types.LONG(), Types.LONG(), Types.INT()]))
        main_sink = DataStreamTestSinkFunction()
        ds2.add_sink(main_sink)
        side_sink = DataStreamTestSinkFunction()
        ds2.get_side_output(tag).add_sink(side_sink)

        self.env.execute('test_side_output_late_data')
        main_expected = ['(a,0,5,1)', '(a,5,10,2)']
        self.assert_equals_sorted(main_expected, main_sink.get_results())
        side_expected = ['+I[a, 4]']
        self.assert_equals_sorted(side_expected, side_sink.get_results())


class ProcessWindowTests(WindowTests, PyFlinkStreamingTestCase):
    def setUp(self) -> None:
        super(ProcessWindowTests, self).setUp()
        config = get_j_env_configuration(self.env._j_stream_execution_environment)
        config.setString("python.execution-mode", "process")


class EmbeddedWindowTests(WindowTests, PyFlinkStreamingTestCase):
    def setUp(self) -> None:
        super(EmbeddedWindowTests, self).setUp()
        config = get_j_env_configuration(self.env._j_stream_execution_environment)
        config.setString("python.execution-mode", "thread")

    def test_chained_window(self):
        class MyTimestampAssigner(TimestampAssigner):
            def extract_timestamp(self, value: tuple, record_timestamp: int) -> int:
                return value[0]

        ds = self.env.from_collection(
            [(1676461680000, "a1", "b1", 1), (1676461680000, "a1", "b1", 1),
             (1676461680000, "a2", "b2", 1), (1676461680000, "a1", "b2", 1),
             (1676461740000, "a1", "b1", 1), (1676461740000, "a2", "b2", 1)]
        ).assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
                MyTimestampAssigner())
        )
        ds.key_by(
            lambda x: (x[0], x[1], x[2])
        ).window(
            TumblingEventTimeWindows.of(Time.minutes(1))
        ).reduce(
            lambda x, y: (x[0], x[1], x[2], x[3] + y[3]),
            output_type=Types.TUPLE([Types.LONG(), Types.STRING(), Types.STRING(), Types.INT()])
        ).map(
            lambda x: (x[0], x[1], x[3]),
            output_type=Types.TUPLE([Types.LONG(), Types.STRING(), Types.INT()])
        ).add_sink(self.test_sink)
        self.env.execute('test_chained_window')
        results = self.test_sink.get_results()
        expected = ['(1676461680000,a1,1)',
                    '(1676461680000,a1,2)',
                    '(1676461680000,a2,1)',
                    '(1676461740000,a1,1)',
                    '(1676461740000,a2,1)']
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


class CountWindowProcessFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):

    def process(self,
                key: str,
                context: ProcessWindowFunction.Context[TimeWindow],
                elements: Iterable[tuple]) -> Iterable[tuple]:
        return [(key, context.window().start, context.window().end, len([e for e in elements]))]


class CountAllWindowProcessFunction(ProcessAllWindowFunction[tuple, tuple, TimeWindow]):

    def process(self,
                context: 'ProcessAllWindowFunction.Context',
                elements: Iterable[tuple]) -> Iterable[tuple]:
        return [(context.window().start, context.window().end, len([e for e in elements]))]
