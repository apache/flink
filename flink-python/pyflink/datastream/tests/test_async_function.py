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
import asyncio
import random

from pyflink.common import Types, Row, Time, Configuration, WatermarkStrategy
from pyflink.datastream import AsyncDataStream, AsyncFunction, ResultFuture, \
    StreamExecutionEnvironment
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction, \
    SecondColumnTimestampAssigner
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase
from pyflink.util.java_utils import get_j_env_configuration


class AsyncFunctionTests(PyFlinkStreamingTestCase):

    def setUp(self) -> None:
        super(AsyncFunctionTests, self).setUp()
        config = get_j_env_configuration(self.env._j_stream_execution_environment)
        config.setString("pekko.ask.timeout", "20 s")
        self.test_sink = DataStreamTestSinkFunction()

    def assert_equals_sorted(self, expected, actual):
        expected.sort()
        actual.sort()
        self.assertEqual(expected, actual)

    def assert_equals(self, expected, actual):
        self.assertEqual(expected, actual)

    def test_basic_functionality(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: Row, result_future: ResultFuture[int]):
                await asyncio.sleep(2)
                result_future.complete([value[0] + value[1]])

            def timeout(self, value: Row, result_future: ResultFuture[int]):
                result_future.complete([value[0] + value[1]])

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(5), 2, Types.INT())
        ds.add_sink(self.test_sink)
        self.env.execute()
        results = self.test_sink.get_results(False)
        expected = ['2', '4', '6', '8', '10']
        self.assert_equals_sorted(expected, results)

    def test_watermark(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )
        jvm = get_gateway().jvm
        watermark_strategy = WatermarkStrategy(
            jvm.org.apache.flink.api.common.eventtime.WatermarkStrategy.forGenerator(
                jvm.org.apache.flink.streaming.api.functions.python.eventtime.
                PerElementWatermarkGenerator.getSupplier()
            )
        ).with_timestamp_assigner(SecondColumnTimestampAssigner())
        ds = ds.assign_timestamps_and_watermarks(watermark_strategy)

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: Row, result_future: ResultFuture[int]):
                await asyncio.sleep(random.randint(1, 3))
                result_future.complete([value[0] + value[1]])

            def timeout(self, value: Row, result_future: ResultFuture[int]):
                result_future.complete([value[0] + value[1]])

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(5), 2, Types.INT())
        ds.add_sink(self.test_sink)
        self.env.execute()
        results = self.test_sink.get_results(False)
        expected = ['2', '4', '6', '8', '10']
        # note that we use assert_equals instead of assert_equals_sorted
        self.assert_equals(expected, results)

    def test_complete_async_function_with_non_iterable_result(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: Row, result_future: ResultFuture[int]):
                await asyncio.sleep(2)
                result_future.complete(value[0] + value[1])

            def timeout(self, value: Row, result_future: ResultFuture[int]):
                result_future.complete(value[0] + value[1])

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(5), 2, Types.INT())
        ds.add_sink(self.test_sink)
        try:
            self.env.execute()
        except Exception as e:
            message = str(e)
            self.assertTrue("The 'result_future' of AsyncFunction should be completed with data of "
                            "list type" in message)

    def test_raise_exception_in_async_invoke(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: Row, result_future: ResultFuture[int]):
                raise Exception("encountered an exception")

            def timeout(self, value: Row, result_future: ResultFuture[int]):
                # raise the same exception to make sure test case is stable in all cases
                raise Exception("encountered an exception")

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(5), 2, Types.INT())
        ds.add_sink(self.test_sink)
        try:
            self.env.execute()
        except Exception as e:
            message = str(e)
            self.assertTrue("encountered an exception" in message)

    def test_raise_exception_in_timeout(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: Row, result_future: ResultFuture[int]):
                await asyncio.sleep(10)
                result_future.complete([value[0] + value[1]])

            def timeout(self, value: Row, result_future: ResultFuture[int]):
                raise Exception("encountered an exception")

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(2), 2, Types.INT())
        ds.add_sink(self.test_sink)
        try:
            self.env.execute()
        except Exception as e:
            message = str(e)
            self.assertTrue("encountered an exception" in message)

    def test_processing_timeout(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: Row, result_future: ResultFuture[int]):
                await asyncio.sleep(10)
                result_future.complete([value[0] + value[1]])

            def timeout(self, value: Row, result_future: ResultFuture[int]):
                result_future.complete([value[0] - value[1]])

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(1), 2, Types.INT())
        ds.add_sink(self.test_sink)
        self.env.execute()
        results = self.test_sink.get_results(False)
        expected = ['0', '0', '0', '0', '0']
        self.assert_equals_sorted(expected, results)


class EmbeddedThreadAsyncFunctionTests(PyFlinkStreamingTestCase):

    def test_run_async_function_in_thread_mode(self):
        config = Configuration()
        config.set_string("python.execution-mode", "thread")
        env = StreamExecutionEnvironment.get_execution_environment(config)
        ds = env.from_collection(
            [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: Row, result_future: ResultFuture[int]):
                await asyncio.sleep(2)
                result_future.complete([value[0] + value[1]])

        try:
            AsyncDataStream.unordered_wait(
                ds, MyAsyncFunction(), Time.seconds(5), 2, Types.INT())
        except Exception as e:
            message = str(e)
            self.assertTrue("AsyncFunction is still not supported for 'thread' mode" in message)
