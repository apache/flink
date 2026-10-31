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
import os
import random
import shutil
import tempfile
from typing import List

from pyflink.common import Types, Row, Time, WatermarkStrategy
from pyflink.datastream import AsyncDataStream, AsyncFunction, AsyncRetryStrategy, \
    async_retry_strategies
from pyflink.datastream.connectors.number_seq import NumberSequenceSource
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

    def test_unordered_mode(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: Row):
                await asyncio.sleep(2)
                return [value[0] + value[1]]

            def timeout(self, value: Row):
                return [value[0] + value[1]]

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(5), 2, Types.INT())
        ds.add_sink(self.test_sink)
        self.env.execute()
        results = self.test_sink.get_results(False)
        expected = ['2', '4', '6', '8', '10']
        self.assert_equals_sorted(expected, results)

    def test_ordered_mode(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: Row):
                await asyncio.sleep(random.randint(1, 2))
                return [value[0] + value[1]]

            def timeout(self, value: Row):
                return [value[0] + value[1]]

        ds = AsyncDataStream.ordered_wait(
            ds, MyAsyncFunction(), Time.seconds(5), 2, Types.INT())
        ds.add_sink(self.test_sink)
        self.env.execute()
        results = self.test_sink.get_results(False)
        expected = ['2', '4', '6', '8', '10']
        self.assert_equals(expected, results)

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

            async def async_invoke(self, value: Row):
                await asyncio.sleep(random.randint(1, 3))
                return [value[0] + value[1]]

            def timeout(self, value: Row):
                return [value[0] + value[1]]

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(5), 2, Types.INT())
        ds.add_sink(self.test_sink)
        self.env.execute()
        results = self.test_sink.get_results(False)
        expected = ['2', '4', '6', '8', '10']
        # note that we use assert_equals instead of assert_equals_sorted
        self.assert_equals(expected, results)

    def test_non_iterable_result(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: Row):
                await asyncio.sleep(2)
                return value[0] + value[1]

            def timeout(self, value: Row):
                return value[0] + value[1]

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(5), 2, Types.INT())
        ds.add_sink(self.test_sink)
        try:
            self.env.execute()
            self.fail()
        except Exception as e:
            message = str(e)
            self.assertTrue("The result of AsyncFunction should be of list type" in message)

    def test_none_result(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: Row):
                await asyncio.sleep(10)
                return None

            def timeout(self, value: Row):
                return None

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(1), 2, Types.INT())
        ds.add_sink(self.test_sink)
        try:
            self.env.execute()
            self.fail()
        except Exception as e:
            message = str(e)
            self.assertTrue("The result of AsyncFunction cannot be none" in message)

    def test_raise_exception_in_async_invoke(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: Row):
                raise Exception("encountered an exception")

            def timeout(self, value: Row):
                # raise the same exception to make sure test case is stable in all cases
                raise Exception("encountered an exception")

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(5), 2, Types.INT())
        ds.add_sink(self.test_sink)
        try:
            self.env.execute()
            self.fail()
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

            async def async_invoke(self, value: Row):
                await asyncio.sleep(10)
                return [value[0] + value[1]]

            def timeout(self, value: Row):
                raise Exception("encountered an exception")

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(2), 2, Types.INT())
        ds.add_sink(self.test_sink)
        try:
            self.env.execute()
            self.fail()
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

            async def async_invoke(self, value: Row):
                await asyncio.sleep(10)
                return [value[0] + value[1]]

            def timeout(self, value: Row):
                return [value[0] - value[1]]

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(1), 2, Types.INT())
        ds.add_sink(self.test_sink)
        self.env.execute()
        results = self.test_sink.get_results(False)
        expected = ['0', '0', '0', '0', '0']
        self.assert_equals_sorted(expected, results)

    def test_async_invoke_error_is_reported(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: Row):
                raise ValueError("failed in async_invoke")

            def timeout(self, value: Row):
                return [value[0] + value[1]]

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(60), 2, Types.INT())
        ds.add_sink(self.test_sink)
        with self.assertRaisesRegex(Exception, "failed in async_invoke"):
            self.env.execute()

    def test_none_async_result(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: Row):
                return None

            def timeout(self, value: Row):
                return [value[0] + value[1]]

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(60), 2, Types.INT())
        ds.add_sink(self.test_sink)
        with self.assertRaisesRegex(Exception, "The result of AsyncFunction cannot be none"):
            self.env.execute()

    def test_multiple_row_results(self):
        for object_reuse in [False, True]:
            with self.subTest(object_reuse=object_reuse):
                if object_reuse:
                    self.env.get_config().enable_object_reuse()
                else:
                    self.env.get_config().disable_object_reuse()
                self.env.set_parallelism(1)
                test_sink = DataStreamTestSinkFunction()
                ds = self.env.from_collection(
                    [(1, 1), (2, 2), (3, 3)],
                    type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
                )

                class MyAsyncFunction(AsyncFunction):

                    async def async_invoke(self, value: Row):
                        await asyncio.sleep(random.random() / 10)
                        return [Row(value[0], i) for i in range(3)]

                ds = AsyncDataStream.ordered_wait(
                    ds, MyAsyncFunction(), Time.seconds(60), 2,
                    Types.ROW([Types.INT(), Types.INT()]))
                ds.add_sink(test_sink)
                self.env.execute()
                results = test_sink.get_results(False)
                expected = ['+I[%d, %d]' % (v, i) for v in [1, 2, 3] for i in range(3)]
                self.assert_equals(expected, results)

    def test_parallelism_two(self):
        self.env.set_parallelism(2)
        ds = self.env.from_collection(
            [(i, i) for i in range(1, 21)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: Row):
                await asyncio.sleep(random.random() / 10)
                return [value[0] + value[1]]

        unordered = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(60), 3, Types.INT())
        ordered = AsyncDataStream.ordered_wait(
            ds, MyAsyncFunction(), Time.seconds(60), 3, Types.INT())
        unordered.union(ordered).add_sink(self.test_sink)
        self.env.execute()
        results = self.test_sink.get_results(False)
        expected = [str(2 * i) for i in range(1, 21)] * 2
        self.assert_equals_sorted(expected, results)

    def test_capacity_one_after_python_map(self):
        self.env.set_parallelism(1)
        ds = self.env.from_source(
            NumberSequenceSource(1, 10), WatermarkStrategy.no_watermarks(), "numbers"
        ).map(lambda v: v * 2, output_type=Types.LONG())

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: int):
                await asyncio.sleep(random.random() / 10)
                return [value + 1]

        ds = AsyncDataStream.ordered_wait(
            ds, MyAsyncFunction(), Time.seconds(60), 1, Types.LONG())
        ds.add_sink(self.test_sink)
        self.env.execute()
        results = self.test_sink.get_results(False)
        expected = [str(v * 2 + 1) for v in range(1, 11)]
        self.assert_equals(expected, results)

    def test_async_with_retry(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncFunction(AsyncFunction):

            def __init__(self):
                self.retries_1 = {}
                self.retries_2 = {}

            async def async_invoke(self, value: Row):
                await asyncio.sleep(1)
                if value in self.retries_2:
                    return [value[0] + value[1]]
                elif value in self.retries_1:
                    self.retries_2[value] = True
                    return [value[0] + value[1] + 1]
                else:
                    self.retries_1[value] = True
                    raise ValueError("failed the first time")

            def timeout(self, value: Row):
                return [value[0] + value[1]]

        def result_predicate(result: List[int]):
            return result[0] % 2 == 1

        def exception_predicate(exception: Exception):
            return "failed the first time" in str(exception)

        async_retry_strategy = AsyncRetryStrategy.fixed_delay(
            max_attempts=5,
            backoff_time_millis=1000,
            result_predicate=result_predicate,
            exception_predicate=exception_predicate
        )
        ds = AsyncDataStream.unordered_wait_with_retry(
            ds, MyAsyncFunction(), Time.seconds(10), async_retry_strategy, 2, Types.INT())
        ds.add_sink(self.test_sink)
        self.env.execute()
        results = self.test_sink.get_results(False)
        expected = ['2', '4', '6']
        self.assert_equals_sorted(expected, results)


class EmbeddedThreadAsyncFunctionTests(AsyncFunctionTests):
    """Runs the same tests with python.execution-mode=thread."""

    def setUp(self) -> None:
        super(EmbeddedThreadAsyncFunctionTests, self).setUp()
        config = get_j_env_configuration(self.env._j_stream_execution_environment)
        config.setString("python.execution-mode", "thread")

    def test_async_with_retry(self):
        self.skipTest("Retry is not supported in thread mode yet.")

    def test_restore_in_flight_inputs(self):
        # process mode waits for in-flight inputs before a checkpoint, thread mode stores them
        for ordered in [True, False]:
            with self.subTest(ordered=ordered):
                self._test_restore_in_flight_inputs(ordered)

    def _test_restore_in_flight_inputs(self, ordered):
        self.env.set_parallelism(1)
        self.env.enable_checkpointing(100)
        work_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, work_dir, True)
        checkpoint_dir = os.path.join(work_dir, "checkpoints")
        marker = os.path.join(work_dir, "failed")
        config = get_j_env_configuration(self.env._j_stream_execution_environment)
        config.setString("execution.checkpointing.storage", "filesystem")
        config.setString("execution.checkpointing.dir", "file://" + checkpoint_dir)
        config.setString("restart-strategy.type", "fixed-delay")
        config.setString("restart-strategy.fixed-delay.attempts", "3")
        config.setString("restart-strategy.fixed-delay.delay", "100 ms")
        test_sink = DataStreamTestSinkFunction()
        ds = self.env.from_source(
            NumberSequenceSource(1, 50), WatermarkStrategy.no_watermarks(), "numbers")

        class FailOnceAsyncFunction(AsyncFunction):

            def __init__(self, checkpoint_path, marker_path):
                self._checkpoint_path = checkpoint_path
                self._marker_path = marker_path

            def _checkpoint_ids(self, completed):
                ids = []
                if os.path.isdir(self._checkpoint_path):
                    for root, _, files in os.walk(self._checkpoint_path):
                        name = os.path.basename(root)
                        if name.startswith("chk-") and (not completed or "_metadata" in files):
                            ids.append(int(name[len("chk-"):]))
                return ids

            async def async_invoke(self, value: int):
                if value == 25 and not os.path.exists(self._marker_path):
                    # Wait for a checkpoint that was triggered after this input became pending.
                    # Checkpoints don't overlap, so one numbered two past any checkpoint that may
                    # already be running is such a checkpoint. Then fail the job once.
                    last_started = max(self._checkpoint_ids(completed=False), default=0)
                    while max(self._checkpoint_ids(completed=True), default=0) < last_started + 2:
                        await asyncio.sleep(0.01)
                    open(self._marker_path, 'w').close()
                    raise RuntimeError("induced failure")
                # keep later inputs in flight as well, so that the source doesn't finish first
                while value > 25 and not os.path.exists(self._marker_path):
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.01)
                return [value * 10]

        wait = AsyncDataStream.ordered_wait if ordered else AsyncDataStream.unordered_wait
        ds = wait(ds, FailOnceAsyncFunction(checkpoint_dir, marker), Time.seconds(60), 5,
                  Types.LONG())
        ds.add_sink(test_sink)
        self.env.execute()

        self.assertTrue(os.path.exists(marker))
        results = test_sink.get_results(False)
        # the test sink is not exactly-once, so outputs after the checkpoint can repeat, but the
        # input that was in flight at the checkpoint must not be lost
        self.assertEqual(sorted(set(int(r) for r in results)), [v * 10 for v in range(1, 51)])

    def test_retry_strategy_rejected(self):
        ds = self.env.from_collection([1, 2, 3], type_info=Types.INT())

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: int):
                return [value]

        strategy = AsyncRetryStrategy.fixed_delay(
            max_attempts=2, backoff_time_millis=100,
            result_predicate=lambda result: False,
            exception_predicate=lambda exception: True)
        with self.assertRaisesRegex(
                Exception, "AsyncFunction with retry is still not supported for 'thread' mode"):
            AsyncDataStream.unordered_wait_with_retry(
                ds, MyAsyncFunction(), Time.seconds(5), strategy, 2, Types.INT())

    def test_no_retry_strategy_accepted(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection([1, 2, 3], type_info=Types.INT())

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: int):
                return [value + 1]

        ds = AsyncDataStream.ordered_wait_with_retry(
            ds, MyAsyncFunction(), Time.seconds(5), async_retry_strategies.NO_RETRY_STRATEGY,
            2, Types.INT())
        ds.add_sink(self.test_sink)
        self.env.execute()
        self.assert_equals(['2', '3', '4'], self.test_sink.get_results(False))

    def test_uses_embedded_async_wait_operator(self):
        ds = self.env.from_collection([1, 2, 3], type_info=Types.INT())

        class MyAsyncFunction(AsyncFunction):

            async def async_invoke(self, value: int):
                return [value]

        ds = AsyncDataStream.unordered_wait(
            ds, MyAsyncFunction(), Time.seconds(5), 2, Types.INT())
        j_factory = ds._j_data_stream.getTransformation().getOperatorFactory()
        self.assertEqual(
            "org.apache.flink.streaming.api.operators.python.embedded."
            "EmbeddedPythonAsyncWaitOperatorFactory",
            j_factory.getClass().getName())
