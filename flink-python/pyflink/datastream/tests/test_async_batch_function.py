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
"""
Tests for AsyncBatchFunction API.

.. versionadded:: 2.1.0
"""
import asyncio
from typing import List

from pyflink.common import Types, Row, Time
from pyflink.datastream import AsyncDataStream, AsyncBatchFunction
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase
from pyflink.util.java_utils import get_j_env_configuration


class AsyncBatchFunctionTests(PyFlinkStreamingTestCase):
    """Tests for AsyncBatchFunction API."""

    def setUp(self) -> None:
        super(AsyncBatchFunctionTests, self).setUp()
        config = get_j_env_configuration(self.env._j_stream_execution_environment)
        config.setString("pekko.ask.timeout", "20 s")
        self.test_sink = DataStreamTestSinkFunction()

    def assert_equals_sorted(self, expected, actual):
        expected.sort()
        actual.sort()
        self.assertEqual(expected, actual)

    def test_basic_batch_execution(self):
        """Test basic async batch function execution with batch size triggering."""
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3), (4, 4)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncBatchFunction(AsyncBatchFunction):
            async def async_invoke_batch(self, inputs: List[Row]) -> List[int]:
                # Process batch of inputs together
                await asyncio.sleep(0.1)
                return [row[0] + row[1] for row in inputs]

        ds = AsyncDataStream.unordered_wait_batch(
            ds,
            MyAsyncBatchFunction(),
            timeout=Time.seconds(10),
            batch_size=2,
            output_type=Types.INT()
        )
        ds.add_sink(self.test_sink)
        self.env.execute()
        results = self.test_sink.get_results(False)
        expected = ['2', '4', '6', '8']
        self.assert_equals_sorted(expected, results)

    def test_batch_size_triggering(self):
        """Test that batch is triggered when batch_size is reached."""
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class BatchSizeTrackingFunction(AsyncBatchFunction):
            def __init__(self):
                self.batch_sizes = []

            async def async_invoke_batch(self, inputs: List[Row]) -> List[int]:
                self.batch_sizes.append(len(inputs))
                await asyncio.sleep(0.05)
                return [row[0] + row[1] for row in inputs]

        func = BatchSizeTrackingFunction()
        ds = AsyncDataStream.unordered_wait_batch(
            ds,
            func,
            timeout=Time.seconds(10),
            batch_size=2,  # Should trigger batches of size 2
            output_type=Types.INT()
        )
        ds.add_sink(self.test_sink)
        self.env.execute()

        results = self.test_sink.get_results(False)
        expected = ['2', '4', '6', '8', '10']
        self.assert_equals_sorted(expected, results)

    def test_batch_timeout_triggering(self):
        """Test that batch is triggered when batch_timeout is reached."""
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1)],  # Only one element, won't reach batch_size
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncBatchFunction(AsyncBatchFunction):
            async def async_invoke_batch(self, inputs: List[Row]) -> List[int]:
                return [row[0] + row[1] for row in inputs]

        ds = AsyncDataStream.unordered_wait_batch(
            ds,
            MyAsyncBatchFunction(),
            timeout=Time.seconds(10),
            batch_size=10,  # Large batch size
            batch_timeout=Time.milliseconds(100),  # Short timeout
            output_type=Types.INT()
        )
        ds.add_sink(self.test_sink)
        self.env.execute()

        results = self.test_sink.get_results(False)
        expected = ['2']
        self.assertEqual(expected, results)

    def test_exception_propagation(self):
        """Test that exceptions in async_invoke_batch are propagated correctly."""
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class FailingBatchFunction(AsyncBatchFunction):
            async def async_invoke_batch(self, inputs: List[Row]) -> List[int]:
                raise Exception("Test batch failure")

        ds = AsyncDataStream.unordered_wait_batch(
            ds,
            FailingBatchFunction(),
            timeout=Time.seconds(5),
            batch_size=2,
            output_type=Types.INT()
        )
        ds.add_sink(self.test_sink)

        try:
            self.env.execute()
            self.fail("Expected exception was not raised")
        except Exception as e:
            self.assertTrue("Test batch failure" in str(e))

    def test_timeout_handling(self):
        """Test that timeout_batch is called when operation times out."""
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class SlowBatchFunction(AsyncBatchFunction):
            async def async_invoke_batch(self, inputs: List[Row]) -> List[int]:
                await asyncio.sleep(10)  # Sleep longer than timeout
                return [row[0] + row[1] for row in inputs]

            def timeout_batch(self, inputs: List[Row]) -> List[int]:
                # Return fallback values
                return [0 for _ in inputs]

        ds = AsyncDataStream.unordered_wait_batch(
            ds,
            SlowBatchFunction(),
            timeout=Time.seconds(1),  # Short timeout
            batch_size=2,
            output_type=Types.INT()
        )
        ds.add_sink(self.test_sink)
        self.env.execute()

        results = self.test_sink.get_results(False)
        expected = ['0', '0']
        self.assert_equals_sorted(expected, results)

    def test_ordered_wait_batch(self):
        """Test ordered async batch execution."""
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3), (4, 4)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncBatchFunction(AsyncBatchFunction):
            async def async_invoke_batch(self, inputs: List[Row]) -> List[int]:
                await asyncio.sleep(0.1)
                return [row[0] + row[1] for row in inputs]

        ds = AsyncDataStream.ordered_wait_batch(
            ds,
            MyAsyncBatchFunction(),
            timeout=Time.seconds(10),
            batch_size=2,
            output_type=Types.INT()
        )
        ds.add_sink(self.test_sink)
        self.env.execute()

        results = self.test_sink.get_results(False)
        # Ordered mode should preserve input order
        expected = ['2', '4', '6', '8']
        self.assertEqual(expected, results)

    def test_non_list_result_error(self):
        """Test that non-list results from async_invoke_batch raise error."""
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class BadReturnTypeBatchFunction(AsyncBatchFunction):
            async def async_invoke_batch(self, inputs: List[Row]):
                return 42  # Return non-list

        ds = AsyncDataStream.unordered_wait_batch(
            ds,
            BadReturnTypeBatchFunction(),
            timeout=Time.seconds(5),
            batch_size=2,
            output_type=Types.INT()
        )
        ds.add_sink(self.test_sink)

        try:
            self.env.execute()
            self.fail("Expected exception was not raised")
        except Exception as e:
            self.assertTrue("list type" in str(e))

    def test_none_result_error(self):
        """Test that None results from async_invoke_batch raise error."""
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class NoneReturnBatchFunction(AsyncBatchFunction):
            async def async_invoke_batch(self, inputs: List[Row]):
                return None

        ds = AsyncDataStream.unordered_wait_batch(
            ds,
            NoneReturnBatchFunction(),
            timeout=Time.seconds(5),
            batch_size=2,
            output_type=Types.INT()
        )
        ds.add_sink(self.test_sink)

        try:
            self.env.execute()
            self.fail("Expected exception was not raised")
        except Exception as e:
            self.assertTrue("cannot be none" in str(e))

    def test_validation_negative_batch_size(self):
        """Test that negative batch_size raises validation error."""
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncBatchFunction(AsyncBatchFunction):
            async def async_invoke_batch(self, inputs: List[Row]) -> List[int]:
                return [row[0] + row[1] for row in inputs]

        try:
            AsyncDataStream.unordered_wait_batch(
                ds,
                MyAsyncBatchFunction(),
                timeout=Time.seconds(10),
                batch_size=-1,  # Invalid
                output_type=Types.INT()
            )
            self.fail("Expected exception was not raised")
        except Exception as e:
            self.assertTrue("positive" in str(e).lower())

    def test_validation_non_async_method(self):
        """Test that non-async async_invoke_batch raises validation error."""
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class NonAsyncBatchFunction(AsyncBatchFunction):
            # Note: NOT async def
            def async_invoke_batch(self, inputs: List[Row]) -> List[int]:
                return [row[0] + row[1] for row in inputs]

        try:
            AsyncDataStream.unordered_wait_batch(
                ds,
                NonAsyncBatchFunction(),
                timeout=Time.seconds(10),
                batch_size=2,
                output_type=Types.INT()
            )
            self.fail("Expected exception was not raised")
        except Exception as e:
            self.assertTrue("async def" in str(e))

    def test_end_of_input_flush(self):
        """Test that remaining elements are flushed at end of input."""
        self.env.set_parallelism(1)
        # 5 elements with batch_size=3 should result in batches of [3, 2]
        ds = self.env.from_collection(
            [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MyAsyncBatchFunction(AsyncBatchFunction):
            async def async_invoke_batch(self, inputs: List[Row]) -> List[int]:
                return [row[0] + row[1] for row in inputs]

        ds = AsyncDataStream.unordered_wait_batch(
            ds,
            MyAsyncBatchFunction(),
            timeout=Time.seconds(10),
            batch_size=3,  # Won't evenly divide 5 elements
            output_type=Types.INT()
        )
        ds.add_sink(self.test_sink)
        self.env.execute()

        results = self.test_sink.get_results(False)
        expected = ['2', '4', '6', '8', '10']
        self.assert_equals_sorted(expected, results)

    def test_large_batch_with_multiple_outputs(self):
        """Test batch function that returns multiple outputs per input."""
        self.env.set_parallelism(1)
        ds = self.env.from_collection(
            [(1, 1), (2, 2)],
            type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()])
        )

        class MultiOutputBatchFunction(AsyncBatchFunction):
            async def async_invoke_batch(self, inputs: List[Row]) -> List[int]:
                # For each input, just return sum (one output per input)
                return [row[0] + row[1] for row in inputs]

        ds = AsyncDataStream.unordered_wait_batch(
            ds,
            MultiOutputBatchFunction(),
            timeout=Time.seconds(10),
            batch_size=2,
            output_type=Types.INT()
        )
        ds.add_sink(self.test_sink)
        self.env.execute()

        results = self.test_sink.get_results(False)
        expected = ['2', '4']
        self.assert_equals_sorted(expected, results)


if __name__ == '__main__':
    import unittest
    unittest.main()
