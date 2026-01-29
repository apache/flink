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
#  limitations under the License.
################################################################################
import asyncio
import uuid

from pyflink.table import DataTypes
from pyflink.table.udf import AsyncScalarFunction, udf, FunctionContext
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, run_with_config


def generate_random_table_name():
    return "Table{0}".format(str(uuid.uuid1()).replace("-", "_"))


class AsyncScalarFunctionTests(PyFlinkStreamTableTestCase):
    """
    Integration tests for Python Async Scalar Function.
    """

    def test_basic_async_scalar_function(self):

        class AsyncFunctionWithLifecycle(AsyncScalarFunction):
            def open(self, function_context: FunctionContext):
                self.prefix = "opened_"

            async def eval(self, value):
                await asyncio.sleep(0.001)
                return self.prefix + value

            def close(self):
                pass

        async_func = udf(
            AsyncFunctionWithLifecycle(),
            input_types=[DataTypes.STRING()],
            result_type=DataTypes.STRING()
        )

        sink_table = generate_random_table_name()
        self.t_env.execute_sql(f"""
            CREATE TABLE {sink_table}(a STRING, b STRING)
            WITH ('connector'='test-sink')
        """)

        t = self.t_env.from_elements([("test1",), ("test2",)], ['a'])
        t.select(t.a, async_func(t.a).alias('b')).execute_insert(sink_table).wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual, [
            "+I[test1, opened_test1]",
            "+I[test2, opened_test2]"
        ])

    def test_raise_exception_in_async_eval(self):
        """Test async scalar function that raises exception during evaluation."""

        class ExceptionAsyncFunction(AsyncScalarFunction):
            async def eval(self, value: str) -> str:
                raise ValueError("Test exception in async eval")

        async_func = udf(
            ExceptionAsyncFunction(),
            input_types=[DataTypes.STRING()],
            result_type=DataTypes.STRING()
        )

        sink_table = generate_random_table_name()
        self.t_env.execute_sql(f"""
            CREATE TABLE {sink_table}(a STRING, b STRING)
            WITH ('connector'='test-sink')
        """)

        t = self.t_env.from_elements([("test1",)], ['a'])

        with self.assertRaises(Exception) as context:
            t.select(t.a, async_func(t.a).alias('b')).execute_insert(sink_table).wait()

        # Verify exception message is propagated
        self.assertIn("Test exception in async eval", str(context.exception))

    def test_async_function_with_retry_logic(self):
        """Test async scalar function with custom retry logic."""

        class RetryAsyncFunction(AsyncScalarFunction):
            def open(self, function_context: FunctionContext):
                self.attempt_count = {}

            async def eval(self, value: str) -> str:
                await asyncio.sleep(0.01)

                # Track attempts for this value
                if value not in self.attempt_count:
                    self.attempt_count[value] = 0

                self.attempt_count[value] += 1

                # Fail first attempts, succeed on 2nd
                if self.attempt_count[value] < 2:
                    raise ValueError(f"Retry attempt {self.attempt_count[value]}")

                return f"success_{value}"

        async_func = udf(
            RetryAsyncFunction(),
            input_types=[DataTypes.STRING()],
            result_type=DataTypes.STRING()
        )

        sink_table = generate_random_table_name()
        self.t_env.execute_sql(f"""
            CREATE TABLE {sink_table}(a STRING, b STRING)
            WITH ('connector'='test-sink')
        """)

        t = self.t_env.from_elements([("test",)], ['a'])
        t.select(t.a, async_func(t.a).alias('b')).execute_insert(sink_table).wait()
        actual = source_sink_utils.results()
        # If retry logic works, we should get success result
        self.assert_equals(actual, [
            "+I[test, success_test]"
        ])

    def test_chaining_async_scalar_functions(self):
        """Test chaining multiple async scalar functions."""

        @udf(result_type=DataTypes.STRING())
        async def async_add_prefix(value: str) -> str:
            await asyncio.sleep(0.001)
            return f"prefix_{value}"

        @udf(result_type=DataTypes.STRING())
        async def async_add_suffix(value: str) -> str:
            await asyncio.sleep(0.001)
            return f"{value}_suffix"

        sink_table = generate_random_table_name()
        self.t_env.execute_sql(f"""
            CREATE TABLE {sink_table}(a STRING, b STRING)
            WITH ('connector'='test-sink')
        """)

        t = self.t_env.from_elements([("test",)], ['a'])
        # Chain async functions
        t.select(t.a, async_add_suffix(async_add_prefix(t.a)).alias('b')) \
            .execute_insert(sink_table).wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[test, prefix_test_suffix]"])

    def test_chaining_async_and_sync_functions(self):
        """Test chaining async scalar functions with regular Python UDFs."""
        from pyflink.table.udf import udf

        # Define a regular synchronous UDF
        @udf(result_type=DataTypes.STRING())
        def sync_upper(value: str) -> str:
            return value.upper()

        # Define async UDFs
        @udf(result_type=DataTypes.STRING())
        async def async_add_prefix(value: str) -> str:
            await asyncio.sleep(0.001)
            return f"prefix_{value}"

        @udf(result_type=DataTypes.STRING())
        async def async_add_suffix(value: str) -> str:
            await asyncio.sleep(0.001)
            return f"{value}_suffix"

        sink_table = generate_random_table_name()
        self.t_env.execute_sql(f"""
            CREATE TABLE {sink_table}(a STRING, b STRING, c STRING)
            WITH ('connector'='test-sink')
        """)

        t = self.t_env.from_elements([("test", "test2")], ['a', 'b'])

        # Test various chaining patterns:
        # 1. sync -> async
        # 2. async -> sync -> async
        t.select(
            t.a,
            async_add_prefix(sync_upper(t.a)).alias('b'),  # sync -> async
            async_add_suffix(sync_upper(async_add_prefix(t.a))).alias('c')  # async -> sync -> async
        ).execute_insert(sink_table).wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual, [
            "+I[test, prefix_TEST, PREFIX_TEST_suffix]"
        ])

    def test_async_udf_with_pandas_raises_error(self):
        """Test that using pandas func_type with async function raises an error."""
        from pyflink.table.udf import udf

        # Test 1: async def function with pandas should raise error
        with self.assertRaises(ValueError) as context:
            @udf(result_type=DataTypes.STRING(), func_type='pandas')
            async def async_func(value: str) -> str:
                await asyncio.sleep(0.001)
                return f"async_{value}"

        self.assertIn("Async scalar functions do not support pandas func_type",
                      str(context.exception))

        # Test 2: AsyncScalarFunction with pandas should raise error
        class MyAsyncFunc(AsyncScalarFunction):
            async def eval(self, value: str) -> str:
                await asyncio.sleep(0.001)
                return f"class_{value}"

        with self.assertRaises(ValueError) as context:
            udf(MyAsyncFunc(), result_type=DataTypes.STRING(), func_type='pandas')

        self.assertIn("Async scalar functions do not support pandas func_type",
                      str(context.exception))

    @run_with_config({"table.exec.async-scalar.timeout": "5s"})
    def test_processing_timeout(self):
        """Test async scalar function processing timeout."""

        class TimeoutAsyncFunction(AsyncScalarFunction):
            async def eval(self, value: str) -> str:
                # Sleep longer than timeout
                await asyncio.sleep(10)
                return f"completed_{value}"

        # Note: For Table API async scalar functions, timeout behavior is handled
        # by the framework configuration. This test verifies the function can be
        # registered and used, but actual timeout handling is framework-level.
        async_func = udf(
            TimeoutAsyncFunction(),
            input_types=[DataTypes.STRING()],
            result_type=DataTypes.STRING()
        )

        sink_table = generate_random_table_name()
        self.t_env.execute_sql(f"""
            CREATE TABLE {sink_table}(a STRING, b STRING)
            WITH ('connector'='test-sink')
        """)

        t = self.t_env.from_elements([("test",)], ['a'])

        # With short processing time limit, this should either timeout or complete
        # based on framework timeout configuration
        try:
            t.select(t.a, async_func(t.a).alias('b')).execute_insert(sink_table).wait()
            # If execution completes, verify results exist
            actual = source_sink_utils.results()
            # Results could be timeout or actual completion depending on timing
            self.assert_equals(actual, [
                "+I[test, completed_test]"
            ])
        except Exception as e:
            # Timeout exceptions are acceptable for this test
            self.assertIn("Async function call has timed out for input: ['test']",
                          str(e))


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
