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
import sys
import threading
import types
import unittest
from unittest import mock


def _import_async_operation():
    # the embedded modules import pemja's findClass, which only exists inside the JVM
    fake_pemja = types.ModuleType("pemja")
    fake_pemja.findClass = mock.Mock()
    with mock.patch.dict(sys.modules, {"pemja": fake_pemja}):
        from pyflink.fn_execution.embedded import async_operation
    return async_operation


_async_operation = _import_async_operation()
AsyncFunctionOperation = _async_operation.AsyncFunctionOperation
AsyncFunctionRunner = _async_operation.AsyncFunctionRunner


class _Handler(object):
    """Stands in for the Java handler and records how it was completed."""

    def __init__(self):
        self.results = []
        self.errors = []
        self.done = threading.Event()

    def complete(self, results):
        self.results.append(results)
        self.done.set()

    def completeExceptionally(self, error):
        self.errors.append(error)
        self.done.set()


class _IdentityConverter(object):

    def to_external(self, value):
        return value


async def _wait_forever(started=None):
    if started is not None:
        started.set()
    await asyncio.Event().wait()


class AsyncFunctionRunnerTests(unittest.TestCase):

    def setUp(self):
        self.runner = AsyncFunctionRunner()
        self.runner.start_and_wait_ready()
        self.addCleanup(self.runner.stop)

    def test_unexpected_loop_exit_fails_pending_inputs(self):
        handler = _Handler()
        started = threading.Event()
        self.runner.submit(_wait_forever(started), handler)
        self.assertTrue(started.wait(10))

        # stop the loop without going through stop()
        self.runner._loop.call_soon_threadsafe(self.runner._loop.stop)

        self.assertTrue(handler.done.wait(10))
        self.assertEqual([], handler.results)
        self.assertEqual(1, len(handler.errors))
        self.assertIn("exited unexpectedly", handler.errors[0])
        self.runner.join(10)
        with self.assertRaisesRegex(RuntimeError, "is not running"):
            self.runner.submit(_wait_forever(), _Handler())

    def test_stop_does_not_report_pending_inputs(self):
        handler = _Handler()
        started = threading.Event()
        self.runner.submit(_wait_forever(started), handler)
        self.assertTrue(started.wait(10))

        self.runner.stop()

        self.assertFalse(self.runner.is_alive())
        self.assertFalse(handler.done.is_set())
        with self.assertRaisesRegex(RuntimeError, "is not running"):
            self.runner.submit(_wait_forever(), _Handler())

    def test_coroutine_that_escapes_reporting_fails_its_input(self):
        async def escape():
            # e.g. an error raised by the handler call itself
            raise RuntimeError("escaped the handler")

        handler = _Handler()
        self.runner.submit(escape(), handler)

        self.assertTrue(handler.done.wait(10))
        self.assertEqual([], handler.results)
        self.assertIn("RuntimeError: escaped the handler", handler.errors[0])

    def test_start_failure_is_raised(self):
        runner = AsyncFunctionRunner()
        with mock.patch("asyncio.new_event_loop", side_effect=OSError("no event loop")):
            with self.assertRaisesRegex(RuntimeError, "Failed to start the event loop"):
                runner.start_and_wait_ready()
        with self.assertRaisesRegex(RuntimeError, "is not running"):
            runner.submit(_wait_forever(), _Handler())


class AsyncFunctionOperationTests(unittest.TestCase):

    def setUp(self):
        self.operation = AsyncFunctionOperation.__new__(AsyncFunctionOperation)
        self.operation._class_name = "TestAsyncFunction"
        self.operation._output_data_converter = _IdentityConverter()
        self.operation._runner = AsyncFunctionRunner()
        self.operation._runner.start_and_wait_ready()
        self.addCleanup(self.operation._runner.stop)

    def _invoke(self, async_invoke):
        self.operation._async_function = mock.Mock(async_invoke=async_invoke)
        handler = _Handler()
        future = asyncio.run_coroutine_threadsafe(
            self.operation._invoke(handler, 1), self.operation._runner._loop)
        return handler, future

    def test_start_failure_closes_the_function(self):
        operation = AsyncFunctionOperation.__new__(AsyncFunctionOperation)
        operation._async_function = mock.Mock()
        operation._runtime_context = mock.Mock()
        operation._closed = False
        with mock.patch("asyncio.new_event_loop", side_effect=OSError("no event loop")):
            with self.assertRaisesRegex(RuntimeError, "Failed to start the event loop"):
                operation.open()
        operation._async_function.open.assert_called_once_with(operation._runtime_context)
        operation._async_function.close.assert_called_once_with()
        operation.close()
        operation._async_function.close.assert_called_once_with()

    def test_result_is_reported(self):
        async def async_invoke(value):
            return [value, value + 1]

        handler, _ = self._invoke(async_invoke)

        self.assertTrue(handler.done.wait(10))
        self.assertEqual([[1, 2]], handler.results)
        self.assertEqual([], handler.errors)

    def test_non_iterable_result_is_reported_as_error(self):
        async def async_invoke(value):
            return value

        handler, _ = self._invoke(async_invoke)

        self.assertTrue(handler.done.wait(10))
        self.assertEqual([], handler.results)
        self.assertIn("should be of list type", handler.errors[0])

    def test_cancellation_is_reported_as_error(self):
        started = threading.Event()

        async def async_invoke(value):
            await _wait_forever(started)

        handler, future = self._invoke(async_invoke)
        self.assertTrue(started.wait(10))

        future.cancel()

        self.assertTrue(handler.done.wait(10))
        self.assertEqual([], handler.results)
        self.assertIn("CancelledError", handler.errors[0])

    def test_timeout_reports_through_the_handler(self):
        self.operation._input_data_converter = mock.Mock(to_internal=lambda value: value)
        self.operation._async_function = mock.Mock(timeout=lambda value: [-value])
        handler = _Handler()
        self.operation.timeout(handler, 1)
        self.assertEqual([[-1]], handler.results)

        def raise_timeout(value):
            raise TimeoutError("timed out: %s" % value)

        self.operation._async_function = mock.Mock(timeout=raise_timeout)
        handler = _Handler()
        self.operation.timeout(handler, 1)
        self.assertEqual([], handler.results)
        self.assertIn("TimeoutError: timed out: 1", handler.errors[0])


if __name__ == '__main__':
    unittest.main()
