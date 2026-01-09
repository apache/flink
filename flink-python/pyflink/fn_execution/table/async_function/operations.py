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

from pyflink.datastream import async_retry_strategies
from pyflink.fn_execution.datastream.operations import Operation, AsyncOperationMixin
from pyflink.fn_execution.datastream.process.async_function.operation import Emitter, \
    AsyncFunctionRunner, ResultHandler, RetryableResultHandler
from pyflink.fn_execution.datastream.process.async_function.queue import OrderedStreamElementQueue
from pyflink.fn_execution.metrics.process.metric_impl import GenericMetricGroup
from pyflink.fn_execution.utils import operation_utils
from pyflink.table import FunctionContext

ASYNC_SCALAR_FUNCTION_URN = "flink:transform:async_scalar_function:v1"


class AsyncScalarFunctionOperation(Operation, AsyncOperationMixin):
    """
    Operation for executing Python async scalar functions.

    This operation implements true asynchronous execution by leveraging the async
    infrastructure from DataStream API's AsyncOperation:
    - AsyncFunctionRunner: Manages asyncio event loop in a separate thread
    - Queue: Maintains in-flight async operations with configurable capacity
    - Emitter: Collects and emits results asynchronously
    - Non-blocking: Multiple async operations can be in-flight simultaneously

    This provides high performance for I/O-bound async operations compared to
    synchronous blocking execution.
    """

    def __init__(self, serialized_fn):
        if serialized_fn.metric_enabled:
            self.base_metric_group = GenericMetricGroup(None, None)
        else:
            self.base_metric_group = None

        self._capacity = serialized_fn.async_options.max_concurrent_operations
        self._timeout = serialized_fn.async_options.timeout_ms / 1000.0
        self._retry_enabled = serialized_fn.async_options.retry_enabled
        self._max_attempts = serialized_fn.async_options.retry_max_attempts
        self._retry_delay = serialized_fn.async_options.retry_delay_ms / 1000.0

        scalar_function, variable_dict, self.user_defined_funcs = \
            operation_utils.extract_user_defined_function(
                serialized_fn.udfs[0], one_arg_optimization=False)

        # Create the eval function
        self._eval_func = eval('lambda value: %s' % scalar_function, variable_dict)

        # Create ordered queue to maintain result order
        self._queue = OrderedStreamElementQueue(self._capacity, self._raise_exception_if_exists)

        # Async execution components
        self._async_function_runner = None
        self._emitter = None
        self._exception = None
        self._output_processor = None

        # Job parameters
        self._job_parameters = {p.key: p.value for p in serialized_fn.job_parameters}

    def set_output_processor(self, output_processor):
        """Set the output processor for emitting results.

        This method is called by FunctionOperation for AsyncOperationMixin implementations.
        """
        self._output_processor = output_processor

    def open(self):
        # Open user defined functions
        for user_defined_func in self.user_defined_funcs:
            if hasattr(user_defined_func, 'open'):
                user_defined_func.open(
                    FunctionContext(self.base_metric_group, self._job_parameters))

        # Start emitter thread to collect async results
        self._emitter = Emitter(self._mark_exception, self._output_processor, self._queue)
        self._emitter.daemon = True
        self._emitter.start()

        # Start async function runner with event loop
        self._async_function_runner = AsyncFunctionRunner()
        self._async_function_runner.daemon = True
        self._async_function_runner.start()
        self._async_function_runner.wait_ready()

    def close(self):
        # Stop emitter
        if self._emitter is not None:
            self._emitter.stop()
            self._emitter = None

        # Stop async function runner
        if self._async_function_runner is not None:
            self._async_function_runner.stop()
            self._async_function_runner = None

        self._exception = None

        # Close user defined functions
        for user_defined_func in self.user_defined_funcs:
            if hasattr(user_defined_func, 'close'):
                user_defined_func.close()

    def process_element(self, value):
        """
        Process an input element asynchronously.

        This is non-blocking - it submits the async operation and returns immediately,
        allowing multiple operations to be in-flight simultaneously.
        """
        self._raise_exception_if_exists()

        entry = self._queue.put(None, 0, 0, value)

        async def execute_async(rh):
            try:
                # Call the eval function
                result = self._eval_func(value)

                # Check if any result is a coroutine and await it
                if asyncio.iscoroutine(result):
                    final_result = await result
                else:
                    final_result = result

                # Complete with results (list format)
                rh.complete([final_result])
            except Exception as e:
                rh.complete_exceptionally(e)

        # Create result handler
        result_handler = ResultHandler(
            self.__class__.__name__,
            self._timeout_func,
            self._mark_exception,
            value,
            entry)

        if self._retry_enabled:
            retry_strategy = async_retry_strategies.FixedDelayRetryStrategy(
                max_attempts=self._max_attempts,
                backoff_time_millis=int(self._retry_delay * 1000),
                result_predicate=None,
                exception_predicate=lambda ex: True)  # Retry on all exceptions

            # Wrap with retryable handler
            retryable_handler = RetryableResultHandler(
                result_handler,
                lambda r, h: self._async_function_runner.run_async(
                    lambda _: execute_async(h), r, h),
                retry_strategy)

            # Register timeout
            assert self._timeout > 0
            retryable_handler.register_timeout(self._timeout)

            # Submit to event loop asynchronously
            self._async_function_runner.run_async(
                lambda r: execute_async(retryable_handler),
                value,
                retryable_handler)
        else:
            # Register timeout if configured
            if self._timeout > 0:
                result_handler.register_timeout(self._timeout)

            # Submit to event loop asynchronously
            self._async_function_runner.run_async(
                lambda r: execute_async(result_handler),
                value,
                result_handler)

    def _timeout_func(self, record):
        """Handle timeout for async operations."""
        raise TimeoutError("Async function call has timed out for input: " + str(record))

    def finish(self):
        """Wait for all in-flight async operations to complete."""
        self._wait_for_in_flight_inputs_finished()
        self._update_gauge(self.base_metric_group)

    def _wait_for_in_flight_inputs_finished(self):
        """Wait until all in-flight async operations are completed."""
        while not self._queue.is_empty():
            self._queue.wait_for_in_flight_elements_processed()
            self._raise_exception_if_exists()

    def _mark_exception(self, exception):
        """Mark an exception that occurred during async execution."""
        self._exception = exception

    def _raise_exception_if_exists(self):
        """Raise exception if one occurred during async execution."""
        if self._exception is not None:
            raise self._exception

    def _update_gauge(self, base_metric_group):
        """Update metric gauges."""
        if base_metric_group is not None:
            for name in base_metric_group._flink_gauge:
                flink_gauge = base_metric_group._flink_gauge[name]
                beam_gauge = base_metric_group._beam_gauge[name]
                beam_gauge.set(flink_gauge())
            for sub_group in base_metric_group._sub_groups:
                self._update_gauge(sub_group)
