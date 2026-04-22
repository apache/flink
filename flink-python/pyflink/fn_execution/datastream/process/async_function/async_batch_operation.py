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
Async batch function operation for AI/ML inference scenarios.

This module provides the runtime operation for batch-oriented async functions,
enabling efficient processing of batched inputs for machine learning inference
and other high-latency external service calls.

.. versionadded:: 2.1.0
"""
import asyncio
import pickle
import threading
from typing import TypeVar, Generic, List, Iterable

from pyflink.datastream import RuntimeContext
from pyflink.datastream.functions import AsyncBatchFunctionDescriptor
from pyflink.fn_execution.datastream.process.async_function.queue import (
    UnorderedStreamElementQueue, StreamElementQueue, OrderedStreamElementQueue, ResultFuture
)
from pyflink.fn_execution.datastream.process.operations import Operation
from pyflink.fn_execution.datastream.process.runtime_context import StreamingRuntimeContext

IN = TypeVar('IN')
OUT = TypeVar('OUT')


class AsyncBatchResultHandler(ResultFuture, Generic[IN, OUT]):
    """
    Handler for async batch operation results.

    This handler manages the completion of batch async operations, including
    timeout handling and result processing.
    """

    def __init__(self,
                 classname: str,
                 timeout_func,
                 exception_handler,
                 batch_records: List[IN],
                 result_future: ResultFuture[OUT]):
        self._classname = classname
        self._timeout_func = timeout_func
        self._exception_handler = exception_handler
        self._batch_records = batch_records
        self._result_future = result_future
        self._timer = None
        self._completed = threading.Event()

    def register_timeout(self, timeout: float):
        """Register a timeout timer for this batch operation."""
        self._timer = threading.Timer(timeout, self._timer_triggered)
        self._timer.start()

    def complete(self, result: List[OUT]):
        """Complete the batch operation with results."""
        if self._completed.is_set():
            return

        self._completed.set()
        self._complete_internal(result)

    def _complete_internal(self, result: List[OUT]):
        if isinstance(result, Iterable) and not isinstance(result, (str, bytes)):
            self._process_results(list(result))
        else:
            if result is None:
                self._exception_handler(
                    RuntimeError("The result of AsyncBatchFunction cannot be none, "
                                 "please check the method 'async_invoke_batch' and "
                                 "'timeout_batch' of class '%s'." % self._classname))
            else:
                self._exception_handler(
                    RuntimeError("The result of AsyncBatchFunction should be of list type, "
                                 "please check the method 'async_invoke_batch' and "
                                 "'timeout_batch' of class '%s'." % self._classname))

            # complete with empty result to move forward
            self._process_results([])

    def complete_exceptionally(self, error: Exception):
        """Complete the batch operation with an exception."""
        if self._completed.is_set():
            return

        self._completed.set()
        self._complete_exceptionally_internal(error)

    def _complete_exceptionally_internal(self, error: Exception):
        self._exception_handler(Exception(
            "Error happens inside the class '%s' during handling batch input"
            % self._classname, error))

        # complete with empty result to move forward
        self._process_results([])

    def _process_results(self, result: List[OUT]):
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

        self._result_future.complete(result)

    def _timer_triggered(self):
        """Handle timeout by calling timeout_batch."""
        if self._completed.is_set():
            return

        self._completed.set()

        try:
            result = self._timeout_func(self._batch_records)
            self._complete_internal(result)
        except Exception as error:
            self._complete_exceptionally_internal(error)


class AsyncBatchEmitter(threading.Thread):
    """
    Emitter thread that processes completed batch results.

    This thread continuously polls for completed elements in the queue
    and emits them to the output processor.
    """

    def __init__(self, exception_handler, output_processor, queue: StreamElementQueue):
        super().__init__()
        self._exception_handler = exception_handler
        self._output_processor = output_processor
        self._queue = queue
        self._running = True

    def run(self):
        while self._running:
            try:
                if self._queue.has_completed_elements():
                    self._queue.emit_completed_element(self._output_processor)
                else:
                    self._queue.wait_for_completed_elements()
            except Exception as e:
                self._running = False
                self._exception_handler(e)

    def stop(self):
        self._running = False


class AsyncBatchFunctionRunner(threading.Thread):
    """
    Runner thread that executes async batch functions.

    This thread maintains an event loop for running coroutines asynchronously.
    """

    def __init__(self):
        super().__init__()
        self._loop = None
        self._ready = threading.Event()

    def run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        # notify that the event loop is ready
        self._ready.set()

        try:
            self._loop.run_forever()
        finally:
            self._loop.close()

    def wait_ready(self):
        """Wait until the event loop is ready."""
        return self._ready.wait()

    def stop(self):
        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
            self.join(timeout=1.0)

    async def exception_handler_wrapper(self, async_batch_function, batch_records, result_handler):
        """Wrapper that catches exceptions from the async batch function."""
        try:
            result = await async_batch_function(batch_records)
            result_handler.complete(result)
        except Exception as e:
            result_handler.complete_exceptionally(e)

    def run_async_batch(self, async_batch_function, batch_records, result_handler):
        """Schedule an async batch function for execution."""
        wrapped_function = self.exception_handler_wrapper(
            async_batch_function, batch_records, result_handler)
        asyncio.run_coroutine_threadsafe(wrapped_function, self._loop)


class AsyncBatchOperation(Operation):
    """
    Operation for executing async batch functions.

    This operation collects input elements into batches based on configured
    batch_size and batch_timeout, then invokes the async batch function.

    The operation is designed for AI/ML inference scenarios where batching
    can significantly improve throughput by better utilizing GPU resources
    or batch APIs of external services.
    """

    def __init__(self, serialized_fn, operator_state_backend):
        super(AsyncBatchOperation, self).__init__(serialized_fn, operator_state_backend)

        (
            self.class_name,
            self.open_func,
            self.close_func,
            self.async_invoke_batch_func,
            self.timeout_batch_func,
            self._timeout,
            self._batch_size,
            self._batch_timeout,
            capacity,
            output_mode
        ) = extract_async_batch_function(
            user_defined_function_proto=serialized_fn,
            runtime_context=StreamingRuntimeContext.of(
                serialized_fn.runtime_context, self.base_metric_group
            )
        )

        # Create queue based on output mode
        if output_mode == AsyncBatchFunctionDescriptor.OutputMode.UNORDERED:
            self._queue = UnorderedStreamElementQueue(capacity, self._raise_exception_if_exists)
        else:
            self._queue = OrderedStreamElementQueue(capacity, self._raise_exception_if_exists)

        self._emitter = None
        self._async_batch_runner = None
        self._exception = None

        # Batch buffer
        self._batch_buffer = []
        self._batch_buffer_metadata = []  # (windowed_value, timestamp, watermark)
        self._batch_start_time = None
        self._batch_timer = None

    def set_output_processor(self, output_processor):
        self._output_processor = output_processor

    def open(self):
        self.open_func()

        self._emitter = AsyncBatchEmitter(self._mark_exception, self._output_processor, self._queue)
        self._emitter.daemon = True
        self._emitter.start()

        self._async_batch_runner = AsyncBatchFunctionRunner()
        self._async_batch_runner.daemon = True
        self._async_batch_runner.start()
        self._async_batch_runner.wait_ready()

    def close(self):
        # Flush any remaining buffered elements
        if self._batch_buffer:
            self._flush_batch()

        if self._emitter is not None:
            self._emitter.stop()
            self._emitter = None

        if self._async_batch_runner is not None:
            self._async_batch_runner.stop()
            self._async_batch_runner = None

        if self._batch_timer is not None:
            self._batch_timer.cancel()
            self._batch_timer = None

        self._exception = None

        self.close_func()

    def process_element(self, windowed_value, element):
        self._raise_exception_if_exists()

        # VALUE[CURRENT_TIMESTAMP, CURRENT_WATERMARK, NORMAL_DATA]
        timestamp = element[0]
        watermark = element[1]
        record = element[2]

        self._queue.advance_watermark(watermark)

        # Add to batch buffer
        self._batch_buffer.append(record)
        self._batch_buffer_metadata.append((windowed_value, timestamp, watermark))

        # Start batch timeout timer if this is the first element in the batch
        if len(self._batch_buffer) == 1 and self._batch_timeout is not None and self._batch_timeout > 0:
            self._batch_start_time = timestamp
            self._batch_timer = threading.Timer(self._batch_timeout, self._on_batch_timeout)
            self._batch_timer.start()

        # Flush if batch is full
        if len(self._batch_buffer) >= self._batch_size:
            self._flush_batch()

    def _on_batch_timeout(self):
        """Called when batch timeout is reached."""
        if self._batch_buffer:
            self._flush_batch()

    def _flush_batch(self):
        """Flush the current batch and invoke the async batch function."""
        if not self._batch_buffer:
            return

        # Cancel any pending timeout timer
        if self._batch_timer is not None:
            self._batch_timer.cancel()
            self._batch_timer = None

        # Get the current batch
        batch_records = self._batch_buffer
        batch_metadata = self._batch_buffer_metadata

        # Clear the buffer for next batch
        self._batch_buffer = []
        self._batch_buffer_metadata = []
        self._batch_start_time = None

        # Create entries in the queue for each element in the batch
        entries = []
        for windowed_value, timestamp, watermark in batch_metadata:
            entry = self._queue.put(windowed_value, timestamp, watermark, None)
            entries.append(entry)

        # Create result handler
        batch_result_handler = BatchResultDistributor(
            self.class_name,
            self.timeout_batch_func,
            self._mark_exception,
            batch_records,
            entries
        )

        if self._timeout > 0:
            batch_result_handler.register_timeout(self._timeout)

        # Run async batch function
        self._async_batch_runner.run_async_batch(
            self.async_invoke_batch_func, batch_records, batch_result_handler)

    def finish(self):
        # Flush any remaining buffered elements
        if self._batch_buffer:
            self._flush_batch()

        self._wait_for_in_flight_inputs_finished()
        super().finish()

    def _wait_for_in_flight_inputs_finished(self):
        while not self._queue.is_empty():
            self._queue.wait_for_in_flight_elements_processed()
            self._raise_exception_if_exists()

    def _mark_exception(self, exception):
        self._exception = exception

    def _raise_exception_if_exists(self):
        if self._exception is not None:
            raise self._exception


class BatchResultDistributor(ResultFuture):
    """
    Distributes batch results to individual queue entries.

    When a batch operation completes, this handler distributes the results
    to the corresponding queue entries for each input element.
    """

    def __init__(self, classname, timeout_func, exception_handler, batch_records, entries):
        self._classname = classname
        self._timeout_func = timeout_func
        self._exception_handler = exception_handler
        self._batch_records = batch_records
        self._entries = entries
        self._timer = None
        self._completed = threading.Event()

    def register_timeout(self, timeout: float):
        """Register a timeout timer for this batch operation."""
        self._timer = threading.Timer(timeout, self._timer_triggered)
        self._timer.start()

    def complete(self, result: List):
        """Distribute batch results to individual entries."""
        if self._completed.is_set():
            return

        self._completed.set()
        self._cancel_timer()

        if result is None:
            self._exception_handler(
                RuntimeError("The result of AsyncBatchFunction cannot be none, "
                             "please check the method 'async_invoke_batch' and "
                             "'timeout_batch' of class '%s'." % self._classname))
            self._complete_entries_empty()
            return

        if not isinstance(result, (list, tuple)):
            self._exception_handler(
                RuntimeError("The result of AsyncBatchFunction should be of list type, "
                             "please check the method 'async_invoke_batch' of class '%s'."
                             % self._classname))
            self._complete_entries_empty()
            return

        # Distribute results to entries
        result_list = list(result)
        for i, entry in enumerate(self._entries):
            if i < len(result_list):
                # Wrap single result in list for consistency with existing queue interface
                entry.complete([result_list[i]])
            else:
                entry.complete([])

    def complete_exceptionally(self, error: Exception):
        """Complete all entries exceptionally."""
        if self._completed.is_set():
            return

        self._completed.set()
        self._cancel_timer()

        self._exception_handler(Exception(
            "Error happens inside the class '%s' during handling batch input"
            % self._classname, error))

        self._complete_entries_empty()

    def _complete_entries_empty(self):
        """Complete all entries with empty results."""
        for entry in self._entries:
            entry.complete([])

    def _cancel_timer(self):
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _timer_triggered(self):
        """Handle timeout by calling timeout_batch."""
        if self._completed.is_set():
            return

        self._completed.set()
        self._cancel_timer()

        try:
            result = self._timeout_func(self._batch_records)
            if result is None or not isinstance(result, (list, tuple)):
                self._complete_entries_empty()
            else:
                result_list = list(result)
                for i, entry in enumerate(self._entries):
                    if i < len(result_list):
                        entry.complete([result_list[i]])
                    else:
                        entry.complete([])
        except Exception as error:
            self._exception_handler(Exception(
                "Error in timeout_batch of class '%s'" % self._classname, error))
            self._complete_entries_empty()


def extract_async_batch_function(user_defined_function_proto, runtime_context: RuntimeContext):
    """
    Extract async batch function configuration from the proto representation.

    :param user_defined_function_proto: the proto representation of the Python function
    :param runtime_context: the streaming runtime context
    :return: tuple of function components and configuration
    """
    async_batch_descriptor = pickle.loads(user_defined_function_proto.payload)
    async_batch_function = async_batch_descriptor.async_batch_function
    class_name = type(async_batch_function).__name__
    timeout = async_batch_descriptor.timeout.to_milliseconds() / 1000 if async_batch_descriptor.timeout else 0
    batch_size = async_batch_descriptor.batch_size
    batch_timeout = (async_batch_descriptor.batch_timeout.to_milliseconds() / 1000
                     if async_batch_descriptor.batch_timeout else None)
    capacity = async_batch_descriptor.capacity
    output_mode = async_batch_descriptor.output_mode

    def open_func():
        if hasattr(async_batch_function, "open"):
            async_batch_function.open(runtime_context)

    def close_func():
        if hasattr(async_batch_function, "close"):
            async_batch_function.close()

    async_invoke_batch_func = async_batch_function.async_invoke_batch
    timeout_batch_func = async_batch_function.timeout_batch

    return (class_name, open_func, close_func, async_invoke_batch_func, timeout_batch_func,
            timeout, batch_size, batch_timeout, capacity, output_mode)
