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
import pickle
import threading
from typing import TypeVar, Generic, List, Iterable, Callable

from pyflink.datastream import RuntimeContext, ResultFuture
from pyflink.datastream.functions import AsyncFunctionDescriptor
from pyflink.fn_execution.datastream.process.async_function.queue import \
    UnorderedStreamElementQueue, StreamElementQueue
from pyflink.fn_execution.datastream.process.operations import Operation
from pyflink.fn_execution.datastream.process.runtime_context import StreamingRuntimeContext

IN = TypeVar('IN')
OUT = TypeVar('OUT')


class AtomicBoolean(object):
    def __init__(self, initial_value=False):
        self._value = initial_value
        self._lock = threading.Lock()

    def get(self):
        with self._lock:
            return self._value

    def set(self, new_value):
        with self._lock:
            self._value = new_value

    def get_and_set(self, new_value):
        with self._lock:
            old_value = self._value
            self._value = new_value
            return old_value

    def compare_and_set(self, expected, new_value):
        with self._lock:
            if self._value == expected:
                self._value = new_value
                return True
            return False


class ResultHandler(ResultFuture, Generic[IN, OUT]):

    def __init__(self,
                 classname: str,
                 timeout_func: Callable[[IN, ResultFuture[[OUT]]], None],
                 exception_handler: Callable[[Exception], None],
                 record: IN,
                 result_future: ResultFuture[OUT]):
        self._classname = classname
        self._timeout_func = timeout_func
        self._exception_handler = exception_handler
        self._record = record
        self._result_future = result_future
        self._timer = None
        self._completed = AtomicBoolean(False)

    def register_timeout(self, timeout: int):
        self._timer = threading.Timer(timeout, self._timer_triggered)
        self._timer.start()

    def complete(self, result: List[OUT]):
        # already completed (exceptionally or with previous complete call from ill-written
        # AsyncFunction), so ignore additional result
        if not self._completed.compare_and_set(False, True):
            return

        if isinstance(result, Iterable):
            self._process_results(result)
        else:
            # complete with empty result, so that we remove timer and move ahead processing
            self._process_results([])

        if not isinstance(result, Iterable):
            raise RuntimeError("The 'result_future' of AsyncFunction should be completed with "
                               "data of list type, please check the methods 'async_invoke' and "
                               "'timeout' of class '%s'." % self._classname)

    def complete_exceptionally(self, error: Exception):
        # already completed, so ignore exception
        if not self._completed.compare_and_set(False, True):
            return

        self._exception_handler(
            Exception("Could not complete the element:" + str(self._record), error))

        #  complete with empty result, so that we remove timer and move ahead processing
        self._process_results([])

    def _process_results(self, result: List[OUT]):
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

        self._result_future.complete(result)

    def _timer_triggered(self):
        if not self._completed.get():
            self._timeout_func(self._record, self)


class Emitter(threading.Thread):

    def __init__(self,
                 exception_handler: Callable[[Exception], None],
                 output_processor,
                 queue: StreamElementQueue):
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


class AsyncFunctionRunner(threading.Thread):
    def __init__(self, exception_handler: Callable[[Exception], None]):
        super().__init__()
        self._exception_handler = exception_handler
        self._loop = None

    def run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def stop(self):
        if self._loop is not None:
            self._loop.stop()
            self._loop = None

    async def exception_handler_wrapper(self, async_function, *arg):
        try:
            await async_function(*arg)
        except Exception as e:
            self._exception_handler(e)

    def run_async(self, async_function, *arg):
        wrapped_function = self.exception_handler_wrapper(async_function, *arg)
        asyncio.run_coroutine_threadsafe(wrapped_function, self._loop)


class AsyncOperation(Operation):
    def __init__(self, serialized_fn, operator_state_backend):
        super(AsyncOperation, self).__init__(serialized_fn, operator_state_backend)
        (
            self.class_name,
            self.open_func,
            self.close_func,
            self.async_invoke_func,
            self.timeout_func,
            self._timeout,
            capacity,
            output_mode
        ) = extract_async_function(
            user_defined_function_proto=serialized_fn,
            runtime_context=StreamingRuntimeContext.of(
                serialized_fn.runtime_context, self.base_metric_group
            )
        )
        self._retry_enabled = False
        if output_mode == AsyncFunctionDescriptor.OutputMode.UNORDERED:
            self._queue = UnorderedStreamElementQueue(capacity, self._raise_exception_if_exists)
        else:
            raise NotImplementedError()
        self._emitter = None
        self._async_function_runner = None
        self._exception = None

    def set_output_processor(self, output_processor):
        self._output_processor = output_processor

    def open(self):
        self.open_func()
        self._emitter = Emitter(self._mark_exception, self._output_processor, self._queue)
        self._emitter.daemon = True
        self._emitter.start()

        self._async_function_runner = AsyncFunctionRunner(self._mark_exception)
        self._async_function_runner.daemon = True
        self._async_function_runner.start()

    def close(self):
        self.close_func()
        if self._emitter is not None:
            self._emitter.stop()
            self._emitter = None

        if self._async_function_runner is not None:
            self._async_function_runner.stop()
            self._async_function_runner = None

    def process_element(self, windowed_value, element):
        self._raise_exception_if_exists()

        # VALUE[CURRENT_TIMESTAMP, CURRENT_WATERMARK, NORMAL_DATA]
        timestamp = element[0]
        watermark = element[1]
        record = element[2]

        self._queue.advance_watermark(watermark)
        entry = self._queue.put(windowed_value, timestamp, watermark, record)

        if self._retry_enabled:
            raise NotImplementedError
        else:
            result_handler = ResultHandler(
                self.class_name, self.timeout_func, self._mark_exception, record, entry)
            if self._timeout > 0:
                result_handler.register_timeout(self._timeout)
            self._async_function_runner.run_async(self.async_invoke_func, record, result_handler)

    def finish(self):
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


def extract_async_function(user_defined_function_proto, runtime_context: RuntimeContext):
    """
    Extracts user-defined-function from the proto representation of a
    :class:`Function`.

    :param user_defined_function_proto: the proto representation of the Python :class:`Function`
    :param runtime_context: the streaming runtime context
    """
    async_function_descriptor = pickle.loads(user_defined_function_proto.payload)
    async_function = async_function_descriptor.async_function
    class_name = type(async_function)
    timeout = async_function_descriptor.timeout.to_milliseconds() / 1000
    capacity = async_function_descriptor.capacity
    output_mode = async_function_descriptor.output_mode

    def open_func():
        if hasattr(async_function, "open"):
            async_function.open(runtime_context)

    def close_func():
        if hasattr(async_function, "close"):
            async_function.close()

    async_invoke_func = async_function.async_invoke
    timeout_func = async_function.timeout

    return (class_name, open_func, close_func, async_invoke_func, timeout_func, timeout, capacity,
            output_mode)
