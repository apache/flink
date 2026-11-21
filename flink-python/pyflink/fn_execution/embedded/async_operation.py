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
import logging
import threading
import traceback
from collections.abc import Iterable

from pyflink.fn_execution import pickle
from pyflink.fn_execution.datastream.embedded.runtime_context import StreamingRuntimeContext
from pyflink.fn_execution.embedded.converters import from_type_info_proto
from pyflink.fn_execution.embedded.operation_utils import (
    pare_user_defined_data_stream_function_proto, parse_coder_proto)


class AsyncFunctionRunner(threading.Thread):
    """
    Runs an asyncio event loop on a daemon thread. Every submitted coroutine reports to its Java
    handler. If a coroutine ends without reporting, or the loop exits without being stopped, the
    handler is completed exceptionally, so that its queue entry can't wait forever.
    """

    def __init__(self):
        super(AsyncFunctionRunner, self).__init__(
            name="flink-python-async-function", daemon=True)
        self._loop = None
        self._ready = threading.Event()
        self._start_error = None
        self._lock = threading.Lock()
        self._pending = {}
        self._exited = False
        self._stop_requested = False

    def run(self):
        try:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        except BaseException as e:
            self._start_error = e
            self._exited = True
            self._ready.set()
            return
        self._ready.set()

        try:
            self._loop.run_forever()
        finally:
            with self._lock:
                self._exited = True
                stranded = list(self._pending.values())
                self._pending.clear()
            if not self._stop_requested:
                for j_handler in stranded:
                    j_handler.completeExceptionally(
                        "The event loop of the async function exited unexpectedly.")
            self._loop.close()

    def start_and_wait_ready(self):
        self.start()
        self._ready.wait()
        if self._start_error is not None:
            raise RuntimeError(
                "Failed to start the event loop of the async function.") from self._start_error

    @property
    def stop_requested(self):
        return self._stop_requested

    def submit(self, coroutine, j_handler):
        with self._lock:
            if self._exited or self._stop_requested:
                coroutine.close()
                raise RuntimeError("The event loop of the async function is not running.")
            future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
            self._pending[id(future)] = j_handler
        future.add_done_callback(self._remove)

    def _remove(self, future):
        with self._lock:
            j_handler = self._pending.pop(id(future), None)
        if j_handler is None or self._stop_requested:
            return
        # the coroutine normally reports itself; this catches what escapes it, for example a
        # BaseException or a cancellation before it started. Only the first report counts.
        if future.cancelled():
            j_handler.completeExceptionally("The async function call was cancelled.")
        elif future.exception() is not None:
            error = future.exception()
            j_handler.completeExceptionally(
                "".join(traceback.format_exception(type(error), error, error.__traceback__)))

    def stop(self):
        self._stop_requested = True
        if self._loop is not None and not self._loop.is_closed():
            try:
                self._loop.call_soon_threadsafe(self._loop.stop)
            except RuntimeError:
                # the loop was closed in the meantime
                pass
            self.join(timeout=1.0)
            if self.is_alive():
                logging.warning("The event loop of the async function didn't stop within 1 second.")


class AsyncFunctionOperation(object):
    """
    Runs a Python AsyncFunction for EmbeddedPythonAsyncWaitOperator. The Java operator owns the
    queue, the timers and the state. This class owns the event loop and reports each result, or
    the traceback of each failure, to the Java handler of the input.
    """

    def __init__(self, serialized_fn, input_data_converter, output_data_converter,
                 j_runtime_context, job_parameters):
        descriptor = pickle.loads(serialized_fn.payload)
        self._async_function = descriptor.async_function
        self._class_name = type(self._async_function)
        self._runtime_context = StreamingRuntimeContext.of(j_runtime_context, job_parameters)
        self._input_data_converter = input_data_converter
        self._output_data_converter = output_data_converter
        self._runner = None
        self._closed = False

    def open(self):
        self._async_function.open(self._runtime_context)
        try:
            self._runner = AsyncFunctionRunner()
            self._runner.start_and_wait_ready()
        except BaseException:
            self._closed = True
            self._async_function.close()
            raise

    def async_invoke(self, j_handler, value):
        record = self._input_data_converter.to_internal(value)
        self._runner.submit(self._invoke(j_handler, record), j_handler)

    async def _invoke(self, j_handler, record):
        try:
            result = await self._async_function.async_invoke(record)
            externals = self._to_externals(result)
        except (Exception, asyncio.CancelledError):
            if not self._runner.stop_requested:
                j_handler.completeExceptionally(traceback.format_exc())
            return
        j_handler.complete(externals)

    def timeout(self, j_handler, value):
        record = self._input_data_converter.to_internal(value)
        try:
            externals = self._to_externals(self._async_function.timeout(record))
        except Exception:
            j_handler.completeExceptionally(traceback.format_exc())
            return
        j_handler.complete(externals)

    def _to_externals(self, result):
        if result is None:
            raise RuntimeError("The result of AsyncFunction cannot be none, please check the "
                               "methods 'async_invoke' and 'timeout' of class '%s'."
                               % self._class_name)
        if not isinstance(result, Iterable):
            raise RuntimeError("The result of AsyncFunction should be of list type, please "
                               "check the methods 'async_invoke' and 'timeout' of class '%s'."
                               % self._class_name)
        return [self._output_data_converter.to_external(item) for item in result]

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            if self._runner is not None:
                self._runner.stop()
        finally:
            self._async_function.close()


def create_async_operation_from_protos(function_infos, input_coder_info, output_coder_info,
                                       runtime_context, job_parameters):
    serialized_fn = pare_user_defined_data_stream_function_proto(function_infos[0])
    input_data_converter = (
        from_type_info_proto(parse_coder_proto(input_coder_info).raw_type.type_info))
    output_data_converter = (
        from_type_info_proto(parse_coder_proto(output_coder_info).raw_type.type_info))
    return AsyncFunctionOperation(serialized_fn, input_data_converter, output_data_converter,
                                  runtime_context, job_parameters)
