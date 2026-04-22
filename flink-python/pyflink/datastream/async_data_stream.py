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
import inspect

from pyflink.common import Time, TypeInformation
from pyflink.datastream import async_retry_strategies
from pyflink.datastream.data_stream import DataStream, _get_one_input_stream_operator
from pyflink.datastream.functions import (AsyncFunctionDescriptor, AsyncFunction,
                                          AsyncRetryStrategy, AsyncBatchFunction,
                                          AsyncBatchFunctionDescriptor)
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import get_j_env_configuration


class AsyncDataStream(object):
    """
    A helper class to apply :class:`~AsyncFunction` or :class:`~AsyncBatchFunction`
    to a data stream.

    .. versionchanged:: 2.1.0
        Added batch async support via :func:`unordered_wait_batch`.
    """

    @staticmethod
    def unordered_wait(
            data_stream: DataStream,
            async_function: AsyncFunction,
            timeout: Time,
            capacity: int = 100,
            output_type: TypeInformation = None) -> 'DataStream':
        """
        Adds an async function to the data stream. The order of output stream records may be
        reordered.

        :param data_stream: The input data stream.
        :param async_function: The async function.
        :param timeout: The timeout for the asynchronous operation to complete.
        :param capacity: The max number of async i/o operation that can be triggered.
        :param output_type: The output data type.
        :return: The transformed DataStream.
        """
        return AsyncDataStream.unordered_wait_with_retry(
            data_stream, async_function, timeout, async_retry_strategies.NO_RETRY_STRATEGY,
            capacity, output_type)

    @staticmethod
    def unordered_wait_with_retry(
            data_stream: DataStream,
            async_function: AsyncFunction,
            timeout: Time,
            async_retry_strategy: AsyncRetryStrategy,
            capacity: int = 100,
            output_type: TypeInformation = None) -> 'DataStream':
        """
        Adds an async function with an AsyncRetryStrategy to support retry of AsyncFunction to the
        data stream. The order of output stream records may be reordered.

        :param data_stream: The input data stream.
        :param async_function: The async function.
        :param timeout: The timeout for the asynchronous operation to complete.
        :param async_retry_strategy: The strategy of reattempt async i/o operation that can be
                                     triggered
        :param capacity: The max number of async i/o operation that can be triggered.
        :param output_type: The output data type.
        :return: The transformed DataStream.
        """
        AsyncDataStream._validate(data_stream, async_function, timeout, async_retry_strategy)

        from pyflink.fn_execution import flink_fn_execution_pb2
        j_python_data_stream_function_operator, j_output_type_info = \
            _get_one_input_stream_operator(
                data_stream,
                AsyncFunctionDescriptor(
                    async_function, timeout, capacity, async_retry_strategy,
                    AsyncFunctionDescriptor.OutputMode.UNORDERED),
                flink_fn_execution_pb2.UserDefinedDataStreamFunction.PROCESS,  # type: ignore
                output_type)
        return DataStream(data_stream._j_data_stream.transform(
            "async wait operator",
            j_output_type_info,
            j_python_data_stream_function_operator))

    @staticmethod
    def unordered_wait_batch(
            data_stream: DataStream,
            async_batch_function: AsyncBatchFunction,
            timeout: Time,
            batch_size: int,
            batch_timeout: Time = None,
            capacity: int = 100,
            output_type: TypeInformation = None) -> 'DataStream':
        """
        Adds an async batch function to the data stream with batch-oriented async execution.
        The order of output stream records may be reordered.

        This method is designed for AI/ML inference scenarios and other high-latency external
        service calls where batching can significantly improve throughput. The Java-side
        AsyncBatchWaitOperator collects input elements into batches based on the configured
        batch_size and batch_timeout, then invokes the Python async batch function.

        Example usage::

            class MyAsyncBatchFunction(AsyncBatchFunction):

                async def async_invoke_batch(self, inputs: List[Row]) -> List[int]:
                    # Process batch of inputs together (e.g., ML model inference)
                    results = await model.predict_batch(inputs)
                    return results

            ds = AsyncDataStream.unordered_wait_batch(
                ds,
                MyAsyncBatchFunction(),
                timeout=Time.seconds(10),
                batch_size=32,
                batch_timeout=Time.milliseconds(100),
                output_type=Types.INT()
            )

        :param data_stream: The input data stream.
        :param async_batch_function: The async batch function to apply.
        :param timeout: The overall timeout for asynchronous operations.
        :param batch_size: Maximum number of elements to batch together before triggering
                          the async function. Must be positive.
        :param batch_timeout: Maximum time to wait before flushing a partial batch.
                             If None, only batch_size triggers flushing.
        :param capacity: The max number of async operations that can be in-flight.
        :param output_type: The output data type.
        :return: The transformed DataStream.

        .. versionadded:: 2.1.0

        .. note:: This is a :class:`PublicEvolving` API and may change in future versions.
        """
        AsyncDataStream._validate_batch(
            data_stream, async_batch_function, timeout, batch_size, batch_timeout)

        from pyflink.fn_execution import flink_fn_execution_pb2
        j_python_data_stream_function_operator, j_output_type_info = \
            _get_one_input_stream_operator(
                data_stream,
                AsyncBatchFunctionDescriptor(
                    async_batch_function,
                    timeout,
                    batch_size,
                    batch_timeout,
                    capacity,
                    AsyncBatchFunctionDescriptor.OutputMode.UNORDERED),
                flink_fn_execution_pb2.UserDefinedDataStreamFunction.ASYNC_BATCH,  # type: ignore
                output_type)
        return DataStream(data_stream._j_data_stream.transform(
            "async batch wait operator",
            j_output_type_info,
            j_python_data_stream_function_operator))

    @staticmethod
    def ordered_wait(
            data_stream: DataStream,
            async_function: AsyncFunction,
            timeout: Time,
            capacity: int = 100,
            output_type: TypeInformation = None) -> 'DataStream':
        """
        Adds an async function to the data stream. The order to process input records
        is guaranteed to be the same as input ones.

        :param data_stream: The input data stream.
        :param async_function: The async function.
        :param timeout: The timeout for the asynchronous operation to complete.
        :param capacity: The max number of async i/o operation that can be triggered.
        :param output_type: The output data type.
        :return: The transformed DataStream.
        """
        return AsyncDataStream.ordered_wait_with_retry(
            data_stream, async_function, timeout, async_retry_strategies.NO_RETRY_STRATEGY,
            capacity, output_type)

    @staticmethod
    def ordered_wait_with_retry(
            data_stream: DataStream,
            async_function: AsyncFunction,
            timeout: Time,
            async_retry_strategy: AsyncRetryStrategy,
            capacity: int = 100,
            output_type: TypeInformation = None) -> 'DataStream':
        """
        Adds an async function with an AsyncRetryStrategy to support retry of AsyncFunction to the
        data stream. The order to process input records is guaranteed to be the same as input ones.

        :param data_stream: The input data stream.
        :param async_function: The async function.
        :param timeout: The timeout for the asynchronous operation to complete.
        :param async_retry_strategy: The strategy of reattempt async i/o operation that can be
                                     triggered
        :param capacity: The max number of async i/o operation that can be triggered.
        :param output_type: The output data type.
        :return: The transformed DataStream.
        """
        AsyncDataStream._validate(data_stream, async_function, timeout, async_retry_strategy)

        from pyflink.fn_execution import flink_fn_execution_pb2
        j_python_data_stream_function_operator, j_output_type_info = \
            _get_one_input_stream_operator(
                data_stream,
                AsyncFunctionDescriptor(
                    async_function, timeout, capacity, async_retry_strategy,
                    AsyncFunctionDescriptor.OutputMode.ORDERED),
                flink_fn_execution_pb2.UserDefinedDataStreamFunction.PROCESS,  # type: ignore
                output_type)
        return DataStream(data_stream._j_data_stream.transform(
            "async wait operator",
            j_output_type_info,
            j_python_data_stream_function_operator))

    @staticmethod
    def ordered_wait_batch(
            data_stream: DataStream,
            async_batch_function: AsyncBatchFunction,
            timeout: Time,
            batch_size: int,
            batch_timeout: Time = None,
            capacity: int = 100,
            output_type: TypeInformation = None) -> 'DataStream':
        """
        Adds an async batch function to the data stream with batch-oriented async execution.
        The order of output stream records is guaranteed to be the same as input ones.

        This method is designed for AI/ML inference scenarios and other high-latency external
        service calls where batching can significantly improve throughput while maintaining
        output order. The Java-side AsyncBatchWaitOperator collects input elements into batches
        based on the configured batch_size and batch_timeout, then invokes the Python async
        batch function, and emits results in the same order as inputs.

        :param data_stream: The input data stream.
        :param async_batch_function: The async batch function to apply.
        :param timeout: The overall timeout for asynchronous operations.
        :param batch_size: Maximum number of elements to batch together before triggering
                          the async function. Must be positive.
        :param batch_timeout: Maximum time to wait before flushing a partial batch.
                             If None, only batch_size triggers flushing.
        :param capacity: The max number of async operations that can be in-flight.
        :param output_type: The output data type.
        :return: The transformed DataStream.

        .. versionadded:: 2.1.0

        .. note:: This is a :class:`PublicEvolving` API and may change in future versions.
        """
        AsyncDataStream._validate_batch(
            data_stream, async_batch_function, timeout, batch_size, batch_timeout)

        from pyflink.fn_execution import flink_fn_execution_pb2
        j_python_data_stream_function_operator, j_output_type_info = \
            _get_one_input_stream_operator(
                data_stream,
                AsyncBatchFunctionDescriptor(
                    async_batch_function,
                    timeout,
                    batch_size,
                    batch_timeout,
                    capacity,
                    AsyncBatchFunctionDescriptor.OutputMode.ORDERED),
                flink_fn_execution_pb2.UserDefinedDataStreamFunction.ASYNC_BATCH,  # type: ignore
                output_type)
        return DataStream(data_stream._j_data_stream.transform(
            "async batch wait operator (ordered)",
            j_output_type_info,
            j_python_data_stream_function_operator))

    @staticmethod
    def _validate(data_stream: DataStream, async_function: AsyncFunction,
                  timeout: Time, async_retry_strategy: AsyncRetryStrategy) -> None:
        if not inspect.iscoroutinefunction(async_function.async_invoke):
            raise Exception("Method 'async_invoke' of class '%s' should be declared as 'async def'."
                            % type(async_function))

        if async_retry_strategy is None:
            raise Exception("Async retry strategy should not be None.")
        if (async_retry_strategy != async_retry_strategies.NO_RETRY_STRATEGY and
                timeout.to_milliseconds() <= 0):
            raise Exception("Timeout should be configured when do async with retry.")
        if async_retry_strategy.get_retry_predicate() is None:
            raise Exception("Retry predicate of the async retry strategy '%s' is None."
                            % type(async_retry_strategy))

        gateway = get_gateway()
        j_conf = get_j_env_configuration(data_stream._j_data_stream.getExecutionEnvironment())
        python_execution_mode = (
            j_conf.get(gateway.jvm.org.apache.flink.python.PythonOptions.PYTHON_EXECUTION_MODE))
        if python_execution_mode == 'thread':
            raise Exception("AsyncFunction is still not supported for 'thread' mode.")

    @staticmethod
    def _validate_batch(data_stream: DataStream, async_batch_function: AsyncBatchFunction,
                        timeout: Time, batch_size: int, batch_timeout: Time) -> None:
        """
        Validates the parameters for async batch operations.
        """
        if not inspect.iscoroutinefunction(async_batch_function.async_invoke_batch):
            raise Exception(
                "Method 'async_invoke_batch' of class '%s' should be declared as 'async def'."
                % type(async_batch_function))

        if batch_size <= 0:
            raise Exception("Batch size must be positive, got: %d" % batch_size)

        if timeout is None or timeout.to_milliseconds() <= 0:
            raise Exception("Timeout must be positive for async batch operations.")

        if batch_timeout is not None and batch_timeout.to_milliseconds() < 0:
            raise Exception("Batch timeout cannot be negative, got: %d ms"
                            % batch_timeout.to_milliseconds())

        gateway = get_gateway()
        j_conf = get_j_env_configuration(data_stream._j_data_stream.getExecutionEnvironment())
        python_execution_mode = (
            j_conf.get(gateway.jvm.org.apache.flink.python.PythonOptions.PYTHON_EXECUTION_MODE))
        if python_execution_mode == 'thread':
            raise Exception("AsyncBatchFunction is still not supported for 'thread' mode.")
