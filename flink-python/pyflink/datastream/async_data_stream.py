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
from pyflink.datastream.functions import AsyncFunctionDescriptor, AsyncFunction, AsyncRetryStrategy
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import get_j_env_configuration


class AsyncDataStream(object):
    """
    A helper class to apply :class:`~AsyncFunction` to a data stream.
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
