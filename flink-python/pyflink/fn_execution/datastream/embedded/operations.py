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

from pyflink.fn_execution import pickle
from pyflink.fn_execution.datastream import operations
from pyflink.fn_execution.datastream.embedded.process_function import (
    InternalProcessFunctionContext, InternalKeyedProcessFunctionContext,
    InternalKeyedProcessFunctionOnTimerContext)
from pyflink.fn_execution.datastream.embedded.runtime_context import StreamingRuntimeContext


class OneInputOperation(operations.OneInputOperation):
    def __init__(self, open_func, close_func, process_element_func, on_timer_func=None):
        self._open_func = open_func
        self._close_func = close_func
        self._process_element_func = process_element_func
        self._on_timer_func = on_timer_func

    def open(self) -> None:
        self._open_func()

    def close(self) -> None:
        self._close_func()

    def process_element(self, value):
        return self._process_element_func(value)

    def on_timer(self, timestamp):
        if self._on_timer_func:
            return self._on_timer_func(timestamp)


def extract_process_function(
        user_defined_function_proto, runtime_context, function_context, timer_context,
        job_parameters):
    from pyflink.fn_execution import flink_fn_execution_pb2

    user_defined_func = pickle.loads(user_defined_function_proto.payload)

    func_type = user_defined_function_proto.function_type

    UserDefinedDataStreamFunction = flink_fn_execution_pb2.UserDefinedDataStreamFunction

    runtime_context = StreamingRuntimeContext.of(runtime_context, job_parameters)

    def open_func():
        if hasattr(user_defined_func, "open"):
            user_defined_func.open(runtime_context)

    def close_func():
        if hasattr(user_defined_func, "close"):
            user_defined_func.close()

    if func_type == UserDefinedDataStreamFunction.PROCESS:
        function_context = InternalProcessFunctionContext(function_context)

        process_element = user_defined_func.process_element

        def process_element_func(value):
            yield from process_element(value, function_context)

        return OneInputOperation(open_func, close_func, process_element_func)

    elif func_type == UserDefinedDataStreamFunction.KEYED_PROCESS:

        function_context = InternalKeyedProcessFunctionContext(
            function_context, user_defined_function_proto.key_type_info)

        timer_context = InternalKeyedProcessFunctionOnTimerContext(
            timer_context, user_defined_function_proto.key_type_info)

        on_timer = user_defined_func.on_timer

        def process_element(value, context):
            return user_defined_func.process_element(value[1], context)

        def on_timer_func(timestamp):
            yield from on_timer(timestamp, timer_context)

        def process_element_func(value):
            yield from process_element(value, function_context)

        return OneInputOperation(open_func, close_func, process_element_func, on_timer_func)
    else:
        raise Exception("Unknown function type {0}.".format(func_type))
