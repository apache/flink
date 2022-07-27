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
from pyflink.fn_execution.datastream.operations import DATA_STREAM_STATELESS_FUNCTION_URN


class OneInputOperation(operations.OneInputOperation):
    def __init__(self,
                 function_urn,
                 serialized_fn,
                 runtime_context,
                 function_context,
                 timer_context,
                 job_parameters):
        (self.open_func,
         self.close_func,
         self.process_element_func,
         self.on_timer_func
         ) = extract_one_input_process_function(
            function_urn=function_urn,
            user_defined_function_proto=serialized_fn,
            runtime_context=StreamingRuntimeContext.of(runtime_context, job_parameters),
            function_context=function_context,
            timer_context=timer_context)

    def open(self) -> None:
        self.open_func()

    def close(self) -> None:
        self.close_func()

    def process_element(self, value):
        return self.process_element_func(value)

    def on_timer(self, timestamp):
        return self.on_timer_func(timestamp)


def extract_one_input_process_function(
        function_urn, user_defined_function_proto, runtime_context, function_context,
        timer_context):
    user_defined_func = pickle.loads(user_defined_function_proto.payload)

    def open_func():
        if hasattr(user_defined_func, "open"):
            user_defined_func.open(runtime_context)

    def close_func():
        if hasattr(user_defined_func, "close"):
            user_defined_func.close()

    if function_urn == DATA_STREAM_STATELESS_FUNCTION_URN:
        function_context = InternalProcessFunctionContext(function_context)

        process_element = user_defined_func.process_element

        def on_timer_func(timestamp):
            raise Exception("This method should not be called.")
    else:

        function_context = InternalKeyedProcessFunctionContext(function_context)

        timer_context = InternalKeyedProcessFunctionOnTimerContext(timer_context)

        on_timer = user_defined_func.on_timer

        def process_element(value, context):
            return user_defined_func.process_element(value[1], context)

        def on_timer_func(timestamp):
            yield from on_timer(timestamp, timer_context)

    def process_element_func(value):
        yield from process_element(value, function_context)

    return open_func, close_func, process_element_func, on_timer_func
