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
from pyflink.datastream import OutputTag
from pyflink.datastream.window import WindowOperationDescriptor
from pyflink.fn_execution import pickle
from pyflink.fn_execution.coders import TimeWindowCoder, CountWindowCoder
from pyflink.fn_execution.datastream import operations
from pyflink.fn_execution.datastream.embedded.process_function import (
    InternalProcessFunctionContext, InternalKeyedProcessFunctionContext,
    InternalKeyedProcessFunctionOnTimerContext, InternalWindowTimerContext,
    InternalBroadcastProcessFunctionContext, InternalBroadcastProcessFunctionReadOnlyContext,
    InternalKeyedBroadcastProcessFunctionContext,
    InternalKeyedBroadcastProcessFunctionReadOnlyContext,
    InternalKeyedBroadcastProcessFunctionOnTimerContext)
from pyflink.fn_execution.datastream.embedded.runtime_context import StreamingRuntimeContext
from pyflink.fn_execution.datastream.embedded.side_output_context import SideOutputContext
from pyflink.fn_execution.datastream.embedded.timerservice_impl import InternalTimerServiceImpl
from pyflink.fn_execution.datastream.window.window_operator import WindowOperator
from pyflink.fn_execution.embedded.converters import (TimeWindowConverter, CountWindowConverter,
                                                      GlobalWindowConverter)
from pyflink.fn_execution.embedded.state_impl import KeyedStateBackend


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


class TwoInputOperation(operations.TwoInputOperation):

    def __init__(self, open_func, close_func, process_element_func1, process_element_func2,
                 on_timer_func=None):
        self._open_func = open_func
        self._close_func = close_func
        self._process_element_func1 = process_element_func1
        self._process_element_func2 = process_element_func2
        self._on_timer_func = on_timer_func

    def open(self) -> None:
        self._open_func()

    def close(self) -> None:
        self._close_func()

    def process_element1(self, value):
        return self._process_element_func1(value)

    def process_element2(self, value):
        return self._process_element_func2(value)

    def on_timer(self, timestamp):
        if self._on_timer_func:
            return self._on_timer_func(timestamp)


def extract_process_function(
        user_defined_function_proto, j_runtime_context, j_function_context, j_timer_context,
        j_side_output_context, job_parameters, j_keyed_state_backend, j_operator_state_backend):
    from pyflink.fn_execution import flink_fn_execution_pb2
    UserDefinedDataStreamFunction = flink_fn_execution_pb2.UserDefinedDataStreamFunction

    user_defined_func = pickle.loads(user_defined_function_proto.payload)
    func_type = user_defined_function_proto.function_type

    runtime_context = StreamingRuntimeContext.of(j_runtime_context, job_parameters)

    if j_side_output_context:
        side_output_context = SideOutputContext(j_side_output_context)

        def process_func(values):
            if values is None:
                return
            for value in values:
                if isinstance(value, tuple) and isinstance(value[0], OutputTag):
                    output_tag = value[0]  # type: OutputTag
                    side_output_context.collect(output_tag.tag_id, value[1])
                else:
                    yield value
    else:
        def process_func(values):
            if values is None:
                return
            yield from values

    def open_func():
        if hasattr(user_defined_func, "open"):
            user_defined_func.open(runtime_context)

    def close_func():
        if hasattr(user_defined_func, "close"):
            user_defined_func.close()

    if func_type == UserDefinedDataStreamFunction.PROCESS:
        function_context = InternalProcessFunctionContext(j_function_context)
        process_element = user_defined_func.process_element

        def process_element_func(value):
            yield from process_func(process_element(value, function_context))

        return OneInputOperation(open_func, close_func, process_element_func)

    elif func_type == UserDefinedDataStreamFunction.KEYED_PROCESS:
        function_context = InternalKeyedProcessFunctionContext(
            j_function_context, user_defined_function_proto.key_type_info)
        timer_context = InternalKeyedProcessFunctionOnTimerContext(
            j_timer_context, user_defined_function_proto.key_type_info)

        keyed_state_backend = KeyedStateBackend(
            function_context,
            j_keyed_state_backend)
        runtime_context.set_keyed_state_backend(keyed_state_backend)

        process_element = user_defined_func.process_element
        on_timer = user_defined_func.on_timer

        def process_element_func(value):
            yield from process_func(process_element(value[1], function_context))

        def on_timer_func(timestamp):
            yield from process_func(on_timer(timestamp, timer_context))

        return OneInputOperation(open_func, close_func, process_element_func, on_timer_func)

    elif func_type == UserDefinedDataStreamFunction.CO_PROCESS:
        function_context = InternalProcessFunctionContext(j_function_context)

        process_element1 = user_defined_func.process_element1
        process_element2 = user_defined_func.process_element2

        def process_element_func1(value):
            yield from process_func(process_element1(value, function_context))

        def process_element_func2(value):
            yield from process_func(process_element2(value, function_context))

        return TwoInputOperation(
            open_func, close_func, process_element_func1, process_element_func2)

    elif func_type == UserDefinedDataStreamFunction.CO_BROADCAST_PROCESS:
        broadcast_ctx = InternalBroadcastProcessFunctionContext(
            j_function_context, j_operator_state_backend)
        read_only_broadcast_ctx = InternalBroadcastProcessFunctionReadOnlyContext(
            j_function_context, j_operator_state_backend)

        process_element = user_defined_func.process_element
        process_broadcast_element = user_defined_func.process_broadcast_element

        def process_element_func1(value):
            yield from process_func(process_element(value, read_only_broadcast_ctx))

        def process_element_func2(value):
            yield from process_func(process_broadcast_element(value, broadcast_ctx))

        return TwoInputOperation(
            open_func, close_func, process_element_func1, process_element_func2)

    elif func_type == UserDefinedDataStreamFunction.KEYED_CO_PROCESS:
        function_context = InternalKeyedProcessFunctionContext(
            j_function_context, user_defined_function_proto.key_type_info)
        timer_context = InternalKeyedProcessFunctionOnTimerContext(
            j_timer_context, user_defined_function_proto.key_type_info)

        keyed_state_backend = KeyedStateBackend(
            function_context,
            j_keyed_state_backend)
        runtime_context.set_keyed_state_backend(keyed_state_backend)

        process_element1 = user_defined_func.process_element1
        process_element2 = user_defined_func.process_element2
        on_timer = user_defined_func.on_timer

        def process_element_func1(value):
            yield from process_func(process_element1(value[1], function_context))

        def process_element_func2(value):
            yield from process_func(process_element2(value[1], function_context))

        def on_timer_func(timestamp):
            yield from process_func(on_timer(timestamp, timer_context))

        return TwoInputOperation(
            open_func, close_func, process_element_func1, process_element_func2, on_timer_func)

    elif func_type == UserDefinedDataStreamFunction.KEYED_CO_BROADCAST_PROCESS:
        broadcast_ctx = InternalKeyedBroadcastProcessFunctionContext(
            j_function_context, j_operator_state_backend)
        read_only_broadcast_ctx = InternalKeyedBroadcastProcessFunctionReadOnlyContext(
            j_function_context, user_defined_function_proto.key_type_info, j_operator_state_backend)
        timer_context = InternalKeyedBroadcastProcessFunctionOnTimerContext(
            j_timer_context, user_defined_function_proto.key_type_info, j_operator_state_backend)

        keyed_state_backend = KeyedStateBackend(
            read_only_broadcast_ctx,
            j_keyed_state_backend)
        runtime_context.set_keyed_state_backend(keyed_state_backend)

        process_element = user_defined_func.process_element
        process_broadcast_element = user_defined_func.process_broadcast_element
        on_timer = user_defined_func.on_timer

        def process_element_func1(value):
            yield from process_func(process_element(value[1], read_only_broadcast_ctx))

        def process_element_func2(value):
            yield from process_func(process_broadcast_element(value, broadcast_ctx))

        def on_timer_func(timestamp):
            yield from on_timer(timestamp, timer_context)

        return TwoInputOperation(
            open_func, close_func, process_element_func1, process_element_func2, on_timer_func)

    elif func_type == UserDefinedDataStreamFunction.WINDOW:

        window_operation_descriptor = (
            user_defined_func
        )  # type: WindowOperationDescriptor

        def user_key_selector(normal_data):
            return normal_data

        window_assigner = window_operation_descriptor.assigner
        window_trigger = window_operation_descriptor.trigger
        allowed_lateness = window_operation_descriptor.allowed_lateness
        late_data_output_tag = window_operation_descriptor.late_data_output_tag
        window_state_descriptor = window_operation_descriptor.window_state_descriptor
        internal_window_function = window_operation_descriptor.internal_window_function
        window_serializer = window_operation_descriptor.window_serializer
        window_coder = window_serializer._get_coder()

        if isinstance(window_coder, TimeWindowCoder):
            window_converter = TimeWindowConverter()
        elif isinstance(window_coder, CountWindowCoder):
            window_converter = CountWindowConverter()
        else:
            window_converter = GlobalWindowConverter()

        internal_timer_service = InternalTimerServiceImpl(
            j_timer_context.timerService(), window_converter)

        function_context = InternalKeyedProcessFunctionContext(
            j_function_context,
            user_defined_function_proto.key_type_info)
        window_timer_context = InternalWindowTimerContext(
            j_timer_context,
            user_defined_function_proto.key_type_info,
            window_converter)

        keyed_state_backend = KeyedStateBackend(
            function_context,
            j_keyed_state_backend,
            j_function_context.getWindowSerializer(),
            window_converter)
        runtime_context.set_keyed_state_backend(keyed_state_backend)

        window_operator = WindowOperator(
            window_assigner,
            keyed_state_backend,
            user_key_selector,
            window_state_descriptor,
            internal_window_function,
            window_trigger,
            allowed_lateness,
            late_data_output_tag)

        def open_func():
            window_operator.open(runtime_context, internal_timer_service)

        def close_func():
            window_operator.close()

        def process_element_func(value):
            yield from process_func(
                window_operator.process_element(value[1], function_context.timestamp()))

        if window_assigner.is_event_time():
            def on_timer_func(timestamp):
                window = window_timer_context.window()
                key = window_timer_context.get_current_key()
                yield from process_func(window_operator.on_event_time(timestamp, key, window))
        else:
            def on_timer_func(timestamp):
                window = window_timer_context.window()
                key = window_timer_context.get_current_key()
                yield from process_func(window_operator.on_processing_time(timestamp, key, window))

        return OneInputOperation(open_func, close_func, process_element_func, on_timer_func)

    else:
        raise Exception("Unknown function type {0}.".format(func_type))
