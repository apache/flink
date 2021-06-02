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
import abc
from enum import Enum

from pyflink.common import Row
from pyflink.common.serializer import VoidNamespaceSerializer
from pyflink.datastream import TimeDomain, RuntimeContext
from pyflink.fn_execution import flink_fn_execution_pb2
from pyflink.fn_execution import pickle
from pyflink.fn_execution.datastream.process_function import \
    InternalKeyedProcessFunctionOnTimerContext, InternalKeyedProcessFunctionContext, \
    InternalProcessFunctionContext
from pyflink.fn_execution.datastream.runtime_context import StreamingRuntimeContext
from pyflink.fn_execution.datastream.timerservice import InternalTimer
from pyflink.fn_execution.datastream.window.window_operator import WindowOperator
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend
from pyflink.fn_execution.datastream.timerservice_impl import (
    InternalTimerImpl, TimerServiceImpl, InternalTimerServiceImpl, NonKeyedTimerServiceImpl)
from pyflink.fn_execution.datastream.input_handler import InputHandler
from pyflink.fn_execution.datastream.output_handler import OutputHandler
from pyflink.metrics.metricbase import GenericMetricGroup


DATA_STREAM_STATELESS_FUNCTION_URN = "flink:transform:ds:stateless_function:v1"
DATA_STREAM_STATEFUL_FUNCTION_URN = "flink:transform:ds:stateful_function:v1"


class Operation(abc.ABC):

    def __init__(self, spec):
        self.spec = spec
        if self.spec.serialized_fn.metric_enabled:
            self.base_metric_group = GenericMetricGroup(None, None)
        else:
            self.base_metric_group = None

    def finish(self):
        self._update_gauge(self.base_metric_group)

    def _update_gauge(self, base_metric_group):
        if base_metric_group is not None:
            for name in base_metric_group._flink_gauge:
                flink_gauge = base_metric_group._flink_gauge[name]
                beam_gauge = base_metric_group._beam_gauge[name]
                beam_gauge.set(flink_gauge())
            for sub_group in base_metric_group._sub_groups:
                self._update_gauge(sub_group)

    def process_element(self, value):
        raise NotImplementedError

    def open(self) -> None:
        pass

    def close(self) -> None:
        pass


class StatelessOperation(Operation):

    def __init__(self, spec):
        super(StatelessOperation, self).__init__(spec)
        self.process_element_func, self.open_func, self.close_func = \
            extract_stateless_function(
                user_defined_function_proto=self.spec.serialized_fn,
                runtime_context=StreamingRuntimeContext.of(
                    self.spec.serialized_fn.runtime_context,
                    self.base_metric_group))

    def process_element(self, value):
        return self.process_element_func(value)

    def open(self):
        self.open_func()

    def close(self):
        self.close_func()


class StatefulOperation(Operation):

    def __init__(self, spec, keyed_state_backend):
        super(StatefulOperation, self).__init__(spec)
        self.keyed_state_backend = keyed_state_backend
        self.process_element_func, self.open_func, self.close_func = \
            extract_stateful_function(
                user_defined_function_proto=self.spec.serialized_fn,
                runtime_context=StreamingRuntimeContext.of(
                    self.spec.serialized_fn.runtime_context,
                    self.base_metric_group,
                    self.keyed_state_backend),
                keyed_state_backend=self.keyed_state_backend)

    def finish(self):
        super().finish()
        self.keyed_state_backend.commit()

    def process_element(self, value):
        return self.process_element_func(value)

    def open(self):
        self.open_func()

    def close(self):
        self.close_func()


def extract_stateless_function(user_defined_function_proto, runtime_context: RuntimeContext):
    """
    Extracts user-defined-function from the proto representation of a
    :class:`Function`.

    :param user_defined_function_proto: the proto representation of the Python :class:`Function`
    :param runtime_context: the streaming runtime context
    """
    func_type = user_defined_function_proto.function_type
    user_defined_func = pickle.loads(user_defined_function_proto.payload)
    process_element_func = None

    UserDefinedDataStreamFunction = flink_fn_execution_pb2.UserDefinedDataStreamFunction
    if func_type == UserDefinedDataStreamFunction.MAP:
        process_element_func = user_defined_func.map

    elif func_type == UserDefinedDataStreamFunction.FLAT_MAP:
        process_element_func = user_defined_func.flat_map

    elif func_type == UserDefinedDataStreamFunction.CO_MAP:
        map1 = user_defined_func.map1
        map2 = user_defined_func.map2

        def wrapped_func(value):
            # value in format of: [INPUT_FLAG, REAL_VALUE]
            # INPUT_FLAG value of True for the left stream, while False for the right stream
            return map1(value[1]) if value[0] else map2(value[2])

        process_element_func = wrapped_func

    elif func_type == UserDefinedDataStreamFunction.CO_FLAT_MAP:
        flat_map1 = user_defined_func.flat_map1
        flat_map2 = user_defined_func.flat_map2

        def wrapped_func(value):
            if value[0]:
                yield from flat_map1(value[1])
            else:
                yield from flat_map2(value[2])

        process_element_func = wrapped_func

    elif func_type == UserDefinedDataStreamFunction.TIMESTAMP_ASSIGNER:
        extract_timestamp = user_defined_func.extract_timestamp

        def wrapped_func(value):
            pre_timestamp = value[0]
            real_data = value[1]
            return extract_timestamp(real_data, pre_timestamp)

        process_element_func = wrapped_func

    elif func_type == UserDefinedDataStreamFunction.PROCESS:
        process_element = user_defined_func.process_element
        ctx = InternalProcessFunctionContext(NonKeyedTimerServiceImpl())

        def wrapped_func(value):
            # VALUE[CURRENT_TIMESTAMP, CURRENT_WATERMARK, NORMAL_DATA]
            ctx.set_timestamp(value[0])
            ctx.timer_service().advance_watermark(value[1])
            output_result = process_element(value[2], ctx)
            return output_result

        process_element_func = wrapped_func

    def open_func():
        if hasattr(user_defined_func, "open"):
            user_defined_func.open(runtime_context)

    def close_func():
        if hasattr(user_defined_func, "close"):
            user_defined_func.close()

    return process_element_func, open_func, close_func


def extract_stateful_function(user_defined_function_proto,
                              runtime_context: RuntimeContext,
                              keyed_state_backend: RemoteKeyedStateBackend):
    func_type = user_defined_function_proto.function_type
    user_defined_func = pickle.loads(user_defined_function_proto.payload)
    internal_timer_service = InternalTimerServiceImpl(keyed_state_backend)

    def state_key_selector(normal_data):
        return Row(normal_data[0])

    def user_key_selector(normal_data):
        return normal_data[0]

    def input_selector(normal_data):
        return normal_data[1]

    UserDefinedDataStreamFunction = flink_fn_execution_pb2.UserDefinedDataStreamFunction
    if func_type in (UserDefinedDataStreamFunction.KEYED_PROCESS,
                     UserDefinedDataStreamFunction.KEYED_CO_PROCESS):
        timer_service = TimerServiceImpl(internal_timer_service)
        ctx = InternalKeyedProcessFunctionContext(timer_service)
        on_timer_ctx = InternalKeyedProcessFunctionOnTimerContext(timer_service)
        output_handler = OutputHandler(VoidNamespaceSerializer())
        process_function = user_defined_func

        def open_func():
            if hasattr(process_function, "open"):
                process_function.open(runtime_context)

        def close_func():
            if hasattr(process_function, "close"):
                process_function.close()

        def on_event_time(internal_timer: InternalTimerImpl):
            keyed_state_backend.set_current_key(internal_timer.get_key())
            return on_timer(TimeDomain.EVENT_TIME, internal_timer)

        def on_processing_time(internal_timer: InternalTimerImpl):
            keyed_state_backend.set_current_key(internal_timer.get_key())
            return on_timer(TimeDomain.PROCESSING_TIME, internal_timer)

        def on_timer(time_domain: TimeDomain, internal_timer: InternalTimer):
            timestamp = internal_timer.get_timestamp()
            state_current_key = internal_timer.get_key()
            user_current_key = user_key_selector(state_current_key)

            on_timer_ctx.set_timestamp(timestamp)
            on_timer_ctx.set_current_key(user_current_key)
            on_timer_ctx.set_time_domain(time_domain)

            return process_function.on_timer(timestamp, on_timer_ctx)

        if func_type == UserDefinedDataStreamFunction.KEYED_PROCESS:

            def process_element(normal_data, timestamp: int):
                ctx.set_timestamp(timestamp)
                ctx.set_current_key(user_key_selector(normal_data))
                keyed_state_backend.set_current_key(state_key_selector(normal_data))
                return process_function.process_element(input_selector(normal_data), ctx)

        elif func_type == UserDefinedDataStreamFunction.KEYED_CO_PROCESS:

            def process_element(normal_data, timestamp: int):
                is_left = normal_data[0]
                if is_left:
                    user_input = normal_data[1]
                else:
                    user_input = normal_data[2]
                user_current_key = user_input[0]
                user_element = user_input[1]
                state_current_key = Row(user_input[0])

                ctx.set_timestamp(timestamp)
                on_timer_ctx.set_current_key(user_current_key)
                keyed_state_backend.set_current_key(state_current_key)

                if is_left:
                    return process_function.process_element1(user_element, ctx)
                else:
                    return process_function.process_element2(user_element, ctx)

        else:
            raise Exception("Unsupported func_type: " + str(func_type))

    elif func_type == UserDefinedDataStreamFunction.WINDOW:
        window_operation_descriptor = user_defined_func
        window_assigner = window_operation_descriptor.assigner
        window_trigger = window_operation_descriptor.trigger
        allowed_lateness = window_operation_descriptor.allowed_lateness
        window_state_descriptor = window_operation_descriptor.window_state_descriptor
        internal_window_function = window_operation_descriptor.internal_window_function
        window_serializer = window_operation_descriptor.window_serializer
        window_coder = window_serializer._get_coder()
        keyed_state_backend.namespace_coder = window_coder
        keyed_state_backend._namespace_coder_impl = window_coder.get_impl()
        window_operator = WindowOperator(
            window_assigner,
            keyed_state_backend,
            user_key_selector,
            window_state_descriptor,
            internal_window_function,
            window_trigger,
            allowed_lateness)
        output_handler = OutputHandler(window_serializer)

        def open_func():
            window_operator.open(runtime_context, internal_timer_service)

        def close_func():
            window_operator.close()

        def process_element(normal_data, timestamp: int):
            keyed_state_backend.set_current_key(state_key_selector(normal_data))
            return window_operator.process_element(input_selector(normal_data), timestamp)

        def on_event_time(internal_timer: InternalTimerImpl):
            keyed_state_backend.set_current_key(internal_timer.get_key())
            return window_operator.on_event_time(internal_timer)

        def on_processing_time(internal_timer: InternalTimerImpl):
            keyed_state_backend.set_current_key(internal_timer.get_key())
            return window_operator.on_processing_time(internal_timer)

    else:
        raise Exception("Unsupported func_type: " + str(func_type))

    input_handler = InputHandler(
        internal_timer_service,
        output_handler,
        process_element,
        on_event_time,
        on_processing_time,
        keyed_state_backend._namespace_coder_impl)

    process_element_func = input_handler.accept

    return process_element_func, open_func, close_func


"""
All these Enum Classes MUST be in sync with
org.apache.flink.streaming.api.utils.PythonOperatorUtils if there are any changes.
"""


class KeyedProcessFunctionInputFlag(Enum):
    EVENT_TIME_TIMER = 0
    PROC_TIME_TIMER = 1
    NORMAL_DATA = 2
