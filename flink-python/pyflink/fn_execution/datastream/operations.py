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

from apache_beam.runners.worker.bundle_processor import TimerInfo

from pyflink.common import Row
from pyflink.common.serializer import VoidNamespaceSerializer
from pyflink.datastream import TimeDomain, RuntimeContext
from pyflink.fn_execution import flink_fn_execution_pb2
from pyflink.fn_execution import pickle
from pyflink.fn_execution.datastream.process_function import \
    InternalKeyedProcessFunctionOnTimerContext, InternalKeyedProcessFunctionContext, \
    InternalProcessFunctionContext
from pyflink.fn_execution.datastream.runtime_context import StreamingRuntimeContext
from pyflink.fn_execution.datastream.window.window_operator import WindowOperator
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend
from pyflink.fn_execution.datastream.timerservice_impl import (
    TimerServiceImpl, InternalTimerServiceImpl, NonKeyedTimerServiceImpl)
from pyflink.fn_execution.datastream.input_handler import (RunnerInputHandler, TimerHandler,
                                                           _emit_results)
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
        self.open_func, self.close_func, self.process_element_func = \
            extract_stateless_function(
                user_defined_function_proto=self.spec.serialized_fn,
                runtime_context=StreamingRuntimeContext.of(
                    self.spec.serialized_fn.runtime_context,
                    self.base_metric_group))

    def open(self):
        self.open_func()

    def close(self):
        self.close_func()

    def process_element(self, value):
        return self.process_element_func(value)


class StatefulOperation(Operation):

    def __init__(self, spec, keyed_state_backend):
        super(StatefulOperation, self).__init__(spec)
        self.keyed_state_backend = keyed_state_backend
        self.open_func, self.close_func, self.process_element_func, self.process_timer_func, \
            self.internal_timer_service = \
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

    def open(self):
        self.open_func()

    def close(self):
        self.close_func()

    def process_element(self, value):
        return self.process_element_func(value)

    def process_timer(self, timer_data):
        return self.process_timer_func(timer_data)

    def add_timer_info(self, timer_info: TimerInfo):
        self.internal_timer_service.add_timer_info(timer_info)


def extract_stateless_function(user_defined_function_proto, runtime_context: RuntimeContext):
    """
    Extracts user-defined-function from the proto representation of a
    :class:`Function`.

    :param user_defined_function_proto: the proto representation of the Python :class:`Function`
    :param runtime_context: the streaming runtime context
    """
    func_type = user_defined_function_proto.function_type
    UserDefinedDataStreamFunction = flink_fn_execution_pb2.UserDefinedDataStreamFunction

    if func_type == UserDefinedDataStreamFunction.REVISE_OUTPUT:
        def open_func():
            pass

        def close_func():
            pass

        def revise_output(value):
            # VALUE[CURRENT_TIMESTAMP, CURRENT_WATERMARK, NORMAL_DATA]
            timestamp = value[0]
            element = value[2]
            yield Row(timestamp, element)

        process_element_func = revise_output

    else:
        user_defined_func = pickle.loads(user_defined_function_proto.payload)

        def open_func():
            if hasattr(user_defined_func, "open"):
                user_defined_func.open(runtime_context)

        def close_func():
            if hasattr(user_defined_func, "close"):
                user_defined_func.close()

        if func_type == UserDefinedDataStreamFunction.PROCESS:
            process_element = user_defined_func.process_element
            ctx = InternalProcessFunctionContext(NonKeyedTimerServiceImpl())

            def wrapped_func(value):
                # VALUE[CURRENT_TIMESTAMP, CURRENT_WATERMARK, NORMAL_DATA]
                timestamp = value[0]
                watermark = value[1]
                ctx.set_timestamp(timestamp)
                ctx.timer_service().advance_watermark(watermark)
                results = process_element(value[2], ctx)
                yield from _emit_results(timestamp, watermark, results)

            process_element_func = wrapped_func

        elif func_type == UserDefinedDataStreamFunction.CO_PROCESS:
            process_element1 = user_defined_func.process_element1
            process_element2 = user_defined_func.process_element2
            ctx = InternalProcessFunctionContext(NonKeyedTimerServiceImpl())

            def wrapped_func(value):
                # VALUE[CURRENT_TIMESTAMP, CURRENT_WATERMARK, [isLeft, leftInput, rightInput]]
                timestamp = value[0]
                watermark = value[1]
                ctx.set_timestamp(timestamp)
                ctx.timer_service().advance_watermark(watermark)

                normal_data = value[2]
                if normal_data[0]:
                    results = process_element1(normal_data[1], ctx)
                else:
                    results = process_element2(normal_data[2], ctx)

                yield from _emit_results(timestamp, watermark, results)

            process_element_func = wrapped_func

        else:
            raise Exception("Unsupported function_type: " + str(func_type))

    return open_func, close_func, process_element_func


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
        process_function = user_defined_func
        internal_timer_service.set_namespace_serializer(VoidNamespaceSerializer())

        def open_func():
            if hasattr(process_function, "open"):
                process_function.open(runtime_context)

        def close_func():
            if hasattr(process_function, "close"):
                process_function.close()

        def on_event_time(timestamp: int, key, namespace):
            keyed_state_backend.set_current_key(key)
            return _on_timer(TimeDomain.EVENT_TIME, timestamp, key)

        def on_processing_time(timestamp: int, key, namespace):
            keyed_state_backend.set_current_key(key)
            return _on_timer(TimeDomain.PROCESSING_TIME, timestamp, key)

        def _on_timer(time_domain: TimeDomain, timestamp: int, key):
            user_current_key = user_key_selector(key)

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

                ctx.set_timestamp(timestamp)
                on_timer_ctx.set_current_key(user_key_selector(user_input))
                keyed_state_backend.set_current_key(state_key_selector(user_input))

                if is_left:
                    return process_function.process_element1(input_selector(user_input), ctx)
                else:
                    return process_function.process_element2(input_selector(user_input), ctx)

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
        internal_timer_service.set_namespace_serializer(window_serializer)

        def open_func():
            window_operator.open(runtime_context, internal_timer_service)

        def close_func():
            window_operator.close()

        def process_element(normal_data, timestamp: int):
            keyed_state_backend.set_current_key(state_key_selector(normal_data))
            return window_operator.process_element(input_selector(normal_data), timestamp)

        def on_event_time(timestamp: int, key, namespace):
            keyed_state_backend.set_current_key(key)
            return window_operator.on_event_time(timestamp, key, namespace)

        def on_processing_time(timestamp: int, key, namespace):
            keyed_state_backend.set_current_key(key)
            return window_operator.on_processing_time(timestamp, key, namespace)

    else:
        raise Exception("Unsupported function_type: " + str(func_type))

    input_handler = RunnerInputHandler(
        internal_timer_service,
        process_element)
    process_element_func = input_handler.process_element

    timer_handler = TimerHandler(
        internal_timer_service,
        on_event_time,
        on_processing_time,
        keyed_state_backend._namespace_coder_impl)
    process_timer_func = timer_handler.process_timer

    return open_func, close_func, process_element_func, process_timer_func, internal_timer_service
