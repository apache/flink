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
from abc import ABC, abstractmethod
from collections import Iterable
from enum import Enum

from pyflink.common import Row
from pyflink.datastream import TimeDomain
from pyflink.datastream.timerservice import InternalTimer
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend
from pyflink.fn_execution.timerservice_impl import InternalTimerImpl, InternalTimerServiceImpl
from pyflink.fn_execution.utils.output_factory import RowWithTimerOutputFactory


class RunnerInputType(Enum):
    NORMAL_RECORD = 0
    TRIGGER_TIMER = 1


class TimerType(Enum):
    EVENT_TIME = 0
    PROCESSING_TIME = 1


class RowWithTimerInputHandler(ABC):

    def __init__(self, namespace_coder=None):
        self._namespace_coder = namespace_coder

    @abstractmethod
    def advance_watermark(self, watermark) -> None:
        pass

    @abstractmethod
    def process_element(self, normal_data, timestamp: int) -> Iterable:
        pass

    @abstractmethod
    def on_event_time(
            self, internal_timer: InternalTimer) -> Iterable:
        pass

    @abstractmethod
    def on_processing_time(
            self, internal_timer: InternalTimer) -> Iterable:
        pass

    def accept(self, operation_input):
        input_type = operation_input[0]
        normal_data = operation_input[1]
        timestamp = operation_input[2]
        watermark = operation_input[3]
        timer_data = operation_input[4]

        self.advance_watermark(watermark)
        if input_type == RunnerInputType.NORMAL_RECORD.value:
            yield from self.process_element(normal_data, timestamp)
        elif input_type == RunnerInputType.TRIGGER_TIMER.value:
            timer_type = timer_data[0]
            key = timer_data[1]
            serialized_namespace = timer_data[2]
            if self._namespace_coder is not None:
                namespace = self._namespace_coder.decode_nested(serialized_namespace)
            else:
                namespace = None
            internal_timer = InternalTimerImpl(timestamp, key, namespace)
            if timer_type == TimerType.EVENT_TIME.value:
                yield from self.on_event_time(internal_timer)
            elif timer_type == TimerType.PROCESSING_TIME.value:
                yield from self.on_processing_time(internal_timer)
            else:
                raise Exception("Unsupported timer type: %d" % timer_type)
        else:
            raise Exception("Unsupported input type: %d" % input_type)


class OneInputRowWithTimerHandler(RowWithTimerInputHandler):

    def __init__(self,
                 internal_timer_service: InternalTimerServiceImpl,
                 state_backend: RemoteKeyedStateBackend,
                 key_selector_func,
                 process_element_func,
                 on_event_time_func,
                 on_processing_time_func,
                 output_factory: RowWithTimerOutputFactory):
        super(OneInputRowWithTimerHandler, self).__init__(state_backend._namespace_coder_impl)
        self._internal_timer_service = internal_timer_service
        self._keyed_state_backend = state_backend
        self._key_selector_func = key_selector_func
        self._process_element_func = process_element_func
        self._on_event_time_func = on_event_time_func
        self._on_processing_time_func = on_processing_time_func
        self._output_factory = output_factory

    def advance_watermark(self, watermark) -> None:
        self._internal_timer_service.advance_watermark(watermark)

    def process_element(self, normal_data, timestamp: int) -> Iterable:
        self._keyed_state_backend.set_current_key(self._key_selector_func(normal_data))
        output_result = self._process_element_func(normal_data, timestamp)
        yield from self._emit_output(output_result)

    def on_event_time(
            self, internal_timer: InternalTimer) -> Iterable:
        self._keyed_state_backend.set_current_key(internal_timer.get_key())
        output_result = self._on_event_time_func(internal_timer)
        yield from self._emit_output(output_result)

    def on_processing_time(
            self, internal_timer: InternalTimer) -> Iterable:
        self._keyed_state_backend.set_current_key(internal_timer.get_key())
        output_result = self._on_processing_time_func(internal_timer)
        yield from self._emit_output(output_result)

    def _emit_output(self, output_result):
        if output_result:
            yield from self._output_factory.from_normal_data(output_result)

        yield from self._output_factory.from_timers(self._internal_timer_service.timers.keys())

        self._internal_timer_service.timers.clear()


class TwoInputRowWithTimerHandler(RowWithTimerInputHandler):

    def __init__(self,
                 context,
                 timer_context,
                 timer_service,
                 state_backend,
                 keyed_co_process_function,
                 output_factory):
        """
        :type context: InternalKeyedProcessFunctionContext
        :type timer_context: InternalKeyedProcessFunctionOnTimerContext
        :type timer_service: TimerServiceImpl
        :type state_backend: RemoteKeyedStateBackend
        :type keyed_co_process_function: KeyedCoProcessFunction
        :type output_factory: RowWithTimerOutputFactory
        """
        super(TwoInputRowWithTimerHandler, self).__init__()
        self._ctx = context
        self._on_timer_ctx = timer_context
        self._timer_service = timer_service
        self._keyed_state_backend = state_backend
        self._process_function = keyed_co_process_function
        self._output_factory = output_factory

    def advance_watermark(self, watermark) -> None:
        self._on_timer_ctx.timer_service().advance_watermark(watermark)

    def process_element(self, unified_input, timestamp: int) -> Iterable:
        self._ctx.set_timestamp(timestamp)
        is_left = unified_input[0]
        if is_left:
            user_input = unified_input[1]
        else:
            user_input = unified_input[2]
        user_current_key = user_input[0]
        user_element = user_input[1]

        state_current_key = Row(user_current_key)
        self._on_timer_ctx.set_current_key(user_current_key)
        self._keyed_state_backend.set_current_key(state_current_key)

        if is_left:
            output_result = self._process_function.process_element1(user_element, self._ctx)
        else:
            output_result = self._process_function.process_element2(user_element, self._ctx)

        yield from self._emit_output(output_result)

    def on_event_time(self, internal_timer: InternalTimer) -> Iterable:
        yield from self._on_timer(TimeDomain.EVENT_TIME,
                                  internal_timer.get_key(),
                                  internal_timer.get_timestamp())

    def on_processing_time(self, internal_timer: InternalTimer) -> Iterable:
        yield from self._on_timer(TimeDomain.PROCESSING_TIME,
                                  internal_timer.get_key(),
                                  internal_timer.get_timestamp())

    def _on_timer(self, time_domain, key, timestamp):
        self._on_timer_ctx.set_timestamp(timestamp)
        state_current_key = key
        user_current_key = state_current_key[0]

        self._on_timer_ctx.set_current_key(user_current_key)
        self._keyed_state_backend.set_current_key(state_current_key)

        self._on_timer_ctx.set_time_domain(time_domain)

        output_result = self._process_function.on_timer(timestamp, self._on_timer_ctx)

        yield from self._emit_output(output_result)

    def _emit_output(self, output_result):
        if output_result:
            yield from self._output_factory.from_normal_data(output_result)

        yield from self._output_factory.from_timers(self._timer_service.timers)

        self._timer_service.timers.clear()
