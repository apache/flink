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


class RunnerInputType(Enum):
    NORMAL_RECORD = 0
    TRIGGER_TIMER = 1


class TimerType(Enum):
    EVENT_TIME = 0
    PROCESSING_TIME = 1


class TimerRowInputHandler(ABC):

    @abstractmethod
    def advance_watermark(self, watermark) -> None:
        pass

    @abstractmethod
    def on_normal_record(self, normal_data, timestamp: int) -> Iterable:
        pass

    @abstractmethod
    def on_event_time(
            self, key, timestamp: int, serialized_namespace: bytes) -> Iterable:
        pass

    @abstractmethod
    def on_processing_time(
            self, key, timestamp: int, serialized_namespace: bytes) -> Iterable:
        pass

    def process_element(self, operation_input):
        input_type = operation_input[0]
        normal_data = operation_input[1]
        timestamp = operation_input[2]
        watermark = operation_input[3]
        timer_data = operation_input[4]

        self.advance_watermark(watermark)
        if input_type == RunnerInputType.NORMAL_RECORD.value:
            yield from self.on_normal_record(normal_data, timestamp)
        elif input_type == RunnerInputType.TRIGGER_TIMER.value:
            timer_type = timer_data[0]
            key = timer_data[1]
            serialized_namespace = timer_data[2]
            if timer_type == TimerType.EVENT_TIME.value:
                yield from self.on_event_time(key, timestamp, serialized_namespace)
            elif timer_type == TimerType.PROCESSING_TIME.value:
                yield from self.on_processing_time(key, timestamp, serialized_namespace)
            else:
                raise Exception("Unsupported timer type: %d" % timer_type)
        else:
            raise Exception("Unsupported input type: %d" % input_type)


class KeyedTwoInputTimerRowHandler(TimerRowInputHandler):

    def __init__(self,
                 context,
                 timer_context,
                 internal_collector,
                 state_backend,
                 keyed_co_process_function):
        """
        :type context: InternalKeyedProcessFunctionContext
        :type timer_context: InternalKeyedProcessFunctionOnTimerContext
        :type internal_collector: InternalCollector
        :type state_backend: RemoteKeyedStateBackend
        :type keyed_co_process_function: KeyedCoProcessFunction
        """
        super(KeyedTwoInputTimerRowHandler, self).__init__()
        self._ctx = context
        self._on_timer_ctx = timer_context
        self._collector = internal_collector
        self._keyed_state_backend = state_backend
        self._process_function = keyed_co_process_function

    def advance_watermark(self, watermark) -> None:
        self._on_timer_ctx.timer_service().set_current_watermark(watermark)

    def on_normal_record(self, unified_input, timestamp: int) -> Iterable:
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

    def on_event_time(
            self, key, timestamp: int, serialized_namespace: bytes) -> Iterable:
        yield from self._on_timer(TimeDomain.EVENT_TIME, key, timestamp)

    def on_processing_time(
            self, key, timestamp: int, serialized_namespace: bytes) -> Iterable:
        yield from self._on_timer(TimeDomain.PROCESSING_TIME, key, timestamp)

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
        for result in output_result:
            yield Row(None, None, None, result)

        for result in self._collector.buf:
            # 0: proc time timer data
            # 1: event time timer data
            # 2: normal data
            # result_row: [TIMER_FLAG, TIMER TYPE, TIMER_KEY, RESULT_DATA]
            yield Row(result[0], result[1], result[2], None)

        self._collector.clear()
