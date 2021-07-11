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
from abc import ABC
from collections import Iterable
from enum import Enum

from pyflink.fn_execution.datastream.timerservice import InternalTimer
from pyflink.fn_execution.datastream.timerservice_impl import (
    InternalTimerImpl, InternalTimerServiceImpl)
from pyflink.fn_execution.datastream.output_handler import OutputHandler


class RunnerInputType(Enum):
    NORMAL_RECORD = 0
    TRIGGER_TIMER = 1


class TimerType(Enum):
    EVENT_TIME = 0
    PROCESSING_TIME = 1


class InputHandler(ABC):
    """
    Handler which handles both normal data and timer data.
    """

    def __init__(self,
                 internal_timer_service: InternalTimerServiceImpl,
                 output_handler: OutputHandler,
                 process_element_func,
                 on_event_time_func,
                 on_processing_time_func,
                 namespace_coder):
        self._internal_timer_service = internal_timer_service
        self._output_handler = output_handler
        self._process_element_func = process_element_func
        self._on_event_time_func = on_event_time_func
        self._on_processing_time_func = on_processing_time_func
        self._namespace_coder = namespace_coder

    def process_element(self, normal_data, timestamp: int) -> Iterable:
        result = self._process_element_func(normal_data, timestamp)
        yield from self._emit_result(result)

    def on_event_time(
            self, internal_timer: InternalTimer) -> Iterable:
        result = self._on_event_time_func(internal_timer)
        yield from self._emit_result(result)

    def on_processing_time(
            self, internal_timer: InternalTimer) -> Iterable:
        result = self._on_processing_time_func(internal_timer)
        yield from self._emit_result(result)

    def advance_watermark(self, watermark: int) -> None:
        self._internal_timer_service.advance_watermark(watermark)

    def accept(self, value):
        input_type = value[0]
        normal_data = value[1]
        timestamp = value[2]
        watermark = value[3]
        timer_data = value[4]

        self.advance_watermark(watermark)
        if input_type == RunnerInputType.NORMAL_RECORD.value:
            yield from self.process_element(normal_data, timestamp)
        elif input_type == RunnerInputType.TRIGGER_TIMER.value:
            timer_type = timer_data[0]
            key = timer_data[1]
            serialized_namespace = timer_data[2]
            if self._namespace_coder is not None:
                namespace = self._namespace_coder.decode(serialized_namespace)
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

    def _emit_result(self, result):
        if result:
            yield from self._output_handler.from_normal_data(result)

        yield from self._output_handler.from_timers(self._internal_timer_service.timers)

        self._internal_timer_service.timers.clear()
