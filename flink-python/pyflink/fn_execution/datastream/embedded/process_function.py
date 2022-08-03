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
from pyflink.datastream import (ProcessFunction, KeyedProcessFunction, CoProcessFunction,
                                KeyedCoProcessFunction, TimerService, TimeDomain)
from pyflink.fn_execution.datastream.embedded.timerservice_impl import TimerServiceImpl
from pyflink.fn_execution.embedded.converters import from_type_info


class InternalProcessFunctionContext(ProcessFunction.Context, CoProcessFunction.Context,
                                     TimerService):
    def __init__(self, context):
        self._context = context

    def timer_service(self) -> TimerService:
        return self

    def timestamp(self) -> int:
        return self._context.timestamp()

    def current_processing_time(self):
        return self._context.currentProcessingTime()

    def current_watermark(self):
        return self._context.currentWatermark()

    def register_processing_time_timer(self, timestamp: int):
        raise Exception("Register timers is only supported on a keyed stream.")

    def register_event_time_timer(self, timestamp: int):
        raise Exception("Register timers is only supported on a keyed stream.")

    def delete_processing_time_timer(self, t: int):
        raise Exception("Deleting timers is only supported on a keyed streams.")

    def delete_event_time_timer(self, t: int):
        raise Exception("Deleting timers is only supported on a keyed streams.")


class InternalKeyedProcessFunctionContext(KeyedProcessFunction.Context,
                                          KeyedCoProcessFunction.Context):

    def __init__(self, context, key_type_info):
        self._context = context
        self._timer_service = TimerServiceImpl(self._context.timerService())
        self._key_converter = from_type_info(key_type_info)

    def get_current_key(self):
        return self._key_converter.to_internal(self._context.getCurrentKey())

    def timer_service(self) -> TimerService:
        return self._timer_service

    def timestamp(self) -> int:
        return self._context.timestamp()


class InternalKeyedProcessFunctionOnTimerContext(KeyedProcessFunction.OnTimerContext,
                                                 KeyedProcessFunction.Context,
                                                 KeyedCoProcessFunction.OnTimerContext,
                                                 KeyedCoProcessFunction.Context):

    def __init__(self, context, key_type_info):
        self._context = context
        self._timer_service = TimerServiceImpl(self._context.timerService())
        self._key_converter = from_type_info(key_type_info)

    def timer_service(self) -> TimerService:
        return self._timer_service

    def timestamp(self) -> int:
        return self._context.timestamp()

    def time_domain(self) -> TimeDomain:
        return TimeDomain(self._context.timeDomain())

    def get_current_key(self):
        return self._key_converter.to_internal(self._context.getCurrentKey())


class InternalWindowTimerContext(object):
    def __init__(self, context, key_type_info, window_converter):
        self._context = context
        self._key_converter = from_type_info(key_type_info)
        self._window_converter = window_converter

    def timestamp(self) -> int:
        return self._context.timestamp()

    def window(self):
        return self._window_converter.to_internal(self._context.getWindow())

    def get_current_key(self):
        return self._key_converter.to_internal(self._context.getCurrentKey())
