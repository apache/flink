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

from pyflink.datastream import (ProcessFunction, KeyedProcessFunction, CoProcessFunction,
                                KeyedCoProcessFunction, TimerService, TimeDomain)
from pyflink.datastream.functions import (BaseBroadcastProcessFunction, BroadcastProcessFunction,
                                          KeyedBroadcastProcessFunction)
from pyflink.datastream.state import MapStateDescriptor, BroadcastState, ReadOnlyBroadcastState
from pyflink.fn_execution.datastream.embedded.state_impl import (ReadOnlyBroadcastStateImpl,
                                                                 BroadcastStateImpl)
from pyflink.fn_execution.datastream.embedded.timerservice_impl import TimerServiceImpl
from pyflink.fn_execution.embedded.converters import from_type_info_proto, from_type_info
from pyflink.fn_execution.embedded.java_utils import to_java_state_descriptor


class InternalProcessFunctionContext(ProcessFunction.Context, CoProcessFunction.Context,
                                     TimerService):
    def __init__(self, j_context):
        self._j_context = j_context

    def timer_service(self) -> TimerService:
        return self

    def timestamp(self) -> int:
        return self._j_context.timestamp()

    def current_processing_time(self):
        return self._j_context.currentProcessingTime()

    def current_watermark(self):
        return self._j_context.currentWatermark()

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

    def __init__(self, j_context, key_type_info):
        self._j_context = j_context
        self._timer_service = TimerServiceImpl(self._j_context.timerService())
        self._key_converter = from_type_info_proto(key_type_info)

    def get_current_key(self):
        return self._key_converter.to_internal(self._j_context.getCurrentKey())

    def timer_service(self) -> TimerService:
        return self._timer_service

    def timestamp(self) -> int:
        return self._j_context.timestamp()


class InternalKeyedProcessFunctionOnTimerContext(KeyedProcessFunction.OnTimerContext,
                                                 KeyedProcessFunction.Context,
                                                 KeyedCoProcessFunction.OnTimerContext,
                                                 KeyedCoProcessFunction.Context):

    def __init__(self, j_timer_context, key_type_info):
        self._j_timer_context = j_timer_context
        self._timer_service = TimerServiceImpl(self._j_timer_context.timerService())
        self._key_converter = from_type_info_proto(key_type_info)

    def timer_service(self) -> TimerService:
        return self._timer_service

    def timestamp(self) -> int:
        return self._j_timer_context.timestamp()

    def time_domain(self) -> TimeDomain:
        return TimeDomain(self._j_timer_context.timeDomain())

    def get_current_key(self):
        return self._key_converter.to_internal(self._j_timer_context.getCurrentKey())


class InternalWindowTimerContext(object):
    def __init__(self, j_timer_context, key_type_info, window_converter):
        self._j_timer_context = j_timer_context
        self._key_converter = from_type_info_proto(key_type_info)
        self._window_converter = window_converter

    def timestamp(self) -> int:
        return self._j_timer_context.timestamp()

    def window(self):
        return self._window_converter.to_internal(self._j_timer_context.getWindow())

    def get_current_key(self):
        return self._key_converter.to_internal(self._j_timer_context.getCurrentKey())


class InternalBaseBroadcastProcessFunctionContext(BaseBroadcastProcessFunction.Context, ABC):

    def __init__(self, j_context, j_operator_state_backend):
        self._j_context = j_context
        self._j_operator_state_backend = j_operator_state_backend

    def timestamp(self) -> int:
        return self._j_context.timestamp()

    def current_processing_time(self) -> int:
        return self._j_context.currentProcessingTime()

    def current_watermark(self) -> int:
        return self._j_context.currentWatermark()


class InternalBroadcastProcessFunctionContext(InternalBaseBroadcastProcessFunctionContext,
                                              BroadcastProcessFunction.Context):

    def __init__(self, j_context, j_operator_state_backend):
        super(InternalBroadcastProcessFunctionContext, self).__init__(
            j_context, j_operator_state_backend)

    def get_broadcast_state(self, state_descriptor: MapStateDescriptor) -> BroadcastState:
        return BroadcastStateImpl(
            self._j_operator_state_backend.getBroadcastState(
                to_java_state_descriptor(state_descriptor)),
            from_type_info(state_descriptor.type_info))


class InternalBroadcastProcessFunctionReadOnlyContext(InternalBaseBroadcastProcessFunctionContext,
                                                      BroadcastProcessFunction.ReadOnlyContext):

    def __init__(self, j_context, j_operator_state_backend):
        super(InternalBroadcastProcessFunctionReadOnlyContext, self).__init__(
            j_context, j_operator_state_backend)

    def get_broadcast_state(self, state_descriptor: MapStateDescriptor) -> ReadOnlyBroadcastState:
        return ReadOnlyBroadcastStateImpl(
            self._j_operator_state_backend.getBroadcastState(
                to_java_state_descriptor(state_descriptor)),
            from_type_info(state_descriptor.type_info))


class InternalKeyedBroadcastProcessFunctionContext(InternalBaseBroadcastProcessFunctionContext,
                                                   KeyedBroadcastProcessFunction.Context):

    def __init__(self, j_context, j_operator_state_backend):
        super(InternalKeyedBroadcastProcessFunctionContext, self).__init__(
            j_context, j_operator_state_backend)

    def get_broadcast_state(self, state_descriptor: MapStateDescriptor) -> BroadcastState:
        return BroadcastStateImpl(
            self._j_operator_state_backend.getBroadcastState(
                to_java_state_descriptor(state_descriptor)),
            from_type_info(state_descriptor.type_info))


class InternalKeyedBroadcastProcessFunctionReadOnlyContext(
    InternalBaseBroadcastProcessFunctionContext,
    KeyedBroadcastProcessFunction.ReadOnlyContext
):

    def __init__(self, j_context, key_type_info, j_operator_state_backend):
        super(InternalKeyedBroadcastProcessFunctionReadOnlyContext, self).__init__(
            j_context, j_operator_state_backend)
        self._key_converter = from_type_info_proto(key_type_info)
        self._timer_service = TimerServiceImpl(self._j_context.timerService())

    def get_broadcast_state(self, state_descriptor: MapStateDescriptor) -> ReadOnlyBroadcastState:
        return ReadOnlyBroadcastStateImpl(
            self._j_operator_state_backend.getBroadcastState(
                to_java_state_descriptor(state_descriptor)),
            from_type_info(state_descriptor.type_info))

    def timer_service(self) -> TimerService:
        return self._timer_service

    def get_current_key(self):
        return self._key_converter.to_internal(self._j_context.getCurrentKey())


class InternalKeyedBroadcastProcessFunctionOnTimerContext(
    InternalBaseBroadcastProcessFunctionContext,
    KeyedBroadcastProcessFunction.OnTimerContext,
):

    def __init__(self, j_timer_context, key_type_info, j_operator_state_backend):
        super(InternalKeyedBroadcastProcessFunctionOnTimerContext, self).__init__(
            j_timer_context, j_operator_state_backend)
        self._timer_service = TimerServiceImpl(self._j_context.timerService())
        self._key_converter = from_type_info_proto(key_type_info)

    def get_broadcast_state(self, state_descriptor: MapStateDescriptor) -> ReadOnlyBroadcastState:
        return ReadOnlyBroadcastStateImpl(
            self._j_operator_state_backend.getBroadcastState(
                to_java_state_descriptor(state_descriptor)),
            from_type_info(state_descriptor.type_info))

    def current_processing_time(self) -> int:
        return self._timer_service.current_processing_time()

    def current_watermark(self) -> int:
        return self._timer_service.current_watermark()

    def timer_service(self) -> TimerService:
        return self._timer_service

    def timestamp(self) -> int:
        return self._j_context.timestamp()

    def time_domain(self) -> TimeDomain:
        return TimeDomain(self._j_context.timeDomain())

    def get_current_key(self):
        return self._key_converter.to_internal(self._j_context.getCurrentKey())
