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
from typing import cast

from pyflink.datastream import TimerService, TimeDomain
from pyflink.datastream.functions import KeyedProcessFunction, KeyedCoProcessFunction, \
    ProcessFunction, CoProcessFunction, BroadcastProcessFunction, KeyedBroadcastProcessFunction, \
    BaseBroadcastProcessFunction
from pyflink.datastream.state import MapStateDescriptor, BroadcastState, ReadOnlyBroadcastState, \
    OperatorStateStore
from pyflink.fn_execution.internal_state import InternalBroadcastState


class InternalKeyedProcessFunctionOnTimerContext(
        KeyedProcessFunction.OnTimerContext, KeyedCoProcessFunction.OnTimerContext):
    """
    Internal implementation of OnTimerContext of KeyedProcessFunction and KeyedCoProcessFunction.
    """

    def __init__(self, timer_service: TimerService):
        self._timer_service = timer_service
        self._time_domain = None
        self._timestamp = None
        self._current_key = None

    def get_current_key(self):
        return self._current_key

    def set_current_key(self, current_key):
        self._current_key = current_key

    def timer_service(self) -> TimerService:
        return self._timer_service

    def timestamp(self) -> int:
        return self._timestamp

    def set_timestamp(self, ts: int):
        self._timestamp = ts

    def time_domain(self) -> TimeDomain:
        return self._time_domain

    def set_time_domain(self, td: TimeDomain):
        self._time_domain = td


class InternalKeyedProcessFunctionContext(
        KeyedProcessFunction.Context, KeyedCoProcessFunction.Context):
    """
    Internal implementation of Context of KeyedProcessFunction and KeyedCoProcessFunction.
    """

    def __init__(self, timer_service: TimerService):
        self._timer_service = timer_service
        self._timestamp = None
        self._current_key = None

    def get_current_key(self):
        return self._current_key

    def set_current_key(self, current_key):
        self._current_key = current_key

    def timer_service(self) -> TimerService:
        return self._timer_service

    def timestamp(self) -> int:
        return self._timestamp

    def set_timestamp(self, ts: int):
        self._timestamp = ts


class InternalProcessFunctionContext(ProcessFunction.Context, CoProcessFunction.Context):
    """
    Internal implementation of ProcessFunction.Context and CoProcessFunction.Context.
    """

    def __init__(self, timer_service: TimerService):
        self._timer_service = timer_service
        self._timestamp = None

    def timer_service(self) -> TimerService:
        return self._timer_service

    def timestamp(self) -> int:
        return self._timestamp

    def set_timestamp(self, ts: int):
        self._timestamp = ts


class InternalBaseBroadcastProcessFunctionContext(BaseBroadcastProcessFunction.Context, ABC):

    def __init__(self, timer_service: TimerService, operator_state_store: OperatorStateStore):
        self._timer_service = timer_service
        self._timestamp = None
        self._operator_state_store = operator_state_store

    def timer_service(self) -> TimerService:
        return self._timer_service

    def timestamp(self) -> int:
        return self._timestamp

    def set_timestamp(self, ts: int):
        self._timestamp = ts

    def current_processing_time(self) -> int:
        return self._timer_service.current_processing_time()

    def current_watermark(self) -> int:
        return self._timer_service.current_watermark()


class InternalBroadcastProcessFunctionContext(InternalBaseBroadcastProcessFunctionContext,
                                              BroadcastProcessFunction.Context):

    def __init__(self, timer_service: TimerService, operator_state_store: OperatorStateStore):
        InternalBaseBroadcastProcessFunctionContext.__init__(
            self, timer_service, operator_state_store
        )

    def get_broadcast_state(self, state_descriptor: MapStateDescriptor) -> BroadcastState:
        return self._operator_state_store.get_broadcast_state(state_descriptor)


class InternalBroadcastProcessFunctionReadOnlyContext(InternalBaseBroadcastProcessFunctionContext,
                                                      BroadcastProcessFunction.ReadOnlyContext):

    def __init__(self, timer_service: TimerService, operator_state_store: OperatorStateStore):
        InternalBaseBroadcastProcessFunctionContext.__init__(
            self, timer_service, operator_state_store
        )

    def get_broadcast_state(self, state_descriptor: MapStateDescriptor) -> ReadOnlyBroadcastState:
        return cast(
            InternalBroadcastState,
            self._operator_state_store.get_broadcast_state(state_descriptor)
        ).to_read_only_broadcast_state()


class InternalKeyedBroadcastProcessFunctionContext(InternalBaseBroadcastProcessFunctionContext,
                                                   KeyedBroadcastProcessFunction.Context):

    def __init__(self, timer_service: TimerService, operator_state_store: OperatorStateStore):
        InternalBaseBroadcastProcessFunctionContext.__init__(
            self, timer_service, operator_state_store
        )

    def get_broadcast_state(self, state_descriptor: MapStateDescriptor) -> BroadcastState:
        return self._operator_state_store.get_broadcast_state(state_descriptor)


class InternalKeyedBroadcastProcessFunctionReadOnlyContext(
        InternalBaseBroadcastProcessFunctionContext,
        KeyedBroadcastProcessFunction.ReadOnlyContext):

    def __init__(self, timer_service: TimerService, operator_state_store: OperatorStateStore):
        InternalBaseBroadcastProcessFunctionContext.__init__(
            self, timer_service, operator_state_store
        )
        self._current_key = None

    def set_current_key(self, key):
        self._current_key = key

    def get_current_key(self):
        return self._current_key

    def get_broadcast_state(self, state_descriptor: MapStateDescriptor) -> ReadOnlyBroadcastState:
        return cast(
            InternalBroadcastState,
            self._operator_state_store.get_broadcast_state(state_descriptor)
        ).to_read_only_broadcast_state()


class InternalKeyedBroadcastProcessFunctionOnTimerContext(
        InternalBaseBroadcastProcessFunctionContext,
        KeyedBroadcastProcessFunction.OnTimerContext):

    def __init__(self, timer_service: TimerService, operator_state_store: OperatorStateStore):
        InternalBaseBroadcastProcessFunctionContext.__init__(
            self, timer_service, operator_state_store
        )
        self._current_key = None
        self._time_domain = None

    def set_current_key(self, key):
        self._current_key = key

    def get_current_key(self):
        return self._current_key

    def set_time_domain(self, time_domain):
        self._time_domain = time_domain

    def time_domain(self):
        return self._time_domain

    def get_broadcast_state(self, state_descriptor: MapStateDescriptor) -> ReadOnlyBroadcastState:
        return cast(
            InternalBroadcastState,
            self._operator_state_store.get_broadcast_state(state_descriptor)
        ).to_read_only_broadcast_state()
