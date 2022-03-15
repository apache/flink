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
from typing import Generic, TypeVar, List, Iterable

from apache_beam.coders import Coder

from pyflink.common.constants import MAX_LONG_VALUE
from pyflink.datastream.state import StateDescriptor, State, ValueStateDescriptor, \
    ListStateDescriptor, MapStateDescriptor
from pyflink.datastream.window import TimeWindow, CountWindow
from pyflink.fn_execution.datastream.timerservice_impl import LegacyInternalTimerServiceImpl
from pyflink.fn_execution.coders import from_type_info, MapCoder, GenericArrayCoder
from pyflink.fn_execution.internal_state import InternalMergingState
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend

K = TypeVar('K')
W = TypeVar('W', TimeWindow, CountWindow)


class Context(Generic[K, W], ABC):
    """
    Information available in an invocation of methods of InternalWindowProcessFunction.
    """

    @abstractmethod
    def get_partitioned_state(self, state_descriptor: StateDescriptor) -> State:
        """
        Creates a partitioned state handle, using the state backend configured for this task.
        """
        pass

    @abstractmethod
    def current_key(self) -> K:
        """
        Returns current key of current processed element.
        """
        pass

    @abstractmethod
    def current_processing_time(self) -> int:
        """
        Returns the current processing time.
        """
        pass

    @abstractmethod
    def current_watermark(self) -> int:
        """
        Returns the current event-time watermark.
        """
        pass

    @abstractmethod
    def get_window_accumulators(self, window: W) -> List:
        """
        Gets the accumulators of the given window.
        """
        pass

    @abstractmethod
    def set_window_accumulators(self, window: W, acc: List):
        """
        Sets the accumulators of the given window.
        """
        pass

    @abstractmethod
    def clear_window_state(self, window: W):
        """
        Clear window state of the given window.
        """
        pass

    @abstractmethod
    def clear_trigger(self, window: W):
        """
        Call Trigger#clear(Window) on trigger.
        """
        pass

    @abstractmethod
    def on_merge(self, new_window: W, merged_windows: Iterable[W]):
        """
        Call Trigger.on_merge() on trigger.
        """
        pass

    @abstractmethod
    def delete_cleanup_timer(self, window: W):
        """
        Deletes the cleanup timer set for the contents of the provided window.
        """
        pass


class WindowContext(Context[K, W]):
    """
    Context of window.
    """

    def __init__(self,
                 window_operator,
                 trigger_context: 'TriggerContext',
                 state_backend: RemoteKeyedStateBackend,
                 state_value_coder: Coder,
                 timer_service: LegacyInternalTimerServiceImpl,
                 is_event_time: bool):
        self._window_operator = window_operator
        self._trigger_context = trigger_context
        self._state_backend = state_backend
        self.timer_service = timer_service
        self.is_event_time = is_event_time
        self.window_state = self._state_backend.get_value_state("window_state", state_value_coder)

    def get_partitioned_state(self, state_descriptor: StateDescriptor) -> State:
        return self._trigger_context.get_partitioned_state(state_descriptor)

    def current_key(self) -> K:
        return self._state_backend.get_current_key()

    def current_processing_time(self) -> int:
        return self.timer_service.current_processing_time()

    def current_watermark(self) -> int:
        return self.timer_service.current_watermark()

    def get_window_accumulators(self, window: W) -> List:
        self.window_state.set_current_namespace(window)
        return self.window_state.value()

    def set_window_accumulators(self, window: W, acc: List):
        self.window_state.set_current_namespace(window)
        self.window_state.update(acc)

    def clear_window_state(self, window: W):
        self.window_state.set_current_namespace(window)
        self.window_state.clear()

    def clear_trigger(self, window: W):
        self._trigger_context.window = window
        self._trigger_context.clear()

    def on_merge(self, new_window: W, merged_windows: Iterable[W]):
        self._trigger_context.window = new_window
        self._trigger_context.merged_windows = merged_windows
        self._trigger_context.on_merge()

    def delete_cleanup_timer(self, window: W):
        cleanup_time = self._window_operator.cleanup_time(window)
        if cleanup_time == MAX_LONG_VALUE:
            # no need to clean up because we didn't set one
            return
        if self.is_event_time:
            self._trigger_context.delete_event_time_timer(cleanup_time)
        else:
            self._trigger_context.delete_processing_time_timer(cleanup_time)


class TriggerContext(object):
    """
    TriggerContext is a utility for handling Trigger invocations. It can be reused by setting the
    key and window fields. No internal state must be kept in the TriggerContext.
    """

    def __init__(self,
                 trigger,
                 timer_service: LegacyInternalTimerServiceImpl[W],
                 state_backend: RemoteKeyedStateBackend):
        self._trigger = trigger
        self._timer_service = timer_service
        self._state_backend = state_backend
        self.window = None  # type: W
        self.merged_windows = None  # type: Iterable[W]

    def open(self):
        self._trigger.open(self)

    def on_element(self, row, timestamp: int) -> bool:
        return self._trigger.on_element(row, timestamp, self.window)

    def on_processing_time(self, timestamp: int) -> bool:
        return self._trigger.on_processing_time(timestamp, self.window)

    def on_event_time(self, timestamp: int) -> bool:
        return self._trigger.on_event_time(timestamp, self.window)

    def on_merge(self):
        self._trigger.on_merge(self.window, self)

    def get_current_processing_time(self) -> int:
        return self._timer_service.current_processing_time()

    def get_current_watermark(self) -> int:
        return self._timer_service.current_watermark()

    def register_processing_time_timer(self, time: int):
        self._timer_service.register_processing_time_timer(self.window, time)

    def register_event_time_timer(self, time: int):
        self._timer_service.register_event_time_timer(self.window, time)

    def delete_processing_time_timer(self, time: int):
        self._timer_service.delete_processing_time_timer(self.window, time)

    def delete_event_time_timer(self, time: int):
        self._timer_service.delete_event_time_timer(self.window, time)

    def clear(self):
        self._trigger.clear(self.window)

    def get_partitioned_state(self, state_descriptor: StateDescriptor) -> State:
        if isinstance(state_descriptor, ValueStateDescriptor):
            state = self._state_backend.get_value_state(
                state_descriptor.name, from_type_info(state_descriptor.type_info))
        elif isinstance(state_descriptor, ListStateDescriptor):
            array_coder = from_type_info(state_descriptor.type_info)  # type: GenericArrayCoder
            state = self._state_backend.get_list_state(
                state_descriptor.name, array_coder._elem_coder)
        elif isinstance(state_descriptor, MapStateDescriptor):
            map_coder = from_type_info(state_descriptor.type_info)  # type: MapCoder
            key_coder = map_coder._key_coder
            value_coder = map_coder._value_coder
            state = self._state_backend.get_map_state(
                state_descriptor.name, key_coder, value_coder)
        else:
            raise Exception("Unknown supported StateDescriptor %s" % state_descriptor)
        state.set_current_namespace(self.window)
        return state

    def merge_partitioned_state(self, state_descriptor: StateDescriptor):
        if not self.merged_windows:
            state = self.get_partitioned_state(state_descriptor)
            if isinstance(state, InternalMergingState):
                state.merge_namespaces(self.window, self.merged_windows)
            else:
                raise Exception("The given state descriptor does not refer to a mergeable state"
                                " (MergingState)")
