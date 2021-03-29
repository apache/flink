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
import typing
from typing import TypeVar, Iterable, Collection

from pyflink.common.typeinfo import Types
from pyflink.datastream import WindowAssigner, Trigger, MergingWindowAssigner, TriggerResult
from pyflink.datastream.functions import KeyedStateStore, RuntimeContext, InternalWindowFunction
from pyflink.datastream.state import StateDescriptor, ListStateDescriptor, \
    ReducingStateDescriptor, AggregatingStateDescriptor, ValueStateDescriptor, MapStateDescriptor, \
    State, AggregatingState, ReducingState, MapState, ListState, ValueState, AppendingState
from pyflink.datastream.timerservice import InternalTimerService, InternalTimer
from pyflink.datastream.window import MAX_LONG_VALUE
from pyflink.fn_execution.datastream.merging_window_set import MergingWindowSet
from pyflink.fn_execution.internal_state import InternalMergingState, InternalKvState, \
    InternalAppendingState
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend
from pyflink.metrics import MetricGroup

T = TypeVar("T")
IN = TypeVar("IN")
OUT = TypeVar("OUT")
KEY = TypeVar("KEY")
W = TypeVar("W")


def get_or_create_keyed_state(runtime_context, state_descriptor):
    if isinstance(state_descriptor, ListStateDescriptor):
        state = runtime_context.get_list_state(state_descriptor)
    elif isinstance(state_descriptor, ReducingStateDescriptor):
        state = runtime_context.get_reducing_state(state_descriptor)
    elif isinstance(state_descriptor, AggregatingStateDescriptor):
        state = runtime_context.get_aggregating_state(state_descriptor)
    elif isinstance(state_descriptor, ValueStateDescriptor):
        state = runtime_context.get_state(state_descriptor)
    elif isinstance(state_descriptor, MapStateDescriptor):
        state = runtime_context.get_map_state(state_descriptor)
    else:
        raise Exception("Unsupported state descriptor: %s" % type(state_descriptor))
    return state


class MergingWindowStateStore(KeyedStateStore):

    def __init__(self):
        self.window = None

    def get_state(self, state_descriptor: ValueStateDescriptor) -> ValueState:
        raise Exception("Per-window state is not allowed when using merging windows.")

    def get_list_state(self, state_descriptor: ListStateDescriptor) -> ListState:
        raise Exception("Per-window state is not allowed when using merging windows.")

    def get_map_state(self, state_descriptor: MapStateDescriptor) -> MapState:
        raise Exception("Per-window state is not allowed when using merging windows.")

    def get_reducing_state(self, state_descriptor: ReducingStateDescriptor) -> ReducingState:
        raise Exception("Per-window state is not allowed when using merging windows.")

    def get_aggregating_state(
            self, state_descriptor: AggregatingStateDescriptor) -> AggregatingState:
        raise Exception("Per-window state is not allowed when using merging windows.")


class PerWindowStateStore(KeyedStateStore):

    def __init__(self, runtime_context):
        self._runtime_context = runtime_context
        self.window = None

    def get_state(self, state_descriptor: ValueStateDescriptor) -> ValueState:
        return self._set_namespace(self._runtime_context.get_state(state_descriptor))

    def get_list_state(self, state_descriptor: ListStateDescriptor) -> ListState:
        return self._set_namespace(self._runtime_context.get_list_state(state_descriptor))

    def get_map_state(self, state_descriptor: MapStateDescriptor) -> MapState:
        return self._set_namespace(self._runtime_context.get_map_state(state_descriptor))

    def get_reducing_state(self, state_descriptor: ReducingStateDescriptor) -> ReducingState:
        return self._set_namespace(self._runtime_context.get_reducing_state(state_descriptor))

    def get_aggregating_state(
            self, state_descriptor: AggregatingStateDescriptor) -> AggregatingState:
        return self._set_namespace(self._runtime_context.get_aggregating_state(state_descriptor))

    def _set_namespace(self, state):
        state.set_current_namespace(self.window)
        return state


class Context(Trigger.OnMergeContext):

    def __init__(
            self,
            runtime_context: RuntimeContext,
            internal_timer_service: InternalTimerService,
            trigger: Trigger):
        self._runtime_context = runtime_context
        self._internal_timer_service = internal_timer_service
        self._trigger = trigger
        self.user_key = None
        self.window = None
        self.merged_windows = None

    def get_current_processing_time(self) -> int:
        return self._internal_timer_service.current_processing_time()

    def get_metric_group(self) -> MetricGroup:
        return self._runtime_context.get_metrics_group()

    def get_current_watermark(self) -> int:
        return self._internal_timer_service.current_watermark()

    def register_processing_time_timer(self, time: int) -> None:
        self._internal_timer_service.register_processing_time_timer(self.window, time)

    def register_event_time_timer(self, time: int) -> None:
        self._internal_timer_service.register_event_time_timer(self.window, time)

    def delete_processing_time_timer(self, time: int) -> None:
        self._internal_timer_service.delete_processing_time_timer(self.window, time)

    def delete_event_time_timer(self, time: int) -> None:
        self._internal_timer_service.delete_event_time_timer(self.window, time)

    def merge_partitioned_state(self, state_descriptor: StateDescriptor) -> None:
        if self.merged_windows is not None and len(self.merged_windows) > 0:
            raw_state = get_or_create_keyed_state(self._runtime_context, state_descriptor)
            if isinstance(raw_state, InternalMergingState):
                raw_state.merge_namespaces(self.window, self.merged_windows)
            else:
                raise Exception(
                    "The given state descriptor does not refer to a mergeable state (MergingState)")

    def get_partitioned_state(self, state_descriptor: StateDescriptor) -> State:
        state = get_or_create_keyed_state(
            self._runtime_context, state_descriptor)  # type: InternalKvState
        state.set_current_namespace(self.window)
        return state

    def on_element(self, value, timestamp) -> TriggerResult:
        return self._trigger.on_element(value, timestamp, self.window, self)

    def on_processing_time(self, time) -> TriggerResult:
        return self._trigger.on_processing_time(time, self.window, self)

    def on_event_time(self, time) -> TriggerResult:
        return self._trigger.on_event_time(time, self.window, self)

    def on_merge(self, merged_windows) -> None:
        self.merged_windows = merged_windows
        self._trigger.on_merge(self.window, self)

    def clear(self) -> None:
        self._trigger.clear(self.window, self)


class WindowContext(InternalWindowFunction.InternalWindowContext):

    def __init__(self,
                 window_assigner: WindowAssigner,
                 runtime_context: RuntimeContext,
                 window_function: InternalWindowFunction,
                 internal_timer_service: InternalTimerService):
        self.window = None
        if isinstance(window_assigner, MergingWindowAssigner):
            self._window_state = MergingWindowStateStore()
        else:
            self._window_state = PerWindowStateStore(runtime_context)
        self._runtime_context = runtime_context
        self._user_function = window_function
        self._internal_timer_service = internal_timer_service

    def current_processing_time(self) -> int:
        return self._internal_timer_service.current_processing_time()

    def current_watermark(self) -> int:
        return self._internal_timer_service.current_watermark()

    def window_state(self) -> KeyedStateStore:
        self._window_state.window = self.window
        return self._window_state

    def global_state(self) -> KeyedStateStore:
        return self._runtime_context

    def clear(self) -> None:
        self._user_function.clear(self.window, self)


class WindowAssignerContext(WindowAssigner.WindowAssignerContext):

    def __init__(self,
                 internal_timer_service: InternalTimerService,
                 runtime_context: RuntimeContext):
        self._internal_timer_service = internal_timer_service
        self._runtime_context = runtime_context

    def get_current_processing_time(self) -> int:
        return self._internal_timer_service.current_processing_time()

    def get_runtime_context(self) -> RuntimeContext:
        return self._runtime_context


class WindowMergeFunction(MergingWindowSet.MergeFunction[W]):

    def __init__(self,
                 window_operator: 'WindowOperator'):
        self._window_assigner = window_operator.window_assigner
        self._internal_timer_service = window_operator.internal_timer_service
        self._allowed_lateness = window_operator.allowed_lateness
        self._trigger_context = window_operator.trigger_context
        self._window_merging_state = window_operator.window_merging_state
        self._user_key_selector = window_operator.user_key_selector
        self.delete_cleanup_timer = window_operator.delete_cleanup_timer

        self.key = None

    def merge(self,
              merge_result: W,
              merged_windows: Collection[W],
              state_window_result: W,
              merged_state_windows: Collection[W]):
        if self._window_assigner.is_event_time() and \
                merge_result.max_timestamp() + self._allowed_lateness <= \
                self._internal_timer_service.current_watermark():
            raise Exception("The end timestamp of an event-time window cannot become earlier than "
                            "the current watermark by merging. Current watermark: %d window: %s" %
                            (self._internal_timer_service.current_watermark(), merge_result))
        elif not self._window_assigner.is_event_time():
            current_processing_time = self._internal_timer_service.current_processing_time()
            if merge_result.max_timestamp() <= current_processing_time:
                raise Exception("The end timestamp of a processing-time window cannot become "
                                "earlier than the current processing time by merging. Current "
                                "processing time: %d window: %s" %
                                (current_processing_time, merge_result))

        self._trigger_context.user_key = self._user_key_selector(self.key)
        self._trigger_context.window = merge_result

        self._trigger_context.on_merge(merged_windows)

        for m in merged_windows:
            self._trigger_context.window = m
            self._trigger_context.clear()
            self.delete_cleanup_timer(m)

        self._window_merging_state.merge_namespaces(state_window_result, merged_state_windows)


class WindowOperator(object):
    LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped"

    def __init__(self,
                 window_assigner: WindowAssigner,
                 keyed_state_backend: RemoteKeyedStateBackend,
                 user_key_selector,
                 window_state_descriptor: StateDescriptor,
                 window_function: InternalWindowFunction,
                 trigger: Trigger,
                 allowed_lateness: int):
        self.window_assigner = window_assigner
        self.keyed_state_backend = keyed_state_backend
        self.user_key_selector = user_key_selector
        self.window_state_descriptor = window_state_descriptor
        self.window_function = window_function
        self.trigger = trigger
        self.allowed_lateness = allowed_lateness

        self.num_late_records_dropped = None
        self.internal_timer_service = None  # type: InternalTimerService
        self.trigger_context = None  # type: Context
        self.process_context = None  # type: WindowContext
        self.window_assigner_context = None  # type: WindowAssignerContext
        self.window_state = None  # type: InternalAppendingState
        self.window_merging_state = None  # type: InternalMergingState
        self.merging_sets_state = None

        self.merge_function = None  # type: WindowMergeFunction

    def open(self, runtime_context: RuntimeContext, internal_timer_service: InternalTimerService):
        self.window_function.open(runtime_context)

        self.num_late_records_dropped = runtime_context.get_metrics_group().counter(
            self.LATE_ELEMENTS_DROPPED_METRIC_NAME)
        self.internal_timer_service = internal_timer_service
        self.trigger_context = Context(runtime_context, internal_timer_service, self.trigger)
        self.process_context = WindowContext(
            self.window_assigner,
            runtime_context,
            self.window_function,
            self.internal_timer_service)
        self.window_assigner_context = WindowAssignerContext(
            self.internal_timer_service,
            runtime_context)

        # create (or restore) the state that hold the actual window contents
        # NOTE - the state may be null in the case of the overriding evicting window operator
        if self.window_state_descriptor is not None:
            self.window_state = get_or_create_keyed_state(
                runtime_context, self.window_state_descriptor)

        if isinstance(self.window_assigner, MergingWindowAssigner):
            if isinstance(self.window_state, InternalMergingState):
                self.window_merging_state = self.window_state

            # TODO: the type info is just a placeholder currently.
            # it should be the real type serializer after supporting the user-defined state type
            # serializer
            merging_sets_state_descriptor = ListStateDescriptor(
                "merging-window-set", Types.PICKLED_BYTE_ARRAY())

            self.merging_sets_state = get_or_create_keyed_state(
                runtime_context, merging_sets_state_descriptor)

        self.merge_function = WindowMergeFunction(self)

    def close(self):
        self.window_function.close()
        self.trigger_context = None
        self.process_context = None
        self.window_assigner_context = None

    def process_element(self, value, timestamp: int):
        element_windows = self.window_assigner.assign_windows(
            value, timestamp, self.window_assigner_context)

        is_skipped_element = True

        key = self.keyed_state_backend.get_current_key()
        self.merge_function.key = key

        if isinstance(self.window_assigner, MergingWindowAssigner):
            merging_windows = self.get_merging_window_set()

            for window in element_windows:
                actual_window = merging_windows.add_window(window, self.merge_function)
                if self.is_window_late(actual_window):
                    merging_windows.retire_window(actual_window)
                    continue
                is_skipped_element = False

                state_window = merging_windows.get_state_window(actual_window)
                if state_window is None:
                    raise Exception("Window %s is not in in-flight window set." % state_window)

                self.window_state.set_current_namespace(state_window)
                self.window_state.add(value)

                self.trigger_context.user_key = self.user_key_selector(key)
                self.trigger_context.window = actual_window

                trigger_result = self.trigger_context.on_element(value, timestamp)

                if trigger_result.is_fire():
                    contents = self.window_state.get()
                    # for list state the iterable will never be none
                    if isinstance(self.window_state, ListState):
                        contents = [i for i in contents]
                        if len(contents) == 0:
                            contents = None
                    if contents is None:
                        continue
                    yield from self.emit_window_contents(actual_window, contents)

                if trigger_result.is_purge():
                    self.window_state.clear()
                self.register_cleanup_timer(window)

            merging_windows.persist()
        else:
            for window in element_windows:
                if self.is_window_late(window):
                    continue
                is_skipped_element = False

                self.window_state.set_current_namespace(window)
                self.window_state.add(value)

                self.trigger_context.user_key = self.user_key_selector(key)
                self.trigger_context.window = window

                trigger_result = self.trigger_context.on_element(value, timestamp)

                if trigger_result.is_fire():
                    contents = self.window_state.get()
                    # for list state the iterable will never be none
                    if isinstance(self.window_state, ListState):
                        contents = [i for i in contents]
                        if len(contents) == 0:
                            contents = None
                    if contents is None:
                        continue
                    yield from self.emit_window_contents(window, contents)

                if trigger_result.is_purge():
                    self.window_state.clear()
                self.register_cleanup_timer(window)

        if is_skipped_element and self.is_element_late(value, timestamp):
            self.num_late_records_dropped.inc()

    def on_event_time(self, timer: InternalTimer) -> None:
        self.trigger_context.user_key = self.user_key_selector(timer.get_key())
        self.trigger_context.window = timer.get_namespace()

        if isinstance(self.window_assigner, MergingWindowAssigner):
            merging_windows = self.get_merging_window_set()
            state_window = merging_windows.get_state_window(self.trigger_context.window)
            if state_window is None:
                # Timer firing for non-existent window, this can only happen if a
                # trigger did not clean up timers. We have already cleared the merging
                # window and therefore the Trigger state, however, so nothing to do.
                return
            else:
                self.window_state.set_current_namespace(state_window)
        else:
            self.window_state.set_current_namespace(self.trigger_context.window)
            merging_windows = None

        trigger_result = self.trigger_context.on_event_time(timer.get_timestamp())

        if trigger_result.is_fire():
            contents = self.window_state.get()
            # for list state the iterable will never be none
            if isinstance(self.window_state, ListState):
                contents = [i for i in contents]
                if len(contents) == 0:
                    contents = None
            if contents is not None:
                yield from self.emit_window_contents(self.trigger_context.window, contents)

        if trigger_result.is_purge():
            self.window_state.clear()

        if self.window_assigner.is_event_time() and self.is_cleanup_time(
                self.trigger_context.window, timer.get_timestamp()):
            self.clear_all_state(self.trigger_context.window, self.window_state, merging_windows)

        if merging_windows is not None:
            merging_windows.persist()

    def on_processing_time(self, timer):
        self.trigger_context.user_key = self.user_key_selector(timer.get_key())
        self.trigger_context.window = timer.get_namespace()

        if isinstance(self.window_assigner, MergingWindowAssigner):
            merging_windows = self.get_merging_window_set()
            state_window = merging_windows.get_state_window(self.trigger_context.window)
            if state_window is None:
                # Timer firing for non-existent window, this can only happen if a
                # trigger did not clean up timers. We have already cleared the merging
                # window and therefore the Trigger state, however, so nothing to do.
                return
            else:
                self.window_state.set_current_namespace(state_window)
        else:
            self.window_state.set_current_namespace(self.trigger_context.window)
            merging_windows = None

        trigger_result = self.trigger_context.on_processing_time(timer.get_timestamp())

        if trigger_result.is_fire():
            contents = self.window_state.get()
            # for list state the iterable will never be none
            if isinstance(self.window_state, ListState):
                contents = [i for i in contents]
                if len(contents) == 0:
                    contents = None
            if contents is not None:
                yield from self.emit_window_contents(self.trigger_context.window, contents)

        if trigger_result.is_purge():
            self.window_state.clear()

        if self.window_assigner.is_event_time() and self.is_cleanup_time(
                self.trigger_context.window, timer.get_timestamp()):
            self.clear_all_state(self.trigger_context.window, self.window_state, merging_windows)

        if merging_windows is not None:
            merging_windows.persist()

    def get_merging_window_set(self) -> MergingWindowSet:
        return MergingWindowSet(
            typing.cast(MergingWindowAssigner[T, W], self.window_assigner),
            self.merging_sets_state)

    def cleanup_time(self, window) -> int:
        if self.window_assigner.is_event_time():
            time = window.max_timestamp() + self.allowed_lateness
            if time >= window.max_timestamp():
                return time
            else:
                return MAX_LONG_VALUE
        else:
            return window.max_timestamp()

    def is_cleanup_time(self, window, time) -> bool:
        return time == self.cleanup_time(window)

    def register_cleanup_timer(self, window) -> None:
        cleanup_time = self.cleanup_time(window)
        if cleanup_time == MAX_LONG_VALUE:
            return
        if self.window_assigner.is_event_time():
            self.trigger_context.register_event_time_timer(cleanup_time)
        else:
            self.trigger_context.register_processing_time_timer(cleanup_time)

    def delete_cleanup_timer(self, window) -> None:
        cleanup_time = self.cleanup_time(window)
        if cleanup_time == MAX_LONG_VALUE:
            return
        if self.window_assigner.is_event_time():
            self.trigger_context.delete_event_time_timer(cleanup_time)
        else:
            self.trigger_context.delete_processing_time_timer(cleanup_time)

    def is_window_late(self, window) -> bool:
        return self.window_assigner.is_event_time() and \
            self.cleanup_time(window) <= self.internal_timer_service.current_watermark()

    def is_element_late(self, value, timestamp) -> bool:
        return self.window_assigner.is_event_time() and timestamp + self.allowed_lateness <= \
            self.internal_timer_service.current_watermark()

    def clear_all_state(
            self, window, window_state: AppendingState, merging_windows: MergingWindowSet):
        window_state.clear()
        self.trigger_context.clear()
        self.process_context.window = window
        self.process_context.clear()
        if merging_windows is not None:
            merging_windows.retire_window(window)
            merging_windows.persist()

    def emit_window_contents(self, window, contents) -> Iterable:
        self.process_context.window = window
        return self.window_function.process(
            self.trigger_context.user_key, window, self.process_context, contents)
