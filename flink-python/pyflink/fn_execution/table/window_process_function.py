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
from abc import abstractmethod, ABC
from typing import Generic, List, Iterable, Dict, Set

from pyflink.common import Row
from pyflink.common.constants import MAX_LONG_VALUE
from pyflink.datastream.state import MapState
from pyflink.fn_execution.state_impl import LRUCache
from pyflink.fn_execution.table.window_assigner import WindowAssigner, PanedWindowAssigner, \
    MergingWindowAssigner
from pyflink.fn_execution.table.window_context import Context, K, W


def join_row(left: List, right: List):
    return Row(*(left + right))


class InternalWindowProcessFunction(Generic[K, W], ABC):
    """
    The internal interface for functions that process over grouped windows.
    """

    def __init__(self,
                 allowed_lateness: int,
                 window_assigner: WindowAssigner[W],
                 window_aggregator):
        self._allowed_lateness = allowed_lateness
        self._window_assigner = window_assigner
        self._window_aggregator = window_aggregator
        self._ctx = None  # type: Context[K, W]

    def open(self, ctx: Context[K, W]):
        self._ctx = ctx
        self._window_assigner.open(ctx)

    def close(self):
        pass

    def is_cleanup_time(self, window: W, time: int) -> bool:
        return time == self._cleanup_time(window)

    def is_window_late(self, window: W) -> bool:
        return self._window_assigner.is_event_time() and \
            self._cleanup_time(window) <= self._ctx.current_watermark()

    def _cleanup_time(self, window: W) -> int:
        if self._window_assigner.is_event_time():
            cleanup_time = window.max_timestamp() + self._allowed_lateness
            if cleanup_time >= window.max_timestamp():
                return cleanup_time
            else:
                return MAX_LONG_VALUE
        else:
            return window.max_timestamp()

    @abstractmethod
    def assign_state_namespace(self, input_row: List, timestamp: int) -> List[W]:
        """
        Assigns the input element into the state namespace which the input element should be
        accumulated/retracted into.

        :param input_row: The input element
        :param timestamp: The timestamp of the element or the processing time (depends on the type
            of assigner)
        :return: The state namespace.
        """
        pass

    @abstractmethod
    def assign_actual_windows(self, input_row: List, timestamp: int) -> List[W]:
        """
        Assigns the input element into the actual windows which the {@link Trigger} should trigger
        on.

        :param input_row: The input element
        :param timestamp: The timestamp of the element or the processing time (depends on the type
            of assigner)
        :return: The actual windows
        """
        pass

    @abstractmethod
    def prepare_aggregate_accumulator_for_emit(self, window: W):
        """
        Prepares the accumulator of the given window before emit the final result. The accumulator
        is stored in the state or will be created if there is no corresponding accumulator in state.

        :param window: The window
        """
        pass

    @abstractmethod
    def clean_window_if_needed(self, window: W, current_time: int):
        """
        Cleans the given window if needed.

        :param window: The window to cleanup
        :param current_time: The current timestamp
        """
        pass


class GeneralWindowProcessFunction(InternalWindowProcessFunction[K, W]):
    """
    The general implementation of InternalWindowProcessFunction. The WindowAssigner should be a
    regular assigner without implement PanedWindowAssigner or MergingWindowAssigner.
    """

    def __init__(self,
                 allowed_lateness: int,
                 window_assigner: WindowAssigner[W],
                 window_aggregator):
        super(GeneralWindowProcessFunction, self).__init__(
            allowed_lateness, window_assigner, window_aggregator)
        self._reuse_affected_windows = None  # type: List[W]

    def assign_state_namespace(self, input_row: List, timestamp: int) -> List[W]:
        element_windows = self._window_assigner.assign_windows(input_row, timestamp)
        self._reuse_affected_windows = []
        for window in element_windows:
            if not self.is_window_late(window):
                self._reuse_affected_windows.append(window)
        return self._reuse_affected_windows

    def assign_actual_windows(self, input_row: List, timestamp: int) -> List[W]:
        # actual windows is equal to affected window, reuse it
        return self._reuse_affected_windows

    def prepare_aggregate_accumulator_for_emit(self, window: W):
        acc = self._ctx.get_window_accumulators(window)
        if acc is None:
            acc = self._window_aggregator.create_accumulators()
        self._window_aggregator.set_accumulators(window, acc)

    def clean_window_if_needed(self, window: W, current_time: int):
        if self.is_cleanup_time(window, current_time):
            self._ctx.clear_window_state(window)
            self._window_aggregator.cleanup(window)
            self._ctx.clear_trigger(window)


class PanedWindowProcessFunction(InternalWindowProcessFunction[K, W]):
    """
    The implementation of InternalWindowProcessFunction for PanedWindowAssigner.
    """

    def __init__(self,
                 allowed_lateness: int,
                 window_assigner: PanedWindowAssigner[W],
                 window_aggregator):
        super(PanedWindowProcessFunction, self).__init__(
            allowed_lateness, window_assigner, window_aggregator)
        self._window_assigner = window_assigner

    def assign_state_namespace(self, input_row: List, timestamp: int) -> List[W]:
        pane = self._window_assigner.assign_pane(input_row, timestamp)
        if not self._is_pane_late(pane):
            return [pane]
        else:
            return []

    def assign_actual_windows(self, input_row: List, timestamp: int) -> List[W]:
        element_windows = self._window_assigner.assign_windows(input_row, timestamp)
        actual_windows = []
        for window in element_windows:
            if not self.is_window_late(window):
                actual_windows.append(window)
        return actual_windows

    def prepare_aggregate_accumulator_for_emit(self, window: W):
        panes = self._window_assigner.split_into_panes(window)
        acc = self._window_aggregator.create_accumulators()
        # null namespace means use heap data views
        self._window_aggregator.set_accumulators(None, acc)
        for pane in panes:
            pane_acc = self._ctx.get_window_accumulators(pane)
            if pane_acc:
                self._window_aggregator.merge(pane, pane_acc)

    def clean_window_if_needed(self, window: W, current_time: int):
        if self.is_cleanup_time(window, current_time):
            panes = self._window_assigner.split_into_panes(window)
            for pane in panes:
                last_window = self._window_assigner.get_last_window(pane)
                if window == last_window:
                    self._ctx.clear_window_state(pane)
            self._ctx.clear_trigger(window)

    def _is_pane_late(self, pane: W):
        # whether the pane is late depends on the last window which the pane is belongs to is late
        return self._window_assigner.is_event_time() and \
            self.is_window_late(self._window_assigner.get_last_window(pane))


class MergeResultCollector(MergingWindowAssigner.MergeCallback):

    def __init__(self):
        self.merge_results = {}  # type: Dict[W, Iterable[W]]

    def merge(self, merge_result: W, to_be_merged: Iterable[W]):
        self.merge_results[merge_result] = to_be_merged


class MergingWindowProcessFunction(InternalWindowProcessFunction[K, W]):
    """
    The implementation of InternalWindowProcessFunction for MergingWindowAssigner.
    """

    def __init__(self,
                 allowed_lateness: int,
                 window_assigner: MergingWindowAssigner[W],
                 window_aggregator,
                 state_backend):
        super(MergingWindowProcessFunction, self).__init__(
            allowed_lateness, window_assigner, window_aggregator)
        self._window_assigner = window_assigner
        self._reuse_actual_windows = None  # type: List
        self._window_mapping = None  # type: MapState
        self._state_backend = state_backend
        self._sorted_windows = None  # type: List
        self._cached_sorted_windows = LRUCache(10000, None)

    def open(self, ctx: Context[K, W]):
        super(MergingWindowProcessFunction, self).open(ctx)
        self._window_mapping = self._state_backend.get_map_state(
            'session-window-mapping',
            self._state_backend.namespace_coder,
            self._state_backend.namespace_coder)

    def assign_state_namespace(self, input_row: List, timestamp: int) -> List[W]:
        element_windows = self._window_assigner.assign_windows(input_row, timestamp)
        self._initialize_cache(self._ctx.current_key())
        self._reuse_actual_windows = []
        for window in element_windows:
            # adding the new window might result in a merge, in that case the actualWindow
            # is the merged window and we work with that. If we don't merge then
            # actualWindow == window
            actual_window = self._add_window(window)

            # drop if the window is already late
            if self.is_window_late(actual_window):
                self._window_mapping.remove(actual_window)
                self._sorted_windows.remove(actual_window)
            else:
                self._reuse_actual_windows.append(actual_window)

        affected_windows = [self._window_mapping.get(actual)
                            for actual in self._reuse_actual_windows]
        return affected_windows

    def assign_actual_windows(self, input_row: List, timestamp: int) -> List[W]:
        # the actual windows is calculated in assignStateNamespace
        return self._reuse_actual_windows

    def prepare_aggregate_accumulator_for_emit(self, window: W):
        state_window = self._window_mapping.get(window)
        acc = self._ctx.get_window_accumulators(state_window)
        if acc is None:
            acc = self._window_aggregator.create_accumulators()

        self._window_aggregator.set_accumulators(state_window, acc)

    def clean_window_if_needed(self, window: W, current_time: int):
        if self.is_cleanup_time(window, current_time):
            self._ctx.clear_trigger(window)
            state_window = self._window_mapping.get(window)
            self._ctx.clear_window_state(state_window)
            # retire expired window
            self._initialize_cache(self._ctx.current_key())
            self._window_mapping.remove(window)
            self._sorted_windows.remove(window)

    def _initialize_cache(self, key):
        tuple_key = tuple(key)
        self._sorted_windows = self._cached_sorted_windows.get(tuple_key)
        if self._sorted_windows is None:
            self._sorted_windows = [k for k in self._window_mapping]
            self._sorted_windows.sort()
            self._cached_sorted_windows.put(tuple_key, self._sorted_windows)

    def _add_window(self, new_window: W):
        collector = MergeResultCollector()
        self._window_assigner.merge_windows(new_window, self._sorted_windows, collector)

        result_window = new_window
        is_new_window_merged = False

        # perform the merge
        merge_results = collector.merge_results
        for merge_result in merge_results:
            merge_windows = merge_results[merge_result]  # type: Set[W]

            # if our new window is in the merged windows make the merge result the result window
            try:
                merge_windows.remove(new_window)
                is_new_window_merged = True
                result_window = merge_result
            except KeyError:
                pass

            # if our new window is the same as a pre-existing window, nothing to do
            if not merge_windows:
                continue

            # pick any of the merged windows and choose that window's state window
            # as the state window for the merge result
            merged_state_namespace = self._window_mapping.get(iter(merge_windows).__next__())
            merged_state_windows = []

            # figure out the state windows that we are merging
            for merged_window in merge_windows:
                res = self._window_mapping.get(merged_window)
                if res is not None:
                    self._window_mapping.remove(merged_window)
                    self._sorted_windows.remove(merged_window)
                    # don't put the target state window into the merged windows
                    if res != merged_state_namespace:
                        merged_state_windows.append(res)

            self._window_mapping.put(merge_result, merged_state_namespace)
            self._sorted_windows.append(merge_result)
            self._sorted_windows.sort()

            # don't merge the new window itself, it never had any state associated with it
            # i.e. if we are only merging one pre-existing window into itself
            # without extending the pre-existing window
            if not (len(merge_windows) == 1 and merge_result in merge_windows):
                self._merge(
                    merge_result, merge_windows, merged_state_namespace, merged_state_windows)

        # the new window created a new, self-contained window without merging
        if len(merge_results) == 0 or result_window == new_window and not is_new_window_merged:
            self._window_mapping.put(result_window, result_window)
            self._sorted_windows.append(result_window)
            self._sorted_windows.sort()

        return result_window

    def _merge(self, merge_result: W, merge_windows: Set[W], state_window_result: W,
               state_windows_tobe_merged: Iterable[W]):
        self._ctx.on_merge(merge_result, state_windows_tobe_merged)

        # clear registered timers
        for window in merge_windows:
            self._ctx.clear_trigger(window)
            self._ctx.delete_cleanup_timer(window)

        # merge the merged state windows into the newly resulting state window
        if state_windows_tobe_merged:
            target_acc = self._ctx.get_window_accumulators(state_window_result)
            if target_acc is None:
                target_acc = self._window_aggregator.create_accumulators()
            self._window_aggregator.set_accumulators(state_window_result, target_acc)
            for window in state_windows_tobe_merged:
                acc = self._ctx.get_window_accumulators(window)
                if acc is not None:
                    self._window_aggregator.merge(window, acc)
                # clear merged window
                self._ctx.clear_window_state(window)
            target_acc = self._window_aggregator.get_accumulators()
            self._ctx.set_window_accumulators(state_window_result, target_acc)
