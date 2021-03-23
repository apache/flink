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
import sys
from abc import abstractmethod, ABC
from typing import Generic, List

from pyflink.common import Row
from pyflink.fn_execution.window_assigner import WindowAssigner
from pyflink.fn_execution.window_context import Context, K, W

MAX_LONG_VALUE = sys.maxsize


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
