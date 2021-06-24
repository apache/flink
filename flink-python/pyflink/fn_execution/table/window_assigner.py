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
import math
from abc import ABC, abstractmethod
from typing import Generic, List, Any, Iterable, Set

from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor, ValueState
from pyflink.datastream.window import TimeWindow, CountWindow
from pyflink.fn_execution.table.window_context import Context, W


class WindowAssigner(Generic[W], ABC):
    """
    A WindowAssigner assigns zero or more Windows to an element.

    In a window operation, elements are grouped by their key (if available) and by the windows to
    which it was assigned. The set of elements with the same key and window is called a pane. When a
    Trigger decides that a certain pane should fire the window to produce output elements for
    that pane.
    """

    def open(self, ctx: Context[Any, W]):
        """
        Initialization method for the function. It is called before the actual working methods.
        """
        pass

    @abstractmethod
    def assign_windows(self, element: List, timestamp: int) -> Iterable[W]:
        """
        Given the timestamp and element, returns the set of windows into which it should be placed.
        :param element: The element to which windows should be assigned.
        :param timestamp: The timestamp of the element when {@link #isEventTime()} returns true, or
            the current system time when {@link #isEventTime()} returns false.
        """
        pass

    @abstractmethod
    def is_event_time(self) -> bool:
        """
        Returns True if elements are assigned to windows based on event time, False otherwise.
        """
        pass


class PanedWindowAssigner(WindowAssigner[W], ABC):
    """
    A WindowAssigner that window can be split into panes.
    """

    @abstractmethod
    def assign_pane(self, element, timestamp: int) -> W:
        pass

    @abstractmethod
    def split_into_panes(self, window: W) -> Iterable[W]:
        pass

    @abstractmethod
    def get_last_window(self, pane: W) -> W:
        pass


class MergingWindowAssigner(WindowAssigner[W], ABC):
    """
    A WindowAssigner that can merge windows.
    """

    class MergeCallback:
        """
        Callback to be used in merge_windows(W, List[W], MergeCallback) for
        specifying which windows should be merged.
        """

        @abstractmethod
        def merge(self, merge_result: W, to_be_merged: Iterable[W]):
            """
            Specifies that the given windows should be merged into the result window.

            :param merge_result: The resulting merged window.
            :param to_be_merged: The list of windows that should be merged into one window.
            """
            pass

    @abstractmethod
    def merge_windows(self, new_window: W, sorted_windows: List[W], merge_callback: MergeCallback):
        """
        Determines which windows (if any) should be merged.

        :param new_window: The new window.
        :param sorted_windows: The sorted window candidates.
        :param merge_callback: A callback that can be invoked to signal which windows should be
            merged.
        """
        pass


class TumblingWindowAssigner(WindowAssigner[TimeWindow]):
    """
    A WindowAssigner that windows elements into fixed-size windows based on the timestamp of
    the elements. Windows cannot overlap.
    """

    def __init__(self, size: int, offset: int, is_event_time: bool):
        self._size = size
        self._offset = offset
        self._is_event_time = is_event_time

    def assign_windows(self, element: List, timestamp: int) -> Iterable[TimeWindow]:
        start = TimeWindow.get_window_start_with_offset(timestamp, self._offset, self._size)
        return [TimeWindow(start, start + self._size)]

    def is_event_time(self) -> bool:
        return self._is_event_time

    def __repr__(self):
        return "TumblingWindow(%s)" % self._size


class CountTumblingWindowAssigner(WindowAssigner[CountWindow]):
    """
    A WindowAssigner that windows elements into fixed-size windows based on the count number
    of the elements. Windows cannot overlap.
    """

    def __init__(self, size: int):
        self._size = size
        self._count = None  # type: ValueState

    def open(self, ctx: Context[Any, CountWindow]):
        value_state_descriptor = ValueStateDescriptor('tumble-count-assigner', Types.LONG())
        self._count = ctx.get_partitioned_state(value_state_descriptor)

    def assign_windows(self, element: List, timestamp: int) -> Iterable[CountWindow]:
        count_value = self._count.value()
        if count_value is None:
            current_count = 0
        else:
            current_count = count_value
        id = current_count // self._size
        self._count.update(current_count + 1)
        return [CountWindow(id)]

    def is_event_time(self) -> bool:
        return False

    def __repr__(self):
        return "CountTumblingWindow(%s)" % self._size


class SlidingWindowAssigner(PanedWindowAssigner[TimeWindow]):
    """
    A WindowAssigner that windows elements into sliding windows based on the timestamp of the
    elements. Windows can possibly overlap.
    """

    def __init__(self, size: int, slide: int, offset: int, is_event_time: bool):
        self._size = size
        self._slide = slide
        self._offset = offset
        self._is_event_time = is_event_time
        self._pane_size = math.gcd(size, slide)
        self._num_panes_per_window = size // self._pane_size

    def assign_pane(self, element, timestamp: int) -> TimeWindow:
        start = TimeWindow.get_window_start_with_offset(timestamp, self._offset, self._pane_size)
        return TimeWindow(start, start + self._pane_size)

    def split_into_panes(self, window: W) -> Iterable[TimeWindow]:
        start = window.start
        for i in range(self._num_panes_per_window):
            yield TimeWindow(start, start + self._pane_size)
            start += self._pane_size

    def get_last_window(self, pane: W) -> TimeWindow:
        last_start = TimeWindow.get_window_start_with_offset(pane.start, self._offset, self._slide)
        return TimeWindow(last_start, last_start + self._size)

    def assign_windows(self, element: List, timestamp: int) -> Iterable[TimeWindow]:
        last_start = TimeWindow.get_window_start_with_offset(timestamp, self._offset, self._slide)
        windows = [TimeWindow(start, start + self._size)
                   for start in range(last_start, timestamp - self._size, -self._slide)]
        return windows

    def is_event_time(self) -> bool:
        return self._is_event_time

    def __repr__(self):
        return "SlidingWindowAssigner(%s, %s)" % (self._size, self._slide)


class CountSlidingWindowAssigner(WindowAssigner[CountWindow]):
    """
    A WindowAssigner that windows elements into sliding windows based on the count number of
    the elements. Windows can possibly overlap.
    """

    def __init__(self, size, slide):
        self._size = size
        self._slide = slide
        self._count = None  # type: ValueState

    def open(self, ctx: Context[Any, CountWindow]):
        count_descriptor = ValueStateDescriptor('slide-count-assigner', Types.LONG())
        self._count = ctx.get_partitioned_state(count_descriptor)

    def assign_windows(self, element: List, timestamp: int) -> Iterable[W]:
        count_value = self._count.value()
        if count_value is None:
            current_count = 0
        else:
            current_count = count_value
        self._count.update(current_count + 1)
        last_id = current_count // self._slide
        last_start = last_id * self._slide
        last_end = last_start + self._size - 1
        windows = []
        while last_id >= 0 and last_start <= current_count <= last_end:
            if last_start <= current_count <= last_end:
                windows.append(CountWindow(last_id))
            last_id -= 1
            last_start -= self._slide
            last_end -= self._slide
        return windows

    def is_event_time(self) -> bool:
        return False

    def __repr__(self):
        return "CountSlidingWindowAssigner(%s, %s)" % (self._size, self._slide)


class SessionWindowAssigner(MergingWindowAssigner[TimeWindow]):
    """
    WindowAssigner that windows elements into sessions based on the timestamp. Windows cannot
    overlap.
    """

    def __init__(self, session_gap: int, is_event_time: bool):
        self._session_gap = session_gap
        self._is_event_time = is_event_time

    def merge_windows(self, new_window: W, sorted_windows: List[TimeWindow],
                      merge_callback: MergingWindowAssigner.MergeCallback):
        ceiling = self._ceiling_window(new_window, sorted_windows)
        floor = self._floor_window(new_window, sorted_windows)
        merge_result = new_window
        merged_windows = set()
        if ceiling:
            merge_result = self._merge_window(merge_result, ceiling, merged_windows)
        if floor:
            merge_result = self._merge_window(merge_result, floor, merged_windows)
        if merged_windows:
            merged_windows.add(new_window)
            merge_callback.merge(merge_result, merged_windows)

    def assign_windows(self, element: List, timestamp: int) -> Iterable[TimeWindow]:
        return [TimeWindow(timestamp, timestamp + self._session_gap)]

    def is_event_time(self) -> bool:
        return self._is_event_time

    @staticmethod
    def _ceiling_window(new_window: TimeWindow, sorted_windows: List[TimeWindow]):
        if not sorted_windows:
            return None
        window_num = len(sorted_windows)
        if sorted_windows[0] >= new_window:
            return None
        for i in range(window_num - 1):
            if sorted_windows[i] <= new_window <= sorted_windows[i + 1]:
                return sorted_windows[i]
        return sorted_windows[window_num - 1]

    @staticmethod
    def _floor_window(new_window: TimeWindow, sorted_windows: List[TimeWindow]):
        if not sorted_windows:
            return None
        window_num = len(sorted_windows)
        if sorted_windows[window_num - 1] <= new_window:
            return None
        for i in range(window_num - 1):
            if sorted_windows[i] <= new_window <= sorted_windows[i + 1]:
                return sorted_windows[i + 1]
        return sorted_windows[0]

    # Merge curWindow and other, return a new window which covers curWindow and other if they are
    # overlapped. Otherwise, returns the curWindow itself.
    @staticmethod
    def _merge_window(cur_window: TimeWindow, other: TimeWindow, merged_windows: Set[TimeWindow]):
        if cur_window.intersects(other):
            merged_windows.add(other)
            return cur_window.cover(other)
        else:
            return cur_window

    def __repr__(self):
        return "SessionWindowAssigner(%s)" % self._session_gap
