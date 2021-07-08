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
from typing import Generic

from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TimeWindow, CountWindow
from pyflink.fn_execution.table.window_context import TriggerContext, W


class Trigger(Generic[W], ABC):
    """
    A Trigger determines when a pane of a window should be evaluated to emit the results for
    that part of the window.

    A pane is the bucket of elements that have the same key and same Window. An element
    an be in multiple panes if it was assigned to multiple windows by the WindowAssigner.
    These panes all have their own instance of the Trigger.

    Triggers must not maintain state internally since they can be re-created or reused for
    different keys. All necessary state should be persisted using the state abstraction available on
    the TriggerContext.
    """

    @abstractmethod
    def open(self, ctx: TriggerContext):
        """
        Initialization method for the trigger. Creates states in this method.

        :param ctx: A context object that can be used to get states.
        """
        pass

    @abstractmethod
    def on_element(self, element, timestamp, window: W) -> bool:
        """
        Called for every element that gets added to a pane. The result of this will determine
        whether the pane is evaluated to emit results.

        :param element: The element that arrived.
        :param timestamp: The timestamp of the element that arrived.
        :param window: The window to which the element is being added.
        :return: True for firing the window, False for no action
        """
        pass

    @abstractmethod
    def on_processing_time(self, time: int, window: W) -> bool:
        """
        Called when a processing-time timer that was set using the trigger context fires.

        This method is not called in case the window does not contain any elements. Thus, if
        you return PURGE from a trigger method and you expect to do cleanup in a future
        invocation of a timer callback it might be wise to clean any state that you would clean in
        the timer callback.

        :param time: The timestamp at which the timer fired.
        :param window: The window for which the timer fired.
        :return: True for firing the window, False for no action
        """
        pass

    @abstractmethod
    def on_event_time(self, time: int, window: W) -> bool:
        """
        Called when a event-time timer that was set using the trigger context fires.

        This method is not called in case the window does not contain any elements. Thus, if
        you return PURGE from a trigger method and you expect to do cleanup in a future
        invocation of a timer callback it might be wise to clean any state that you would clean in
        the timer callback.

        :param time: The timestamp at which the timer fired.
        :param window: The window for which the timer fired.
        :return: True for firing the window, False for no action
        """
        pass

    @abstractmethod
    def on_merge(self, window: W, merge_context: TriggerContext):
        """
        Called when several windows have been merged into one window by the WindowAssigner.
        """
        pass

    @abstractmethod
    def clear(self, window: W) -> None:
        """
        Clears any state that the trigger might still hold for the given window. This is called when
        a window is purged. Timers set using TriggerContext.register_event_time_timer(int) and
        TriggerContext.register_processing_time_timer(int) should be deleted here as well as
        state acquired using TriggerContext.get_partitioned_state(StateDescriptor).
        """
        pass


class ProcessingTimeTrigger(Trigger[TimeWindow]):
    """
    A Trigger that fires once the current system time passes the end of the window to which a
    pane belongs.
    """

    def __init__(self):
        self._ctx = None  # type: TriggerContext

    def open(self, ctx: TriggerContext):
        self._ctx = ctx

    def on_element(self, element, timestamp, window: W) -> bool:
        self._ctx.register_processing_time_timer(window.max_timestamp())
        return False

    def on_processing_time(self, time: int, window: W) -> bool:
        return time == window.max_timestamp()

    def on_event_time(self, time: int, window: W) -> bool:
        return False

    def on_merge(self, window: W, merge_context: TriggerContext):
        self._ctx.register_processing_time_timer(window.max_timestamp())

    def clear(self, window: W) -> None:
        self._ctx.delete_processing_time_timer(window.max_timestamp())


class EventTimeTrigger(Trigger[TimeWindow]):
    """
    A Trigger that fires once the watermark passes the end of the window to which a pane
    belongs.
    """

    def __init__(self):
        self._ctx = None  # type: TriggerContext

    def open(self, ctx: TriggerContext):
        self._ctx = ctx

    def on_element(self, element, timestamp, window: W) -> bool:
        if window.max_timestamp() <= self._ctx.get_current_watermark():
            # if the watermark is already past the window fire immediately
            return True
        else:
            self._ctx.register_event_time_timer(window.max_timestamp())
            return False

    def on_processing_time(self, time: int, window: W) -> bool:
        return False

    def on_event_time(self, time: int, window: W) -> bool:
        return time == window.max_timestamp()

    def on_merge(self, window: W, merge_context: TriggerContext):
        self._ctx.register_event_time_timer(window.max_timestamp())

    def clear(self, window: W) -> None:
        self._ctx.delete_event_time_timer(window.max_timestamp())


class CountTrigger(Trigger[CountWindow]):
    """
    A Trigger that fires once the count of elements in a pane reaches the given count.
    """

    def __init__(self, count_elements: int):
        self._count_elements = count_elements
        self._count_state_desc = ValueStateDescriptor(
            "trigger-count-%s" % count_elements, Types.PICKLED_BYTE_ARRAY())
        self._ctx = None  # type: TriggerContext

    def open(self, ctx: TriggerContext):
        self._ctx = ctx

    def on_element(self, element, timestamp, window: W) -> bool:
        count_state = self._ctx.get_partitioned_state(self._count_state_desc)
        count = count_state.value()
        if count is None:
            count = 0
        count += 1
        count_state.update(count)
        if count >= self._count_elements:
            count_state.clear()
            return True
        else:
            return False

    def on_processing_time(self, time: int, window: W) -> bool:
        return False

    def on_event_time(self, time: int, window: W) -> bool:
        return False

    def on_merge(self, window: W, merge_context: TriggerContext):
        merge_context.merge_partitioned_state(self._count_state_desc)

    def clear(self, window: W) -> None:
        self._ctx.get_partitioned_state(self._count_state_desc).clear()
