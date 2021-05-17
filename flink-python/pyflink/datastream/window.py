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
from abc import ABC, abstractmethod
from enum import Enum
from io import BytesIO
from typing import TypeVar, Generic, Iterable, Collection

from pyflink.common.serializer import TypeSerializer
from pyflink.datastream.functions import RuntimeContext, InternalWindowFunction
from pyflink.datastream.state import StateDescriptor, State
from pyflink.metrics import MetricGroup

__all__ = ['Window',
           'TimeWindow',
           'CountWindow',
           'WindowAssigner',
           'MergingWindowAssigner',
           'TriggerResult',
           'Trigger',
           'TimeWindowSerializer',
           'CountWindowSerializer']

MAX_LONG_VALUE = sys.maxsize


def long_to_int_with_bit_mixing(x: int) -> int:
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
    x = (x ^ (x >> 27)) * 0x94d049bb133111eb
    x = x ^ (x >> 31)
    return x


def mod_inverse(x: int) -> int:
    inverse = x * x * x
    inverse *= 2 - x * inverse
    inverse *= 2 - x * inverse
    inverse *= 2 - x * inverse
    return inverse


class Window(ABC):
    """
    Window is a grouping of elements into finite buckets. Windows have a maximum timestamp
    which means that, at some point, all elements that go into one window will have arrived.
    """

    @abstractmethod
    def max_timestamp(self) -> int:
        pass


class TimeWindow(Window):
    """
    Window that represents a time interval from start (inclusive) to end (exclusive).
    """

    def __init__(self, start: int, end: int):
        super(TimeWindow, self).__init__()
        self.start = start
        self.end = end

    def max_timestamp(self) -> int:
        return self.end - 1

    def intersects(self, other: 'TimeWindow') -> bool:
        """
        Returns True if this window intersects the given window.
        """
        return self.start <= other.end and self.end >= other.start

    def cover(self, other: 'TimeWindow') -> 'TimeWindow':
        """
        Returns the minimal window covers both this window and the given window.
        """
        return TimeWindow(min(self.start, other.start), max(self.end, other.end))

    @staticmethod
    def get_window_start_with_offset(timestamp: int, offset: int, window_size: int):
        """
        Method to get the window start for a timestamp.

        :param timestamp: epoch millisecond to get the window start.
        :param offset: The offset which window start would be shifted by.
        :param window_size: The size of the generated windows.
        :return: window start
        """
        return timestamp - (timestamp - offset + window_size) % window_size

    def __hash__(self):
        return self.start + mod_inverse((self.end << 1) + 1)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.end == other.end \
            and self.start == other.start

    def __lt__(self, other: 'TimeWindow'):
        if not isinstance(other, TimeWindow):
            raise Exception("Does not support comparison with non-TimeWindow %s" % other)

        return self.start == other.start and self.end < other.end or self.start < other.start

    def __le__(self, other: 'TimeWindow'):
        return self.__eq__(other) and self.__lt__(other)

    def __repr__(self):
        return "TimeWindow(start={}, end={})".format(self.start, self.end)


class CountWindow(Window):
    """
    A Window that represents a count window. For each count window, we will assign a unique
    id. Thus this CountWindow can act as namespace part in state. We can attach data to each
    different CountWindow.
    """

    def __init__(self, id: int):
        super(CountWindow, self).__init__()
        self.id = id

    def max_timestamp(self) -> int:
        return MAX_LONG_VALUE

    def __hash__(self):
        return long_to_int_with_bit_mixing(self.id)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.id == other.id

    def __repr__(self):
        return "CountWindow(id={})".format(self.id)


class TimeWindowSerializer(TypeSerializer[TimeWindow]):

    def __init__(self):
        self._underlying_coder = None

    def serialize(self, element: TimeWindow, stream: BytesIO) -> None:
        if self._underlying_coder is None:
            self._underlying_coder = self._get_coder()
        bytes_data = self._underlying_coder.encode_nested(element)
        stream.write(bytes_data)

    def deserialize(self, stream: BytesIO) -> TimeWindow:
        if self._underlying_coder is None:
            self._underlying_coder = self._get_coder()
        bytes_data = stream.read(16)
        return self._underlying_coder.decode_nested(bytes_data)

    def _get_coder(self):
        try:
            from pyflink.fn_execution import coder_impl_fast as coder_impl
        except:
            from pyflink.fn_execution.beam import beam_coder_impl_slow as coder_impl
        return coder_impl.TimeWindowCoderImpl()


class CountWindowSerializer(TypeSerializer[CountWindow]):

    def __init__(self):
        self._underlying_coder = None

    def serialize(self, element: CountWindow, stream: BytesIO) -> None:
        if self._underlying_coder is None:
            self._underlying_coder = self._get_coder()
        bytes_data = self._underlying_coder.encode_nested(element)
        stream.write(bytes_data)

    def deserialize(self, stream: BytesIO) -> CountWindow:
        if self._underlying_coder is None:
            self._underlying_coder = self._get_coder()
        bytes_data = stream.read(8)
        return self._underlying_coder.decode_nested(bytes_data)

    def _get_coder(self):
        try:
            from pyflink.fn_execution import coder_impl_fast as coder_impl
        except:
            from pyflink.fn_execution.beam import beam_coder_impl_slow as coder_impl
        return coder_impl.CountWindowCoderImpl()


T = TypeVar('T')
W = TypeVar('W')
W2 = TypeVar('W2')
IN = TypeVar('IN')
OUT = TypeVar('OUT')
KEY = TypeVar('KEY')


class TriggerResult(Enum):
    """
    Result type for trigger methods. This determines what happens with the window, for example
    whether the window function should be called, or the window should be discarded.

    If a :class:`Trigger` returns TriggerResult.FIRE or TriggerResult.FIRE_AND_PURGE but the window
    does not contain any data the window function will not be invoked, i.e. no data will be produced
    for the window.

      - CONTINUE: No action is taken on the window.
      - FIRE_AND_PURGE: Evaluates the window function and emits the 'window result'.
      - FIRE: On FIRE, the window is evaluated and results are emitted. The window is not purged
              though, all elements are retained.
      - PURGE: All elements in the window are cleared and the window is discarded, without
               evaluating the window function or emitting any elements.
    """
    CONTINUE = (False, False)
    FIRE_AND_PURGE = (True, True)
    FIRE = (True, False)
    PURGE = (False, True)

    def is_fire(self) -> bool:
        return self.value[0]

    def is_purge(self) -> bool:
        return self.value[1]


class Trigger(ABC, Generic[T, W]):
    """
    A Trigger determines when a pane of a window should be evaluated to emit the results for that
    part of the window.

    A pane is the bucket of elements that have the same key (assigned by the KeySelector) and same
    Window. An element can be in multiple panes if it was assigned to multiple windows by the
    WindowAssigner. These panes all have their own instance of the Trigger.

    Triggers must not maintain state internally since they can be re-created or reused for different
    keys. All necessary state should be persisted using the state abstraction available on the
    TriggerContext.

    When used with a MergingWindowAssigner the Trigger must return true from :func:`can_merge` and
    :func:`on_merge` most be properly implemented.
    """

    class TriggerContext(ABC):
        """
        A context object that is given to :class:`Trigger` methods to allow them to register timer
        callbacks and deal with state.
        """

        @abstractmethod
        def get_current_processing_time(self) -> int:
            """
            :return: The current processing time.
            """
            pass

        @abstractmethod
        def get_metric_group(self) -> MetricGroup:
            """
            Returns the metric group for this :class:`Trigger`. This is the same metric group that
            would be returned from
            :func:`~pyflink.datasteam.functions.RuntimeContext.get_metric_group` in a user function.

            :return: The metric group.
            """
            pass

        @abstractmethod
        def get_current_watermark(self) -> int:
            """
            :return: The current watermark time.
            """
            pass

        @abstractmethod
        def register_processing_time_timer(self, time: int) -> None:
            """
            Register a system time callback. When the current system time passes the specified time
            :func:`~Trigger.on_processing_time` is called with the time specified here.

            :param time: The time at which to invoke :func:`~Trigger.on_processing_time`.
            """
            pass

        @abstractmethod
        def register_event_time_timer(self, time: int) -> None:
            """
            Register an event-time callback. When the current watermark passes the specified time
            :func:`~Trigger.on_event_time` is called with the time specified here.

            :param time: The watermark at which to invoke :func:`~Trigger.on_event_time`.
            """
            pass

        @abstractmethod
        def delete_processing_time_timer(self, time: int) -> None:
            """
            Delete the processing time trigger for the given time.
            """
            pass

        @abstractmethod
        def delete_event_time_timer(self, time: int) -> None:
            """
            Delete the event-time trigger for the given time.
            """
            pass

        @abstractmethod
        def get_partitioned_state(self, state_descriptor: StateDescriptor) -> State:
            """
            Retrieves a :class:`State` object that can be used to interact with fault-tolerant state
            that is scoped to the window and key of the current trigger invocation.

            :param state_descriptor: The StateDescriptor that contains the name and type of the
                                     state that is being accessed.
            :return: The partitioned state object.
            """
            pass

    class OnMergeContext(TriggerContext):
        """
        Extension of :class:`TriggerContext` that is given to :func:`~Trigger.on_merge`.
        """

        @abstractmethod
        def merge_partitioned_state(self, state_descriptor: StateDescriptor) -> None:
            pass

    @abstractmethod
    def on_element(self,
                   element: T,
                   timestamp: int,
                   window: W,
                   ctx: 'Trigger.TriggerContext') -> TriggerResult:
        """
        Called for every element that gets added to a pane. The result of this will determine
        whether the pane is evaluated to emit results.

        :param element: The element that arrived.
        :param timestamp: The timestamp of the element that arrived.
        :param window: The window to which the element is being added.
        :param ctx: A context object that can be used to register timer callbacks.
        """
        pass

    @abstractmethod
    def on_processing_time(self,
                           time: int,
                           window: W,
                           ctx: 'Trigger.TriggerContext') -> TriggerResult:
        """
        Called when a processing-time timer that was set using the trigger context fires.

        :param time: The timestamp at which the timer fired.
        :param window: The window for which the timer fired.
        :param ctx: A context object that can be used to register timer callbacks.
        """
        pass

    @abstractmethod
    def on_event_time(self, time: int, window: W, ctx: 'Trigger.TriggerContext') -> TriggerResult:
        """
        Called when an event-time timer that was set using the trigger context fires.

        :param time: The timestamp at which the timer fired.
        :param window: The window for which the timer fired.
        :param ctx: A context object that can be used to register timer callbacks.
        """
        pass

    def can_merge(self) -> bool:
        """
        .. note:: If this returns true you must properly implement :func:`~Trigger.on_merge`

        :return: True if this trigger supports merging of trigger state and can therefore be used
                 with a MergingWindowAssigner.
        """
        return False

    @abstractmethod
    def on_merge(self, window: W, ctx: 'Trigger.OnMergeContext') -> None:
        """
        Called when several windows have been merged into one window by the :class:`WindowAssigner`.

        :param window: The new window that results from the merge.
        :param ctx: A context object that can be used to register timer callbacks and access state.
        """
        pass

    @abstractmethod
    def clear(self, window: W, ctx: 'Trigger.TriggerContext') -> None:
        """
        Clears any state that the trigger might still hold for the given window. This is called when
        a window is purged. Timers set using :func:`~TriggerContext.register_event_time_timer` and
        :func:`~TriggerContext.register_processing_time_timer` should be deleted here as well as
        state acquired using :func:`~TriggerContext.get_partitioned_state`.
        """
        pass


class WindowAssigner(ABC, Generic[T, W]):
    """
    A :class:`WindowAssigner` assigns zero or more :class:`Window` to an element.

    In a window operation, elements are grouped by their key (if available) and by the windows to
    which it was assigned. The set of elements with the same key and window is called a pane. When a
    :class:`Trigger` decides that a certain pane should fire the WindowFunction is applied to
    produce output elements for that pane.
    """

    class WindowAssignerContext(ABC):
        """
        A context provided to the :class:`WindowAssigner` that allows it to query the current
        processing time.
        """

        @abstractmethod
        def get_current_processing_time(self) -> int:
            """
            :return: The current processing time.
            """
            pass

        @abstractmethod
        def get_runtime_context(self) -> RuntimeContext:
            """
            :return: The current runtime context.
            """
            pass

    @abstractmethod
    def assign_windows(self,
                       element: T,
                       timestamp: int,
                       context: 'WindowAssigner.WindowAssignerContext') -> Collection[W]:
        """
        :param element: The element to which windows should be assigned.
        :param timestamp: The timestamp of the element.
        :param context: The :class:`WindowAssignerContext` in which the assigner operates.
        :return: A collection of windows that should be assigned to the element.
        """
        pass

    @abstractmethod
    def get_default_trigger(self, env) -> Trigger[T, W]:
        """
        :param env: The StreamExecutionEnvironment used to compile the DataStream job.
        :return: The default trigger associated with this :class:`WindowAssigner`.
        """
        pass

    @abstractmethod
    def get_window_serializer(self) -> TypeSerializer[W]:
        """
        :return: A :class:`TypeSerializer` for serializing windows that are assigned by this
                 :class:`WindowAssigner`.
        """
        pass

    @abstractmethod
    def is_event_time(self) -> bool:
        """
        :return: True if elements are assigned to windows based on event time, false otherwise.
        """
        pass


class MergingWindowAssigner(WindowAssigner[T, W]):
    """
    A `WindowAssigner` that can merge windows.
    """

    class MergeCallback(ABC, Generic[W2]):
        """
        Callback to be used in :func:`~MergingWindowAssigner.merge_windows` for specifying which
        windows should be merged.
        """

        @abstractmethod
        def merge(self, to_be_merged: Iterable[W2], merge_result: W2) -> None:
            """
            Specifies that the given windows should be merged into the result window.

            :param to_be_merged: The list of windows that should be merged into one window.
            :param merge_result: The resulting merged window.
            """
            pass

    @abstractmethod
    def merge_windows(self,
                      windows: Iterable[W],
                      callback: 'MergingWindowAssigner.MergeCallback[W]') -> None:
        """
        Determines which windows (if any) should be merged.

        :param windows: The window candidates.
        :param callback: A callback that can be invoked to signal which windows should be merged.
        """
        pass


class WindowOperationDescriptor(object):

    def __init__(self,
                 assigner: WindowAssigner,
                 trigger: Trigger,
                 allowed_lateness: int,
                 window_state_descriptor: StateDescriptor,
                 window_serializer: TypeSerializer,
                 internal_window_function: InternalWindowFunction):
        self.assigner = assigner
        self.trigger = trigger
        self.allowed_lateness = allowed_lateness
        self.window_state_descriptor = window_state_descriptor
        self.internal_window_function = internal_window_function
        self.window_serializer = window_serializer
