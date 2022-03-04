################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License") you may not use this file except in compliance
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
import math
from abc import ABC, abstractmethod
from enum import Enum
from io import BytesIO
from typing import TypeVar, Generic, Iterable, Collection, Any
from pyflink.common import TypeSerializer, Time
from pyflink.common.serializer import TypeSerializer
from pyflink.datastream.functions import RuntimeContext, InternalWindowFunction
from pyflink.metrics import MetricGroup
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import StateDescriptor, State, ValueStateDescriptor, \
    ValueState, ReducingStateDescriptor

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

class SessionWindowTimeGapExtractor(ABC):
    """
    Window is a grouping of elements into finite buckets. Windows have a maximum timestamp
    which means that, at some point, all elements that go into one window will have arrived.
    """
    @abstractmethod
    def extract(self, element:Any) -> int:
        """
        Extracts the session time gap.
        Params:
        :param ABC: element – The input element.
        :return: The session time gap in milliseconds.
        """
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
            self._underlying_coder = self._get_coder().get_impl()
        bytes_data = self._underlying_coder.encode(element)
        stream.write(bytes_data)

    def deserialize(self, stream: BytesIO) -> TimeWindow:
        if self._underlying_coder is None:
            self._underlying_coder = self._get_coder().get_impl()
        bytes_data = stream.read(16)
        return self._underlying_coder.decode(bytes_data)

    def _get_coder(self):
        from pyflink.fn_execution import coders
        return coders.TimeWindowCoder()


class CountWindowSerializer(TypeSerializer[CountWindow]):

    def __init__(self):
        self._underlying_coder = None

    def serialize(self, element: CountWindow, stream: BytesIO) -> None:
        if self._underlying_coder is None:
            self._underlying_coder = self._get_coder().get_impl()
        bytes_data = self._underlying_coder.encode(element)
        stream.write(bytes_data)

    def deserialize(self, stream: BytesIO) -> CountWindow:
        if self._underlying_coder is None:
            self._underlying_coder = self._get_coder().get_impl()
        bytes_data = stream.read(8)
        return self._underlying_coder.decode(bytes_data)

    def _get_coder(self):
        from pyflink.fn_execution import coders
        return coders.CountWindowCoder()


T = TypeVar('T')
W = TypeVar('W')
W2 = TypeVar('W2')
IN = TypeVar('IN')
OUT = TypeVar('OUT')
KEY = TypeVar('KEY')

MAX_LONG_VALUE = sys.maxsize
MIN_LONG_VALUE = -MAX_LONG_VALUE - 1

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


class EventTimeTrigger(Trigger[T, TimeWindow]):
    """
    A Trigger that fires once the watermark passes the end of the window to which a pane belongs.
    """
    def on_element(self,
                   element: T,
                   timestamp: int,
                   window: TimeWindow,
                   ctx: 'Trigger.TriggerContext') -> TriggerResult:
        if window.max_timestamp() <= ctx.get_current_watermark():
            return TriggerResult.FIRE
        else:
            ctx.register_event_time_timer(window.max_timestamp())
            # No action is taken on the window.
            return TriggerResult.CONTINUE

    def on_processing_time(self,
                           time: int,
                           window: TimeWindow,
                           ctx: 'Trigger.TriggerContext') -> TriggerResult:
        # No action is taken on the window.
        return TriggerResult.CONTINUE

    def on_event_time(self,
                      time: int,
                      window: TimeWindow,
                      ctx: 'Trigger.TriggerContext') -> TriggerResult:
        if time == window.max_timestamp():
            return TriggerResult.FIRE
        else:
            # No action is taken on the window.
            return TriggerResult.CONTINUE

    def on_merge(self,
                 window: TimeWindow,
                 ctx: 'Trigger.OnMergeContext') -> None:
        window_max_timestamp = window.max_timestamp()
        if window_max_timestamp > ctx.get_current_watermark():
            ctx.register_event_time_timer(window_max_timestamp)

    def can_merge(self) -> bool:
        return True

    def clear(self,
              window: TimeWindow,
              ctx: 'Trigger.TriggerContext') -> None:
        ctx.delete_event_time_timer(window.max_timestamp())


class ProcessingTimeTrigger(Trigger[T, TimeWindow]):
    """
    A Trigger that fires once the current system time passes the end of the window to
    which a pane belongs.
    """
    def on_element(self,
                   element: T,
                   timestamp: int,
                   window: W,
                   ctx: 'Trigger.TriggerContext') -> TriggerResult:
        ctx.register_processing_time_timer(window.max_timestamp())
        return TriggerResult.CONTINUE

    def on_processing_time(self,
                           time: int,
                           window: W,
                           ctx: 'Trigger.TriggerContext') -> TriggerResult:
        return TriggerResult.FIRE

    def on_event_time(self,
                      time: int,
                      window: W,
                      ctx: 'Trigger.TriggerContext') -> TriggerResult:
        return TriggerResult.CONTINUE

    def on_merge(self,
                 window: W,
                 ctx: 'Trigger.OnMergeContext') -> None:
        window_max_timestamp = window.max_timestamp()
        if window_max_timestamp > ctx.get_current_processing_time():
            ctx.register_processing_time_timer(window_max_timestamp)

    def can_merge(self) -> bool:
        return True

    def clear(self,
              window: W,
              ctx: 'Trigger.TriggerContext') -> None:
        ctx.delete_processing_time_timer(window.max_timestamp())


class CountTrigger(Trigger[T, CountWindow]):
    """
    A Trigger that fires once the count of elements in a pane reaches the given count.
    """
    def __init__(self, window_size: int):
        self._window_size = window_size
        self._count_state_descriptor = ReducingStateDescriptor(
            "trigger_counter", lambda a, b: a + b, Types.LONG())

    def on_element(self,
                   element: T,
                   timestamp: int,
                   window: CountWindow,
                   ctx: Trigger.TriggerContext) -> TriggerResult:
        state = ctx.get_partitioned_state(self._count_state_descriptor)  # type: ReducingState
        state.add(1)
        if state.get() >= self._window_size:
            # On FIRE, the window is evaluated and results are emitted. The window is not purged
            #               though, all elements are retained.
            return TriggerResult.FIRE
        else:
            # No action is taken on the window.
            return TriggerResult.CONTINUE

    def on_processing_time(self,
                           time: int,
                           window: CountWindow,
                           ctx: Trigger.TriggerContext) -> TriggerResult:
        # No action is taken on the window.
        return TriggerResult.CONTINUE

    def on_event_time(self,
                      time: int,
                      window: CountWindow,
                      ctx: Trigger.TriggerContext) -> TriggerResult:
        # No action is taken on the window.
        return TriggerResult.CONTINUE

    def on_merge(self, window: CountWindow, ctx: Trigger.OnMergeContext) -> None:
        ctx.merge_partitioned_state(self._count_state_descriptor)

    def can_merge(self) -> bool:
        return True

    def clear(self, window: CountWindow, ctx: Trigger.TriggerContext) -> None:
        state = ctx.get_partitioned_state(self._count_state_descriptor)
        state.clear()


class TumblingWindowAssigner(WindowAssigner[T, TimeWindow]):
    """
    A WindowAssigner that windows elements into windows based on the current system time of the
    machine the operation is running on. Windows cannot overlap.
    For example, in order to window into windows of 1 minute, every 10 seconds:
    ::
            >>> data_stream.key_by(lambda x: x[0], key_type=Types.STRING()) \
            >>> .window(TumblingWindowAssigner(Time.minutes(1), Time.seconds(10), False))

    A WindowAssigner that windows elements into windows based on the timestamp of the elements.
    Windows cannot overlap.
    For example, in order to window into windows of 1 minute:
     ::
            >>> data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            >>> .key_by(lambda x: x[0], key_type=Types.STRING()) \
            >>> .window(TumblingWindowAssigner(60000, 0, True))
    """
    def __init__(self, size: int, offset: int, is_event_time: bool):
        if abs(offset) >= size:
            raise Exception("TumblingWindowAssigner parameters must satisfy abs(offset) < size")

        self._size = size
        self._offset = offset
        self._is_event_time = is_event_time

    def assign_windows(self,
                       element: T,
                       timestamp: int,
                       context: WindowAssigner.WindowAssignerContext) -> Collection[TimeWindow]:
        if self._is_event_time is False:
            current_processing_time = context.get_current_processing_time()
            start = TimeWindow.get_window_start_with_offset(current_processing_time, self._offset, self._size)
            return [TimeWindow(start, start + self._size)]
        else:
            if timestamp > MIN_LONG_VALUE:
                start = TimeWindow.get_window_start_with_offset(timestamp, self._offset, self._size)
                return [TimeWindow(start, start + self._size)]
            else:
                raise Exception("Record has MIN_LONG_VALUE timestamp (= no timestamp marker). "
                            + "Is the time characteristic set to 'ProcessingTime', or did you forget to call "
                            + "'data_stream.assign_timestamps_and_watermarks(...)'?")

    def get_default_trigger(self, env) -> Trigger[T, W]:
        if self._is_event_time is True:
            return EventTimeTrigger()
        else:
            return ProcessingTimeTrigger()

    def get_window_serializer(self) -> TypeSerializer[W]:
        return TimeWindowSerializer()

    def is_event_time(self) -> bool:
        return self._is_event_time

    def __repr__(self):
        return "TumblingWindowAssigner(%s,%s,%s)" % (self._size, self._offset, self.is_event_time)


class CountTumblingWindowAssigner(WindowAssigner[T, CountWindow]):
    """
    A WindowAssigner that windows elements into fixed-size windows based on the count number
    of the elements. Windows cannot overlap.
    """
    def __init__(self, window_size: int):
        """
        Windows this KeyedStream into tumbling count windows.
        :param window_size: The size of the windows in number of elements.
        """
        self._window_size = window_size
        self._counter_state_descriptor = ReducingStateDescriptor(
            "assigner_counter", lambda a, b: a + b, Types.LONG())

    @staticmethod
    def of(window_size: int):
        return CountTumblingWindowAssigner(window_size)

    def assign_windows(self,
                       element: T,
                       timestamp: int,
                       context: 'WindowAssigner.WindowAssignerContext') -> Collection[W]:
        counter = context.get_runtime_context().get_reducing_state(
            self._counter_state_descriptor)
        if counter.get() is None:
            counter.add(0)
        result = [CountWindow(counter.get() // self._window_size)]
        counter.add(1)
        return result

    def get_default_trigger(self, env) -> Trigger[T, W]:
        return CountTrigger(self._window_size)

    def get_window_serializer(self) -> TypeSerializer[W]:
        return CountWindowSerializer()

    def is_event_time(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "CountTumblingWindowAssigner(%s)" % (self._window_size)


class SlidingWindowAssigner(WindowAssigner[T, TimeWindow]):
    """
    A WindowAssigner that windows elements into sliding windows based on the current system time
    of the machine the operation is running on. Windows can possibly overlap.
    For example, in order to window into windows of 1 minute, every 10 seconds:
    ::
            >>> data_stream.key_by(lambda x: x[0], key_type=Types.STRING()) \
            >>> .window(SlidingWindowAssigner(60000, 10000, 0, False))

    A WindowAssigner that windows elements into sliding windows based on the timestamp of the
    elements. Windows can possibly overlap.
    For example, in order to window into windows of 1 minute, every 10 seconds:
    ::
            >>> data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            >>> .key_by(lambda x: x[0], key_type=Types.STRING()) \
            >>> .window(SlidingWindowAssigner(60000, 10000, 0, True))
    """
    def __init__(self, size: int, slide: int, offset: int, is_event_time: bool):
        if abs(offset) >= slide or size<= 0:
            raise Exception("SlidingWindowAssigner parameters must satisfy "
                + "abs(offset) < slide and size > 0")

        self._size = size
        self._slide = slide
        self._offset = offset
        self._is_event_time = is_event_time
        self._pane_size = math.gcd(size, slide)
        self._num_panes_per_window = size // self._pane_size

    def assign_windows(
        self,
        element: T,
        timestamp: int,
        context: 'WindowAssigner.WindowAssignerContext') -> Collection[W]:
        if self._is_event_time is False:
            current_processing_time = context.get_current_processing_time()
            last_start = TimeWindow.get_window_start_with_offset(
                current_processing_time, self._offset, self._slide)
            windows = [TimeWindow(start, start + self._size)
                       for start in range(last_start,
                                          current_processing_time - self._size, -self._slide)]
            return windows
        else:
            if timestamp > MIN_LONG_VALUE:
                last_start = TimeWindow.get_window_start_with_offset(timestamp,
                                                                     self._offset, self._slide)
                windows = [TimeWindow(start, start + self._size)
                           for start in range(last_start, timestamp - self._size, -self._slide)]
                return windows
            else:
                raise Exception("Record has MIN_LONG_VALUE timestamp (= no timestamp marker). "
                                + "Is the time characteristic set to 'ProcessingTime', "
                                  "or did you forget to call "
                                + "'data_stream.assign_timestamps_and_watermarks(...)'?")

    def get_default_trigger(self, env) -> Trigger[T, W]:
        if self._is_event_time is True:
            return EventTimeTrigger()
        else:
            return ProcessingTimeTrigger()

    def get_window_serializer(self) -> TypeSerializer[W]:
        return TimeWindowSerializer()

    def is_event_time(self) -> bool:
        return self._is_event_time

    def __repr__(self) -> str:
        return "SlidingWindowAssigner(%s, %s, %s, %s)" % (
            self._size, self._slide, self._offset, self._is_event_time)


class CountSlidingWindowAssigner(WindowAssigner[T, CountWindow]):
    """
    A WindowAssigner that windows elements into sliding windows based on the count number of
    the elements. Windows can possibly overlap.
    """
    def __init__(self, window_size: int, window_slide: int):
        """
        Windows this KeyedStream into sliding count windows.
        :param window_size: The size of the windows in number of elements.
        :param window_slide: The slide interval in number of elements.
        """
        self._window_size = window_size
        self._window_slide = window_slide
        self._count = None  # type: ValueState
        self._counter_state_descriptor = ReducingStateDescriptor(
            "slide-count-assigner", lambda a, b: a + b, Types.LONG())

    def assign_windows(self,
                       element: T,
                       timestamp: int,
                       context: 'WindowAssigner.WindowAssignerContext') -> Collection[W]:
        count_descriptor = ValueStateDescriptor('slide-count-assigner', Types.LONG())
        self._count = context.get_runtime_context().get_state(count_descriptor)
        count_value = self._count.value()
        if count_value is None:
            current_count = 0
        else:
            current_count = count_value
        self._count.update(current_count + 1)
        last_id = current_count // self._window_slide
        last_start = last_id * self._window_slide
        last_end = last_start + self._window_size - 1
        windows = []
        while last_id >= 0 and last_start <= current_count <= last_end:
            if last_start <= current_count <= last_end:
                windows.append(CountWindow(last_id))
            last_id -= 1
            last_start -= self._window_slide
            last_end -= self._window_slide
        return windows

    def get_default_trigger(self, env) -> Trigger[T, W]:
        return CountTrigger(self._window_size)

    def get_window_serializer(self) -> TypeSerializer[W]:
        return CountWindowSerializer()

    def is_event_time(self) -> bool:
        return False

    def __repr__(self):
        return "CountSlidingWindowAssigner(%s, %s)" % (self._window_size, self._window_slide)


class SessionWindowAssigner(MergingWindowAssigner[T, TimeWindow]):
    """
        WindowAssigner that windows elements into sessions based on the timestamp. Windows cannot
        overlap.
        """
    def __init__(self, session_gap: Time, is_event_time: bool):
        self._session_gap = session_gap.to_milliseconds()
        self._is_event_time = is_event_time

    def merge_windows(self,
                      windows: Iterable[W],
                      callback: 'MergingWindowAssigner.MergeCallback[W]') -> None:
        window_list = [w for w in windows]
        window_list.sort()
        for i in range(1, len(window_list)):
            if window_list[i - 1].end > window_list[i].start:
                callback.merge([window_list[i - 1], window_list[i]],
                               TimeWindow(window_list[i - 1].start, window_list[i].end))

    def assign_windows(self,
                       element: T,
                       timestamp: int,
                       context: 'WindowAssigner.WindowAssignerContext') -> Collection[W]:
        if self._is_event_time is False:
            timestamp = context.get_current_processing_time()

        return [TimeWindow(timestamp, timestamp + self._session_gap)]

    def get_default_trigger(self, env) -> Trigger[T, W]:
        if self._is_event_time is True:
            return EventTimeTrigger()
        else:
            return ProcessingTimeTrigger()

    def get_window_serializer(self) -> TypeSerializer[W]:
        return TimeWindowSerializer()

    def is_event_time(self) -> bool:
        return self._is_event_time

    def __repr__(self):
        return "SessionWindowAssigner(%s, %s)" % (self._session_gap, self._is_event_time)


class DynamicSessionWindowAssigner(MergingWindowAssigner[T, TimeWindow]):
    """
        WindowAssigner that windows elements into sessions based on the timestamp. Windows cannot
        overlap.
        """
    def __init__(self,
                 session_window_time_gap_extractor: SessionWindowTimeGapExtractor,
                 is_event_time: bool):
        self._session_window_time_gap_extractor = session_window_time_gap_extractor
        self._is_event_time = is_event_time

    def merge_windows(self,
                      windows: Iterable[W],
                      callback: 'MergingWindowAssigner.MergeCallback[W]') -> None:
        window_list = [w for w in windows]
        window_list.sort()
        for i in range(1, len(window_list)):
            if window_list[i - 1].end > window_list[i].start:
                callback.merge([window_list[i - 1], window_list[i]],
                               TimeWindow(window_list[i - 1].start, window_list[i].end))

    def assign_windows(self,
                       element: T,
                       timestamp: int,
                       context: 'WindowAssigner.WindowAssignerContext') -> Collection[W]:
        if self._is_event_time is False:
            timestamp = context.get_current_processing_time()
        self._session_gap = self._session_window_time_gap_extractor.extract(element)

        return [TimeWindow(timestamp, timestamp + self._session_gap)]

    def get_default_trigger(self, env) -> Trigger[T, W]:
        if self._is_event_time is True:
            return EventTimeTrigger()
        else:
            return ProcessingTimeTrigger()

    def get_window_serializer(self) -> TypeSerializer[W]:
        return TimeWindowSerializer()

    def is_event_time(self) -> bool:
        return self._is_event_time

    def __repr__(self):
        return "SessionWindowAssigner(%s, %s)" % (self._session_gap, self._is_event_time)


class TumblingProcessingTimeWindows():
    """
    A WindowAssigner that windows elements into windows based on the current system time of
    the machine the operation is running on. Windows cannot overlap.
    For example, in order to window into windows of 1 minute, every 10 seconds:

     in = ...
     keyed = in.keyBy(...)
     windowed =
       keyed.window(TumblingProcessingTimeWindows.of(Time.of(1, MINUTES), Time.of(10, SECONDS))
    """
    @staticmethod
    def of(size: Time, offset = None):
        """
        Creates a new TumblingProcessingTimeWindows WindowAssigner that assigns elements to time
        windows based on the element timestamp and offset.
        For example, if you want window a stream by hour,but window begins at the 15th minutes of
        each hour, you can use of(Time.hours(1),Time.minutes(15)),then you will get time windows
        start at 0:15:00,1:15:00,2:15:00,etc.
        Rather than that,if you are living in somewhere which is not using UTC±00:00 time,
        such as China which is using UTC+08:00,and you want a time window with size of one day,
        and window begins at every 00:00:00 of local time,you may use of(Time.days(1),
        Time.hours(-8)). The parameter of offset is Time.hours(-8))
        since UTC+08:00 is 8 hours earlier than UTC time.
        Params:
            size – The size of the generated windows.
            offset – The offset which window start would be shifted by.
        Returns: The time policy.
        """
        if offset is None:
            return TumblingWindowAssigner(size.to_milliseconds(), 0, False)
        else:
            return TumblingWindowAssigner(size.to_milliseconds(), offset.to_milliseconds(), False)


class TumblingEventTimeWindows():
    """
    A WindowAssigner that windows elements into windows based on the timestamp of the elements.
    Windows cannot overlap.
    For example, in order to window into windows of 1 minute:

     in = ...
     keyed = in.keyBy(...)
     windowed = keyed.window(TumblingEventTimeWindows.of(Time.minutes(1)))
    """

    @staticmethod
    def of(size: Time, offset = None):
        """
        Creates a new TumblingEventTimeWindows WindowAssigner that assigns elements to time
        windows based on the element timestamp and offset.
        For example, if you want window a stream by hour,but window begins at the 15th minutes of
        each hour, you can use of(Time.hours(1),Time.minutes(15)),then you will get time windows
        start at 0:15:00,1:15:00,2:15:00,etc.
        Rather than that,if you are living in somewhere which is not using UTC±00:00 time,
        such as China which is using UTC+08:00,and you want a time window with size of one day,
        and window begins at every 00:00:00 of local time,you may use of(Time.days(1),
        Time.hours(-8)). The parameter of offset is Time.hours(-8)) since UTC+08:00
        is 8 hours earlier than UTC time.
        Params:
        :param size: The size of the generated windows.
        :param offset: The offset which window start would be shifted by.
        """
        if offset is None:
            return TumblingWindowAssigner(size.to_milliseconds(), 0, True)
        else:
            return TumblingWindowAssigner(size.to_milliseconds(), offset.to_milliseconds(), True)


class SlidingProcessingTimeWindows():
    @staticmethod
    def of(size: Time, slide: Time, offset = None):
        """
        Creates a new SlidingProcessingTimeWindows WindowAssigner that assigns elements to time
        windows based on the element timestamp and offset.
        For example, if you want window a stream by hour,but window begins at the 15th minutes of
        each hour, you can use of(Time.hours(1),Time.minutes(15)),then you will get time windows
        start at 0:15:00,1:15:00,2:15:00,etc.
        Rather than that,if you are living in somewhere which is not using UTC±00:00 time,
        such as China which is using UTC+08:00,and you want a time window with size of one day,
        and window begins at every 00:00:00 of local time,you may use of(Time.days(1),
        Time.hours(-8)). The parameter of offset is Time.hours(-8)) since UTC+08:00
        is 8 hours earlier than UTC time.
        Params:
        :param size: The size of the generated windows.
        :param slide: The slide interval of the generated windows.
        :param offset: The offset which window start would be shifted by.
        :return: The time policy.
        """
        if offset is None:
            return SlidingWindowAssigner(size.to_milliseconds(), slide.to_milliseconds(),
                                         0, False)
        else:
            return SlidingWindowAssigner(size.to_milliseconds(), slide.to_milliseconds(),
                                     offset.to_milliseconds(), False)


class SlidingEventTimeWindows():
    @staticmethod
    def of(size: Time, slide: Time, offset = None):
        """
        Creates a new SlidingEventTimeWindows WindowAssigner that assigns elements to time
        windows based on the element timestamp and offset.
        For example, if you want window a stream by hour,but window begins at the 15th minutes of
        each hour, you can use of(Time.hours(1),Time.minutes(15)),then you will get time windows
        start at 0:15:00,1:15:00,2:15:00,etc.
        Rather than that,if you are living in somewhere which is not using UTC±00:00 time, such as
        China which is using UTC+08:00,and you want a time window with size of one day, and window
        begins at every 00:00:00 of local time,you may use of(Time.days(1),Time.hours(-8)).
        The parameter of offset is Time.hours(-8)) since UTC+08:00 is 8 hours earlier than UTC time
        Params:
        :param size: The size of the generated windows.
        :param slide: The slide interval of the generated windows.
        :param offset: The offset which window start would be shifted by.
        :return: The time policy.
        """
        if offset is None:
            return SlidingWindowAssigner(size.to_milliseconds(), slide.to_milliseconds(),
                                        0, True)
        else:
            return SlidingWindowAssigner(size.to_milliseconds(), slide.to_milliseconds(),
                                        offset.to_milliseconds(), True)


class ProcessingTimeSessionWindows():
    """
    A WindowAssigner that windows elements into sessions based on the current processing time.
    Windows cannot overlap.
    For example, in order to window into windows of 1 minute, every 10 seconds:
    in = ...
    keyed = in.keyBy(...)
    windowed = keyed.window(ProcessingTimeSessionWindows.with_gap(Time.minutes(1)))
    """
    @staticmethod
    def with_gap(size: Time):
        """
        Creates a new SessionWindows WindowAssigner that assigns elements to sessions based on
        the element timestamp.
        Params:
        :param size: The session timeout, i.e. the time gap between sessions
        :return: The policy.
        """
        return SessionWindowAssigner(size.to_milliseconds(), False)

    @staticmethod
    def with_dynamic_gap(session_window_time_gap_extractor: SessionWindowTimeGapExtractor):
        """
        Creates a new SessionWindows WindowAssigner that assigns elements to sessions based on the
        element timestamp.
        Params:
        :param session_window_time_gap_extractor: The extractor to use to extract the time gap
            from the input elements
        :return: The policy.
        """
        return DynamicSessionWindowAssigner(session_window_time_gap_extractor, False)


class EventTimeSessionWindows():
    """
    A WindowAssigner that windows elements into sessions based on the timestamp of the elements.
    Windows cannot overlap.
    For example, in order to window into windows of 1 minute, every 10 seconds:
    in = ...
    keyed = in.keyBy(...)
    windowed = keyed.window(EventTimeSessionWindows.with_gap(Time.minutes(1)))
    """
    @staticmethod
    def with_gap(size: Time):
        """
        Creates a new SessionWindows WindowAssigner that assigns elements to sessions
        based on the element timestamp.
        Params:
        :param size: The session timeout, i.e. the time gap between sessions
        :return: The policy.
        """
        return SessionWindowAssigner(size.to_milliseconds(), True)

    @staticmethod
    def with_dynamic_gap(session_window_time_gap_extractor: SessionWindowTimeGapExtractor):
        """
        Creates a new SessionWindows WindowAssigner that assigns elements to sessions based on
        the element timestamp.
        Params:
        :param session_window_time_gap_extractor: The extractor to use to extract the time gap
        from the input elements
        :return: The policy.
        """
        return DynamicSessionWindowAssigner(session_window_time_gap_extractor, True)


class DynamicProcessingTimeSessionWindows():
    """
    A WindowAssigner that windows elements into sessions based on the current processing time.
    Windows cannot overlap.
    For example, in order to window into windows with a dynamic time gap:
    in = ...
    keyed = in.keyBy(...)
    windowed = keyed.window(DynamicProcessingTimeSessionWindows.withDynamicGap({@link
    SessionWindowTimeGapExtractor }))
    Type parameters:
    <T> – The type of the input elements
    """

    @staticmethod
    def with_dynamic_gap(session_window_time_gap_extractor: SessionWindowTimeGapExtractor):
        """
        Creates a new SessionWindows WindowAssigner that assigns elements to sessions based
        on the element timestamp.
        Params:
        :param session_window_time_gap_extractor: The extractor to use to extract the time
        gap from the input elements
        :return: The policy.
        """
        return DynamicSessionWindowAssigner(session_window_time_gap_extractor, False)


class DynamicEventTimeSessionWindows():
    """
    A WindowAssigner that windows elements into sessions based on the timestamp of the elements.
    Windows cannot overlap.
    For example, in order to window into windows with a dynamic time gap:
    in = ...
    keyed = in.keyBy(...)
    windowed = keyed.window(DynamicEventTimeSessionWindows.withDynamicGap({@link
    SessionWindowTimeGapExtractor }))
    Type parameters:
    <T> – The type of the input elements
    """
    @staticmethod
    def with_dynamic_gap(session_window_time_gap_extractor: SessionWindowTimeGapExtractor):
        """
        Creates a new SessionWindows WindowAssigner that assigns elements to sessions
        based on the element timestamp.
        Params:
        :param session_window_time_gap_extractor: The extractor to use to extract the
        time gap from the input elements
        :return: The policy.
        """
        return DynamicSessionWindowAssigner(session_window_time_gap_extractor, True)
