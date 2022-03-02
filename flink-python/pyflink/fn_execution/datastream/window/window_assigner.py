import math
from typing import Iterable, Collection

from pyflink.common import TypeSerializer
from pyflink.common.typeinfo import Types
from pyflink.datastream import Trigger
from pyflink.datastream.state import ValueStateDescriptor, ValueState, ReducingStateDescriptor
from pyflink.datastream.window import TimeWindow, CountWindow, WindowAssigner, T, TimeWindowSerializer, TriggerResult, \
    CountWindowSerializer, MergingWindowAssigner
from pyflink.fn_execution.table.window_context import W


class EventTimeTrigger(Trigger[object, TimeWindow]):
    """
    A Trigger that fires once the watermark passes the end of the window to which a pane belongs.
    """
    def on_element(self,
                   element: object,
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
        windowMaxTimestamp = window.max_timestamp()
        if windowMaxTimestamp >= ctx.get_current_watermark():
            ctx.register_event_time_timer(windowMaxTimestamp)

    def clear(self,
              window: TimeWindow,
              ctx: 'Trigger.TriggerContext') -> None:
        ctx.delete_event_time_timer(window.max_timestamp())


class ProcessingTimeTrigger(Trigger[object, TimeWindow]):
    """
    A Trigger that fires once the current system time passes the end of the window to which a pane belongs.
    """
    def on_element(self,
                   element: T,
                   timestamp: int,
                   window: W,
                   ctx: 'Trigger.TriggerContext') -> TriggerResult:
        ctx.register_processing_time_timer(window.max_timestamp());
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
        windowMaxTimestamp = window.max_timestamp();
        if windowMaxTimestamp > ctx.get_current_processing_time():
            ctx.register_processing_time_timer(windowMaxTimestamp)

    def clear(self,
              window: W,
              ctx: 'Trigger.TriggerContext') -> None:
        ctx.delete_processing_time_timer(window.max_timestamp());


class CountTrigger(Trigger[object, CountWindow]):
    """
    A Trigger that fires once the count of elements in a pane reaches the given count.
    """
    def __init__(self, window_size: int):
        self._window_size = int(window_size)
        self._count_state_descriptor = ReducingStateDescriptor(
            "trigger_counter", lambda a, b: a + b, Types.BIG_INT())

    def on_element(self,
                   element: object,
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

    def clear(self, window: CountWindow, ctx: Trigger.TriggerContext) -> None:
        state = ctx.get_partitioned_state(self._count_state_descriptor)
        state.clear()


class TumblingWindowAssigner(WindowAssigner[object, TimeWindow]):
    """
    A WindowAssigner that windows elements into windows based on the current system time of the machine the operation is running on. Windows cannot overlap.
    For example, in order to window into windows of 1 minute, every 10 seconds:
    ::
            >>> data_stream.key_by(lambda x: x[0], key_type=Types.STRING()) \
            >>> .window(TumblingWindowAssigner(60000, 10000, False))

    A WindowAssigner that windows elements into windows based on the timestamp of the elements. Windows cannot overlap.
    For example, in order to window into windows of 1 minute:
     ::
            >>> data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            >>> .key_by(lambda x: x[0], key_type=Types.STRING()) \
            >>> .window(TumblingWindowAssigner(60000, 0, True))
    """
    def __init__(self, size: int, offset: int, is_event_time: bool):
        self._size = size
        self._offset = offset
        self._is_event_time = is_event_time

    def assign_windows(self,
                       element: object,
                       timestamp: int,
                       context: WindowAssigner.WindowAssignerContext) -> Collection[TimeWindow]:
        if self._is_event_time is False:
            timestamp = context.get_current_processing_time()

        start = TimeWindow.get_window_start_with_offset(timestamp, self._offset, self._size)
        return [TimeWindow(start, start + self._size)]

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

'''
A WindowAssigner that windows elements into windows based on the number of the elements. Windows cannot overlap.
'''
class CountTumblingWindowAssigner(WindowAssigner[object, CountWindow]):
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
            "assigner_counter", lambda a, b: a + b, Types.BIG_INT())

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


class SlidingWindowAssigner(WindowAssigner[object, TimeWindow]):
    """
    A WindowAssigner that windows elements into sliding windows based on the current system time of the machine the operation is running on. Windows can possibly overlap.
    For example, in order to window into windows of 1 minute, every 10 seconds:
    ::
            >>> data_stream.key_by(lambda x: x[0], key_type=Types.STRING()) \
            >>> .window(SlidingWindowAssigner(60000, 10000, 0, False))

    A WindowAssigner that windows elements into sliding windows based on the timestamp of the elements. Windows can possibly overlap.
    For example, in order to window into windows of 1 minute, every 10 seconds:
    ::
            >>> data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            >>> .key_by(lambda x: x[0], key_type=Types.STRING()) \
            >>> .window(SlidingWindowAssigner(60000, 10000, 0, True))
    """
    def __init__(self, size: int, slide: int, offset: int, is_event_time: bool):
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
            timestamp = context.get_current_processing_time()

        last_start = TimeWindow.get_window_start_with_offset(timestamp, self._offset, self._slide)
        windows = [TimeWindow(start, start + self._size)
                   for start in range(last_start, timestamp - self._size, -self._slide)]
        return windows

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
        return "SlidingWindowAssigner(%s, %s, %s, %s)" % (self._size, self._slide, self._offset, self._is_event_time)


class CountSlidingWindowAssigner(WindowAssigner[object, CountWindow]):
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
            "slide-count-assigner", lambda a, b: a + b, Types.BIG_INT())

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


class SessionWindowAssigner(MergingWindowAssigner[object, TimeWindow]):
    """
        WindowAssigner that windows elements into sessions based on the timestamp. Windows cannot
        overlap.
        """
    def __init__(self, session_gap: int, is_event_time: bool):
        self._session_gap = session_gap
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
