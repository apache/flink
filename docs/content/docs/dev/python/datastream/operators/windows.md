---
title: "Windows"
weight: 2
type: docs
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Windows

Windows are at the heart of processing infinite streams. Windows split the stream into "buckets" of finite size,
over which we can apply computations. This document focuses on how windowing is performed in Flink and how the
programmer can benefit to the maximum from its offered functionality.

Currently, the widow operation is only supported in *keyed* streams

**Keyed Windows**

    stream
           .key_by(...)
           .window(...)              <-  required: "assigner"
          [.trigger(...)]            <-  optional: "trigger" (else default trigger)
          [.allowed_lateness(...)]   <-  optional: "lateness" (else zero)
           .apply/process()          <-  required: "function"

In the above, the commands in square brackets ([...]) are optional. This reveals that Flink allows you to customize your
windowing logic in many different ways so that it best fits your needs.

## Window Lifecycle

In a nutshell, a window is **created** as soon as the first element that should belong to this window arrives, and the
window is **completely removed** when the time (event or processing time) passes its end timestamp plus the user-specified
`allowed lateness` (see [Allowed Lateness](#allowed-lateness)). Flink guarantees removal only for time-based
windows and not for other types, *e.g.* global windows (see [Window Assigners](#window-assigners)). For example, with an
event-time-based windowing strategy that creates non-overlapping (or tumbling) windows every 5 minutes and has an allowed
lateness of 1 min, Flink will create a new window for the interval between `12:00` and `12:05` when the first element with
a timestamp that falls into this interval arrives, and it will remove it when the watermark passes the `12:06`
timestamp.

In addition, each window will have a `Trigger` (see [Triggers](#triggers)) and a function (`WindowFunction` or `ProcessWindowFunction`)
(see [Window Functions](#window-functions)) attached to it. The function will contain the computation to
be applied to the contents of the window, while the `Trigger` specifies the conditions under which the window is
considered ready for the function to be applied. A triggering policy might be something like "when the number of elements
in the window is more than 4", or "when the watermark passes the end of the window". A trigger can also decide to
purge a window's contents any time between its creation and removal. Purging in this case only refers to the elements
in the window, and *not* the window metadata. This means that new data can still be added to that window.

In the following we go into more detail for each of the components above. We start with the required parts in the above
snippet (see [Keyed Windows](#keyed-windows), [Window Assigner](#window-assigners), and [Window Function](#window-functions)) 
before moving to the optional ones.

## Keyed Windows

The first thing to specify is whether your stream should be keyed or not. This has to be done before defining the window.
Using the `key_by(...)` will split your infinite stream into logical keyed streams. If `key_by(...)` is not called, your
stream is not keyed.

In the case of keyed streams, any attribute of your incoming events can be used as a key
(more details [here]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#keyed-datastream)). Having a keyed stream will
allow your windowed computation to be performed in parallel by multiple tasks, as each logical keyed stream can be processed
independently from the rest. All elements referring to the same key will be sent to the same parallel task.

## Window Assigners

After specifying your stream is keyed, the next step is to define a *window assigner*.
The window assigner defines how elements are assigned to windows. This is done by specifying the `WindowAssigner`
of your choice in the `window(...)` (for *keyed* streams) call.

A `WindowAssigner` is responsible for assigning each incoming element to one or more windows.
You can implement a custom window assigner by extending the `WindowAssigner` class.

Time-based windows have a *start timestamp* (inclusive) and an *end timestamp* (exclusive)
that together describe the size of the window. In code, Flink uses `TimeWindow` when working with
time-based windows which has methods for querying the start- and end-timestamp and also an
additional method `max_timestamp()` that returns the largest allowed timestamp for a given windows.

In the following, we show how to custom a *tumbling windows* assigner. For details of Tumbling Windows, you can
refer to the [the relevant documentation]({{< ref "docs/dev/datastream/operators/windows" >}}#tumbling-windows).

```python
from typing import Tuple, Collection

from pyflink.common.serializer import TypeSerializer
from pyflink.datastream import WindowAssigner, Trigger
from pyflink.datastream.window import TimeWindow, TimeWindowSerializer

class TumblingEventWindowAssigner(WindowAssigner[Tuple, TimeWindow]):

    def __init__(self, size: int, offset: int, is_event_time: bool):
        self._size = size
        self._offset = offset
        self._is_event_time = is_event_time

    def assign_windows(self,
                       element: Tuple,
                       timestamp: int,
                       context: WindowAssigner.WindowAssignerContext) -> Collection[TimeWindow]:
        start = TimeWindow.get_window_start_with_offset(timestamp, self._offset, self._size)
        return [TimeWindow(start, start + self._size)]

    def get_default_trigger(self, env) -> Trigger[Tuple, TimeWindow]:
        return EventTimeTrigger()

    def get_window_serializer(self) -> TypeSerializer[TimeWindow]:
        return TimeWindowSerializer()

    def is_event_time(self) -> bool:
        return False
```

## Window Functions

After defining the window assigner, we need to specify the computation that we want
to perform on each of these windows. This is the responsibility of the *window function*, which is used to process the
elements of each keyed window once the system determines that a window is ready for processing
(see [triggers](#triggers) for how Flink determines when a window is ready).

The window function can be `ProcessWindowFunction` or `WindowFunction`. They get an `Iterable` for all the elements contained in a
window and additional meta information about the window to which the elements belong.

In some places where a `ProcessWindowFunction` can be used you can also use a `WindowFunction`.
This is an older version of ProcessWindowFunction that provides less contextual information and
does not have some advances features, such as per-window keyed state. We will look at examples for each of these variants.

### ProcessWindowFunction

A ProcessWindowFunction gets an Iterable containing all the elements of the window, and a Context
object with access to time and state information, which enables it to provide more flexibility than
other window functions. This comes at the cost of performance and resource consumption, because
elements cannot be incrementally aggregated but instead need to be buffered internally until the
window is considered ready for processing.

The signature of `ProcessWindowFunction` looks as follows:

```python
class ProcessWindowFunction(Function, Generic[IN, OUT, KEY, W]):
    """
    Base interface for functions that are evaluated over keyed (grouped) windows using a context
    for retrieving extra information.
    """

    class Context(ABC, Generic[W2]):
        """
        The context holding window metadata.
        """

        @abstractmethod
        def window(self) -> W2:
            """
            :return: The window that is being evaluated.
            """
            pass

        @abstractmethod
        def current_processing_time(self) -> int:
            """
            :return: The current processing time.
            """
            pass

        @abstractmethod
        def current_watermark(self) -> int:
            """
            :return: The current event-time watermark.
            """
            pass

        @abstractmethod
        def window_state(self) -> KeyedStateStore:
            """
            State accessor for per-key and per-window state.

            .. note::
                If you use per-window state you have to ensure that you clean it up by implementing
                :func:`~ProcessWindowFunction.clear`.

            :return: The :class:`KeyedStateStore` used to access per-key and per-window states.
            """
            pass

        @abstractmethod
        def global_state(self) -> KeyedStateStore:
            """
            State accessor for per-key global state.
            """
            pass

    @abstractmethod
    def process(self,
                key: KEY,
                content: 'ProcessWindowFunction.Context',
                elements: Iterable[IN]) -> Iterable[OUT]:
        """
        Evaluates the window and outputs none or several elements.

        :param key: The key for which this window is evaluated.
        :param content: The context in which the window is being evaluated.
        :param elements: The elements in the window being evaluated.
        :return: The iterable object which produces the elements to emit.
        """
        pass

    @abstractmethod
    def clear(self, context: 'ProcessWindowFunction.Context') -> None:
        """
        Deletes any state in the :class:`Context` when the Window expires (the watermark passes its
        max_timestamp + allowed_lateness).

        :param context: The context to which the window is being evaluated.
        """
        pass
```

The `key` parameter is the key that is extracted
via the `KeySelector` that was specified for the `key_by()` invocation. In case of tuple-index
keys or string-field references this key type is always `Tuple` and you have to manually cast
it to a tuple of the correct size to extract the key fields.

A `ProcessWindowFunction` can be defined and used like this:

```python
from typing import Tuple, Iterable

from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TimeWindow

class SumWindowProcessFunction(ProcessWindowFunction[Tuple, Tuple, str, TimeWindow]):

    def process(self,
                key: str,
                content: ProcessWindowFunction.Context,
                elements: Iterable[Tuple]) -> Iterable[tuple]:
        result = 0
        for i in elements:
            result += i[0]
        return [(key, result)]

    def clear(self, context: ProcessWindowFunction.Context) -> None:
        pass

data_stream = env.from_collection([
    (1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello')],
    type_info=Types.TUPLE([Types.INT(), Types.STRING()]))  # type: DataStream
data_stream.key_by(lambda x: x[1], key_type=Types.STRING()) \
    .window(TumblingEventWindowAssigner()) \
    .process(SumWindowProcessFunction(), Types.TUPLE([Types.STRING(), Types.INT()]))
```
### WindowFunction

In some places where a `ProcessWindowFunction` can be used you can also use a `WindowFunction`. This
is an older version of `ProcessWindowFunction` that provides less contextual information and does
not have some advances features, such as per-window keyed state. This interface will be deprecated
at some point.

The signature of a `WindowFunction` looks as follows:

```python
class WindowFunction(Function, Generic[IN, OUT, KEY, W]):
    """
    Base interface for functions that are evaluated over keyed (grouped) windows.
    """

    @abstractmethod
    def apply(self, key: KEY, window: W, inputs: Iterable[IN]) -> Iterable[OUT]:
        """
        Evaluates the window and outputs none or several elements.

        :param key: The key for which this window is evaluated.
        :param window: The window that is being evaluated.
        :param inputs: The elements in the window being evaluated.
        """
        pass
```

It can be used like this:

```python
from typing import Tuple, Iterable

from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TimeWindow

class SumWindowFunction(WindowFunction[Tuple, Tuple, str, TimeWindow]):

    def apply(self, key: str, window: TimeWindow, inputs: Iterable[Tuple]):
        result = 0
        for i in inputs:
            result += i[0]
        return [(key, result)]

data_stream = env.from_collection([
    (1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello')],
    type_info=Types.TUPLE([Types.INT(), Types.STRING()]))  # type: DataStream
data_stream.key_by(lambda x: x[1], key_type=Types.STRING()) \
    .window(TumblingEventWindowAssigner()) \
    .apply(SumWindowFunction(), Types.TUPLE([Types.STRING(), Types.INT()]))
```

## Triggers

A Trigger determines when a window (as formed by the window assigner) is ready to be processed by the window function.
Each WindowAssigner comes with a default Trigger. You can specify a custom trigger using trigger(...).

The signature of `ProcessWindowFunction` looks as follows:

```python
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
```

Two things to notice about the above methods are:

1) The first three(*on_element*, *on_processing_time* and *on_event_time*) decide how to act on their invocation event by returning a `TriggerResult`.
The action can be one of the following:

* `CONTINUE`: do nothing,
* `FIRE`: trigger the computation,
* `PURGE`: clear the elements in the window, and
* `FIRE_AND_PURGE`: trigger the computation and clear the elements in the window afterwards.

2) Any of these methods can be used to register processing- or event-time timers for future actions.

### Fire and Purge

Once a trigger determines that a window is ready for processing, it fires, *i.e.*, it returns `FIRE` or `FIRE_AND_PURGE`. This is the signal for the window operator
to emit the result of the current window. Given a window with a `ProcessWindowFunction`, all elements are passed to the `ProcessWindowFunction`.

When a trigger fires, it can either `FIRE` or `FIRE_AND_PURGE`. While `FIRE` keeps the contents of the window, `FIRE_AND_PURGE` removes its content.
By default, the pre-implemented triggers simply `FIRE` without purging the window state.

You can implement a custom EventTimeTrigger as follows:

```python
from typing import Tuple
from pyflink.datastream.window import TimeWindow

class EventTimeTrigger(Trigger[Tuple, TimeWindow]):

    def on_element(self,
                   element: Tuple,
                   timestamp: int,
                   window: TimeWindow,
                   ctx: 'Trigger.TriggerContext') -> TriggerResult:
        return TriggerResult.CONTINUE

    def on_processing_time(self,
                           time: int,
                           window: TimeWindow,
                           ctx: 'Trigger.TriggerContext') -> TriggerResult:
        return TriggerResult.CONTINUE

    def on_event_time(self,
                      time: int,
                      window: TimeWindow,
                      ctx: 'Trigger.TriggerContext') -> TriggerResult:
        if time >= window.max_timestamp():
            return TriggerResult.FIRE_AND_PURGE
        else:
            return TriggerResult.CONTINUE

    def on_merge(self,
                 window: TimeWindow,
                 ctx: 'Trigger.OnMergeContext') -> None:
        pass

    def clear(self,
              window: TimeWindow,
              ctx: 'Trigger.TriggerContext') -> None:
        pass
```

## Allowed Lateness

When working with *event-time* windowing, it can happen that elements arrive late, *i.e.* the watermark that Flink uses to
keep track of the progress of event-time is already past the end timestamp of a window to which an element belongs. See
[event time]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}) and especially [late elements]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}#late-elements) for a more thorough
discussion of how Flink deals with event time.

By default, late elements are dropped when the watermark is past the end of the window. However,
Flink allows to specify a maximum *allowed lateness* for window operators. Allowed lateness
specifies by how much time elements can be late before they are dropped, and its default value is 0.
Elements that arrive after the watermark has passed the end of the window but before it passes the end of
the window plus the allowed lateness, are still added to the window. Depending on the trigger used,
a late but not dropped element may cause the window to fire again.

In order to make this work, Flink keeps the state of windows until their allowed lateness expires. Once this happens, Flink removes the window and deletes its state, as
also described in the [Window Lifecycle](#window-lifecycle) section.

By default, the allowed lateness is set to `0`. That is, elements that arrive behind the watermark will be dropped.

You can specify an allowed lateness like this:

```python
data_stream.key_by(<key selector>) \
    .window(<window assigner>) \
    .allowed_lateness(<time>) \
    .<windowed transformation>(<window function>)
```
