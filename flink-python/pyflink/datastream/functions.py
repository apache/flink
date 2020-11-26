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

import abc
from abc import ABC
from typing import Union, Any, Dict

from py4j.java_gateway import JavaObject

from pyflink.datastream.time_domain import TimeDomain
from pyflink.java_gateway import get_gateway

__all__ = [
    'RuntimeContext',
    'MapFunction',
    'CoMapFunction',
    'FlatMapFunction',
    'CoFlatMapFunction',
    'ReduceFunction',
    'KeySelector',
    'FilterFunction',
    'Partitioner',
    'SourceFunction',
    'SinkFunction',
    'ProcessFunction',
    'Collector',
    'KeyedProcessFunction']


class RuntimeContext(object):
    """
    A RuntimeContext contains information about the context in which functions are executed.
    Each parallel instance of the function will have a context through which it can access
    static contextual information (such as the current parallelism).
    """

    def __init__(self,
                 task_name: str,
                 task_name_with_subtasks: str,
                 number_of_parallel_subtasks: int,
                 max_number_of_parallel_subtasks: int,
                 index_of_this_subtask: int,
                 attempt_number: int,
                 job_parameters: Dict[str, str]):
        self._task_name = task_name
        self._task_name_with_subtasks = task_name_with_subtasks
        self._number_of_parallel_subtasks = number_of_parallel_subtasks
        self._max_number_of_parallel_subtasks = max_number_of_parallel_subtasks
        self._index_of_this_subtask = index_of_this_subtask
        self._attempt_number = attempt_number
        self._job_parameters = job_parameters

    def get_task_name(self) -> str:
        """
        Returns the name of the task in which the UDF runs, as assigned during plan construction.
        """
        return self._task_name

    def get_number_of_parallel_subtasks(self) -> int:
        """
        Gets the parallelism with which the parallel task runs.
        """
        return self._number_of_parallel_subtasks

    def get_max_number_of_parallel_subtasks(self) -> int:
        """
        Gets the number of max-parallelism with which the parallel task runs.
        """
        return self._max_number_of_parallel_subtasks

    def get_index_of_this_subtask(self) -> int:
        """
        Gets the number of this parallel subtask. The numbering starts from 0 and goes up to
        parallelism-1 (parallelism as returned by
        :func:`~RuntimeContext.get_number_of_parallel_subtasks`).
        """
        return self._index_of_this_subtask

    def get_attempt_number(self) -> int:
        """
        Gets the attempt number of this parallel subtask. First attempt is numbered 0.
        """
        return self._attempt_number

    def get_task_name_with_subtasks(self) -> str:
        """
        Returns the name of the task, appended with the subtask indicator, such as "MyTask (3/6)",
        where 3 would be (:func:`~RuntimeContext.get_index_of_this_subtask` + 1), and 6 would be
        :func:`~RuntimeContext.get_number_of_parallel_subtasks`.
        """
        return self._task_name_with_subtasks

    def get_job_parameter(self, key: str, default_value: str):
        """
        Gets the global job parameter value associated with the given key as a string.
        """
        return self._job_parameters[key] if key in self._job_parameters else default_value


class Function(abc.ABC):
    """
    The base class for all user-defined functions.
    """
    def open(self, runtime_context: RuntimeContext):
        pass

    def close(self):
        pass


class MapFunction(Function):
    """
    Base class for Map functions. Map functions take elements and transform them, element wise. A
    Map function always produces a single result element for each input element. Typical
    applications are parsing elements, converting data types, or projecting out fields. Operations
    that produce multiple result elements from a single input element can be implemented using the
    FlatMapFunction.
    The basic syntax for using a MapFunction is as follows:

    ::
        >>> ds = ...
        >>> new_ds = ds.map(MyMapFunction())
    """

    @abc.abstractmethod
    def map(self, value):
        """
        The mapping method. Takes an element from the input data and transforms it into exactly one
        element.

        :param value: The input value.
        :return: The transformed value.
        """
        pass


class CoMapFunction(Function):
    """
    A CoMapFunction implements a map() transformation over two connected streams.

    The same instance of the transformation function is used to transform both of
    the connected streams. That way, the stream transformations can share state.

    The basic syntax for using a CoMapFunction is as follows:

    ::
        >>> ds1 = ...
        >>> ds2 = ...
        >>> new_ds = ds1.connect(ds2).map(MyCoMapFunction())
    """

    @abc.abstractmethod
    def map1(self, value):
        """
        This method is called for each element in the first of the connected streams.

        :param value: The stream element
        :return: The resulting element
        """
        pass

    @abc.abstractmethod
    def map2(self, value):
        """
        This method is called for each element in the second of the connected streams.

        :param value: The stream element
        :return: The resulting element
        """
        pass


class FlatMapFunction(Function):
    """
    Base class for flatMap functions. FlatMap functions take elements and transform them, into zero,
    one, or more elements. Typical applications can be splitting elements, or unnesting lists and
    arrays. Operations that produce multiple strictly one result element per input element can also
    use the MapFunction.
    The basic syntax for using a MapFUnction is as follows:

    ::
        >>> ds = ...
        >>> new_ds = ds.flat_map(MyFlatMapFunction())
    """

    @abc.abstractmethod
    def flat_map(self, value):
        """
        The core mthod of the FlatMapFunction. Takes an element from the input data and transforms
        it into zero, one, or more elements.
        A basic implementation of flat map is as follows:

        ::
                >>> class MyFlatMapFunction(FlatMapFunction):
                >>>     def flat_map(self, value):
                >>>         for i in range(value):
                >>>             yield i

        :param value: The input value.
        :return: A generator
        """
        pass


class CoFlatMapFunction(Function):
    """
    A CoFlatMapFunction implements a flat-map transformation over two connected streams.

    The same instance of the transformation function is used to transform both of the
    connected streams. That way, the stream transformations can share state.

    An example for the use of connected streams would be to apply rules that change over time
    onto elements of a stream. One of the connected streams has the rules, the other stream the
    elements to apply the rules to. The operation on the connected stream maintains the
    current set of rules in the state. It may receive either a rule update (from the first stream)
    and update the state, or a data element (from the second stream) and apply the rules in the
    state to the element. The result of applying the rules would be emitted.

    The basic syntax for using a CoFlatMapFunction is as follows:

    ::
        >>> ds1 = ...
        >>> ds2 = ...

        >>> class MyCoFlatMapFunction(CoFlatMapFunction):
        >>>     def flat_map1(self, value):
        >>>         for i in range(value):
        >>>             yield i
        >>>     def flat_map2(self, value):
        >>>         for i in range(value):
        >>>             yield i

        >>> new_ds = ds1.connect(ds2).flat_map(MyCoFlatMapFunction())
    """

    @abc.abstractmethod
    def flat_map1(self, value):
        """
        This method is called for each element in the first of the connected streams.

        :param value: The input value.
        :return: A generator
        """
        pass

    @abc.abstractmethod
    def flat_map2(self, value):
        """
        This method is called for each element in the second of the connected streams.

        :param value: The input value.
        :return: A generator
        """
        pass


class ReduceFunction(Function):
    """
    Base interface for Reduce functions. Reduce functions combine groups of elements to a single
    value, by taking always two elements and combining them into one. Reduce functions may be
    used on entire data sets, or on grouped data sets. In the latter case, each group is reduced
    individually.

    The basic syntax for using a ReduceFunction is as follows:

    ::
        >>> ds = ...
        >>> new_ds = ds.key_by(lambda x: x[1]).reduce(MyReduceFunction())
    """

    @abc.abstractmethod
    def reduce(self, value1, value2):
        """
        The core method of ReduceFunction, combining two values into one value of the same type.
        The reduce function is consecutively applied to all values of a group until only a single
        value remains.

        :param value1: The first value to combine.
        :param value2: The second value to combine.
        :return: The combined value of both input values.
        """
        pass


class KeySelector(Function):
    """
    The KeySelector allows to use deterministic objects for operations such as reduce, reduceGroup,
    join coGroup, etc. If invoked multiple times on the same object, the returned key must be the
    same. The extractor takes an object an returns the deterministic key for that object.
    """

    @abc.abstractmethod
    def get_key(self, value):
        """
        User-defined function that deterministically extracts the key from an object.

        :param value: The object to get the key from.
        :return: The extracted key.
        """
        pass


class FilterFunction(Function):
    """
    A filter function is a predicate applied individually to each record. The predicate decides
    whether to keep the element, or to discard it.

    The basic syntax for using a FilterFunction is as follows:

    ::
         >>> ds = ...
         >>> result = ds.filter(MyFilterFunction())

    Note that the system assumes that the function does not modify the elements on which the
    predicate is applied. Violating this assumption can lead to incorrect results.
    """

    @abc.abstractmethod
    def filter(self, value):
        """
        The filter function that evaluates the predicate.

        :param value: The value to be filtered.
        :return: True for values that should be retained, false for values to be filtered out.
        """
        pass


class Partitioner(Function):
    """
    Function to implement a custom partition assignment for keys.
    """

    @abc.abstractmethod
    def partition(self, key: Any, num_partitions: int) -> int:
        """
        Computes the partition for the given key.

        :param key: The key.
        :param num_partitions: The number of partitions to partition into.
        :return: The partition index.
        """
        pass


class FunctionWrapper(Function):
    """
    A basic wrapper class for user defined function.
    """

    def __init__(self, func):
        self._func = func


class MapFunctionWrapper(FunctionWrapper):
    """
    A wrapper class for MapFunction. It's used for wrapping up user defined function in a
    MapFunction when user does not implement a MapFunction but directly pass a function object or
    a lambda function to map() function.
    """

    def __init__(self, func):
        """
        The constructor of MapFunctionWrapper.

        :param func: user defined function object.
        """
        super(MapFunctionWrapper, self).__init__(func)

    def map(self, value):
        """
        A delegated map function to invoke user defined function.

        :param value: The input value.
        :return: the return value of user defined map function.
        """
        return self._func(value)


class FlatMapFunctionWrapper(FunctionWrapper):
    """
    A wrapper class for FlatMapFunction. It's used for wrapping up user defined function in a
    FlatMapFunction when user does not implement a FlatMapFunction but directly pass a function
    object or a lambda function to flat_map() function.
    """

    def __init__(self, func):
        """
        The constructor of MapFunctionWrapper.

        :param func: user defined function object.
        """
        super(FlatMapFunctionWrapper, self).__init__(func)

    def flat_map(self, value):
        """
        A delegated flat_map function to invoke user defined function.

        :param value: The input value.
        :return: the return value of user defined flat_map function.
        """
        return self._func(value)


class FilterFunctionWrapper(FunctionWrapper):
    """
    A wrapper class for FilterFunction. It's used for wrapping up user defined function in a
    FilterFunction when user does not implement a FilterFunction but directly pass a function
    object or a lambda function to filter() function.
    """

    def __init__(self, func):
        super(FilterFunctionWrapper, self).__init__(func)

    def filter(self, value):
        return self._func(value)


class ReduceFunctionWrapper(FunctionWrapper):
    """
    A wrapper class for ReduceFunction. It's used for wrapping up user defined function in a
    ReduceFunction when user does not implement a ReduceFunction but directly pass a function
    object or a lambda function to reduce() function.
    """

    def __init__(self, func):
        """
        The constructor of ReduceFunctionWrapper.

        :param func: user defined function object.
        """
        super(ReduceFunctionWrapper, self).__init__(func)

    def reduce(self, value1, value2):
        """
        A delegated reduce function to invoke user defined function.

        :param value1: The first value to combine.
        :param value2: The second value to combine.
        :return: The combined value of both input values.
        """
        return self._func(value1, value2)


class KeySelectorFunctionWrapper(FunctionWrapper):
    """
    A wrapper class for KeySelector. It's used for wrapping up user defined function in a
    KeySelector when user does not implement a KeySelector but directly pass a function
    object or a lambda function to key_by() function.
    """

    def __init__(self, func):
        """
        The constructor of MapFunctionWrapper.

        :param func: user defined function object.
        """
        super(KeySelectorFunctionWrapper, self).__init__(func)

    def get_key(self, value):
        """
        A delegated get_key function to invoke user defined function.

        :param value: The input value.
        :return: the return value of user defined get_key function.
        """
        return self._func(value)


class PartitionerFunctionWrapper(FunctionWrapper):
    """
    A wrapper class for Partitioner. It's used for wrapping up user defined function in a
    Partitioner when user does not implement a Partitioner but directly pass a function
    object or a lambda function to partition_custom() function.
    """

    def __init__(self, func):
        """
        The constructor of PartitionerFunctionWrapper.

        :param func: user defined function object.
        """
        super(PartitionerFunctionWrapper, self).__init__(func)

    def partition(self, key: Any, num_partitions: int) -> int:
        """
        A delegated partition function to invoke user defined function.

        :param key: The key.
        :param num_partitions: The number of partitions to partition into.
        :return: The partition index.
        """
        return self._func(key, num_partitions)


def _get_python_env():
    """
    An util function to get a python user defined function execution environment.
    """
    gateway = get_gateway()
    exec_type = gateway.jvm.org.apache.flink.table.functions.python.PythonEnv.ExecType.PROCESS
    return gateway.jvm.org.apache.flink.table.functions.python.PythonEnv(exec_type)


class JavaFunctionWrapper(object):
    """
    A wrapper class that maintains a Function implemented in Java.
    """

    def __init__(self, j_function: Union[str, JavaObject]):
        if isinstance(j_function, str):
            j_func_class = get_gateway().jvm.__getattr__(j_function)
            j_function = j_func_class()
        self._j_function = j_function

    def get_java_function(self):
        return self._j_function


class SourceFunction(JavaFunctionWrapper):
    """
    Base class for all stream data source in Flink.
    """

    def __init__(self, source_func: Union[str, JavaObject]):
        """
        Constructor of SinkFunction.

        :param source_func: The java SourceFunction object.
        """
        super(SourceFunction, self).__init__(source_func)


class SinkFunction(JavaFunctionWrapper):
    """
    The base class for SinkFunctions.
    """

    def __init__(self, sink_func: Union[str, JavaObject]):
        """
        Constructor of SinkFunction.

        :param sink_func: The java SinkFunction object or the full name of the SinkFunction class.
        """
        super(SinkFunction, self).__init__(sink_func)


class Collector(abc.ABC):
    """
    Collects a record and forwards it.
    """
    @abc.abstractmethod
    def collect(self, value):
        """
        Emits a record.

        :param value: The record to collect.
        """
        pass


class TimerService(abc.ABC):
    """
    Interface for working with time and timers.
    """

    @abc.abstractmethod
    def current_processing_time(self):
        """
        Returns the current processing time.
        """
        pass

    @abc.abstractmethod
    def current_watermark(self):
        """
        Returns the current event-time watermark.
        """
        pass

    @abc.abstractmethod
    def register_processing_time_timer(self, time: int):
        """
        Registers a timer to be fired when processing time passes the given time.

        Timers can internally be scoped to keys and/or windows. When you set a timer in a keyed
        context, such as in an operation on KeyedStream then that context will so be active when you
        receive the timer notification.

        :param time: The processing time of the timer to be registered.
        """
        pass

    @abc.abstractmethod
    def register_event_time_timer(self, time: int):
        """
        Registers a timer tobe fired when the event time watermark passes the given time.

        Timers can internally be scoped to keys and/or windows. When you set a timer in a keyed
        context, such as in an operation on KeyedStream then that context will so be active when you
        receive the timer notification.

        :param time: The event time of the timer to be registered.
        """
        pass

    def delete_processing_time_timer(self, time: int):
        """
        Deletes the processing-time timer with the given trigger time. This method has only an
        effect if such a timer was previously registered and did not already expire.

        Timers can internally be scoped to keys and/or windows. When you delete a timer, it is
        removed from the current keyed context.

        :param time: The given trigger time of timer to be deleted.
        """
        pass

    def delete_event_time_timer(self, time: int):
        """
        Deletes the event-time timer with the given trigger time. This method has only an effect if
        such a timer was previously registered and did not already expire.

        Timers can internally be scoped to keys and/or windows. When you delete a timer, it is
        removed from the current keyed context.

        :param time: The given trigger time of timer to be deleted.
        """
        pass


class ProcessFunction(Function):
    """
    A function that process elements of a stream.

    For every element in the input stream process_element(value, ctx, out) is invoked. This can
    produce zero or more elements as output. Implementations can also query the time and set timers
    through the provided Context. For firing timers on_timer(long, ctx, out) will be invoked. This
    can again produce zero or more elements as output and register further timers.

    Note that access to keyed state and timers (which are also scoped to a key) is only available if
    the ProcessFunction is applied on a KeyedStream.
    """

    class Context(abc.ABC):
        """
        Information available in an invocation of process_element(value, ctx, out) or
        on_timer(value, ctx, out).
        """

        @abc.abstractmethod
        def timer_service(self) -> TimerService:
            """
            A Timer service for querying time and registering timers.
            """
            pass

        @abc.abstractmethod
        def timestamp(self) -> int:
            """
            Timestamp of the element currently being processed or timestamp of a firing timer.

            This might be None, for example if the time characteristic of your program is set to
            TimeCharacteristic.ProcessTime.
            """
            pass

    @abc.abstractmethod
    def process_element(self, value, ctx: 'ProcessFunction.Context', out: Collector):
        """
        Process one element from the input stream.

        This function can output zero or more elements using the Collector parameter and also update
        internal state or set timers using the Context parameter.

        :param value: The input value.
        :param ctx:  A Context that allows querying the timestamp of the element and getting a
                     TimerService for registering timers and querying the time. The context is only
                     valid during the invocation of this method, do not store it.
        :param out: The collector for returning result values.
        """
        pass


class KeyedProcessFunction(Function, ABC):
    """
    A keyed function processes elements of a stream.

    For every element in the input stream, process_element() is invoked. This can produce zero or
    more elements as output. Implementations can also query the time and set timers through the
    provided Context. For firing timers on_timer() will be invoked. This can again produce zero or
    more elements as output and register further timers.

    Note that access to keyed state and timers (which are also scoped to a key) is only available if
    the KeyedProcessFunction is applied on a KeyedStream.
    """

    """
    A function that process elements of a stream.

    For every element in the input stream process_element(value, ctx, out) is invoked. This can
    produce zero or more elements as output. Implementations can also query the time and set timers
    through the provided Context. For firing timers on_timer(long, ctx, out) will be invoked. This
    can again produce zero or more elements as output and register further timers.

    Note that access to keyed state and timers (which are also scoped to a key) is only available if
    the ProcessFunction is applied on a KeyedStream.
    """

    class Context(ABC):

        @abc.abstractmethod
        def get_current_key(self):
            pass

        @abc.abstractmethod
        def timer_service(self) -> TimerService:
            """
            A Timer service for querying time and registering timers.
            """
            pass

        @abc.abstractmethod
        def timestamp(self) -> int:
            """
            Timestamp of the element currently being processed or timestamp of a firing timer.

            This might be None, for example if the time characteristic of your program is set to
            TimeCharacteristic.ProcessTime.
            """
            pass

    class OnTimerContext(Context):

        @abc.abstractmethod
        def time_domain(self) -> TimeDomain:
            """
            The TimeDomain of the firing timer.
            :return: The TimeDomain of current fired timer.
            """
            pass

    @abc.abstractmethod
    def process_element(self, value, ctx: 'KeyedProcessFunction.Context', out: Collector):
        """
        Process one element from the input stream.

        This function can output zero or more elements using the Collector parameter and also update
        internal state or set timers using the Context parameter.

        :param value: The input value.
        :param ctx:  A Context that allows querying the timestamp of the element and getting a
                     TimerService for registering timers and querying the time. The context is only
                     valid during the invocation of this method, do not store it.
        :param out: The collector for returning result values.
        """
        pass

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext', out: Collector):
        """
        Called when a timer set using TimerService fires.

        :param timestamp: The timestamp of the firing timer.
        :param ctx: An OnTimerContext that allows querying the timestamp of the firing timer,
                    querying the TimeDomain of the firing timer and getting a TimerService for
                    registering timers and querying the time. The context is only valid during the
                    invocation of this method, do not store it.
        :param out: The collector for returning result values.
        """
        pass
