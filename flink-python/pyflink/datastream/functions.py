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
from typing import Union

from py4j.java_gateway import JavaObject

from pyflink.java_gateway import get_gateway


class Function(abc.ABC):
    """
    The base class for all user-defined functions.
    """
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


class FlatMapFunction(Function):
    """
    Base class for flatMap functions. FLatMap functions take elements and transform them, into zero,
    one, or more elements. Typical applications can be splitting elements, or unesting lists and
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
        :return: A genertaor
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


class FunctionWrapper(object):
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
