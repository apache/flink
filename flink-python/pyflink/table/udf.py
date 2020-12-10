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
import collections
import functools
import inspect
from typing import Union, List, Type, Callable, TypeVar, Generic

from pyflink.java_gateway import get_gateway
from pyflink.metrics import MetricGroup
from pyflink.table import Expression
from pyflink.table.types import DataType, _to_java_type, _to_java_data_type
from pyflink.util import utils

__all__ = ['FunctionContext', 'AggregateFunction', 'ScalarFunction', 'TableFunction',
           'udf', 'udtf', 'udaf']


class FunctionContext(object):
    """
    Used to obtain global runtime information about the context in which the
    user-defined function is executed. The information includes the metric group,
    and global job parameters, etc.
    """

    def __init__(self, base_metric_group):
        self._base_metric_group = base_metric_group

    def get_metric_group(self) -> MetricGroup:
        """
        Returns the metric group for this parallel subtask.

        .. versionadded:: 1.11.0
        """
        if self._base_metric_group is None:
            raise RuntimeError("Metric has not been enabled. You can enable "
                               "metric with the 'python.metric.enabled' configuration.")
        return self._base_metric_group


class UserDefinedFunction(abc.ABC):
    """
    Base interface for user-defined function.

    .. versionadded:: 1.10.0
    """

    def open(self, function_context: FunctionContext):
        """
        Initialization method for the function. It is called before the actual working methods
        and thus suitable for one time setup work.

        :param function_context: the context of the function
        :type function_context: FunctionContext
        """
        pass

    def close(self):
        """
        Tear-down method for the user code. It is called after the last call to the main
        working methods.
        """
        pass

    def is_deterministic(self) -> bool:
        """
        Returns information about the determinism of the function's results.
        It returns true if and only if a call to this function is guaranteed to
        always return the same result given the same parameters. true is assumed by default.
        If the function is not pure functional like random(), date(), now(),
        this method must return false.

        :return: the determinism of the function's results.
        """
        return True


class ScalarFunction(UserDefinedFunction):
    """
    Base interface for user-defined scalar function. A user-defined scalar functions maps zero, one,
    or multiple scalar values to a new scalar value.

    .. versionadded:: 1.10.0
    """

    @abc.abstractmethod
    def eval(self, *args):
        """
        Method which defines the logic of the scalar function.
        """
        pass


class TableFunction(UserDefinedFunction):
    """
    Base interface for user-defined table function. A user-defined table function creates zero, one,
    or multiple rows to a new row value.

    .. versionadded:: 1.11.0
    """

    @abc.abstractmethod
    def eval(self, *args):
        """
        Method which defines the logic of the table function.
        """
        pass


T = TypeVar('T')
ACC = TypeVar('ACC')


class AggregateFunction(UserDefinedFunction, Generic[T, ACC]):
    """
    Base interface for user-defined aggregate function. A user-defined aggregate function maps
    scalar values of multiple rows to a new scalar value.

    .. versionadded:: 1.12.0
    """

    @abc.abstractmethod
    def get_value(self, accumulator: ACC) -> T:
        """
        Called every time when an aggregation result should be materialized. The returned value
        could be either an early and incomplete result (periodically emitted as data arrives) or
        the final result of the aggregation.

        :param accumulator: the accumulator which contains the current intermediate results
        :return: the aggregation result
        """
        pass

    @abc.abstractmethod
    def create_accumulator(self) -> ACC:
        """
        Creates and initializes the accumulator for this AggregateFunction.

        :return: the accumulator with the initial value
        """
        pass

    @abc.abstractmethod
    def accumulate(self, accumulator: ACC, *args):
        """
        Processes the input values and updates the provided accumulator instance.

        :param accumulator: the accumulator which contains the current aggregated results
        :param args: the input value (usually obtained from new arrived data)
        """
        pass

    def retract(self, accumulator: ACC, *args):
        """
        Retracts the input values from the accumulator instance.The current design assumes the
        inputs are the values that have been previously accumulated.

        :param accumulator: the accumulator which contains the current aggregated results
        :param args: the input value (usually obtained from new arrived data).
        """
        pass

    def merge(self, accumulator: ACC, accumulators):
        """
        Merges a group of accumulator instances into one accumulator instance. This method must be
        implemented for unbounded session window grouping aggregates and bounded grouping
        aggregates.

        :param accumulator: the accumulator which will keep the merged aggregate results. It should
                            be noted that the accumulator may contain the previous aggregated
                            results. Therefore user should not replace or clean this instance in the
                            custom merge method.
        :param accumulators: a group of accumulators that will be merged.
        """
        pass

    def get_result_type(self) -> DataType:
        """
        Returns the DataType of the AggregateFunction's result.

        :return: The :class:`~pyflink.table.types.DataType` of the AggregateFunction's result.

        """
        pass

    def get_accumulator_type(self) -> DataType:
        """
        Returns the DataType of the AggregateFunction's accumulator.

        :return: The :class:`~pyflink.table.types.DataType` of the AggregateFunction's accumulator.

        """
        pass


class DelegatingScalarFunction(ScalarFunction):
    """
    Helper scalar function implementation for lambda expression and python function. It's for
    internal use only.
    """

    def __init__(self, func):
        self.func = func

    def eval(self, *args):
        return self.func(*args)


class DelegationTableFunction(TableFunction):
    """
    Helper table function implementation for lambda expression and python function. It's for
    internal use only.
    """

    def __init__(self, func):
        self.func = func

    def eval(self, *args):
        return self.func(*args)


class DelegatingPandasAggregateFunction(AggregateFunction):
    """
    Helper pandas aggregate function implementation for lambda expression and python function.
    It's for internal use only.
    """

    def __init__(self, func):
        self.func = func

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        return []

    def accumulate(self, accumulator, *args):
        accumulator.append(self.func(*args))


class PandasAggregateFunctionWrapper(object):
    """
    Wrapper for Pandas Aggregate function.
    """
    def __init__(self, func: AggregateFunction):
        self.func = func

    def open(self, function_context: FunctionContext):
        self.func.open(function_context)

    def eval(self, *args):
        accumulator = self.func.create_accumulator()
        self.func.accumulate(accumulator, *args)
        return self.func.get_value(accumulator)

    def close(self):
        self.func.close()


class UserDefinedFunctionWrapper(object):
    """
    Base Wrapper for Python user-defined function. It handles things like converting lambda
    functions to user-defined functions, creating the Java user-defined function representation,
    etc. It's for internal use only.
    """

    def __init__(self, func, input_types, func_type, deterministic=None, name=None):
        if inspect.isclass(func) or (
                not isinstance(func, UserDefinedFunction) and not callable(func)):
            raise TypeError(
                "Invalid function: not a function or callable (__call__ is not defined): {0}"
                .format(type(func)))

        if input_types is not None:
            from pyflink.table.types import RowType
            if not isinstance(input_types, collections.Iterable) \
                    or isinstance(input_types, RowType):
                input_types = [input_types]

            for input_type in input_types:
                if not isinstance(input_type, DataType):
                    raise TypeError(
                        "Invalid input_type: input_type should be DataType but contains {}".format(
                            input_type))

        self._func = func
        self._input_types = input_types
        self._name = name or (
            func.__name__ if hasattr(func, '__name__') else func.__class__.__name__)

        if deterministic is not None and isinstance(func, UserDefinedFunction) and deterministic \
                != func.is_deterministic():
            raise ValueError("Inconsistent deterministic: {} and {}".format(
                deterministic, func.is_deterministic()))

        # default deterministic is True
        self._deterministic = deterministic if deterministic is not None else (
            func.is_deterministic() if isinstance(func, UserDefinedFunction) else True)
        self._func_type = func_type
        self._judf_placeholder = None

    def __call__(self, *args) -> Expression:
        from pyflink.table import expressions as expr
        return expr.call(self, *args)

    def java_user_defined_function(self):
        if self._judf_placeholder is None:
            gateway = get_gateway()

            def get_python_function_kind():
                JPythonFunctionKind = gateway.jvm.org.apache.flink.table.functions.python. \
                    PythonFunctionKind
                if self._func_type == "general":
                    return JPythonFunctionKind.GENERAL
                elif self._func_type == "pandas":
                    return JPythonFunctionKind.PANDAS
                else:
                    raise TypeError("Unsupported func_type: %s." % self._func_type)

            if self._input_types is not None:
                j_input_types = utils.to_jarray(
                    gateway.jvm.TypeInformation, [_to_java_type(i) for i in self._input_types])
            else:
                j_input_types = None
            j_function_kind = get_python_function_kind()
            func = self._func
            if not isinstance(self._func, UserDefinedFunction):
                func = self._create_delegate_function()

            import cloudpickle
            serialized_func = cloudpickle.dumps(func)
            self._judf_placeholder = \
                self._create_judf(serialized_func, j_input_types, j_function_kind)
        return self._judf_placeholder

    def _create_delegate_function(self) -> UserDefinedFunction:
        pass

    def _create_judf(self, serialized_func, j_input_types, j_function_kind):
        pass


class UserDefinedScalarFunctionWrapper(UserDefinedFunctionWrapper):
    """
    Wrapper for Python user-defined scalar function.
    """

    def __init__(self, func, input_types, result_type, func_type, deterministic, name):
        super(UserDefinedScalarFunctionWrapper, self).__init__(
            func, input_types, func_type, deterministic, name)

        if not isinstance(result_type, DataType):
            raise TypeError(
                "Invalid returnType: returnType should be DataType but is {}".format(result_type))
        self._result_type = result_type
        self._judf_placeholder = None

    def _create_judf(self, serialized_func, j_input_types, j_function_kind):
        gateway = get_gateway()
        j_result_type = _to_java_type(self._result_type)
        PythonScalarFunction = gateway.jvm \
            .org.apache.flink.table.functions.python.PythonScalarFunction
        j_scalar_function = PythonScalarFunction(
            self._name,
            bytearray(serialized_func),
            j_input_types,
            j_result_type,
            j_function_kind,
            self._deterministic,
            _get_python_env())
        return j_scalar_function

    def _create_delegate_function(self) -> UserDefinedFunction:
        return DelegatingScalarFunction(self._func)


class UserDefinedTableFunctionWrapper(UserDefinedFunctionWrapper):
    """
    Wrapper for Python user-defined table function.
    """

    def __init__(self, func, input_types, result_types, deterministic=None, name=None):
        super(UserDefinedTableFunctionWrapper, self).__init__(
            func, input_types, "general", deterministic, name)

        from pyflink.table.types import RowType
        if not isinstance(result_types, collections.Iterable) \
                or isinstance(result_types, RowType):
            result_types = [result_types]

        for result_type in result_types:
            if not isinstance(result_type, DataType):
                raise TypeError(
                    "Invalid result_type: result_type should be DataType but contains {}".format(
                        result_type))

        self._result_types = result_types

    def _create_judf(self, serialized_func, j_input_types, j_function_kind):
        gateway = get_gateway()
        j_result_types = utils.to_jarray(gateway.jvm.TypeInformation,
                                         [_to_java_type(i) for i in self._result_types])
        j_result_type = gateway.jvm.org.apache.flink.api.java.typeutils.RowTypeInfo(j_result_types)
        PythonTableFunction = gateway.jvm \
            .org.apache.flink.table.functions.python.PythonTableFunction
        j_table_function = PythonTableFunction(
            self._name,
            bytearray(serialized_func),
            j_input_types,
            j_result_type,
            j_function_kind,
            self._deterministic,
            _get_python_env())
        return j_table_function

    def _create_delegate_function(self) -> UserDefinedFunction:
        return DelegationTableFunction(self._func)


class UserDefinedAggregateFunctionWrapper(UserDefinedFunctionWrapper):
    """
    Wrapper for Python user-defined aggregate function.
    """
    def __init__(self, func, input_types, result_type, accumulator_type, func_type,
                 deterministic, name):
        super(UserDefinedAggregateFunctionWrapper, self).__init__(
            func, input_types, func_type, deterministic, name)

        if accumulator_type is None and func_type == "general":
            accumulator_type = func.get_accumulator_type()
        if result_type is None:
            result_type = func.get_result_type()
        if not isinstance(result_type, DataType):
            raise TypeError(
                "Invalid returnType: returnType should be DataType but is {}".format(result_type))
        from pyflink.table.types import MapType
        if func_type == 'pandas' and isinstance(result_type, MapType):
            raise TypeError(
                "Invalid returnType: Pandas UDAF doesn't support DataType type {} currently"
                .format(result_type))
        if accumulator_type is not None and not isinstance(accumulator_type, DataType):
            raise TypeError(
                "Invalid accumulator_type: accumulator_type should be DataType but is {}".format(
                    accumulator_type))
        self._result_type = result_type
        self._accumulator_type = accumulator_type

    def _create_judf(self, serialized_func, j_input_types, j_function_kind):
        if self._func_type == "pandas":
            from pyflink.table.types import DataTypes
            self._accumulator_type = DataTypes.ARRAY(self._result_type)

        if j_input_types is not None:
            gateway = get_gateway()
            j_input_types = utils.to_jarray(
                gateway.jvm.DataType, [_to_java_data_type(i) for i in self._input_types])
        j_result_type = _to_java_data_type(self._result_type)
        j_accumulator_type = _to_java_data_type(self._accumulator_type)

        gateway = get_gateway()
        PythonAggregateFunction = gateway.jvm \
            .org.apache.flink.table.functions.python.PythonAggregateFunction
        j_aggregate_function = PythonAggregateFunction(
            self._name,
            bytearray(serialized_func),
            j_input_types,
            j_result_type,
            j_accumulator_type,
            j_function_kind,
            self._deterministic,
            _get_python_env())
        return j_aggregate_function

    def _create_delegate_function(self) -> UserDefinedFunction:
        assert self._func_type == 'pandas'
        return DelegatingPandasAggregateFunction(self._func)


# TODO: support to configure the python execution environment
def _get_python_env():
    gateway = get_gateway()
    exec_type = gateway.jvm.org.apache.flink.table.functions.python.PythonEnv.ExecType.PROCESS
    return gateway.jvm.org.apache.flink.table.functions.python.PythonEnv(exec_type)


def _create_udf(f, input_types, result_type, func_type, deterministic, name):
    return UserDefinedScalarFunctionWrapper(
        f, input_types, result_type, func_type, deterministic, name)


def _create_udtf(f, input_types, result_types, deterministic, name):
    return UserDefinedTableFunctionWrapper(f, input_types, result_types, deterministic, name)


def _create_udaf(f, input_types, result_type, accumulator_type, func_type, deterministic, name):
    return UserDefinedAggregateFunctionWrapper(
        f, input_types, result_type, accumulator_type, func_type, deterministic, name)


def udf(f: Union[Callable, UserDefinedFunction, Type] = None,
        input_types: Union[List[DataType], DataType] = None, result_type: DataType = None,
        deterministic: bool = None, name: str = None, func_type: str = "general",
        udf_type: str = None) -> Union[UserDefinedScalarFunctionWrapper, Callable]:
    """
    Helper method for creating a user-defined function.

    Example:
        ::

            >>> add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT())

            >>> # The input_types is optional.
            >>> @udf(result_type=DataTypes.BIGINT())
            ... def add(i, j):
            ...     return i + j

            >>> class SubtractOne(ScalarFunction):
            ...     def eval(self, i):
            ...         return i - 1
            >>> subtract_one = udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT())

    :param f: lambda function or user-defined function.
    :param input_types: optional, the input data types.
    :param result_type: the result data type.
    :param deterministic: the determinism of the function's results. True if and only if a call to
                          this function is guaranteed to always return the same result given the
                          same parameters. (default True)
    :param name: the function name.
    :param func_type: the type of the python function, available value: general, pandas,
                     (default: general)
    :param udf_type: the type of the python function, available value: general, pandas,
                    (default: general)
    :return: UserDefinedScalarFunctionWrapper or function.

    .. versionadded:: 1.10.0
    """
    if udf_type:
        import warnings
        warnings.warn("The param udf_type is deprecated in 1.12. Use func_type instead.")
        func_type = udf_type

    if func_type not in ('general', 'pandas'):
        raise ValueError("The func_type must be one of 'general, pandas', got %s."
                         % func_type)

    # decorator
    if f is None:
        return functools.partial(_create_udf, input_types=input_types, result_type=result_type,
                                 func_type=func_type, deterministic=deterministic,
                                 name=name)
    else:
        return _create_udf(f, input_types, result_type, func_type, deterministic, name)


def udtf(f: Union[Callable, UserDefinedFunction, Type] = None,
         input_types: Union[List[DataType], DataType] = None,
         result_types: Union[List[DataType], DataType] = None, deterministic: bool = None,
         name: str = None) -> Union[UserDefinedTableFunctionWrapper, Callable]:
    """
    Helper method for creating a user-defined table function.

    Example:
        ::

            >>> # The input_types is optional.
            >>> @udtf(result_types=[DataTypes.BIGINT(), DataTypes.BIGINT()])
            ... def range_emit(s, e):
            ...     for i in range(e):
            ...         yield s, i

            >>> class MultiEmit(TableFunction):
            ...     def eval(self, i):
            ...         return range(i)
            >>> multi_emit = udtf(MultiEmit(), DataTypes.BIGINT(), DataTypes.BIGINT())

    :param f: user-defined table function.
    :param input_types: optional, the input data types.
    :param result_types: the result data types.
    :param name: the function name.
    :param deterministic: the determinism of the function's results. True if and only if a call to
                          this function is guaranteed to always return the same result given the
                          same parameters. (default True)
    :return: UserDefinedTableFunctionWrapper or function.

    .. versionadded:: 1.11.0
    """
    # decorator
    if f is None:
        return functools.partial(_create_udtf, input_types=input_types, result_types=result_types,
                                 deterministic=deterministic, name=name)
    else:
        return _create_udtf(f, input_types, result_types, deterministic, name)


def udaf(f: Union[Callable, UserDefinedFunction, Type] = None,
         input_types: Union[List[DataType], DataType] = None, result_type: DataType = None,
         accumulator_type: DataType = None, deterministic: bool = None, name: str = None,
         func_type: str = "general") -> Union[UserDefinedAggregateFunctionWrapper, Callable]:
    """
    Helper method for creating a user-defined aggregate function.

    Example:
        ::

            >>> # The input_types is optional.
            >>> @udaf(result_type=DataTypes.FLOAT(), func_type="pandas")
            ... def mean_udaf(v):
            ...     return v.mean()

    :param f: user-defined aggregate function.
    :param input_types: optional, the input data types.
    :param result_type: the result data type.
    :param accumulator_type: optional, the accumulator data type.
    :param deterministic: the determinism of the function's results. True if and only if a call to
                          this function is guaranteed to always return the same result given the
                          same parameters. (default True)
    :param name: the function name.
    :param func_type: the type of the python function, available value: general, pandas,
                     (default: general)
    :return: UserDefinedAggregateFunctionWrapper or function.

    .. versionadded:: 1.12.0
    """
    if func_type not in ('general', 'pandas'):
        raise ValueError("The func_type must be one of 'general, pandas', got %s."
                         % func_type)
    # decorator
    if f is None:
        return functools.partial(_create_udaf, input_types=input_types, result_type=result_type,
                                 accumulator_type=accumulator_type, func_type=func_type,
                                 deterministic=deterministic, name=name)
    else:
        return _create_udaf(f, input_types, result_type, accumulator_type, func_type,
                            deterministic, name)
