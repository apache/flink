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

from pyflink.java_gateway import get_gateway
from pyflink.metrics import MetricGroup
from pyflink.table.types import DataType, _to_java_type
from pyflink.util import utils

__all__ = ['FunctionContext', 'ScalarFunction', 'TableFunction', 'udf', 'udtf']


class FunctionContext(object):
    """
    Used to obtain global runtime information about the context in which the
    user-defined function is executed. The information includes the metric group,
    and global job parameters, etc.
    """

    def __init__(self, base_metric_group):
        self._base_metric_group = base_metric_group

    def get_metric_group(self) -> MetricGroup:
        return self._base_metric_group


class UserDefinedFunction(abc.ABC):
    """
    Base interface for user-defined function.
    """

    def open(self, function_context):
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

    def is_deterministic(self):
        """
        Returns information about the determinism of the function's results.
        It returns true if and only if a call to this function is guaranteed to
        always return the same result given the same parameters. true is assumed by default.
        If the function is not pure functional like random(), date(), now(),
        this method must return false.

        :return: the determinism of the function's results.
        :rtype: bool
        """
        return True


class ScalarFunction(UserDefinedFunction):
    """
    Base interface for user-defined scalar function. A user-defined scalar functions maps zero, one,
    or multiple scalar values to a new scalar value.
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
    """

    @abc.abstractmethod
    def eval(self, *args):
        """
        Method which defines the logic of the table function.
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


class UserDefinedFunctionWrapper(object):
    """
    Base Wrapper for Python user-defined function. It handles things like converting lambda
    functions to user-defined functions, creating the Java user-defined function representation,
    etc. It's for internal use only.
    """

    def __init__(self, func, input_types, deterministic=None, name=None):
        if inspect.isclass(func) or (
                not isinstance(func, UserDefinedFunction) and not callable(func)):
            raise TypeError(
                "Invalid function: not a function or callable (__call__ is not defined): {0}"
                .format(type(func)))

        if not isinstance(input_types, collections.Iterable):
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

    def java_user_defined_function(self):
        pass


class UserDefinedScalarFunctionWrapper(UserDefinedFunctionWrapper):
    """
    Wrapper for Python user-defined scalar function.
    """

    def __init__(self, func, input_types, result_type, udf_type, deterministic, name):
        super(UserDefinedScalarFunctionWrapper, self).__init__(
            func, input_types, deterministic, name)

        if not isinstance(result_type, DataType):
            raise TypeError(
                "Invalid returnType: returnType should be DataType but is {}".format(result_type))
        self._result_type = result_type
        self._udf_type = udf_type
        self._judf_placeholder = None

    def java_user_defined_function(self):
        if self._judf_placeholder is None:
            self._judf_placeholder = self._create_judf()
        return self._judf_placeholder

    def _create_judf(self):
        gateway = get_gateway()

        def get_python_function_kind(udf_type):
            JPythonFunctionKind = gateway.jvm.org.apache.flink.table.functions.python.\
                PythonFunctionKind
            if udf_type == "general":
                return JPythonFunctionKind.GENERAL
            elif udf_type == "pandas":
                return JPythonFunctionKind.PANDAS
            else:
                raise TypeError("Unsupported udf_type: %s." % udf_type)

        func = self._func
        if not isinstance(self._func, UserDefinedFunction):
            func = DelegatingScalarFunction(self._func)

        import cloudpickle
        serialized_func = cloudpickle.dumps(func)

        j_input_types = utils.to_jarray(gateway.jvm.TypeInformation,
                                        [_to_java_type(i) for i in self._input_types])
        j_result_type = _to_java_type(self._result_type)
        j_function_kind = get_python_function_kind(self._udf_type)
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


class UserDefinedTableFunctionWrapper(UserDefinedFunctionWrapper):
    """
    Wrapper for Python user-defined table function.
    """

    def __init__(self, func, input_types, result_types, deterministic=None, name=None):
        super(UserDefinedTableFunctionWrapper, self).__init__(
            func, input_types, deterministic, name)

        if not isinstance(result_types, collections.Iterable):
            result_types = [result_types]

        for result_type in result_types:
            if not isinstance(result_type, DataType):
                raise TypeError(
                    "Invalid result_type: result_type should be DataType but contains {}".format(
                        result_type))

        self._result_types = result_types
        self._judtf_placeholder = None

    def java_user_defined_function(self):
        if self._judtf_placeholder is None:
            self._judtf_placeholder = self._create_judtf()
        return self._judtf_placeholder

    def _create_judtf(self):
        func = self._func
        if not isinstance(self._func, UserDefinedFunction):
            func = DelegationTableFunction(self._func)

        import cloudpickle
        serialized_func = cloudpickle.dumps(func)

        gateway = get_gateway()
        j_input_types = utils.to_jarray(gateway.jvm.TypeInformation,
                                        [_to_java_type(i) for i in self._input_types])

        j_result_types = utils.to_jarray(gateway.jvm.TypeInformation,
                                         [_to_java_type(i) for i in self._result_types])
        j_result_type = gateway.jvm.org.apache.flink.api.java.typeutils.RowTypeInfo(j_result_types)
        j_function_kind = gateway.jvm.org.apache.flink.table.functions.python. \
            PythonFunctionKind.GENERAL
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


# TODO: support to configure the python execution environment
def _get_python_env():
    gateway = get_gateway()
    exec_type = gateway.jvm.org.apache.flink.table.functions.python.PythonEnv.ExecType.PROCESS
    return gateway.jvm.org.apache.flink.table.functions.python.PythonEnv(exec_type)


def _create_udf(f, input_types, result_type, udf_type, deterministic, name):
    return UserDefinedScalarFunctionWrapper(
        f, input_types, result_type, udf_type, deterministic, name)


def _create_udtf(f, input_types, result_types, deterministic, name):
    return UserDefinedTableFunctionWrapper(f, input_types, result_types, deterministic, name)


def udf(f=None, input_types=None, result_type=None, deterministic=None, name=None,
        udf_type="general"):
    """
    Helper method for creating a user-defined function.

    Example:
        ::

            >>> add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT())

            >>> @udf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()],
            ...      result_type=DataTypes.BIGINT())
            ... def add(i, j):
            ...     return i + j

            >>> class SubtractOne(ScalarFunction):
            ...     def eval(self, i):
            ...         return i - 1
            >>> subtract_one = udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT())

    :param f: lambda function or user-defined function.
    :type f: function or UserDefinedFunction or type
    :param input_types: the input data types.
    :type input_types: list[DataType] or DataType
    :param result_type: the result data type.
    :type result_type: DataType
    :param deterministic: the determinism of the function's results. True if and only if a call to
                          this function is guaranteed to always return the same result given the
                          same parameters. (default True)
    :type deterministic: bool
    :param name: the function name.
    :type name: str
    :param udf_type: the type of the python function, available value: general, pandas,
                     (default: general)
    :type udf_type: str
    :return: UserDefinedScalarFunctionWrapper or function.
    :rtype: UserDefinedScalarFunctionWrapper or function
    """
    if udf_type not in ('general', 'pandas'):
        raise ValueError("The udf_type must be one of 'general, pandas', got %s." % udf_type)

    # decorator
    if f is None:
        return functools.partial(_create_udf, input_types=input_types, result_type=result_type,
                                 udf_type=udf_type, deterministic=deterministic, name=name)
    else:
        return _create_udf(f, input_types, result_type, udf_type, deterministic, name)


def udtf(f=None, input_types=None, result_types=None, deterministic=None, name=None):
    """
    Helper method for creating a user-defined table function.

    Example:
        ::

            >>> @udtf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()],
            ...      result_types=[DataTypes.BIGINT(), DataTypes.BIGINT()])
            ... def range_emit(s, e):
            ...     for i in range(e):
            ...         yield s, i

            >>> class MultiEmit(TableFunction):
            ...     def eval(self, i):
            ...         return range(i)
            >>> multi_emit = udtf(MultiEmit(), DataTypes.BIGINT(), DataTypes.BIGINT())

    :param f: user-defined table function.
    :type f: function or UserDefinedFunction or type
    :param input_types: the input data types.
    :type input_types: list[DataType] or DataType
    :param result_types: the result data types.
    :type result_types: list[DataType] or DataType
    :param name: the function name.
    :type name: str
    :param deterministic: the determinism of the function's results. True if and only if a call to
                          this function is guaranteed to always return the same result given the
                          same parameters. (default True)
    :type deterministic: bool
    :return: UserDefinedTableFunctionWrapper or function.
    :rtype: UserDefinedTableFunctionWrapper or function
    """
    # decorator
    if f is None:
        return functools.partial(_create_udtf, input_types=input_types, result_types=result_types,
                                 deterministic=deterministic, name=name)
    else:
        return _create_udtf(f, input_types, result_types, deterministic, name)
