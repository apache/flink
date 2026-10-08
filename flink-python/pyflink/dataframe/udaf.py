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

"""User-defined aggregate functions for the DataFrame API."""

import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Tuple, Type, Union, cast, overload

from pyflink.dataframe.datatype import DataType
from pyflink.dataframe.udf import (
    _DataTypeLike,
    _UDFDeclarationContext,
    _convert_to_dtype,
    _create_declaration_context,
    _create_result_normalizer,
    _infer_return_dtype,
    _resolve_udf,
    _validate_determinism_agreement,
    _validate_zero_argument_class,
)
from pyflink.table.expression import Expression
from pyflink.table.types import DataType as TableDataType, MapType
from pyflink.table.udf import (
    AggregateFunction,
    DelegatingPandasAggregateFunction,
    UserDefinedAggregateFunctionWrapper,
    UserDefinedFunction,
    udaf as table_udaf,
)
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ["udaf"]

_UDAFInput = Union[Callable[..., Any], AggregateFunction, Type]


@dataclass(frozen=True)
class _DataFrameUDAFWrapper:
    _func: _UDAFInput
    return_dtype: DataType
    accumulator_type: Optional[DataType]
    _func_type: str
    _deterministic: bool
    _name: Optional[str]
    _cached_table_udf_wrapper: Optional[UserDefinedAggregateFunctionWrapper] = field(
        default=None, init=False, repr=False, compare=False)

    @property
    def __name__(self) -> str:
        return self._table_udf_wrapper._name

    def __call__(self, *args: Any) -> Expression:
        return self._table_udf_wrapper(*args)

    @property
    def _table_udf_wrapper(self) -> UserDefinedAggregateFunctionWrapper:
        if self._cached_table_udf_wrapper is None:
            wrapper = table_udaf(
                _DataFrameAggregateFunctionAdapter(
                    self._func, self.return_dtype, self._deterministic),
                result_type=self.return_dtype._to_table_data_type(),
                accumulator_type=(self.accumulator_type._to_table_data_type()
                                  if self.accumulator_type is not None else None),
                func_type=self._func_type,
                deterministic=self._deterministic,
                name=self._name,
            )
            object.__setattr__(self, "_cached_table_udf_wrapper", wrapper)
        return cast(UserDefinedAggregateFunctionWrapper, self._cached_table_udf_wrapper)


@overload
def udaf(
    func: _UDAFInput, *, return_dtype: Optional[_DataTypeLike] = ...,
    accumulator_type: Optional[_DataTypeLike] = ..., func_type: str = ...,
    deterministic: bool = ..., name: Optional[str] = ...,
) -> _DataFrameUDAFWrapper:
    ...


@overload
def udaf(
    func: None = ..., *, return_dtype: Optional[_DataTypeLike] = ...,
    accumulator_type: Optional[_DataTypeLike] = ..., func_type: str = ...,
    deterministic: bool = ..., name: Optional[str] = ...,
) -> Callable[[_UDAFInput], _DataFrameUDAFWrapper]:
    ...


@PublicEvolving()
def udaf(
    func: Optional[_UDAFInput] = None,
    *,
    return_dtype: Optional[_DataTypeLike] = None,
    accumulator_type: Optional[_DataTypeLike] = None,
    func_type: str = "general",
    deterministic: bool = True,
    name: Optional[str] = None,
) -> Union[_DataFrameUDAFWrapper, Callable[[_UDAFInput], _DataFrameUDAFWrapper]]:
    """
    Declare a function that aggregates multiple rows into one result.

    General UDAFs require an :class:`~pyflink.table.udf.AggregateFunction` instance
    or class. Pandas UDAFs also accept functions and callable instances or classes;
    each argument contains a group or window's column values as pandas data.
    Calling the declaration with column expressions produces an aggregation expression
    for ``agg()``. Declarations can also be bound in :func:`pyflink.dataframe.sql`.

    ``AggregateFunction`` classes are constructed on the client. Plain callable classes
    are constructed on workers. Classes must have a zero-argument constructor;
    provided instances are serialized with the job. ``AggregateFunction.open`` and
    ``AggregateFunction.close`` run on workers.

    Explicit types take precedence over ``get_result_type()`` and
    ``get_accumulator_type()``. Without a result type, the return annotation of
    ``get_value()`` or the callable is used. Accumulator types are not inferred.
    Aggregation results follow the nested value conversion rules of DataFrame scalar UDFs.

    Ordinary grouped and global aggregations support general UDAFs in streaming mode
    and pandas UDAFs in batch mode. Window aggregations follow Table API support.

    Example::

        >>> import pyflink.dataframe as pf
        >>> from pyflink.table import EnvironmentSettings, TableEnvironment
        >>> from pyflink.table.udf import AggregateFunction
        >>> pf.set_table_environment(TableEnvironment.create(
        ...     EnvironmentSettings.in_streaming_mode()))
        >>> class Sum(AggregateFunction):
        ...     def create_accumulator(self):
        ...         return [0]
        ...     def accumulate(self, accumulator, value):
        ...         accumulator[0] += value
        ...     def get_value(self, accumulator) -> int:
        ...         return accumulator[0]
        >>> total = pf.udaf(Sum, accumulator_type=list[int])
        >>> df = pf.from_dict({"category": ["a", "a", "b"], "value": [1, 2, 3]})
        >>> result = df.group_by("category").agg(total=total(pf.col("value")))
        >>> result.columns
        ['category', 'total']

    A pandas callable can be used in batch aggregations::

        >>> pf.set_table_environment(TableEnvironment.create(
        ...     EnvironmentSettings.in_batch_mode()))
        >>> df = pf.from_dict({"category": ["a", "a", "b"], "value": [1, 2, 3]})
        >>> @pf.udaf(return_dtype=float, func_type="pandas")
        ... def mean(values):
        ...     return values.mean()
        >>> result = df.group_by("category").agg(mean=mean(pf.col("value")))
        >>> result = pf.sql(
        ...     "SELECT category, avg_value(`value`) AS mean FROM src GROUP BY category",
        ...     auto_bind=False, src=df, avg_value=mean)

    :param func: AggregateFunction instance/class, or a pandas callable instance/class/function.
    :param return_dtype: Result type as a DataFrame DataType, Python type, or SQL type string.
    :param accumulator_type: General UDAF state type, in the same formats as ``return_dtype``.
                             Omit when ``get_accumulator_type()`` supplies it. Pandas UDAFs
                             use the accumulator type managed by the Table API.
    :param func_type: ``"general"`` (default) or ``"pandas"``.
    :param deterministic: Must agree with ``AggregateFunction.is_deterministic()``.
    :param name: Optional function name. None or an empty string uses the callable's name.
    :return: A reusable aggregate declaration, or a decorator when ``func`` is omitted.

    .. versionadded:: 2.4.0
    """
    if func_type not in ("general", "pandas"):
        raise ValueError("func_type must be 'general' or 'pandas'.")
    if not isinstance(deterministic, bool):
        raise TypeError("deterministic must be a bool.")

    def decorator(f: _UDAFInput) -> _DataFrameUDAFWrapper:
        actual_func, context = _resolve_udaf(f, func_type)
        result = _resolve_aggregate_type(actual_func, return_dtype, "get_result_type")
        if result is None:
            result = _infer_return_dtype(context, None, name or "UDAF")
        if func_type == "pandas" and isinstance(result._to_table_data_type(), MapType):
            raise TypeError("Pandas UDAFs do not support MAP results.")
        accumulator = _resolve_aggregate_type(
            actual_func, accumulator_type, "get_accumulator_type"
        ) if func_type == "general" or accumulator_type is not None else None
        if func_type == "general" and accumulator is None:
            raise TypeError("Specify accumulator_type or implement get_accumulator_type().")
        if isinstance(actual_func, AggregateFunction):
            _validate_determinism_agreement(deterministic, actual_func.is_deterministic())
        return _DataFrameUDAFWrapper(
            actual_func, result, accumulator, func_type, deterministic, name)

    return decorator if func is None else decorator(func)


def _resolve_udaf(func: _UDAFInput, func_type: str) -> Tuple[_UDAFInput, _UDFDeclarationContext]:
    if inspect.isclass(func) and issubclass(func, AggregateFunction):
        _validate_zero_argument_class(func)
        func = func()
        if not isinstance(func, AggregateFunction):
            raise TypeError(
                "An AggregateFunction class must construct an AggregateFunction instance.")
    if isinstance(func, AggregateFunction):
        for method_name in (
            "create_accumulator", "accumulate", "get_value", "retract", "merge",
            "open", "close", "get_result_type", "get_accumulator_type", "is_deterministic",
        ):
            _validate_sync_aggregate_method(getattr(func, method_name))
        context = _create_declaration_context(func.get_value, partial_source=func.get_value)
    else:
        if func_type == "general":
            raise TypeError("General UDAFs require an AggregateFunction instance or class.")
        if isinstance(func, UserDefinedFunction) or (
            inspect.isclass(func) and issubclass(func, UserDefinedFunction)
        ):
            raise TypeError("Pandas UDAFs require an AggregateFunction or a Python callable.")
        context = _resolve_udf(func).declaration_context
        _validate_sync_aggregate_method(context.annotation_target)
    return func, context


def _validate_sync_aggregate_method(func: Callable[..., Any]) -> None:
    if not callable(func):
        raise TypeError("UDAF methods must be callable.")
    for candidate in (func, inspect.unwrap(func)):
        if inspect.iscoroutinefunction(candidate) or inspect.isasyncgenfunction(candidate):
            raise TypeError("UDAFs must be synchronous; async functions are not supported.")


def _resolve_aggregate_type(
    func: _UDAFInput, explicit_type: Optional[_DataTypeLike], method_name: str,
) -> Optional[DataType]:
    if explicit_type is not None:
        return _convert_to_dtype(explicit_type)
    if isinstance(func, AggregateFunction):
        method = getattr(func, method_name)
        if getattr(method, "__func__", None) is not getattr(AggregateFunction, method_name):
            declared_type = method()
            if isinstance(declared_type, TableDataType):
                return DataType(declared_type)
            if isinstance(declared_type, str):
                return _convert_to_dtype(declared_type)
            raise TypeError(f"{method_name}() must return a Table DataType or SQL type string.")
    return None


class _DataFrameAggregateFunctionAdapter(AggregateFunction):
    def __init__(self, func: _UDAFInput, return_dtype: DataType, deterministic: bool) -> None:
        self._func = func
        self._return_dtype = return_dtype
        self._deterministic = deterministic
        self._active_func: Optional[AggregateFunction] = None
        self._normalizer: Optional[Callable[[Any], Any]] = None
        self.__name__ = getattr(func, "__name__", type(func).__name__)

    def open(self, function_context: Any) -> None:
        if isinstance(self._func, AggregateFunction):
            active_func = self._func
        else:
            invoke = self._func() if inspect.isclass(self._func) else self._func
            if not callable(invoke):
                raise TypeError("UDAF class must construct a callable instance.")
            active_func = DelegatingPandasAggregateFunction(invoke)
        normalizer = _create_result_normalizer(self._return_dtype._to_table_data_type())
        active_func.open(function_context)
        self._active_func = active_func
        self._normalizer = normalizer

    def close(self) -> None:
        try:
            if self._active_func is not None:
                self._active_func.close()
        finally:
            self._active_func = None
            self._normalizer = None

    def is_deterministic(self) -> bool:
        if isinstance(self._func, AggregateFunction):
            return self._func.is_deterministic()
        return self._deterministic

    def _active(self) -> AggregateFunction:
        if self._active_func is None:
            raise RuntimeError("UDAF was invoked before open().")
        return self._active_func

    def create_accumulator(self) -> Any:
        return self._active().create_accumulator()

    def accumulate(self, accumulator: Any, *args: Any) -> None:
        self._active().accumulate(accumulator, *args)

    def retract(self, accumulator: Any, *args: Any) -> None:
        self._active().retract(accumulator, *args)

    def merge(self, accumulator: Any, accumulators: Any) -> None:
        self._active().merge(accumulator, accumulators)

    def get_value(self, accumulator: Any) -> Any:
        result: Any = self._active().get_value(accumulator)
        return self._normalizer(result) if self._normalizer is not None else result
