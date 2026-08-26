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

"""User-defined scalar functions for the DataFrame API."""

import functools
import inspect
from collections.abc import Mapping
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
    get_type_hints,
    overload,
)

from pyflink.common import Row
from pyflink.dataframe.datatype import DataType
from pyflink.table.expression import Expression
from pyflink.table.expressions import call as table_call
from pyflink.table.types import ArrayType, MapType, RowType
from pyflink.table.udf import (
    AsyncScalarFunction,
    ScalarFunction,
    UserDefinedFunction,
    UserDefinedFunctionWrapper,
    udf as table_udf,
)
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ["DataFrameUDFWrapper", "udf"]

_UDFInput = Union[Callable[..., Any], ScalarFunction, AsyncScalarFunction, Type]
_DataTypeLike = Union[DataType, Type, str]


class _UDFUsage(Enum):
    EXPRESSION = "expression"
    MAP = "map"
    MAP_BATCHES = "map_batches"


@PublicEvolving()
class DataFrameUDFWrapper:
    """
    A callable DataFrame scalar UDF declaration.

    Instances are created with :func:`udf` and can be called with DataFrame
    expressions or Python literals to produce an expression.

    Example::

        >>> import pyflink.dataframe as pf
        >>> @pf.udf
        ... def add_one(value: int) -> int:
        ...     return value + 1
        >>> expression = add_one(pf.col("value"))

    .. versionadded:: 2.4.0
    """

    _func: _UDFInput
    _return_dtype: DataType
    _deterministic: bool
    _func_type: str
    _is_async: bool
    _cached_table_udf_wrapper: Optional[UserDefinedFunctionWrapper]
    _frozen: bool
    __name__: str

    def __init__(
        self,
        func: _UDFInput,
        return_dtype: DataType,
        deterministic: bool,
        name: str,
        func_type: str,
        is_async: bool,
        metadata_source: Optional[_UDFInput] = None,
    ) -> None:
        object.__setattr__(self, "_func", func)
        object.__setattr__(self, "_return_dtype", return_dtype)
        object.__setattr__(self, "_deterministic", deterministic)
        object.__setattr__(self, "_func_type", func_type)
        object.__setattr__(self, "_is_async", is_async)
        object.__setattr__(self, "_cached_table_udf_wrapper", None)

        declaration = func if metadata_source is None else metadata_source
        declaration_metadata = _unwrap_partial(declaration)
        functools.update_wrapper(self, declaration_metadata, updated=())
        object.__setattr__(self, "__name__", name)
        object.__setattr__(self, "__wrapped__", declaration)
        object.__setattr__(self, "_frozen", True)

    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self, "_frozen", False):
            raise AttributeError("DataFrameUDFWrapper declarations are immutable.")
        object.__setattr__(self, name, value)

    @PublicEvolving()
    def __call__(self, *args: Any) -> Expression:
        """
        Create an expression that calls this UDF.

        Example::

            >>> import pyflink.dataframe as pf
            >>> @pf.udf
            ... def add_one(value: int) -> int:
            ...     return value + 1
            >>> expression = add_one(pf.col("value"))
        """
        return table_call(self._table_udf_wrapper, *args)

    @property
    def _table_udf_wrapper(self) -> UserDefinedFunctionWrapper:
        if self._cached_table_udf_wrapper is None:
            object.__setattr__(
                self,
                "_cached_table_udf_wrapper",
                self._create_table_udf_wrapper(_UDFUsage.EXPRESSION),
            )
        return cast(UserDefinedFunctionWrapper, self._cached_table_udf_wrapper)

    def _create_table_udf_wrapper(
        self, usage: _UDFUsage
    ) -> UserDefinedFunctionWrapper:
        adapter_type = (
            _DataFrameAsyncScalarFunctionAdapter
            if self._is_async
            else _DataFrameScalarFunctionAdapter
        )
        actual_func = cast(
            Union[ScalarFunction, AsyncScalarFunction],
            adapter_type(
                self._func,
                self._return_dtype,
                self._deterministic,
                usage,
                self._func_type,
            ),
        )
        return cast(
            UserDefinedFunctionWrapper,
            table_udf(
                actual_func,
                result_type=self._return_dtype._to_table_data_type(),
                deterministic=self._deterministic,
                name=self.__name__,
                func_type=self._func_type,
            ),
        )

    @property
    @PublicEvolving()
    def return_dtype(self) -> DataType:
        """
        The logical result type of this UDF.

        Example::

            >>> import pyflink.dataframe as pf
            >>> @pf.udf
            ... def add_one(value: int) -> int:
            ...     return value + 1
            >>> add_one.return_dtype == pf.DataType.int64()
            True
        """
        return self._return_dtype


@overload
def udf(
    func: _UDFInput,
    *,
    return_dtype: Optional[_DataTypeLike] = ...,
    deterministic: bool = ...,
    name: Optional[str] = ...,
    func_type: Optional[str] = ...,
) -> DataFrameUDFWrapper:
    ...


@overload
def udf(
    func: None = ...,
    *,
    return_dtype: Optional[_DataTypeLike] = ...,
    deterministic: bool = ...,
    name: Optional[str] = ...,
    func_type: Optional[str] = ...,
) -> Callable[[_UDFInput], DataFrameUDFWrapper]:
    ...


@PublicEvolving()
def udf(
    func: Optional[_UDFInput] = None,
    *,
    return_dtype: Optional[_DataTypeLike] = None,
    deterministic: bool = True,
    name: Optional[str] = None,
    func_type: Optional[str] = None,
) -> Union[DataFrameUDFWrapper, Callable[[_UDFInput], DataFrameUDFWrapper]]:
    """
    Create a scalar UDF for DataFrame expressions.

    The function may be synchronous or asynchronous. Pandas UDFs operate on
    ``pandas.Series`` or ``pandas.DataFrame`` batches and must declare
    ``return_dtype``. Plain callable class objects must have a zero-argument
    constructor and are instantiated on the worker.

    A UDF can be declared with a bare decorator, a configured decorator, or a
    direct call. General UDFs may infer ``return_dtype`` from the return
    annotation of the function, ``__call__``, or ``eval``. A ``TypedDict``
    return annotation becomes a struct column::

        >>> import pyflink.dataframe as pf

        >>> @pf.udf
        ... def add_one(value: int) -> int:
        ...     return value + 1

        >>> @pf.udf(return_dtype=str)
        ... def as_text(value):
        ...     return str(value)

        >>> increment = pf.udf(
        ...     lambda value, amount: value + amount,
        ...     return_dtype="BIGINT",
        ... )

        >>> from typing import TypedDict

        >>> class LabeledValue(TypedDict):
        ...     value: int
        ...     label: str

        >>> @pf.udf
        ... def describe(value: int) -> LabeledValue:
        ...     return {"value": value, "label": str(value)}

    Plain callable classes can be supplied as zero-argument class objects or
    as configured instances. Class objects, including their ``__init__``, are
    constructed during worker initialization, so expensive initialization is
    deferred to the TaskManager::

        >>> class AddOne:
        ...     def __call__(self, value: int) -> int:
        ...         return value + 1

        >>> add_one_from_class = pf.udf(AddOne)
        >>> add_one_from_instance = pf.udf(AddOne())

        >>> @pf.udf
        ... class ModelInference:
        ...     def __init__(self):
        ...         self.model = load_model()
        ...     def __call__(self, features: list[float]) -> float:
        ...         return self.model.predict(features)

    :class:`~pyflink.table.udf.ScalarFunction` and
    :class:`~pyflink.table.udf.AsyncScalarFunction` class objects and instances
    are also supported. Their logical result type is inferred from ``eval``
    when it is not given explicitly. Class objects are instantiated on the
    client; their ``open`` and ``close`` methods still run on the worker::

        >>> from pyflink.table.udf import AsyncScalarFunction, ScalarFunction

        >>> class AddOneFunction(ScalarFunction):
        ...     def eval(self, value: int) -> int:
        ...         return value + 1

        >>> add_one_class = pf.udf(AddOneFunction)
        >>> add_one_instance = pf.udf(AddOneFunction())

        >>> class AsyncLookup(AsyncScalarFunction):
        ...     async def eval(self, key: int) -> str:
        ...         return await lookup(key)

        >>> async_lookup = pf.udf(AsyncLookup)

    Plain ``async def`` functions and callable objects with an asynchronous
    ``__call__`` use general asynchronous execution::

        >>> @pf.udf
        ... async def async_add_one(value: int) -> int:
        ...     return value + 1

    Pandas UDFs receive and return ``pandas.Series`` or ``pandas.DataFrame``
    batches and always require an explicit logical ``return_dtype``. Pandas
    mode can be selected explicitly, or inferred from a pandas container
    annotation on any unbound parameter or the return value::

        >>> import pandas as pd

        >>> @pf.udf(return_dtype=pf.DataType.int64(), func_type="pandas")
        ... def pandas_add_one(values):
        ...     return values + 1

        >>> @pf.udf(return_dtype=pf.DataType.int64())
        ... def inferred_pandas_add_one(values: pd.Series) -> pd.Series:
        ...     return values + 1

    A declared UDF is called with DataFrame expressions or Python literals to
    produce a single-column expression::

        >>> df = pf.from_records([(1,), (2,)], schema=["value"])

        >>> result = df.with_columns(
        ...     next_value=add_one(pf.col("value")),
        ...     incremented=increment(pf.col("value"), 2),
        ... )

    :param func: Function, callable object, scalar UDF instance, or zero-argument
                 callable/scalar-UDF class.
    :param return_dtype: DataFrame logical type, Python type, or SQL type string.
                         General UDFs may infer it from a return annotation;
                         pandas UDFs require it.
    :param deterministic: Whether equal inputs always produce equal results.
                          Must agree with scalar-function metadata.
    :param name: Non-empty function identity used by the Table planner.
    :param func_type: ``"general"`` or ``"pandas"``. If omitted, any unbound
                      pandas container annotation selects pandas mode.
    :return: A :class:`DataFrameUDFWrapper`, or a decorator when ``func`` is omitted.

    .. versionadded:: 2.4.0
    """

    def decorator(f: _UDFInput) -> DataFrameUDFWrapper:
        _validate_scalar_udf_source(f)
        (
            actual_func,
            inspection_target,
            skip_first_parameter,
            is_async,
        ) = _resolve_udf_source(f)
        actual_func_type = (
            func_type
            if func_type is not None
            else _detect_func_type(
                inspection_target, skip_first=skip_first_parameter
            )
        )
        _validate_scalar_udf_options(actual_func_type, return_dtype, is_async)
        actual_return_dtype = _infer_return_dtype(inspection_target, return_dtype)
        actual_deterministic = _resolve_deterministic(actual_func, deterministic)
        actual_name = _resolve_name(actual_func, name)

        return DataFrameUDFWrapper(
            actual_func,
            actual_return_dtype,
            actual_deterministic,
            actual_name,
            actual_func_type,
            is_async,
            metadata_source=f,
        )

    return decorator if func is None else decorator(func)


# ======================== Declaration Validation ========================


def _validate_scalar_udf_source(func: Any) -> None:
    if inspect.isclass(func):
        if issubclass(func, UserDefinedFunction) and not issubclass(
            func, (ScalarFunction, AsyncScalarFunction)
        ):
            raise TypeError(f"func must be a scalar UDF, got {func.__name__}.")
        if not issubclass(
            func, (ScalarFunction, AsyncScalarFunction)
        ) and not _has_custom_call(func):
            raise TypeError(f"func must be callable, got {func.__name__}.")
        return
    if isinstance(func, UserDefinedFunction) and not isinstance(
        func, (ScalarFunction, AsyncScalarFunction)
    ):
        raise TypeError(f"func must be a scalar UDF, got {type(func).__name__}.")
    if not isinstance(func, (ScalarFunction, AsyncScalarFunction)) and not callable(func):
        raise TypeError(f"func must be callable, got {type(func).__name__}.")


def _validate_scalar_udf_options(
    func_type: str,
    return_dtype: Optional[_DataTypeLike],
    is_async: bool,
) -> None:
    if func_type not in ("general", "pandas"):
        raise ValueError(
            f"The func_type must be one of 'general, pandas', got {func_type}."
        )
    if return_dtype is None and func_type == "pandas":
        raise TypeError(
            "return_dtype is required for pandas UDFs because pandas container "
            "annotations do not describe the logical result type."
        )
    if is_async and func_type == "pandas":
        raise ValueError(
            "Async scalar functions do not support pandas func_type. "
            "Use func_type='general'."
        )


# ======================== Callable Inspection and Resolution ========================


def _has_custom_call(cls: Type) -> bool:
    """Check whether a class defines ``__call__`` in its MRO."""
    return any("__call__" in base.__dict__ for base in cls.__mro__ if base is not object)


def _get_callable_class_hint_method(
    func_class: Type, method_name: str = "__call__"
) -> Tuple[Optional[Callable[..., Any]], bool]:
    """Return a class method that can be inspected without constructing the class."""
    descriptor = inspect.getattr_static(func_class, method_name, None)
    if isinstance(descriptor, staticmethod):
        return cast(Callable[..., Any], descriptor.__func__), False
    if isinstance(descriptor, classmethod):
        return cast(Callable[..., Any], descriptor.__func__), True
    if inspect.isroutine(descriptor):
        return cast(Callable[..., Any], descriptor), True
    return None, False


def _is_class_udf(func: Any) -> bool:
    """Check whether ``func`` uses a class-based scalar UDF declaration form."""
    if isinstance(func, functools.partial):
        return False
    if isinstance(func, (ScalarFunction, AsyncScalarFunction)):
        return True
    if inspect.isclass(func):
        if issubclass(func, (ScalarFunction, AsyncScalarFunction)):
            return True
        return _has_custom_call(func)
    if not inspect.isroutine(func) and hasattr(func, "__call__"):
        return _has_custom_call(type(func))
    return False


def _resolve_udf_source(
    func: _UDFInput,
) -> Tuple[_UDFInput, Callable[..., Any], bool, bool]:
    """Resolve the runtime source and callable that describes its declaration."""
    if not _is_class_udf(func):
        callable_func = cast(Callable[..., Any], func)
        return func, callable_func, False, _is_async_callable(callable_func)

    skip_first_parameter = False
    if inspect.isclass(func):
        _validate_zero_argument_class(func)
        if issubclass(func, (ScalarFunction, AsyncScalarFunction)):
            actual_func = func()
            hint_method = actual_func.eval
            is_async = isinstance(
                actual_func, AsyncScalarFunction
            ) or inspect.iscoroutinefunction(hint_method)
            return actual_func, hint_method, False, is_async

        class_hint_method, skip_first_parameter = _get_callable_class_hint_method(
            func
        )
        if class_hint_method is None:
            raise TypeError(
                f"Callable class '{func.__name__}': __call__ must be defined as a method."
            )
        hint_method = class_hint_method
        is_async = inspect.iscoroutinefunction(hint_method)
    elif isinstance(func, (ScalarFunction, AsyncScalarFunction)):
        hint_method = func.eval
        is_async = isinstance(func, AsyncScalarFunction) or inspect.iscoroutinefunction(
            hint_method
        )
    else:
        hint_method = cast(Callable[..., Any], getattr(func, "__call__"))
        is_async = inspect.iscoroutinefunction(hint_method)
    return func, hint_method, skip_first_parameter, is_async


def _validate_zero_argument_class(func_class: Type) -> None:
    if inspect.isabstract(func_class):
        raise TypeError(f"UDF class '{func_class.__name__}' must not be abstract.")
    try:
        constructor_signature = inspect.signature(func_class)
    except (TypeError, ValueError) as exc:
        raise TypeError(
            f"Cannot verify that UDF class '{func_class.__name__}' has a zero-argument "
            "constructor; pass a configured instance instead."
        ) from exc
    try:
        constructor_signature.bind()
    except TypeError as exc:
        raise TypeError(
            f"UDF class '{func_class.__name__}' must have a zero-argument constructor; "
            "pass a configured instance instead."
        ) from exc


def _infer_return_dtype(
    func: Callable[..., Any], return_dtype: Optional[_DataTypeLike]
) -> DataType:
    """Infer the DataFrame return type or validate its explicit declaration."""
    if return_dtype is not None:
        return _convert_to_dtype(return_dtype)

    hint_func = _get_callable_inspection_target(func)
    hints = _get_callable_type_hints(hint_func)
    if "return" not in hints:
        func_name = _default_udf_name(func)
        raise TypeError(
            f"Cannot infer return_dtype for '{func_name}': add a return annotation "
            "or specify return_dtype explicitly."
        )
    return _data_type_from_type_hint(hints["return"])


def _convert_to_dtype(dtype_like: _DataTypeLike) -> DataType:
    if isinstance(dtype_like, DataType):
        return dtype_like
    if isinstance(dtype_like, str):
        return DataType._from_sql(dtype_like)
    try:
        return DataType._from_type_hint(dtype_like)
    except TypeError as exc:
        raise TypeError(
            "return_dtype must be a DataFrame DataType, Python type, or SQL "
            f"type string, got {type(dtype_like).__name__}."
        ) from exc


def _is_typed_dict(type_hint: Any) -> bool:
    try:
        from typing import is_typeddict

        if is_typeddict(type_hint):
            return True
    except ImportError:
        pass
    return (
        isinstance(type_hint, type)
        and issubclass(type_hint, dict)
        and hasattr(type_hint, "__required_keys__")
    )


def _data_type_from_type_hint(type_hint: Any) -> DataType:
    if _is_typed_dict(type_hint):
        return DataType.struct(
            {
                name: _data_type_from_type_hint(field_hint)
                for name, field_hint in get_type_hints(type_hint).items()
            }
        )
    return DataType._from_type_hint(type_hint)


def _detect_func_type(func: Callable[..., Any], skip_first: bool = False) -> str:
    """Detect pandas mode from an unbound pandas container annotation."""
    hint_func = _get_callable_inspection_target(func)
    try:
        import pandas as pd
    except ImportError:
        return "general"
    hints = _get_callable_type_hints(
        hint_func, fallback_globals={"pandas": pd, "pd": pd}
    )

    try:
        parameters = list(inspect.signature(hint_func).parameters)
    except (TypeError, ValueError):
        parameters = []
    ignored_hint_names: Set[str] = set()
    if skip_first and parameters:
        ignored_hint_names.add(parameters[0])
    if isinstance(func, functools.partial):
        try:
            ignored_hint_names.update(
                inspect.signature(hint_func)
                .bind_partial(*func.args, **(func.keywords or {}))
                .arguments
            )
        except (TypeError, ValueError):
            pass

    pandas_types = (pd.Series, pd.DataFrame)
    return (
        "pandas"
        if any(
            name not in ignored_hint_names and hint in pandas_types
            for name, hint in hints.items()
        )
        else "general"
    )


def _unwrap_partial(func: Any) -> Any:
    while isinstance(func, functools.partial):
        func = func.func
    return func


def _get_callable_inspection_target(
    func: Callable[..., Any],
) -> Callable[..., Any]:
    target = _unwrap_partial(func)
    if callable(target) and not inspect.isroutine(target) and not inspect.isclass(target):
        return cast(Callable[..., Any], getattr(target, "__call__"))
    return cast(Callable[..., Any], target)


def _get_callable_type_hints(
    func: Callable[..., Any], fallback_globals: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    try:
        if fallback_globals is None:
            return get_type_hints(func)
        func_globals = getattr(func, "__globals__", None)
        if func_globals is None:
            func_globals = getattr(
                getattr(func, "__func__", None), "__globals__", {}
            )
        return get_type_hints(
            func,
            globalns={**fallback_globals, **func_globals},
        )
    except (NameError, TypeError):
        return {}


def _is_async_callable(func: Callable[..., Any]) -> bool:
    return inspect.iscoroutinefunction(_get_callable_inspection_target(func))


def _resolve_deterministic(func: _UDFInput, deterministic: bool) -> bool:
    if not isinstance(deterministic, bool):
        raise TypeError("deterministic must be a bool.")
    if isinstance(func, (ScalarFunction, AsyncScalarFunction)):
        _validate_deterministic(deterministic, func.is_deterministic())
    return deterministic


def _validate_deterministic(declared: bool, actual: bool) -> None:
    if declared != actual:
        raise ValueError(f"Inconsistent deterministic: {declared} and {actual}.")


def _resolve_name(func: _UDFInput, name: Optional[str]) -> str:
    actual_name = _default_udf_name(func) if name is None else name
    if not isinstance(actual_name, str):
        raise TypeError("name must be a str or None.")
    if not actual_name:
        raise ValueError("name must not be empty.")
    return actual_name


def _default_udf_name(func: _UDFInput) -> str:
    target = _unwrap_partial(func)
    name = getattr(target, "__name__", None)
    return name if isinstance(name, str) else type(target).__name__


# ======================== Worker Adapters ========================


def _wrap_scalar_general_result(
    func: Callable[..., Any], return_dtype: DataType
) -> Callable[..., Any]:
    result_type = return_dtype._to_table_data_type()

    if _is_async_callable(func):

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            return _normalize_user_value(await func(*args, **kwargs), result_type)

        wrapper = async_wrapper
    else:

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            return _normalize_user_value(func(*args, **kwargs), result_type)

        wrapper = sync_wrapper

    if not hasattr(func, "__name__"):
        wrapper.__name__ = type(func).__name__
    return wrapper


class _DataFrameUDFAdapterBase:
    """Bind a lazy DataFrame UDF source to one worker invocation protocol."""

    def __init__(
        self,
        func: _UDFInput,
        return_dtype: DataType,
        deterministic: bool,
        usage: _UDFUsage,
        func_type: str,
    ) -> None:
        self._func_class: Optional[Type] = func if inspect.isclass(func) else None
        self._func: Optional[_UDFInput] = None if self._func_class is not None else func
        self._return_dtype = return_dtype if func_type == "general" else None
        self._deterministic = deterministic
        self._usage = usage
        self._func_type = func_type
        self._bound_func: Optional[Callable[..., Any]] = None
        self.__name__ = _default_udf_name(func)
        self.__doc__ = getattr(func, "__doc__", None)

    def open(self, function_context: Any) -> None:
        if self._func_class is not None:
            self._func = self._func_class()

        func = self._func
        if func is None:
            raise RuntimeError("DataFrame UDF source was not initialized.")
        if not isinstance(func, (ScalarFunction, AsyncScalarFunction)) and not callable(func):
            class_name = self._func_class.__name__ if self._func_class else type(func).__name__
            raise TypeError(
                f"Callable class '{class_name}' constructed a non-callable "
                f"object of type '{type(func).__name__}'."
            )

        if isinstance(func, (ScalarFunction, AsyncScalarFunction)):
            _validate_deterministic(self._deterministic, func.is_deterministic())
            func.open(function_context)
            invoke_func = func.eval
        elif inspect.isroutine(func) or isinstance(func, functools.partial):
            invoke_func = cast(Callable[..., Any], func)
        else:
            invoke_func = cast(Callable[..., Any], getattr(func, "__call__"))
        self._bound_func = self._bind_func(invoke_func)

    def _bind_func(self, invoke_func: Callable[..., Any]) -> Callable[..., Any]:
        if self._usage is not _UDFUsage.EXPRESSION:
            raise NotImplementedError(
                f"DataFrame UDF usage {self._usage.value!r} is not supported yet."
            )
        if self._func_type == "general":
            return _wrap_scalar_general_result(
                invoke_func, cast(DataType, self._return_dtype)
            )
        return invoke_func

    def close(self) -> None:
        func = self._func
        try:
            if isinstance(func, (ScalarFunction, AsyncScalarFunction)):
                func.close()
        finally:
            self._bound_func = None
            if self._func_class is not None:
                self._func = None

    def is_deterministic(self) -> bool:
        return self._deterministic

    def _invocation(self) -> Callable[..., Any]:
        if self._bound_func is None:
            raise RuntimeError("DataFrame UDF was invoked before open().")
        return self._bound_func


class _DataFrameScalarFunctionAdapter(_DataFrameUDFAdapterBase, ScalarFunction):
    """Synchronous terminal adapter for a bound DataFrame UDF."""

    def eval(self, *args: Any) -> Any:
        invoke_func = self._invocation()
        if self._func_type == "pandas":
            from pyflink.fn_execution.utils.operation_utils import (
                check_pandas_udf_result,
            )

            return check_pandas_udf_result(invoke_func, *args)
        return invoke_func(*args)


class _DataFrameAsyncScalarFunctionAdapter(
    _DataFrameUDFAdapterBase, AsyncScalarFunction
):
    """Asynchronous terminal adapter for a bound DataFrame UDF."""

    async def eval(self, *args: Any) -> Any:
        return await self._invocation()(*args)


# ======================== Result Normalization ========================


def _row_value_by_type(value: Any, row_type: RowType, index: int) -> Any:
    field_name = row_type.field_names()[index]
    if isinstance(value, Mapping):
        return value.get(field_name)
    if isinstance(value, Row) and hasattr(value, "_fields"):
        return _named_row_field_value(value, field_name)
    if isinstance(value, (Row, tuple, list)):
        if len(value) != len(row_type.fields):
            raise ValueError(
                f"Expected {len(row_type.fields)} value(s) for RowType "
                f"{row_type.field_names()}, got {len(value)}."
            )
        return value[index]
    attributes = getattr(value, "__dict__", None)
    if isinstance(attributes, Mapping):
        return attributes.get(field_name)
    try:
        return getattr(value, field_name)
    except AttributeError:
        raise TypeError(
            f"Expected a Mapping, Row, tuple, list, or object with fields for RowType "
            f"{row_type.field_names()}, got {type(value).__name__}."
        ) from None


def _named_row_field_value(row: Row, field_name: str) -> Any:
    if field_name not in row._fields:
        raise ValueError(
            f"Field name {field_name!r} does not exist in Row fields {row._fields}."
        )
    field_index = row._fields.index(field_name)
    if field_index >= len(row):
        raise ValueError(
            f"Field name {field_name!r} is declared in Row fields {row._fields} "
            "but has no value."
        )
    return row[field_name]


def _normalize_user_value(value: Any, data_type: Any) -> Any:
    """Normalize nested user values to the Python shape expected by Table coders."""
    if value is None:
        return None
    if isinstance(data_type, RowType):
        row = Row(
            *[
                _normalize_user_value(
                    _row_value_by_type(value, data_type, index), field.data_type
                )
                for index, field in enumerate(data_type)
            ]
        )
        row.set_field_names(data_type.field_names())
        if isinstance(value, Row):
            row.set_row_kind(value.get_row_kind())
        return row
    if isinstance(data_type, ArrayType):
        return [
            _normalize_user_value(item, data_type.element_type) for item in value
        ]
    if isinstance(data_type, MapType):
        items_method = getattr(value, "items", None)
        if callable(items_method):
            items = list(items_method())
        else:
            try:
                items = list(value)
            except TypeError as exc:
                raise TypeError(
                    f"Expected a Mapping or iterable of key/value pairs for {data_type}, "
                    f"got {type(value).__name__}."
                ) from exc
        if any(
            not isinstance(item, (tuple, list)) or len(item) != 2 for item in items
        ):
            raise TypeError(
                f"Expected a Mapping or iterable of key/value pairs for {data_type}, "
                f"got {type(value).__name__}."
            )
        if any(item[0] is None for item in items):
            raise TypeError(f"MapType keys must not be null for {data_type}.")
        return {
            _normalize_user_value(key, data_type.key_type): _normalize_user_value(
                item_value, data_type.value_type
            )
            for key, item_value in items
        }
    return value
