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
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
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

__all__ = ["udf"]

_UDFInput = Union[Callable[..., Any], ScalarFunction, AsyncScalarFunction, Type]
_DataTypeLike = Union[DataType, Type, str]
_UNRESOLVED_TYPE_HINT = object()


class _UDFUsage(Enum):
    EXPRESSION = "expression"
    MAP = "map"
    MAP_BATCHES = "map_batches"


class _UDFSourceKind(Enum):
    """How a resolved UDF source is initialized and invoked on a worker."""

    DIRECT_CALLABLE = "direct_callable"
    CALLABLE_INSTANCE = "callable_instance"
    CALLABLE_CLASS = "callable_class"
    SCALAR_FUNCTION_INSTANCE = "scalar_function_instance"
    SCALAR_FUNCTION_CLASS = "scalar_function_class"


@dataclass(frozen=True)
class _ResolvedUDFSource:
    """Callable metadata resolved once on the client and reused on workers."""

    source: _UDFInput
    kind: _UDFSourceKind
    is_async: bool
    ignored_hint_names: FrozenSet[str] = frozenset()

    def _inspection_target_and_skip_first(
        self,
    ) -> Tuple[Callable[..., Any], bool]:
        if self.kind is _UDFSourceKind.DIRECT_CALLABLE:
            return (
                _get_callable_inspection_target(
                    cast(Callable[..., Any], self.source)
                ),
                False,
            )
        if self.kind is _UDFSourceKind.SCALAR_FUNCTION_INSTANCE:
            return (
                cast(
                    Union[ScalarFunction, AsyncScalarFunction], self.source
                ).eval,
                False,
            )
        if self.kind in (
            _UDFSourceKind.CALLABLE_CLASS,
            _UDFSourceKind.SCALAR_FUNCTION_CLASS,
        ):
            hint_method, skip_first_parameter = _get_callable_class_hint_method(
                cast(Type, self.source),
                "eval"
                if self.kind is _UDFSourceKind.SCALAR_FUNCTION_CLASS
                else "__call__",
            )
            if hint_method is None:
                raise RuntimeError("Resolved UDF class has no inspection target.")
            return hint_method, skip_first_parameter
        return cast(Callable[..., Any], getattr(self.source, "__call__")), False

    @property
    def inspection_target(self) -> Callable[..., Any]:
        target, _ = self._inspection_target_and_skip_first()
        return target

    @property
    def invocation_signature(self) -> Optional[inspect.Signature]:
        target, skip_first_parameter = self._inspection_target_and_skip_first()
        if not self.is_scalar_function and not self.constructs_on_worker:
            target = cast(Callable[..., Any], self.source)

        try:
            signature = inspect.signature(target)
            if skip_first_parameter:
                parameters = tuple(signature.parameters.values())
                signature = signature.replace(parameters=parameters[1:])
        except Exception:
            return None
        return signature

    @property
    def default_name(self) -> str:
        return _default_udf_name(self.source)

    @property
    def is_scalar_function(self) -> bool:
        return self.kind in (
            _UDFSourceKind.SCALAR_FUNCTION_INSTANCE,
            _UDFSourceKind.SCALAR_FUNCTION_CLASS,
        )

    @property
    def constructs_on_worker(self) -> bool:
        return self.kind in (
            _UDFSourceKind.CALLABLE_CLASS,
            _UDFSourceKind.SCALAR_FUNCTION_CLASS,
        )

    def create_worker_source(self) -> _UDFInput:
        if not self.constructs_on_worker:
            return self.source
        source_class = cast(Type, self.source)
        source = source_class()
        if self.is_scalar_function:
            if not isinstance(source, (ScalarFunction, AsyncScalarFunction)):
                raise TypeError(
                    f"Scalar UDF class '{source_class.__name__}' constructed an "
                    f"unsupported object of type '{type(source).__name__}'."
                )
        elif not callable(source):
            raise TypeError(
                f"Callable class '{source_class.__name__}' constructed a non-callable "
                f"object of type '{type(source).__name__}'."
            )
        return cast(_UDFInput, source)

    def validate_deterministic(
        self, declared: bool, worker_source: Optional[_UDFInput] = None
    ) -> None:
        source: Optional[_UDFInput]
        if self.kind is _UDFSourceKind.SCALAR_FUNCTION_INSTANCE:
            source = self.source
        elif self.kind is _UDFSourceKind.SCALAR_FUNCTION_CLASS:
            source = worker_source
        else:
            source = None
        if source is not None:
            _validate_deterministic(
                declared,
                cast(
                    Union[ScalarFunction, AsyncScalarFunction], source
                ).is_deterministic(),
            )

    def open_worker_source(
        self, worker_source: _UDFInput, function_context: Any
    ) -> None:
        if self.is_scalar_function:
            cast(
                Union[ScalarFunction, AsyncScalarFunction], worker_source
            ).open(function_context)

    def worker_invocation(
        self, worker_source: _UDFInput
    ) -> Callable[..., Any]:
        if self.kind is _UDFSourceKind.DIRECT_CALLABLE:
            return cast(Callable[..., Any], worker_source)
        if self.is_scalar_function:
            return cast(
                Union[ScalarFunction, AsyncScalarFunction], worker_source
            ).eval
        return cast(Callable[..., Any], getattr(worker_source, "__call__"))

    def close_worker_source(self, worker_source: Optional[_UDFInput]) -> None:
        if self.is_scalar_function and worker_source is not None:
            cast(
                Union[ScalarFunction, AsyncScalarFunction], worker_source
            ).close()


class _DataFrameUDFWrapper:
    """Internal callable binding a DataFrame scalar UDF to Table expressions."""

    _source: _ResolvedUDFSource
    _return_dtype: DataType
    _deterministic: bool
    _func_type: str
    _cached_table_udf_wrapper: Optional[UserDefinedFunctionWrapper]
    _frozen: bool
    __name__: str

    def __init__(
        self,
        source: _ResolvedUDFSource,
        return_dtype: DataType,
        deterministic: bool,
        name: str,
        func_type: str,
    ) -> None:
        object.__setattr__(self, "_source", source)
        object.__setattr__(self, "_return_dtype", return_dtype)
        object.__setattr__(self, "_deterministic", deterministic)
        object.__setattr__(self, "_func_type", func_type)
        object.__setattr__(self, "_cached_table_udf_wrapper", None)

        declaration_metadata = _unwrap_partial(source.source)
        functools.update_wrapper(self, declaration_metadata, updated=())
        object.__setattr__(self, "__name__", name)
        object.__setattr__(self, "__wrapped__", source.source)
        invocation_signature = source.invocation_signature
        if invocation_signature is not None:
            object.__setattr__(self, "__signature__", invocation_signature)
        object.__setattr__(self, "_frozen", True)

    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self, "_frozen", False):
            raise AttributeError("DataFrame UDF declarations are immutable.")
        object.__setattr__(self, name, value)

    def __call__(self, *args: Any) -> Expression:
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
            if self._source.is_async
            else _DataFrameScalarFunctionAdapter
        )
        actual_func = cast(
            Union[ScalarFunction, AsyncScalarFunction],
            adapter_type(
                self._source,
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
    def return_dtype(self) -> DataType:
        return self._return_dtype


@overload
def udf(
    func: _UDFInput,
    *,
    return_dtype: Optional[_DataTypeLike] = ...,
    deterministic: bool = ...,
    name: Optional[str] = ...,
    func_type: Optional[str] = ...,
) -> Callable[..., Expression]:
    ...


@overload
def udf(
    func: None = ...,
    *,
    return_dtype: Optional[_DataTypeLike] = ...,
    deterministic: bool = ...,
    name: Optional[str] = ...,
    func_type: Optional[str] = ...,
) -> Callable[[_UDFInput], Callable[..., Expression]]:
    ...


@PublicEvolving()
def udf(
    func: Optional[_UDFInput] = None,
    *,
    return_dtype: Optional[_DataTypeLike] = None,
    deterministic: bool = True,
    name: Optional[str] = None,
    func_type: Optional[str] = None,
) -> Union[
    Callable[..., Expression],
    Callable[[_UDFInput], Callable[..., Expression]],
]:
    """
    Create a scalar UDF for DataFrame expressions.

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
    initialized on the TaskManager, so expensive initialization is deferred::

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
    when it is not given explicitly. Class objects are initialized on the
    TaskManager, where their ``open`` and ``close`` methods also run::

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

    Pandas UDFs always require an explicit logical ``return_dtype``. Each
    ``ROW``-typed argument is received as a ``pandas.DataFrame`` with one column
    per field; other arguments are received as ``pandas.Series``. A ``ROW``-typed
    result should be returned as a ``pandas.DataFrame``, while other results
    should be returned as ``pandas.Series``. Pandas mode can be selected
    explicitly, or inferred from a pandas container annotation on any unbound
    parameter or the return value::

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
    :return: A callable that accepts DataFrame expressions or Python literals and
             returns an :class:`~pyflink.table.expression.Expression`, or a decorator
             producing such a callable when ``func`` is omitted.

    .. versionadded:: 2.4.0
    """

    def decorator(f: _UDFInput) -> Callable[..., Expression]:
        source = _resolve_udf_source(f)
        actual_func_type = (
            func_type
            if func_type is not None
            else _detect_func_type(source)
        )
        _validate_scalar_udf_options(
            actual_func_type, return_dtype, source.is_async
        )
        actual_return_dtype = _infer_return_dtype(
            source.inspection_target, return_dtype
        )
        actual_deterministic = _resolve_deterministic(source, deterministic)
        actual_name = _resolve_name(source, name)

        return _DataFrameUDFWrapper(
            source,
            actual_return_dtype,
            actual_deterministic,
            actual_name,
            actual_func_type,
        )

    return decorator if func is None else decorator(func)


# ======================== Declaration Validation ========================


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


def _resolve_udf_source(
    func: _UDFInput,
) -> _ResolvedUDFSource:
    """Validate and classify one callable declaration."""
    if isinstance(func, functools.partial) or inspect.isroutine(func):
        inspection_target = _get_callable_inspection_target(
            cast(Callable[..., Any], func)
        )
        return _ResolvedUDFSource(
            func,
            _UDFSourceKind.DIRECT_CALLABLE,
            inspect.iscoroutinefunction(inspection_target),
            _ignored_hint_names(func, inspection_target),
        )

    if inspect.isclass(func):
        if issubclass(func, UserDefinedFunction) and not issubclass(
            func, (ScalarFunction, AsyncScalarFunction)
        ):
            raise TypeError(f"func must be a scalar UDF, got {func.__name__}.")
        if issubclass(func, (ScalarFunction, AsyncScalarFunction)):
            _validate_zero_argument_class(func)
            hint_method, skip_first_parameter = _get_callable_class_hint_method(
                func, "eval"
            )
            if hint_method is None:
                raise TypeError(
                    f"Scalar UDF class '{func.__name__}': eval must be defined as a "
                    "method."
                )
            return _ResolvedUDFSource(
                func,
                _UDFSourceKind.SCALAR_FUNCTION_CLASS,
                issubclass(func, AsyncScalarFunction)
                or inspect.iscoroutinefunction(hint_method),
                _ignored_hint_names(
                    func, hint_method, skip_first=skip_first_parameter
                ),
            )

        if not _has_custom_call(func):
            raise TypeError(f"func must be callable, got {func.__name__}.")
        _validate_zero_argument_class(func)
        class_hint_method, skip_first_parameter = (
            _get_callable_class_hint_method(func)
        )
        if class_hint_method is None:
            raise TypeError(
                f"Callable class '{func.__name__}': __call__ must be defined as a method."
            )
        return _ResolvedUDFSource(
            func,
            _UDFSourceKind.CALLABLE_CLASS,
            inspect.iscoroutinefunction(class_hint_method),
            _ignored_hint_names(
                func, class_hint_method, skip_first=skip_first_parameter
            ),
        )

    if isinstance(func, UserDefinedFunction) and not isinstance(
        func, (ScalarFunction, AsyncScalarFunction)
    ):
        raise TypeError(f"func must be a scalar UDF, got {type(func).__name__}.")
    if isinstance(func, (ScalarFunction, AsyncScalarFunction)):
        hint_method = func.eval
        is_async = isinstance(func, AsyncScalarFunction) or inspect.iscoroutinefunction(
            hint_method
        )
        return _ResolvedUDFSource(
            func,
            _UDFSourceKind.SCALAR_FUNCTION_INSTANCE,
            is_async,
        )

    if not callable(func):
        raise TypeError(f"func must be callable, got {type(func).__name__}.")
    hint_method = cast(Callable[..., Any], getattr(func, "__call__"))
    return _ResolvedUDFSource(
        func,
        _UDFSourceKind.CALLABLE_INSTANCE,
        inspect.iscoroutinefunction(hint_method),
    )


def _ignored_hint_names(
    func: _UDFInput,
    inspection_target: Callable[..., Any],
    skip_first: bool = False,
) -> FrozenSet[str]:
    try:
        parameters = list(inspect.signature(inspection_target).parameters)
    except (TypeError, ValueError):
        parameters = []
    ignored_names = set(parameters[:1]) if skip_first else set()
    if isinstance(func, functools.partial):
        try:
            ignored_names.update(
                inspect.signature(inspection_target)
                .bind_partial(*func.args, **(func.keywords or {}))
                .arguments
            )
        except (TypeError, ValueError):
            pass
    return frozenset(ignored_names)


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

    return_hint = _get_callable_return_type_hint(func)
    if return_hint is _UNRESOLVED_TYPE_HINT:
        func_name = _default_udf_name(func)
        raise TypeError(
            f"Cannot infer return_dtype for '{func_name}': add a return annotation "
            "or specify return_dtype explicitly."
        )
    return _data_type_from_type_hint(return_hint)


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


def _detect_func_type(source: _ResolvedUDFSource) -> str:
    """Detect pandas mode from an unbound pandas container annotation."""
    hint_func = source.inspection_target
    try:
        import pandas as pd
    except ImportError:
        return "general"
    hints = _get_callable_type_hints(
        hint_func, fallback_globals={"pandas": pd, "pd": pd}
    )

    pandas_types = (pd.Series, pd.DataFrame)
    return (
        "pandas"
        if any(
            name not in source.ignored_hint_names and hint in pandas_types
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
        return get_type_hints(
            func,
            globalns={**fallback_globals, **_get_callable_globals(func)},
        )
    except (NameError, TypeError):
        return {}


def _get_callable_return_type_hint(func: Callable[..., Any]) -> Any:
    annotations = getattr(func, "__annotations__", {})
    if "return" not in annotations:
        return _UNRESOLVED_TYPE_HINT

    # Resolve the return annotation in isolation so an unresolvable parameter
    # annotation does not prevent return-type inference.
    def return_annotation_holder() -> None:
        pass

    return_annotation_holder.__annotations__ = {
        "return": annotations["return"]
    }
    try:
        return get_type_hints(
            return_annotation_holder,
            globalns=_get_callable_globals(func),
        ).get("return", _UNRESOLVED_TYPE_HINT)
    except (NameError, TypeError):
        return _UNRESOLVED_TYPE_HINT


def _get_callable_globals(func: Callable[..., Any]) -> Dict[str, Any]:
    func_globals = getattr(func, "__globals__", None)
    if func_globals is None:
        func_globals = getattr(
            getattr(func, "__func__", None), "__globals__", {}
        )
    return cast(Dict[str, Any], func_globals)


def _resolve_deterministic(
    source: _ResolvedUDFSource, deterministic: bool
) -> bool:
    if not isinstance(deterministic, bool):
        raise TypeError("deterministic must be a bool.")
    source.validate_deterministic(deterministic)
    return deterministic


def _validate_deterministic(declared: bool, actual: bool) -> None:
    if declared != actual:
        raise ValueError(f"Inconsistent deterministic: {declared} and {actual}.")


def _resolve_name(source: _ResolvedUDFSource, name: Optional[str]) -> str:
    actual_name = source.default_name if name is None else name
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
    func: Callable[..., Any],
    result_normalizer: Callable[[Any], Any],
    is_async: bool,
) -> Callable[..., Any]:
    if is_async:

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            return result_normalizer(await func(*args, **kwargs))

        wrapper = async_wrapper
    else:

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            return result_normalizer(func(*args, **kwargs))

        wrapper = sync_wrapper

    if not hasattr(func, "__name__"):
        wrapper.__name__ = type(func).__name__
    return wrapper


class _DataFrameUDFAdapterBase:
    """Bind a lazy DataFrame UDF source to one worker invocation protocol."""

    def __init__(
        self,
        source: _ResolvedUDFSource,
        return_dtype: DataType,
        deterministic: bool,
        usage: _UDFUsage,
        func_type: str,
    ) -> None:
        self._source = source
        self._func: Optional[_UDFInput] = (
            None if source.constructs_on_worker else source.source
        )
        self._return_dtype = return_dtype if func_type == "general" else None
        self._deterministic = deterministic
        self._usage = usage
        self._func_type = func_type
        self._bound_func: Optional[Callable[..., Any]] = None
        self._lifecycle_opened = False
        self.__name__ = source.default_name
        self.__doc__ = getattr(source.source, "__doc__", None)

    def open(self, function_context: Any) -> None:
        lifecycle_opened = False
        try:
            if self._source.constructs_on_worker:
                self._func = self._source.create_worker_source()

            func = self._func
            if func is None:
                raise RuntimeError("DataFrame UDF source was not initialized.")
            self._source.validate_deterministic(self._deterministic, func)
            self._source.open_worker_source(func, function_context)
            lifecycle_opened = self._source.is_scalar_function
            invoke_func = self._source.worker_invocation(func)
            self._bound_func = self._bind_func(invoke_func)
            self._lifecycle_opened = lifecycle_opened
        except Exception:
            if lifecycle_opened:
                try:
                    self._source.close_worker_source(self._func)
                except Exception:
                    pass
            self._bound_func = None
            self._lifecycle_opened = False
            if self._source.constructs_on_worker:
                self._func = None
            raise

    def _bind_func(self, invoke_func: Callable[..., Any]) -> Callable[..., Any]:
        if self._usage is not _UDFUsage.EXPRESSION:
            raise NotImplementedError(
                f"DataFrame UDF usage {self._usage.value!r} is not supported yet."
            )
        if self._func_type == "general":
            result_normalizer = _create_result_normalizer(
                cast(DataType, self._return_dtype)._to_table_data_type()
            )
            if result_normalizer is None:
                return invoke_func
            return _wrap_scalar_general_result(
                invoke_func,
                result_normalizer,
                self._source.is_async,
            )
        return invoke_func

    def close(self) -> None:
        func = self._func
        try:
            if self._lifecycle_opened:
                self._source.close_worker_source(func)
        finally:
            self._bound_func = None
            self._lifecycle_opened = False
            if self._source.constructs_on_worker:
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


def _row_field_values(value: Any, field_names: List[str]) -> List[Any]:
    if isinstance(value, Mapping):
        return [value.get(field_name) for field_name in field_names]
    if isinstance(value, Row) and hasattr(value, "_fields"):
        field_indices: Dict[str, int] = {}
        for index, field_name in enumerate(value._fields):
            field_indices.setdefault(field_name, index)
        field_values: List[Any] = []
        for field_name in field_names:
            if field_name not in field_indices:
                raise ValueError(
                    f"Field name {field_name!r} does not exist in Row fields "
                    f"{value._fields}."
                )
            field_index = field_indices[field_name]
            if field_index >= len(value):
                raise ValueError(
                    f"Field name {field_name!r} is declared in Row fields "
                    f"{value._fields} but has no value."
                )
            field_values.append(value[field_index])
        return field_values
    if isinstance(value, (Row, tuple, list)):
        field_count = len(field_names)
        if len(value) != field_count:
            raise ValueError(
                f"Expected {field_count} value(s) for RowType "
                f"{field_names}, got {len(value)}."
            )
        return list(value)
    return [
        _object_row_field_value(value, field_name, field_names)
        for field_name in field_names
    ]


def _object_row_field_value(
    value: Any, field_name: str, field_names: List[str]
) -> Any:
    try:
        return getattr(value, field_name)
    except AttributeError:
        try:
            inspect.getattr_static(value, field_name)
        except AttributeError:
            attributes = getattr(value, "__dict__", None)
            has_slots = any("__slots__" in cls.__dict__ for cls in type(value).__mro__)
            if isinstance(attributes, Mapping) or has_slots:
                return None
        else:
            raise
        raise TypeError(
            f"Expected a Mapping, Row, tuple, list, or object with fields for RowType "
            f"{field_names}, got {type(value).__name__}."
        ) from None


def _create_result_normalizer(
    data_type: Any,
) -> Optional[Callable[[Any], Any]]:
    if isinstance(data_type, RowType):
        field_names = data_type.field_names()
        field_normalizers = tuple(
            _create_result_normalizer(field.data_type) for field in data_type
        )

        def normalize_row(value: Any) -> Any:
            if value is None:
                return None
            field_values = _row_field_values(value, field_names)
            normalized_fields = [
                field_value
                if field_normalizer is None
                else field_normalizer(field_value)
                for field_value, field_normalizer in zip(
                    field_values, field_normalizers
                )
            ]
            row = Row(*normalized_fields)
            row.set_field_names(field_names)
            if isinstance(value, Row):
                row.set_row_kind(value.get_row_kind())
            return row

        return normalize_row
    if isinstance(data_type, ArrayType):
        element_normalizer = _create_result_normalizer(data_type.element_type)
        if element_normalizer is None:

            def normalize_leaf_array(value: Any) -> Any:
                return None if value is None else list(value)

            return normalize_leaf_array

        def normalize_array(value: Any) -> Any:
            if value is None:
                return None
            return [element_normalizer(item) for item in value]

        return normalize_array
    if isinstance(data_type, MapType):
        key_normalizer = _create_result_normalizer(data_type.key_type)
        value_normalizer = _create_result_normalizer(data_type.value_type)

        def normalize_map(value: Any) -> Any:
            if value is None:
                return None
            items_method = getattr(value, "items", None)
            if callable(items_method):
                items = list(cast(Iterable[Any], items_method()))
            else:
                try:
                    items = list(value)
                except TypeError as exc:
                    raise TypeError(
                        f"Expected a Mapping or iterable of key/value pairs for "
                        f"{data_type}, got {type(value).__name__}."
                    ) from exc
            if any(
                not isinstance(item, (tuple, list)) or len(item) != 2
                for item in items
            ):
                raise TypeError(
                    f"Expected a Mapping or iterable of key/value pairs for {data_type}, "
                    f"got {type(value).__name__}."
                )
            if any(item[0] is None for item in items):
                raise TypeError(f"MapType keys must not be null for {data_type}.")
            return {
                key if key_normalizer is None else key_normalizer(key): (
                    item_value
                    if value_normalizer is None
                    else value_normalizer(item_value)
                )
                for key, item_value in items
            }

        return normalize_map
    return None
