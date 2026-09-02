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
from dataclasses import dataclass, field
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
_ActiveUDFSource = Union[Callable[..., Any], ScalarFunction, AsyncScalarFunction]
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

    @property
    def is_scalar_function(self) -> bool:
        return self in (
            _UDFSourceKind.SCALAR_FUNCTION_INSTANCE,
            _UDFSourceKind.SCALAR_FUNCTION_CLASS,
        )


@dataclass(frozen=True)
class _UDFDeclarationContext:
    """Client-only metadata used while declaring a UDF."""

    annotation_target: Callable[..., Any]
    defining_class: Optional[Type]
    globalns: Dict[str, Any]
    localns: Optional[Dict[str, Any]]
    ignored_hint_names: FrozenSet[str]


@dataclass(frozen=True)
class _UDFRuntimeSource:
    """Worker-facing recipe used to initialize a UDF."""

    callable_source: _UDFInput
    kind: _UDFSourceKind
    is_async: bool

    @property
    def default_name(self) -> str:
        return _default_udf_name(self.callable_source)

    @property
    def constructs_on_worker(self) -> bool:
        return self.kind in (
            _UDFSourceKind.CALLABLE_CLASS,
            _UDFSourceKind.SCALAR_FUNCTION_CLASS,
        )

    def validate_declared_determinism(self, declared: bool) -> None:
        if self.kind is _UDFSourceKind.SCALAR_FUNCTION_INSTANCE:
            actual = cast(
                Union[ScalarFunction, AsyncScalarFunction], self.callable_source
            ).is_deterministic()
            _validate_determinism_agreement(declared, actual)

    def create_worker_udf(self) -> "_WorkerUDF":
        source = self.callable_source
        if self.constructs_on_worker:
            source_class = cast(Type, source)
            source = source_class()
            if self.kind.is_scalar_function:
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
        return _WorkerUDF(cast(_ActiveUDFSource, source), self.kind)


@dataclass(frozen=True)
class _ResolvedUDF:
    """A resolved declaration split into client and worker metadata."""

    runtime_source: _UDFRuntimeSource
    declaration_context: _UDFDeclarationContext


@dataclass
class _WorkerUDF:
    """An initialized UDF owned by one worker adapter lifecycle."""

    active_source: _ActiveUDFSource
    kind: _UDFSourceKind
    _lifecycle_opened: bool = field(default=False, init=False, repr=False)

    def validate_deterministic(self, declared: bool) -> None:
        if self.kind is _UDFSourceKind.SCALAR_FUNCTION_CLASS:
            actual = cast(
                Union[ScalarFunction, AsyncScalarFunction], self.active_source
            ).is_deterministic()
            _validate_determinism_agreement(declared, actual)

    def open(self, function_context: Any) -> None:
        if self.kind.is_scalar_function:
            cast(
                Union[ScalarFunction, AsyncScalarFunction], self.active_source
            ).open(function_context)
            self._lifecycle_opened = True

    @property
    def invocation(self) -> Callable[..., Any]:
        if self.kind is _UDFSourceKind.DIRECT_CALLABLE:
            return cast(Callable[..., Any], self.active_source)
        if self.kind.is_scalar_function:
            return cast(
                Union[ScalarFunction, AsyncScalarFunction], self.active_source
            ).eval
        return cast(Callable[..., Any], getattr(self.active_source, "__call__"))

    def close(self) -> None:
        try:
            if self._lifecycle_opened:
                cast(
                    Union[ScalarFunction, AsyncScalarFunction], self.active_source
                ).close()
        finally:
            self._lifecycle_opened = False


class _DataFrameUDFWrapper:
    """Internal callable binding a DataFrame scalar UDF to Table expressions."""

    _runtime_source: _UDFRuntimeSource
    _return_dtype: DataType
    _deterministic: bool
    _func_type: str
    _cached_table_udf_wrapper: Optional[UserDefinedFunctionWrapper]
    _frozen: bool
    __name__: str

    def __init__(
        self,
        runtime_source: _UDFRuntimeSource,
        return_dtype: DataType,
        deterministic: bool,
        name: str,
        func_type: str,
    ) -> None:
        object.__setattr__(self, "_runtime_source", runtime_source)
        object.__setattr__(self, "_return_dtype", return_dtype)
        object.__setattr__(self, "_deterministic", deterministic)
        object.__setattr__(self, "_func_type", func_type)
        object.__setattr__(self, "_cached_table_udf_wrapper", None)

        declaration_metadata = _unwrap_partial(runtime_source.callable_source)
        for attribute_name in ("__module__", "__qualname__", "__doc__"):
            try:
                attribute_value = getattr(declaration_metadata, attribute_name)
            except AttributeError:
                continue
            object.__setattr__(self, attribute_name, attribute_value)
        object.__setattr__(self, "__name__", name)
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
            if self._runtime_source.is_async
            else _DataFrameScalarFunctionAdapter
        )
        actual_func = cast(
            Union[ScalarFunction, AsyncScalarFunction],
            adapter_type(
                self._runtime_source,
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
        resolved_udf = _resolve_udf(f)
        runtime_source = resolved_udf.runtime_source
        declaration_context = resolved_udf.declaration_context
        actual_func_type = (
            func_type
            if func_type is not None
            else _detect_func_type(declaration_context)
        )
        _validate_scalar_udf_options(
            actual_func_type, return_dtype, runtime_source.is_async
        )
        actual_return_dtype = _infer_return_dtype(
            declaration_context, return_dtype, runtime_source.default_name
        )
        if not isinstance(deterministic, bool):
            raise TypeError("deterministic must be a bool.")
        runtime_source.validate_declared_determinism(deterministic)
        actual_name = runtime_source.default_name if name is None else name
        if not isinstance(actual_name, str):
            raise TypeError("name must be a str or None.")
        if not actual_name:
            raise ValueError("name must not be empty.")

        return _DataFrameUDFWrapper(
            runtime_source,
            actual_return_dtype,
            deterministic,
            actual_name,
            actual_func_type,
        )

    return decorator if func is None else decorator(func)


# ======================== Declaration Validation ========================


def _validate_determinism_agreement(declared: bool, actual: bool) -> None:
    if declared != actual:
        raise ValueError(
            f"Inconsistent deterministic: {declared} and {actual}."
        )


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

# ---- Invocation target resolution ----


def _unwrap_partial(func: Any) -> Any:
    while isinstance(func, functools.partial):
        func = func.func
    return func


def _default_udf_name(func: _UDFInput) -> str:
    target = _unwrap_partial(func)
    name = getattr(target, "__name__", None)
    return name if isinstance(name, str) else type(target).__name__


def _get_callable_inspection_target(
    func: Callable[..., Any],
) -> Callable[..., Any]:
    target = _unwrap_partial(func)
    if callable(target) and not inspect.isroutine(target) and not inspect.isclass(target):
        return cast(Callable[..., Any], getattr(target, "__call__"))
    return cast(Callable[..., Any], target)


def _first_parameter_name(func: Callable[..., Any]) -> Optional[str]:
    try:
        parameters = tuple(
            inspect.signature(func, follow_wrapped=False).parameters.values()
        )
    except (TypeError, ValueError):
        return None
    return parameters[0].name if parameters else None


def _resolve_class_invocation_target(
    func_class: Type, method_name: str
) -> Tuple[Optional[Callable[..., Any]], Optional[Type], Optional[str]]:
    """Resolve the nearest supported invocation method without constructing a class."""
    descriptor_owner = None
    descriptor = None
    for candidate in func_class.__mro__:
        if method_name in candidate.__dict__:
            descriptor_owner = candidate
            descriptor = candidate.__dict__[method_name]
            break

    if descriptor_owner is None:
        return None, None, None
    if isinstance(descriptor, staticmethod):
        target = descriptor.__func__
        implicit_parameter_name = None
    elif isinstance(descriptor, classmethod):
        target = descriptor.__func__
        implicit_parameter_name = (
            _first_parameter_name(target) if inspect.isroutine(target) else None
        )
    elif inspect.isroutine(descriptor):
        target = descriptor
        implicit_parameter_name = _first_parameter_name(target)
    else:
        return None, descriptor_owner, None

    if not callable(target) or not inspect.isroutine(target):
        return None, descriptor_owner, None
    return cast(Callable[..., Any], target), descriptor_owner, implicit_parameter_name


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


# ---- Annotation namespace resolution ----


def _function_qualname(func: Callable[..., Any]) -> Optional[str]:
    target = _unwrap_partial(func)
    target = getattr(target, "__func__", target)
    qualname = getattr(target, "__qualname__", None)
    return qualname if isinstance(qualname, str) else None


def _lexical_defining_class(
    target: Callable[..., Any], candidate: Optional[Type] = None
) -> Optional[Type]:
    qualname = _function_qualname(target)
    if qualname is None:
        return None
    owner_qualname, separator, _ = qualname.rpartition(".")
    if not separator:
        return None
    if candidate is not None:
        # Same-qualified-name method transplantation is indistinguishable because
        # Python functions do not retain an exact defining-class identity.
        return candidate if candidate.__qualname__ == owner_qualname else None

    bound_target = _unwrap_partial(target)
    receiver = getattr(bound_target, "__self__", None)
    if receiver is None:
        return None
    receiver_class = receiver if inspect.isclass(receiver) else type(receiver)
    return next(
        (
            owner
            for owner in receiver_class.__mro__
            if owner.__qualname__ == owner_qualname
        ),
        None,
    )


def _get_callable_globals(func: Callable[..., Any]) -> Dict[str, Any]:
    func_globals = getattr(func, "__globals__", None)
    if func_globals is None:
        func_globals = getattr(
            getattr(func, "__func__", None), "__globals__", {}
        )
    return cast(Dict[str, Any], func_globals)


def _get_annotation_globals(func: Callable[..., Any]) -> Dict[str, Any]:
    try:
        unwrapped = inspect.unwrap(func)
    except ValueError:
        return _get_callable_globals(func)

    annotations = getattr(func, "__annotations__", None)
    if annotations is not None and annotations is getattr(
        unwrapped, "__annotations__", None
    ):
        return _get_callable_globals(unwrapped)
    return _get_callable_globals(func)


# ---- Signature and declaration context assembly ----


def _preserves_method_binding(
    target: Callable[..., Any], defining_class: Optional[Type]
) -> bool:
    target = _unwrap_partial(target)
    target = getattr(target, "__func__", target)
    if defining_class is None or not hasattr(target, "__wrapped__"):
        return False
    try:
        unwrapped_target = inspect.unwrap(target)
    except ValueError:
        return False
    return _lexical_defining_class(
        cast(Callable[..., Any], unwrapped_target), defining_class
    ) is defining_class


def _resolve_ignored_hint_names(
    annotation_target: Callable[..., Any],
    implicit_parameter_name: Optional[str],
    partial_source: Any,
    preserves_method_binding: bool,
) -> FrozenSet[str]:
    ignored_hint_names = set()
    if implicit_parameter_name is not None:
        ignored_hint_names.add(implicit_parameter_name)

    if not isinstance(partial_source, functools.partial):
        return frozenset(ignored_hint_names)

    bound_function = getattr(annotation_target, "__func__", None)
    is_wrapped_bound_method = bound_function is not None and hasattr(
        bound_function, "__wrapped__"
    )
    uses_unbound_wrapped_signature = (
        is_wrapped_bound_method and not preserves_method_binding
    )
    try:
        partial_target_signature = inspect.signature(
            cast(Callable[..., Any], bound_function)
            if uses_unbound_wrapped_signature
            else partial_source.func
        )
    except Exception:
        return frozenset(ignored_hint_names)
    try:
        bound_arguments = partial_target_signature.bind_partial(
            *partial_source.args, **(partial_source.keywords or {})
        )
    except TypeError as exc:
        raise TypeError(
            f"Invalid functools.partial UDF "
            f"'{_default_udf_name(partial_source)}': {exc}."
        ) from exc
    ignored_hint_names.update(bound_arguments.arguments)
    return frozenset(ignored_hint_names)


def _create_declaration_context(
    annotation_target: Callable[..., Any],
    *,
    descriptor_owner: Optional[Type] = None,
    implicit_parameter_name: Optional[str] = None,
    partial_source: Any = None,
) -> _UDFDeclarationContext:
    annotation_target = cast(
        Callable[..., Any], _get_callable_inspection_target(annotation_target)
    )
    defining_class = _lexical_defining_class(
        annotation_target, descriptor_owner
    )
    preserves_method_binding = _preserves_method_binding(
        annotation_target, defining_class
    )
    if preserves_method_binding:
        implicit_parameter_name = _first_parameter_name(
            cast(Callable[..., Any], inspect.unwrap(annotation_target))
        )
    if implicit_parameter_name is None:
        bound_target = _unwrap_partial(annotation_target)
        bound_function = getattr(bound_target, "__func__", None)
        if bound_function is not None:
            implicit_parameter_name = _first_parameter_name(bound_function)

    localns = None
    if defining_class is not None:
        localns = dict(vars(defining_class))
        localns[defining_class.__name__] = defining_class

    ignored_hint_names = _resolve_ignored_hint_names(
        annotation_target,
        implicit_parameter_name,
        partial_source,
        preserves_method_binding,
    )
    return _UDFDeclarationContext(
        annotation_target=annotation_target,
        defining_class=defining_class,
        globalns=_get_annotation_globals(annotation_target),
        localns=localns,
        ignored_hint_names=ignored_hint_names,
    )


def _create_resolved_udf(
    func: _UDFInput,
    kind: _UDFSourceKind,
    declaration_context: _UDFDeclarationContext,
    *,
    async_marker: bool = False,
) -> _ResolvedUDF:
    target = declaration_context.annotation_target
    target_is_async = inspect.iscoroutinefunction(target)
    try:
        unwrapped_target = inspect.unwrap(target)
    except ValueError as exc:
        raise TypeError(
            "Cannot inspect a UDF with a wrapper cycle."
        ) from exc
    if not target_is_async and inspect.iscoroutinefunction(unwrapped_target):
        raise TypeError(
            "A synchronous UDF wrapper cannot wrap an async target; define the "
            "wrapper with async def."
        )
    if async_marker and not target_is_async:
        raise TypeError(
            f"AsyncScalarFunction '{_default_udf_name(func)}': eval must be "
            "defined with async def."
        )
    is_async = target_is_async
    return _ResolvedUDF(
        _UDFRuntimeSource(func, kind, is_async), declaration_context
    )


def _resolve_udf(func: _UDFInput) -> _ResolvedUDF:
    """Validate a UDF and resolve its declaration and runtime metadata."""
    if isinstance(func, functools.partial) or inspect.isroutine(func):
        declaration_context = _create_declaration_context(
            cast(Callable[..., Any], func),
            partial_source=func,
        )
        return _create_resolved_udf(
            func, _UDFSourceKind.DIRECT_CALLABLE, declaration_context
        )

    if inspect.isclass(func):
        if issubclass(func, UserDefinedFunction) and not issubclass(
            func, (ScalarFunction, AsyncScalarFunction)
        ):
            raise TypeError(f"func must be a scalar UDF, got {func.__name__}.")
        if issubclass(func, (ScalarFunction, AsyncScalarFunction)):
            _validate_zero_argument_class(func)
            target, descriptor_owner, implicit_parameter_name = (
                _resolve_class_invocation_target(func, "eval")
            )
            if target is None:
                raise TypeError(
                    f"Scalar UDF class '{func.__name__}' has an unsupported eval "
                    "definition.\nDefine eval as an instance, class, or static method."
                )
            declaration_context = _create_declaration_context(
                target,
                descriptor_owner=descriptor_owner,
                implicit_parameter_name=implicit_parameter_name,
            )
            return _create_resolved_udf(
                func,
                _UDFSourceKind.SCALAR_FUNCTION_CLASS,
                declaration_context,
                async_marker=issubclass(func, AsyncScalarFunction),
            )

        target, descriptor_owner, implicit_parameter_name = (
            _resolve_class_invocation_target(func, "__call__")
        )
        if target is None:
            if descriptor_owner is None:
                raise TypeError(f"func must be callable, got {func.__name__}.")
            raise TypeError(
                f"Callable class '{func.__name__}' has an unsupported __call__ "
                "definition.\nDefine __call__ as an instance, class, or static method."
            )
        _validate_zero_argument_class(func)
        declaration_context = _create_declaration_context(
            target,
            descriptor_owner=descriptor_owner,
            implicit_parameter_name=implicit_parameter_name,
        )
        return _create_resolved_udf(
            func, _UDFSourceKind.CALLABLE_CLASS, declaration_context
        )

    if isinstance(func, UserDefinedFunction) and not isinstance(
        func, (ScalarFunction, AsyncScalarFunction)
    ):
        raise TypeError(f"func must be a scalar UDF, got {type(func).__name__}.")
    if isinstance(func, (ScalarFunction, AsyncScalarFunction)):
        target = func.eval
        if not callable(target):
            raise TypeError(
                f"Scalar UDF instance '{type(func).__name__}': eval must be callable."
            )
        declaration_context = _create_declaration_context(
            cast(Callable[..., Any], target),
            partial_source=target,
        )
        return _create_resolved_udf(
            func,
            _UDFSourceKind.SCALAR_FUNCTION_INSTANCE,
            declaration_context,
            async_marker=isinstance(func, AsyncScalarFunction),
        )

    if not callable(func):
        raise TypeError(f"func must be callable, got {type(func).__name__}.")
    target = getattr(func, "__call__")
    if not callable(target):
        raise TypeError(
            f"Callable instance '{type(func).__name__}': __call__ must be callable."
        )
    declaration_context = _create_declaration_context(
        cast(Callable[..., Any], target),
        partial_source=target,
    )
    return _create_resolved_udf(
        func, _UDFSourceKind.CALLABLE_INSTANCE, declaration_context
    )


# ---- Annotation and type inference ----


def _resolve_callable_annotation(
    declaration_context: _UDFDeclarationContext,
    annotation_name: str,
    globalns: Optional[Dict[str, Any]] = None,
) -> Any:
    func = declaration_context.annotation_target
    annotations = getattr(func, "__annotations__", {})
    if annotation_name not in annotations:
        return _UNRESOLVED_TYPE_HINT

    def annotation_holder() -> None:
        pass

    annotation_holder.__annotations__ = {
        annotation_name: annotations[annotation_name]
    }
    try:
        return get_type_hints(
            annotation_holder,
            globalns=(
                declaration_context.globalns if globalns is None else globalns
            ),
            localns=declaration_context.localns,
        ).get(annotation_name, _UNRESOLVED_TYPE_HINT)
    except (NameError, AttributeError, SyntaxError, TypeError):
        return _UNRESOLVED_TYPE_HINT


def _get_callable_return_type_hint(
    declaration_context: _UDFDeclarationContext,
) -> Any:
    # Resolve the return annotation in isolation so an unresolvable parameter
    # annotation does not prevent return-type inference.
    return _resolve_callable_annotation(declaration_context, "return")


def _infer_return_dtype(
    declaration_context: _UDFDeclarationContext,
    return_dtype: Optional[_DataTypeLike],
    udf_name: str,
) -> DataType:
    """Infer the DataFrame return type or validate its explicit declaration."""
    if return_dtype is not None:
        return _convert_to_dtype(return_dtype)

    annotations = getattr(
        declaration_context.annotation_target, "__annotations__", {}
    ) or {}
    if "return" not in annotations:
        raise TypeError(
            f"Cannot infer return_dtype for '{udf_name}': add a return annotation "
            "or specify return_dtype explicitly."
        )

    return_hint = _get_callable_return_type_hint(declaration_context)
    if return_hint is _UNRESOLVED_TYPE_HINT:
        raise TypeError(
            f"Cannot infer return_dtype for '{udf_name}' from its return annotation.\n"
            "Specify return_dtype explicitly."
        )
    try:
        return _data_type_from_type_hint(return_hint)
    except (NameError, AttributeError, SyntaxError, TypeError) as exc:
        raise TypeError(
            f"Cannot infer return_dtype for '{udf_name}' from its return annotation.\n"
            "Specify return_dtype explicitly."
        ) from exc


def _convert_to_dtype(dtype_like: _DataTypeLike) -> DataType:
    if isinstance(dtype_like, DataType):
        return dtype_like
    if isinstance(dtype_like, str):
        return DataType._from_sql(dtype_like)
    try:
        return _data_type_from_type_hint(dtype_like)
    except (NameError, AttributeError, SyntaxError, TypeError) as exc:
        if _is_typed_dict(dtype_like):
            raise TypeError(
                "Cannot resolve return_dtype from the supplied TypedDict; use a "
                "concrete DataFrame DataType or SQL type string."
            ) from exc
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


def _detect_func_type(declaration_context: _UDFDeclarationContext) -> str:
    """Detect pandas mode from an unbound pandas container annotation."""
    hint_func = declaration_context.annotation_target
    try:
        import pandas as pd
    except ImportError:
        return "general"

    pandas_types = (pd.Series, pd.DataFrame)
    pandas_globalns = {
        "pandas": pd,
        "pd": pd,
        **declaration_context.globalns,
    }
    for name in getattr(hint_func, "__annotations__", {}):
        if name in declaration_context.ignored_hint_names:
            continue
        hint = _resolve_callable_annotation(
            declaration_context,
            name,
            globalns=pandas_globalns,
        )
        if hint in pandas_types:
            return "pandas"
    return "general"


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
        runtime_source: _UDFRuntimeSource,
        return_dtype: DataType,
        deterministic: bool,
        usage: _UDFUsage,
        func_type: str,
    ) -> None:
        self._runtime_source = runtime_source
        self._worker_udf: Optional[_WorkerUDF] = None
        self._return_dtype = return_dtype if func_type == "general" else None
        self._deterministic = deterministic
        self._usage = usage
        self._func_type = func_type
        self._bound_invocation: Optional[Callable[..., Any]] = None
        self.__name__ = runtime_source.default_name
        self.__doc__ = getattr(runtime_source.callable_source, "__doc__", None)

    def open(self, function_context: Any) -> None:
        worker_udf = self._runtime_source.create_worker_udf()
        try:
            worker_udf.validate_deterministic(self._deterministic)
            worker_udf.open(function_context)
            self._bound_invocation = self._bind_func(worker_udf.invocation)
            self._worker_udf = worker_udf
        except Exception:
            try:
                worker_udf.close()
            except Exception:
                pass
            self._bound_invocation = None
            self._worker_udf = None
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
                self._runtime_source.is_async,
            )
        return invoke_func

    def close(self) -> None:
        worker_udf = self._worker_udf
        try:
            if worker_udf is not None:
                worker_udf.close()
        finally:
            self._bound_invocation = None
            self._worker_udf = None

    def is_deterministic(self) -> bool:
        return self._deterministic

    def _invocation(self) -> Callable[..., Any]:
        if self._bound_invocation is None:
            raise RuntimeError("DataFrame UDF was invoked before open().")
        return self._bound_invocation


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
