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

"""User-defined table functions for the DataFrame API."""

import collections.abc
import functools
import inspect
import types
from dataclasses import dataclass
from typing import (
    Annotated, Any, Callable, Iterator, List, Optional, Tuple, Type, Union, cast, get_args,
    get_origin, overload,
)

from pyflink.common import Row
from pyflink.dataframe.datatype import DataType
from pyflink.dataframe.udf import (
    _DataTypeLike,
    _UDFDeclarationContext,
    _UNRESOLVED_TYPE_HINT,
    _convert_to_dtype,
    _create_declaration_context,
    _create_result_normalizer,
    _data_type_from_type_hint,
    _get_callable_inspection_target,
    _get_callable_return_type_hint,
    _is_typed_dict,
    _preserves_method_binding,
    _resolve_callable_annotation,
    _resolve_udf,
    _validate_determinism_agreement,
    _validate_zero_argument_class,
)
from pyflink.table.expression import Expression
from pyflink.table.expressions import col, with_columns
from pyflink.table.types import RowType, _to_java_data_type
from pyflink.table.udf import (
    TableFunction,
    UserDefinedFunction,
    UserDefinedTableFunctionWrapper,
    udtf as table_udtf,
)
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ["udtf"]

_UDTFInput = Union[Callable[..., Any], TableFunction, Type]


@dataclass(frozen=True)
class _DataFrameUDTFCall:
    expression: Expression
    return_dtype: DataType
    has_named_fields: bool
    output_aliases: Optional[Tuple[str, ...]] = None

    def alias(self, name: str, *extra_names: str) -> "_DataFrameUDTFCall":
        names = (name,) + extra_names
        table_type = self.return_dtype._to_table_data_type()
        arity = len(table_type.fields) if isinstance(table_type, RowType) else 1
        if any(not isinstance(value, str) for value in names):
            raise TypeError("UDTF output aliases must be strings.")
        if len(names) != arity or len(set(names)) != len(names) or not all(names):
            raise ValueError(
                "UDTF output aliases must be non-empty, unique, and match output arity.")
        return _DataFrameUDTFCall(
            self.expression.alias(*names), self.return_dtype, self.has_named_fields, names
        )


@dataclass(frozen=True)
class _DataFrameUDTFWrapper:
    _func: _UDTFInput
    _declaration_context: _UDFDeclarationContext
    return_dtype: DataType
    _has_named_fields: bool
    _deterministic: bool
    _name: Optional[str]

    @property
    def __name__(self) -> str:
        return self._create_table_wrapper()._name

    def __call__(self, *args: Any) -> _DataFrameUDTFCall:
        return _DataFrameUDTFCall(
            self._create_table_wrapper()(*args), self.return_dtype, self._has_named_fields
        )

    def _create_table_wrapper(
        self, input_columns: Optional[Tuple[str, ...]] = None, *,
        preserve_field_names: bool = False,
    ) -> UserDefinedTableFunctionWrapper:
        """Keep Table's mutable row-input flag local to each use of this declaration."""
        result_types = self.return_dtype._to_table_data_type()
        if preserve_field_names and isinstance(result_types, RowType):
            # Table's RowType path drops field names; its SQL type declaration preserves them.
            result_types = _to_java_data_type(result_types).getLogicalType().asSerializableString()
        return cast(UserDefinedTableFunctionWrapper, table_udtf(
            _DataFrameTableFunctionAdapter(
                self._func,
                self.return_dtype,
                self._deterministic,
                input_columns,
            ),
            result_types=result_types,
            deterministic=self._deterministic,
            name=self._name,
        ))


@overload
def udtf(
    func: _UDTFInput,
    *,
    return_dtype: Optional[_DataTypeLike] = ...,
    deterministic: bool = ...,
    name: Optional[str] = ...,
) -> _DataFrameUDTFWrapper:
    ...


@overload
def udtf(
    func: None = ...,
    *,
    return_dtype: Optional[_DataTypeLike] = ...,
    deterministic: bool = ...,
    name: Optional[str] = ...,
) -> Callable[[_UDTFInput], _DataFrameUDTFWrapper]:
    ...


@PublicEvolving()
def udtf(
    func: Optional[_UDTFInput] = None,
    *,
    return_dtype: Optional[_DataTypeLike] = None,
    deterministic: bool = True,
    name: Optional[str] = None,
) -> Union[_DataFrameUDTFWrapper, Callable[[_UDTFInput], _DataFrameUDTFWrapper]]:
    """
    Declare a synchronous function that emits zero or more rows per invocation.

    Supports functions, callable classes and instances, and ``TableFunction``
    classes and instances. Classes must have a zero-argument constructor. Plain
    callable classes are instantiated on workers, as with :func:`pyflink.dataframe.udf`.
    ``TableFunction`` classes are instantiated on the client when declared;
    their ``open`` and ``close`` methods run on workers. Explicitly provided
    instances are serialized with the job.

    The output type may be explicit or inferred from ``Iterator[T]``, ``Iterable[T]``,
    ``Generator[T, ...]``, or ``list[T]``. A ``TypedDict`` supplies output field names.
    Return ``None`` for no rows, a list/generator for multiple rows, or a scalar,
    tuple, ``Row``, or dict for one row. Nested output values follow scalar UDF rules.

    When used with :meth:`DataFrame.flat_map`, the function receives a dictionary
    keyed by input column name. Expression and SQL calls pass the specified arguments.

    Example::

        >>> from typing import Any, Dict, Iterator, TypedDict
        >>> import pyflink.dataframe as pf

        >>> class Output(TypedDict):
        ...     value: int

        >>> @pf.udtf
        ... def expand(record: Dict[str, Any]) -> Iterator[Output]:
        ...     yield {"value": record["x"]}
        ...     yield {"value": record["x"] + 1}

        >>> df = pf.from_dict({"x": [1, 2]})
        >>> result = df.flat_map(expand)

    Pass a callable class directly for a one-off ``flat_map`` operation. The class
    must support zero-argument construction and is instantiated on workers::

        >>> class Repeat:
        ...     def __init__(self, times=2):
        ...         self.times = times
        ...
        ...     def __call__(self, record: Dict[str, Any]):
        ...         for _ in range(self.times):
        ...             yield record["x"]

        >>> repeated = df.flat_map(Repeat, return_dtype=int)
        >>> repeated.columns
        ['f0']

    Create a UDTF declaration when the callable will be reused or its metadata should
    be declared once. Passing the class keeps worker-side construction::

        >>> repeat = pf.udtf(Repeat, return_dtype=int)
        >>> first = df.flat_map(repeat)
        >>> second = df.flat_map(repeat)

    When constructor arguments are needed, create an instance on the client. Pass it
    directly for one-off use, or wrap it with :func:`udtf` to reuse the declaration
    without repeating its metadata::

        >>> repeat_three_times = Repeat(3)
        >>> one_off = df.flat_map(repeat_three_times, return_dtype=int)
        >>> reusable_repeat_three_times = pf.udtf(repeat_three_times, return_dtype=int)
        >>> reused = df.flat_map(reusable_repeat_three_times)

    A ``TableFunction`` class is instantiated on the client when declared. Use an
    instance for constructor configuration; in both cases, ``open`` runs on workers::

        >>> from pyflink.table.udf import TableFunction

        >>> class Expand(TableFunction):
        ...     def __init__(self, offset=1):
        ...         self.offset = offset
        ...
        ...     def open(self, context):
        ...         self.worker_offset = self.offset
        ...
        ...     def eval(self, record: Dict[str, Any]) -> Iterator[int]:
        ...         yield record["x"] + self.worker_offset

        >>> default_expand = pf.udtf(Expand)
        >>> configured_expand = pf.udtf(Expand(offset=10))
        >>> default_result = df.flat_map(default_expand)
        >>> configured_result = df.flat_map(configured_expand)

    :param func: Function, callable class/instance, or ``TableFunction`` class/instance.
    :param return_dtype: Emitted row type, as a DataFrame DataType, Python type,
                         or SQL type string. Inferred from annotations when omitted.
    :param deterministic: Whether equal inputs produce equal results; must agree
                          with ``TableFunction.is_deterministic``.
    :param name: Optional function name. None or an empty string uses the callable's name.
    :return: A reusable UDTF declaration, or a decorator when ``func`` is omitted.

    .. versionadded:: 2.4.0
    """
    def decorator(f: _UDTFInput) -> _DataFrameUDTFWrapper:
        actual_func, context = _resolve_udtf(f)
        dtype, has_named_fields = _infer_udtf_return_dtype(context, return_dtype)
        if not isinstance(deterministic, bool):
            raise TypeError("deterministic must be a bool.")
        if isinstance(actual_func, TableFunction):
            _validate_determinism_agreement(deterministic, actual_func.is_deterministic())
        return _DataFrameUDTFWrapper(
            actual_func, context, dtype, has_named_fields, deterministic, name)

    return decorator if func is None else decorator(func)


def _resolve_udtf(
    func: _UDTFInput,
) -> Tuple[_UDTFInput, _UDFDeclarationContext]:
    if inspect.isclass(func) and issubclass(func, TableFunction):
        _validate_zero_argument_class(func)
        func = func()
        if not isinstance(func, TableFunction):
            raise TypeError("A TableFunction class must construct a TableFunction instance.")
    if isinstance(func, TableFunction):
        if not callable(func.eval):
            raise TypeError("TableFunction.eval must be callable.")
        context = _create_declaration_context(func.eval, partial_source=func.eval)
    else:
        if isinstance(func, UserDefinedFunction) or (
            inspect.isclass(func) and issubclass(func, UserDefinedFunction)
        ):
            raise TypeError("func must be a table UDF or a Python callable.")
        context = _resolve_udf(func).declaration_context
    target = context.annotation_target
    try:
        unwrapped_target = inspect.unwrap(target)
    except ValueError as exc:
        raise TypeError("Cannot inspect a UDTF with a wrapper cycle.") from exc
    if any(
        inspect.iscoroutinefunction(candidate) or inspect.isasyncgenfunction(candidate)
        for candidate in (target, unwrapped_target)
    ):
        raise TypeError("DataFrame UDTFs must be synchronous; async functions are not supported.")
    return func, context


def _infer_udtf_return_dtype(
    context: _UDFDeclarationContext, return_dtype: Optional[_DataTypeLike]
) -> Tuple[DataType, bool]:
    if return_dtype is not None:
        dtype = _convert_to_dtype(return_dtype)
        has_named_fields = isinstance(dtype._to_table_data_type(), RowType)
    else:
        hint = _get_callable_return_type_hint(context)
        origin, arguments = get_origin(hint), get_args(hint)
        if origin in (
            collections.abc.Iterator, collections.abc.Iterable, collections.abc.Generator, list
        ) and arguments:
            hint = arguments[0]
        elif hint is _UNRESOLVED_TYPE_HINT or origin in (
            collections.abc.Iterator, collections.abc.Iterable, collections.abc.Generator, list
        ):
            raise TypeError("Cannot infer UDTF return type; specify return_dtype explicitly.")
        if get_origin(hint) is tuple:
            fields = get_args(hint)
            if not fields or Ellipsis in fields:
                raise TypeError("UDTF tuple outputs must have a fixed number of fields.")
            dtype = DataType.struct([
                (f"f{index}", _data_type_from_type_hint(field))
                for index, field in enumerate(fields)
            ])
            has_named_fields = False
        else:
            dtype = _data_type_from_type_hint(hint)
            has_named_fields = isinstance(dtype._to_table_data_type(), RowType)
    table_type = dtype._to_table_data_type()
    if isinstance(table_type, RowType) and not table_type.fields:
        raise ValueError("A UDTF must declare at least one output field.")
    return dtype, has_named_fields


def _validate_flat_map_input(declaration: _DataFrameUDTFWrapper) -> None:
    source = declaration._func
    target: Callable[..., Any]
    if inspect.isclass(source):
        target = declaration._declaration_context.annotation_target
    elif isinstance(source, TableFunction):
        target = source.eval
    elif isinstance(source, functools.partial):
        target = source
    else:
        target = _get_callable_inspection_target(source)
    if inspect.ismethod(target) and hasattr(target, "__wrapped__") and not (
        _preserves_method_binding(target, declaration._declaration_context.defining_class)
    ):
        target = target.__func__
    try:
        signature = inspect.signature(target)
    except (TypeError, ValueError):
        return
    if inspect.isclass(source):
        descriptor = inspect.getattr_static(source, "__call__")
        binds_receiver = not isinstance(descriptor, staticmethod)
        if hasattr(target, "__wrapped__"):
            binds_receiver = binds_receiver and _preserves_method_binding(
                target, declaration._declaration_context.defining_class)
        parameters = list(signature.parameters.values())
        if binds_receiver and parameters and parameters[0].kind in (
            inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD
        ):
            signature = signature.replace(parameters=parameters[1:])
    parameters = list(signature.parameters.values())
    try:
        signature.bind(object())
    except TypeError as exc:
        raise ValueError("flat_map requires a function accepting one row argument.") from exc
    positional = [p for p in parameters if p.kind in (
        inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD
    )]
    if not positional:
        return
    hint = _resolve_callable_annotation(declaration._declaration_context, positional[0].name)
    if not _is_flat_map_row_hint(hint):
        raise ValueError(f"flat_map receives one dict row argument, got annotation {hint}.")


def _is_flat_map_row_hint(hint: Any) -> bool:
    if hint in (_UNRESOLVED_TYPE_HINT, Any, object):
        return True
    origin = get_origin(hint)
    if origin is Annotated:
        return _is_flat_map_row_hint(get_args(hint)[0])
    if origin in (Union, getattr(types, "UnionType", Union)):
        return any(_is_flat_map_row_hint(member) for member in get_args(hint))
    if _is_typed_dict(hint):
        return True
    target = origin or hint
    try:
        return issubclass(dict, target)
    except TypeError:
        # Leave annotations that cannot be checked at runtime to the user's type checker.
        return True


def _resolve_flat_map_udtf(
    func: Union[Callable[..., Any], Type, _DataFrameUDTFWrapper],
    return_dtype: Optional[_DataTypeLike],
    input_columns: List[str],
) -> Tuple[Expression, List[str]]:
    if isinstance(func, _DataFrameUDTFWrapper):
        if return_dtype is not None:
            raise ValueError("return_dtype must not be specified for a DataFrame UDTF declaration.")
        declaration = func
    else:
        if func is None:
            raise TypeError("flat_map requires a callable or a pf.udtf declaration.")
        if isinstance(func, UserDefinedFunction) or (
            inspect.isclass(func) and issubclass(func, UserDefinedFunction)
        ):
            raise TypeError("flat_map accepts Python callables or a pf.udtf declaration.")
        declaration = udtf(func, return_dtype=return_dtype)
    _validate_flat_map_input(declaration)
    table_type = declaration.return_dtype._to_table_data_type()
    if declaration._has_named_fields:
        output_columns = cast(RowType, table_type).field_names()
    elif isinstance(table_type, RowType) and len(table_type.fields) > 1:
        raise ValueError("flat_map requires named output fields; use TypedDict or a named struct.")
    else:
        output_columns = ["f0"]
    wrapper = declaration._create_table_wrapper(tuple(input_columns))
    wrapper._set_takes_row_as_input()
    return wrapper(with_columns(col("*"))), output_columns


def _iter_user_results(result: Any) -> Iterator[Any]:
    if result is None:
        return
    if isinstance(result, (Row, tuple, collections.abc.Mapping, str, bytes, bytearray)):
        yield result
    elif isinstance(result, collections.abc.Iterable):
        yield from result
    else:
        yield result


class _DataFrameTableFunctionAdapter(TableFunction):
    def __init__(
        self,
        func: _UDTFInput,
        return_dtype: DataType,
        deterministic: bool,
        input_columns: Optional[Tuple[str, ...]],
    ) -> None:
        self._func = func
        self._return_dtype = return_dtype
        self._deterministic = deterministic
        self._input_columns = input_columns
        self._bound_invocation: Optional[Callable[..., Any]] = None
        self._lifecycle_opened = False
        self.__name__ = getattr(func, "__name__", type(func).__name__)

    def open(self, function_context: Any) -> None:
        if isinstance(self._func, TableFunction):
            self._func.open(function_context)
            self._lifecycle_opened = True
            invoke_func = self._func.eval
        else:
            invoke_func = self._func() if inspect.isclass(self._func) else self._func
            if not callable(invoke_func):
                raise TypeError("UDTF class must construct a callable instance.")
        try:
            self._bound_invocation = self._bind_func(invoke_func)
        except Exception:
            try:
                self.close()
            except Exception:
                pass
            raise

    def close(self) -> None:
        try:
            if self._lifecycle_opened:
                cast(TableFunction, self._func).close()
        finally:
            self._bound_invocation = None
            self._lifecycle_opened = False

    def is_deterministic(self) -> bool:
        if isinstance(self._func, TableFunction):
            return self._func.is_deterministic()
        return self._deterministic

    def _bind_func(self, invoke_func: Callable[..., Any]) -> Callable[..., Any]:
        table_type = self._return_dtype._to_table_data_type()
        normalizer = _create_result_normalizer(table_type)
        is_struct = isinstance(table_type, RowType)
        field_count = len(table_type.fields) if isinstance(table_type, RowType) else 1
        input_columns = list(self._input_columns) if self._input_columns is not None else None

        def invoke(*args: Any) -> Iterator[Row]:
            if input_columns is not None:
                input_row = args[0]
                row = {name: input_row[i] for i, name in enumerate(input_columns)}
                result = invoke_func(row)
            else:
                result = invoke_func(*args)
            for item in _iter_user_results(result):
                if is_struct and field_count == 1 and item is not None:
                    if not isinstance(item, (Row, tuple, collections.abc.Mapping)):
                        item = Row(item)
                if not is_struct and isinstance(item, (Row, tuple)):
                    if len(item) != 1:
                        raise ValueError("UDTF scalar output requires exactly one field.")
                    item = item[0]
                normalized = normalizer(item) if normalizer is not None else item
                if is_struct:
                    if normalized is None:
                        yield Row(*([None] * field_count))
                    else:
                        yield normalized
                else:
                    yield Row(normalized)

        return invoke

    def eval(self, *args: Any) -> Iterator[Row]:
        if self._bound_invocation is None:
            raise RuntimeError("DataFrame UDTF was invoked before open().")
        return self._bound_invocation(*args)
