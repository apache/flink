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
"""Shared inference from Python type hints to table :class:`DataType`s.

This is the neutral core used to resolve a standard Python annotation into a
:mod:`pyflink.table.types` ``DataType``. It is consumed by the DataFrame API and
by UDF type-hint inference so both derive types from a single mapping.
"""

import collections.abc
import dataclasses
import datetime
import decimal
import sys
import types
from functools import partial
from typing import (
    Any,
    Callable,
    Dict,
    ForwardRef,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from pyflink.table.types import DataType, DataTypes

_PEP_604_UNION_TYPE = getattr(types, "UnionType", None)
_AWAITABLE_ORIGINS = (collections.abc.Coroutine, collections.abc.Awaitable)

try:
    # Prior to py 3.11 get_type_hints leaves these markers on the field type itself, and
    # typing.TypedDict does not account for them in __optional_keys__
    from typing_extensions import NotRequired, Required

    _TYPED_DICT_KEY_MARKER_OPTIONALITY: Dict[Any, bool] = {NotRequired: True, Required: False}
except ImportError:
    _TYPED_DICT_KEY_MARKER_OPTIONALITY = {}

_BASIC_TYPE_HINT_FACTORIES: Dict[Any, Callable[[], DataType]] = {
    bool: DataTypes.BOOLEAN,
    int: DataTypes.BIGINT,
    float: DataTypes.DOUBLE,
    str: DataTypes.STRING,
    bytes: DataTypes.BYTES,
    bytearray: DataTypes.BYTES,
    decimal.Decimal: partial(DataTypes.DECIMAL, 38, 18),
    datetime.date: DataTypes.DATE,
    # TIME is stored at runtime as an int number of milliseconds of the day, so 3 is
    # the highest fractional-second precision that survives a round trip.
    datetime.time: partial(DataTypes.TIME, 3),
    datetime.datetime: DataTypes.TIMESTAMP,
}


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


def _is_dataclass_type(type_hint: Any) -> bool:
    return isinstance(type_hint, type) and dataclasses.is_dataclass(type_hint)


def _is_named_tuple_type(type_hint: Any) -> bool:
    return (
        isinstance(type_hint, type)
        and issubclass(type_hint, tuple)
        and hasattr(type_hint, "_fields")
    )


def _is_composite_type(type_hint: Any) -> bool:
    return (
        _is_typed_dict(type_hint)
        or _is_dataclass_type(type_hint)
        or _is_named_tuple_type(type_hint)
    )


def _generic_composite_error(type_hint: Any) -> TypeError:
    return TypeError(
        f"Cannot infer DataType from type hint '{type_hint}'. Generic composite "
        "types are not supported. Please specify the data type explicitly."
    )


def _no_fields_error(type_hint: Any) -> TypeError:
    name = type_hint.__name__ if isinstance(type_hint, type) else type_hint
    return TypeError(
        f"Cannot infer DataType from '{name}': it has no fields. "
        "Please specify the data type explicitly."
    )


def _unresolved_forward_reference_error(hint: Union[ForwardRef, str]) -> TypeError:
    name = hint.__forward_arg__ if isinstance(hint, ForwardRef) else hint
    advice = "Please specify the data type explicitly."
    # typing generics wrap quoted names in ForwardRef, built-in generics keep the bare str
    if isinstance(hint, str) and sys.version_info < (3, 11):
        advice = (
            "Before Python 3.11, quoted names inside built-in generics such as "
            f"list['{name}'] are not resolved. Use typing.List['{name}'] or specify "
            "the data type explicitly."
        )
    return TypeError(
        f"Cannot infer DataType from unresolved forward reference '{name}'. {advice}"
    )


def _resolve_field_hints(composite: type) -> Dict[str, Any]:
    try:
        return get_type_hints(composite)
    except (NameError, AttributeError, SyntaxError, TypeError) as exc:
        raise TypeError(
            f"Cannot resolve the field type hints of '{composite.__name__}': "
            f"{exc}. Please specify the data type explicitly."
        ) from exc


def _composite_field_hints(composite: Any) -> List[Tuple[str, Any]]:
    field_hints = _resolve_field_hints(composite)
    if _is_typed_dict(composite):
        return [
            (name, _typed_dict_field_hint(hint, name in composite.__optional_keys__))
            for name, hint in field_hints.items()
        ]
    if _is_dataclass_type(composite):
        return [
            (field.name, field_hints[field.name]) for field in dataclasses.fields(composite)
        ]
    unannotated = [name for name in composite._fields if name not in field_hints]
    if unannotated:
        raise TypeError(
            f"Cannot infer DataType from NamedTuple '{composite.__name__}': fields "
            f"{unannotated} have no type annotations. Declare it with "
            "typing.NamedTuple or specify the data type explicitly."
        )
    return [(name, field_hints[name]) for name in composite._fields]


def _typed_dict_field_hint(hint: Any, is_optional_key: bool) -> Any:
    origin = get_origin(hint)
    if origin in _TYPED_DICT_KEY_MARKER_OPTIONALITY:
        is_optional_key = _TYPED_DICT_KEY_MARKER_OPTIONALITY[origin]
        hint = get_args(hint)[0]
    return Optional[hint] if is_optional_key else hint


def _tuple_field_hints(hint: Any, arguments: Tuple[Any, ...]) -> List[Tuple[str, Any]]:
    if Ellipsis in arguments:
        raise TypeError(
            f"Cannot infer DataType from type hint '{hint}': ROW types need a fixed "
            "number of elements. Use list[T] for variable-length sequences."
        )
    # Before Python 3.11, get_args(Tuple[()]) returns ((),)
    element_hints = () if arguments == ((),) else arguments
    return [
        (f"_{position}", element_hint)
        for position, element_hint in enumerate(element_hints, start=1)
    ]


def _infer_basic(hint: Any) -> Optional[DataType]:
    # The dict lookup fails on unhashable hints such as a dataclass instance.
    if not isinstance(hint, type):
        return None
    factory = _BASIC_TYPE_HINT_FACTORIES.get(hint)
    return factory() if factory is not None else None


def _from_python_type(type_hint: Any) -> DataType:
    """Resolve a Python type hint into a table :class:`DataType`.

    Supported hints:

    * basic scalar types such as ``int`` and ``str``
    * ``list[T]`` and ``dict[K, V]``
    * ``TypedDict``, dataclass, ``NamedTuple`` and fixed-length ``tuple``, which map
      to ``ROW``. Tuple fields are named ``_1``, ``_2`` and so on.

    Inferred types are ``NOT NULL``. Use ``Optional[T]`` or ``T | None`` for a
    nullable type. ``Any`` maps to a nullable ``STRING``.

    Raises :class:`TypeError` for hints that have no single mapping.
    """

    composites_in_progress: Set[Any] = set()

    def infer_composite(composite: type) -> DataType:
        if getattr(composite, "__parameters__", ()):
            raise _generic_composite_error(composite)
        if composite in composites_in_progress:
            raise TypeError(
                f"Cannot infer DataType from '{composite.__name__}': it refers to "
                "itself, which ROW types cannot represent. "
                "Please specify the data type explicitly."
            )
        composites_in_progress.add(composite)
        try:
            return infer_row(composite, _composite_field_hints(composite))
        finally:
            composites_in_progress.discard(composite)

    def infer_row(hint: Any, field_hints: List[Tuple[str, Any]]) -> DataType:
        if not field_hints:
            raise _no_fields_error(hint)
        return DataTypes.ROW(
            [DataTypes.FIELD(name, infer(field_hint)) for name, field_hint in field_hints]
        )

    def infer_union(hint: Any, arguments) -> DataType:
        non_none_types = [
            argument for argument in arguments if argument is not type(None)
        ]
        if len(non_none_types) == 1:
            return infer(non_none_types[0]).nullable()

        raise TypeError(
            f"Cannot infer DataType from type hint '{hint}'. "
            "Please specify the data type explicitly."
        )

    def infer_not_null(hint: Any, origin: Any, arguments) -> DataType:
        if _is_composite_type(hint):
            return infer_composite(hint)

        if _is_composite_type(origin):
            raise _generic_composite_error(hint)

        if isinstance(hint, TypeVar):
            raise TypeError(
                f"Cannot infer DataType from type variable '{hint}'. "
                "Please specify the data type explicitly."
            )

        if hint is tuple or hint is Tuple:
            raise TypeError(
                "Cannot infer DataType from tuple without type arguments. "
                "Use tuple[T1, T2, ...], for example tuple[int, str]."
            )

        if origin is tuple:
            return infer_row(hint, _tuple_field_hints(hint, arguments))

        if origin is list:
            if not arguments:
                raise TypeError(
                    "Cannot infer DataType from list without type argument. "
                    "Use list[T], for example list[int]."
                )
            return DataTypes.ARRAY(infer(arguments[0]))

        if origin is dict:
            if len(arguments) != 2:
                raise TypeError(
                    "Cannot infer DataType from dict without key and value type arguments. "
                    "Use dict[K, V], for example dict[str, int]."
                )
            return DataTypes.MAP(infer(arguments[0]), infer(arguments[1]))

        data_type = _infer_basic(hint)
        if data_type is not None:
            return data_type

        raise TypeError(
            f"Cannot infer DataType from type hint '{hint}'. "
            "Please specify the data type explicitly."
        )

    def infer(hint: Any) -> DataType:
        if isinstance(hint, (ForwardRef, str)):
            raise _unresolved_forward_reference_error(hint)

        origin = get_origin(hint)
        arguments = get_args(hint)

        if origin is Union or (
            _PEP_604_UNION_TYPE is not None and origin is _PEP_604_UNION_TYPE
        ):
            return infer_union(hint, arguments)

        # `Any` opts out of the type system, so it stays nullable rather than
        # acquiring the NOT NULL default applied to the concrete hints.
        if hint is Any:
            return DataTypes.STRING()

        return infer_not_null(hint, origin, arguments).not_null()

    return infer(type_hint)


def _unwrap_awaitable(type_hint: Any) -> Any:
    """Unwrap ``Coroutine[Any, Any, T]``/``Awaitable[T]`` to ``T``.

    Returns the hint unchanged when it is not an awaitable wrapper, so it can be
    applied unconditionally before resolving an async function's result type.
    """
    if get_origin(type_hint) in _AWAITABLE_ORIGINS:
        arguments = get_args(type_hint)
        if arguments:
            return arguments[-1]
    return type_hint
