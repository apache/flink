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
import datetime
import decimal
import types
from functools import partial
from typing import Any, Callable, Dict, Union, get_args, get_origin, get_type_hints

from pyflink.table.types import DataType, DataTypes

_PEP_604_UNION_TYPE = getattr(types, "UnionType", None)
_AWAITABLE_ORIGINS = (collections.abc.Coroutine, collections.abc.Awaitable)

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


def _from_python_type(type_hint: Any) -> DataType:
    """Resolve a Python type hint into a table :class:`DataType`.

    Supports the basic scalar types, ``list[T]``/``dict[K, V]`` containers, and
    ``TypedDict`` (mapped to ``ROW``). Inferred types are ``NOT NULL``;
    ``Optional[T]``/``T | None`` is the marker that widens a type to nullable,
    and ``Any`` (an opt-out of the type system) stays nullable ``STRING``.
    Raises :class:`TypeError` for hints that cannot be resolved unambiguously.
    """

    def infer_typed_dict(hint: Any) -> DataType:
        return DataTypes.ROW(
            [
                DataTypes.FIELD(name, infer(field_hint))
                for name, field_hint in get_type_hints(hint).items()
            ]
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

    def infer_basic(hint: Any):
        factory = _BASIC_TYPE_HINT_FACTORIES.get(hint)
        return factory() if factory is not None else None

    def infer_not_null(hint: Any, origin: Any, arguments) -> DataType:
        if _is_typed_dict(hint):
            return infer_typed_dict(hint)

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

        data_type = infer_basic(hint)
        if data_type is not None:
            return data_type

        raise TypeError(
            f"Cannot infer DataType from type hint '{hint}'. "
            "Please specify the data type explicitly."
        )

    def infer(hint: Any) -> DataType:
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
