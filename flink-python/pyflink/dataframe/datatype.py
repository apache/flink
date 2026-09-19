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

import datetime
import decimal
import types
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, get_args, get_origin

from pyflink.table.types import DataType as TableDataType, DataTypes, NullType
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ["DataType"]

_INT_MIN = -(1 << 31)
_INT_MAX = (1 << 31) - 1
_BIGINT_MIN = -(1 << 63)
_BIGINT_MAX = (1 << 63) - 1
_PEP_604_UNION_TYPE = getattr(types, "UnionType", None)

_BASIC_TYPE_HINT_FACTORIES: Dict[Any, Callable[[], TableDataType]] = {
    bool: DataTypes.BOOLEAN,
    int: DataTypes.BIGINT,
    float: DataTypes.DOUBLE,
    str: DataTypes.STRING,
    bytes: DataTypes.BYTES,
    bytearray: DataTypes.BYTES,
    decimal.Decimal: partial(DataTypes.DECIMAL, 38, 18),
    datetime.date: DataTypes.DATE,
    datetime.time: DataTypes.TIME,
    datetime.datetime: DataTypes.TIMESTAMP,
    Any: DataTypes.STRING,
}


@PublicEvolving()
class DataType:
    """
    Describes the logical data type of a value in the DataFrame API.

    Data types declare the types of values used by DataFrame expressions and operations.

    Example::

        >>> import pyflink.dataframe as pf
        >>> integer_type = pf.DataType.int64()
        >>> string_type = pf.DataType.string()

    .. versionadded:: 2.4.0
    """

    def __init__(self, table_data_type: TableDataType):
        if isinstance(table_data_type, NullType) and not table_data_type._nullable:
            raise ValueError("NULL data type must be nullable")
        self._table_data_type = table_data_type

    def __repr__(self) -> str:
        return f"DataType({self._table_data_type!r})"

    @PublicEvolving()
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DataType):
            return False
        return self._table_data_type == other._table_data_type

    @PublicEvolving()
    def __hash__(self) -> int:
        return hash(repr(self._table_data_type))

    @PublicEvolving()
    def not_null(self) -> "DataType":
        """
        Return a non-nullable version of this data type.

        .. versionadded:: 2.4.0
        """
        return DataType(self._table_data_type.not_null())

    @PublicEvolving()
    def nullable(self) -> "DataType":
        """
        Return a nullable version of this data type.

        .. versionadded:: 2.4.0
        """
        return DataType(self._table_data_type.nullable())

    @classmethod
    @PublicEvolving()
    def int8(cls) -> "DataType":
        """
        Create an 8-bit signed integer type.

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.TINYINT())

    @classmethod
    @PublicEvolving()
    def int16(cls) -> "DataType":
        """
        Create a 16-bit signed integer type.

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.SMALLINT())

    @classmethod
    @PublicEvolving()
    def int32(cls) -> "DataType":
        """
        Create a 32-bit signed integer type.

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.INT())

    @classmethod
    @PublicEvolving()
    def int64(cls) -> "DataType":
        """
        Create a 64-bit integer type.

        Example::

            >>> import pyflink.dataframe as pf
            >>> expression = pf.lit(42, pf.DataType.int64())

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.BIGINT())

    @classmethod
    @PublicEvolving()
    def float32(cls) -> "DataType":
        """
        Create a 32-bit floating point type.

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.FLOAT())

    @classmethod
    @PublicEvolving()
    def float64(cls) -> "DataType":
        """
        Create a 64-bit floating point type.

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.DOUBLE())

    @classmethod
    @PublicEvolving()
    def decimal(cls, precision: int, scale: int) -> "DataType":
        """
        Create a decimal type with the given precision and scale.

        :param precision: Total number of digits.
        :param scale: Number of digits to the right of the decimal point.

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.DECIMAL(precision, scale))

    @classmethod
    @PublicEvolving()
    def string(cls) -> "DataType":
        """
        Create a variable-length string type.

        Example::

            >>> import pyflink.dataframe as pf
            >>> expression = pf.lit("Alice", pf.DataType.string())

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.STRING())

    @classmethod
    @PublicEvolving()
    def fixed_size_string(cls, length: int) -> "DataType":
        """
        Create a fixed-length character string type.

        :param length: Number of characters.

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.CHAR(length))

    @classmethod
    @PublicEvolving()
    def binary(cls) -> "DataType":
        """
        Create a variable-length binary string type.

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.BYTES())

    @classmethod
    @PublicEvolving()
    def fixed_size_binary(cls, length: int) -> "DataType":
        """
        Create a fixed-length binary string type.

        :param length: Number of bytes.

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.BINARY(length))

    @classmethod
    @PublicEvolving()
    def bool(cls) -> "DataType":
        """
        Create a boolean type.

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.BOOLEAN())

    @classmethod
    @PublicEvolving()
    def null(cls) -> "DataType":
        """
        Create a null type.

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.NULL())

    @classmethod
    @PublicEvolving()
    def date(cls) -> "DataType":
        """
        Create a date type.

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.DATE())

    @classmethod
    @PublicEvolving()
    def time(cls, precision: int = 0) -> "DataType":
        """
        Create a time type.

        :param precision: Number of fractional-second digits.

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.TIME(precision))

    @classmethod
    @PublicEvolving()
    def timestamp(cls, precision: int = 6) -> "DataType":
        """
        Create a timestamp type without a time zone.

        :param precision: Number of fractional-second digits.

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.TIMESTAMP(precision))

    @classmethod
    @PublicEvolving()
    def timestamp_ltz(cls, precision: int = 6) -> "DataType":
        """
        Create a timestamp type with a local time zone.

        :param precision: Number of fractional-second digits.

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.TIMESTAMP_LTZ(precision))

    @classmethod
    @PublicEvolving()
    def list(cls, dtype: "DataType") -> "DataType":
        """
        Create a list type.

        :param dtype: Type of each list element.

        Example::

            >>> import pyflink.dataframe as pf
            >>> scores_type = pf.DataType.list(pf.DataType.int32())

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.ARRAY(dtype._to_table_data_type()))

    @classmethod
    @PublicEvolving()
    def map(cls, key_type: "DataType", value_type: "DataType") -> "DataType":
        """
        Create a map type.

        :param key_type: Type of each map key.
        :param value_type: Type of each map value.

        Example::

            >>> import pyflink.dataframe as pf
            >>> config_type = pf.DataType.map(
            ...     pf.DataType.string(),
            ...     pf.DataType.int64(),
            ... )

        .. versionadded:: 2.4.0
        """
        return cls(
            DataTypes.MAP(
                key_type._to_table_data_type(),
                value_type._to_table_data_type(),
            )
        )

    @classmethod
    @PublicEvolving()
    def struct(
        cls,
        fields: Union[
            Dict[str, "DataType"],
            List[Tuple[str, "DataType"]],
        ],
    ) -> "DataType":
        """
        Create a struct type with named fields.

        ``fields`` may be an insertion-ordered dictionary or a list of name and type pairs.

        :param fields: Field names and their data types.

        Example::

            >>> import pyflink.dataframe as pf
            >>> person_type = pf.DataType.struct({
            ...     "name": pf.DataType.string(),
            ...     "age": pf.DataType.int32(),
            ... })

        Fields may also be passed as a list of name and type pairs::

            >>> person_type = pf.DataType.struct([
            ...     ("name", pf.DataType.string()),
            ...     ("age", pf.DataType.int32()),
            ... ])

        .. versionadded:: 2.4.0
        """
        field_items = fields.items() if isinstance(fields, dict) else fields

        return cls(
            DataTypes.ROW(
                [
                    DataTypes.FIELD(name, data_type._to_table_data_type())
                    for name, data_type in field_items
                ]
            )
        )

    @classmethod
    def _from_type_hint(cls, type_hint: Any) -> "DataType":
        def infer_union_type(hint: Any, arguments: Tuple[Any, ...]) -> "DataType":
            non_none_types = [
                argument for argument in arguments if argument is not type(None)
            ]
            if len(non_none_types) == 1:
                return infer(non_none_types[0]).nullable()

            raise TypeError(
                f"Cannot infer DataType from type hint '{hint}'. "
                "Please specify the data type explicitly."
            )

        def infer_basic_type(hint: Any) -> Optional["DataType"]:
            factory = _BASIC_TYPE_HINT_FACTORIES.get(hint)
            return cls(factory()) if factory is not None else None

        def infer(hint: Any) -> "DataType":
            origin = get_origin(hint)
            arguments = get_args(hint)

            if origin is Union or (
                _PEP_604_UNION_TYPE is not None
                and origin is _PEP_604_UNION_TYPE
            ):
                return infer_union_type(hint, arguments)

            if origin is list:
                if not arguments:
                    raise TypeError(
                        "Cannot infer DataType from list without type argument. "
                        "Use list[T], for example list[int]."
                    )
                return cls.list(infer(arguments[0]))

            if origin is dict:
                if len(arguments) != 2:
                    raise TypeError(
                        "Cannot infer DataType from dict without key and value type arguments. "
                        "Use dict[K, V], for example dict[str, int]."
                    )
                return cls.map(
                    infer(arguments[0]),
                    infer(arguments[1]),
                )

            data_type = infer_basic_type(hint)
            if data_type is not None:
                return data_type

            raise TypeError(
                f"Cannot infer DataType from type hint '{hint}'. "
                "Please specify the data type explicitly."
            )

        return infer(type_hint)

    @classmethod
    def _from_sql(cls, sql_type: str) -> "DataType":
        """
        Create a data type from its SQL representation.

        :param sql_type: SQL data type string, such as ``INT`` or ``ARRAY<BIGINT>``.
        :raises ValueError: If the SQL data type cannot be parsed.
        """
        from py4j.protocol import Py4JJavaError

        from pyflink.java_gateway import get_gateway
        from pyflink.table.types import _from_java_data_type
        from pyflink.util.exceptions import JavaException

        try:
            gateway = get_gateway()
            j_logical_type = (
                gateway.jvm.org.apache.flink.table.types.logical.utils.LogicalTypeParser.parse(
                    sql_type,
                    gateway.jvm.Thread.currentThread().getContextClassLoader(),
                )
            )
            j_data_type = (
                gateway.jvm.org.apache.flink.table.types.utils.TypeConversions
                .fromLogicalToDataType(j_logical_type)
            )
            return cls(_from_java_data_type(j_data_type))
        except (JavaException, Py4JJavaError) as exc:
            raise ValueError(str(exc)) from None

    def _to_table_data_type(self) -> TableDataType:
        return self._table_data_type
