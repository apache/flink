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

import builtins
from enum import Enum
from typing import (
    Any,
    Collection,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    TYPE_CHECKING,
    Tuple,
    Union,
    cast,
)

if TYPE_CHECKING:
    import pandas
    import pyarrow

from pyflink.dataframe.context import get_or_create_table_environment
from pyflink.dataframe.dataframe import DataFrame
from pyflink.dataframe.datatype import _BIGINT_MAX, _BIGINT_MIN
from pyflink.table import Schema, Table
from pyflink.table.types import (
    _create_converter,
    _create_type_verifier,
    _has_nulltype,
    _infer_schema_from_data,
    DataTypes,
    LocalZonedTimestampType,
    RowField,
    RowType,
    TimestampType,
    from_arrow_type,
)
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = [
    "from_arrow",
    "from_dict",
    "from_pandas",
    "from_records",
    "from_table",
    "range",
]

_SCALAR_SEQUENCE_TYPES = (str, bytes, bytearray, memoryview)


class _WatermarkSpec(NamedTuple):
    column: str
    expression: str

    @classmethod
    def parse(cls, watermark: Optional[Tuple[str, str]]) -> Optional["_WatermarkSpec"]:
        if watermark is None:
            return None
        if not isinstance(watermark, tuple) or len(watermark) != 2:
            raise TypeError("watermark must be a tuple of (column, expression)")
        if any(not isinstance(value, str) or not value.strip() for value in watermark):
            raise TypeError(
                "watermark column and expression must be non-empty strings"
            )
        return cls(*watermark)

    def normalize_row_type(self, row_type: RowType) -> RowType:
        matching_fields = [
            field for field in row_type.fields if field.name == self.column
        ]
        if not matching_fields:
            raise ValueError(
                f"watermark column {self.column!r} is not present in data"
            )

        watermark_type = matching_fields[0].data_type
        if not isinstance(watermark_type, (TimestampType, LocalZonedTimestampType)):
            raise ValueError(
                f"watermark column {self.column!r} must have a timestamp type"
            )

        fields = []
        for field in row_type.fields:
            data_type = field.data_type
            if field.name == self.column and data_type.precision != 3:
                data_type = type(data_type)(3, data_type._nullable)
            fields.append(RowField(field.name, data_type, field.description))
        return RowType(fields, row_type._nullable)


class _RecordType(Enum):
    NAMED_TUPLE = "named_tuple"
    MAPPING = "mapping"
    SEQUENCE = "sequence"

    @classmethod
    def from_record(cls, record: Any) -> "_RecordType":
        if isinstance(record, tuple) and isinstance(
            getattr(record, "_fields", None), tuple
        ):
            return cls.NAMED_TUPLE
        if isinstance(record, Mapping):
            return cls.MAPPING
        if isinstance(record, Sequence) and not isinstance(
            record, _SCALAR_SEQUENCE_TYPES
        ):
            return cls.SEQUENCE
        raise TypeError(
            "record must be a mapping or a sequence of values, "
            "such as a list or tuple"
        )

    def validate(self, record: Any) -> None:
        try:
            record_type = self.from_record(record)
        except TypeError:
            record_type = None

        if record_type is self:
            return
        # Treat named tuples as tuples when validating sequence records.
        if self is _RecordType.SEQUENCE and record_type is _RecordType.NAMED_TUPLE:
            return

        raise TypeError(f"record must be a {self.value.replace('_', ' ')}")

    def field_names(self, record: Any) -> Collection[str]:
        if self is _RecordType.NAMED_TUPLE:
            return cast(Tuple[str, ...], getattr(record, "_fields"))
        if self is _RecordType.MAPPING:
            return cast(Mapping[str, Any], record).keys()
        raise TypeError("sequence records do not have named fields")

    def normalize_record(
        self,
        record: Any,
        schema: List[str],
        require_exact_fields: bool,
    ) -> Tuple[Any, ...]:
        if self is _RecordType.SEQUENCE:
            row = tuple(record)
            if require_exact_fields and len(row) != len(schema):
                raise ValueError(
                    f"record has {len(row)} values but schema has {len(schema)} fields"
                )
            return row
        record_fields = self.field_names(record)
        for name in schema:
            if name not in record_fields:
                raise ValueError(f"record is missing schema field {name!r}")
        if require_exact_fields and len(record_fields) != len(schema):
            extra_fields = [name for name in record_fields if name not in schema]
            raise ValueError(
                f"record has fields not present in schema: {extra_fields!r}"
            )
        if self is _RecordType.MAPPING:
            field_values = cast(Mapping[str, Any], record)
            return tuple(field_values[name] for name in schema)
        return tuple(getattr(record, name) for name in schema)


def _validate_schema(schema: List[str]) -> None:
    if not isinstance(schema, list) or any(not isinstance(name, str) for name in schema):
        raise TypeError("schema must be a list of strings")
    if not schema:
        raise ValueError("schema must not be empty")
    if any(not name for name in schema):
        raise ValueError("schema field names must not be empty")
    if len(set(schema)) != len(schema):
        raise ValueError("schema field names must be unique")


def _resolve_column_names(
    input_names: Sequence[str], schema: Optional[List[str]]
) -> List[str]:
    column_names = list(input_names) if schema is None else schema
    if (
        schema is not None
        and isinstance(schema, list)
        and len(schema) != len(input_names)
    ):
        raise ValueError(
            f"schema has {len(schema)} fields but data has "
            f"{len(input_names)} columns"
        )
    _validate_schema(column_names)
    return column_names


def _normalize_pandas_column_name(name: Any) -> str:
    """Normalize a pandas column label to the corresponding Arrow field name."""
    if isinstance(name, str):
        return name
    if isinstance(name, bytes):
        return name.decode("utf-8")
    if isinstance(name, tuple):
        return str(tuple(_normalize_pandas_column_name(value) for value in name))
    return str(name)


def _infer_schema_and_create_dataframe(
    rows: Sequence[Sequence[Any]],
    column_names: List[str],
    watermark: Optional[_WatermarkSpec] = None,
) -> DataFrame:
    row_type = _infer_schema_from_data(rows, names=column_names)
    if watermark is None:
        table_schema = None
    else:
        row_type = watermark.normalize_row_type(row_type)
        table_schema = (
            Schema.new_builder()
            .from_row_data_type(row_type)
            .watermark(*watermark)
            .build()
        )
    converter = _create_converter(row_type)
    verify_row = _create_type_verifier(row_type)
    sql_rows = []
    for row in rows:
        row = converter(row)
        verify_row(row)
        sql_rows.append(row_type.to_sql_type(row))

    table = get_or_create_table_environment()._from_elements(
        sql_rows, row_type, table_schema
    )
    return DataFrame(table)


@PublicEvolving()
def from_table(table: Table) -> DataFrame:
    """
    Create a DataFrame that wraps a PyFlink Table.

    :param table: Table to wrap without copying or converting it.
    :return: A DataFrame backed by the exact supplied Table.
    :raises TypeError: If ``table`` is not a :class:`~pyflink.table.Table`.

    Example::

        >>> import pyflink.dataframe as pf
        >>> table = table_env.from_elements([(1, "Alice")], ["id", "name"])
        >>> dataframe = pf.from_table(table)
        >>> dataframe.to_table() is table
        True

    .. versionadded:: 2.4.0
    """
    if not isinstance(table, Table):
        raise TypeError("table must be a pyflink.table.Table")
    return DataFrame(table)


@PublicEvolving()
def from_pandas(
    pdf: "pandas.DataFrame",
    schema: Optional[List[str]] = None,
    watermark: Optional[Tuple[str, str]] = None,
) -> DataFrame:
    """
    Create a DataFrame from a pandas DataFrame.

    Types are inferred from the Arrow representation of the pandas columns. An explicit ``schema``
    renames columns positionally and must contain exactly one unique, non-empty name per input
    column. Empty inputs are supported when their pandas dtypes can be converted to Flink types.
    Timezone-aware timestamps are represented as ``TIMESTAMP`` in the TableEnvironment's
    configured local timezone; timezone-naive timestamps are unchanged.

    ``watermark`` declares an event-time column and its SQL watermark expression. The selected
    column must have a timestamp-compatible type. Its precision is normalized to milliseconds;
    values with finer precision are truncated to ``TIMESTAMP(3)``.

    :param pdf: pandas DataFrame to convert.
    :param schema: Optional list of positional result column names.
    :param watermark: Optional ``(column, expression)`` watermark declaration.
    :return: A DataFrame containing the pandas rows.
    :raises TypeError: If the input, schema, watermark, or inferred types are invalid.
    :raises ValueError: If schema width or watermark column requirements are not met.

    Example::

        >>> import pandas as pd
        >>> import pyflink.dataframe as pf
        >>> pdf = pd.DataFrame({"identifier": [1, 2], "name": ["Alice", "Bob"]})
        >>> dataframe = pf.from_pandas(pdf, schema=["id", "name"])
        >>> events = pf.from_pandas(
        ...     pd.DataFrame({"ts": pd.to_datetime(["2026-01-01T00:00:00Z"])}),
        ...     watermark=("ts", "ts - INTERVAL '5' SECOND"),
        ... )

    .. versionadded:: 2.4.0
    """
    import pandas as pd

    if not isinstance(pdf, pd.DataFrame):
        raise TypeError(
            f"data must be a pandas.DataFrame, but was {type(pdf).__name__}"
        )

    import pyarrow as pa

    input_names = [_normalize_pandas_column_name(name) for name in pdf.columns]
    arrow_pdf = pdf.copy(deep=False)
    arrow_pdf.columns = [
        f"__pyflink_dataframe_column_{index}"
        for index in builtins.range(len(input_names))
    ]
    return from_arrow(
        pa.Table.from_pandas(arrow_pdf, preserve_index=False),
        schema=input_names if schema is None else schema,
        watermark=watermark,
    )


@PublicEvolving()
def from_arrow(
    table: "pyarrow.Table",
    schema: Optional[List[str]] = None,
    watermark: Optional[Tuple[str, str]] = None,
) -> DataFrame:
    """
    Create a DataFrame from a PyArrow Table without converting through pandas.

    An explicit ``schema`` renames columns positionally and must contain exactly one unique,
    non-empty name per input column. Empty tables are supported when their Arrow field types can be
    converted to Flink types.

    ``watermark`` declares an event-time column and its SQL watermark expression. The selected
    column must have a timestamp-compatible type. Its precision is normalized to milliseconds;
    values with finer precision are truncated to ``TIMESTAMP(3)``.

    :param table: PyArrow Table to convert.
    :param schema: Optional list of positional result column names.
    :param watermark: Optional ``(column, expression)`` watermark declaration.
    :return: A DataFrame containing the Arrow rows.
    :raises TypeError: If the input, schema, watermark, or inferred types are invalid.
    :raises ValueError: If schema width or watermark column requirements are not met.

    Example::

        >>> import pyarrow as pa
        >>> import pyflink.dataframe as pf
        >>> table = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})
        >>> dataframe = pf.from_arrow(table)
        >>> events = pf.from_arrow(
        ...     pa.table({"ts": pa.array([0], type=pa.timestamp("ms"))}),
        ...     watermark=("ts", "ts - INTERVAL '5' SECOND"),
        ... )

    .. versionadded:: 2.4.0
    """
    import pyarrow as pa

    if not isinstance(table, pa.Table):
        raise TypeError(
            f"data must be a pyarrow.Table, but was {type(table).__name__}"
        )
    watermark_spec = _WatermarkSpec.parse(watermark)
    names = _resolve_column_names(table.column_names, schema)
    row_type = RowType(
        [
            RowField(
                name,
                from_arrow_type(field.type, field.nullable),
            )
            for name, field in zip(names, table.schema)
        ]
    )
    null_field_names = [
        field.name for field in row_type.fields if _has_nulltype(field.data_type)
    ]
    if null_field_names:
        columns = ", ".join(repr(name) for name in null_field_names)
        raise TypeError(
            f"Cannot infer Flink data types for columns with Arrow null types: {columns}. "
            "Use explicit pandas or Arrow dtypes for these columns."
        )
    if watermark_spec is None:
        table_schema = None
    else:
        row_type = watermark_spec.normalize_row_type(row_type)
        table_schema = (
            Schema.new_builder()
            .from_row_data_type(row_type)
            .watermark(*watermark_spec)
            .build()
        )
    result = get_or_create_table_environment()._from_arrow(
        table, row_type, table_schema
    )
    return DataFrame(result)


@PublicEvolving()
def from_records(
    data: Sequence[Union[Sequence[Any], Mapping[str, Any]]],
    schema: Optional[List[str]] = None,
    watermark: Optional[Tuple[str, str]] = None,
) -> DataFrame:
    """
    Create a DataFrame from row-oriented records.

    For mapping and named tuple records with an explicit ``schema``, every record must contain all
    schema fields; other fields are ignored. When ``schema`` is omitted, the keys or fields from
    the first record are used as the schema and every record must have exactly those fields.

    For other sequence records, every record must have the same number of values. A ``schema`` is
    required to provide the field names.

    Field types are inferred from the record values.

    ``watermark`` declares an event-time column and its SQL watermark expression. The selected
    column must have a timestamp-compatible type. Its precision is normalized to milliseconds;
    values with finer precision are truncated to ``TIMESTAMP(3)`` or ``TIMESTAMP_LTZ(3)``.

    :param data: Non-empty sequence of mapping or sequence records.
    :param schema: Optional non-empty list of field names.
    :param watermark: Optional ``(column, expression)`` watermark declaration.
    :return: A DataFrame containing the records.
    :raises TypeError: If a record or schema has an invalid type.
    :raises ValueError: If data or schema is empty, schema field names are invalid, a required
        schema is omitted, a required field is absent, inferred record fields differ, or record
        widths differ.

    Example::

        >>> import pyflink.dataframe as pf
        >>> users = pf.from_records([
        ...     {"id": 1, "name": "Alice"},
        ...     {"id": 2, "name": "Bob"},
        ... ])
        >>> users = pf.from_records(
        ...     [(1, "Alice"), (2, "Bob")], schema=["id", "name"]
        ... )
        >>> from typing import NamedTuple
        >>> class User(NamedTuple):
        ...     id: int
        ...     name: str
        >>> users = pf.from_records([User(1, "Alice"), User(2, "Bob")])
        >>> selected_users = pf.from_records(
        ...     [User(1, "Alice")], schema=["name", "id"]
        ... )
        >>> from datetime import datetime
        >>> events = pf.from_records(
        ...     [{"id": 1, "ts": datetime(2026, 1, 1)}],
        ...     watermark=("ts", "ts - INTERVAL '5' SECOND"),
        ... )

    .. versionadded:: 2.4.0
    """
    if not isinstance(data, Sequence) or isinstance(data, _SCALAR_SEQUENCE_TYPES):
        raise TypeError(
            "data must be a sequence of records, such as a list or tuple"
        )
    if not data:
        raise ValueError("data must not be empty")
    watermark_spec = _WatermarkSpec.parse(watermark)

    first_record = data[0]
    try:
        expected_record_type = _RecordType.from_record(first_record)
    except TypeError as error:
        raise TypeError("invalid record at index 0") from error

    if expected_record_type is _RecordType.SEQUENCE:
        if schema is None:
            raise ValueError("schema is required for sequence records")
        require_exact_fields = True
    elif schema is None:
        schema = list(expected_record_type.field_names(first_record))
        require_exact_fields = True
    else:
        require_exact_fields = False

    _validate_schema(schema)
    rows: List[Sequence[Any]] = []
    for index, record in enumerate(data):
        try:
            if index > 0:
                expected_record_type.validate(record)
            row = expected_record_type.normalize_record(
                record, schema, require_exact_fields
            )
        except TypeError as error:
            raise TypeError(f"invalid record at index {index}") from error
        except ValueError as error:
            raise ValueError(f"invalid record at index {index}") from error
        rows.append(row)

    return _infer_schema_and_create_dataframe(rows, schema, watermark_spec)


@PublicEvolving()
def from_dict(
    data: Mapping[str, Sequence[Any]],
    schema: Optional[List[str]] = None,
    watermark: Optional[Tuple[str, str]] = None,
) -> DataFrame:
    """
    Create a DataFrame from a column-oriented dictionary.

    All selected columns must contain the same non-zero number of values. ``schema`` can select a
    subset of columns and controls their order. If omitted, dictionary insertion order is used.

    ``watermark`` declares an event-time column and its SQL watermark expression. The selected
    column must have a timestamp-compatible type. Its precision is normalized to milliseconds;
    values with finer precision are truncated to ``TIMESTAMP(3)`` or ``TIMESTAMP_LTZ(3)``.

    :param data: Non-empty mapping of column names to value sequences.
    :param schema: Optional non-empty list of selected column names.
    :param watermark: Optional ``(column, expression)`` watermark declaration.
    :return: A DataFrame containing the selected columns.
    :raises TypeError: If ``data`` is not a mapping, or the selected schema or a selected column
        value has an invalid type.
    :raises ValueError: If the input is empty, schema field names are invalid, selected column
        lengths differ, or a selected column is missing.

    Example::

        >>> from datetime import datetime
        >>> import pyflink.dataframe as pf
        >>> users = pf.from_dict(
        ...     {"name": ["Alice", "Bob"], "id": [1, 2]},
        ...     schema=["id", "name"],
        ... )
        >>> events = pf.from_dict(
        ...     {"id": [1], "ts": [datetime(2026, 1, 1)]},
        ...     watermark=("ts", "ts - INTERVAL '5' SECOND"),
        ... )

    .. versionadded:: 2.4.0
    """
    if not isinstance(data, Mapping):
        raise TypeError("data must be a mapping")
    if not data:
        raise ValueError("data must not be empty")
    watermark_spec = _WatermarkSpec.parse(watermark)
    if schema is None:
        schema = list(data.keys())
    _validate_schema(schema)
    for name in schema:
        if name not in data:
            raise ValueError(f"column {name!r} is not present in data")
        values = data[name]
        if not isinstance(values, Sequence) or isinstance(
            values, _SCALAR_SEQUENCE_TYPES
        ):
            raise TypeError(
                f"column {name!r} values must be a sequence, "
                "such as a list or tuple"
            )
    lengths = {name: len(data[name]) for name in schema}
    if len(set(lengths.values())) != 1:
        raise ValueError("columns must have equal lengths")
    row_count = next(iter(lengths.values()))
    if row_count == 0:
        raise ValueError("data must contain at least one row")
    rows = [
        tuple(data[name][row_index] for name in schema)
        for row_index in builtins.range(row_count)
    ]
    return _infer_schema_and_create_dataframe(rows, schema, watermark_spec)


@PublicEvolving()
def range(start_or_end: int, end: Optional[int] = None, step: int = 1) -> DataFrame:
    """
    Create a DataFrame containing an integer range in one ``id`` column.

    The arguments follow Python's built-in :func:`range` semantics. The result always has an
    ``id BIGINT`` column, including when the requested range is empty.

    :param start_or_end: End value when ``end`` is omitted, otherwise the start value.
    :param end: Optional exclusive end value.
    :param step: Distance between adjacent values; must not be zero.
    :return: A DataFrame with one ``id`` column.
    :raises TypeError: If an argument is not an integer.
    :raises ValueError: If ``step`` is zero or the range contains values outside the signed
        ``BIGINT`` bounds.

    Example::

        >>> import pyflink.dataframe as pf
        >>> identifiers = pf.range(1, 6, 2)
        >>> identifiers.collect()
        [<Row(1)>, <Row(3)>, <Row(5)>]

    .. versionadded:: 2.4.0
    """
    if not isinstance(start_or_end, int):
        raise TypeError("start_or_end must be an integer")
    if end is not None and not isinstance(end, int):
        raise TypeError("end must be an integer")
    if not isinstance(step, int):
        raise TypeError("step must be an integer")
    if step == 0:
        raise ValueError("step must not be zero")

    if end is None:
        start = 0
        stop = start_or_end
    else:
        start = start_or_end
        stop = end
    values = builtins.range(start, stop, step)
    has_values = start < stop if step > 0 else start > stop
    if has_values and not (
        _BIGINT_MIN <= values[0] <= _BIGINT_MAX
        and _BIGINT_MIN <= values[-1] <= _BIGINT_MAX
    ):
        raise ValueError("range values must fit in signed BIGINT")

    row_type = DataTypes.ROW([DataTypes.FIELD("id", DataTypes.BIGINT())])
    sql_rows = [row_type.to_sql_type((value,)) for value in values]
    table = get_or_create_table_environment()._from_elements(sql_rows, row_type)
    return DataFrame(table)
