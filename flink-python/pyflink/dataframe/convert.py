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

from typing import Any, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple, Union, cast

from pyflink.dataframe.context import get_or_create_table_environment
from pyflink.dataframe.dataframe import DataFrame
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ["from_dict", "from_records"]

_SCALAR_SEQUENCE_TYPES = (str, bytes, bytearray, memoryview)


class _SequenceRowIterable:
    """Produce tuple rows from row-oriented sequence records."""

    def __init__(self, records: Sequence[Sequence[Any]]):
        self._records = records

    def __iter__(self) -> Iterator[Sequence[Any]]:
        return (tuple(record) for record in self._records)


class _ColumnRowIterable:
    """Produce tuple rows from column-oriented sequences."""

    def __init__(
        self, data: Mapping[str, Sequence[Any]], columns: Sequence[str]
    ):
        self._data = data
        self._columns = columns

    def __iter__(self) -> Iterator[Sequence[Any]]:
        return zip(*(self._data[name] for name in self._columns))


def _validate_schema(schema: List[str]) -> None:
    if not isinstance(schema, list) or any(not isinstance(name, str) for name in schema):
        raise TypeError("schema must be a list of strings")
    if not schema:
        raise ValueError("schema must not be empty")
    if any(not name for name in schema):
        raise ValueError("schema field names must not be empty")
    if len(set(schema)) != len(schema):
        raise ValueError("schema field names must be unique")


def _is_named_tuple_record(record: Any) -> bool:
    return isinstance(record, tuple) and isinstance(
        getattr(record, "_fields", None), tuple
    )


def _to_field_mapping(
    record: Any, named_tuple_records: bool, index: int
) -> Mapping[str, Any]:
    if named_tuple_records:
        if not _is_named_tuple_record(record):
            raise TypeError("record at index %d must be a named tuple" % index)
        fields = cast(Tuple[str, ...], getattr(record, "_fields"))
        return dict(zip(fields, record))
    if not isinstance(record, Mapping):
        raise TypeError("record at index %d must be a mapping" % index)
    return cast(Mapping[str, Any], record)


def _from_rows(rows: Iterable[Sequence[Any]], columns: Sequence[str]) -> DataFrame:
    table = get_or_create_table_environment().from_elements(rows, list(columns))
    return DataFrame(table)


@PublicEvolving()
def from_records(
    data: Sequence[Union[Sequence[Any], Mapping[str, Any]]],
    schema: Optional[List[str]] = None,
) -> DataFrame:
    """
    Create a DataFrame from row-oriented records.

    For mapping and named tuple records with an explicit ``schema``, every record must contain all
    schema fields; other fields are ignored. When ``schema`` is omitted, the keys or fields from
    the first record are used as the schema and every record must have exactly those fields.

    For other sequence records, every record must have the same number of values. A ``schema`` is
    required to provide the field names.

    Field types are inferred from the record values.

    :param data: Non-empty sequence of mapping or sequence records.
    :param schema: Optional non-empty list of field names.
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

    .. versionadded:: 2.4.0
    """
    if not isinstance(data, Sequence) or isinstance(data, _SCALAR_SEQUENCE_TYPES):
        raise TypeError(
            "data must be a sequence of records, such as a list or tuple"
        )
    if not data:
        raise ValueError("data must not be empty")

    first_record = data[0]
    rows: Iterable[Sequence[Any]]
    named_tuple_records = _is_named_tuple_record(first_record)
    if isinstance(first_record, Mapping) or named_tuple_records:
        first_fields = _to_field_mapping(first_record, named_tuple_records, 0)
        columns = list(first_fields.keys()) if schema is None else schema
        _validate_schema(columns)
        expected_fields = set(columns)
        field_rows: List[Sequence[Any]] = []
        for index, record in enumerate(data):
            field_values = _to_field_mapping(record, named_tuple_records, index)
            if schema is None and set(field_values.keys()) != expected_fields:
                raise ValueError(
                    "record at index %d must have the same fields as schema" % index
                )
            if schema is not None:
                for name in columns:
                    if name not in field_values:
                        raise ValueError(
                            "record at index %d is missing schema field %r"
                            % (index, name)
                        )
            field_rows.append(tuple(field_values[name] for name in columns))
        rows = field_rows
    elif isinstance(first_record, Sequence) and not isinstance(
        first_record, _SCALAR_SEQUENCE_TYPES
    ):
        if schema is None:
            raise ValueError("schema is required for sequence records")
        columns = schema
        _validate_schema(columns)
        requires_normalization = False
        for index, record in enumerate(data):
            if not isinstance(record, Sequence) or isinstance(
                record, _SCALAR_SEQUENCE_TYPES
            ):
                raise TypeError(
                    "each record must be a sequence of values, "
                    "such as a list or tuple; invalid record at index %d" % index
                )
            if type(record) not in (list, tuple):
                requires_normalization = True
            if len(record) != len(columns):
                raise ValueError(
                    "record at index %d has %d values but schema has %d fields"
                    % (index, len(record), len(columns))
                )
        sequence_rows = cast(Sequence[Sequence[Any]], data)
        if requires_normalization:
            rows = _SequenceRowIterable(sequence_rows)
        else:
            rows = sequence_rows
    else:
        raise TypeError(
            "each record must be a mapping or a sequence of values, "
            "such as a list or tuple; invalid record at index 0"
        )

    return _from_rows(rows, columns)


@PublicEvolving()
def from_dict(
    data: Mapping[str, Sequence[Any]], schema: Optional[List[str]] = None
) -> DataFrame:
    """
    Create a DataFrame from a column-oriented dictionary.

    All selected columns must contain the same non-zero number of values. ``schema`` can select a
    subset of columns and controls their order. If omitted, dictionary insertion order is used.

    :param data: Non-empty mapping of column names to value sequences.
    :param schema: Optional non-empty list of selected column names.
    :return: A DataFrame containing the selected columns.
    :raises TypeError: If ``data`` is not a mapping, or the selected schema or a selected column
        value has an invalid type.
    :raises ValueError: If the input is empty, schema field names are invalid, selected column
        lengths differ, or a selected column is missing.

    Example::

        >>> import pyflink.dataframe as pf
        >>> users = pf.from_dict(
        ...     {"name": ["Alice", "Bob"], "id": [1, 2]},
        ...     schema=["id", "name"],
        ... )

    .. versionadded:: 2.4.0
    """
    if not isinstance(data, Mapping):
        raise TypeError("data must be a mapping")
    if not data:
        raise ValueError("data must not be empty")
    columns = list(data.keys()) if schema is None else schema
    _validate_schema(columns)
    for name in columns:
        if name not in data:
            raise ValueError("column %r is not present in data" % name)
        values = data[name]
        if not isinstance(values, Sequence) or isinstance(
            values, _SCALAR_SEQUENCE_TYPES
        ):
            raise TypeError(
                "column %r values must be a sequence, "
                "such as a list or tuple" % name
            )
    lengths = {name: len(data[name]) for name in columns}
    if len(set(lengths.values())) != 1:
        raise ValueError("columns must have equal lengths")
    if next(iter(lengths.values())) == 0:
        raise ValueError("data must contain at least one row")
    return _from_rows(_ColumnRowIterable(data, columns), columns)
