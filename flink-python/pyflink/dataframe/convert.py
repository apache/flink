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

from typing import Any, Iterable, Iterator, List, Mapping, Optional, Sequence, Union, cast

from pyflink.dataframe.context import get_or_create_table_environment
from pyflink.dataframe.dataframe import DataFrame
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ["from_dict", "from_records"]


class _TupleRecordIterable:
    def __init__(self, records: Sequence[Sequence[Any]]):
        self._records = records

    def __iter__(self) -> Iterator[Sequence[Any]]:
        return (tuple(record) for record in self._records)


def _validate_schema(schema: List[str]) -> None:
    if not schema:
        raise ValueError("schema must not be empty")
    if not isinstance(schema, list) or any(not isinstance(name, str) for name in schema):
        raise TypeError("schema must be a list of strings")


@PublicEvolving()
def from_records(
    data: Sequence[Union[Sequence[Any], Mapping[str, Any]]],
    schema: Optional[List[str]] = None,
) -> DataFrame:
    """
    Create a DataFrame from row-oriented records.

    Records may be mappings or equal-width non-string sequences. Mapping keys from the first
    record define the field names when ``schema`` is omitted. A schema is required for sequence
    records. Field types are inferred from the record values.

    :param data: Non-empty sequence of mapping or non-string sequence records.
    :param schema: Optional non-empty list of field names.
    :return: A DataFrame containing the records.
    :raises TypeError: If a record or schema has an invalid type.
    :raises ValueError: If data or schema is empty, a required schema is omitted, or record widths
        differ.

    Example::

        >>> import pyflink.dataframe as pf
        >>> users = pf.from_records([
        ...     {"id": 1, "name": "Alice"},
        ...     {"id": 2, "name": "Bob"},
        ... ])
        >>> users = pf.from_records(
        ...     [(1, "Alice"), (2, "Bob")], schema=["id", "name"]
        ... )

    .. versionadded:: 2.4.0
    """
    if not data:
        raise ValueError("data must not be empty")

    first_record = data[0]
    rows: Iterable[Sequence[Any]]
    if isinstance(first_record, Mapping):
        columns = list(first_record.keys()) if schema is None else schema
        _validate_schema(columns)
        mapping_rows: List[Sequence[Any]] = []
        for record in data:
            if not isinstance(record, Mapping):
                raise TypeError("records must all be mappings")
            mapping_rows.append(tuple(record.get(name) for name in columns))
        rows = mapping_rows
    elif isinstance(first_record, Sequence) and not isinstance(
        first_record, (str, bytes, bytearray)
    ):
        if schema is None:
            raise ValueError("schema is required for sequence records")
        _validate_schema(schema)
        requires_normalization = False
        for index, record in enumerate(data):
            if not isinstance(record, Sequence) or isinstance(
                record, (str, bytes, bytearray)
            ):
                raise TypeError("records must all be non-string sequences")
            if not isinstance(record, (list, tuple)):
                requires_normalization = True
            if len(record) != len(schema):
                raise ValueError(
                    "record at index %d has %d values but schema has %d fields"
                    % (index, len(record), len(schema))
                )
        columns = schema
        sequence_rows = cast(Sequence[Sequence[Any]], data)
        if requires_normalization:
            rows = _TupleRecordIterable(sequence_rows)
        else:
            rows = sequence_rows
    else:
        raise TypeError("records must be mappings or non-string sequences")

    table = get_or_create_table_environment().from_elements(rows, list(columns))
    return DataFrame(table)


@PublicEvolving()
def from_dict(
    data: Mapping[str, Sequence[Any]], schema: Optional[List[str]] = None
) -> DataFrame:
    """
    Create a DataFrame from a column-oriented dictionary.

    All columns must contain the same non-zero number of values. ``schema`` can select a subset of
    columns and controls their order. If omitted, dictionary insertion order is used.

    :param data: Non-empty mapping of column names to value sequences.
    :param schema: Optional non-empty list of selected column names.
    :return: A DataFrame containing the selected columns.
    :raises TypeError: If the selected schema is not a list of strings.
    :raises ValueError: If the input is empty, column lengths differ, or a column is missing.

    Example::

        >>> import pyflink.dataframe as pf
        >>> users = pf.from_dict(
        ...     {"name": ["Alice", "Bob"], "id": [1, 2]},
        ...     schema=["id", "name"],
        ... )

    .. versionadded:: 2.4.0
    """
    if not data:
        raise ValueError("data must not be empty")
    lengths = {name: len(values) for name, values in data.items()}
    if len(set(lengths.values())) != 1:
        raise ValueError("columns must have equal lengths")
    if next(iter(lengths.values())) == 0:
        raise ValueError("data must contain at least one row")
    columns = list(data.keys()) if schema is None else schema
    _validate_schema(columns)
    for name in columns:
        if name not in data:
            raise ValueError("column %r is not present in data" % name)
    row_count = next(iter(lengths.values()))
    records = [
        tuple(data[name][row_index] for name in columns)
        for row_index in range(row_count)
    ]
    return from_records(records, schema=list(columns))
