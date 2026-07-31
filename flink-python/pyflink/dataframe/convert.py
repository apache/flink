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

from enum import Enum
from typing import (
    Any,
    Collection,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from pyflink.dataframe.context import get_or_create_table_environment
from pyflink.dataframe.dataframe import DataFrame
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ["from_dict", "from_records"]

_SCALAR_SEQUENCE_TYPES = (str, bytes, bytearray, memoryview)


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

    return DataFrame(
        get_or_create_table_environment().from_elements(rows, schema)
    )


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
        for row_index in range(row_count)
    ]
    return DataFrame(
        get_or_create_table_environment().from_elements(rows, schema)
    )
