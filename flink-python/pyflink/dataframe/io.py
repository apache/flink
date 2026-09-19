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

from typing import Dict, Optional, Tuple

from pyflink.dataframe.context import get_or_create_table_environment
from pyflink.dataframe.dataframe import DataFrame
from pyflink.dataframe.datatype import DataType
from pyflink.table import Schema, TableDescriptor
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ["read_generic"]


def _validate_connector(connector: str) -> None:
    if not isinstance(connector, str):
        raise TypeError("connector must be a string")
    if not connector:
        raise ValueError("connector must not be empty")


def _validate_options(options: Dict[str, str]) -> None:
    if not isinstance(options, dict):
        raise TypeError("options must be a dict of string keys and values")
    for key, value in options.items():
        if not isinstance(key, str):
            raise TypeError("option keys must be strings")
        if not key:
            raise ValueError("option keys must not be empty")
        if key == "connector":
            raise ValueError(
                "connector must be specified with the connector argument, not in options"
            )
        if not isinstance(value, str):
            raise TypeError(f"option {key!r} must have a string value")


def _validate_computed_columns(
    computed_columns: Optional[Dict[str, str]], physical_columns: Dict[str, DataType]
) -> Dict[str, str]:
    if computed_columns is None:
        return {}
    if not isinstance(computed_columns, dict):
        raise TypeError("computed_columns must be a dict of string keys and values")

    validated_columns: Dict[str, str] = {}
    for name, expression in computed_columns.items():
        if not isinstance(name, str):
            raise TypeError("computed column names must be strings")
        if not name:
            raise ValueError("computed column names must not be empty")
        if name in physical_columns:
            raise ValueError(
                f"computed column {name!r} conflicts with a physical column"
            )
        if not isinstance(expression, str):
            raise TypeError(f"computed column {name!r} must use a string expression")
        if not expression:
            raise ValueError(
                f"computed column {name!r} expression must not be empty"
            )
        validated_columns[name] = expression
    return validated_columns


def _validate_watermark(
    watermark: Optional[Tuple[str, str]],
) -> Optional[Tuple[str, str]]:
    if watermark is None:
        return None
    if not isinstance(watermark, tuple) or len(watermark) != 2:
        raise TypeError("watermark must be a tuple of (column, expression)")
    if any(not isinstance(value, str) for value in watermark):
        raise TypeError("watermark column and expression must be strings")
    if any(not value for value in watermark):
        raise ValueError("watermark column and expression must not be empty")
    return watermark


def _build_source_schema(
    schema: Dict[str, DataType],
    computed_columns: Optional[Dict[str, str]],
    watermark: Optional[Tuple[str, str]],
) -> Schema:
    if not isinstance(schema, dict):
        raise TypeError("schema must be a dict of column names and DataType values")
    if not schema:
        raise ValueError("schema must not be empty")

    schema_builder = Schema.new_builder()
    for name, data_type in schema.items():
        if not isinstance(name, str):
            raise TypeError("schema column names must be strings")
        if not name:
            raise ValueError("schema column names must not be empty")
        if not isinstance(data_type, DataType):
            raise TypeError(f"schema column {name!r} must use a DataType value")
        schema_builder.column(name, data_type._to_table_data_type())

    for name, expression in _validate_computed_columns(
        computed_columns, schema
    ).items():
        schema_builder.column_by_expression(name, expression)

    validated_watermark = _validate_watermark(watermark)
    if validated_watermark is not None:
        schema_builder.watermark(*validated_watermark)

    return schema_builder.build()


def _build_generic_descriptor(
    connector: str,
    options: Dict[str, str],
    schema: Optional[Schema] = None,
) -> TableDescriptor:
    _validate_connector(connector)
    _validate_options(options)

    descriptor_builder = TableDescriptor.for_connector(connector)
    if schema is not None:
        descriptor_builder.schema(schema)
    for key, value in options.items():
        descriptor_builder.option(key, value)
    return descriptor_builder.build()


@PublicEvolving()
def read_generic(
    connector: str,
    *,
    schema: Dict[str, DataType],
    options: Dict[str, str],
    computed_columns: Optional[Dict[str, str]] = None,
    watermark: Optional[Tuple[str, str]] = None,
) -> DataFrame:
    """
    Read data from a connector using its raw Table connector options.

    The connector must be available through Flink's factory discovery mechanism. Physical columns
    are followed by computed columns in dictionary insertion order. A watermark can reference a
    physical or computed timestamp column.

    :param connector: Factory identifier used as the ``connector`` Table option.
    :param schema: Non-empty mapping of physical column names to DataFrame data types.
    :param options: Connector options, excluding the reserved ``connector`` option.
    :param computed_columns: Optional SQL expressions keyed by computed column name.
    :param watermark: Optional ``(column, expression)`` watermark declaration.
    :return: A DataFrame backed by the configured source.
    :raises TypeError: If an argument has an invalid type.
    :raises ValueError: If a connector, schema, option key, computed column, or watermark value is
        empty, or if a computed column conflicts with a physical column.

    Example::

        >>> import pyflink.dataframe as pf
        >>> events = pf.read_generic(
        ...     "filesystem",
        ...     schema={
        ...         "id": pf.DataType.int64(),
        ...         "ts_millis": pf.DataType.int64(),
        ...     },
        ...     options={"path": "file:///tmp/events", "format": "csv"},
        ...     computed_columns={
        ...         "event_time": "TO_TIMESTAMP_LTZ(ts_millis, 3)"
        ...     },
        ...     watermark=(
        ...         "event_time", "event_time - INTERVAL '5' SECOND"
        ...     ),
        ... )

    .. versionadded:: 2.4.0
    """
    source_schema = _build_source_schema(schema, computed_columns, watermark)
    descriptor = _build_generic_descriptor(connector, options, source_schema)
    table_environment = get_or_create_table_environment()
    return DataFrame(table_environment.from_descriptor(descriptor))
