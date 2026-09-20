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

__all__ = ["read_generic", "read_json", "read_parquet"]


def _build_filesystem_options(
    path: str,
    file_format: str,
    options: Dict[str, Optional[str]],
    format_options: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    if not isinstance(path, str):
        raise TypeError("path must be a string")
    if not path:
        raise ValueError("path must not be empty")

    result = {"path": path, "format": file_format}
    result.update({key: value for key, value in options.items() if value is not None})
    if format_options is not None:
        _validate_options(format_options)
        for key, value in format_options.items():
            option = key if key.startswith(file_format + ".") else file_format + "." + key
            if option in result:
                raise ValueError(f"duplicate format option: {option!r}")
            result[option] = value
    _validate_options(result)
    return result


def _build_filesystem_sink_options(
    path: str,
    file_format: str,
    rolling_policy_file_size: str,
    rolling_policy_rollover_interval: str,
    rolling_policy_check_interval: Optional[str],
    partition_commit_trigger: str,
    partition_commit_delay: str,
    partition_commit_policy_kind: Optional[str],
    format_options: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    options = {
        "sink.rolling-policy.file-size": rolling_policy_file_size,
        "sink.rolling-policy.rollover-interval": rolling_policy_rollover_interval,
        "sink.partition-commit.trigger": partition_commit_trigger,
        "sink.partition-commit.delay": partition_commit_delay,
    }
    _validate_options(options)
    return _build_filesystem_options(
        path,
        file_format,
        {
            **options,
            "sink.rolling-policy.check-interval": rolling_policy_check_interval,
            "sink.partition-commit.policy.kind": partition_commit_policy_kind,
        },
        format_options,
    )


@PublicEvolving()
def read_parquet(
    path: str,
    *,
    schema: Dict[str, DataType],
    monitor_interval: Optional[str] = None,
    path_regex_pattern: Optional[str] = None,
) -> DataFrame:
    """
    Read Parquet files using Flink's filesystem connector.

    The filesystem connector and Parquet format must be available to Flink. By default,
    the source reads the existing files once. Setting ``monitor_interval`` creates a
    continuous source that discovers new files.

    :param path: File or directory URI supported by Flink's filesystem implementations.
    :param schema: Mapping of column names to DataFrame data types.
    :param monitor_interval: Optional file discovery interval, for example ``"60s"``.
    :param path_regex_pattern: Optional regular expression filtering source file paths.
    :return: A DataFrame backed by the Parquet source.
    :raises TypeError: If an argument has an invalid type.
    :raises ValueError: If the path or schema is empty.

    Example::

        >>> import pyflink.dataframe as pf
        >>> events = pf.read_parquet(
        ...     "file:///tmp/events",
        ...     schema={"id": pf.DataType.int64(), "name": pf.DataType.string()},
        ... )

    .. versionadded:: 2.4.0
    """
    options = _build_filesystem_options(
        path,
        "parquet",
        {
            "source.monitor-interval": monitor_interval,
            "source.path.regex-pattern": path_regex_pattern,
        },
    )
    return read_generic("filesystem", schema=schema, options=options)


@PublicEvolving()
def read_json(
    path: str,
    *,
    schema: Dict[str, DataType],
    monitor_interval: Optional[str] = None,
    path_regex_pattern: Optional[str] = None,
    format_options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """
    Read newline-delimited JSON files using Flink's filesystem connector.

    The filesystem connector and JSON format must be available to Flink. By default,
    the source reads the existing files once. Setting ``monitor_interval`` creates a
    continuous source that discovers new files.

    :param path: File or directory URI supported by Flink's filesystem implementations.
    :param schema: Mapping of column names to DataFrame data types.
    :param monitor_interval: Optional file discovery interval, for example ``"60s"``.
    :param path_regex_pattern: Optional regular expression filtering source file paths.
    :param format_options: JSON format options with string values. Keys may include or omit
        the ``json.`` prefix, for example ``{"ignore-parse-errors": "true"}``.
    :return: A DataFrame backed by the JSON source.
    :raises TypeError: If an argument has an invalid type.
    :raises ValueError: If the path or schema is empty, or format option keys are invalid
        or duplicated after adding the ``json.`` prefix.

    Example::

        >>> import pyflink.dataframe as pf
        >>> events = pf.read_json(
        ...     "file:///tmp/events.json",
        ...     schema={"id": pf.DataType.int64()},
        ...     format_options={"ignore-parse-errors": "true"},
        ... )

    .. versionadded:: 2.4.0
    """
    options = _build_filesystem_options(
        path,
        "json",
        {
            "source.monitor-interval": monitor_interval,
            "source.path.regex-pattern": path_regex_pattern,
        },
        format_options,
    )
    return read_generic("filesystem", schema=schema, options=options)


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
