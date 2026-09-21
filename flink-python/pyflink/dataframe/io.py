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

from typing import Dict, List, Optional, Tuple, Union

from pyflink.dataframe.context import get_or_create_table_environment
from pyflink.dataframe.dataframe import DataFrame, _normalize_subset
from pyflink.dataframe.datatype import DataType
from pyflink.table import Schema, TableDescriptor
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ["read_generic", "read_json", "read_parquet"]


def _build_filesystem_options(
    path: str,
    file_format: str,
    options: Dict[str, Optional[str]],
    format_options: Optional[Dict[str, str]] = None,
    *,
    connector_options: Optional[Dict[str, str]] = None,
    format_parameters: Optional[Dict[str, Optional[str]]] = None,
) -> Dict[str, str]:
    if not isinstance(path, str):
        raise TypeError("path must be a string")
    if not path:
        raise ValueError("path must not be empty")

    result = {"path": path, "format": file_format}
    if connector_options is not None:
        _validate_options(connector_options)
        for key in connector_options:
            if key in ("path", "format"):
                raise ValueError(f"{key!r} must not be specified in connector_options")
            if key.startswith(file_format + "."):
                raise ValueError(f"format option {key!r} must be specified in format_options")
        _merge_options(result, connector_options)
    _merge_options(result, {key: value for key, value in options.items() if value is not None})

    normalized_format_options: Dict[str, str] = {}
    if format_options is not None:
        _validate_options(format_options)
        for key, value in format_options.items():
            option = key if key.startswith(file_format + ".") else file_format + "." + key
            if option in normalized_format_options:
                raise ValueError(f"duplicate format option: {option!r}")
            normalized_format_options[option] = value
    if format_parameters is not None:
        _merge_options(normalized_format_options, {
            file_format + "." + key: value
            for key, value in format_parameters.items() if value is not None
        })
    _merge_options(result, normalized_format_options)
    return result


def _merge_options(target: Dict[str, str], options: Dict[str, str]) -> None:
    _validate_options(options)
    for key, value in options.items():
        if key in target and target[key] != value:
            raise ValueError(f"conflicting values for option {key!r}")
        target[key] = value


def _boolean_option(value: Optional[bool], name: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a bool or None")
    return str(value).lower()


def _parallelism_option(value: Optional[int]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("sink_parallelism must be an int or None")
    return str(value)


def _build_filesystem_sink_options(
    path: str,
    file_format: str,
    options: Dict[str, Optional[str]],
    format_parameters: Dict[str, Optional[str]],
    connector_options: Optional[Dict[str, str]] = None,
    format_options: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    result = _build_filesystem_options(
        path, file_format, options, format_options,
        connector_options=connector_options, format_parameters=format_parameters,
    )
    # Apply defaults after merging explicit settings from either API entry point.
    defaults = {
        "sink.rolling-policy.file-size": "128mb",
        "sink.rolling-policy.rollover-interval": "30min",
        "sink.partition-commit.trigger": "process-time",
        "sink.partition-commit.delay": "0s",
    }
    if file_format == "parquet":
        defaults["parquet.compression"] = "SNAPPY"
    for key, value in defaults.items():
        result.setdefault(key, value)
    return result


@PublicEvolving()
def read_parquet(
    path: str,
    *,
    schema: Dict[str, DataType],
    partition_by: Optional[Union[str, List[str]]] = None,
    monitor_interval: Optional[str] = None,
    path_regex_pattern: Optional[str] = None,
    utc_timezone: Optional[bool] = None,
    connector_options: Optional[Dict[str, str]] = None,
    format_options: Optional[Dict[str, str]] = None,
    computed_columns: Optional[Dict[str, str]] = None,
    watermark: Optional[Tuple[str, str]] = None,
) -> DataFrame:
    """
    Read Parquet files using Flink's filesystem connector.

    The filesystem connector and Parquet format must be available to Flink. By default,
    the source reads the existing files once. Setting ``monitor_interval`` creates an
    unbounded source that discovers new files; it requires streaming execution. Files are
    identified by path and processed once, rather than tailed for appended data.
    With ``partition_by``, monitoring is limited to the partitions discovered during planning;
    new partition directories are not discovered, and an initially empty partitioned source
    remains empty.
    The current filesystem connector cannot read default partitions representing null or empty
    partition values.

    :param path: File or directory URI supported by Flink's filesystem implementations.
    :param schema: Mapping of column names to DataFrame data types.
    :param partition_by: Partition column name or non-empty list of names in directory order.
        These columns must be declared in ``schema`` and are read from the partition paths.
    :param monitor_interval: Positive file discovery interval, for example ``"60s"``.
        If unset, the source performs a bounded scan.
    :param path_regex_pattern: Java regular expression matched against the entire file path
        (excluding the URI scheme and authority), for example ``".*[.]parquet"``. This is not
        a substring or filename-only match. Hidden files are excluded.
    :param utc_timezone: Use UTC for Parquet timestamp conversion. Defaults to ``False``,
        which uses the JVM default time zone, independently of the session time zone.
    :param connector_options: Additional filesystem options with string keys and values.
        The ``connector``, ``path`` and ``format`` keys are reserved. Format options belong in
        ``format_options``. Explicit parameter and dictionary values must agree when both are set.
    :param format_options: Parquet options with string values, with or without the ``parquet.``
        prefix. Duplicate normalized keys are rejected. ``None`` parameters leave dictionary
        values unchanged; conflicting explicit values are rejected.
    :param computed_columns: Optional SQL expressions keyed by computed column name.
    :param watermark: Optional ``(column, expression)`` watermark declaration. The column must
        have a TIMESTAMP or TIMESTAMP_LTZ type with precision from 0 to 3.
    :return: A DataFrame backed by the Parquet source.
    :raises TypeError: If an argument has an invalid type.
    :raises ValueError: If the path, schema or partition keys are invalid, or options conflict.

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
        format_options,
        connector_options=connector_options,
        format_parameters={"utc-timezone": _boolean_option(utc_timezone, "utc_timezone")},
    )
    return _read("filesystem", schema, options, computed_columns, watermark, partition_by)


@PublicEvolving()
def read_json(
    path: str,
    *,
    schema: Dict[str, DataType],
    partition_by: Optional[Union[str, List[str]]] = None,
    monitor_interval: Optional[str] = None,
    path_regex_pattern: Optional[str] = None,
    ignore_parse_errors: Optional[bool] = None,
    fail_on_missing_field: Optional[bool] = None,
    timestamp_format: Optional[str] = None,
    connector_options: Optional[Dict[str, str]] = None,
    format_options: Optional[Dict[str, str]] = None,
    computed_columns: Optional[Dict[str, str]] = None,
    watermark: Optional[Tuple[str, str]] = None,
) -> DataFrame:
    """
    Read newline-delimited JSON files using Flink's filesystem connector.

    The filesystem connector and JSON format must be available to Flink. By default,
    the source reads the existing files once. Setting ``monitor_interval`` creates an
    unbounded source that discovers new files; it requires streaming execution. Files are
    identified by path and processed once, rather than tailed for appended data.
    With ``partition_by``, monitoring is limited to the partitions discovered during planning;
    new partition directories are not discovered, and an initially empty partitioned source
    remains empty.
    The current filesystem connector cannot read default partitions representing null or empty
    partition values.

    :param path: File or directory URI supported by Flink's filesystem implementations.
    :param schema: Mapping of column names to DataFrame data types.
    :param partition_by: Partition column name or non-empty list of names in directory order.
        These columns must be declared in ``schema`` and are read from the partition paths.
    :param monitor_interval: Positive file discovery interval, for example ``"60s"``.
        If unset, the source performs a bounded scan.
    :param path_regex_pattern: Java regular expression matched against the entire file path
        (excluding the URI scheme and authority), for example ``".*[.]json"``. This is not
        a substring or filename-only match. Hidden files are excluded.
    :param ignore_parse_errors: Skip malformed fields or rows instead of failing; defaults to
        ``False``. Invalid fields are set to null where possible.
    :param fail_on_missing_field: Fail on missing JSON fields; defaults to ``False``.
        Cannot be enabled together with ``ignore_parse_errors``.
    :param timestamp_format: Timestamp representation, ``"SQL"`` (default) or ``"ISO-8601"``.
    :param connector_options: Additional filesystem options with string keys and values.
        The ``connector``, ``path`` and ``format`` keys are reserved. Format options belong in
        ``format_options``. Explicit parameter and dictionary values must agree when both are set.
    :param format_options: JSON format options with string values. Keys may include or omit
        the ``json.`` prefix, for example ``{"ignore-parse-errors": "true"}``. ``None`` parameters
        leave dictionary values unchanged; conflicting explicit values are rejected.
    :param computed_columns: Optional SQL expressions keyed by computed column name.
    :param watermark: Optional ``(column, expression)`` watermark declaration. The column must
        have a TIMESTAMP or TIMESTAMP_LTZ type with precision from 0 to 3.
    :return: A DataFrame backed by the JSON source.
    :raises TypeError: If an argument has an invalid type.
    :raises ValueError: If the path, schema or partition keys are invalid, options conflict,
        or format option keys are duplicated after adding the ``json.`` prefix.

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
        connector_options=connector_options,
        format_parameters={
            "ignore-parse-errors": _boolean_option(ignore_parse_errors, "ignore_parse_errors"),
            "fail-on-missing-field": _boolean_option(
                fail_on_missing_field, "fail_on_missing_field"),
            "timestamp-format.standard": timestamp_format,
        },
    )
    return _read("filesystem", schema, options, computed_columns, watermark, partition_by)


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
    partition_by: Optional[Union[str, List[str]]] = None,
) -> TableDescriptor:
    _validate_connector(connector)
    _validate_options(options)
    partition_keys = _normalize_subset(partition_by, "partition_by") or []
    if any(not key for key in partition_keys):
        raise ValueError("partition_by column names must not be empty")
    if len(set(partition_keys)) != len(partition_keys):
        raise ValueError("partition_by column names must not be duplicated")

    descriptor_builder = TableDescriptor.for_connector(connector)
    if schema is not None:
        descriptor_builder.schema(schema)
    if partition_keys:
        descriptor_builder.partitioned_by(*partition_keys)
    for key, value in options.items():
        descriptor_builder.option(key, value)
    return descriptor_builder.build()


def _read(
    connector: str,
    schema: Dict[str, DataType],
    options: Dict[str, str],
    computed_columns: Optional[Dict[str, str]],
    watermark: Optional[Tuple[str, str]],
    partition_by: Optional[Union[str, List[str]]] = None,
) -> DataFrame:
    source_schema = _build_source_schema(schema, computed_columns, watermark)
    descriptor = _build_generic_descriptor(connector, options, source_schema, partition_by)
    table_environment = get_or_create_table_environment()
    return DataFrame(table_environment.from_descriptor(descriptor))


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
    return _read(connector, schema, options, computed_columns, watermark)
