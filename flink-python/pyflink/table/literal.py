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
from array import array

from pyflink.common import Row
from pyflink.java_gateway import get_gateway
from pyflink.table.types import (
    _array_type_mappings,
    _to_java_data_type,
    ArrayType,
    DataType,
    DateType,
    DayTimeIntervalType,
    LocalZonedTimestampType,
    MapType,
    MultisetType,
    RowType,
    TimeType,
    TimestampType,
)
from pyflink.util.api_stability_decorators import Internal


@Internal()
def _to_java_literal_value(value, data_type: DataType = None):
    """Converts Python-only literal values into objects accepted by Py4J."""
    if data_type is None:
        return _to_java_inferred_literal_value(value)
    return _to_java_typed_literal_value(value, data_type)


def _to_java_inferred_literal_value(value):
    if value is None:
        return value

    gateway = get_gateway()
    jvm = gateway.jvm
    if isinstance(value, datetime.datetime):
        return _to_java_typed_literal_value(value, TimestampType())
    elif isinstance(value, datetime.date):
        return _to_java_typed_literal_value(value, DateType())
    elif isinstance(value, datetime.time):
        return _to_java_typed_literal_value(value, TimeType())
    elif isinstance(value, datetime.timedelta):
        return _to_java_typed_literal_value(
            value,
            DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND),
        )
    elif isinstance(value, array):
        if value.typecode not in _array_type_mappings:
            raise TypeError(f"not supported type: array({value.typecode})")
        element_data_type = _to_java_data_type(_array_type_mappings[value.typecode])
        j_array = jvm.java.lang.reflect.Array.newInstance(
            element_data_type.getConversionClass(), len(value)
        )
        for pos, element in enumerate(value):
            j_array[pos] = element
        if not value:
            array_data_type = ArrayType(_array_type_mappings[value.typecode]).not_null()
            return (
                jvm.org.apache.flink.table.utils.python.PythonTableUtils
                .createInferredArrayValue(j_array, _to_java_data_type(array_data_type))
            )
        return j_array
    elif isinstance(value, (list, tuple)):
        j_values = jvm.java.util.ArrayList()
        for element in value:
            j_values.add(_to_java_inferred_literal_value(element))
        return j_values
    elif isinstance(value, Row):
        return _to_java_row(value)
    return value


def _to_java_instant(value, jvm):
    utc_value = value.astimezone(datetime.timezone.utc)
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    delta = utc_value - epoch
    # Avoid floating-point timestamp conversion for pre-epoch and subsecond values.
    seconds = delta.days * 86400 + delta.seconds
    return jvm.java.time.Instant.ofEpochSecond(seconds, utc_value.microsecond * 1000)


def _to_java_local_datetime(value, jvm):
    return jvm.java.time.LocalDateTime.ofInstant(
        _to_java_instant(value, jvm), jvm.java.time.ZoneId.systemDefault()
    )


def _to_java_typed_literal_value(value, data_type: DataType):
    if value is None or data_type._conversion_cls:
        return value

    jvm = get_gateway().jvm
    if isinstance(data_type, DateType) and isinstance(value, datetime.datetime):
        value = value.date()
    if isinstance(data_type, DateType) and isinstance(value, datetime.date):
        return jvm.java.time.LocalDate.of(value.year, value.month, value.day)
    elif isinstance(data_type, TimeType) and isinstance(value, datetime.time):
        if value.utcoffset() is not None:
            # TIME has no date. Use the Unix epoch date to match PyFlink's existing transport
            # conversion when rendering an offset time in the client JVM's local time zone.
            date_time = datetime.datetime.combine(datetime.date(1970, 1, 1), value)
            return _to_java_local_datetime(date_time, jvm).toLocalTime()
        return jvm.java.time.LocalTime.of(
            value.hour, value.minute, value.second, value.microsecond * 1000
        )
    elif isinstance(data_type, TimestampType) and isinstance(value, datetime.datetime):
        if value.utcoffset() is not None:
            # Match PyFlink's TIMESTAMP transport conversion by rendering the instant in the
            # client JVM's local time zone before dropping the zone information.
            return _to_java_local_datetime(value, jvm)
        return jvm.java.time.LocalDateTime.of(
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            value.microsecond * 1000,
        )
    elif isinstance(data_type, LocalZonedTimestampType) and isinstance(
        value, datetime.datetime
    ):
        if value.utcoffset() is None:
            value = value.astimezone()
        return _to_java_instant(value, jvm)
    elif isinstance(data_type, DayTimeIntervalType) and isinstance(
        value, datetime.timedelta
    ):
        seconds = value.days * 86400 + value.seconds
        return jvm.java.time.Duration.ofSeconds(seconds, value.microseconds * 1000)
    elif isinstance(data_type, ArrayType) and isinstance(value, (list, tuple, array)):
        j_values = jvm.java.util.ArrayList()
        for element in value:
            j_values.add(_to_java_typed_literal_value(element, data_type.element_type))
        return j_values
    elif isinstance(data_type, MultisetType) and isinstance(value, dict):
        j_values = jvm.java.util.HashMap()
        for element, count in value.items():
            j_values.put(
                _to_java_typed_literal_value(element, data_type.element_type), count
            )
        return j_values
    elif isinstance(data_type, MapType) and isinstance(value, dict):
        j_values = jvm.java.util.HashMap()
        for key, map_value in value.items():
            j_values.put(
                _to_java_typed_literal_value(key, data_type.key_type),
                _to_java_typed_literal_value(map_value, data_type.value_type),
            )
        return j_values
    elif isinstance(data_type, RowType):
        if isinstance(value, Row):
            return _to_java_row(value, data_type)
        elif isinstance(value, dict):
            j_values = jvm.java.util.HashMap()
            for field in data_type.fields:
                j_values.put(
                    field.name,
                    _to_java_typed_literal_value(value.get(field.name), field.data_type),
                )
            return j_values
        elif isinstance(value, (list, tuple)):
            j_values = jvm.java.util.ArrayList()
            for pos, field_value in enumerate(value):
                if pos < len(data_type.fields):
                    field_value = _to_java_typed_literal_value(
                        field_value, data_type.fields[pos].data_type
                    )
                else:
                    field_value = _to_java_inferred_literal_value(field_value)
                j_values.add(field_value)
            return j_values
    return value


def _to_java_row(value: Row, data_type: RowType = None):
    jvm = get_gateway().jvm
    if hasattr(value, "_fields"):
        j_row = jvm.org.apache.flink.types.Row.withNames(value.get_row_kind().to_j_row_kind())
        field_names = (
            value._fields
            if data_type is None
            else [field.name for field in data_type.fields]
        )
        for pos, field_name in enumerate(field_names):
            field_value = value[field_name]
            if data_type is not None:
                field_value = _to_java_typed_literal_value(
                    field_value, data_type.fields[pos].data_type
                )
            else:
                field_value = _to_java_inferred_literal_value(field_value)
            j_row.setField(field_name, field_value)
        if data_type is not None:
            # Keep undeclared fields so Java validates the original Row arity.
            for field_name in value._fields:
                if field_name not in field_names:
                    j_row.setField(
                        field_name,
                        _to_java_inferred_literal_value(value[field_name]),
                    )
        return j_row

    j_row = jvm.org.apache.flink.types.Row.withPositions(
        value.get_row_kind().to_j_row_kind(), len(value)
    )
    for pos, field_value in enumerate(value):
        if data_type is not None and pos < len(data_type.fields):
            field_value = _to_java_typed_literal_value(
                field_value, data_type.fields[pos].data_type
            )
        else:
            field_value = _to_java_inferred_literal_value(field_value)
        j_row.setField(pos, field_value)
    return j_row
