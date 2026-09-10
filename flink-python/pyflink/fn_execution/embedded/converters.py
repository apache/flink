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
import datetime as _dt
import pickle
from abc import ABC, abstractmethod
from typing import TypeVar, List, Tuple

from pemja import findClass

from pyflink.common import Row, RowKind, TypeInformation
from pyflink.common.typeinfo import (
    PickledBytesTypeInfo,
    PrimitiveArrayTypeInfo,
    BasicArrayTypeInfo,
    ObjectArrayTypeInfo,
    RowTypeInfo,
    TupleTypeInfo,
    MapTypeInfo,
    ListTypeInfo,
)
from pyflink.datastream import TimeWindow, CountWindow, GlobalWindow

IN = TypeVar("IN")
OUT = TypeVar("OUT")

# Java Window
JTimeWindow = findClass("org.apache.flink.table.runtime.operators.window.TimeWindow")
JCountWindow = findClass("org.apache.flink.table.runtime.operators.window.CountWindow")
JGlobalWindow = findClass(
    "org.apache.flink.streaming.api.windowing.windows.GlobalWindow"
)

# Java time types used by the temporal converters below. Looked up once at
# module load time so the converters don't pay the JNI cost per call.
JLocalDateTime = findClass("java.time.LocalDateTime")
JLocalDate = findClass("java.time.LocalDate")
JLocalTime = findClass("java.time.LocalTime")
JInstant = findClass("java.time.Instant")


class DataConverter(ABC):
    @abstractmethod
    def to_internal(self, value) -> IN:
        pass

    @abstractmethod
    def to_external(self, value) -> OUT:
        pass

    def __eq__(self, other):
        return type(self) == type(other)


class IdentityDataConverter(DataConverter):
    def to_internal(self, value) -> IN:
        return value

    def to_external(self, value) -> OUT:
        return value


class PickleDataConverter(DataConverter):
    def to_internal(self, value) -> IN:
        if value is None:
            return None

        return pickle.loads(value)

    def to_external(self, value) -> OUT:
        if value is None:
            return None

        return pickle.dumps(value)


class FlattenRowDataConverter(DataConverter):
    def __init__(self, field_data_converters: List[DataConverter]):
        self._field_data_converters = field_data_converters

    def to_internal(self, value) -> IN:
        if value is None:
            return None

        return tuple(
            [
                self._field_data_converters[i].to_internal(item)
                for i, item in enumerate(value)
            ]
        )

    def to_external(self, value) -> OUT:
        if value is None:
            return None

        return tuple(
            [
                self._field_data_converters[i].to_external(item)
                for i, item in enumerate(value)
            ]
        )


class RowDataConverter(DataConverter):
    def __init__(
        self, field_data_converters: List[DataConverter], field_names: List[str]
    ):
        self._field_data_converters = field_data_converters
        self._field_names = field_names

    def to_internal(self, value) -> IN:
        if value is None:
            return None

        row = Row()
        row._values = [
            self._field_data_converters[i].to_internal(item)
            for i, item in enumerate(value[1])
        ]
        row.set_field_names(self._field_names)
        row.set_row_kind(RowKind(value[0]))

        return row

    def to_external(self, value: Row) -> OUT:
        if value is None:
            return None

        values = value._values
        fields = tuple(
            [
                self._field_data_converters[i].to_external(values[i])
                for i in range(len(values))
            ]
        )
        return value.get_row_kind().value, fields


class TupleDataConverter(DataConverter):
    def __init__(self, field_data_converters: List[DataConverter]):
        self._field_data_converters = field_data_converters

    def to_internal(self, value) -> IN:
        if value is None:
            return None

        return tuple(
            [
                self._field_data_converters[i].to_internal(item)
                for i, item in enumerate(value)
            ]
        )

    def to_external(self, value: Tuple) -> OUT:
        if value is None:
            return None

        return tuple(
            [
                self._field_data_converters[i].to_external(item)
                for i, item in enumerate(value)
            ]
        )


class ListDataConverter(DataConverter):
    def __init__(self, field_converter: DataConverter):
        self._field_converter = field_converter

    def to_internal(self, value) -> IN:
        if value is None:
            return None

        return [self._field_converter.to_internal(item) for item in value]

    def to_external(self, value) -> OUT:
        if value is None:
            return None

        return [self._field_converter.to_external(item) for item in value]


class ArrayDataConverter(ListDataConverter):
    """Converter for ARRAY columns in embedded (thread) mode.

    The return types are intentionally asymmetric:

    - ``to_internal`` returns a ``list`` to match process-mode's Beam-based
      path (UDFs see mutable sequences on both modes; returning a ``tuple``
      would make ``arr.append(x)`` / ``arr[i] = v`` work only in process mode).
    - ``to_external`` returns a ``tuple`` because pemja's Python→Java coercion
      maps ``tuple`` → ``Object[]`` (what the downstream Java ``ArrayDataConverter``
      expects) but ``list`` → ``ArrayList`` (wrong shape — would break
      ``ArrayDataConverter.toExternalImpl``'s array-element loop).
    """

    def __init__(self, field_converter: DataConverter):
        super(ArrayDataConverter, self).__init__(field_converter)

    def to_internal(self, value) -> IN:
        if value is None:
            return None

        return list(super(ArrayDataConverter, self).to_internal(value))

    def to_external(self, value) -> OUT:
        if value is None:
            return None

        return tuple(super(ArrayDataConverter, self).to_external(value))


class LocalDateTimeConverter(DataConverter):
    """Converter for TIMESTAMP(precision) columns in embedded (thread) mode.

    On the input side (Java -> Python UDF argument) the value can arrive as
    either (a) a native Python ``datetime.datetime`` — pemja's automatic
    Java->Python conversion handles top-level ``java.sql.Timestamp`` /
    ``LocalDateTime`` method arguments — or (b) a ``pemja.PyJObject``
    wrapping a ``java.time.LocalDateTime``, which is what happens for values
    nested inside arrays, rows, or map values (pemja does not recursively
    auto-convert Object[] elements).

    On the output side (Python return -> Java) the converter explicitly
    constructs a ``java.time.LocalDateTime`` so the downstream Java converter
    chain — which uses ``DataFormatConverters.LocalDateTimeConverter`` as the
    default for TIMESTAMP fields in nested rows — receives the type it
    expects instead of a pemja-defaulted ``java.sql.Timestamp``.

    Precision: Python ``datetime`` is microsecond-precise, so values with
    TIMESTAMP(p) where p <= 6 round-trip without loss. For TIMESTAMP(9) the
    sub-microsecond nanoseconds are silently truncated.
    """

    def to_internal(self, value) -> IN:
        if value is None:
            return None
        # Fast path: pemja already produced a Python datetime (happens for
        # top-level method arguments when pemja auto-converts the bare Java
        # value crossing the JNI boundary).
        #
        # We check via ``type(value) is datetime`` rather than ``isinstance``
        # because pemja's C extension causes CPython's ``isinstance`` to raise
        # "SystemError: <built-in function isinstance> returned NULL without
        # setting an error" when invoked on a ``pemja.PyJObject``.
        # ``type(value)`` uses Py_TYPE at the C level and is unaffected.
        if type(value) is _dt.datetime:
            return value
        # Nested case: value is a pemja.PyJObject wrapping a Java
        # java.time.LocalDateTime. Extract the fields via Java getters.
        return _dt.datetime(
            value.getYear(),
            value.getMonthValue(),
            value.getDayOfMonth(),
            value.getHour(),
            value.getMinute(),
            value.getSecond(),
            value.getNano() // 1000,
        )

    def to_external(self, value) -> OUT:
        if value is None:
            return None
        return JLocalDateTime.of(
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            value.microsecond * 1000,
        )


class LocalDateConverter(DataConverter):
    """Converter for DATE columns in embedded (thread) mode.

    Mirrors LocalDateTimeConverter's pattern: the input value may be a native
    ``datetime.date`` (top-level method arg auto-converted by pemja) or a
    ``pemja.PyJObject`` wrapping a ``java.time.LocalDate`` (nested positions).
    The output is always an explicit Java ``LocalDate`` to match the external
    type the downstream Java converter chain expects.
    """

    def to_internal(self, value) -> IN:
        if value is None:
            return None
        if type(value) is _dt.date:
            return value
        return _dt.date(
            value.getYear(),
            value.getMonthValue(),
            value.getDayOfMonth(),
        )

    def to_external(self, value) -> OUT:
        if value is None:
            return None
        return JLocalDate.of(value.year, value.month, value.day)


class LocalTimeConverter(DataConverter):
    """Converter for TIME(precision) columns in embedded (thread) mode.

    Mirrors LocalDateTimeConverter's pattern against ``java.time.LocalTime``.
    Python ``datetime.time`` is microsecond-precise so TIME(9) loses
    sub-microsecond nanoseconds silently.
    """

    def to_internal(self, value) -> IN:
        if value is None:
            return None
        if type(value) is _dt.time:
            return value
        return _dt.time(
            value.getHour(),
            value.getMinute(),
            value.getSecond(),
            value.getNano() // 1000,
        )

    def to_external(self, value) -> OUT:
        if value is None:
            return None
        return JLocalTime.of(
            value.hour,
            value.minute,
            value.second,
            value.microsecond * 1000,
        )


class InstantConverter(DataConverter):
    """Converter for TIMESTAMP_LTZ / TIMESTAMP WITH LOCAL TIME ZONE columns.

    Java's ``java.time.Instant`` is absolute UTC. Python's ``datetime`` may be
    naive or tz-aware. Process mode returns naive ``datetime`` (interpreted as
    UTC); we match that shape to keep thread mode compatible.

    On the input side we extract epoch-second and nano from the Java Instant
    and construct a naive UTC ``datetime``.

    On the output side we accept both naive (treated as UTC, matching process
    mode) and tz-aware datetimes and convert to a Java ``Instant`` via
    ``Instant.ofEpochSecond(seconds, nanos)``.
    """

    def to_internal(self, value) -> IN:
        if value is None:
            return None
        if type(value) is _dt.datetime:
            return value
        # pemja.PyJObject wrapping java.time.Instant
        epoch_s = value.getEpochSecond()
        nano = value.getNano()
        # Construct a naive UTC datetime. Python datetime is microsecond
        # precise, so sub-microsecond nanoseconds are truncated.
        return _dt.datetime(1970, 1, 1) + _dt.timedelta(
            seconds=epoch_s, microseconds=nano // 1000
        )

    def to_external(self, value) -> OUT:
        if value is None:
            return None
        # Normalise to an epoch-second + nanos. For naive datetimes we assume
        # UTC, matching process-mode behaviour.
        if value.tzinfo is None:
            delta = value - _dt.datetime(1970, 1, 1)
        else:
            delta = value - _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)
        # Compute via integer microseconds to stay correct for pre-1970
        # (negative) epochs: int(-99.3) == -99 truncates toward zero, which
        # would produce a wrong (epoch_s, nano) pair. divmod flooring
        # guarantees 0 <= us_rem < 1_000_000.
        total_us = delta // _dt.timedelta(microseconds=1)
        epoch_s, us_rem = divmod(total_us, 1_000_000)
        return JInstant.ofEpochSecond(epoch_s, us_rem * 1000)


class DataStreamLocalDateTimeConverter(DataConverter):
    """Converter for TIMESTAMP columns on the DataStream path.

    Identical to ``LocalDateTimeConverter`` on the input side (Java → Python),
    but ``to_external`` returns the Python ``datetime`` as-is, letting pemja's
    auto-conversion produce ``java.sql.Timestamp``.  The DataStream→Table
    ``InputConversionOperator`` expects ``Timestamp``, whereas the Table API's
    internal ``DataFormatConverters.RowConverter`` expects ``LocalDateTime`` —
    hence the two separate converter classes.
    """

    def to_internal(self, value) -> IN:
        if value is None:
            return None
        if type(value) is _dt.datetime:
            return value
        return _dt.datetime(
            value.getYear(),
            value.getMonthValue(),
            value.getDayOfMonth(),
            value.getHour(),
            value.getMinute(),
            value.getSecond(),
            value.getNano() // 1000,
        )

    def to_external(self, value) -> OUT:
        if value is None:
            return None
        return value


class DataStreamLocalDateConverter(DataConverter):
    """DataStream variant of LocalDateConverter — to_external returns as-is."""

    def to_internal(self, value) -> IN:
        if value is None:
            return None
        if type(value) is _dt.date:
            return value
        return _dt.date(
            value.getYear(),
            value.getMonthValue(),
            value.getDayOfMonth(),
        )

    def to_external(self, value) -> OUT:
        if value is None:
            return None
        return value


class DataStreamLocalTimeConverter(DataConverter):
    """DataStream/Table variant of LocalTimeConverter.

    Unlike the other temporal converters, ``to_external`` must explicitly build a
    ``java.time.LocalTime``: pemja auto-converts Python ``datetime.time`` to
    ``java.sql.Time`` which only has seconds precision, silently dropping the
    sub-second microseconds needed for TIME(3)+ columns. Wrapping via ``JLocalTime.of``
    preserves nanos all the way to the Java-side converter's ``toInternalImpl``.
    """

    def to_internal(self, value) -> IN:
        if value is None:
            return None
        elif type(value) is _dt.time:
            return value
        else:
            return _dt.time(
                value.getHour(),
                value.getMinute(),
                value.getSecond(),
                value.getNano() // 1000,
            )

    def to_external(self, value) -> OUT:
        return (
            JLocalTime.of(
                value.hour,
                value.minute,
                value.second,
                value.microsecond * 1000,
            )
            if value is not None
            else None
        )


class DataStreamInstantConverter(DataConverter):
    """DataStream variant of InstantConverter — to_external returns as-is."""

    def to_internal(self, value) -> IN:
        if value is None:
            return None
        if type(value) is _dt.datetime:
            return value
        epoch_s = value.getEpochSecond()
        nano = value.getNano()
        return _dt.datetime(1970, 1, 1) + _dt.timedelta(
            seconds=epoch_s, microseconds=nano // 1000
        )

    def to_external(self, value) -> OUT:
        if value is None:
            return None
        return value


class DictDataConverter(DataConverter):
    def __init__(self, key_converter: DataConverter, value_converter: DataConverter):
        self._key_converter = key_converter
        self._value_converter = value_converter

    def to_internal(self, value) -> IN:
        if value is None:
            return None

        return {
            self._key_converter.to_internal(k): self._value_converter.to_internal(v)
            for k, v in value.items()
        }

    def to_external(self, value) -> OUT:
        if value is None:
            return None

        return {
            self._key_converter.to_external(k): self._value_converter.to_external(v)
            for k, v in value.items()
        }


class TimeWindowConverter(DataConverter):
    def to_internal(self, value) -> TimeWindow:
        return TimeWindow(value.getStart(), value.getEnd())

    def to_external(self, value: TimeWindow) -> OUT:
        return JTimeWindow(value.start, value.end)


class CountWindowConverter(DataConverter):
    def to_internal(self, value) -> CountWindow:
        return CountWindow(value.getId())

    def to_external(self, value: CountWindow) -> OUT:
        return JCountWindow(value.id)


class GlobalWindowConverter(DataConverter):
    def to_internal(self, value) -> IN:
        return GlobalWindow()

    def to_external(self, value) -> OUT:
        return JGlobalWindow.get()


def from_type_info_proto(type_info):
    # for data stream type information.
    from pyflink.fn_execution import flink_fn_execution_pb2

    type_info_name = flink_fn_execution_pb2.TypeInfo

    type_name = type_info.type_name
    if type_name == type_info_name.PICKLED_BYTES:
        return PickleDataConverter()
    elif type_name == type_info_name.ROW:
        return RowDataConverter(
            [
                from_type_info_proto(f.field_type)
                for f in type_info.row_type_info.fields
            ],
            [f.field_name for f in type_info.row_type_info.fields],
        )
    elif type_name == type_info_name.TUPLE:
        return TupleDataConverter(
            [
                from_type_info_proto(field_type)
                for field_type in type_info.tuple_type_info.field_types
            ]
        )
    elif type_name in (type_info_name.BASIC_ARRAY, type_info_name.OBJECT_ARRAY):
        return ArrayDataConverter(
            from_type_info_proto(type_info.collection_element_type)
        )
    elif type_name == type_info_name.LIST:
        return ListDataConverter(
            from_type_info_proto(type_info.collection_element_type)
        )
    elif type_name == type_info_name.MAP:
        return DictDataConverter(
            from_type_info_proto(type_info.map_type_info.key_type),
            from_type_info_proto(type_info.map_type_info.value_type),
        )
    elif type_name in (type_info_name.LOCAL_DATETIME, type_info_name.SQL_TIMESTAMP):
        return DataStreamLocalDateTimeConverter()
    elif type_name in (type_info_name.LOCAL_DATE, type_info_name.SQL_DATE):
        return DataStreamLocalDateConverter()
    elif type_name in (type_info_name.LOCAL_TIME, type_info_name.SQL_TIME):
        return DataStreamLocalTimeConverter()
    elif type_name in (type_info_name.INSTANT, type_info_name.LOCAL_ZONED_TIMESTAMP):
        return DataStreamInstantConverter()

    return IdentityDataConverter()


def from_schema_proto(schema, one_arg_optimized=False):
    field_converters = [from_field_type_proto(f.type) for f in schema.fields]
    if one_arg_optimized and len(field_converters) == 1:
        return field_converters[0]
    else:
        return FlattenRowDataConverter(field_converters)


def from_field_type_proto(field_type):
    from pyflink.fn_execution import flink_fn_execution_pb2

    schema_type_name = flink_fn_execution_pb2.Schema

    type_name = field_type.type_name
    if type_name == schema_type_name.ROW:
        return RowDataConverter(
            [from_field_type_proto(f.type) for f in field_type.row_schema.fields],
            [f.name for f in field_type.row_schema.fields],
        )
    elif type_name == schema_type_name.BASIC_ARRAY:
        return ArrayDataConverter(
            from_field_type_proto(field_type.collection_element_type)
        )
    elif type_name == schema_type_name.MAP:
        return DictDataConverter(
            from_field_type_proto(field_type.map_info.key_type),
            from_field_type_proto(field_type.map_info.value_type),
        )
    elif type_name == schema_type_name.TIMESTAMP:
        return DataStreamLocalDateTimeConverter()
    elif type_name == schema_type_name.LOCAL_ZONED_TIMESTAMP:
        return DataStreamInstantConverter()
    elif type_name == schema_type_name.DATE:
        return DataStreamLocalDateConverter()
    elif type_name == schema_type_name.TIME:
        return DataStreamLocalTimeConverter()

    return IdentityDataConverter()


def from_type_info(type_info: TypeInformation):
    if isinstance(type_info, (PickledBytesTypeInfo, RowTypeInfo, TupleTypeInfo)):
        return PickleDataConverter()
    elif isinstance(
        type_info, (PrimitiveArrayTypeInfo, BasicArrayTypeInfo, ObjectArrayTypeInfo)
    ):
        return ArrayDataConverter(from_type_info(type_info._element_type))
    elif isinstance(type_info, ListTypeInfo):
        return ListDataConverter(from_type_info(type_info.elem_type))
    elif isinstance(type_info, MapTypeInfo):
        return DictDataConverter(
            from_type_info(type_info._key_type_info),
            from_type_info(type_info._value_type_info),
        )

    return IdentityDataConverter()
