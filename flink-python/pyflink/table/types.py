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

import calendar
import ctypes
import datetime
import decimal
import sys
import time
from array import array
from copy import copy
from functools import reduce
from threading import RLock

from py4j.java_gateway import JavaClass, JavaObject, get_java_class

from pyflink.util.utils import to_jarray
from pyflink.java_gateway import get_gateway

__all__ = ['DataTypes', 'UserDefinedType', 'Row']


class DataType(object):
    """
    Describes the data type of a value in the table ecosystem. Instances of this class can be used
    to declare input and/or output types of operations.

    :class:`DataType` has two responsibilities: declaring a logical type and giving hints
    about the physical representation of data to the optimizer. While the logical type is mandatory,
    hints are optional but useful at the edges to other APIs.

    The logical type is independent of any physical representation and is close to the "data type"
    terminology of the SQL standard.

    Physical hints are required at the edges of the table ecosystem. Hints indicate the data format
    that an implementation expects.

    :param nullable: boolean, whether the type can be null (None) or not.
    """

    def __init__(self, nullable=True):
        self._nullable = nullable
        self._conversion_cls = ''

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, str(self._nullable).lower())

    def __str__(self, *args, **kwargs):
        return self.__class__.type_name()

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)

    def not_null(self):
        cp = copy(self)
        cp._nullable = False
        return cp

    def nullable(self):
        cp = copy(self)
        cp._nullable = True
        return cp

    @classmethod
    def type_name(cls):
        return cls.__name__[:-4].upper()

    def bridged_to(self, conversion_cls):
        """
        Adds a hint that data should be represented using the given class when entering or leaving
        the table ecosystem.

        :param conversion_cls: the string representation of the conversion class
        """
        self._conversion_cls = conversion_cls
        return self

    def need_conversion(self):
        """
        Does this type need to conversion between Python object and internal SQL object.

        This is used to avoid the unnecessary conversion for ArrayType/MultisetType/MapType/RowType.
        """
        return False

    def to_sql_type(self, obj):
        """
        Converts a Python object into an internal SQL object.
        """
        return obj

    def from_sql_type(self, obj):
        """
        Converts an internal SQL object into a native Python object.
        """
        return obj


class AtomicType(DataType):
    """
    An internal type used to represent everything that is not
    arrays, rows, and maps.
    """

    def __init__(self, nullable=True):
        super(AtomicType, self).__init__(nullable)


class NullType(AtomicType):
    """
    Null type.

    The data type representing None.
    """

    def __init__(self):
        super(NullType, self).__init__(True)


class NumericType(AtomicType):
    """
    Numeric data types.
    """

    def __init__(self, nullable=True):
        super(NumericType, self).__init__(nullable)


class IntegralType(NumericType):
    """
    Integral data types.
    """

    def __init__(self, nullable=True):
        super(IntegralType, self).__init__(nullable)


class FractionalType(NumericType):
    """
    Fractional data types.
    """

    def __init__(self, nullable=True):
        super(FractionalType, self).__init__(nullable)


class CharType(AtomicType):
    """
    Char data type. SQL CHAR(n)

    The serialized string representation is ``char(n)`` where ``n`` (default: 1) is the number of
    code points. ``n`` must have a value between 1 and 2147483647(0x7fffffff) (both inclusive).

    :param length: int, the string representation length.
    :param nullable: boolean, whether the type can be null (None) or not.
    """

    def __init__(self, length=1, nullable=True):
        super(CharType, self).__init__(nullable)
        self.length = length

    def __repr__(self):
        return 'CharType(%d, %s)' % (self.length, str(self._nullable).lower())


class VarCharType(AtomicType):
    """
    Varchar data type. SQL VARCHAR(n)

    The serialized string representation is ``varchar(n)`` where 'n' (default: 1) is the maximum
    number of code points. 'n' must have a value between 1 and 2147483647(0x7fffffff)
    (both inclusive).

    :param length: int, the maximum string representation length.
    :param nullable: boolean, whether the type can be null (None) or not.
    """

    def __init__(self, length=1, nullable=True):
        super(VarCharType, self).__init__(nullable)
        self.length = length

    def __repr__(self):
        return "VarCharType(%d, %s)" % (self.length, str(self._nullable).lower())


class BinaryType(AtomicType):
    """
    Binary (byte array) data type. SQL BINARY(n)

    The serialized string representation is ``binary(n)`` where ``n`` (default: 1) is the number of
    bytes. ``n`` must have a value between 1 and 2147483647(0x7fffffff) (both inclusive).

    :param length: int, the number of bytes.
    :param nullable: boolean, whether the type can be null (None) or not.
    """

    def __init__(self, length=1, nullable=True):
        super(BinaryType, self).__init__(nullable)
        self.length = length

    def __repr__(self):
        return "BinaryType(%d, %s)" % (self.length, str(self._nullable).lower())


class VarBinaryType(AtomicType):
    """
    Binary (byte array) data type. SQL VARBINARY(n)

    The serialized string representation is ``varbinary(n)`` where ``n`` (default: 1) is the
    maximum number of bytes. ``n`` must have a value between 1 and 0x7fffffff (both inclusive).

    :param length: int, the maximum number of bytes.
    :param nullable: boolean, whether the type can be null (None) or not.
    """

    def __init__(self, length=1, nullable=True):
        super(VarBinaryType, self).__init__(nullable)
        self.length = length

    def __repr__(self):
        return "VarBinaryType(%d, %s)" % (self.length, str(self._nullable).lower())


class BooleanType(AtomicType):
    """
    Boolean data types. SQL BOOLEAN

    :param nullable: boolean, whether the field can be null (None) or not.
    """

    def __init__(self, nullable=True):
        super(BooleanType, self).__init__(nullable)


class TinyIntType(IntegralType):
    """
    Byte data type. SQL TINYINT (8bits)

    :param nullable: boolean, whether the field can be null (None) or not.
    """

    def __init__(self, nullable=True):
        super(TinyIntType, self).__init__(nullable)


class SmallIntType(IntegralType):
    """
    Short data type. SQL SMALLINT (16bits)

    :param nullable: boolean, whether the field can be null (None) or not.
    """

    def __init__(self, nullable=True):
        super(SmallIntType, self).__init__(nullable)


class IntType(IntegralType):
    """
    Int data types. SQL INT (32bits)

    :param nullable: boolean, whether the field can be null (None) or not.
    """

    def __init__(self, nullable=True):
        super(IntType, self).__init__(nullable)


class BigIntType(IntegralType):
    """
    Long data types. SQL BIGINT (64bits)

    :param nullable: boolean, whether the field can be null (None) or not.
    """

    def __init__(self, nullable=True):
        super(BigIntType, self).__init__(nullable)


class FloatType(FractionalType):
    """
    Float data type. SQL FLOAT

    :param nullable: boolean, whether the field can be null (None) or not.
    """

    def __init__(self, nullable=True):
        super(FloatType, self).__init__(nullable)


class DoubleType(FractionalType):
    """
    Double data type. SQL DOUBLE

    :param nullable: boolean, whether the field can be null (None) or not.
    """

    def __init__(self, nullable=True):
        super(DoubleType, self).__init__(nullable)


class DecimalType(FractionalType):
    """
    Decimal (decimal.Decimal) data type.

    The DecimalType must have fixed precision (the maximum total number of digits)
    and scale (the number of digits on the right of dot). For example, (5, 2) can
    support the value from [-999.99 to 999.99].

    The precision can be up to 38, the scale must be less or equal to precision.

    When create a DecimalType, the default precision and scale is (10, 0). When infer
    schema from decimal.Decimal objects, it will be DecimalType(38, 18).

    :param precision: the number of digits in a number (default: 10)
    :param scale: the number of digits on right side of dot. (default: 0)
    :param nullable: boolean, whether the field can be null (None) or not.
    """

    def __init__(self, precision=10, scale=0, nullable=True):
        super(DecimalType, self).__init__(nullable)
        assert 1 <= precision <= 38
        assert 0 <= scale <= precision
        self.precision = precision
        self.scale = scale
        self.has_precision_info = True  # this is public API

    def __repr__(self):
        return "DecimalType(%d, %d, %s)" % (self.precision, self.scale, str(self._nullable).lower())


class DateType(AtomicType):
    """
    Date data type.  SQL DATE

    :param nullable: boolean, whether the field can be null (None) or not.
    """

    def __init__(self, nullable=True):
        super(DateType, self).__init__(nullable)

    EPOCH_ORDINAL = datetime.datetime(1970, 1, 1).toordinal()

    def need_conversion(self):
        return True

    def to_sql_type(self, d):
        if d is not None:
            return d.toordinal() - self.EPOCH_ORDINAL

    def from_sql_type(self, v):
        if v is not None:
            return datetime.date.fromordinal(v + self.EPOCH_ORDINAL)


class TimeType(AtomicType):
    """
    Time data type. SQL TIME

    The precision must be greater than or equal to 0 and less than or equal to 9.

    :param precision: int, the number of digits of fractional seconds (default: 0)
    :param nullable: boolean, whether the field can be null (None) or not.
    """

    EPOCH_ORDINAL = calendar.timegm(time.localtime(0)) * 10 ** 6

    def __init__(self, precision=0, nullable=True):
        super(TimeType, self).__init__(nullable)
        assert 0 <= precision <= 9
        self.precision = precision

    def __repr__(self):
        return "TimeType(%s, %s)" % (self.precision, str(self._nullable).lower())

    def need_conversion(self):
        return True

    def to_sql_type(self, t):
        if t is not None:
            if t.tzinfo is not None:
                offset = t.utcoffset()
                offset = offset if offset else datetime.timedelta()
                offset_microseconds =\
                    (offset.days * 86400 + offset.seconds) * 10 ** 6 + offset.microseconds
            else:
                offset_microseconds = self.EPOCH_ORDINAL
            minutes = t.hour * 60 + t.minute
            seconds = minutes * 60 + t.second
            return seconds * 10 ** 6 + t.microsecond - offset_microseconds

    def from_sql_type(self, t):
        if t is not None:
            seconds, microseconds = divmod(t + self.EPOCH_ORDINAL, 10 ** 6)
            minutes, seconds = divmod(seconds, 60)
            hours, minutes = divmod(minutes, 60)
            return datetime.time(hours, minutes, seconds, microseconds)


class TimestampType(AtomicType):
    """
    Timestamp data type. SQL TIMESTAMP WITHOUT TIME ZONE.

    Consisting of ``year-month-day hour:minute:second[.fractional]`` with up to nanosecond
    precision and values ranging from ``0000-01-01 00:00:00.000000000`` to
    ``9999-12-31 23:59:59.999999999``. Compared to the SQL standard, leap seconds (23:59:60 and
    23:59:61) are not supported.

    This class does not store or represent a time-zone. Instead, it is a description of
    the date, as used for birthdays, combined with the local time as seen on a wall clock.
    It cannot represent an instant on the time-line without additional information
    such as an offset or time-zone.

    The precision must be greater than or equal to 0 and less than or equal to 9.

    :param precision: int, the number of digits of fractional seconds (default: 6)
    :param nullable: boolean, whether the field can be null (None) or not.
    """

    def __init__(self, precision=6, nullable=True):
        super(TimestampType, self).__init__(nullable)
        assert 0 <= precision <= 9
        self.precision = precision

    def __repr__(self):
        return "TimestampType(%s, %s)" % (self.precision, str(self._nullable).lower())

    def need_conversion(self):
        return True

    def to_sql_type(self, dt):
        if dt is not None:
            seconds = (calendar.timegm(dt.utctimetuple()) if dt.tzinfo
                       else time.mktime(dt.timetuple()))
            return int(seconds) * 10 ** 6 + dt.microsecond

    def from_sql_type(self, ts):
        if ts is not None:
            return datetime.datetime.fromtimestamp(ts // 10 ** 6).replace(microsecond=ts % 10 ** 6)


class LocalZonedTimestampType(AtomicType):
    """
    Timestamp data type. SQL TIMESTAMP WITH LOCAL TIME ZONE.

    Consisting of ``year-month-day hour:minute:second[.fractional] zone`` with up to nanosecond
    precision and values ranging from ``0000-01-01 00:00:00.000000000 +14:59`` to
    ``9999-12-31 23:59:59.999999999 -14:59``. Compared to the SQL standard, Leap seconds (23:59:60
    and 23:59:61) are not supported.

    The value will be stored internally as a long value which stores all date and time
    fields, to a precision of nanoseconds, as well as the offset from UTC/Greenwich.

    The precision must be greater than or equal to 0 and less than or equal to 9.

    :param precision: int, the number of digits of fractional seconds (default: 6)
    :param nullable: boolean, whether the field can be null (None) or not.
    """

    EPOCH_ORDINAL = calendar.timegm(time.localtime(0)) * 10 ** 6

    def __init__(self, precision=6, nullable=True):
        super(LocalZonedTimestampType, self).__init__(nullable)
        assert 0 <= precision <= 9
        self.precision = precision

    def __repr__(self):
        return "LocalZonedTimestampType(%s, %s)" % (self.precision, str(self._nullable).lower())

    def need_conversion(self):
        return True

    def to_sql_type(self, dt):
        if dt is not None:
            seconds = (calendar.timegm(dt.utctimetuple()) if dt.tzinfo
                       else time.mktime(dt.timetuple()))
            return int(seconds) * 10 ** 6 + dt.microsecond + self.EPOCH_ORDINAL

    def from_sql_type(self, ts):
        if ts is not None:
            ts = ts - self.EPOCH_ORDINAL
            return datetime.datetime.fromtimestamp(ts // 10 ** 6).replace(microsecond=ts % 10 ** 6)


class ZonedTimestampType(AtomicType):
    """
    Timestamp data type with time zone. SQL TIMESTAMP WITH TIME ZONE.

    Consisting of ``year-month-day hour:minute:second[.fractional] zone`` with up to nanosecond
    precision and values ranging from {@code 0000-01-01 00:00:00.000000000 +14:59} to
    ``9999-12-31 23:59:59.999999999 -14:59``. Compared to the SQL standard, leap seconds (23:59:60
    and 23:59:61) are not supported.

    The value will be stored internally all date and time fields, to a precision of
    nanoseconds, and a time-zone, with a zone offset used to handle ambiguous local date-times.

    The precision must be greater than or equal to 0 and less than or equal to 9.

    :param precision: int, the number of digits of fractional seconds (default: 6)
    :param nullable: boolean, whether the field can be null (None) or not.
    """

    def __init__(self, precision=6, nullable=True):
        super(ZonedTimestampType, self).__init__(nullable)
        assert 0 <= precision <= 9
        self.precision = precision

    def __repr__(self):
        return "ZonedTimestampType(%s, %s)" % (self.precision, str(self._nullable).lower())

    def need_conversion(self):
        return True

    def to_sql_type(self, dt):
        if dt is not None:
            seconds = (calendar.timegm(dt.utctimetuple()) if dt.tzinfo
                       else time.mktime(dt.timetuple()))
            tzinfo = dt.tzinfo if dt.tzinfo else datetime.datetime.now(
                datetime.timezone.utc).astimezone().tzinfo
            offset = int(tzinfo.utcoffset(dt).total_seconds())
            return int(seconds + offset) * 10 ** 6 + dt.microsecond, offset

    def from_sql_type(self, zoned_ts):
        if zoned_ts is not None:
            from dateutil import tz
            ts = zoned_ts[0] - zoned_ts[1] * 10 ** 6
            tzinfo = tz.tzoffset(None, zoned_ts[1])
            return datetime.datetime.fromtimestamp(ts // 10 ** 6, tz=tzinfo).replace(
                microsecond=ts % 10 ** 6)


class Resolution(object):
    """
    Helper class for defining the resolution of an interval.

    :param unit: value defined in the constants of :class:`IntervalUnit`.
    :param precision: the number of digits of years (=year precision) or the number of digits of
                      days (=day precision) or the number of digits of fractional seconds (
                      =fractional precision).
    """

    class IntervalUnit(object):
        SECOND = 0
        MINUTE = 1
        HOUR = 2
        DAY = 3
        MONTH = 4
        YEAR = 5

    def __init__(self, unit, precision=-1):
        self._unit = unit
        self._precision = precision

    @property
    def unit(self):
        return self._unit

    @property
    def precision(self):
        return self._precision

    def __str__(self):
        return '%s(%s)' % (str(self._unit), str(self._precision))


class YearMonthIntervalType(AtomicType):
    """
    Year-month interval types. The type must be parameterized to one of the following
    resolutions: interval of years, interval of years to months, or interval of months.

    An interval of year-month consists of ``+years-months`` with values ranging from ``-9999-11``
    to ``+9999-11``. The value representation is the same for all types of resolutions. For
    example, an interval of months of 50 is always represented in an interval-of-years-to-months
    format (with default year precision): ``+04-02``.

    :param resolution: value defined in the constants of :class:`YearMonthResolution`,
                       representing one of the following resolutions: interval of years,
                       interval of years to months, or interval of months.
    :param precision: int, the number of digits of years, must have a value
                      between 1 and 4 (both inclusive), default (2).
    :param nullable: boolean, whether the field can be null (None) or not.
    """

    class YearMonthResolution(object):
        """
        Supported resolutions of :class:`YearMonthIntervalType`.
        """
        YEAR = 1
        MONTH = 2
        YEAR_TO_MONTH = 3

    DEFAULT_PRECISION = 2

    def __init__(self, resolution, precision=DEFAULT_PRECISION, nullable=True):
        assert resolution == YearMonthIntervalType.YearMonthResolution.YEAR or \
            resolution == YearMonthIntervalType.YearMonthResolution.MONTH or \
            resolution == YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH
        assert resolution != YearMonthIntervalType.YearMonthResolution.MONTH or \
            precision == self.DEFAULT_PRECISION
        assert 1 <= precision <= 4
        self._resolution = resolution
        self._precision = precision
        super(YearMonthIntervalType, self).__init__(nullable)

    @property
    def resolution(self):
        return self._resolution

    @property
    def precision(self):
        return self._precision


class DayTimeIntervalType(AtomicType):
    """
    Day-time interval types. The type must be parameterized to one of the following resolutions
    with up to nanosecond precision: interval of days, interval of days to hours, interval of
    days to minutes, interval of days to seconds, interval of hours, interval of hours to minutes,
    interval of hours to seconds, interval of minutes, interval of minutes to seconds,
    or interval of seconds.

    An interval of day-time consists of ``+days hours:months:seconds.fractional`` with values
    ranging from ``-999999 23:59:59.999999999`` to ``+999999 23:59:59.999999999``. The value
    representation is the same for all types of resolutions. For example, an interval of seconds
    of 70 is always represented in an interval-of-days-to-seconds format (with default precisions):
    ``+00 00:01:10.000000``.

    :param resolution: value defined in the constants of :class:`DayTimeResolution`,
                       representing one of the following resolutions: interval of days, interval
                       of days to hours, interval of days to minutes, interval of days to seconds,
                       interval of hours, interval of hours to minutes, interval of hours to
                       seconds, interval of minutes, interval of minutes to seconds, or interval
                       of seconds.
    :param day_precision: the number of digits of days, must have a value between 1 and 6 (both
                          inclusive) (default 2).
    :param fractional_precision: the number of digits of fractional seconds, must have a value
                                 between 0 and 9 (both inclusive) (default 6).
    """

    class DayTimeResolution(object):
        """
        Supported resolutions of :class:`DayTimeIntervalType`.
        """
        DAY = 1
        DAY_TO_HOUR = 2
        DAY_TO_MINUTE = 3
        DAY_TO_SECOND = 4
        HOUR = 5
        HOUR_TO_MINUTE = 6
        HOUR_TO_SECOND = 7
        MINUTE = 8
        MINUTE_TO_SECOND = 9
        SECOND = 10

    DEFAULT_DAY_PRECISION = 2
    DEFAULT_FRACTIONAL_PRECISION = 6

    def __init__(self, resolution, day_precision=DEFAULT_DAY_PRECISION,
                 fractional_precision=DEFAULT_FRACTIONAL_PRECISION, nullable=True):
        assert resolution == DayTimeIntervalType.DayTimeResolution.DAY or \
            resolution == DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR or \
            resolution == DayTimeIntervalType.DayTimeResolution.DAY_TO_MINUTE or \
            resolution == DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND or \
            resolution == DayTimeIntervalType.DayTimeResolution.HOUR or \
            resolution == DayTimeIntervalType.DayTimeResolution.HOUR_TO_MINUTE or \
            resolution == DayTimeIntervalType.DayTimeResolution.HOUR_TO_SECOND or \
            resolution == DayTimeIntervalType.DayTimeResolution.MINUTE or \
            resolution == DayTimeIntervalType.DayTimeResolution.MINUTE_TO_SECOND or \
            resolution == DayTimeIntervalType.DayTimeResolution.SECOND

        assert not self._needs_default_day_precision(
            resolution) or day_precision == self.DEFAULT_DAY_PRECISION
        assert not self._needs_default_fractional_precision(
            resolution) or fractional_precision == self.DEFAULT_FRACTIONAL_PRECISION
        assert 1 <= day_precision <= 6
        assert 0 <= fractional_precision <= 9
        self._resolution = resolution
        self._day_precision = day_precision
        self._fractional_precision = fractional_precision
        super(DayTimeIntervalType, self).__init__(nullable)

    def need_conversion(self):
        return True

    def to_sql_type(self, timedelta):
        if timedelta is not None:
            return (timedelta.days * 86400 + timedelta.seconds) * 10 ** 6 + timedelta.microseconds

    def from_sql_type(self, ts):
        if ts is not None:
            return datetime.timedelta(microseconds=ts)

    @property
    def resolution(self):
        return self._resolution

    @property
    def day_precision(self):
        return self._day_precision

    @property
    def fractional_precision(self):
        return self._fractional_precision

    @staticmethod
    def _needs_default_day_precision(resolution):
        if resolution == DayTimeIntervalType.DayTimeResolution.HOUR or \
                resolution == DayTimeIntervalType.DayTimeResolution.HOUR_TO_MINUTE or \
                resolution == DayTimeIntervalType.DayTimeResolution.HOUR_TO_SECOND or \
                resolution == DayTimeIntervalType.DayTimeResolution.MINUTE or \
                resolution == DayTimeIntervalType.DayTimeResolution.MINUTE_TO_SECOND or \
                resolution == DayTimeIntervalType.DayTimeResolution.SECOND:
            return True
        else:
            return False

    @staticmethod
    def _needs_default_fractional_precision(resolution):
        if resolution == DayTimeIntervalType.DayTimeResolution.DAY or \
                resolution == DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR or \
                resolution == DayTimeIntervalType.DayTimeResolution.DAY_TO_MINUTE or \
                resolution == DayTimeIntervalType.DayTimeResolution.HOUR or \
                resolution == DayTimeIntervalType.DayTimeResolution.HOUR_TO_MINUTE or \
                resolution == DayTimeIntervalType.DayTimeResolution.MINUTE:
            return True
        else:
            return False


_resolution_mappings = {
    (Resolution.IntervalUnit.YEAR, None):
        lambda p1, p2: YearMonthIntervalType(
            YearMonthIntervalType.YearMonthResolution.YEAR, p1),
    (Resolution.IntervalUnit.MONTH, None):
        lambda p1, p2: YearMonthIntervalType(
            YearMonthIntervalType.YearMonthResolution.MONTH),
    (Resolution.IntervalUnit.YEAR, Resolution.IntervalUnit.MONTH):
        lambda p1, p2: YearMonthIntervalType(
            YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH),
    (Resolution.IntervalUnit.DAY, None):
        lambda p1, p2: DayTimeIntervalType(
            DayTimeIntervalType.DayTimeResolution.DAY,
            p1,
            DayTimeIntervalType.DEFAULT_FRACTIONAL_PRECISION),
    (Resolution.IntervalUnit.DAY, Resolution.IntervalUnit.HOUR):
        lambda p1, p2: DayTimeIntervalType(
            DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR,
            p1,
            DayTimeIntervalType.DEFAULT_FRACTIONAL_PRECISION),
    (Resolution.IntervalUnit.DAY, Resolution.IntervalUnit.MINUTE):
        lambda p1, p2: DayTimeIntervalType(
            DayTimeIntervalType.DayTimeResolution.DAY_TO_MINUTE,
            p1,
            DayTimeIntervalType.DEFAULT_FRACTIONAL_PRECISION),
    (Resolution.IntervalUnit.DAY, Resolution.IntervalUnit.SECOND):
        lambda p1, p2: DayTimeIntervalType(
            DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND, p1, p2),
    (Resolution.IntervalUnit.HOUR, None):
        lambda p1, p2: DayTimeIntervalType(
            DayTimeIntervalType.DayTimeResolution.HOUR),
    (Resolution.IntervalUnit.HOUR, Resolution.IntervalUnit.MINUTE):
        lambda p1, p2: DayTimeIntervalType(
            DayTimeIntervalType.DayTimeResolution.HOUR_TO_MINUTE),
    (Resolution.IntervalUnit.HOUR, Resolution.IntervalUnit.SECOND):
        lambda p1, p2: DayTimeIntervalType(
            DayTimeIntervalType.DayTimeResolution.HOUR_TO_SECOND,
            DayTimeIntervalType.DEFAULT_DAY_PRECISION,
            p2),
    (Resolution.IntervalUnit.MINUTE, None):
        lambda p1, p2: DayTimeIntervalType(
            DayTimeIntervalType.DayTimeResolution.MINUTE),
    (Resolution.IntervalUnit.MINUTE, Resolution.IntervalUnit.SECOND):
        lambda p1, p2: DayTimeIntervalType(
            DayTimeIntervalType.DayTimeResolution.MINUTE_TO_SECOND,
            DayTimeIntervalType.DEFAULT_DAY_PRECISION,
            p2),
    (Resolution.IntervalUnit.SECOND, None):
        lambda p1, p2: DayTimeIntervalType(
            DayTimeIntervalType.DayTimeResolution.SECOND,
            DayTimeIntervalType.DEFAULT_DAY_PRECISION,
            p1)
}


def _from_resolution(upper_resolution, lower_resolution=None):
    """
    Creates an interval type (YearMonthIntervalType or DayTimeIntervalType) from the
    upper_resolution and lower_resolution.
    """
    lower_unit = None if lower_resolution is None else lower_resolution.unit
    lower_precision = -1 if lower_resolution is None else lower_resolution.precision
    interval_type_provider = _resolution_mappings[(upper_resolution.unit, lower_unit)]
    if interval_type_provider is None:
        raise ValueError(
            "Unsupported interval definition '%s TO %s'. Please check the documentation for "
            "supported combinations for year-month and day-time intervals."
            % (upper_resolution, lower_resolution))

    return interval_type_provider(upper_resolution.precision, lower_precision)


def _from_java_interval_type(j_interval_type):
    """
    Creates an interval type from the specified Java interval type.

    :param j_interval_type: the Java interval type.
    :return: :class:`YearMonthIntervalType` or :class:`DayTimeIntervalType`.
    """
    gateway = get_gateway()
    if _is_instance_of(j_interval_type, gateway.jvm.YearMonthIntervalType):
        resolution = j_interval_type.getResolution()
        precision = j_interval_type.getYearPrecision()

        def _from_java_year_month_resolution(j_resolution):
            if j_resolution == gateway.jvm.YearMonthIntervalType.YearMonthResolution.YEAR:
                return YearMonthIntervalType.YearMonthResolution.YEAR
            elif j_resolution == gateway.jvm.YearMonthIntervalType.YearMonthResolution.MONTH:
                return YearMonthIntervalType.YearMonthResolution.MONTH
            else:
                return YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH

        return YearMonthIntervalType(_from_java_year_month_resolution(resolution), precision)

    else:
        resolution = j_interval_type.getResolution()
        day_precision = j_interval_type.getDayPrecision()
        fractional_precision = j_interval_type.getFractionalPrecision()

        def _from_java_day_time_resolution(j_resolution):
            if j_resolution == gateway.jvm.DayTimeIntervalType.DayTimeResolution.DAY:
                return DayTimeIntervalType.DayTimeResolution.DAY
            elif j_resolution == gateway.jvm.DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR:
                return DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR
            elif j_resolution == gateway.jvm.DayTimeIntervalType.DayTimeResolution.DAY_TO_MINUTE:
                return DayTimeIntervalType.DayTimeResolution.DAY_TO_MINUTE
            elif j_resolution == gateway.jvm.DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND:
                return DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND
            elif j_resolution == gateway.jvm.DayTimeIntervalType.DayTimeResolution.HOUR:
                return DayTimeIntervalType.DayTimeResolution.HOUR
            elif j_resolution == gateway.jvm.DayTimeIntervalType.DayTimeResolution.HOUR_TO_MINUTE:
                return DayTimeIntervalType.DayTimeResolution.HOUR_TO_MINUTE
            elif j_resolution == gateway.jvm.DayTimeIntervalType.DayTimeResolution.HOUR_TO_SECOND:
                return DayTimeIntervalType.DayTimeResolution.HOUR_TO_SECOND
            elif j_resolution == gateway.jvm.DayTimeIntervalType.DayTimeResolution.MINUTE:
                return DayTimeIntervalType.DayTimeResolution.MINUTE
            elif j_resolution == gateway.jvm.DayTimeIntervalType.DayTimeResolution.MINUTE_TO_SECOND:
                return DayTimeIntervalType.DayTimeResolution.MINUTE_TO_SECOND
            else:
                return DayTimeIntervalType.DayTimeResolution.SECOND

        return DayTimeIntervalType(
            _from_java_day_time_resolution(resolution), day_precision, fractional_precision)


_boxed_to_primitive_array_map = \
    {'java.lang.Integer': '[I',
     'java.lang.Long': '[J',
     'java.lang.Byte': '[B',
     'java.lang.Short': '[S',
     'java.lang.Character': '[C',
     'java.lang.Boolean': '[Z',
     'java.lang.Float': '[F',
     'java.lang.Double': '[D'}


class ArrayType(DataType):
    """
    Array data type.

    :param element_type: :class:`DataType` of each element in the array.
    :param nullable: boolean, whether the field can be null (None) or not.
    """

    def __init__(self, element_type, nullable=True):
        """
        >>> ArrayType(VarCharType(100)) == ArrayType(VarCharType(100))
        True
        >>> ArrayType(VarCharType(100)) == ArrayType(BigIntType())
        False
        """
        assert isinstance(element_type, DataType), \
            "element_type %s should be an instance of %s" % (element_type, DataType)
        super(ArrayType, self).__init__(nullable)
        self.element_type = element_type

    def __repr__(self):
        return "ArrayType(%s, %s)" % (repr(self.element_type), str(self._nullable).lower())

    def need_conversion(self):
        return self.element_type.need_conversion()

    def to_sql_type(self, obj):
        if not self.need_conversion():
            return obj
        return obj and [self.element_type.to_sql_type(v) for v in obj]

    def from_sql_type(self, obj):
        if not self.need_conversion():
            return obj
        return obj and [self.element_type.to_sql_type(v) for v in obj]


class MapType(DataType):
    """
    Map data type.

    :param key_type: :class:`DataType` of the keys in the map.
    :param value_type: :class:`DataType` of the values in the map.
    :param nullable: boolean, whether the field can be null (None) or not.

    Keys in a map data type are not allowed to be null (None).
    """

    def __init__(self, key_type, value_type, nullable=True):
        """
        >>> (MapType(VarCharType(100, nullable=False), IntType())
        ...        == MapType(VarCharType(100, nullable=False), IntType()))
        True
        >>> (MapType(VarCharType(100, nullable=False), IntType())
        ...        == MapType(VarCharType(100, nullable=False), FloatType()))
        False
        """
        assert isinstance(key_type, DataType), \
            "key_type %s should be an instance of %s" % (key_type, DataType)
        assert isinstance(value_type, DataType), \
            "value_type %s should be an instance of %s" % (value_type, DataType)
        super(MapType, self).__init__(nullable)
        self.key_type = key_type
        self.value_type = value_type

    def __repr__(self):
        return "MapType(%s, %s, %s)" % (
            repr(self.key_type), repr(self.value_type), str(self._nullable).lower())

    def need_conversion(self):
        return self.key_type.need_conversion() or self.value_type.need_conversion()

    def to_sql_type(self, obj):
        if not self.need_conversion():
            return obj
        return obj and dict((self.key_type.to_sql_type(k), self.value_type.to_sql_type(v))
                            for k, v in obj.items())

    def from_sql_type(self, obj):
        if not self.need_conversion():
            return obj
        return obj and dict((self.key_type.from_sql_type(k), self.value_type.from_sql_type(v))
                            for k, v in obj.items())


class MultisetType(DataType):
    """
    MultisetType data type.

    :param element_type: :class:`DataType` of each element in the multiset.
    :param nullable: boolean, whether the field can be null (None) or not.
    """

    def __init__(self, element_type, nullable=True):
        """
        >>> MultisetType(VarCharType(100)) == MultisetType(VarCharType(100))
        True
        >>> MultisetType(VarCharType(100)) == MultisetType(BigIntType())
        False
        """
        assert isinstance(element_type, DataType), \
            "element_type %s should be an instance of %s" % (element_type, DataType)
        super(MultisetType, self).__init__(nullable)
        self.element_type = element_type

    def __repr__(self):
        return "MultisetType(%s, %s)" % (repr(self.element_type), str(self._nullable).lower())

    def need_conversion(self):
        return self.element_type.need_conversion()

    def to_sql_type(self, obj):
        if not self.need_conversion():
            return obj
        return obj and [self.element_type.to_sql_type(v) for v in obj]

    def from_sql_type(self, obj):
        if not self.need_conversion():
            return obj
        return obj and [self.element_type.to_sql_type(v) for v in obj]


class RowField(object):
    """
    A field in :class:`RowType`.

    :param name: string, name of the field.
    :param data_type: :class:`DataType` of the field.
    :param description: string, description of the field.
    """

    def __init__(self, name, data_type, description=None):
        """
        >>> (RowField("f1", VarCharType(100)) == RowField("f1", VarCharType(100)))
        True
        >>> (RowField("f1", VarCharType(100)) == RowField("f2", VarCharType(100)))
        False
        """
        assert isinstance(data_type, DataType), \
            "data_type %s should be an instance of %s" % (data_type, DataType)
        assert isinstance(name, str), "field name %s should be string" % name
        if not isinstance(name, str):
            name = name.encode('utf-8')
        if description is not None:
            assert isinstance(description, str), \
                "description %s should be string" % description
            if not isinstance(description, str):
                description = description.encode('utf-8')
        self.name = name
        self.data_type = data_type
        self.description = '...' if description is None else description

    def __repr__(self):
        return "RowField(%s, %s, %s)" % (self.name, repr(self.data_type), self.description)

    def __str__(self, *args, **kwargs):
        return "RowField(%s, %s)" % (self.name, self.data_type)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def need_conversion(self):
        return self.data_type.need_conversion()

    def to_sql_type(self, obj):
        return self.data_type.to_sql_type(obj)

    def from_sql_type(self, obj):
        return self.data_type.from_sql_type(obj)


class RowType(DataType):
    """
    Row type, consisting of a list of :class:`RowField`.

    This is the data type representing a :class:`Row`.

    Iterating a :class:`RowType` will iterate its :class:`RowField`\\s.
    A contained :class:`RowField` can be accessed by name or position.

    >>> row1 = RowType([RowField("f1", VarCharType(100))])
    >>> row1["f1"]
    RowField(f1, VarCharType(100))
    >>> row1[0]
    RowField(f1, VarCharType(100))
    """

    def __init__(self, fields=None, nullable=True):
        """
        >>> row1 = RowType([RowField("f1", VarCharType(100))])
        >>> row2 = RowType([RowField("f1", VarCharType(100))])
        >>> row1 == row2
        True
        >>> row1 = RowType([RowField("f1", VarCharType(100))])
        >>> row2 = RowType([RowField("f1", VarCharType(100)), RowField("f2", IntType())])
        >>> row1 == row2
        False
        """
        super(RowType, self).__init__(nullable)
        if not fields:
            self.fields = []
            self.names = []
        else:
            self.fields = fields
            self.names = [f.name for f in fields]
            assert all(isinstance(f, RowField) for f in fields), \
                "fields should be a list of RowField"
        # Precalculated list of fields that need conversion with
        # from_sql_type/to_sql_type functions
        self._need_conversion = [f.need_conversion() for f in self]
        self._need_serialize_any_field = any(self._need_conversion)

    def add(self, field, data_type=None):
        """
        Constructs a RowType by adding new elements to it to define the schema. The method accepts
        either:

            a) A single parameter which is a RowField object.
            b) 2 parameters as (name, data_type). The data_type parameter may be either a String
               or a DataType object.

        >>> row1 = RowType().add("f1", VarCharType(100)).add("f2", VarCharType(100))
        >>> row2 = RowType([RowField("f1", VarCharType(100)), RowField("f2", VarCharType(100))])
        >>> row1 == row2
        True
        >>> row1 = RowType().add(RowField("f1", VarCharType(100)))
        >>> row2 = RowType([RowField("f1", VarCharType(100))])
        >>> row1 == row2
        True
        >>> row2 = RowType([RowField("f1", VarCharType(100))])
        >>> row1 == row2
        True

        :param field: Either the name of the field or a RowField object
        :param data_type: If present, the DataType of the RowField to create
        :return: a new updated RowType
        """
        if isinstance(field, RowField):
            self.fields.append(field)
            self.names.append(field.name)
        else:
            if isinstance(field, str) and data_type is None:
                raise ValueError("Must specify DataType if passing name of row_field to create.")

            self.fields.append(RowField(field, data_type))
            self.names.append(field)
        # Precalculated list of fields that need conversion with
        # from_sql_type/to_sql_type functions
        self._need_conversion = [f.need_conversion() for f in self]
        self._need_serialize_any_field = any(self._need_conversion)
        return self

    def __iter__(self):
        """
        Iterate the fields.
        """
        return iter(self.fields)

    def __len__(self):
        """
        Returns the number of fields.
        """
        return len(self.fields)

    def __getitem__(self, key):
        """
        Accesses fields by name or slice.
        """
        if isinstance(key, str):
            for field in self:
                if field.name == key:
                    return field
            raise KeyError('No RowField named {0}'.format(key))
        elif isinstance(key, int):
            try:
                return self.fields[key]
            except IndexError:
                raise IndexError('RowType index out of range')
        elif isinstance(key, slice):
            return RowType(self.fields[key])
        else:
            raise TypeError('RowType keys should be strings, integers or slices')

    def __repr__(self):
        return "RowType(%s)" % ",".join(repr(field) for field in self)

    def field_names(self):
        """
        Returns all field names in a list.

        >>> row = RowType([RowField("f1", VarCharType(100))])
        >>> row.field_names()
        ['f1']
        """
        return list(self.names)

    def need_conversion(self):
        # We need convert Row()/namedtuple into tuple()
        return True

    def to_sql_type(self, obj):
        if obj is None:
            return

        if self._need_serialize_any_field:
            # Only calling to_sql_type function for fields that need conversion
            if isinstance(obj, dict):
                return tuple(f.to_sql_type(obj.get(n)) if c else obj.get(n)
                             for n, f, c in zip(self.names, self.fields, self._need_conversion))
            elif isinstance(obj, (tuple, list)):
                return tuple(f.to_sql_type(v) if c else v
                             for f, v, c in zip(self.fields, obj, self._need_conversion))
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return tuple(f.to_sql_type(d.get(n)) if c else d.get(n)
                             for n, f, c in zip(self.names, self.fields, self._need_conversion))
            else:
                raise ValueError("Unexpected tuple %r with RowType" % obj)
        else:
            if isinstance(obj, dict):
                return tuple(obj.get(n) for n in self.names)
            elif isinstance(obj, Row) and getattr(obj, "_from_dict", False):
                return tuple(obj[n] for n in self.names)
            elif isinstance(obj, (list, tuple)):
                return tuple(obj)
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return tuple(d.get(n) for n in self.names)
            else:
                raise ValueError("Unexpected tuple %r with RowType" % obj)

    def from_sql_type(self, obj):
        if obj is None:
            return
        if isinstance(obj, Row):
            # it's already converted by pickler
            return obj
        if self._need_serialize_any_field:
            # Only calling from_sql_type function for fields that need conversion
            values = [f.from_sql_type(v) if c else v
                      for f, v, c in zip(self.fields, obj, self._need_conversion)]
        else:
            values = obj
        return _create_row(self.names, values)


class UserDefinedType(DataType):
    """
    User-defined type (UDT).

    .. note:: WARN: Flink Internal Use Only
    """

    def __eq__(self, other):
        return type(self) == type(other)

    @classmethod
    def type_name(cls):
        return cls.__name__.lower()

    @classmethod
    def sql_type(cls):
        """
        Underlying SQL storage type for this UDT.
        """
        raise NotImplementedError("UDT must implement sql_type().")

    @classmethod
    def module(cls):
        """
        The Python module of the UDT.
        """
        raise NotImplementedError("UDT must implement module().")

    @classmethod
    def java_udt(cls):
        """
        The class name of the paired Java UDT (could be '', if there
        is no corresponding one).
        """
        return ''

    def need_conversion(self):
        return True

    @classmethod
    def _cached_sql_type(cls):
        """
        Caches the sql_type() into class, because it's heavy used in `to_sql_type`.
        """
        if not hasattr(cls, "__cached_sql_type"):
            cls.__cached_sql_type = cls.sql_type()
        return cls.__cached_sql_type

    def to_sql_type(self, obj):
        if obj is not None:
            return self._cached_sql_type().to_sql_type(self.serialize(obj))

    def from_sql_type(self, obj):
        v = self._cached_sql_type().from_sql_type(obj)
        if v is not None:
            return self.deserialize(v)

    def serialize(self, obj):
        """
        Converts the a user-type object into a SQL datum.
        """
        raise NotImplementedError("UDT must implement serialize().")

    def deserialize(self, datum):
        """
        Converts a SQL datum into a user-type object.
        """
        raise NotImplementedError("UDT must implement deserialize().")


# Mapping Python types to Flink SQL types
_type_mappings = {
    bool: BooleanType(),
    int: BigIntType(),
    float: DoubleType(),
    str: VarCharType(0x7fffffff),
    bytearray: VarBinaryType(0x7fffffff),
    decimal.Decimal: DecimalType(38, 18),
    datetime.date: DateType(),
    datetime.datetime: LocalZonedTimestampType(),
    datetime.time: TimeType(),
}

# Mapping Python array types to Flink SQL types
# We should be careful here. The size of these types in python depends on C
# implementation. We need to make sure that this conversion does not lose any
# precision. Also, JVM only support signed types, when converting unsigned types,
# keep in mind that it requires 1 more bit when stored as singed types.
#
# Reference for C integer size, see:
# ISO/IEC 9899:201x specification, chapter 5.2.4.2.1 Sizes of integer types <limits.h>.
# Reference for python array typecode, see:
# https://docs.python.org/2/library/array.html
# https://docs.python.org/3.6/library/array.html
# Reference for JVM's supported integral types:
# http://docs.oracle.com/javase/specs/jvms/se8/html/jvms-2.html#jvms-2.3.1

_array_signed_int_typecode_ctype_mappings = {
    'b': ctypes.c_byte,
    'h': ctypes.c_short,
    'i': ctypes.c_int,
    'l': ctypes.c_long,
}

_array_unsigned_int_typecode_ctype_mappings = {
    'B': ctypes.c_ubyte,
    'H': ctypes.c_ushort,
    'I': ctypes.c_uint,
    'L': ctypes.c_ulong
}


def _int_size_to_type(size):
    """
    Returns the data type from the size of integers.
    """
    if size <= 8:
        return TinyIntType()
    if size <= 16:
        return SmallIntType()
    if size <= 32:
        return IntType()
    if size <= 64:
        return BigIntType()


# The list of all supported array typecodes is stored here
_array_type_mappings = {
    # Warning: Actual properties for float and double in C is not specified in C.
    # On almost every system supported by both python and JVM, they are IEEE 754
    # single-precision binary floating-point format and IEEE 754 double-precision
    # binary floating-point format. And we do assume the same thing here for now.
    'f': FloatType(),
    'd': DoubleType()
}

# compute array typecode mappings for signed integer types
for _typecode in _array_signed_int_typecode_ctype_mappings.keys():
    size = ctypes.sizeof(_array_signed_int_typecode_ctype_mappings[_typecode]) * 8
    dt = _int_size_to_type(size)
    if dt is not None:
        _array_type_mappings[_typecode] = dt

# compute array typecode mappings for unsigned integer types
for _typecode in _array_unsigned_int_typecode_ctype_mappings.keys():
    # JVM does not have unsigned types, so use signed types that is at least 1
    # bit larger to store
    size = ctypes.sizeof(_array_unsigned_int_typecode_ctype_mappings[_typecode]) * 8 + 1
    dt = _int_size_to_type(size)
    if dt is not None:
        _array_type_mappings[_typecode] = dt

# Type code 'u' in Python's array is deprecated since version 3.3, and will be
# removed in version 4.0. See: https://docs.python.org/3/library/array.html
if sys.version_info[0] < 4:
    # it can be 16 bits or 32 bits depending on the platform
    _array_type_mappings['u'] = CharType(ctypes.sizeof(ctypes.c_wchar))


def _infer_type(obj):
    """
    Infers the data type from obj.
    """
    if obj is None:
        return NullType()

    if hasattr(obj, '__UDT__'):
        return obj.__UDT__

    data_type = _type_mappings.get(type(obj))
    if data_type is not None:
        return data_type

    if isinstance(obj, dict):
        for key, value in obj.items():
            if key is not None and value is not None:
                return MapType(_infer_type(key).not_null(), _infer_type(value))
        else:
            return MapType(NullType(), NullType())
    elif isinstance(obj, list):
        for v in obj:
            if v is not None:
                return ArrayType(_infer_type(obj[0]))
        else:
            return ArrayType(NullType())
    elif isinstance(obj, array):
        if obj.typecode in _array_type_mappings:
            return ArrayType(_array_type_mappings[obj.typecode].not_null())
        else:
            raise TypeError("not supported type: array(%s)" % obj.typecode)
    else:
        try:
            return _infer_schema(obj)
        except TypeError:
            raise TypeError("not supported type: %s" % type(obj))


def _infer_schema(row, names=None):
    """
    Infers the schema from dict/row/namedtuple/object.
    """
    if isinstance(row, dict):  # dict
        items = sorted(row.items())

    elif isinstance(row, (tuple, list)):
        if hasattr(row, "_fields"):  # namedtuple and Row
            items = zip(row._fields, tuple(row))
        else:
            if names is None:
                names = ['_%d' % i for i in range(1, len(row) + 1)]
            elif len(names) < len(row):
                names.extend('_%d' % i for i in range(len(names) + 1, len(row) + 1))
            items = zip(names, row)

    elif hasattr(row, "__dict__"):  # object
        items = sorted(row.__dict__.items())

    else:
        raise TypeError("Can not infer schema for type: %s" % type(row))

    fields = [RowField(k, _infer_type(v)) for k, v in items]
    return RowType(fields)


def _has_nulltype(dt):
    """
    Returns whether there is NullType in `dt` or not.
    """
    if isinstance(dt, RowType):
        return any(_has_nulltype(f.data_type) for f in dt.fields)
    elif isinstance(dt, ArrayType) or isinstance(dt, MultisetType):
        return _has_nulltype(dt.element_type)
    elif isinstance(dt, MapType):
        return _has_nulltype(dt.key_type) or _has_nulltype(dt.value_type)
    else:
        return isinstance(dt, NullType)


def _merge_type(a, b, name=None):
    if name is None:
        def new_msg(msg):
            return msg

        def new_name(n):
            return "field %s" % n
    else:
        def new_msg(msg):
            return "%s: %s" % (name, msg)

        def new_name(n):
            return "field %s in %s" % (n, name)

    if isinstance(a, NullType):
        return b
    elif isinstance(b, NullType):
        return a
    elif type(a) is not type(b):
        # TODO: type cast (such as int -> long)
        raise TypeError(new_msg("Can not merge type %s and %s" % (type(a), type(b))))

    # same type
    if isinstance(a, RowType):
        nfs = dict((f.name, f.data_type) for f in b.fields)
        fields = [RowField(f.name, _merge_type(f.data_type, nfs.get(f.name, None),
                                               name=new_name(f.name)))
                  for f in a.fields]
        names = set([f.name for f in fields])
        for n in nfs:
            if n not in names:
                fields.append(RowField(n, nfs[n]))
        return RowType(fields)

    elif isinstance(a, ArrayType):
        return ArrayType(_merge_type(a.element_type, b.element_type,
                                     name='element in array %s' % name))

    elif isinstance(a, MultisetType):
        return MultisetType(_merge_type(a.element_type, b.element_type,
                                        name='element in multiset %s' % name))

    elif isinstance(a, MapType):
        return MapType(_merge_type(a.key_type, b.key_type, name='key of map %s' % name),
                       _merge_type(a.value_type, b.value_type, name='value of map %s' % name))
    else:
        return a


def _infer_schema_from_data(elements, names=None):
    """
    Infers schema from list of Row or tuple.

    :param elements: list of Row or tuple
    :param names: list of column names
    :return: :class:`RowType`
    """
    if not elements:
        raise ValueError("can not infer schema from empty data set")
    schema = reduce(_merge_type, (_infer_schema(row, names) for row in elements))
    if _has_nulltype(schema):
        raise ValueError("Some column types cannot be determined after inferring")
    return schema


def _need_converter(data_type):
    if isinstance(data_type, RowType):
        return True
    elif isinstance(data_type, ArrayType) or isinstance(data_type, MultisetType):
        return _need_converter(data_type.element_type)
    elif isinstance(data_type, MapType):
        return _need_converter(data_type.key_type) or _need_converter(data_type.value_type)
    elif isinstance(data_type, NullType):
        return True
    else:
        return False


def _create_converter(data_type):
    """
    Creates a converter to drop the names of fields in obj.
    """
    if not _need_converter(data_type):
        return lambda x: x

    if isinstance(data_type, ArrayType) or isinstance(data_type, MultisetType):
        conv = _create_converter(data_type.element_type)
        return lambda row: [conv(v) for v in row]

    elif isinstance(data_type, MapType):
        kconv = _create_converter(data_type.key_type)
        vconv = _create_converter(data_type.value_type)
        return lambda row: dict((kconv(k), vconv(v)) for k, v in row.items())

    elif isinstance(data_type, NullType):
        return lambda x: None

    elif not isinstance(data_type, RowType):
        return lambda x: x

    # dataType must be RowType
    names = [f.name for f in data_type.fields]
    converters = [_create_converter(f.data_type) for f in data_type.fields]
    convert_fields = any(_need_converter(f.data_type) for f in data_type.fields)

    def convert_row(obj):
        if obj is None:
            return

        if isinstance(obj, (tuple, list)):
            if convert_fields:
                return tuple(conv(v) for v, conv in zip(obj, converters))
            else:
                return tuple(obj)

        if isinstance(obj, dict):
            d = obj
        elif hasattr(obj, "__dict__"):  # object
            d = obj.__dict__
        else:
            raise TypeError("Unexpected obj type: %s" % type(obj))

        if convert_fields:
            return tuple([conv(d.get(name)) for name, conv in zip(names, converters)])
        else:
            return tuple([d.get(name) for name in names])

    return convert_row


_python_java_types_mapping = None
_python_java_types_mapping_lock = RLock()
_primitive_array_element_types = {BooleanType, TinyIntType, SmallIntType, IntType, BigIntType,
                                  FloatType, DoubleType}


def _to_java_type(data_type):
    """
    Converts Python type to Java type.
    """

    global _python_java_types_mapping
    global _python_java_types_mapping_lock

    gateway = get_gateway()
    Types = gateway.jvm.org.apache.flink.table.api.Types

    if _python_java_types_mapping is None:
        with _python_java_types_mapping_lock:
            _python_java_types_mapping = {
                BooleanType: Types.BOOLEAN(),
                TinyIntType: Types.BYTE(),
                SmallIntType: Types.SHORT(),
                IntType: Types.INT(),
                BigIntType: Types.LONG(),
                FloatType: Types.FLOAT(),
                DoubleType: Types.DOUBLE(),
                DateType: Types.SQL_DATE(),
            }

    # basic types
    if type(data_type) in _python_java_types_mapping:
        return _python_java_types_mapping[type(data_type)]

    # DecimalType
    elif isinstance(data_type, DecimalType):
        if data_type.precision == 38 and data_type.scale == 18:
            return Types.DECIMAL()
        else:
            raise TypeError("The precision must be 38 and the scale must be 18 for DecimalType, "
                            "got %s" % repr(data_type))

    # TimeType
    elif isinstance(data_type, TimeType):
        if data_type.precision == 0:
            return Types.SQL_TIME()
        else:
            raise TypeError("The precision must be 0 for TimeType, got %s" % repr(data_type))

    # TimestampType
    elif isinstance(data_type, TimestampType):
        if data_type.precision == 3:
            return Types.SQL_TIMESTAMP()
        else:
            raise TypeError("The precision must be 3 for TimestampType, got %s" % repr(data_type))

    # LocalZonedTimestampType
    elif isinstance(data_type, LocalZonedTimestampType):
        if data_type.precision == 3:
            return gateway.jvm.org.apache.flink.api.common.typeinfo.Types.INSTANT
        else:
            raise TypeError("The precision must be 3 for LocalZonedTimestampType, got %s"
                            % repr(data_type))

    # VarCharType
    elif isinstance(data_type, VarCharType):
        if data_type.length == 0x7fffffff:
            return Types.STRING()
        else:
            raise TypeError("The length limit must be 0x7fffffff(2147483647) for VarCharType, "
                            "got %s" % repr(data_type))

    # VarBinaryType
    elif isinstance(data_type, VarBinaryType):
        if data_type.length == 0x7fffffff:
            return Types.PRIMITIVE_ARRAY(Types.BYTE())
        else:
            raise TypeError("The length limit must be 0x7fffffff(2147483647) for VarBinaryType, "
                            "got %s" % repr(data_type))

    # YearMonthIntervalType
    elif isinstance(data_type, YearMonthIntervalType):
        if data_type.resolution == YearMonthIntervalType.YearMonthResolution.MONTH and \
                data_type.precision == 2:
            return Types.INTERVAL_MONTHS()
        else:
            raise TypeError("The resolution must be YearMonthResolution.MONTH and the precision "
                            "must be 2 for YearMonthIntervalType, got %s" % repr(data_type))

    # DayTimeIntervalType
    elif isinstance(data_type, DayTimeIntervalType):
        if data_type.resolution == DayTimeIntervalType.DayTimeResolution.SECOND and \
                data_type.day_precision == 2 and data_type.fractional_precision == 3:
            return Types.INTERVAL_MILLIS()
        else:
            raise TypeError("The resolution must be DayTimeResolution.SECOND, the day_precision "
                            "must be 2 and the fractional_precision must be 3 for "
                            "DayTimeIntervalType, got %s" % repr(data_type))

    # ArrayType
    elif isinstance(data_type, ArrayType):
        return Types.OBJECT_ARRAY(_to_java_type(data_type.element_type))

    # MapType
    elif isinstance(data_type, MapType):
        return Types.MAP(_to_java_type(data_type.key_type), _to_java_type(data_type.value_type))

    # MultisetType
    elif isinstance(data_type, MultisetType):
        return Types.MULTISET(_to_java_type(data_type.element_type))

    # RowType
    elif isinstance(data_type, RowType):
        return Types.ROW(
            to_jarray(gateway.jvm.String, data_type.field_names()),
            to_jarray(gateway.jvm.TypeInformation,
                      [_to_java_type(f.data_type) for f in data_type.fields]))

    # UserDefinedType
    elif isinstance(data_type, UserDefinedType):
        if data_type.java_udt():
            return gateway.jvm.org.apache.flink.util.InstantiationUtil.instantiate(
                gateway.jvm.Class.forName(data_type.java_udt()))
        else:
            return _to_java_type(data_type.sql_type())

    else:
        raise TypeError("Not supported type: %s" % repr(data_type))


def _is_instance_of(java_data_type, java_class):
    gateway = get_gateway()
    if isinstance(java_class, str):
        param = java_class
    elif isinstance(java_class, JavaClass):
        param = get_java_class(java_class)
    elif isinstance(java_class, JavaObject):
        if not _is_instance_of(java_class, gateway.jvm.Class):
            param = java_class.getClass()
        else:
            param = java_class
    else:
        raise TypeError(
            "java_class must be a string, a JavaClass, or a JavaObject")

    return gateway.jvm.org.apache.flink.api.python.shaded.py4j.reflection.TypeUtil.isInstanceOf(
        param, java_data_type)


def _from_java_type(j_data_type):
    gateway = get_gateway()

    if _is_instance_of(j_data_type, gateway.jvm.TypeInformation):
        # input is TypeInformation
        LegacyTypeInfoDataTypeConverter = \
            gateway.jvm.org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter
        java_data_type = LegacyTypeInfoDataTypeConverter.toDataType(j_data_type)
    else:
        # input is DataType
        java_data_type = j_data_type

    # Atomic Type with parameters.
    if _is_instance_of(java_data_type, gateway.jvm.AtomicDataType):
        logical_type = java_data_type.getLogicalType()
        if _is_instance_of(logical_type, gateway.jvm.CharType):
            data_type = DataTypes.CHAR(logical_type.getLength(), logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.VarCharType):
            data_type = DataTypes.VARCHAR(logical_type.getLength(), logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.BinaryType):
            data_type = DataTypes.BINARY(logical_type.getLength(), logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.VarBinaryType):
            data_type = DataTypes.VARBINARY(logical_type.getLength(), logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.DecimalType):
            data_type = DataTypes.DECIMAL(logical_type.getPrecision(),
                                          logical_type.getScale(),
                                          logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.DateType):
            data_type = DataTypes.DATE(logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.TimeType):
            data_type = DataTypes.TIME(logical_type.getPrecision(), logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.TimestampType):
            data_type = DataTypes.TIMESTAMP(precision=3, nullable=logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.BooleanType):
            data_type = DataTypes.BOOLEAN(logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.TinyIntType):
            data_type = DataTypes.TINYINT(logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.SmallIntType):
            data_type = DataTypes.SMALLINT(logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.IntType):
            data_type = DataTypes.INT(logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.BigIntType):
            data_type = DataTypes.BIGINT(logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.FloatType):
            data_type = DataTypes.FLOAT(logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.DoubleType):
            data_type = DataTypes.DOUBLE(logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.ZonedTimestampType):
            raise \
                TypeError("Unsupported type: %s, ZonedTimestampType is not supported yet."
                          % j_data_type)
        elif _is_instance_of(logical_type, gateway.jvm.LocalZonedTimestampType):
            data_type = DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(nullable=logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.DayTimeIntervalType) or \
                _is_instance_of(logical_type, gateway.jvm.YearMonthIntervalType):
            data_type = _from_java_interval_type(logical_type)
        elif _is_instance_of(logical_type, gateway.jvm.LegacyTypeInformationType):
            type_info = logical_type.getTypeInformation()
            BasicArrayTypeInfo = gateway.jvm.org.apache.flink.api.common.typeinfo.\
                BasicArrayTypeInfo
            BasicTypeInfo = gateway.jvm.org.apache.flink.api.common.typeinfo.BasicTypeInfo
            if type_info == BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO:
                data_type = DataTypes.ARRAY(DataTypes.STRING())
            elif type_info == BasicTypeInfo.BIG_DEC_TYPE_INFO:
                data_type = DataTypes.DECIMAL(10, 0)
            elif type_info.getClass() == \
                get_java_class(gateway.jvm.org.apache.flink.table.runtime.typeutils
                               .BigDecimalTypeInfo):
                data_type = DataTypes.DECIMAL(type_info.precision(), type_info.scale())
            else:
                raise TypeError("Unsupported type: %s, it is recognized as a legacy type."
                                % type_info)
        else:
            raise TypeError("Unsupported type: %s, it is not supported yet in current python type"
                            " system" % j_data_type)

        return data_type

    # Array Type, MultiSet Type.
    elif _is_instance_of(java_data_type, gateway.jvm.CollectionDataType):
        logical_type = java_data_type.getLogicalType()
        element_type = java_data_type.getElementDataType()
        if _is_instance_of(logical_type, gateway.jvm.ArrayType):
            data_type = DataTypes.ARRAY(_from_java_type(element_type), logical_type.isNullable())
        elif _is_instance_of(logical_type, gateway.jvm.MultisetType):
            data_type = DataTypes.MULTISET(_from_java_type(element_type),
                                           logical_type.isNullable())
        else:
            raise TypeError("Unsupported collection data type: %s" % j_data_type)

        return data_type

    # Map Type.
    elif _is_instance_of(java_data_type, gateway.jvm.KeyValueDataType):
        logical_type = java_data_type.getLogicalType()
        key_type = java_data_type.getKeyDataType()
        value_type = java_data_type.getValueDataType()
        if _is_instance_of(logical_type, gateway.jvm.MapType):
            data_type = DataTypes.MAP(
                _from_java_type(key_type),
                _from_java_type(value_type),
                logical_type.isNullable())
        else:
            raise TypeError("Unsupported map data type: %s" % j_data_type)

        return data_type

    # Row Type.
    elif _is_instance_of(java_data_type, gateway.jvm.FieldsDataType):
        logical_type = java_data_type.getLogicalType()
        field_data_types = java_data_type.getFieldDataTypes()
        if _is_instance_of(logical_type, gateway.jvm.RowType):
            fields = [DataTypes.FIELD(item,
                                      _from_java_type(
                                          field_data_types[item])) for item in field_data_types]
            data_type = DataTypes.ROW(fields, logical_type.isNullable())
        else:
            raise TypeError("Unsupported row data type: %s" % j_data_type)

        return data_type

    # Unrecognized type.
    else:
        TypeError("Unsupported data type: %s" % j_data_type)


def _create_row(fields, values):
    row = Row(*values)
    row._fields = fields
    return row


class Row(tuple):
    """
    A row in Table.
    The fields in it can be accessed:

    * like attributes (``row.key``)
    * like dictionary values (``row[key]``)

    ``key in row`` will search through row keys.

    Row can be used to create a row object by using named arguments,
    the fields will be sorted by names. It is not allowed to omit
    a named argument to represent the value is None or missing. This should be
    explicitly set to None in this case.

    ::

        >>> row = Row(name="Alice", age=11)
        >>> row
        Row(age=11, name='Alice')
        >>> row['name'], row['age']
        ('Alice', 11)
        >>> row.name, row.age
        ('Alice', 11)
        >>> 'name' in row
        True
        >>> 'wrong_key' in row
        False

    Row can also be used to create another Row like class, then it
    could be used to create Row objects, such as

    ::

        >>> Person = Row("name", "age")
        >>> Person
        <Row(name, age)>
        >>> 'name' in Person
        True
        >>> 'wrong_key' in Person
        False
        >>> Person("Alice", 11)
        Row(name='Alice', age=11)
    """

    def __new__(cls, *args, **kwargs):
        if args and kwargs:
            raise ValueError("Can not use both args "
                             "and kwargs to create Row")
        if kwargs:
            # create row objects
            names = sorted(kwargs.keys())
            row = tuple.__new__(cls, [kwargs[n] for n in names])
            row._fields = names
            row._from_dict = True
            return row

        else:
            # create row class or objects
            return tuple.__new__(cls, args)

    def as_dict(self, recursive=False):
        """
        Returns as a dict.

        Example:
        ::

            >>> Row(name="Alice", age=11).as_dict() == {'name': 'Alice', 'age': 11}
            True
            >>> row = Row(key=1, value=Row(name='a', age=2))
            >>> row.as_dict() == {'key': 1, 'value': Row(age=2, name='a')}
            True
            >>> row.as_dict(True) == {'key': 1, 'value': {'name': 'a', 'age': 2}}
            True

        :param recursive: turns the nested Row as dict (default: False).
        """
        if not hasattr(self, "_fields"):
            raise TypeError("Cannot convert a Row class into dict")

        if recursive:
            def conv(obj):
                if isinstance(obj, Row):
                    return obj.as_dict(True)
                elif isinstance(obj, list):
                    return [conv(o) for o in obj]
                elif isinstance(obj, dict):
                    return dict((k, conv(v)) for k, v in obj.items())
                else:
                    return obj

            return dict(zip(self._fields, (conv(o) for o in self)))
        else:
            return dict(zip(self._fields, self))

    def __contains__(self, item):
        if hasattr(self, "_fields"):
            return item in self._fields
        else:
            return super(Row, self).__contains__(item)

    # let object acts like class
    def __call__(self, *args):
        """
        Creates new Row object
        """
        if len(args) > len(self):
            raise ValueError("Can not create Row with fields %s, expected %d values "
                             "but got %s" % (self, len(self), args))
        return _create_row(self, args)

    def __getitem__(self, item):
        if isinstance(item, (int, slice)):
            return super(Row, self).__getitem__(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self._fields.index(item)
            return super(Row, self).__getitem__(idx)
        except IndexError:
            raise KeyError(item)
        except ValueError:
            raise ValueError(item)

    def __getattr__(self, item):
        if item.startswith("_"):
            raise AttributeError(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self._fields.index(item)
            return self[idx]
        except IndexError:
            raise AttributeError(item)
        except ValueError:
            raise AttributeError(item)

    def __setattr__(self, key, value):
        if key != '_fields' and key != "_from_dict":
            raise Exception("Row is read-only")
        self.__dict__[key] = value

    def __reduce__(self):
        """
        Returns a tuple so Python knows how to pickle Row.
        """
        if hasattr(self, "_fields"):
            return _create_row, (self._fields, tuple(self))
        else:
            return tuple.__reduce__(self)

    def __repr__(self):
        """
        Printable representation of Row used in Python REPL.
        """
        if hasattr(self, "_fields"):
            return "Row(%s)" % ", ".join("%s=%r" % (k, v)
                                         for k, v in zip(self._fields, tuple(self)))
        else:
            return "<Row(%s)>" % ", ".join("%r" % field for field in self)


_acceptable_types = {
    BooleanType: (bool,),
    TinyIntType: (int,),
    SmallIntType: (int,),
    IntType: (int,),
    BigIntType: (int,),
    FloatType: (float,),
    DoubleType: (float,),
    DecimalType: (decimal.Decimal,),
    CharType: (str,),
    VarCharType: (str,),
    BinaryType: (bytearray,),
    VarBinaryType: (bytearray,),
    DateType: (datetime.date, datetime.datetime),
    TimeType: (datetime.time,),
    TimestampType: (datetime.datetime,),
    DayTimeIntervalType: (datetime.timedelta,),
    LocalZonedTimestampType: (datetime.datetime,),
    ZonedTimestampType: (datetime.datetime,),
    ArrayType: (list, tuple, array),
    MapType: (dict,),
    RowType: (tuple, list, dict),
}


def _create_type_verifier(data_type, name=None):
    """
    Creates a verifier that checks the type of obj against data_type and raises a TypeError if they
    do not match.

    This verifier also checks the value of obj against data_type and raises a ValueError if it's
    not within the allowed range, e.g. using 128 as TinyIntType will overflow. Note that, Python
    float is not checked, so it will become infinity when cast to Java float if it overflows.

    >>> _create_type_verifier(RowType([]))(None)
    >>> _create_type_verifier(VarCharType(100))("")
    >>> _create_type_verifier(BigIntType())(0)
    >>> _create_type_verifier(ArrayType(SmallIntType()))(list(range(3)))
    >>> _create_type_verifier(ArrayType(VarCharType(10)))(set()) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    TypeError:...
    >>> _create_type_verifier(MapType(VarCharType(100), IntType()))({})
    >>> _create_type_verifier(RowType([]))(())
    >>> _create_type_verifier(RowType([]))([])
    >>> _create_type_verifier(RowType([]))([1]) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> # Check if numeric values are within the allowed range.
    >>> _create_type_verifier(TinyIntType())(12)
    >>> _create_type_verifier(TinyIntType())(1234) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _create_type_verifier(TinyIntType(), False)(None) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _create_type_verifier(
    ...     ArrayType(SmallIntType(), False))([1, None]) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _create_type_verifier(MapType(VarCharType(100), IntType()))({None: 1})
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> schema = RowType().add("a", IntType()).add("b", VarCharType(100), False)
    >>> _create_type_verifier(schema)((1, None)) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    """

    if name is None:
        new_msg = lambda msg: msg
        new_name = lambda n: "field %s" % n
    else:
        new_msg = lambda msg: "%s: %s" % (name, msg)
        new_name = lambda n: "field %s in %s" % (n, name)

    def verify_nullability(obj):
        if obj is None:
            if data_type._nullable:
                return True
            else:
                raise ValueError(new_msg("This field is not nullable, but got None"))
        else:
            return False

    _type = type(data_type)

    assert _type in _acceptable_types or isinstance(data_type, UserDefinedType),\
        new_msg("unknown datatype: %s" % data_type)

    def verify_acceptable_types(obj):
        # subclass of them can not be from_sql_type in JVM
        if type(obj) not in _acceptable_types[_type]:
            raise TypeError(new_msg("%s can not accept object %r in type %s"
                                    % (data_type, obj, type(obj))))

    if isinstance(data_type, CharType):
        def verify_char(obj):
            verify_acceptable_types(obj)
            if len(obj) != data_type.length:
                raise ValueError(new_msg(
                    "length of object (%s) of CharType is not: %d" % (obj, data_type.length)))

        verify_value = verify_char

    elif isinstance(data_type, VarCharType):
        def verify_varchar(obj):
            verify_acceptable_types(obj)
            if len(obj) > data_type.length:
                raise ValueError(new_msg(
                    "length of object (%s) of VarCharType exceeds: %d" % (obj, data_type.length)))

        verify_value = verify_varchar

    elif isinstance(data_type, BinaryType):
        def verify_binary(obj):
            verify_acceptable_types(obj)
            if len(obj) != data_type.length:
                raise ValueError(new_msg(
                    "length of object (%s) of BinaryType is not: %d" % (obj, data_type.length)))

        verify_value = verify_binary

    elif isinstance(data_type, VarBinaryType):
        def verify_varbinary(obj):
            verify_acceptable_types(obj)
            if len(obj) > data_type.length:
                raise ValueError(new_msg(
                    "length of object (%s) of VarBinaryType exceeds: %d"
                    % (obj, data_type.length)))

        verify_value = verify_varbinary

    elif isinstance(data_type, UserDefinedType):
        verifier = _create_type_verifier(data_type.sql_type(), name=name)

        def verify_udf(obj):
            if not (hasattr(obj, '__UDT__') and obj.__UDT__ == data_type):
                raise ValueError(new_msg("%r is not an instance of type %r" % (obj, data_type)))
            verifier(data_type.to_sql_type(obj))

        verify_value = verify_udf

    elif isinstance(data_type, TinyIntType):
        def verify_tiny_int(obj):
            verify_acceptable_types(obj)
            if obj < -128 or obj > 127:
                raise ValueError(new_msg("object of TinyIntType out of range, got: %s" % obj))

        verify_value = verify_tiny_int

    elif isinstance(data_type, SmallIntType):
        def verify_small_int(obj):
            verify_acceptable_types(obj)
            if obj < -32768 or obj > 32767:
                raise ValueError(new_msg("object of SmallIntType out of range, got: %s" % obj))

        verify_value = verify_small_int

    elif isinstance(data_type, IntType):
        def verify_integer(obj):
            verify_acceptable_types(obj)
            if obj < -2147483648 or obj > 2147483647:
                raise ValueError(
                    new_msg("object of IntType out of range, got: %s" % obj))

        verify_value = verify_integer

    elif isinstance(data_type, ArrayType):
        element_verifier = _create_type_verifier(
            data_type.element_type, name="element in array %s" % name)

        def verify_array(obj):
            verify_acceptable_types(obj)
            for i in obj:
                element_verifier(i)

        verify_value = verify_array

    elif isinstance(data_type, MapType):
        key_verifier = _create_type_verifier(data_type.key_type, name="key of map %s" % name)
        value_verifier = _create_type_verifier(data_type.value_type, name="value of map %s" % name)

        def verify_map(obj):
            verify_acceptable_types(obj)
            for k, v in obj.items():
                key_verifier(k)
                value_verifier(v)

        verify_value = verify_map

    elif isinstance(data_type, RowType):
        verifiers = []
        for f in data_type.fields:
            verifier = _create_type_verifier(f.data_type, name=new_name(f.name))
            verifiers.append((f.name, verifier))

        def verify_row_field(obj):
            if isinstance(obj, dict):
                for f, verifier in verifiers:
                    verifier(obj.get(f))
            elif isinstance(obj, Row) and getattr(obj, "_from_dict", False):
                # the order in obj could be different than dataType.fields
                for f, verifier in verifiers:
                    verifier(obj[f])
            elif isinstance(obj, (tuple, list)):
                if len(obj) != len(verifiers):
                    raise ValueError(
                        new_msg("Length of object (%d) does not match with "
                                "length of fields (%d)" % (len(obj), len(verifiers))))
                for v, (_, verifier) in zip(obj, verifiers):
                    verifier(v)
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                for f, verifier in verifiers:
                    verifier(d.get(f))
            else:
                raise TypeError(new_msg("RowType can not accept object %r in type %s"
                                        % (obj, type(obj))))

        verify_value = verify_row_field

    else:
        def verify_default(obj):
            verify_acceptable_types(obj)

        verify_value = verify_default

    def verify(obj):
        if not verify_nullability(obj):
            verify_value(obj)

    return verify


class DataTypes(object):
    """
    A :class:`DataType` can be used to declare input and/or output types of operations.
    This class enumerates all supported data types of the Table & SQL API.
    """

    @staticmethod
    def NULL():
        """
        Data type for representing untyped null (None) values. A null type has no
        other value except null (None), thus, it can be cast to any nullable type.

        This type helps in representing unknown types in API calls that use a null
        (None) literal as well as bridging to formats such as JSON or Avro that
        define such a type as well.

        The null type is an extension to the SQL standard.

        .. note:: `NullType` is still not supported yet.
        """
        return NullType()

    @staticmethod
    def CHAR(length, nullable=True):
        """
        Data type of a fixed-length character string.

        :param length: int, the string representation length. It must have a value
                       between 1 and 2147483647(0x7fffffff) (both inclusive).
        :param nullable: boolean, whether the type can be null (None) or not.

        .. note:: `CharType` is still not supported yet.
        """
        return CharType(length, nullable)

    @staticmethod
    def VARCHAR(length, nullable=True):
        """
        Data type of a variable-length character string.

        :param length: int, the maximum string representation length. It must have a
                       value between 1 and 2147483647(0x7fffffff) (both inclusive).
        :param nullable: boolean, whether the type can be null (None) or not.

        .. note:: The length limit must be 0x7fffffff(2147483647) currently.
        .. seealso:: :func:`~DataTypes.STRING`
        """
        return VarCharType(length, nullable)

    @staticmethod
    def STRING(nullable=True):
        """
        Data type of a variable-length character string with defined maximum length.
        This is a shortcut for ``DataTypes.VARCHAR(2147483647)``.

        :param nullable: boolean, whether the type can be null (None) or not.

        .. seealso:: :func:`~DataTypes.VARCHAR`
        """
        return DataTypes.VARCHAR(0x7fffffff, nullable)

    @staticmethod
    def BOOLEAN(nullable=True):
        """
        Data type of a boolean with a (possibly) three-valued logic of
        TRUE, FALSE, UNKNOWN.

        :param nullable: boolean, whether the type can be null (None) or not.
        """
        return BooleanType(nullable)

    @staticmethod
    def BINARY(length, nullable=True):
        """
        Data type of a fixed-length binary string (=a sequence of bytes).

        :param length: int, the number of bytes. It must have a value between
                       1 and 2147483647(0x7fffffff) (both inclusive).
        :param nullable: boolean, whether the type can be null (None) or not.

        .. note:: `BinaryType` is still not supported yet.
        """
        return BinaryType(length, nullable)

    @staticmethod
    def VARBINARY(length, nullable=True):
        """
        Data type of a variable-length binary string (=a sequence of bytes)

        :param length: int, the maximum number of bytes. It must have a value
                       between 1 and 2147483647(0x7fffffff) (both inclusive).
        :param nullable: boolean, whether the type can be null (None) or not.

        .. note:: The length limit must be 0x7fffffff(2147483647) currently.
        .. seealso:: :func:`~DataTypes.BYTES`
        """
        return VarBinaryType(length, nullable)

    @staticmethod
    def BYTES(nullable=True):
        """
        Data type of a variable-length binary string (=a sequence of bytes) with
        defined maximum length. This is a shortcut for ``DataTypes.VARBINARY(2147483647)``.

        :param nullable: boolean, whether the type can be null (None) or not.

        .. seealso:: :func:`~DataTypes.VARBINARY`
        """
        return DataTypes.VARBINARY(0x7fffffff, nullable)

    @staticmethod
    def DECIMAL(precision, scale, nullable=True):
        """
        Data type of a decimal number with fixed precision and scale.

        :param precision: the number of digits in a number. It must have a value
                          between 1 and 38 (both inclusive).
        :param scale: the number of digits on right side of dot. It must have
                      a value between 0 and precision (both inclusive).
        :param nullable: boolean, whether the type can be null (None) or not.

        .. note:: The precision must be 38 and the scale must be 18 currently.
        """
        return DecimalType(precision, scale, nullable)

    @staticmethod
    def TINYINT(nullable=True):
        """
        Data type of a 1-byte signed integer with values from -128 to 127.

        :param nullable: boolean, whether the type can be null (None) or not.
        """
        return TinyIntType(nullable)

    @staticmethod
    def SMALLINT(nullable=True):
        """
        Data type of a 2-byte signed integer with values from -32,768 to 32,767.

        :param nullable: boolean, whether the type can be null (None) or not.
        """
        return SmallIntType(nullable)

    @staticmethod
    def INT(nullable=True):
        """
        Data type of a 2-byte signed integer with values from -2,147,483,648
        to 2,147,483,647.

        :param nullable: boolean, whether the type can be null (None) or not.
        """
        return IntType(nullable)

    @staticmethod
    def BIGINT(nullable=True):
        """
        Data type of an 8-byte signed integer with values from
        -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.

        :param nullable: boolean, whether the type can be null (None) or not.
        """
        return BigIntType(nullable)

    @staticmethod
    def FLOAT(nullable=True):
        """
        Data type of a 4-byte single precision floating point number.

        :param nullable: boolean, whether the type can be null (None) or not.
        """
        return FloatType(nullable)

    @staticmethod
    def DOUBLE(nullable=True):
        """
        Data type of an 8-byte double precision floating point number.

        :param nullable: boolean, whether the type can be null (None) or not.
        """
        return DoubleType(nullable)

    @staticmethod
    def DATE(nullable=True):
        """
        Data type of a date consisting of year-month-day with values ranging
        from ``0000-01-01`` to ``9999-12-31``.

        Compared to the SQL standard, the range starts at year 0000.

        :param nullable: boolean, whether the type can be null (None) or not.
        """
        return DateType(nullable)

    @staticmethod
    def TIME(precision=0, nullable=True):
        """
        Data type of a time WITHOUT time zone.

        An instance consists of hour:minute:second[.fractional with up to nanosecond
        precision and values ranging from ``00:00:00.000000000`` to ``23:59:59.999999999``.

        Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61)
        are not supported.

        :param precision: int, the number of digits of fractional seconds. It must
                          have a value between 0 and 9 (both inclusive).
        :param nullable: boolean, whether the type can be null (None) or not.

        .. note:: The precision must be 0 currently.
        """
        return TimeType(precision, nullable)

    @staticmethod
    def TIMESTAMP(precision=6, nullable=True):
        """
        Data type of a timestamp WITHOUT time zone.

        An instance consists of year-month-day hour:minute:second[.fractional
        with up to nanosecond precision and values ranging from
        ``0000-01-01 00:00:00.000000000`` to ``9999-12-31 23:59:59.999999999``.

        Compared to the SQL standard, leap seconds (``23:59:60`` and ``23:59:61``)
        are not supported.

        This class does not store or represent a time-zone. Instead, it is a description of
        the date, as used for birthdays, combined with the local time as seen on a wall clock.
        It cannot represent an instant on the time-line without additional information
        such as an offset or time-zone.

        :param precision: int, the number of digits of fractional seconds.
                          It must have a value between 0 and 9 (both inclusive). (default: 6)
        :param nullable: boolean, whether the type can be null (None) or not.

        .. note:: The precision must be 3 currently.
        """
        return TimestampType(precision, nullable)

    @staticmethod
    def TIMESTAMP_WITH_LOCAL_TIME_ZONE(precision=6, nullable=True):
        """
        Data type of a timestamp WITH LOCAL time zone.

        An instance consists of year-month-day hour:minute:second[.fractional
        with up to nanosecond precision and values ranging from
        ``0000-01-01 00:00:00.000000000 +14:59`` to ``9999-12-31 23:59:59.999999999 -14:59``.

        Compared to the SQL standard, leap seconds (``23:59:60`` and ``23:59:61``)
        are not supported.

        The value will be stored internally as a long value which stores all date and time
        fields, to a precision of nanoseconds, as well as the offset from UTC/Greenwich.

        :param precision: int, the number of digits of fractional seconds.
                          It must have a value between 0 and 9 (both inclusive). (default: 6)
        :param nullable: boolean, whether the type can be null (None) or not.

        .. note:: `LocalZonedTimestampType` is currently only supported in blink planner and the
                  precision must be 3.
        """
        return LocalZonedTimestampType(precision, nullable)

    @staticmethod
    def ARRAY(element_type, nullable=True):
        """
        Data type of an array of elements with same subtype.

        Compared to the SQL standard, the maximum cardinality of an array cannot
        be specified but is fixed at 2147483647(0x7fffffff). Also, any valid
        type is supported as a subtype.

        :param element_type: :class:`DataType` of each element in the array.
        :param nullable: boolean, whether the type can be null (None) or not.
        """
        return ArrayType(element_type, nullable)

    @staticmethod
    def MAP(key_type, value_type, nullable=True):
        """
        Data type of an associative array that maps keys to values. A map
        cannot contain duplicate keys; each key can map to at most one value.

        There is no restriction of key types; it is the responsibility of the
        user to ensure uniqueness. The map type is an extension to the SQL standard.

        :param key_type: :class:`DataType` of the keys in the map.
        :param value_type: :class:`DataType` of the values in the map.
        :param nullable: boolean, whether the type can be null (None) or not.
        """
        return MapType(key_type, value_type, nullable)

    @staticmethod
    def MULTISET(element_type, nullable=True):
        """
        Data type of a multiset (=bag). Unlike a set, it allows for multiple
        instances for each of its elements with a common subtype. Each unique
        value is mapped to some multiplicity.

        There is no restriction of element types; it is the responsibility
        of the user to ensure uniqueness.

        :param element_type: :class:`DataType` of each element in the multiset.
        :param nullable: boolean, whether the type can be null (None) or not.
        """
        return MultisetType(element_type, nullable)

    @staticmethod
    def ROW(row_fields=[], nullable=True):
        """
        Data type of a sequence of fields. A field consists of a field name,
        field type, and an optional description. The most specific type of
        a row of a table is a row type. In this case, each column of the row
        corresponds to the field of the row type that has the same ordinal
        position as the column.

        Compared to the SQL standard, an optional field description simplifies
        the handling with complex structures.

        :param row_fields: a list of row field types which can be created via
                           :func:`DataTypes.FIELD`.
        :param nullable: boolean, whether the type can be null (None) or not.
        """
        return RowType(row_fields, nullable)

    @staticmethod
    def FIELD(name, data_type, description=None):
        """
        Field definition with field name, data type, and a description.

        :param name: string, name of the field.
        :param data_type: :class:`DataType` of the field.
        :param description: string, description of the field.
        """
        return RowField(name, data_type, description)

    @staticmethod
    def SECOND(precision=DayTimeIntervalType.DEFAULT_FRACTIONAL_PRECISION):
        """
        Resolution in seconds and (possibly) fractional seconds.

        :param precision: int, the number of digits of fractional seconds. It must have a value
                          between 0 and 9 (both inclusive), (default: 6).
        :return: the specified :class:`Resolution`.

        .. note:: the precision must be 3 currently.
        .. seealso:: :func:`~pyflink.table.DataTypes.INTERVAL`
        """
        return Resolution(Resolution.IntervalUnit.SECOND, precision)

    @staticmethod
    def MINUTE():
        """
        Resolution in minutes.

        :return: the specified :class:`Resolution`.

        .. seealso:: :func:`~pyflink.table.DataTypes.INTERVAL`
        """
        return Resolution(Resolution.IntervalUnit.MINUTE)

    @staticmethod
    def HOUR():
        """
        Resolution in hours.

        :return: :class:`Resolution`

        .. seealso:: :func:`~pyflink.table.DataTypes.INTERVAL`
        """
        return Resolution(Resolution.IntervalUnit.HOUR)

    @staticmethod
    def DAY(precision=DayTimeIntervalType.DEFAULT_DAY_PRECISION):
        """
        Resolution in days.

        :param precision: int, the number of digits of days. It must have a value between 1 and
                          6 (both inclusive), (default: 2).
        :return: the specified :class:`Resolution`.

        .. seealso:: :func:`~pyflink.table.DataTypes.INTERVAL`
        """
        return Resolution(Resolution.IntervalUnit.DAY, precision)

    @staticmethod
    def MONTH():
        """
        Resolution in months.

        :return: the specified :class:`Resolution`.

        .. seealso:: :func:`~pyflink.table.DataTypes.INTERVAL`
        """
        return Resolution(Resolution.IntervalUnit.MONTH)

    @staticmethod
    def YEAR(precision=YearMonthIntervalType.DEFAULT_PRECISION):
        """
        Resolution in years with 2 digits for the number of years by default.

        :param precision: the number of digits of years. It must have a value between 1 and
                          4 (both inclusive), (default 2).
        :return: the specified :class:`Resolution`.

        .. seealso:: :func:`~pyflink.table.DataTypes.INTERVAL`
        """
        return Resolution(Resolution.IntervalUnit.YEAR, precision)

    @staticmethod
    def INTERVAL(upper_resolution, lower_resolution=None):
        """
        Data type of a temporal interval. There are two types of temporal intervals: day-time
        intervals with up to nanosecond granularity or year-month intervals with up to month
        granularity.

        An interval of day-time consists of ``+days hours:months:seconds.fractional`` with values
        ranging from ``-999999 23:59:59.999999999`` to ``+999999 23:59:59.999999999``. The type
        must be parameterized to one of the following resolutions: interval of days, interval of
        days to hours, interval of days to minutes, interval of days to seconds, interval of hours,
        interval of hours to minutes, interval of hours to seconds, interval of minutes,
        interval of minutes to seconds, or interval of seconds. The value representation is the
        same for all types of resolutions. For example, an interval of seconds of 70 is always
        represented in an interval-of-days-to-seconds format (with default precisions):
        ``+00 00:01:10.000000``.

        An interval of year-month consists of ``+years-months`` with values ranging from
        ``-9999-11`` to ``+9999-11``. The type must be parameterized to one of the following
        resolutions: interval of years, interval of years to months, or interval of months. The
        value representation is the same for all types of resolutions. For example, an interval
        of months of 50 is always represented in an interval-of-years-to-months format (with
        default year precision): ``+04-02``.

        Examples: ``INTERVAL(DAY(2), SECOND(9))`` for a day-time interval or
        ``INTERVAL(YEAR(4), MONTH())`` for a year-month interval.

        :param upper_resolution: :class:`Resolution`, the upper resolution of the interval.
        :param lower_resolution: :class:`Resolution`, the lower resolution of the interval.

        .. note:: the upper_resolution must be `MONTH` for `YearMonthIntervalType`, `SECOND` for
                  `DayTimeIntervalType` and the lower_resolution must be None currently.
        .. seealso:: :func:`~pyflink.table.DataTypes.SECOND`
        .. seealso:: :func:`~pyflink.table.DataTypes.MINUTE`
        .. seealso:: :func:`~pyflink.table.DataTypes.HOUR`
        .. seealso:: :func:`~pyflink.table.DataTypes.DAY`
        .. seealso:: :func:`~pyflink.table.DataTypes.MONTH`
        .. seealso:: :func:`~pyflink.table.DataTypes.YEAR`
        """
        return _from_resolution(upper_resolution, lower_resolution)
