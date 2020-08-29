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
from typing import Union

from pyflink import add_version_doc
from pyflink.java_gateway import get_gateway
from pyflink.table.types import DataType, _to_java_data_type
from pyflink.util.utils import to_jarray

__all__ = ['Expression', 'TimeIntervalUnit', 'TimePointUnit']


_aggregation_doc = """
{op_desc}

Example:
::

    >>> tab \\
    >>>     .group_by(col("a")) \\
    >>>     .select(col("a"),
    >>>             col("b").sum().alias("d"),
    >>>             col("b").sum0().alias("e"),
    >>>             col("b").min().alias("f"),
    >>>             col("b").max().alias("g"),
    >>>             col("b").count().alias("h"),
    >>>             col("b").avg().alias("i"),
    >>>             col("b").stddev_pop().alias("j"),
    >>>             col("b").stddev_samp().alias("k"),
    >>>             col("b").var_pop().alias("l"),
    >>>             col("b").var_samp().alias("m"),
    >>>             col("b").collect().alias("n"))

.. seealso:: :func:`~Expression.sum`, :func:`~Expression.sum0`, :func:`~Expression.min`,
             :func:`~Expression.max`, :func:`~Expression.count`, :func:`~Expression.avg`,
             :func:`~Expression.stddev_pop`, :func:`~Expression.stddev_samp`,
             :func:`~Expression.var_pop`, :func:`~Expression.var_samp`,
             :func:`~Expression.collect`
"""


_math_log_doc = """
{op_desc}

.. seealso:: :func:`~Expression.log10`, :func:`~Expression.log2`, :func:`~Expression.ln`,
             :func:`~Expression.log`
"""


_math_trigonometric_doc = """
Calculates the {op_desc} of a given number.

.. seealso:: :func:`~Expression.sin`, :func:`~Expression.cos`, :func:`~Expression.sinh`,
             :func:`~Expression.cosh`, :func:`~Expression.tan`, :func:`~Expression.cot`,
             :func:`~Expression.asin`, :func:`~Expression.acos`, :func:`~Expression.atan`,
             :func:`~Expression.tanh`
"""

_string_doc_seealso = """
.. seealso:: :func:`~Expression.trim_leading`, :func:`~Expression.trim_trailing`,
             :func:`~Expression.trim`, :func:`~Expression.replace`, :func:`~Expression.char_length`,
             :func:`~Expression.upper_case`, :func:`~Expression.lower_case`,
             :func:`~Expression.init_cap`, :func:`~Expression.like`, :func:`~Expression.similar`,
             :func:`~Expression.position`, :func:`~Expression.lpad`, :func:`~Expression.rpad`,
             :func:`~Expression.overlay`, :func:`~Expression.regexp_replace`,
             :func:`~Expression.regexp_extract`, :func:`~Expression.substring`,
             :func:`~Expression.from_base64`, :func:`~Expression.to_base64`,
             :func:`~Expression.ltrim`, :func:`~Expression.rtrim`, :func:`~Expression.repeat`
"""

_temporal_doc_seealso = """
.. seealso:: :func:`~Expression.to_date`, :func:`~Expression.to_time`,
             :func:`~Expression.to_timestamp`, :func:`~Expression.extract`,
             :func:`~Expression.floor`, :func:`~Expression.ceil`
"""


_time_doc = """
Creates an interval of the given number of {op_desc}.

The produced expression is of type :func:`~DataTypes.INTERVAL`.

.. seealso:: :func:`~Expression.year`, :func:`~Expression.years`, :func:`~Expression.quarter`,
             :func:`~Expression.quarters`, :func:`~Expression.month`, :func:`~Expression.months`,
             :func:`~Expression.week`, :func:`~Expression.weeks`, :func:`~Expression.day`,
             :func:`~Expression.days`, :func:`~Expression.hour`, :func:`~Expression.hours`,
             :func:`~Expression.minute`, :func:`~Expression.minutes`, :func:`~Expression.second`,
             :func:`~Expression.seconds`, :func:`~Expression.milli`, :func:`~Expression.millis`
"""


_hash_doc = """
Returns the {op_desc} hash of the string argument; null if string is null.

:return: string of {bit} hexadecimal digits or null.

.. seealso:: :func:`~Expression.md5`, :func:`~Expression.sha1`, :func:`~Expression.sha224`,
             :func:`~Expression.sha256`, :func:`~Expression.sha384`, :func:`~Expression.sha512`,
             :func:`~Expression.sha2`
"""


def _make_math_log_doc():
    math_log_funcs = {
        Expression.log10: "Calculates the base 10 logarithm of the given value.",
        Expression.log2: "Calculates the base 2 logarithm of the given value.",
        Expression.ln: "Calculates the natural logarithm of the given value.",
        Expression.log: "Calculates the natural logarithm of the given value if base is not "
                        "specified. Otherwise, calculates the logarithm of the given value to the "
                        "given base.",
    }

    for func, op_desc in math_log_funcs.items():
        func.__doc__ = _math_log_doc.format(op_desc=op_desc)


def _make_math_trigonometric_doc():
    math_trigonometric_funcs = {
        Expression.cosh: "hyperbolic cosine",
        Expression.sinh: "hyperbolic sine",
        Expression.sin: "sine",
        Expression.cos: "cosine",
        Expression.tan: "tangent",
        Expression.cot: "cotangent",
        Expression.asin: "arc sine",
        Expression.acos: "arc cosine",
        Expression.atan: "arc tangent",
        Expression.tanh: "hyperbolic tangent",
    }

    for func, op_desc in math_trigonometric_funcs.items():
        func.__doc__ = _math_trigonometric_doc.format(op_desc=op_desc)


def _make_aggregation_doc():
    aggregation_funcs = {
        Expression.sum: "Returns the sum of the numeric field across all input values. "
                        "If all values are null, null is returned.",
        Expression.sum0: "Returns the sum of the numeric field across all input values. "
                        "If all values are null, 0 is returned.",
        Expression.min: "Returns the minimum value of field across all input values.",
        Expression.max: "Returns the maximum value of field across all input values.",
        Expression.count: "Returns the number of input rows for which the field is not null.",
        Expression.avg: "Returns the average (arithmetic mean) of the numeric field across all "
                        "input values.",
        Expression.stddev_pop: "Returns the population standard deviation of an expression(the "
                               "square root of varPop()).",
        Expression.stddev_samp: "Returns the sample standard deviation of an expression(the square "
                                "root of varSamp()).",
        Expression.var_pop: "Returns the population standard variance of an expression.",
        Expression.var_samp: "Returns the sample variance of a given expression.",
        Expression.collect: "Returns multiset aggregate of a given expression.",
    }

    for func, op_desc in aggregation_funcs.items():
        func.__doc__ = _aggregation_doc.format(op_desc=op_desc)


def _make_string_doc():
    string_funcs = [
        Expression.substring, Expression.trim_leading, Expression.trim_trailing, Expression.trim,
        Expression.replace, Expression.char_length, Expression.upper_case, Expression.lower_case,
        Expression.init_cap, Expression.like, Expression.similar, Expression.position,
        Expression.lpad, Expression.rpad, Expression.overlay, Expression.regexp_replace,
        Expression.regexp_extract, Expression.from_base64, Expression.to_base64,
        Expression.ltrim, Expression.rtrim, Expression.repeat
    ]

    for func in string_funcs:
        func.__doc__ = func.__doc__.replace('  ', '') + _string_doc_seealso


def _make_temporal_doc():
    temporal_funcs = [
        Expression.to_date, Expression.to_time, Expression.to_timestamp, Expression.extract,
        Expression.floor, Expression.ceil
    ]

    for func in temporal_funcs:
        func.__doc__ = func.__doc__.replace('  ', '') + _temporal_doc_seealso


def _make_time_doc():
    time_funcs = {
        Expression.year: "years",
        Expression.years: "years",
        Expression.quarter: "quarters",
        Expression.quarters: "quarters",
        Expression.month: "months",
        Expression.months: "months",
        Expression.week: "weeks",
        Expression.weeks: "weeks",
        Expression.day: "days",
        Expression.days: "days",
        Expression.hour: "hours",
        Expression.hours: "hours",
        Expression.minute: "minutes",
        Expression.minutes: "minutes",
        Expression.second: "seconds",
        Expression.seconds: "seconds",
        Expression.milli: "millis",
        Expression.millis: "millis"
    }

    for func, op_desc in time_funcs.items():
        func.__doc__ = _time_doc.format(op_desc=op_desc)


def _make_hash_doc():
    hash_funcs = {
        Expression.md5: ("MD5", 32),
        Expression.sha1: ("SHA-1", 40),
        Expression.sha224: ("SHA-224", 56),
        Expression.sha256: ("SHA-256", 64),
        Expression.sha384: ("SHA-384", 96),
        Expression.sha512: ("SHA-512", 128)
    }

    for func, (op_desc, bit) in hash_funcs.items():
        func.__doc__ = _hash_doc.format(op_desc=op_desc, bit=bit)


def _add_version_doc():
    for func_name in dir(Expression):
        if not func_name.startswith("_"):
            add_version_doc(getattr(Expression, func_name), "1.12.0")


def _get_java_expression(expr):
    return expr._j_expr if isinstance(expr, Expression) else expr


def _get_or_create_java_expression(expr: Union["Expression", str]):
    if isinstance(expr, Expression):
        return expr._j_expr
    elif isinstance(expr, str):
        from pyflink.table.expressions import col
        return col(expr)._j_expr
    else:
        raise TypeError(
            "Invalid argument: expected Expression or string, got {0}.".format(type(expr)))


def _unary_op(op_name: str):
    def _(self):
        return Expression(getattr(self._j_expr, op_name)())

    return _


def _binary_op(op_name: str, reverse: bool = False):
    def _(self, other):
        if reverse:
            return Expression(getattr(_get_java_expression(other), op_name)(self._j_expr))
        else:
            return Expression(getattr(self._j_expr, op_name)(_get_java_expression(other)))

    return _


def _ternary_op(op_name: str):
    def _(self, first, second):
        return Expression(getattr(self._j_expr, op_name)(
            _get_java_expression(first), _get_java_expression(second)))

    return _


def _expressions_op(op_name: str):
    def _(self, *args):
        from pyflink.table import expressions
        return getattr(expressions, op_name)(self, *[_get_java_expression(arg) for arg in args])

    return _


class TimeIntervalUnit(object):
    """
    Units for working with time intervals.

    .. versionadded:: 1.12.0
    """

    YEAR = 0,
    YEAR_TO_MONTH = 1,
    QUARTER = 2,
    MONTH = 3,
    WEEK = 4,
    DAY = 5,
    DAY_TO_HOUR = 6,
    DAY_TO_MINUTE = 7,
    DAY_TO_SECOND = 8,
    HOUR = 9,
    SECOND = 10,
    HOUR_TO_MINUTE = 11,
    HOUR_TO_SECOND = 12,
    MINUTE = 13,
    MINUTE_TO_SECOND = 14

    @staticmethod
    def _from_j_time_interval_unit(j_time_interval_unit):
        gateway = get_gateway()
        JTimeIntervalUnit = gateway.jvm.org.apache.flink.table.expressions.TimeIntervalUnit
        if j_time_interval_unit == JTimeIntervalUnit.YEAR:
            return TimeIntervalUnit.YEAR
        elif j_time_interval_unit == JTimeIntervalUnit.YEAR_TO_MONTH:
            return TimeIntervalUnit.YEAR_TO_MONTH
        elif j_time_interval_unit == JTimeIntervalUnit.QUARTER:
            return TimeIntervalUnit.QUARTER
        elif j_time_interval_unit == JTimeIntervalUnit.MONTH:
            return TimeIntervalUnit.MONTH
        elif j_time_interval_unit == JTimeIntervalUnit.WEEK:
            return TimeIntervalUnit.WEEK
        elif j_time_interval_unit == JTimeIntervalUnit.DAY:
            return TimeIntervalUnit.DAY
        elif j_time_interval_unit == JTimeIntervalUnit.DAY_TO_HOUR:
            return TimeIntervalUnit.DAY_TO_HOUR
        elif j_time_interval_unit == JTimeIntervalUnit.DAY_TO_MINUTE:
            return TimeIntervalUnit.DAY_TO_MINUTE
        elif j_time_interval_unit == JTimeIntervalUnit.DAY_TO_SECOND:
            return TimeIntervalUnit.DAY_TO_SECOND
        elif j_time_interval_unit == JTimeIntervalUnit.HOUR:
            return TimeIntervalUnit.HOUR
        elif j_time_interval_unit == JTimeIntervalUnit.SECOND:
            return TimeIntervalUnit.SECOND
        elif j_time_interval_unit == JTimeIntervalUnit.HOUR_TO_MINUTE:
            return TimeIntervalUnit.HOUR_TO_MINUTE
        elif j_time_interval_unit == JTimeIntervalUnit.HOUR_TO_SECOND:
            return TimeIntervalUnit.HOUR_TO_SECOND
        elif j_time_interval_unit == JTimeIntervalUnit.MINUTE:
            return TimeIntervalUnit.MINUTE
        elif j_time_interval_unit == JTimeIntervalUnit.MINUTE_TO_SECOND:
            return TimeIntervalUnit.MINUTE_TO_SECOND
        else:
            raise Exception("Unsupported Java time interval unit: %s." % j_time_interval_unit)

    @staticmethod
    def _to_j_time_interval_unit(time_interval_unit):
        gateway = get_gateway()
        JTimeIntervalUnit = gateway.jvm.org.apache.flink.table.expressions.TimeIntervalUnit
        if time_interval_unit == TimeIntervalUnit.YEAR:
            j_time_interval_unit = JTimeIntervalUnit.YEAR
        elif time_interval_unit == TimeIntervalUnit.YEAR_TO_MONTH:
            j_time_interval_unit = JTimeIntervalUnit.YEAR_TO_MONTH
        elif time_interval_unit == TimeIntervalUnit.QUARTER:
            j_time_interval_unit = JTimeIntervalUnit.QUARTER
        elif time_interval_unit == TimeIntervalUnit.MONTH:
            j_time_interval_unit = JTimeIntervalUnit.MONTH
        elif time_interval_unit == TimeIntervalUnit.WEEK:
            j_time_interval_unit = JTimeIntervalUnit.WEEK
        elif time_interval_unit == TimeIntervalUnit.DAY:
            j_time_interval_unit = JTimeIntervalUnit.DAY
        elif time_interval_unit == TimeIntervalUnit.DAY_TO_HOUR:
            j_time_interval_unit = JTimeIntervalUnit.DAY_TO_HOUR
        elif time_interval_unit == TimeIntervalUnit.DAY_TO_MINUTE:
            j_time_interval_unit = JTimeIntervalUnit.DAY_TO_MINUTE
        elif time_interval_unit == TimeIntervalUnit.DAY_TO_SECOND:
            j_time_interval_unit = JTimeIntervalUnit.DAY_TO_SECOND
        elif time_interval_unit == TimeIntervalUnit.HOUR:
            j_time_interval_unit = JTimeIntervalUnit.HOUR
        elif time_interval_unit == TimeIntervalUnit.SECOND:
            j_time_interval_unit = JTimeIntervalUnit.SECOND
        elif time_interval_unit == TimeIntervalUnit.HOUR_TO_MINUTE:
            j_time_interval_unit = JTimeIntervalUnit.HOUR_TO_MINUTE
        elif time_interval_unit == TimeIntervalUnit.HOUR_TO_SECOND:
            j_time_interval_unit = JTimeIntervalUnit.HOUR_TO_SECOND
        elif time_interval_unit == TimeIntervalUnit.MINUTE:
            j_time_interval_unit = JTimeIntervalUnit.MINUTE
        elif time_interval_unit == TimeIntervalUnit.MINUTE_TO_SECOND:
            j_time_interval_unit = JTimeIntervalUnit.MINUTE_TO_SECOND
        else:
            raise TypeError("Unsupported time interval unit: %s, supported time interval unit "
                            "are: YEAR, YEAR_TO_MONTH, QUARTER, MONTH, WEEK, DAY, DAY_TO_HOUR, "
                            "DAY_TO_MINUTE, DAY_TO_SECOND, HOUR, SECOND, HOUR_TO_MINUTE, "
                            "HOUR_TO_SECOND, MINUTE, MINUTE_TO_SECOND" % time_interval_unit)
        return j_time_interval_unit


class TimePointUnit(object):
    """
    Units for working with points in time.

    .. versionadded:: 1.12.0
    """

    YEAR = 0,
    MONTH = 1,
    DAY = 2,
    HOUR = 3,
    MINUTE = 4,
    SECOND = 5,
    QUARTER = 6,
    WEEK = 7,
    MILLISECOND = 8,
    MICROSECOND = 9

    @staticmethod
    def _from_j_time_point_unit(j_time_point_unit):
        gateway = get_gateway()
        JTimePointUnit = gateway.jvm.org.apache.flink.table.expressions.TimePointUnit
        if j_time_point_unit == JTimePointUnit.YEAR:
            return TimePointUnit.YEAR
        elif j_time_point_unit == JTimePointUnit.MONTH:
            return TimePointUnit.MONTH
        elif j_time_point_unit == JTimePointUnit.DAY:
            return TimePointUnit.DAY
        elif j_time_point_unit == JTimePointUnit.HOUR:
            return TimePointUnit.HOUR
        elif j_time_point_unit == JTimePointUnit.MINUTE:
            return TimePointUnit.MINUTE
        elif j_time_point_unit == JTimePointUnit.SECOND:
            return TimePointUnit.SECOND
        elif j_time_point_unit == JTimePointUnit.QUARTER:
            return TimePointUnit.QUARTER
        elif j_time_point_unit == JTimePointUnit.WEEK:
            return TimePointUnit.WEEK
        elif j_time_point_unit == JTimePointUnit.MILLISECOND:
            return TimePointUnit.MILLISECOND
        elif j_time_point_unit == JTimePointUnit.MICROSECOND:
            return TimePointUnit.MICROSECOND
        else:
            raise Exception("Unsupported Java time point unit: %s." % j_time_point_unit)

    @staticmethod
    def _to_j_time_point_unit(time_point_unit):
        gateway = get_gateway()
        JTimePointUnit = gateway.jvm.org.apache.flink.table.expressions.TimePointUnit
        if time_point_unit == TimePointUnit.YEAR:
            j_time_point_unit = JTimePointUnit.YEAR
        elif time_point_unit == TimePointUnit.MONTH:
            j_time_point_unit = JTimePointUnit.MONTH
        elif time_point_unit == TimePointUnit.DAY:
            j_time_point_unit = JTimePointUnit.DAY
        elif time_point_unit == TimePointUnit.HOUR:
            j_time_point_unit = JTimePointUnit.HOUR
        elif time_point_unit == TimePointUnit.MINUTE:
            j_time_point_unit = JTimePointUnit.MINUTE
        elif time_point_unit == TimePointUnit.SECOND:
            j_time_point_unit = JTimePointUnit.SECOND
        elif time_point_unit == TimePointUnit.QUARTER:
            j_time_point_unit = JTimePointUnit.QUARTER
        elif time_point_unit == TimePointUnit.WEEK:
            j_time_point_unit = JTimePointUnit.WEEK
        elif time_point_unit == TimePointUnit.MILLISECOND:
            j_time_point_unit = JTimePointUnit.MILLISECOND
        elif time_point_unit == TimePointUnit.MICROSECOND:
            j_time_point_unit = JTimePointUnit.MICROSECOND
        else:
            raise TypeError("Unsupported time point unit: %s, supported time point unit are: "
                            "YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, QUARTER, "
                            "WEEK, MILLISECOND, MICROSECOND" % time_point_unit)
        return j_time_point_unit


class Expression(object):
    """
    Expressions represent a logical tree for producing a computation result.
    Expressions might be literal values, function calls, or field references.

    .. versionadded:: 1.12.0
    """

    def __init__(self, j_expr_or_property_name):
        self._j_expr_or_property_name = j_expr_or_property_name

    __abs__ = _unary_op("abs")

    # comparison functions
    __eq__ = _binary_op("isEqual")
    __ne__ = _binary_op("isNotEqual")
    __lt__ = _binary_op("isLess")
    __gt__ = _binary_op("isGreater")
    __le__ = _binary_op("isLessOrEqual")
    __ge__ = _binary_op("isGreaterOrEqual")

    # logic functions
    __and__ = _binary_op("and")
    __or__ = _binary_op("or")
    __invert__ = _unary_op('isNotTrue')

    __rand__ = _binary_op("and")
    __ror__ = _binary_op("or")

    # arithmetic functions
    __add__ = _binary_op("plus")
    __sub__ = _binary_op("minus")
    __mul__ = _binary_op("times")
    __truediv__ = _binary_op("dividedBy")
    __mod__ = _binary_op("mod")
    __pow__ = _binary_op("power")
    __neg__ = _expressions_op("negative")

    __radd__ = _binary_op("plus")
    __rsub__ = _binary_op("minus", True)
    __rmul__ = _binary_op("times")
    __rtruediv__ = _binary_op("dividedBy", True)
    __rmod__ = _binary_op("mod", True)
    __rpow__ = _binary_op("power", True)

    def __str__(self):
        return self._j_expr.asSummaryString()

    def __getattr__(self, name):
        if name == '_j_expr':
            if isinstance(self._j_expr_or_property_name, str):
                gateway = get_gateway()
                return getattr(gateway.jvm.Expressions, self._j_expr_or_property_name)
            else:
                return self._j_expr_or_property_name
        return self.get(name)

    def __getitem__(self, index):
        return self.at(index)

    # ---------------------------- arithmetic functions ----------------------------------

    def exp(self):
        """
        Calculates the Euler's number raised to the given power.
        """
        return _unary_op("exp")(self)

    def log10(self):
        return _unary_op("log10")(self)

    def log2(self):
        return _unary_op("log2")(self)

    def ln(self):
        return _unary_op("ln")(self)

    def log(self, base=None):
        if base is None:
            return _unary_op("log")(self)
        else:
            return _binary_op("log")(self, base)

    def cosh(self):
        return _unary_op("cosh")(self)

    def sinh(self):
        return _unary_op("sinh")(self)

    def sin(self):
        return _unary_op("sin")(self)

    def cos(self):
        return _unary_op("cos")(self)

    def tan(self):
        return _unary_op("tan")(self)

    def cot(self):
        return _unary_op("cot")(self)

    def asin(self):
        return _unary_op("asin")(self)

    def acos(self):
        return _unary_op("acos")(self)

    def atan(self):
        return _unary_op("atan")(self)

    def tanh(self):
        return _unary_op("tanh")(self)

    def degrees(self):
        """
        Converts numeric from radians to degrees.

        .. seealso:: :func:`~Expression.radians`
        """
        return _unary_op("degrees")(self)

    def radians(self):
        """
        Converts numeric from degrees to radians.

        .. seealso:: :func:`~Expression.degrees`
        """
        return _unary_op("radians")(self)

    def sqrt(self):
        """
        Calculates the square root of a given value.
        """
        return _unary_op("sqrt")(self)

    def abs(self):
        """
        Calculates the absolute value of given value.
        """
        return _unary_op("abs")(self)

    def sign(self):
        """
        Calculates the signum of a given number.
        """
        return _unary_op("sign")(self)

    def round(self, places):
        """
        Rounds the given number to integer places right to the decimal point.
        """
        return _binary_op("round")(self, places)

    def between(self, lower_bound, upper_bound):
        """
        Returns true if the given expression is between lower_bound and upper_bound
        (both inclusive). False otherwise. The parameters must be numeric types or identical
        comparable types.

        :param lower_bound: numeric or comparable expression
        :param upper_bound: numeric or comparable expression

        .. seealso:: :func:`~Expression.not_between`
        """
        return _ternary_op("between")(self, lower_bound, upper_bound)

    def not_between(self, lower_bound, upper_bound):
        """
        Returns true if the given expression is not between lower_bound and upper_bound
        (both inclusive). False otherwise. The parameters must be numeric types or identical
        comparable types.

        :param lower_bound: numeric or comparable expression
        :param upper_bound: numeric or comparable expression

        .. seealso:: :func:`~Expression.between`
        """
        return _ternary_op("notBetween")(self, lower_bound, upper_bound)

    def then(self, if_true, if_false):
        """
        Ternary conditional operator that decides which of two other expressions should be evaluated
        based on a evaluated boolean condition.

        e.g. lit(42).is_greater(5).then("A", "B") leads to "A"

        :param if_true: expression to be evaluated if condition holds
        :param if_false: expression to be evaluated if condition does not hold
        """
        return _ternary_op("then")(self, if_true, if_false)

    def is_null(self):
        """
        Returns true if the given expression is null.

        .. seealso:: :func:`~Expression.is_not_null`
        """
        return _unary_op("isNull")(self)

    def is_not_null(self):
        """
        Returns true if the given expression is not null.

        .. seealso:: :func:`~Expression.is_null`
        """
        return _unary_op("isNotNull")(self)

    def is_true(self):
        """
        Returns true if given boolean expression is true. False otherwise (for null and false).

        .. seealso:: :func:`~Expression.is_false`, :func:`~Expression.is_not_true`,
                     :func:`~Expression.is_not_false`
        """
        return _unary_op("isTrue")(self)

    def is_false(self):
        """
        Returns true if given boolean expression is false. False otherwise (for null and true).

        .. seealso:: :func:`~Expression.is_true`, :func:`~Expression.is_not_true`,
                     :func:`~Expression.is_not_false`
        """
        return _unary_op("isFalse")(self)

    def is_not_true(self):
        """
        Returns true if given boolean expression is not true (for null and false). False otherwise.

        .. seealso:: :func:`~Expression.is_true`, :func:`~Expression.is_false`,
                     :func:`~Expression.is_not_false`
        """
        return _unary_op("isNotTrue")(self)

    def is_not_false(self):
        """
        Returns true if given boolean expression is not false (for null and true). False otherwise.

        .. seealso:: :func:`~Expression.is_true`, :func:`~Expression.is_false`,
                     :func:`~Expression.is_not_true`
        """
        return _unary_op("isNotFalse")(self)

    def distinct(self):
        """
        Similar to a SQL distinct aggregation clause such as COUNT(DISTINCT a), declares that an
        aggregation function is only applied on distinct input values.

        Example:
        ::

            >>> tab \\
            >>>     .group_by(col("a")) \\
            >>>     .select(col("a"), col("b").sum().distinct().alias("d"))
        """
        return _unary_op("distinct")(self)

    def sum(self):
        return _unary_op("sum")(self)

    def sum0(self):
        return _unary_op("sum0")(self)

    def min(self):
        return _unary_op("min")(self)

    def max(self):
        return _unary_op("max")(self)

    def count(self):
        return _unary_op("count")(self)

    def avg(self):
        return _unary_op("avg")(self)

    def stddev_pop(self):
        return _unary_op("stddevPop")(self)

    def stddev_samp(self):
        return _unary_op("stddevSamp")(self)

    def var_pop(self):
        return _unary_op("varPop")(self)

    def var_samp(self):
        return _unary_op("varSamp")(self)

    def collect(self):
        return _unary_op("collect")(self)

    def alias(self, name: str, *extra_names: str):
        """
        Specifies a name for an expression i.e. a field.

        Example:
        ::

            >>> tab.select(col('a').alias('b'))

        :param name: name for one field.
        :param extra_names: additional names if the expression expands to multiple fields
        """
        gateway = get_gateway()
        return _ternary_op("as")(self, name, to_jarray(gateway.jvm.String, extra_names))

    def cast(self, data_type: DataType):
        """
        Converts a value to a given data type.

        e.g. "42".cast(DataTypes.INT()) leads to 42.
        """
        return _binary_op("cast")(self, _to_java_data_type(data_type))

    def asc(self):
        """
        Specifies ascending order of an expression i.e. a field for order_by.

        Example:
        ::

            >>> tab.order_by(col('a').asc())

        .. seealso:: :func:`~Expression.desc`
        """
        return _unary_op("asc")(self)

    def desc(self):
        """
        Specifies descending order of an expression i.e. a field for order_by.

        Example:
        ::

            >>> tab.order_by(col('a').desc())

        .. seealso:: :func:`~Expression.asc`
        """
        return _unary_op("desc")(self)

    def in_(self, first_element_or_table, *remaining_elements):
        """
        If first_element_or_table is a Table, Returns true if an expression exists in a given table
        sub-query. The sub-query table must consist of one column. This column must have the same
        data type as the expression.

        .. note::

            This operation is not supported in a streaming environment yet if
            first_element_or_table is a Table.

        Otherwise, Returns true if an expression exists in a given list of expressions. This is a
        shorthand for multiple OR conditions.

        If the testing set contains null, the result will be null if the element can not be found
        and true if it can be found. If the element is null, the result is always null.

        e.g. lit("42").in(1, 2, 3) leads to false.

        Example:
        ::

            >>> tab.where(col("a").in_(1, 2, 3))
            >>> table_a.where(col("x").in_(table_b.select("y")))
        """
        from pyflink.table import Table
        if isinstance(first_element_or_table, Table):
            assert len(remaining_elements) == 0
            return _binary_op("in")(self, first_element_or_table._j_table)
        else:
            gateway = get_gateway()
            ApiExpressionUtils = gateway.jvm.org.apache.flink.table.expressions.ApiExpressionUtils
            remaining_elements = (first_element_or_table, *remaining_elements)
            exprs = [ApiExpressionUtils.objectToExpression(_get_java_expression(e))
                     for e in remaining_elements]
            return _binary_op("in")(self, to_jarray(gateway.jvm.Object, exprs))

    def start(self):
        """
        Returns the start time (inclusive) of a window when applied on a window reference.

        Example:
        ::

            >>> tab.window(Tumble
            >>>         .over(row_interval(2))
            >>>         .on(col("a"))
            >>>         .alias("w")) \\
            >>>     .group_by(col("c"), col("w")) \\
            >>>     .select(col("c"), col("w").start(), col("w").end(), col("w").proctime())

        .. seealso:: :func:`~Expression.end`
        """
        return _unary_op("start")(self)

    def end(self):
        """
        Returns the end time (exclusive) of a window when applied on a window reference.

        e.g. if a window ends at 10:59:59.999 this property will return 11:00:00.000.

        Example:
        ::

            >>> orders.window(Tumble
            >>>             .over(row_interval(2))
            >>>             .on(col("a"))
            >>>             .alias("w")) \\
            >>>     .group_by(col("c"), col("w")) \\
            >>>     .select(col("c"), col("w").start(), col("w").end(), col("w").proctime())

        .. seealso:: :func:`~Expression.start`
        """
        return _unary_op("end")(self)

    def bin(self):
        """
        Returns a string representation of an integer numeric value in binary format. Returns null
        if numeric is null. E.g. "4" leads to "100", "12" leads to "1100".

        .. seealso:: :func:`~Expression.hex`
        """
        return _unary_op("bin")(self)

    def hex(self):
        """
        Returns a string representation of an integer numeric value or a string in hex format.
        Returns null if numeric or string is null.

        E.g. a numeric 20 leads to "14", a numeric 100 leads to "64", and a string "hello,world"
        leads to "68656c6c6f2c776f726c64".

        .. seealso:: :func:`~Expression.bin`
        """
        return _unary_op("hex")(self)

    def truncate(self, n=0):
        """
        Returns a number of truncated to n decimal places.
        If n is 0, the result has no decimal point or fractional part.
        n can be negative to cause n digits left of the decimal point of the value to become zero.
        E.g. truncate(42.345, 2) to 42.34, 42.truncate(-1) to 40
        """
        return _binary_op("truncate")(self, n)

    # ---------------------------- string functions ----------------------------------

    def substring(self, begin_index, length=None):
        """
        Creates a substring of the given string at given index for a given length.

        :param begin_index: first character of the substring (starting at 1, inclusive)
        :param length: number of characters of the substring
        """
        if length is None:
            return _binary_op("substring")(self, begin_index)
        else:
            return _ternary_op("substring")(self, begin_index, length)

    def trim_leading(self, character=None):
        """
        Removes leading space characters from the given string if character is None.
        Otherwise, removes leading specified characters from the given string.
        """
        if character is None:
            return _unary_op("trimLeading")(self)
        else:
            return _binary_op("trimLeading")(self, character)

    def trim_trailing(self, character=None):
        """
        Removes trailing space characters from the given string if character is None.
        Otherwise, removes trailing specified characters from the given string.
        """
        if character is None:
            return _unary_op("trimTrailing")(self)
        else:
            return _binary_op("trimTrailing")(self, character)

    def trim(self, character=None):
        """
        Removes leading and trailing space characters from the given string if character
        is None. Otherwise, removes leading and trailing specified characters from the given string.
        """
        if character is None:
            return _unary_op("trim")(self)
        else:
            return _binary_op("trim")(self, character)

    def replace(self, search, replacement):
        """
        Returns a new string which replaces all the occurrences of the search target
        with the replacement string (non-overlapping).
        """
        return _ternary_op("replace")(self, search, replacement)

    def char_length(self):
        """
        Returns the length of a string.
        """
        return _unary_op("charLength")(self)

    def upper_case(self):
        """
        Returns all of the characters in a string in upper case using the rules of the default
        locale.
        """
        return _unary_op("upperCase")(self)

    def lower_case(self):
        """
        Returns all of the characters in a string in lower case using the rules of the default
        locale.
        """
        return _unary_op("lowerCase")(self)

    def init_cap(self):
        """
        Converts the initial letter of each word in a string to uppercase. Assumes a
        string containing only [A-Za-z0-9], everything else is treated as whitespace.
        """
        return _unary_op("initCap")(self)

    def like(self, pattern):
        """
        Returns true, if a string matches the specified LIKE pattern.
        e.g. 'Jo_n%' matches all strings that start with 'Jo(arbitrary letter)n'
        """
        return _binary_op("like")(self, pattern)

    def similar(self, pattern):
        """
        Returns true, if a string matches the specified SQL regex pattern.
        e.g. 'A+' matches all strings that consist of at least one A
        """
        return _binary_op("similar")(self, pattern)

    def position(self, haystack):
        """
        Returns the position of string in an other string starting at 1.
        Returns 0 if string could not be found. e.g. lit('a').position('bbbbba') leads to 6.
        """
        return _binary_op("position")(self, haystack)

    def lpad(self, length, pad):
        """
        Returns a string left-padded with the given pad string to a length of len characters.
        If the string is longer than len, the return value is shortened to len characters.
        e.g. lit('hi').lpad(4, '??') returns '??hi', lit('hi').lpad(1, '??') returns 'h'
        """
        return _ternary_op("lpad")(self, length, pad)

    def rpad(self, length, pad):
        """
        Returns a string right-padded with the given pad string to a length of len characters.
        If the string is longer than len, the return value is shortened to len characters.
        e.g. lit('hi').rpad(4, '??') returns 'hi??', lit('hi').rpad(1, '??') returns 'h'
        """
        return _ternary_op("rpad")(self, length, pad)

    def overlay(self, new_string, starting, length=None):
        """
        Replaces a substring of string with a string starting at a position
        (starting at 1). e.g. lit('xxxxxtest').overlay('xxxx', 6) leads to 'xxxxxxxxx'
        lit('xxxxxtest').overlay('xxxx', 6, 2) leads to 'xxxxxxxxxst'
        """
        if length is None:
            return _ternary_op("overlay")(self, new_string, starting)
        else:
            j_expr_new_string = new_string._j_expr \
                if isinstance(new_string, Expression) else new_string
            j_expr_starting = starting._j_expr \
                if isinstance(starting, Expression) else starting
            j_expr_length = length._j_expr \
                if isinstance(length, Expression) else length
            return Expression(getattr(self._j_expr, "overlay")(
                j_expr_new_string, j_expr_starting, j_expr_length))

    def regexp_replace(self, regex, replacement):
        """
        Returns a string with all substrings that match the regular expression
        consecutively being replaced.
        """
        return _ternary_op("regexpReplace")(self, regex, replacement)

    def regexp_extract(self, regex, extract_index=None):
        """
        Returns a string extracted with a specified regular expression and a regex match
        group index.
        """
        if extract_index is None:
            return _ternary_op("regexpExtract")(self, regex)
        else:
            return _ternary_op("regexpExtract")(self, regex, extract_index)

    def from_base64(self):
        """
        Returns the base string decoded with base64.
        """
        return _unary_op("fromBase64")(self)

    def to_base64(self):
        """
        Returns the base64-encoded result of the input string.
        """
        return _unary_op("toBase64")(self)

    def ltrim(self):
        """
        Returns a string that removes the left whitespaces from the given string.
        """
        return _unary_op("ltrim")(self)

    def rtrim(self):
        """
        Returns a string that removes the right whitespaces from the given string.
        """
        return _unary_op("rtrim")(self)

    def repeat(self, n):
        """
        Returns a string that repeats the base string n times.
        """
        return _binary_op("repeat")(self, n)

    def over(self, alias):
        """
        Defines an aggregation to be used for a previously specified over window.

        Example:
        ::

            >>> tab.window(Over
            >>>         .partition_by(col('c'))
            >>>         .order_by(col('rowtime'))
            >>>         .preceding(row_interval(2))
            >>>         .following(CURRENT_ROW)
            >>>         .alias("w")) \\
            >>>     .select(col('c'), col('a'), col('a').count().over(col('w')))
        """
        return _binary_op("over")(self, alias)

    # ---------------------------- temporal functions ----------------------------------

    def to_date(self):
        """
        Parses a date string in the form "yyyy-MM-dd" to a SQL Date.
        """
        return _unary_op("toDate")(self)

    def to_time(self):
        """
        Parses a time string in the form "HH:mm:ss" to a SQL Time.
        """
        return _unary_op("toTime")(self)

    def to_timestamp(self):
        """
        Parses a timestamp string in the form "yyyy-MM-dd HH:mm:ss[.SSS]" to a SQL Timestamp.
        """
        return _unary_op("toTimestamp")(self)

    def extract(self, time_interval_unit: TimeIntervalUnit):
        """
        Extracts parts of a time point or time interval. Returns the part as a long value.
        e.g. `lit("2006-06-05").to_date().extract(TimeIntervalUnit.DAY)` leads to `5`.
        """
        return _binary_op("extract")(
            self, TimeIntervalUnit._to_j_time_interval_unit(time_interval_unit))

    def floor(self, time_interval_unit: TimeIntervalUnit = None):
        """
        If time_interval_unit is specified, it rounds down a time point to the given
        unit, e.g. `lit("12:44:31").to_date().floor(TimeIntervalUnit.MINUTE)` leads to
        `12:44:00`. Otherwise, it calculates the largest integer less than or equal to a
        given number.
        """
        if time_interval_unit is None:
            return _unary_op("floor")(self)
        else:
            return _binary_op("floor")(
                self, TimeIntervalUnit._to_j_time_interval_unit(time_interval_unit))

    def ceil(self, time_interval_unit: TimeIntervalUnit = None):
        """
        If time_interval_unit is specified, it rounds up a time point to the given unit,
        e.g. `lit("12:44:31").to_date().floor(TimeIntervalUnit.MINUTE)` leads to 12:45:00.
        Otherwise, it calculates the smallest integer greater than or equal to a given number.
        """
        if time_interval_unit is None:
            return _unary_op("ceil")(self)
        else:
            return _binary_op("ceil")(
                self, TimeIntervalUnit._to_j_time_interval_unit(time_interval_unit))

    # ---------------------------- advanced type helper functions -----------------------------

    def get(self, name_or_index: Union[str, int]):
        """
        Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by name or index
        and returns it's value.

        :param name_or_index: name or index of the field (similar to Flink's field expressions)

        .. seealso:: :func:`~Expression.flatten`
        """
        return _binary_op("get")(self, name_or_index)

    def flatten(self):
        """
        Converts a Flink composite type (such as Tuple, POJO, etc.) and all of its direct subtypes
        into a flat representation where every subtype is a separate field.

        .. seealso:: :func:`~Expression.get`
        """
        return _unary_op("flatten")(self)

    def at(self, index):
        """
        Accesses the element of an array or map based on a key or an index (starting at 1).

        :param index: index key or position of the element (array index starting at 1)

        .. seealso:: :func:`~Expression.cardinality`, :func:`~Expression.element`
        """
        return _binary_op("at")(self, index)

    def cardinality(self):
        """
        Returns the number of elements of an array or number of entries of a map.

        .. seealso:: :func:`~Expression.at`, :func:`~Expression.element`
        """
        return _unary_op("cardinality")(self)

    def element(self):
        """
        Returns the sole element of an array with a single element. Returns null if the array is
        empty. Throws an exception if the array has more than one element.

        .. seealso:: :func:`~Expression.at`, :func:`~Expression.cardinality`
        """
        return _unary_op("element")(self)

    # ---------------------------- time definition functions -----------------------------

    def rowtime(self):
        """
        Declares a field as the rowtime attribute for indicating, accessing, and working in
        Flink's event time.

        .. seealso:: :func:`~Expression.proctime`
        """
        return _unary_op("rowtime")(self)

    def proctime(self):
        """
        Declares a field as the proctime attribute for indicating, accessing, and working in
        Flink's processing time.

        .. seealso:: :func:`~Expression.rowtime`
        """
        return _unary_op("proctime")(self)

    def year(self):
        return _unary_op("year")(self)

    def years(self):
        return _unary_op("years")(self)

    def quarter(self):
        return _unary_op("quarter")(self)

    def quarters(self):
        return _unary_op("quarters")(self)

    def month(self):
        return _unary_op("month")(self)

    def months(self):
        return _unary_op("months")(self)

    def week(self):
        return _unary_op("week")(self)

    def weeks(self):
        return _unary_op("weeks")(self)

    def day(self):
        return _unary_op("day")(self)

    def days(self):
        return _unary_op("days")(self)

    def hour(self):
        return _unary_op("hour")(self)

    def hours(self):
        return _unary_op("hours")(self)

    def minute(self):
        return _unary_op("minute")(self)

    def minutes(self):
        return _unary_op("minutes")(self)

    def second(self):
        return _unary_op("second")(self)

    def seconds(self):
        return _unary_op("seconds")(self)

    def milli(self):
        return _unary_op("milli")(self)

    def millis(self):
        return _unary_op("millis")(self)

    # ---------------------------- hash functions -----------------------------

    def md5(self):
        return _unary_op("md5")(self)

    def sha1(self):
        return _unary_op("sha1")(self)

    def sha224(self):
        return _unary_op("sha224")(self)

    def sha256(self):
        return _unary_op("sha256")(self)

    def sha384(self):
        return _unary_op("sha384")(self)

    def sha512(self):
        return _unary_op("sha512")(self)

    def sha2(self, hash_length):
        """
        Returns the hash for the given string expression using the SHA-2 family of hash
        functions (SHA-224, SHA-256, SHA-384, or SHA-512).

        :param hash_length: bit length of the result (either 224, 256, 384, or 512)
        :return: string or null if one of the arguments is null.

        .. seealso:: :func:`~Expression.md5`, :func:`~Expression.sha1`, :func:`~Expression.sha224`,
                     :func:`~Expression.sha256`, :func:`~Expression.sha384`,
                     :func:`~Expression.sha512`
        """
        return _binary_op("sha2")(self, hash_length)


# add the docs
_make_math_log_doc()
_make_math_trigonometric_doc()
_make_aggregation_doc()
_make_string_doc()
_make_temporal_doc()
_make_time_doc()
_make_hash_doc()

# add the version docs
_add_version_doc()
