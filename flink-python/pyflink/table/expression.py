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
from typing import Union, TypeVar, Generic

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
    >>>             col("b").sum.alias("d"),
    >>>             col("b").sum0.alias("e"),
    >>>             col("b").min.alias("f"),
    >>>             col("b").max.alias("g"),
    >>>             col("b").count.alias("h"),
    >>>             col("b").avg.alias("i"),
    >>>             col("b").stddev_pop.alias("j"),
    >>>             col("b").stddev_samp.alias("k"),
    >>>             col("b").var_pop.alias("l"),
    >>>             col("b").var_samp.alias("m"),
    >>>             col("b").collect.alias("n"))

.. seealso:: :py:attr:`~Expression.sum`, :py:attr:`~Expression.sum0`, :py:attr:`~Expression.min`,
             :py:attr:`~Expression.max`, :py:attr:`~Expression.count`, :py:attr:`~Expression.avg`,
             :py:attr:`~Expression.stddev_pop`, :py:attr:`~Expression.stddev_samp`,
             :py:attr:`~Expression.var_pop`, :py:attr:`~Expression.var_samp`,
             :py:attr:`~Expression.collect`
"""

_math_log_doc = """
{op_desc}

.. seealso:: :py:attr:`~Expression.log10`, :py:attr:`~Expression.log2`, :py:attr:`~Expression.ln`,
             :func:`~Expression.log`
"""

_math_trigonometric_doc = """
Calculates the {op_desc} of a given number.

.. seealso:: :py:attr:`~Expression.sin`, :py:attr:`~Expression.cos`, :py:attr:`~Expression.sinh`,
             :py:attr:`~Expression.cosh`, :py:attr:`~Expression.tan`, :py:attr:`~Expression.cot`,
             :py:attr:`~Expression.asin`, :py:attr:`~Expression.acos`, :py:attr:`~Expression.atan`,
             :py:attr:`~Expression.tanh`
"""

_string_doc_seealso = """
.. seealso:: :func:`~Expression.trim_leading`, :func:`~Expression.trim_trailing`,
             :func:`~Expression.trim`, :func:`~Expression.replace`,
             :py:attr:`~Expression.char_length`, :py:attr:`~Expression.upper_case`,
             :py:attr:`~Expression.lower_case`, :py:attr:`~Expression.init_cap`,
             :func:`~Expression.like`, :func:`~Expression.similar`,
             :func:`~Expression.position`, :func:`~Expression.lpad`, :func:`~Expression.rpad`,
             :func:`~Expression.overlay`, :func:`~Expression.regexp_replace`,
             :func:`~Expression.regexp_extract`, :func:`~Expression.substring`,
             :py:attr:`~Expression.from_base64`, :py:attr:`~Expression.to_base64`,
             :py:attr:`~Expression.ltrim`, :py:attr:`~Expression.rtrim`, :func:`~Expression.repeat`
"""

_temporal_doc_seealso = """
.. seealso:: :py:attr:`~Expression.to_date`, :py:attr:`~Expression.to_time`,
             :py:attr:`~Expression.to_timestamp`, :func:`~Expression.extract`,
             :func:`~Expression.floor`, :func:`~Expression.ceil`
"""

_time_doc = """
Creates an interval of the given number of {op_desc}.

The produced expression is of type :func:`~DataTypes.INTERVAL`.

.. seealso:: :py:attr:`~Expression.year`, :py:attr:`~Expression.years`,
             :py:attr:`~Expression.quarter`, :py:attr:`~Expression.quarters`,
             :py:attr:`~Expression.month`, :py:attr:`~Expression.months`,
             :py:attr:`~Expression.week`, :py:attr:`~Expression.weeks`, :py:attr:`~Expression.day`,
             :py:attr:`~Expression.days`, :py:attr:`~Expression.hour`, :py:attr:`~Expression.hours`,
             :py:attr:`~Expression.minute`, :py:attr:`~Expression.minutes`,
             :py:attr:`~Expression.second`, :py:attr:`~Expression.seconds`,
             :py:attr:`~Expression.milli`, :py:attr:`~Expression.millis`
"""

_hash_doc = """
Returns the {op_desc} hash of the string argument; null if string is null.

:return: string of {bit} hexadecimal digits or null.

.. seealso:: :py:attr:`~Expression.md5`, :py:attr:`~Expression.sha1`, :py:attr:`~Expression.sha224`,
             :py:attr:`~Expression.sha256`, :py:attr:`~Expression.sha384`,
             :py:attr:`~Expression.sha512`, :py:attr:`~Expression.sha2`
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
                               "square root of var_pop).",
        Expression.stddev_samp: "Returns the sample standard deviation of an expression(the square "
                                "root of var_samp).",
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


def _get_java_expression(expr, to_expr: bool = False):
    """
    Returns the Java expression for the given expr. If expr is a Python expression, returns the
    underlying Java expression, otherwise, convert it to a Java expression if to_expr is true.
    """
    if isinstance(expr, Expression):
        return expr._j_expr
    elif to_expr:
        gateway = get_gateway()
        return gateway.jvm.Expressions.lit(expr)
    else:
        return expr


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
    def _(self) -> 'Expression':
        return Expression(getattr(self._j_expr, op_name)())

    return _


def _binary_op(op_name: str, reverse: bool = False):
    def _(self, other) -> 'Expression':
        if reverse:
            return Expression(getattr(_get_java_expression(other, True), op_name)(self._j_expr))
        else:
            return Expression(getattr(self._j_expr, op_name)(_get_java_expression(other)))

    return _


def _ternary_op(op_name: str):
    def _(self, first, second) -> 'Expression':
        return Expression(getattr(self._j_expr, op_name)(
            _get_java_expression(first), _get_java_expression(second)))

    return _


def _expressions_op(op_name: str):
    def _(self, *args) -> 'Expression':
        from pyflink.table import expressions
        return getattr(expressions, op_name)(self, *[_get_java_expression(arg) for arg in args])

    return _


class TimeIntervalUnit(Enum):
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

    def _to_j_time_interval_unit(self):
        gateway = get_gateway()
        JTimeIntervalUnit = gateway.jvm.org.apache.flink.table.expressions.TimeIntervalUnit
        return getattr(JTimeIntervalUnit, self.name)


class TimePointUnit(Enum):
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

    def _to_j_time_point_unit(self):
        gateway = get_gateway()
        JTimePointUnit = gateway.jvm.org.apache.flink.table.expressions.TimePointUnit
        return getattr(JTimePointUnit, self.name)


T = TypeVar('T')


class Expression(Generic[T]):
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

    __radd__ = _binary_op("plus", True)
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

    @property
    def exp(self) -> 'Expression[float]':
        """
        Calculates the Euler's number raised to the given power.
        """
        return _unary_op("exp")(self)

    @property
    def log10(self) -> 'Expression[float]':
        return _unary_op("log10")(self)

    @property
    def log2(self) -> 'Expression[float]':
        return _unary_op("log2")(self)

    @property
    def ln(self) -> 'Expression[float]':
        return _unary_op("ln")(self)

    def log(self, base=None) -> 'Expression[float]':
        if base is None:
            return _unary_op("log")(self)
        else:
            return _binary_op("log")(self, base)

    @property
    def cosh(self) -> 'Expression[float]':
        return _unary_op("cosh")(self)

    @property
    def sinh(self) -> 'Expression[float]':
        return _unary_op("sinh")(self)

    @property
    def sin(self) -> 'Expression[float]':
        return _unary_op("sin")(self)

    @property
    def cos(self) -> 'Expression[float]':
        return _unary_op("cos")(self)

    @property
    def tan(self) -> 'Expression[float]':
        return _unary_op("tan")(self)

    @property
    def cot(self) -> 'Expression[float]':
        return _unary_op("cot")(self)

    @property
    def asin(self) -> 'Expression[float]':
        return _unary_op("asin")(self)

    @property
    def acos(self) -> 'Expression[float]':
        return _unary_op("acos")(self)

    @property
    def atan(self) -> 'Expression[float]':
        return _unary_op("atan")(self)

    @property
    def tanh(self) -> 'Expression[float]':
        return _unary_op("tanh")(self)

    @property
    def degrees(self) -> 'Expression[float]':
        """
        Converts numeric from radians to degrees.

        .. seealso:: :py:attr:`~Expression.radians`
        """
        return _unary_op("degrees")(self)

    @property
    def radians(self) -> 'Expression[float]':
        """
        Converts numeric from degrees to radians.

        .. seealso:: :py:attr:`~Expression.degrees`
        """
        return _unary_op("radians")(self)

    @property
    def sqrt(self) -> 'Expression[float]':
        """
        Calculates the square root of a given value.
        """
        return _unary_op("sqrt")(self)

    @property
    def abs(self) -> 'Expression[T]':
        """
        Calculates the absolute value of given value.
        """
        return _unary_op("abs")(self)

    @property
    def sign(self) -> 'Expression[T]':
        """
        Calculates the signum of a given number.
        """
        return _unary_op("sign")(self)

    def round(self, places: Union[int, 'Expression[int]']):
        """
        Rounds the given number to integer places right to the decimal point.
        """
        return _binary_op("round")(self, places)

    def between(self, lower_bound, upper_bound) -> 'Expression[bool]':
        """
        Returns true if the given expression is between lower_bound and upper_bound
        (both inclusive). False otherwise. The parameters must be numeric types or identical
        comparable types.

        :param lower_bound: numeric or comparable expression
        :param upper_bound: numeric or comparable expression

        .. seealso:: :func:`~Expression.not_between`
        """
        return _ternary_op("between")(self, lower_bound, upper_bound)

    def not_between(self, lower_bound, upper_bound) -> 'Expression[bool]':
        """
        Returns true if the given expression is not between lower_bound and upper_bound
        (both inclusive). False otherwise. The parameters must be numeric types or identical
        comparable types.

        :param lower_bound: numeric or comparable expression
        :param upper_bound: numeric or comparable expression

        .. seealso:: :func:`~Expression.between`
        """
        return _ternary_op("notBetween")(self, lower_bound, upper_bound)

    def then(self, if_true, if_false) -> 'Expression':
        """
        Ternary conditional operator that decides which of two other expressions should be evaluated
        based on a evaluated boolean condition.

        e.g. lit(42).is_greater(5).then("A", "B") leads to "A"

        :param if_true: expression to be evaluated if condition holds
        :param if_false: expression to be evaluated if condition does not hold
        """
        return _ternary_op("then")(self, if_true, if_false)

    @property
    def is_null(self) -> 'Expression[bool]':
        """
        Returns true if the given expression is null.

        .. seealso:: :py:attr:`~Expression.is_not_null`
        """
        return _unary_op("isNull")(self)

    @property
    def is_not_null(self) -> 'Expression[bool]':
        """
        Returns true if the given expression is not null.

        .. seealso:: :py:attr:`~Expression.is_null`
        """
        return _unary_op("isNotNull")(self)

    @property
    def is_true(self) -> 'Expression[bool]':
        """
        Returns true if given boolean expression is true. False otherwise (for null and false).

        .. seealso:: :py:attr:`~Expression.is_false`, :py:attr:`~Expression.is_not_true`,
                     :py:attr:`~Expression.is_not_false`
        """
        return _unary_op("isTrue")(self)

    @property
    def is_false(self) -> 'Expression[bool]':
        """
        Returns true if given boolean expression is false. False otherwise (for null and true).

        .. seealso:: :py:attr:`~Expression.is_true`, :py:attr:`~Expression.is_not_true`,
                     :py:attr:`~Expression.is_not_false`
        """
        return _unary_op("isFalse")(self)

    @property
    def is_not_true(self) -> 'Expression[bool]':
        """
        Returns true if given boolean expression is not true (for null and false). False otherwise.

        .. seealso:: :py:attr:`~Expression.is_true`, :py:attr:`~Expression.is_false`,
                     :py:attr:`~Expression.is_not_false`
        """
        return _unary_op("isNotTrue")(self)

    @property
    def is_not_false(self) -> 'Expression[bool]':
        """
        Returns true if given boolean expression is not false (for null and true). False otherwise.

        .. seealso:: :py:attr:`~Expression.is_true`, :py:attr:`~Expression.is_false`,
                     :py:attr:`~Expression.is_not_true`
        """
        return _unary_op("isNotFalse")(self)

    @property
    def distinct(self) -> 'Expression':
        """
        Similar to a SQL distinct aggregation clause such as COUNT(DISTINCT a), declares that an
        aggregation function is only applied on distinct input values.

        Example:
        ::

            >>> tab \\
            >>>     .group_by(col("a")) \\
            >>>     .select(col("a"), col("b").sum.distinct.alias("d"))
        """
        return _unary_op("distinct")(self)

    @property
    def sum(self) -> 'Expression':
        return _unary_op("sum")(self)

    @property
    def sum0(self) -> 'Expression':
        return _unary_op("sum0")(self)

    @property
    def min(self) -> 'Expression':
        return _unary_op("min")(self)

    @property
    def max(self) -> 'Expression':
        return _unary_op("max")(self)

    @property
    def count(self) -> 'Expression':
        return _unary_op("count")(self)

    @property
    def avg(self) -> 'Expression':
        return _unary_op("avg")(self)

    @property
    def stddev_pop(self) -> 'Expression':
        return _unary_op("stddevPop")(self)

    @property
    def stddev_samp(self) -> 'Expression':
        return _unary_op("stddevSamp")(self)

    @property
    def var_pop(self) -> 'Expression':
        return _unary_op("varPop")(self)

    @property
    def var_samp(self) -> 'Expression':
        return _unary_op("varSamp")(self)

    @property
    def collect(self) -> 'Expression':
        return _unary_op("collect")(self)

    def alias(self, name: str, *extra_names: str) -> 'Expression[T]':
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

    def cast(self, data_type: DataType) -> 'Expression':
        """
        Converts a value to a given data type.

        e.g. lit("42").cast(DataTypes.INT()) leads to 42.
        """
        return _binary_op("cast")(self, _to_java_data_type(data_type))

    @property
    def asc(self) -> 'Expression':
        """
        Specifies ascending order of an expression i.e. a field for order_by.

        Example:
        ::

            >>> tab.order_by(col('a').asc)

        .. seealso:: :py:attr:`~Expression.desc`
        """
        return _unary_op("asc")(self)

    @property
    def desc(self) -> 'Expression':
        """
        Specifies descending order of an expression i.e. a field for order_by.

        Example:
        ::

            >>> tab.order_by(col('a').desc)

        .. seealso:: :py:attr:`~Expression.asc`
        """
        return _unary_op("desc")(self)

    def in_(self, first_element_or_table, *remaining_elements) -> 'Expression':
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

    @property
    def start(self) -> 'Expression':
        """
        Returns the start time (inclusive) of a window when applied on a window reference.

        Example:
        ::

            >>> tab.window(Tumble
            >>>         .over(row_interval(2))
            >>>         .on(col("a"))
            >>>         .alias("w")) \\
            >>>     .group_by(col("c"), col("w")) \\
            >>>     .select(col("c"), col("w").start, col("w").end, col("w").proctime)

        .. seealso:: :py:attr:`~Expression.end`
        """
        return _unary_op("start")(self)

    @property
    def end(self) -> 'Expression':
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
            >>>     .select(col("c"), col("w").start, col("w").end, col("w").proctime)

        .. seealso:: :py:attr:`~Expression.start`
        """
        return _unary_op("end")(self)

    @property
    def bin(self) -> 'Expression[str]':
        """
        Returns a string representation of an integer numeric value in binary format. Returns null
        if numeric is null. E.g. "4" leads to "100", "12" leads to "1100".

        .. seealso:: :py:attr:`~Expression.hex`
        """
        return _unary_op("bin")(self)

    @property
    def hex(self) -> 'Expression[str]':
        """
        Returns a string representation of an integer numeric value or a string in hex format.
        Returns null if numeric or string is null.

        E.g. a numeric 20 leads to "14", a numeric 100 leads to "64", and a string "hello,world"
        leads to "68656c6c6f2c776f726c64".

        .. seealso:: :py:attr:`~Expression.bin`
        """
        return _unary_op("hex")(self)

    def truncate(self, n: Union[int, 'Expression[int]'] = 0) -> 'Expression[T]':
        """
        Returns a number of truncated to n decimal places.
        If n is 0, the result has no decimal point or fractional part.
        n can be negative to cause n digits left of the decimal point of the value to become zero.
        E.g. truncate(42.345, 2) to 42.34, 42.truncate(-1) to 40
        """
        return _binary_op("truncate")(self, n)

    # ---------------------------- string functions ----------------------------------

    def substring(self,
                  begin_index: Union[int, 'Expression[int]'],
                  length: Union[int, 'Expression[int]'] = None) -> 'Expression[str]':
        """
        Creates a substring of the given string at given index for a given length.

        :param begin_index: first character of the substring (starting at 1, inclusive)
        :param length: number of characters of the substring
        """
        if length is None:
            return _binary_op("substring")(self, begin_index)
        else:
            return _ternary_op("substring")(self, begin_index, length)

    def trim_leading(self, character: Union[str, 'Expression[str]'] = None) -> 'Expression[str]':
        """
        Removes leading space characters from the given string if character is None.
        Otherwise, removes leading specified characters from the given string.
        """
        if character is None:
            return _unary_op("trimLeading")(self)
        else:
            return _binary_op("trimLeading")(self, character)

    def trim_trailing(self, character: Union[str, 'Expression[str]'] = None) -> 'Expression[str]':
        """
        Removes trailing space characters from the given string if character is None.
        Otherwise, removes trailing specified characters from the given string.
        """
        if character is None:
            return _unary_op("trimTrailing")(self)
        else:
            return _binary_op("trimTrailing")(self, character)

    def trim(self, character: Union[str, 'Expression[str]'] = None) -> 'Expression[str]':
        """
        Removes leading and trailing space characters from the given string if character
        is None. Otherwise, removes leading and trailing specified characters from the given string.
        """
        if character is None:
            return _unary_op("trim")(self)
        else:
            return _binary_op("trim")(self, character)

    def replace(self,
                search: Union[str, 'Expression[str]'] = None,
                replacement: Union[str, 'Expression[str]'] = None) -> 'Expression[str]':
        """
        Returns a new string which replaces all the occurrences of the search target
        with the replacement string (non-overlapping).
        """
        return _ternary_op("replace")(self, search, replacement)

    @property
    def char_length(self) -> 'Expression[int]':
        """
        Returns the length of a string.
        """
        return _unary_op("charLength")(self)

    @property
    def upper_case(self) -> 'Expression[str]':
        """
        Returns all of the characters in a string in upper case using the rules of the default
        locale.
        """
        return _unary_op("upperCase")(self)

    @property
    def lower_case(self) -> 'Expression[str]':
        """
        Returns all of the characters in a string in lower case using the rules of the default
        locale.
        """
        return _unary_op("lowerCase")(self)

    @property
    def init_cap(self) -> 'Expression[str]':
        """
        Converts the initial letter of each word in a string to uppercase. Assumes a
        string containing only [A-Za-z0-9], everything else is treated as whitespace.
        """
        return _unary_op("initCap")(self)

    def like(self, pattern: Union[str, 'Expression[str]'] = None) -> 'Expression[bool]':
        """
        Returns true, if a string matches the specified LIKE pattern.
        e.g. 'Jo_n%' matches all strings that start with 'Jo(arbitrary letter)n'
        """
        return _binary_op("like")(self, pattern)

    def similar(self, pattern: Union[str, 'Expression[str]'] = None) -> 'Expression[bool]':
        """
        Returns true, if a string matches the specified SQL regex pattern.
        e.g. 'A+' matches all strings that consist of at least one A
        """
        return _binary_op("similar")(self, pattern)

    def position(self, haystack: Union[str, 'Expression[str]'] = None) -> 'Expression[int]':
        """
        Returns the position of string in an other string starting at 1.
        Returns 0 if string could not be found. e.g. lit('a').position('bbbbba') leads to 6.
        """
        return _binary_op("position")(self, haystack)

    def lpad(self,
             length: Union[int, 'Expression[int]'],
             pad: Union[str, 'Expression[str]']) -> 'Expression[str]':
        """
        Returns a string left-padded with the given pad string to a length of len characters.
        If the string is longer than len, the return value is shortened to len characters.
        e.g. lit('hi').lpad(4, '??') returns '??hi', lit('hi').lpad(1, '??') returns 'h'
        """
        return _ternary_op("lpad")(self, length, pad)

    def rpad(self,
             length: Union[int, 'Expression[int]'],
             pad: Union[str, 'Expression[str]']) -> 'Expression[str]':
        """
        Returns a string right-padded with the given pad string to a length of len characters.
        If the string is longer than len, the return value is shortened to len characters.
        e.g. lit('hi').rpad(4, '??') returns 'hi??', lit('hi').rpad(1, '??') returns 'h'
        """
        return _ternary_op("rpad")(self, length, pad)

    def overlay(self,
                new_string: Union[str, 'Expression[str]'],
                starting: Union[int, 'Expression[int]'],
                length: Union[int, 'Expression[int]'] = None) -> 'Expression[str]':
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

    def regexp_replace(self,
                       regex: Union[str, 'Expression[str]'],
                       replacement: Union[str, 'Expression[str]']) -> 'Expression[str]':
        """
        Returns a string with all substrings that match the regular expression
        consecutively being replaced.
        """
        return _ternary_op("regexpReplace")(self, regex, replacement)

    def regexp_extract(
            self,
            regex: Union[str, 'Expression[str]'],
            extract_index: Union[int, 'Expression[int]'] = None) -> 'Expression[str]':
        """
        Returns a string extracted with a specified regular expression and a regex match
        group index.
        """
        if extract_index is None:
            return _ternary_op("regexpExtract")(self, regex)
        else:
            return _ternary_op("regexpExtract")(self, regex, extract_index)

    @property
    def from_base64(self) -> 'Expression[str]':
        """
        Returns the base string decoded with base64.
        """
        return _unary_op("fromBase64")(self)

    @property
    def to_base64(self) -> 'Expression[str]':
        """
        Returns the base64-encoded result of the input string.
        """
        return _unary_op("toBase64")(self)

    @property
    def ltrim(self) -> 'Expression[str]':
        """
        Returns a string that removes the left whitespaces from the given string.
        """
        return _unary_op("ltrim")(self)

    @property
    def rtrim(self) -> 'Expression[str]':
        """
        Returns a string that removes the right whitespaces from the given string.
        """
        return _unary_op("rtrim")(self)

    def repeat(self, n: Union[int, 'Expression[int]']) -> 'Expression[str]':
        """
        Returns a string that repeats the base string n times.
        """
        return _binary_op("repeat")(self, n)

    def over(self, alias) -> 'Expression':
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
            >>>     .select(col('c'), col('a'), col('a').count.over(col('w')))
        """
        return _binary_op("over")(self, alias)

    # ---------------------------- temporal functions ----------------------------------

    @property
    def to_date(self) -> 'Expression':
        """
        Parses a date string in the form "yyyy-MM-dd" to a SQL Date.
        """
        return _unary_op("toDate")(self)

    @property
    def to_time(self) -> 'Expression':
        """
        Parses a time string in the form "HH:mm:ss" to a SQL Time.
        """
        return _unary_op("toTime")(self)

    @property
    def to_timestamp(self) -> 'Expression':
        """
        Parses a timestamp string in the form "yyyy-MM-dd HH:mm:ss[.SSS]" to a SQL Timestamp.
        """
        return _unary_op("toTimestamp")(self)

    def extract(self, time_interval_unit: TimeIntervalUnit) -> 'Expression':
        """
        Extracts parts of a time point or time interval. Returns the part as a long value.
        e.g. `lit("2006-06-05").to_date.extract(TimeIntervalUnit.DAY)` leads to `5`.
        """
        return _binary_op("extract")(
            self, time_interval_unit._to_j_time_interval_unit())

    def floor(self, time_interval_unit: TimeIntervalUnit = None) -> 'Expression':
        """
        If time_interval_unit is specified, it rounds down a time point to the given
        unit, e.g. `lit("12:44:31").to_date.floor(TimeIntervalUnit.MINUTE)` leads to
        `12:44:00`. Otherwise, it calculates the largest integer less than or equal to a
        given number.
        """
        if time_interval_unit is None:
            return _unary_op("floor")(self)
        else:
            return _binary_op("floor")(
                self, time_interval_unit._to_j_time_interval_unit())

    def ceil(self, time_interval_unit: TimeIntervalUnit = None) -> 'Expression':
        """
        If time_interval_unit is specified, it rounds up a time point to the given unit,
        e.g. `lit("12:44:31").to_date.floor(TimeIntervalUnit.MINUTE)` leads to 12:45:00.
        Otherwise, it calculates the smallest integer greater than or equal to a given number.
        """
        if time_interval_unit is None:
            return _unary_op("ceil")(self)
        else:
            return _binary_op("ceil")(
                self, time_interval_unit._to_j_time_interval_unit())

    # ---------------------------- advanced type helper functions -----------------------------

    def get(self, name_or_index: Union[str, int]) -> 'Expression':
        """
        Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by name or index
        and returns it's value.

        :param name_or_index: name or index of the field (similar to Flink's field expressions)

        .. seealso:: :py:attr:`~Expression.flatten`
        """
        return _binary_op("get")(self, name_or_index)

    @property
    def flatten(self) -> 'Expression':
        """
        Converts a Flink composite type (such as Tuple, POJO, etc.) and all of its direct subtypes
        into a flat representation where every subtype is a separate field.

        .. seealso:: :func:`~Expression.get`
        """
        return _unary_op("flatten")(self)

    def at(self, index) -> 'Expression':
        """
        Accesses the element of an array or map based on a key or an index (starting at 1).

        :param index: index key or position of the element (array index starting at 1)

        .. seealso:: :py:attr:`~Expression.cardinality`, :py:attr:`~Expression.element`
        """
        return _binary_op("at")(self, index)

    @property
    def cardinality(self) -> 'Expression':
        """
        Returns the number of elements of an array or number of entries of a map.

        .. seealso:: :func:`~Expression.at`, :py:attr:`~Expression.element`
        """
        return _unary_op("cardinality")(self)

    @property
    def element(self) -> 'Expression':
        """
        Returns the sole element of an array with a single element. Returns null if the array is
        empty. Throws an exception if the array has more than one element.

        .. seealso:: :func:`~Expression.at`, :py:attr:`~Expression.cardinality`
        """
        return _unary_op("element")(self)

    # ---------------------------- time definition functions -----------------------------

    @property
    def rowtime(self) -> 'Expression':
        """
        Declares a field as the rowtime attribute for indicating, accessing, and working in
        Flink's event time.

        .. seealso:: :py:attr:`~Expression.proctime`
        """
        return _unary_op("rowtime")(self)

    @property
    def proctime(self) -> 'Expression':
        """
        Declares a field as the proctime attribute for indicating, accessing, and working in
        Flink's processing time.

        .. seealso:: :py:attr:`~Expression.rowtime`
        """
        return _unary_op("proctime")(self)

    @property
    def year(self) -> 'Expression':
        return _unary_op("year")(self)

    @property
    def years(self) -> 'Expression':
        return _unary_op("years")(self)

    @property
    def quarter(self) -> 'Expression':
        return _unary_op("quarter")(self)

    @property
    def quarters(self) -> 'Expression':
        return _unary_op("quarters")(self)

    @property
    def month(self) -> 'Expression':
        return _unary_op("month")(self)

    @property
    def months(self) -> 'Expression':
        return _unary_op("months")(self)

    @property
    def week(self) -> 'Expression':
        return _unary_op("week")(self)

    @property
    def weeks(self) -> 'Expression':
        return _unary_op("weeks")(self)

    @property
    def day(self) -> 'Expression':
        return _unary_op("day")(self)

    @property
    def days(self) -> 'Expression':
        return _unary_op("days")(self)

    @property
    def hour(self) -> 'Expression':
        return _unary_op("hour")(self)

    @property
    def hours(self) -> 'Expression':
        return _unary_op("hours")(self)

    @property
    def minute(self) -> 'Expression':
        return _unary_op("minute")(self)

    @property
    def minutes(self) -> 'Expression':
        return _unary_op("minutes")(self)

    @property
    def second(self) -> 'Expression':
        return _unary_op("second")(self)

    @property
    def seconds(self) -> 'Expression':
        return _unary_op("seconds")(self)

    @property
    def milli(self) -> 'Expression':
        return _unary_op("milli")(self)

    @property
    def millis(self) -> 'Expression':
        return _unary_op("millis")(self)

    # ---------------------------- hash functions -----------------------------

    @property
    def md5(self) -> 'Expression[str]':
        return _unary_op("md5")(self)

    @property
    def sha1(self) -> 'Expression[str]':
        return _unary_op("sha1")(self)

    @property
    def sha224(self) -> 'Expression[str]':
        return _unary_op("sha224")(self)

    @property
    def sha256(self) -> 'Expression[str]':
        return _unary_op("sha256")(self)

    @property
    def sha384(self) -> 'Expression[str]':
        return _unary_op("sha384")(self)

    @property
    def sha512(self) -> 'Expression[str]':
        return _unary_op("sha512")(self)

    def sha2(self, hash_length: Union[int, 'Expression[int]']) -> 'Expression[str]':
        """
        Returns the hash for the given string expression using the SHA-2 family of hash
        functions (SHA-224, SHA-256, SHA-384, or SHA-512).

        :param hash_length: bit length of the result (either 224, 256, 384, or 512)
        :return: string or null if one of the arguments is null.

        .. seealso:: :py:attr:`~Expression.md5`, :py:attr:`~Expression.sha1`,
                     :py:attr:`~Expression.sha224`, :py:attr:`~Expression.sha256`,
                     :py:attr:`~Expression.sha384`, :py:attr:`~Expression.sha512`
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
