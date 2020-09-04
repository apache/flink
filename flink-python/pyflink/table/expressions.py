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
from pyflink.table.expression import Expression, _get_java_expression, TimePointUnit
from pyflink.table.types import _to_java_data_type, DataType, _to_java_type
from pyflink.table.udf import UserDefinedFunctionWrapper, UserDefinedTableFunctionWrapper
from pyflink.util.utils import to_jarray, load_java_class

__all__ = ['if_then_else', 'lit', 'col', 'range_', 'and_', 'or_', 'UNBOUNDED_ROW',
           'UNBOUNDED_RANGE', 'CURRENT_ROW', 'CURRENT_RANGE', 'current_date', 'current_time',
           'current_timestamp', 'local_time', 'local_timestamp', 'temporal_overlaps',
           'date_format', 'timestamp_diff', 'array', 'row', 'map_', 'row_interval', 'pi', 'e',
           'rand', 'rand_integer', 'atan2', 'negative', 'concat', 'concat_ws', 'uuid', 'null_of',
           'log', 'with_columns', 'without_columns', 'call']


def _leaf_op(op_name: str):
    gateway = get_gateway()
    return Expression(getattr(gateway.jvm.Expressions, op_name)())


def _unary_op(op_name: str, arg):
    gateway = get_gateway()
    return Expression(getattr(gateway.jvm.Expressions, op_name)(_get_java_expression(arg)))


def _binary_op(op_name: str, first, second):
    gateway = get_gateway()
    return Expression(getattr(gateway.jvm.Expressions, op_name)(
        _get_java_expression(first),
        _get_java_expression(second)))


def _ternary_op(op_name: str, first, second, third):
    gateway = get_gateway()
    return Expression(getattr(gateway.jvm.Expressions, op_name)(
        _get_java_expression(first),
        _get_java_expression(second),
        _get_java_expression(third)))


def _quaternion_op(op_name: str, first, second, third, forth):
    gateway = get_gateway()
    return Expression(getattr(gateway.jvm.Expressions, op_name)(
        _get_java_expression(first),
        _get_java_expression(second),
        _get_java_expression(third),
        _get_java_expression(forth)))


def _add_version_doc():
    from inspect import getmembers, isfunction
    from pyflink.table import expressions
    for o in getmembers(expressions):
        if isfunction(o[1]) and not o[0].startswith('_'):
            add_version_doc(o[1], "1.12.0")


def col(name: str) -> Expression:
    """
    Creates an expression which refers to a table's field.

    Example:
    ::

        >>> tab.select(col("key"), col("value"))

    :param name: the field name to refer to
    """
    return _unary_op("$", name)


def lit(v, data_type: DataType = None) -> Expression:
    """
    Creates a SQL literal.

    The data type is derived from the object's class and its value. For example, `lit(12)` leads
    to `INT`, `lit("abc")` leads to `CHAR(3)`.

    Example:
    ::

        >>> tab.select(col("key"), lit("abc"))
    """
    if data_type is None:
        return _unary_op("lit", v)
    else:
        return _binary_op("lit", v, _to_java_data_type(data_type))


def range_(start: Union[str, int], end: Union[str, int]) -> Expression:
    """
    Indicates a range from 'start' to 'end', which can be used in columns selection.

    Example:
    ::

        >>> tab.select(with_columns(range_('b', 'c')))

    .. seealso:: :func:`~pyflink.table.expressions.with_columns`
    """
    return _binary_op("range", start, end)


def and_(predicate0: Union[bool, Expression[bool]],
         predicate1: Union[bool, Expression[bool]],
         *predicates: Union[bool, Expression[bool]]) -> Expression[bool]:
    """
    Boolean AND in three-valued logic.
    """
    gateway = get_gateway()
    predicates = to_jarray(gateway.jvm.Object, [_get_java_expression(p) for p in predicates])
    return _ternary_op("and", predicate0, predicate1, predicates)


def or_(predicate0: Union[bool, Expression[bool]],
        predicate1: Union[bool, Expression[bool]],
        *predicates: Union[bool, Expression[bool]]) -> Expression[bool]:
    """
    Boolean OR in three-valued logic.
    """
    gateway = get_gateway()
    predicates = to_jarray(gateway.jvm.Object, [_get_java_expression(p) for p in predicates])
    return _ternary_op("or", predicate0, predicate1, predicates)


"""
Offset constant to be used in the `preceding` clause of unbounded
:class:`~pyflink.table.window.Over`. Use this constant for a time interval.
Unbounded over windows start with the first row of a partition.

.. versionadded:: 1.12.0
"""
UNBOUNDED_ROW = Expression("UNBOUNDED_ROW")


"""
Offset constant to be used in the `preceding` clause of unbounded
:class:`~pyflink.table.window.Over` windows. Use this constant for a row-count interval.
Unbounded over windows start with the first row of a partition.

.. versionadded:: 1.12.0
"""
UNBOUNDED_RANGE = Expression("UNBOUNDED_RANGE")


"""
Offset constant to be used in the `following` clause of :class:`~pyflink.table.window.Over` windows.
Use this for setting the upper bound of the window to the current row.

.. versionadded:: 1.12.0
"""
CURRENT_ROW = Expression("CURRENT_ROW")


"""
Offset constant to be used in the `following` clause of :class:`~pyflink.table.window.Over` windows.
Use this for setting the upper bound of the window to the sort key of the current row, i.e.,
all rows with the same sort key as the current row are included in the window.

.. versionadded:: 1.12.0
"""
CURRENT_RANGE = Expression("CURRENT_RANGE")


def current_date() -> Expression:
    """
    Returns the current SQL date in UTC time zone.
    """
    return _leaf_op("currentDate")


def current_time() -> Expression:
    """
    Returns the current SQL time in UTC time zone.
    """
    return _leaf_op("currentTime")


def current_timestamp() -> Expression:
    """
    Returns the current SQL timestamp in UTC time zone.
    """
    return _leaf_op("currentTimestamp")


def local_time() -> Expression:
    """
    Returns the current SQL time in local time zone.
    """
    return _leaf_op("localTime")


def local_timestamp() -> Expression:
    """
    Returns the current SQL timestamp in local time zone.
    """
    return _leaf_op("localTimestamp")


def temporal_overlaps(left_time_point,
                      left_temporal,
                      right_time_point,
                      right_temporal) -> Expression:
    """
    Determines whether two anchored time intervals overlap. Time point and temporal are
    transformed into a range defined by two time points (start, end). The function
    evaluates `left_end >= right_start && right_end >= left_start`.

    e.g.
        temporal_overlaps(
            lit("2:55:00").to_time,
            lit(1).hours,
            lit("3:30:00").to_time,
            lit(2).hours) leads to true.

    :param left_time_point: The left time point
    :param left_temporal: The time interval from the left time point
    :param right_time_point: The right time point
    :param right_temporal: The time interval from the right time point
    :return: An expression which indicates whether two anchored time intervals overlap.
    """
    return _quaternion_op("temporalOverlaps",
                          left_time_point, left_temporal, right_time_point, right_temporal)


def date_format(timestamp, format) -> Expression:
    """
    Formats a timestamp as a string using a specified format.
    The format must be compatible with MySQL's date formatting syntax as used by the
    date_parse function.

    For example `date_format(col("time"), "%Y, %d %M")` results in strings formatted as
    "2017, 05 May".

    :param timestamp: The timestamp to format as string.
    :param format: The format of the string.
    :return: The formatted timestamp as string.
    """
    return _binary_op("dateFormat", timestamp, format)


def timestamp_diff(time_point_unit: TimePointUnit, time_point1, time_point2) -> Expression:
    """
    Returns the (signed) number of :class:`~pyflink.table.expression.TimePointUnit` between
    time_point1 and time_point2.

    For example,
    `timestamp_diff(TimePointUnit.DAY, lit("2016-06-15").to_date, lit("2016-06-18").to_date`
    leads to 3.

    :param time_point_unit: The unit to compute diff.
    :param time_point1: The first point in time.
    :param time_point2: The second point in time.
    :return: The number of intervals as integer value.
    """
    return _ternary_op("timestampDiff", time_point_unit._to_j_time_point_unit(),
                       time_point1, time_point2)


def array(head, *tail) -> Expression:
    """
    Creates an array of literals.

    Example:
    ::

        >>> tab.select(array(1, 2, 3))
    """
    gateway = get_gateway()
    tail = to_jarray(gateway.jvm.Object, [_get_java_expression(t) for t in tail])
    return _binary_op("array", head, tail)


def row(head, *tail) -> Expression:
    """
    Creates a row of expressions.

    Example:
    ::

        >>> tab.select(row("key1", 1))
    """
    gateway = get_gateway()
    tail = to_jarray(gateway.jvm.Object, [_get_java_expression(t) for t in tail])
    return _binary_op("row", head, tail)


def map_(key, value, *tail) -> Expression:
    """
    Creates a map of expressions.

    Example:
    ::

        >>> tab.select(
        >>>     map_(
        >>>         "key1", 1,
        >>>         "key2", 2,
        >>>         "key3", 3
        >>>     ))

    .. note::

        keys and values should have the same types for all entries.
    """
    gateway = get_gateway()
    tail = to_jarray(gateway.jvm.Object, [_get_java_expression(t) for t in tail])
    return _ternary_op("map", key, value, tail)


def row_interval(rows: int) -> Expression:
    """
    Creates an interval of rows.

    Example:
    ::

        >>> tab.window(Over
        >>>         .partition_by(col('a'))
        >>>         .order_by(col('proctime'))
        >>>         .preceding(row_interval(4))
        >>>         .following(CURRENT_ROW)
        >>>         .alias('w'))

    :param rows: the number of rows
    """
    return _unary_op("rowInterval", rows)


def pi() -> Expression[float]:
    """
    Returns a value that is closer than any other value to `pi`.
    """
    return _leaf_op("pi")


def e() -> Expression[float]:
    """
    Returns a value that is closer than any other value to `e`.
    """
    return _leaf_op("e")


def rand(seed: Union[int, Expression[int]] = None) -> Expression[float]:
    """
    Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive) with a
    initial seed if specified. Two rand() functions will return identical sequences of numbers if
    they have same initial seed.
    """
    if seed is None:
        return _leaf_op("rand")
    else:
        return _unary_op("rand", seed)


def rand_integer(bound: Union[int, Expression[int]],
                 seed: Union[int, Expression[int]] = None) -> Expression:
    """
    Returns a pseudorandom integer value between 0.0 (inclusive) and the specified value
    (exclusive) with a initial seed if specified. Two rand_integer() functions will return
    identical sequences of numbers if they have same initial seed and same bound.
    """
    if seed is None:
        return _unary_op("randInteger", bound)
    else:
        return _binary_op("randInteger", seed, bound)


def atan2(y, x) -> Expression[float]:
    """
    Calculates the arc tangent of a given coordinate.
    """
    return _binary_op("atan2", y, x)


def negative(v) -> Expression:
    """
    Returns negative numeric.
    """
    return _unary_op("negative", v)


def concat(first: Union[str, Expression[str]],
           *others: Union[str, Expression[str]]) -> Expression[str]:
    """
    Returns the string that results from concatenating the arguments.
    Returns NULL if any argument is NULL.
    """
    gateway = get_gateway()
    return _binary_op("concat",
                      first,
                      to_jarray(gateway.jvm.Object,
                                [_get_java_expression(other) for other in others]))


def concat_ws(separator: Union[str, Expression[str]],
              first: Union[str, Expression[str]],
              *others: Union[str, Expression[str]]) -> Expression[str]:
    """
    Returns the string that results from concatenating the arguments and separator.
    Returns NULL If the separator is NULL.

    .. note::

        this function does not skip empty strings. However, it does skip any NULL
        values after the separator argument.
    """
    gateway = get_gateway()
    return _ternary_op("concatWs",
                       separator,
                       first,
                       to_jarray(gateway.jvm.Object,
                                 [_get_java_expression(other) for other in others]))


def uuid() -> Expression[str]:
    """
    Returns an UUID (Universally Unique Identifier) string (e.g.,
    "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly
    generated) UUID. The UUID is generated using a cryptographically strong pseudo random number
    generator.
    """
    return _leaf_op("uuid")


def null_of(data_type: DataType) -> Expression:
    """
    Returns a null literal value of a given data type.
    """
    return _unary_op("nullOf", _to_java_data_type(data_type))


def log(v, base=None) -> Expression[float]:
    """
    If base is specified, calculates the logarithm of the given value to the given base.
    Otherwise, calculates the natural logarithm of the given value.
    """
    if base is None:
        return _unary_op("log", v)
    else:
        return _binary_op("log", base, v)


def if_then_else(condition: Union[bool, Expression[bool]], if_true, if_false) -> Expression:
    """
    Ternary conditional operator that decides which of two other expressions should be evaluated
    based on a evaluated boolean condition.

    e.g. if_then_else(col("f0") > 5, "A", "B") leads to "A"

    :param condition: condition boolean condition
    :param if_true: expression to be evaluated if condition holds
    :param if_false: expression to be evaluated if condition does not hold
    """
    return _ternary_op("ifThenElse", condition, if_true, if_false)


def with_columns(head, *tails) -> Expression:
    """
    Creates an expression that selects a range of columns. It can be used wherever an array of
    expression is accepted such as function calls, projections, or groupings.

    A range can either be index-based or name-based. Indices start at 1 and boundaries are
    inclusive.

    e.g. with_columns(range_("b", "c")) or with_columns(col("*"))

    .. seealso:: :func:`~pyflink.table.expressions.range_`,
                 :func:`~pyflink.table.expressions.without_columns`
    """
    gateway = get_gateway()
    tails = to_jarray(gateway.jvm.Object, [_get_java_expression(t) for t in tails])
    return _binary_op("withColumns", head, tails)


def without_columns(head, tails) -> Expression:
    """
    Creates an expression that selects all columns except for the given range of columns. It can
    be used wherever an array of expression is accepted such as function calls, projections, or
    groupings.

    A range can either be index-based or name-based. Indices start at 1 and boundaries are
    inclusive.

    e.g. without_columns(range_("b", "c")) or without_columns(col("c"))

    .. seealso:: :func:`~pyflink.table.expressions.range_`,
                 :func:`~pyflink.table.expressions.with_columns`
    """
    gateway = get_gateway()
    tails = to_jarray(gateway.jvm.Object, [_get_java_expression(t) for t in tails])
    return _binary_op("withoutColumns", head, tails)


def call(f: Union[str, UserDefinedFunctionWrapper], *args) -> Expression:
    """
    The first parameter `f` could be a str or a Python user-defined function.

    When it is str, this is a call to a function that will be looked up in a catalog. There
    are two kinds of functions:

        - System functions - which are identified with one part names
        - Catalog functions - which are identified always with three parts names
            (catalog, database, function)

    Moreover each function can either be a temporary function or permanent one
    (which is stored in an external catalog).

    Based on that two properties the resolution order for looking up a function based on
    the provided `function_name` is following:

        - Temporary system function
        - System function
        - Temporary catalog function
        - Catalog function

    :param f: the path of the function or the Python user-defined function.
    :param args: parameters of the user-defined function.
    """
    gateway = get_gateway()

    if isinstance(f, str):
        return Expression(gateway.jvm.Expressions.call(
            f, to_jarray(gateway.jvm.Object, [_get_java_expression(arg) for arg in args])))

    def get_function_definition(f):
        if isinstance(f, UserDefinedTableFunctionWrapper):
            """
            TypeInference was not supported for TableFunction in the old planner. Use
            TableFunctionDefinition to work around this issue.
            """
            j_result_types = to_jarray(gateway.jvm.TypeInformation,
                                       [_to_java_type(i) for i in f._result_types])
            j_result_type = gateway.jvm.org.apache.flink.api.java.typeutils.RowTypeInfo(
                j_result_types)
            return gateway.jvm.org.apache.flink.table.functions.TableFunctionDefinition(
                'f', f.java_user_defined_function(), j_result_type)
        else:
            return f.java_user_defined_function()

    expressions_clz = load_java_class("org.apache.flink.table.api.Expressions")
    function_definition_clz = load_java_class('org.apache.flink.table.functions.FunctionDefinition')
    j_object_array_type = to_jarray(gateway.jvm.Object, []).getClass()

    api_call_method = expressions_clz.getDeclaredMethod(
        "apiCall",
        to_jarray(gateway.jvm.Class, [function_definition_clz, j_object_array_type]))
    api_call_method.setAccessible(True)

    return Expression(api_call_method.invoke(
        None,
        to_jarray(gateway.jvm.Object,
                  [get_function_definition(f),
                   to_jarray(gateway.jvm.Object, [_get_java_expression(arg) for arg in args])])))


_add_version_doc()
