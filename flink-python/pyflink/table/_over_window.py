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

"""Helpers for constructing expression-level OVER windows.

The public descriptors in this module are deliberately independent of a table
environment.  They are converted to Flink expressions only when an OVER call
is built, which keeps the DataFrame API on top of the Table expression layer.
"""

import datetime
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Tuple, Union

from pyflink.java_gateway import get_gateway
from pyflink.table.expression import Expression
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = [
    "UNBOUNDED",
    "UNBOUNDED_PRECEDING",
    "UNBOUNDED_FOLLOWING",
    "CURRENT_ROW",
    "preceding",
    "following",
]


class _BoundaryDirection(Enum):
    PRECEDING = "preceding"
    FOLLOWING = "following"


@dataclass(frozen=True)
class _FrameBoundary:
    direction: _BoundaryDirection
    value: Any

    def __repr__(self) -> str:
        if self.value is UNBOUNDED:
            return "UNBOUNDED_%s" % self.direction.name
        if self.value is CURRENT_ROW:
            return "CURRENT_ROW"
        return "%s(%r)" % (self.direction.value, self.value)


class _Unbounded:
    def __repr__(self) -> str:
        return "UNBOUNDED"


class _CurrentRow:
    def __repr__(self) -> str:
        return "CURRENT_ROW"


UNBOUNDED = _Unbounded()
UNBOUNDED_PRECEDING = _FrameBoundary(_BoundaryDirection.PRECEDING, UNBOUNDED)
UNBOUNDED_FOLLOWING = _FrameBoundary(_BoundaryDirection.FOLLOWING, UNBOUNDED)
CURRENT_ROW = _CurrentRow()


@PublicEvolving()
def preceding(value: Any) -> _FrameBoundary:
    """Create an explicit preceding frame boundary."""

    return _FrameBoundary(_BoundaryDirection.PRECEDING, value)


@PublicEvolving()
def following(value: Any) -> _FrameBoundary:
    """Create an explicit following frame boundary."""

    return _FrameBoundary(_BoundaryDirection.FOLLOWING, value)


def _to_column(value: Union[str, Expression], parameter_name: str) -> Expression:
    if isinstance(value, str):
        from pyflink.table.expressions import col

        return col(value)
    if isinstance(value, Expression):
        return value
    raise TypeError("%s must be a column name or Expression" % parameter_name)


def _normalize_partition_by(value: Any) -> List[Expression]:
    if value is None:
        return []
    if isinstance(value, (str, Expression)):
        values = [value]
    elif isinstance(value, (list, tuple)):
        values = list(value)
    else:
        raise TypeError("partition_by must be a column name or Expression sequence")
    if not values:
        raise TypeError("partition_by must not be empty")
    return [_to_column(item, "partition_by") for item in values]


def _interval_expression(value: Union[str, datetime.timedelta]) -> Expression:
    if isinstance(value, datetime.timedelta):
        if value < datetime.timedelta(0):
            raise ValueError("time interval must not be negative")
        millis = value // datetime.timedelta(milliseconds=1)
    elif isinstance(value, str):
        try:
            duration = get_gateway().jvm.org.apache.flink.util.TimeUtils.parseDuration(value)
            millis = duration.toMillis()
        except Exception as exc:
            raise ValueError("range must be a valid duration string") from exc
    else:
        raise TypeError("range boundaries must be a datetime.timedelta or duration string")

    if millis < 0:
        raise ValueError("time interval must not be negative")
    j_expr = (
        get_gateway()
        .jvm.org.apache.flink.table.expressions.ApiExpressionUtils.intervalOfMillis(millis)
    )
    return Expression(j_expr)


def _row_expression(value: Any) -> Expression:
    if isinstance(value, bool):
        raise TypeError("row interval must not be bool")
    if not isinstance(value, int):
        raise TypeError("row boundaries must be integers")
    if value <= 0:
        raise ValueError("row interval must be larger than 0")
    from pyflink.table.expressions import row_interval

    return row_interval(value)


def _current_expression(kind: str) -> Expression:
    # Keep Expression instances out of module globals: introspection can initialize the gateway.
    from pyflink.table.expressions import CURRENT_RANGE, CURRENT_ROW

    return CURRENT_ROW if kind == "rows" else CURRENT_RANGE


def _unbounded_expression(kind: str) -> Expression:
    from pyflink.table.expressions import UNBOUNDED_RANGE, UNBOUNDED_ROW

    if kind == "rows":
        return UNBOUNDED_ROW
    return UNBOUNDED_RANGE


def _value_expression(value: Any, kind: str) -> Expression:
    return _row_expression(value) if kind == "rows" else _interval_expression(value)


def _normalize_bound(bound: Any, kind: str, position: str) -> Expression:
    if bound is UNBOUNDED:
        return _unbounded_expression(kind)
    if bound is CURRENT_ROW:
        return _current_expression(kind)
    from pyflink.table.expressions import (
        CURRENT_RANGE,
        CURRENT_ROW as TABLE_CURRENT_ROW,
        UNBOUNDED_RANGE,
        UNBOUNDED_ROW,
    )

    table_bound_names = (
        (TABLE_CURRENT_ROW, "CURRENT_ROW"),
        (CURRENT_RANGE, "CURRENT_RANGE"),
        (UNBOUNDED_ROW, "UNBOUNDED_ROW"),
        (UNBOUNDED_RANGE, "UNBOUNDED_RANGE"),
    )
    for table_bound, table_name in table_bound_names:
        if bound is table_bound:
            raise TypeError(
                "Use pyflink.dataframe.%s for DataFrame OVER frame bounds, not "
                "pyflink.table.expressions.%s" % (table_name, table_name)
            )
    if not isinstance(bound, _FrameBoundary):
        raise TypeError(
            "%s bound must be preceding(), following(), CURRENT_ROW, or UNBOUNDED"
            % position
        )

    expected_direction = (
        _BoundaryDirection.PRECEDING
        if position == "lower"
        else _BoundaryDirection.FOLLOWING
    )
    if bound.direction is not expected_direction:
        raise ValueError("invalid %s bound direction" % position)
    if bound.value is UNBOUNDED:
        return _unbounded_expression(kind)
    if bound.value is CURRENT_ROW:
        return _current_expression(kind)
    return _value_expression(bound.value, kind)


def _validate_explicit_bound_kinds(bounds: Tuple[Any, Any], kind: str) -> None:
    for bound in bounds:
        if not isinstance(bound, _FrameBoundary):
            continue
        value = bound.value
        if value is UNBOUNDED or value is CURRENT_ROW:
            continue
        if kind == "rows" and (
            isinstance(value, bool)
            or not isinstance(value, int)
        ):
            raise ValueError("rows bounds must use the same frame kind")
        if kind == "range" and isinstance(value, (bool, int)):
            raise ValueError("range bounds must use the same frame kind")


def _normalize_scalar_frame(value: Any, kind: str) -> Tuple[Expression, Expression]:
    if value is UNBOUNDED:
        return (
            _unbounded_expression(kind),
            _current_expression(kind),
        )
    return _value_expression(value, kind), _current_expression(kind)


def _normalize_frame(
    rows: Any, range_: Any
) -> Tuple[Expression, Expression]:
    if rows is not None and range_ is not None:
        raise ValueError("rows and range are mutually exclusive")

    if rows is None and range_ is None:
        return _unbounded_expression("range"), _current_expression("range")

    kind = "rows" if rows is not None else "range"
    value = rows if rows is not None else range_
    if isinstance(value, tuple):
        if len(value) != 2:
            raise TypeError("frame tuple must contain exactly two-element bounds")
        lower_bound, upper_bound = value
        _validate_explicit_bound_kinds((lower_bound, upper_bound), kind)
        return (
            _normalize_bound(lower_bound, kind, "lower"),
            _normalize_bound(upper_bound, kind, "upper"),
        )
    return _normalize_scalar_frame(value, kind)


def _build_over_expression(
    aggregate: Expression,
    order_by: Union[str, Expression],
    partition_by: Any = None,
    rows: Any = None,
    range_: Any = None,
) -> Expression:
    if not isinstance(aggregate, Expression):
        raise TypeError("aggregate must be an Expression")
    order_expression = _to_column(order_by, "order_by")
    partitions = _normalize_partition_by(partition_by)
    preceding_expression, following_expression = _normalize_frame(rows, range_)

    gateway = get_gateway()
    java_args = [
        aggregate._j_expr,
        order_expression._j_expr,
        preceding_expression._j_expr,
        following_expression._j_expr,
    ] + [partition._j_expr for partition in partitions]
    java_list = gateway.jvm.java.util.ArrayList()
    for java_arg in java_args:
        java_list.add(java_arg)
    unresolved = gateway.jvm.org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall(
        gateway.jvm.org.apache.flink.table.functions.BuiltInFunctionDefinitions.OVER,
        java_list,
    )
    return Expression(gateway.jvm.org.apache.flink.table.api.ApiExpression(unresolved))
