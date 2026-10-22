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

"""Utilities for constructing DataFrame OVER windows."""

import datetime
from dataclasses import dataclass
from typing import Any, Union

from pyflink.table._over_window import _BoundaryDirection, _FrameBoundary
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ["FrameBoundary", "preceding", "following"]


@PublicEvolving()
@dataclass(frozen=True, init=False)
class FrameBoundary(_FrameBoundary):
    """
    An opaque bound for an inline OVER window frame.

    Create instances with :func:`preceding` or :func:`following` instead of constructing this
    class directly.

    .. versionadded:: 2.4.0
    """

    _direction: _BoundaryDirection
    _value: Any

    def __init__(self) -> None:
        raise TypeError("FrameBoundary instances must be created with preceding() or following()")

    @classmethod
    def _create(cls, direction: _BoundaryDirection, value: Any) -> "FrameBoundary":
        boundary = object.__new__(cls)
        object.__setattr__(boundary, "_direction", direction)
        object.__setattr__(boundary, "_value", value)
        return boundary

    def __repr__(self) -> str:
        return "%s(%r)" % (self._direction.value, self._value)


@PublicEvolving()
def preceding(value: Union[int, str, datetime.timedelta]) -> FrameBoundary:
    """
    Create a preceding bound for an inline OVER window frame.

    When used in ``rows``, ``value`` must be a positive integer row count. When used in
    ``range``, it must be a non-negative :class:`datetime.timedelta` or a Flink duration string,
    such as ``"1 hour"``. The value is validated when the boundary is passed to
    :meth:`~pyflink.table.expression.Expression.over`.

    :param value: Row count or time interval before the current row or range.
    :return: A frame-bound descriptor for the lower bound of an explicit OVER frame.

    Example::

        >>> from datetime import timedelta
        >>> import pyflink.dataframe as pf
        >>> from pyflink.table.expressions import CURRENT_RANGE, CURRENT_ROW
        >>> running = pf.col("amount").sum.over(
        ...     order_by="event_time",
        ...     rows=(pf.preceding(2), CURRENT_ROW),
        ... )
        >>> last_hour = pf.col("amount").sum.over(
        ...     order_by="event_time",
        ...     range=(pf.preceding("1 hour"), CURRENT_RANGE),
        ... )
        >>> last_five_minutes = pf.col("amount").sum.over(
        ...     order_by="event_time",
        ...     range=(pf.preceding(timedelta(minutes=5)), CURRENT_RANGE),
        ... )

    .. versionadded:: 2.4.0
    """

    return FrameBoundary._create(_BoundaryDirection.PRECEDING, value)


@PublicEvolving()
def following(value: Union[int, str, datetime.timedelta]) -> FrameBoundary:
    """
    Create a following bound for an inline OVER window frame.

    When used in ``rows``, ``value`` must be a positive integer row count. When used in
    ``range``, it must be a non-negative :class:`datetime.timedelta` or a Flink duration string,
    such as ``"1 hour"``. The value is validated when the boundary is passed to
    :meth:`~pyflink.table.expression.Expression.over`. Following upper bounds are supported only
    in batch mode.

    :param value: Row count or time interval after the current row or range.
    :return: A frame-bound descriptor for the upper bound of an explicit OVER frame.

    Example::

        >>> from datetime import timedelta
        >>> import pyflink.dataframe as pf
        >>> from pyflink.table.expressions import CURRENT_RANGE
        >>> centered = pf.col("amount").sum.over(
        ...     order_by="event_time",
        ...     rows=(pf.preceding(2), pf.following(1)),
        ... )
        >>> next_hour = pf.col("amount").sum.over(
        ...     order_by="event_time",
        ...     range=(CURRENT_RANGE, pf.following("1 hour")),
        ... )
        >>> next_five_minutes = pf.col("amount").sum.over(
        ...     order_by="event_time",
        ...     range=(CURRENT_RANGE, pf.following(timedelta(minutes=5))),
        ... )

    .. versionadded:: 2.4.0
    """

    return FrameBoundary._create(_BoundaryDirection.FOLLOWING, value)
