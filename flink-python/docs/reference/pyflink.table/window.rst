.. ################################################################################
     Licensed to the Apache Software Foundation (ASF) under one
     or more contributor license agreements.  See the NOTICE file
     distributed with this work for additional information
     regarding copyright ownership.  The ASF licenses this file
     to you under the Apache License, Version 2.0 (the
     "License"); you may not use this file except in compliance
     with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
    limitations under the License.
   ################################################################################

======
Window
======

Tumble Window
-------------

Tumbling windows are consecutive, non-overlapping
windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
elements in 5 minutes intervals.

Example:
::

    >>> from pyflink.table.expressions import col, lit
    >>> Tumble.over(lit(10).minutes) \
    ...       .on(col("rowtime")) \
    ...       .alias("w")

.. currentmodule:: pyflink.table.window

.. autosummary::
    :toctree: api/

    Tumble.over
    TumbleWithSize.on
    TumbleWithSizeOnTime.alias


Sliding Window
--------------

Sliding windows have a fixed size and slide by
a specified slide interval. If the slide interval is smaller than the window size, sliding
windows are overlapping. Thus, an element can be assigned to multiple windows.

For example, a sliding window of size 15 minutes with 5 minutes sliding interval groups
elements of 15 minutes and evaluates every five minutes. Each element is contained in three
consecutive window evaluations.

Example:
::

    >>> from pyflink.table.expressions import col, lit
    >>> Slide.over(lit(10).minutes) \
    ...      .every(lit(5).minutes) \
    ...      .on(col("rowtime")) \
    ...      .alias("w")

.. currentmodule:: pyflink.table.window

.. autosummary::
    :toctree: api/

    Slide.over
    SlideWithSize.every
    SlideWithSizeAndSlide.on
    SlideWithSizeAndSlideOnTime.alias


Session Window
--------------

The boundary of session windows are defined by
intervals of inactivity, i.e., a session window is closes if no event appears for a defined
gap period.

Example:
::

    >>> from pyflink.table.expressions import col, lit
    >>> Session.with_gap(lit(10).minutes) \\
    ...        .on(col("rowtime")) \\
    ...        .alias("w")


.. currentmodule:: pyflink.table.window

.. autosummary::
    :toctree: api/

    Session.with_gap
    SessionWithGap.on
    SessionWithGapOnTime.alias


Over Window
-----------

Similar to SQL, over window aggregates compute an
aggregate for each input row over a range of its neighboring rows.

Example:
::

    >>> from pyflink.table.expressions import col, UNBOUNDED_RANGE
    >>> Over.partition_by(col("a")) \
    ...     .order_by(col("rowtime")) \
    ...     .preceding(UNBOUNDED_RANGE) \
    ...     .alias("w")

.. currentmodule:: pyflink.table.window

.. autosummary::
    :toctree: api/

    Over.order_by
    Over.partition_by
    OverWindowPartitionedOrdered.alias
    OverWindowPartitionedOrdered.preceding
    OverWindowPartitionedOrderedPreceding.alias
    OverWindowPartitionedOrderedPreceding.following
    OverWindowPartitioned.order_by
