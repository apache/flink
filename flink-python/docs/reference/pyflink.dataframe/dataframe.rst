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

=========
DataFrame
=========

A DataFrame provides a Pythonic interface for composing data transformations.
Transformation methods return new DataFrames and support fluent chaining. They build execution
plans lazily without starting a Flink job; execution is triggered by an action such as
``DataFrame.collect`` or ``DataFrame.to_pandas``.

Columns can also be referenced as attributes, such as ``df.name``, when their names are valid
Python identifiers, do not start with an underscore, are not keywords, and do not conflict with
existing DataFrame attributes. Use bracket access for other names, such as ``df["select"]``,
``df["_name"]``, or ``df["first name"]``.

Example::

    >>> import pyflink.dataframe as pf
    >>> df = pf.from_dict({"id": [1, 2], "name": ["a", "b"]})
    >>> result = df.select("id", "name") \
    ...            .with_column("id_doubled", pf.col("id") * 2) \
    ...            .filter(pf.col("id") > 0)
    >>> names = df.select(df.name)

Pass a list to :meth:`DataFrame.explode` to expand multiple ARRAY columns together by position.
The arrays must have equal lengths in each row; empty and null arrays count as length zero.
Unequal lengths fail during execution. If all arrays are empty or null, the default retains
one row with null expanded values; ``ignore_empty_and_null=True`` drops that row::

    >>> df = pf.from_dict({"items": [["apple", "pear"]], "counts": [[2, 3]]})
    >>> result = df.explode(["items", "counts"])

DataFrame
---------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame

Transformations
---------------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame.select
    DataFrame.with_column
    DataFrame.with_columns
    DataFrame.drop_columns
    DataFrame.drop
    DataFrame.rename_columns
    DataFrame.rename
    DataFrame.filter
    DataFrame.map
    DataFrame.map_batches
    DataFrame.where
    DataFrame.explode
    DataFrame.drop_duplicates
    DataFrame.distinct
    DataFrame.unique
    DataFrame.sort
    DataFrame.top_n
    DataFrame.limit
    DataFrame.offset
    DataFrame.head
    DataFrame.flat_map
    DataFrame.__getitem__
    DataFrame.__getattr__

Set Operations
--------------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame.union
    DataFrame.union_all
    DataFrame.intersect
    DataFrame.intersect_all
    DataFrame.minus
    DataFrame.minus_all

Joins
-----

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame.join
    DataFrame.join_asof
    DataFrame.join_lateral

Use ``join_asof`` to enrich rows from a dimension table at a left time attribute.
Processing-time lookup joins require a lookup-capable right table; event-time temporal
joins require watermarks on both inputs and the right primary key in the equality keys::

    enriched = orders.join_asof(customers, on=pf.proctime(), by="customer_id", how="left")
    historical = orders.join_asof(
        rates, on="event_time", left_by="currency", right_by="currency_id")

Only inner and left joins are supported. Conditions need at least one equality between
the inputs and may include additional AND-combined predicates, but not OR conditions.
The time attribute must not also be a join key.

Missing-Value Handling
----------------------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame.drop_null
    DataFrame.drop_nan
    DataFrame.fill_null
    DataFrame.fill_nan

Aggregations
------------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame.group_by
    DataFrame.agg
    GroupedDataFrame
    GroupedDataFrame.agg

Composition
-----------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame.pipe

Properties
----------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame.schema
    DataFrame.columns
    DataFrame.llm

Results
-------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame.collect
    DataFrame.to_table
    DataFrame.to_pandas
    DataFrame.iter_rows
    DataFrame.iter_batches
    DataFrame.take
    DataFrame.take_batch
    CloseableIterator
    CloseableIterator.close

Windowing
---------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame.tumble
    DataFrame.hop
    DataFrame.cumulate
    DataFrame.session

Expressions
-----------

Functions for constructing column references, literals, and processing-time attributes.

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    col
    lit
    proctime

OVER windows
------------

An aggregate expression can define an inline OVER window. The ordering column may be a column
name or expression. Use the keyword ``order_by=`` to define an inline window; a single positional
argument such as ``over(col("w"))`` or ``over(alias=col("w"))`` refers to a Table API named window.
If neither ``rows`` nor ``range`` is provided, the default frame is unbounded RANGE through the
current range. A scalar ``rows`` bound includes that many preceding rows and the current row, so
``rows=10`` covers up to 11 rows. RANGE frames include all peers with the same ordering value.

``rows`` uses integer row offsets. ``range`` accepts ``datetime.timedelta`` or a Flink duration
string such as ``"1 hour"`` or ``"1 h"``; this is not SQL ``INTERVAL '1' HOUR`` syntax. Duration
values are converted to milliseconds, truncating sub-millisecond precision. Use Table API frame
constants from ``pyflink.table.expressions`` when matched to the frame type: ``*_ROW`` with
``rows`` and ``*_RANGE`` with ``range``.

The current expression layer requires a single time attribute in ``order_by``. Streaming OVER
windows additionally require ascending order, and all OVER aggregates in the same projection must
use the same frame. The upper bound must be the current row/range; FOLLOWING bounds are not
supported in streaming. Without ``partition_by``, streaming OVER uses a singleton distribution and
therefore runs with parallelism one, which can limit throughput. Batch plans support multiple
different OVER frames in the same projection.

.. code-block:: python

    import pyflink.dataframe as pf

    running = pf.col("amount").sum.over(
        order_by="event_time",
        partition_by="user_id",
        rows=10,
    )
    explicit = pf.col("amount").sum.over(
        order_by="event_time",
        rows=(pf.preceding(2), pf.following(1)),
    )

The public frame-bound descriptor and helpers are:

.. autosummary::
    :toctree: api/

    over_window.FrameBoundary
    preceding
    following
