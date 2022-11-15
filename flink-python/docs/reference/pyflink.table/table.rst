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

=====
Table
=====

Table
=====

A :class:`~pyflink.table.Table` object is the core abstraction of the Table API.
Similar to how the DataStream API has DataStream,
the Table API is built around :class:`~pyflink.table.Table`.

A :class:`~pyflink.table.Table` object describes a pipeline of data transformations. It does not
contain the data itself in any way. Instead, it describes how to read data from a table source,
and how to eventually write data to a table sink. The declared pipeline can be
printed, optimized, and eventually executed in a cluster. The pipeline can work with bounded or
unbounded streams which enables both streaming and batch scenarios.

By the definition above, a :class:`~pyflink.table.Table` object can actually be considered as
a view in SQL terms.

The initial :class:`~pyflink.table.Table` object is constructed by a
:class:`~pyflink.table.TableEnvironment`. For example,
:func:`~pyflink.table.TableEnvironment.from_path` obtains a table from a catalog.
Every :class:`~pyflink.table.Table` object has a schema that is available through
:func:`~pyflink.table.Table.get_schema`. A :class:`~pyflink.table.Table` object is
always associated with its original table environment during programming.

Every transformation (i.e. :func:`~pyflink.table.Table.select`} or
:func:`~pyflink.table.Table.filter` on a :class:`~pyflink.table.Table` object leads to a new
:class:`~pyflink.table.Table` object.

Use :func:`~pyflink.table.Table.execute` to execute the pipeline and retrieve the transformed
data locally during development. Otherwise, use :func:`~pyflink.table.Table.execute_insert` to
write the data into a table sink.

Many methods of this class take one or more :class:`~pyflink.table.Expression` as parameters.
For fluent definition of expressions and easier readability, we recommend to add a star import:

Example:
::

    >>> from pyflink.table.expressions import *

Check the documentation for more programming language specific APIs.

The following example shows how to work with a :class:`~pyflink.table.Table` object.

Example:
::

    >>> from pyflink.table import EnvironmentSettings, TableEnvironment
    >>> from pyflink.table.expressions import *
    >>> env_settings = EnvironmentSettings.in_streaming_mode()
    >>> t_env = TableEnvironment.create(env_settings)
    >>> table = t_env.from_path("my_table").select(col("colA").trim(), col("colB") + 12)
    >>> table.execute().print()

.. currentmodule:: pyflink.table

.. autosummary::
    :toctree: api/

    Table.add_columns
    Table.add_or_replace_columns
    Table.aggregate
    Table.alias
    Table.distinct
    Table.drop_columns
    Table.drop_columns
    Table.execute
    Table.execute_insert
    Table.explain
    Table.fetch
    Table.filter
    Table.flat_aggregate
    Table.flat_map
    Table.full_outer_join
    Table.get_schema
    Table.group_by
    Table.intersect
    Table.intersect_all
    Table.join
    Table.join_lateral
    Table.left_outer_join
    Table.left_outer_join_lateral
    Table.limit
    Table.map
    Table.minus
    Table.minus_all
    Table.offset
    Table.order_by
    Table.over_window
    Table.print_schema
    Table.rename_columns
    Table.right_outer_join
    Table.select
    Table.to_pandas
    Table.union
    Table.union_all
    Table.where
    Table.window


GroupedTable
============

A table that has been grouped on a set of grouping keys.

.. currentmodule:: pyflink.table

.. autosummary::
    :toctree: api/

    GroupedTable.select
    GroupedTable.aggregate
    GroupedTable.flat_aggregate


GroupWindowedTable
==================

A table that has been windowed for :class:`~pyflink.table.GroupWindow`.

.. currentmodule:: pyflink.table

.. autosummary::
    :toctree: api/

    GroupWindowedTable.group_by


WindowGroupedTable
==================

A table that has been windowed and grouped for :class:`~pyflink.table.window.GroupWindow`.

.. currentmodule:: pyflink.table

.. autosummary::
    :toctree: api/

    WindowGroupedTable.select
    WindowGroupedTable.aggregate


OverWindowedTable
=================

A table that has been windowed for :class:`~pyflink.table.window.OverWindow`.

Unlike group windows, which are specified in the GROUP BY clause, over windows do not collapse
rows. Instead over window aggregates compute an aggregate for each input row over a range of
its neighboring rows.

.. currentmodule:: pyflink.table

.. autosummary::
    :toctree: api/

    OverWindowedTable.select


AggregatedTable
===============

A table that has been performed on the aggregate function.

.. currentmodule:: pyflink.table.table

.. autosummary::
    :toctree: api/

    AggregatedTable.select


FlatAggregateTable
==================

A table that performs flatAggregate on a :class:`~pyflink.table.Table`, a
:class:`~pyflink.table.GroupedTable` or a :class:`~pyflink.table.WindowGroupedTable`

.. currentmodule:: pyflink.table.table

.. autosummary::
    :toctree: api/

    FlatAggregateTable.select
