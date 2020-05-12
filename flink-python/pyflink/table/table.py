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

from py4j.java_gateway import get_method
from pyflink.java_gateway import get_gateway
from pyflink.table.table_schema import TableSchema

from pyflink.util.utils import to_jarray
from pyflink.util.utils import to_j_explain_detail_arr

__all__ = ['Table', 'GroupedTable', 'GroupWindowedTable', 'OverWindowedTable', 'WindowGroupedTable']


class Table(object):

    """
    A :class:`~pyflink.table.Table` is the core component of the Table API.
    Similar to how the batch and streaming APIs have DataSet and DataStream,
    the Table API is built around :class:`~pyflink.table.Table`.

    Use the methods of :class:`~pyflink.table.Table` to transform data.

    Example:
    ::

        >>> env = StreamExecutionEnvironment.get_execution_environment()
        >>> env.set_parallelism(1)
        >>> t_env = StreamTableEnvironment.create(env)
        >>> ...
        >>> t_env.register_table_source("source", ...)
        >>> t = t_env.scan("source")
        >>> t.select(...)
        >>> ...
        >>> t_env.register_table_sink("result", ...)
        >>> t.insert_into("result")
        >>> t_env.execute("table_job")

    Operations such as :func:`~pyflink.table.Table.join`, :func:`~pyflink.table.Table.select`,
    :func:`~pyflink.table.Table.where` and :func:`~pyflink.table.Table.group_by`
    take arguments in an expression string. Please refer to the documentation for
    the expression syntax.
    """

    def __init__(self, j_table):
        self._j_table = j_table

    def select(self, fields):
        """
        Performs a selection operation. Similar to a SQL SELECT statement. The field expressions
        can contain complex expressions.

        Example:
        ::

            >>> tab.select("key, value + 'hello'")

        :param fields: Expression string.
        :type fields: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.select(fields))

    def alias(self, field, *fields):
        """
        Renames the fields of the expression result. Use this to disambiguate fields before
        joining to operations.

        Example:
        ::

            >>> tab.alias("a", "b")

        :param field: Field alias.
        :type field: str
        :param fields: Additional field aliases.
        :type fields: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        gateway = get_gateway()
        extra_fields = to_jarray(gateway.jvm.String, fields)
        return Table(get_method(self._j_table, "as")(field, extra_fields))

    def filter(self, predicate):
        """
        Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
        clause.

        Example:
        ::

            >>> tab.filter("name = 'Fred'")

        :param predicate: Predicate expression string.
        :type predicate: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.filter(predicate))

    def where(self, predicate):
        """
        Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
        clause.

        Example:
        ::

            >>> tab.where("name = 'Fred'")

        :param predicate: Predicate expression string.
        :type predicate: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.where(predicate))

    def group_by(self, fields):
        """
        Groups the elements on some grouping keys. Use this before a selection with aggregations
        to perform the aggregation on a per-group basis. Similar to a SQL GROUP BY statement.

        Example:
        ::

            >>> tab.group_by("key").select("key, value.avg")

        :param fields: Group keys.
        :type fields: str
        :return: The grouped table.
        :rtype: pyflink.table.GroupedTable
        """
        return GroupedTable(self._j_table.groupBy(fields))

    def distinct(self):
        """
        Removes duplicate values and returns only distinct (different) values.

        Example:
        ::

            >>> tab.select("key, value").distinct()

        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.distinct())

    def join(self, right, join_predicate=None):
        """
        Joins two :class:`~pyflink.table.Table`. Similar to a SQL join. The fields of the two joined
        operations must not overlap, use :func:`~pyflink.table.Table.alias` to rename fields if
        necessary. You can use where and select clauses after a join to further specify the
        behaviour of the join.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment` .

        Example:
        ::

            >>> left.join(right).where("a = b && c > 3").select("a, b, d")
            >>> left.join(right, "a = b")

        :param right: Right table.
        :type right: pyflink.table.Table
        :param join_predicate: Optional, the join predicate expression string.
        :type join_predicate: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        if join_predicate is not None:
            return Table(self._j_table.join(right._j_table, join_predicate))
        else:
            return Table(self._j_table.join(right._j_table))

    def left_outer_join(self, right, join_predicate=None):
        """
        Joins two :class:`~pyflink.table.Table`. Similar to a SQL left outer join. The fields of
        the two joined operations must not overlap, use :func:`~pyflink.table.Table.alias` to
        rename fields if necessary.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment` and its
            :class:`~pyflink.table.TableConfig` must have null check enabled (default).

        Example:
        ::

            >>> left.left_outer_join(right).select("a, b, d")
            >>> left.left_outer_join(right, "a = b").select("a, b, d")

        :param right: Right table.
        :type right: pyflink.table.Table
        :param join_predicate: Optional, the join predicate expression string.
        :type join_predicate: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        if join_predicate is None:
            return Table(self._j_table.leftOuterJoin(right._j_table))
        else:
            return Table(self._j_table.leftOuterJoin(right._j_table, join_predicate))

    def right_outer_join(self, right, join_predicate):
        """
        Joins two :class:`~pyflink.table.Table`. Similar to a SQL right outer join. The fields of
        the two joined operations must not overlap, use :func:`~pyflink.table.Table.alias` to
        rename fields if necessary.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment` and its
            :class:`~pyflink.table.TableConfig` must have null check enabled (default).

        Example:
        ::

            >>> left.right_outer_join(right, "a = b").select("a, b, d")

        :param right: Right table.
        :type right: pyflink.table.Table
        :param join_predicate: The join predicate expression string.
        :type join_predicate: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.rightOuterJoin(right._j_table, join_predicate))

    def full_outer_join(self, right, join_predicate):
        """
        Joins two :class:`~pyflink.table.Table`. Similar to a SQL full outer join. The fields of
        the two joined operations must not overlap, use :func:`~pyflink.table.Table.alias` to
        rename fields if necessary.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment` and its
            :class:`~pyflink.table.TableConfig` must have null check enabled (default).

        Example:
        ::

            >>> left.full_outer_join(right, "a = b").select("a, b, d")

        :param right: Right table.
        :type right: pyflink.table.Table
        :param join_predicate: The join predicate expression string.
        :type join_predicate: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.fullOuterJoin(right._j_table, join_predicate))

    def join_lateral(self, table_function_call, join_predicate=None):
        """
        Joins this Table with an user-defined TableFunction. This join is similar to a SQL inner
        join but works with a table function. Each row of the table is joined with the rows
        produced by the table function.

        Example:
        ::

            >>> t_env.register_java_function("split", "java.table.function.class.name")
            >>> tab.join_lateral("split(text, ' ') as (b)", "a = b")

        :param table_function_call: An expression representing a table function call.
        :type table_function_call: str
        :param join_predicate: Optional, The join predicate expression string, join ON TRUE if not
                               exist.
        :type join_predicate: str
        :return: The result Table.
        :rtype: pyflink.table.Table
        """
        if join_predicate is None:
            return Table(self._j_table.joinLateral(table_function_call))
        else:
            return Table(self._j_table.joinLateral(table_function_call, join_predicate))

    def left_outer_join_lateral(self, table_function_call, join_predicate=None):
        """
        Joins this Table with an user-defined TableFunction. This join is similar to
        a SQL left outer join but works with a table function. Each row of the table is joined
        with all rows produced by the table function. If the join does not produce any row, the
        outer row is padded with nulls.

        Example:
        ::

            >>> t_env.register_java_function("split", "java.table.function.class.name")
            >>> tab.left_outer_join_lateral("split(text, ' ') as (b)")

        :param table_function_call: An expression representing a table function call.
        :type table_function_call: str
        :param join_predicate: Optional, The join predicate expression string, join ON TRUE if not
                               exist.
        :type join_predicate: str
        :return: The result Table.
        :rtype: pyflink.table.Table
        """
        if join_predicate is None:
            return Table(self._j_table.leftOuterJoinLateral(table_function_call))
        else:
            return Table(self._j_table.leftOuterJoinLateral(table_function_call, join_predicate))

    def minus(self, right):
        """
        Minus of two :class:`~pyflink.table.Table` with duplicate records removed.
        Similar to a SQL EXCEPT clause. Minus returns records from the left table that do not
        exist in the right table. Duplicate records in the left table are returned
        exactly once, i.e., duplicates are removed. Both tables must have identical field types.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment`.

        Example:
        ::

            >>> left.minus(right)

        :param right: Right table.
        :type right: pyflink.table.Table
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.minus(right._j_table))

    def minus_all(self, right):
        """
        Minus of two :class:`~pyflink.table.Table`. Similar to a SQL EXCEPT ALL.
        Similar to a SQL EXCEPT ALL clause. MinusAll returns the records that do not exist in
        the right table. A record that is present n times in the left table and m times
        in the right table is returned (n - m) times, i.e., as many duplicates as are present
        in the right table are removed. Both tables must have identical field types.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment`.

        Example:
        ::

            >>> left.minus_all(right)

        :param right: Right table.
        :type right: pyflink.table.Table
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.minusAll(right._j_table))

    def union(self, right):
        """
        Unions two :class:`~pyflink.table.Table` with duplicate records removed.
        Similar to a SQL UNION. The fields of the two union operations must fully overlap.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment`.

        Example:
        ::

            >>> left.union(right)

        :param right: Right table.
        :type right: pyflink.table.Table
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.union(right._j_table))

    def union_all(self, right):
        """
        Unions two :class:`~pyflink.table.Table`. Similar to a SQL UNION ALL. The fields of the
        two union operations must fully overlap.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment`.

        Example:
        ::

            >>> left.union_all(right)

        :param right: Right table.
        :type right: pyflink.table.Table
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.unionAll(right._j_table))

    def intersect(self, right):
        """
        Intersects two :class:`~pyflink.table.Table` with duplicate records removed. Intersect
        returns records that exist in both tables. If a record is present in one or both tables
        more than once, it is returned just once, i.e., the resulting table has no duplicate
        records. Similar to a SQL INTERSECT. The fields of the two intersect operations must fully
        overlap.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment`.

        Example:
        ::

            >>> left.intersect(right)

        :param right: Right table.
        :type right: pyflink.table.Table
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.intersect(right._j_table))

    def intersect_all(self, right):
        """
        Intersects two :class:`~pyflink.table.Table`. IntersectAll returns records that exist in
        both tables. If a record is present in both tables more than once, it is returned as many
        times as it is present in both tables, i.e., the resulting table might have duplicate
        records. Similar to an SQL INTERSECT ALL. The fields of the two intersect operations must
        fully overlap.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment`.

        Example:
        ::

            >>> left.intersect_all(right)

        :param right: Right table.
        :type right: pyflink.table.Table
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.intersectAll(right._j_table))

    def order_by(self, fields):
        """
        Sorts the given :class:`~pyflink.table.Table`. Similar to SQL ORDER BY.
        The resulting Table is sorted globally sorted across all parallel partitions.

        Example:
        ::

            >>> tab.order_by("name.desc")

        :param fields: Order fields expression string.
        :type fields: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.orderBy(fields))

    def offset(self, offset):
        """
        Limits a sorted result from an offset position.
        Similar to a SQL OFFSET clause. Offset is technically part of the Order By operator and
        thus must be preceded by it.
        :func:`~pyflink.table.Table.offset` can be combined with a subsequent
        :func:`~pyflink.table.Table.fetch` call to return n rows after skipping the first o rows.

        Example:
        ::

            # skips the first 3 rows and returns all following rows.
            >>> tab.order_by("name.desc").offset(3)
            # skips the first 10 rows and returns the next 5 rows.
            >>> tab.order_by("name.desc").offset(10).fetch(5)

        :param offset: Number of records to skip.
        :type offset: int
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.offset(offset))

    def fetch(self, fetch):
        """
        Limits a sorted result to the first n rows.
        Similar to a SQL FETCH clause. Fetch is technically part of the Order By operator and
        thus must be preceded by it.
        :func:`~pyflink.table.Table.offset` can be combined with a preceding
        :func:`~pyflink.table.Table.fetch` call to return n rows after skipping the first o rows.

        Example:

        Returns the first 3 records.
        ::

            >>> tab.order_by("name.desc").fetch(3)

        Skips the first 10 rows and returns the next 5 rows.
        ::

            >>> tab.order_by("name.desc").offset(10).fetch(5)

        :param fetch: The number of records to return. Fetch must be >= 0.
        :type fetch: int
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.fetch(fetch))

    def window(self, window):
        """
        Defines group window on the records of a table.

        A group window groups the records of a table by assigning them to windows defined by a time
        or row interval.

        For streaming tables of infinite size, grouping into windows is required to define finite
        groups on which group-based aggregates can be computed.

        For batch tables of finite size, windowing essentially provides shortcuts for time-based
        groupBy.

        .. note::

            Computing windowed aggregates on a streaming table is only a parallel operation
            if additional grouping attributes are added to the
            :func:`~pyflink.table.GroupWindowedTable.group_by` clause.
            If the :func:`~pyflink.table.GroupWindowedTable.group_by` only references a GroupWindow
            alias, the streamed table will be processed by a single task, i.e., with parallelism 1.

        Example:
        ::

            >>> tab.window(Tumble.over("10.minutes").on("rowtime").alias("w")) \\
            ...     .group_by("w") \\
            ...     .select("a.sum as a, w.start as b, w.end as c, w.rowtime as d")

        :param window: A :class:`~pyflink.table.window.GroupWindow` created from
                       :class:`~pyflink.table.window.Tumble`, :class:`~pyflink.table.window.Session`
                       or :class:`~pyflink.table.window.Slide`.
        :type window: pyflink.table.window.GroupWindow
        :return: A group windowed table.
        :rtype: GroupWindowedTable
        """
        return GroupWindowedTable(self._j_table.window(window._java_window))

    def over_window(self, *over_windows):
        """
        Defines over-windows on the records of a table.

        An over-window defines for each record an interval of records over which aggregation
        functions can be computed.

        Example:
        ::

            >>> table.window(Over.partition_by("c").order_by("rowTime") \\
            ...     .preceding("10.seconds").alias("ow")) \\
            ...     .select("c, b.count over ow, e.sum over ow")

        .. note::

            Computing over window aggregates on a streaming table is only a parallel
            operation if the window is partitioned. Otherwise, the whole stream will be processed
            by a single task, i.e., with parallelism 1.

        .. note::

            Over-windows for batch tables are currently not supported.

        :param over_windows: over windows created from :class:`~pyflink.table.window.Over`.
        :type over_windows: pyflink.table.window.OverWindow
        :return: A over windowed table.
        :rtype: pyflink.table.OverWindowedTable
        """
        gateway = get_gateway()
        window_array = to_jarray(gateway.jvm.OverWindow,
                                 [item._java_over_window for item in over_windows])
        return OverWindowedTable(self._j_table.window(window_array))

    def add_columns(self, fields):
        """
        Adds additional columns. Similar to a SQL SELECT statement. The field expressions
        can contain complex expressions, but can not contain aggregations. It will throw an
        exception if the added fields already exist.

        Example:
        ::

            >>> tab.add_columns("a + 1 as a1, concat(b, 'sunny') as b1")

        :param fields: Column list string.
        :type fields: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.addColumns(fields))

    def add_or_replace_columns(self, fields):
        """
        Adds additional columns. Similar to a SQL SELECT statement. The field expressions
        can contain complex expressions, but can not contain aggregations. Existing fields will be
        replaced if add columns name is the same as the existing column name. Moreover, if the added
        fields have duplicate field name, then the last one is used.

        Example:
        ::

            >>> tab.add_or_replace_columns("a + 1 as a1, concat(b, 'sunny') as b1")

        :param fields: Column list string.
        :type fields: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.addOrReplaceColumns(fields))

    def rename_columns(self, fields):
        """
        Renames existing columns. Similar to a field alias statement. The field expressions
        should be alias expressions, and only the existing fields can be renamed.

        Example:
        ::

            >>> tab.rename_columns("a as a1, b as b1")

        :param fields: Column list string.
        :type fields: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.renameColumns(fields))

    def drop_columns(self, fields):
        """
        Drops existing columns. The field expressions should be field reference expressions.

        Example:
        ::

            >>> tab.drop_columns("a, b")

        :param fields: Column list string.
        :type fields: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.dropColumns(fields))

    def insert_into(self, table_path):
        """
        Writes the :class:`~pyflink.table.Table` to a :class:`~pyflink.table.TableSink` that was
        registered under the specified name. For the path resolution algorithm see
        :func:`~TableEnvironment.use_database`.

        Example:
        ::

            >>> tab.insert_into("sink")

        :param table_path: The path of the registered :class:`~pyflink.table.TableSink` to which
               the :class:`~pyflink.table.Table` is written.
        :type table_path: str
        """
        self._j_table.insertInto(table_path)

    def get_schema(self):
        """
        Returns the :class:`~pyflink.table.TableSchema` of this table.

        :return: The schema of this table.
        :rtype: pyflink.table.TableSchema
        """
        return TableSchema(j_table_schema=self._j_table.getSchema())

    def print_schema(self):
        """
        Prints the schema of this table to the console in a tree format.
        """
        self._j_table.printSchema()

    def execute_insert(self, table_path, overwrite=False):
        """
        Writes the :class:`~pyflink.table.Table` to a :class:`~pyflink.table.TableSink` that was
        registered under the specified name, and then execute the insert operation.
        For the path resolution algorithm see :func:`~TableEnvironment.use_database`.

        Example:
        ::

            >>> tab.execute_insert("sink")

        :param table_path: The path of the registered :class:`~pyflink.table.TableSink` to which
               the :class:`~pyflink.table.Table` is written.
        :type table_path: str
        :param overwrite: The flag that indicates whether the insert should overwrite
               existing data or not.
        :type overwrite: bool
        :return: The table result.
        """
        # TODO convert java TableResult to python TableResult once FLINK-17303 is finished
        self._j_table.executeInsert(table_path, overwrite)

    def execute(self):
        """
        Collects the contents of the current table local client.

        Example:
        ::

            >>> tab.execute()

        :return: The content of the table.
        """
        # TODO convert java TableResult to python TableResult once FLINK-17303 is finished
        self._j_table.execute()

    def explain(self, *extra_details):
        """
        Returns the AST of this table and the execution plan.

        :param extra_details: The extra explain details which the explain result should include,
                              e.g. estimated cost, changelog mode for streaming
        :type extra_details: tuple[ExplainDetail] (variable-length arguments of ExplainDetail)
        :return: The statement for which the AST and execution plan will be returned.
        :rtype: str
        """
        j_extra_details = to_j_explain_detail_arr(extra_details)
        return self._j_table.explain(j_extra_details)

    def __str__(self):
        return self._j_table.toString()


class GroupedTable(object):
    """
    A table that has been grouped on a set of grouping keys.
    """

    def __init__(self, java_table):
        self._j_table = java_table

    def select(self, fields):
        """
        Performs a selection operation on a grouped table. Similar to an SQL SELECT statement.
        The field expressions can contain complex expressions and aggregations.

        Example:
        ::

            >>> tab.group_by("key").select("key, value.avg + ' The average' as average")


        :param fields: Expression string that contains group keys and aggregate function calls.
        :type fields: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.select(fields))


class GroupWindowedTable(object):
    """
    A table that has been windowed for :class:`~pyflink.table.GroupWindow`.
    """

    def __init__(self, java_group_windowed_table):
        self._j_table = java_group_windowed_table

    def group_by(self, fields):
        """
        Groups the elements by a mandatory window and one or more optional grouping attributes.
        The window is specified by referring to its alias.

        If no additional grouping attribute is specified and if the input is a streaming table,
        the aggregation will be performed by a single task, i.e., with parallelism 1.

        Aggregations are performed per group and defined by a subsequent
        :func:`~pyflink.table.WindowGroupedTable.select` clause similar to SQL SELECT-GROUP-BY
        query.

        Example:
        ::

            >>> tab.window(group_window.alias("w")).group_by("w, key").select("key, value.avg")

        :param fields: Group keys.
        :type fields: str
        :return: A window grouped table.
        :rtype: pyflink.table.WindowGroupedTable
        """
        return WindowGroupedTable(self._j_table.groupBy(fields))


class WindowGroupedTable(object):
    """
    A table that has been windowed and grouped for :class:`~pyflink.table.window.GroupWindow`.
    """

    def __init__(self, java_window_grouped_table):
        self._j_table = java_window_grouped_table

    def select(self, fields):
        """
        Performs a selection operation on a window grouped table. Similar to an SQL SELECT
        statement.
        The field expressions can contain complex expressions and aggregations.

        Example:
        ::

            >>> window_grouped_table.select("key, window.start, value.avg as valavg")

        :param fields: Expression string.
        :type fields: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.select(fields))


class OverWindowedTable(object):
    """
    A table that has been windowed for :class:`~pyflink.table.window.OverWindow`.

    Unlike group windows, which are specified in the GROUP BY clause, over windows do not collapse
    rows. Instead over window aggregates compute an aggregate for each input row over a range of
    its neighboring rows.
    """

    def __init__(self, java_over_windowed_table):
        self._j_table = java_over_windowed_table

    def select(self, fields):
        """
        Performs a selection operation on a over windowed table. Similar to an SQL SELECT
        statement.
        The field expressions can contain complex expressions and aggregations.

        Example:
        ::

            >>> over_windowed_table.select("c, b.count over ow, e.sum over ow")

        :param fields: Expression string.
        :type fields: str
        :return: The result table.
        :rtype: pyflink.table.Table
        """
        return Table(self._j_table.select(fields))
