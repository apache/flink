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

import warnings

from py4j.java_gateway import get_method
from typing import Union

from pyflink.java_gateway import get_gateway
from pyflink.table import ExplainDetail
from pyflink.table.expression import Expression, _get_java_expression
from pyflink.table.expressions import col
from pyflink.table.serializers import ArrowSerializer
from pyflink.table.table_result import TableResult
from pyflink.table.table_schema import TableSchema
from pyflink.table.types import create_arrow_schema
from pyflink.table.utils import tz_convert_from_internal, to_expression_jarray
from pyflink.table.window import OverWindow, GroupWindow

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
        >>> t = t_env.from_path("source")
        >>> t.select(...)
        >>> ...
        >>> t_env.register_table_sink("result", ...)
        >>> t.execute_insert("result")

    Operations such as :func:`~pyflink.table.Table.join`, :func:`~pyflink.table.Table.select`,
    :func:`~pyflink.table.Table.where` and :func:`~pyflink.table.Table.group_by`
    take arguments in an expression string. Please refer to the documentation for
    the expression syntax.
    """

    def __init__(self, j_table, t_env):
        self._j_table = j_table
        self._t_env = t_env

    def __str__(self):
        return self._j_table.toString()

    def __getattr__(self, name) -> Expression:
        """
        Returns the :class:`Expression` of the column `name`.

        Example:
        ::

            >>> tab.select(tab.a)
        """
        if name not in self.get_schema().get_field_names():
            raise AttributeError(
                "The current table has no column named '%s', available columns: [%s]"
                % (name, ', '.join(self.get_schema().get_field_names())))
        return col(name)

    def select(self, *fields: Union[str, Expression]) -> 'Table':
        """
        Performs a selection operation. Similar to a SQL SELECT statement. The field expressions
        can contain complex expressions.

        Example:
        ::

            >>> from pyflink.table import expressions as expr
            >>> tab.select(tab.key, expr.concat(tab.value, 'hello'))
            >>> tab.select(expr.col('key'), expr.concat(expr.col('value'), 'hello'))

            >>> tab.select("key, value + 'hello'")

        :return: The result table.
        """
        if all(isinstance(f, Expression) for f in fields):
            return Table(self._j_table.select(to_expression_jarray(fields)), self._t_env)
        else:
            assert len(fields) == 1
            assert isinstance(fields[0], str)
            return Table(self._j_table.select(fields[0]), self._t_env)

    def alias(self, field: str, *fields: str) -> 'Table':
        """
        Renames the fields of the expression result. Use this to disambiguate fields before
        joining two tables.

        Example:
        ::

            >>> tab.alias("a", "b", "c")
            >>> tab.alias("a, b, c")

        :param field: Field alias.
        :param fields: Additional field aliases.
        :return: The result table.
        """
        gateway = get_gateway()
        extra_fields = to_jarray(gateway.jvm.String, fields)
        return Table(get_method(self._j_table, "as")(field, extra_fields), self._t_env)

    def filter(self, predicate: Union[str, Expression[bool]]) -> 'Table':
        """
        Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
        clause.

        Example:
        ::

            >>> tab.filter(tab.name == 'Fred')
            >>> tab.filter("name = 'Fred'")

        :param predicate: Predicate expression string.
        :return: The result table.
        """
        return Table(self._j_table.filter(_get_java_expression(predicate)), self._t_env)

    def where(self, predicate: Union[str, Expression[bool]]) -> 'Table':
        """
        Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
        clause.

        Example:
        ::

            >>> tab.where(tab.name == 'Fred')
            >>> tab.where("name = 'Fred'")

        :param predicate: Predicate expression string.
        :return: The result table.
        """
        return Table(self._j_table.where(_get_java_expression(predicate)), self._t_env)

    def group_by(self, *fields: Union[str, Expression]) -> 'GroupedTable':
        """
        Groups the elements on some grouping keys. Use this before a selection with aggregations
        to perform the aggregation on a per-group basis. Similar to a SQL GROUP BY statement.

        Example:
        ::

            >>> tab.group_by(tab.key).select(tab.key, tab.value.avg)
            >>> tab.group_by("key").select("key, value.avg")

        :param fields: Group keys.
        :return: The grouped table.
        """
        if all(isinstance(f, Expression) for f in fields):
            return GroupedTable(self._j_table.groupBy(to_expression_jarray(fields)), self._t_env)
        else:
            assert len(fields) == 1
            assert isinstance(fields[0], str)
            return GroupedTable(self._j_table.groupBy(fields[0]), self._t_env)

    def distinct(self) -> 'Table':
        """
        Removes duplicate values and returns only distinct (different) values.

        Example:
        ::

            >>> tab.select(tab.key, tab.value).distinct()

        :return: The result table.
        """
        return Table(self._j_table.distinct(), self._t_env)

    def join(self, right: 'Table', join_predicate: Union[str, Expression[bool]] = None):
        """
        Joins two :class:`~pyflink.table.Table`. Similar to a SQL join. The fields of the two joined
        operations must not overlap, use :func:`~pyflink.table.Table.alias` to rename fields if
        necessary. You can use where and select clauses after a join to further specify the
        behaviour of the join.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment` .

        Example:
        ::

            >>> left.join(right).where((left.a == right.b) && (left.c > 3))
            >>> left.join(right).where("a = b && c > 3")
            >>> left.join(right, left.a == right.b)

        :param right: Right table.
        :param join_predicate: Optional, the join predicate expression string.
        :return: The result table.
        """
        if join_predicate is not None:
            return Table(self._j_table.join(
                right._j_table, _get_java_expression(join_predicate)), self._t_env)
        else:
            return Table(self._j_table.join(right._j_table), self._t_env)

    def left_outer_join(self,
                        right: 'Table',
                        join_predicate: Union[str, Expression[bool]] = None) -> 'Table':
        """
        Joins two :class:`~pyflink.table.Table`. Similar to a SQL left outer join. The fields of
        the two joined operations must not overlap, use :func:`~pyflink.table.Table.alias` to
        rename fields if necessary.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment` and its
            :class:`~pyflink.table.TableConfig` must have null check enabled (default).

        Example:
        ::

            >>> left.left_outer_join(right)
            >>> left.left_outer_join(right, left.a == right.b)
            >>> left.left_outer_join(right, "a = b")

        :param right: Right table.
        :param join_predicate: Optional, the join predicate expression string.
        :return: The result table.
        """
        if join_predicate is None:
            return Table(self._j_table.leftOuterJoin(right._j_table), self._t_env)
        else:
            return Table(self._j_table.leftOuterJoin(
                right._j_table, _get_java_expression(join_predicate)), self._t_env)

    def right_outer_join(self,
                         right: 'Table',
                         join_predicate: Union[str, Expression[bool]]) -> 'Table':
        """
        Joins two :class:`~pyflink.table.Table`. Similar to a SQL right outer join. The fields of
        the two joined operations must not overlap, use :func:`~pyflink.table.Table.alias` to
        rename fields if necessary.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment` and its
            :class:`~pyflink.table.TableConfig` must have null check enabled (default).

        Example:
        ::

            >>> left.right_outer_join(right, left.a == right.b)
            >>> left.right_outer_join(right, "a = b")

        :param right: Right table.
        :param join_predicate: The join predicate expression string.
        :return: The result table.
        """
        return Table(self._j_table.rightOuterJoin(
            right._j_table, _get_java_expression(join_predicate)), self._t_env)

    def full_outer_join(self,
                        right: 'Table',
                        join_predicate: Union[str, Expression[bool]]) -> 'Table':
        """
        Joins two :class:`~pyflink.table.Table`. Similar to a SQL full outer join. The fields of
        the two joined operations must not overlap, use :func:`~pyflink.table.Table.alias` to
        rename fields if necessary.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment` and its
            :class:`~pyflink.table.TableConfig` must have null check enabled (default).

        Example:
        ::

            >>> left.full_outer_join(right, left.a == right.b)
            >>> left.full_outer_join(right, "a = b")

        :param right: Right table.
        :param join_predicate: The join predicate expression string.
        :return: The result table.
        """
        return Table(self._j_table.fullOuterJoin(
            right._j_table, _get_java_expression(join_predicate)), self._t_env)

    def join_lateral(self,
                     table_function_call: Union[str, Expression],
                     join_predicate: Union[str, Expression[bool]] = None) -> 'Table':
        """
        Joins this Table with an user-defined TableFunction. This join is similar to a SQL inner
        join but works with a table function. Each row of the table is joined with the rows
        produced by the table function.

        Example:
        ::

            >>> t_env.create_java_temporary_system_function("split",
           ...     "java.table.function.class.name")
            >>> tab.join_lateral("split(text, ' ') as (b)", "a = b")

            >>> from pyflink.table import expressions as expr
            >>> tab.join_lateral(expr.call('split', ' ').alias('b'), expr.col('a') == expr.col('b'))

        :param table_function_call: An expression representing a table function call.
        :param join_predicate: Optional, The join predicate expression string, join ON TRUE if not
                               exist.
        :return: The result Table.
        """
        if join_predicate is None:
            return Table(self._j_table.joinLateral(
                _get_java_expression(table_function_call)), self._t_env)
        else:
            return Table(self._j_table.joinLateral(
                _get_java_expression(table_function_call),
                _get_java_expression(join_predicate)),
                self._t_env)

    def left_outer_join_lateral(self,
                                table_function_call: Union[str, Expression],
                                join_predicate: Union[str, Expression[bool]] = None) -> 'Table':
        """
        Joins this Table with an user-defined TableFunction. This join is similar to
        a SQL left outer join but works with a table function. Each row of the table is joined
        with all rows produced by the table function. If the join does not produce any row, the
        outer row is padded with nulls.

        Example:
        ::

            >>> t_env.create_java_temporary_system_function("split",
            ...     "java.table.function.class.name")
            >>> tab.left_outer_join_lateral("split(text, ' ') as (b)")
            >>> from pyflink.table import expressions as expr
            >>> tab.left_outer_join_lateral(expr.call('split', ' ').alias('b'))

        :param table_function_call: An expression representing a table function call.
        :param join_predicate: Optional, The join predicate expression string, join ON TRUE if not
                               exist.
        :return: The result Table.
        """
        if join_predicate is None:
            return Table(self._j_table.leftOuterJoinLateral(
                _get_java_expression(table_function_call)), self._t_env)
        else:
            return Table(self._j_table.leftOuterJoinLateral(
                _get_java_expression(table_function_call),
                _get_java_expression(join_predicate)),
                self._t_env)

    def minus(self, right: 'Table') -> 'Table':
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
        :return: The result table.
        """
        return Table(self._j_table.minus(right._j_table), self._t_env)

    def minus_all(self, right: 'Table') -> 'Table':
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
        :return: The result table.
        """
        return Table(self._j_table.minusAll(right._j_table), self._t_env)

    def union(self, right: 'Table') -> 'Table':
        """
        Unions two :class:`~pyflink.table.Table` with duplicate records removed.
        Similar to a SQL UNION. The fields of the two union operations must fully overlap.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment`.

        Example:
        ::

            >>> left.union(right)

        :param right: Right table.
        :return: The result table.
        """
        return Table(self._j_table.union(right._j_table), self._t_env)

    def union_all(self, right: 'Table') -> 'Table':
        """
        Unions two :class:`~pyflink.table.Table`. Similar to a SQL UNION ALL. The fields of the
        two union operations must fully overlap.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment`.

        Example:
        ::

            >>> left.union_all(right)

        :param right: Right table.
        :return: The result table.
        """
        return Table(self._j_table.unionAll(right._j_table), self._t_env)

    def intersect(self, right: 'Table') -> 'Table':
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
        :return: The result table.
        """
        return Table(self._j_table.intersect(right._j_table), self._t_env)

    def intersect_all(self, right: 'Table') -> 'Table':
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
        :return: The result table.
        """
        return Table(self._j_table.intersectAll(right._j_table), self._t_env)

    def order_by(self, *fields: Union[str, Expression]) -> 'Table':
        """
        Sorts the given :class:`~pyflink.table.Table`. Similar to SQL ORDER BY.
        The resulting Table is sorted globally sorted across all parallel partitions.

        Example:
        ::

            >>> tab.order_by(tab.name.desc)
            >>> tab.order_by("name.desc")

        For unbounded tables, this operation requires a sorting on a time attribute or a subsequent
        fetch operation.

        :param fields: Order fields expression string.
        :return: The result table.
        """
        if all(isinstance(f, Expression) for f in fields):
            return Table(self._j_table.orderBy(to_expression_jarray(fields)), self._t_env)
        else:
            assert len(fields) == 1
            assert isinstance(fields[0], str)
            return Table(self._j_table.orderBy(fields[0]), self._t_env)

    def offset(self, offset: int) -> 'Table':
        """
        Limits a (possibly sorted) result from an offset position.

        This method can be combined with a preceding :func:`~pyflink.table.Table.order_by` call for
        a deterministic order and a subsequent :func:`~pyflink.table.Table.fetch` call to return n
        rows after skipping the first o rows.

        Example:
        ::

            # skips the first 3 rows and returns all following rows.
            >>> tab.order_by(tab.name.desc).offset(3)
            >>> tab.order_by("name.desc").offset(3)
            # skips the first 10 rows and returns the next 5 rows.
            >>> tab.order_by(tab.name.desc).offset(10).fetch(5)

        For unbounded tables, this operation requires a subsequent fetch operation.

        :param offset: Number of records to skip.
        :return: The result table.
        """
        return Table(self._j_table.offset(offset), self._t_env)

    def fetch(self, fetch: int) -> 'Table':
        """
        Limits a (possibly sorted) result to the first n rows.

        This method can be combined with a preceding :func:`~pyflink.table.Table.order_by` call for
        a deterministic order and :func:`~pyflink.table.Table.offset` call to return n rows after
        skipping the first o rows.

        Example:

        Returns the first 3 records.
        ::

            >>> tab.order_by(tab.name.desc).fetch(3)
            >>> tab.order_by("name.desc").fetch(3)

        Skips the first 10 rows and returns the next 5 rows.
        ::

            >>> tab.order_by(tab.name.desc).offset(10).fetch(5)

        :param fetch: The number of records to return. Fetch must be >= 0.
        :return: The result table.
        """
        return Table(self._j_table.fetch(fetch), self._t_env)

    def limit(self, fetch: int, offset: int = 0) -> 'Table':
        """
        Limits a (possibly sorted) result to the first n rows.

        This method is a synonym for :func:`~pyflink.table.Table.offset` followed by
        :func:`~pyflink.table.Table.fetch`.

        Example:

        Returns the first 3 records.
        ::

            >>> tab.limit(3)

        Skips the first 10 rows and returns the next 5 rows.
        ::

            >>> tab.limit(5, 10)

        :param fetch: the first number of rows to fetch.
        :param offset: the number of records to skip, default 0.
        :return: The result table.
        """
        return self.offset(offset).fetch(fetch)

    def window(self, window: GroupWindow) -> 'GroupWindowedTable':
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

            >>> from pyflink.table import expressions as expr
            >>> tab.window(Tumble.over(expr.lit(10).minutes).on(tab.rowtime).alias('w')) \\
            ...     .group_by(col('w')) \\
            ...     .select(tab.a.sum.alias('a'),
            ...             col('w').start.alias('b'),
            ...             col('w').end.alias('c'),
            ...             col('w').rowtime.alias('d'))

        :param window: A :class:`~pyflink.table.window.GroupWindow` created from
                       :class:`~pyflink.table.window.Tumble`, :class:`~pyflink.table.window.Session`
                       or :class:`~pyflink.table.window.Slide`.
        :return: A group windowed table.
        """
        return GroupWindowedTable(self._j_table.window(window._java_window), self._t_env)

    def over_window(self, *over_windows: OverWindow) -> 'OverWindowedTable':
        """
        Defines over-windows on the records of a table.

        An over-window defines for each record an interval of records over which aggregation
        functions can be computed.

        Example:
        ::

            >>> from pyflink.table import expressions as expr
            >>> tab.over_window(Over.partition_by(tab.c).order_by(tab.rowtime) \\
            ...     .preceding(lit(10).seconds).alias("ow")) \\
            ...     .select(tab.c, tab.b.count.over(col('ow'), tab.e.sum.over(col('ow'))))

        .. note::

            Computing over window aggregates on a streaming table is only a parallel
            operation if the window is partitioned. Otherwise, the whole stream will be processed
            by a single task, i.e., with parallelism 1.

        .. note::

            Over-windows for batch tables are currently not supported.

        :param over_windows: over windows created from :class:`~pyflink.table.window.Over`.
        :return: A over windowed table.
        """
        gateway = get_gateway()
        window_array = to_jarray(gateway.jvm.OverWindow,
                                 [item._java_over_window for item in over_windows])
        return OverWindowedTable(self._j_table.window(window_array), self._t_env)

    def add_columns(self, *fields: Union[str, Expression]) -> 'Table':
        """
        Adds additional columns. Similar to a SQL SELECT statement. The field expressions
        can contain complex expressions, but can not contain aggregations. It will throw an
        exception if the added fields already exist.

        Example:
        ::

            >>> from pyflink.table import expressions as expr
            >>> tab.add_columns((tab.a + 1).alias('a1'), expr.concat(tab.b, 'sunny').alias('b1'))
            >>> tab.add_columns("a + 1 as a1, concat(b, 'sunny') as b1")

        :param fields: Column list string.
        :return: The result table.
        """
        if all(isinstance(f, Expression) for f in fields):
            return Table(self._j_table.addColumns(to_expression_jarray(fields)), self._t_env)
        else:
            assert len(fields) == 1
            assert isinstance(fields[0], str)
            return Table(self._j_table.addColumns(fields[0]), self._t_env)

    def add_or_replace_columns(self, *fields: Union[str, Expression]) -> 'Table':
        """
        Adds additional columns. Similar to a SQL SELECT statement. The field expressions
        can contain complex expressions, but can not contain aggregations. Existing fields will be
        replaced if add columns name is the same as the existing column name. Moreover, if the added
        fields have duplicate field name, then the last one is used.

        Example:
        ::

            >>> from pyflink.table import expressions as expr
            >>> tab.add_or_replace_columns((tab.a + 1).alias('a1'),
            ...                            expr.concat(tab.b, 'sunny').alias('b1'))
            >>> tab.add_or_replace_columns("a + 1 as a1, concat(b, 'sunny') as b1")

        :param fields: Column list string.
        :return: The result table.
        """
        if all(isinstance(f, Expression) for f in fields):
            return Table(self._j_table.addOrReplaceColumns(to_expression_jarray(fields)),
                         self._t_env)
        else:
            assert len(fields) == 1
            assert isinstance(fields[0], str)
            return Table(self._j_table.addOrReplaceColumns(fields[0]), self._t_env)

    def rename_columns(self, *fields: Union[str, Expression]) -> 'Table':
        """
        Renames existing columns. Similar to a field alias statement. The field expressions
        should be alias expressions, and only the existing fields can be renamed.

        Example:
        ::

            >>> tab.rename_columns(tab.a.alias('a1'), tab.b.alias('b1'))
            >>> tab.rename_columns("a as a1, b as b1")

        :param fields: Column list string.
        :return: The result table.
        """
        if all(isinstance(f, Expression) for f in fields):
            return Table(self._j_table.renameColumns(to_expression_jarray(fields)),
                         self._t_env)
        else:
            assert len(fields) == 1
            assert isinstance(fields[0], str)
            return Table(self._j_table.renameColumns(fields[0]), self._t_env)

    def drop_columns(self, *fields: Union[str, Expression]) -> 'Table':
        """
        Drops existing columns. The field expressions should be field reference expressions.

        Example:
        ::

            >>> tab.drop_columns(tab.a, tab.b)
            >>> tab.drop_columns("a, b")

        :param fields: Column list string.
        :return: The result table.
        """
        if all(isinstance(f, Expression) for f in fields):
            return Table(self._j_table.dropColumns(to_expression_jarray(fields)),
                         self._t_env)
        else:
            assert len(fields) == 1
            assert isinstance(fields[0], str)
            return Table(self._j_table.dropColumns(fields[0]), self._t_env)

    def map(self, func: Union[str, Expression]) -> 'Table':
        """
        Performs a map operation with a user-defined scalar function.

        Example:
        ::

            >>> add = udf(lambda x: Row(x + 1, x * x), result_type=DataTypes.Row(
            ... [DataTypes.FIELD("a", DataTypes.INT()), DataTypes.FIELD("b", DataTypes.INT())]))
            >>> tab.map(add(tab.a)).alias("a, b")

        :param func: user-defined scalar function.
        :return: The result table.

        .. versionadded:: 1.13.0
        """
        if isinstance(func, str):
            return Table(self._j_table.map(func), self._t_env)
        else:
            return Table(self._j_table.map(func._j_expr), self._t_env)

    def flat_map(self, func: Union[str, Expression]) -> 'Table':
        """
        Performs a flatMap operation with a user-defined table function.

        Example:
        ::

            >>> @udtf(result_types=[DataTypes.INT(), DataTypes.STRING()])
            ... def split(x, string):
            ...     for s in string.split(","):
            ...         yield x, s
            >>> tab.flat_map(split(tab.a, table.b))

        :param func: user-defined table function.
        :return: The result table.

        .. versionadded:: 1.13.0
        """
        if isinstance(func, str):
            return Table(self._j_table.flatMap(func), self._t_env)
        else:
            return Table(self._j_table.flatMap(func._j_expr), self._t_env)

    def aggregate(self, func: Union[str, Expression]) -> 'AggregatedTable':
        """
        Performs a global aggregate operation with an aggregate function. You have to close the
        aggregate with a select statement.

        Example:
        ::

            >>> agg = udaf(lambda a: (a.mean(), a.max()),
            ...               result_type=DataTypes.ROW(
            ...                   [DataTypes.FIELD("a", DataTypes.FLOAT()),
            ...                    DataTypes.FIELD("b", DataTypes.INT())]),
            ...               func_type="pandas")
            >>> tab.aggregate(agg(tab.a).alias("a", "b")).select("a, b")

        :param func: user-defined aggregate function.
        :return: The result table.

        .. versionadded:: 1.13.0
        """
        if isinstance(func, str):
            return AggregatedTable(self._j_table.aggregate(func), self._t_env)
        else:
            return AggregatedTable(self._j_table.aggregate(func._j_expr), self._t_env)

    def flat_aggregate(self, func: Union[str, Expression]) -> 'FlatAggregateTable':
        """
        Perform a global flat_aggregate without group_by. flat_aggregate takes a
        :class:`~pyflink.table.TableAggregateFunction` which returns multiple rows. Use a selection
        after the flat_aggregate.

        Example:
        ::

            >>> table_agg = udtaf(MyTableAggregateFunction())
            >>> tab.flat_aggregate(table_agg(tab.a).alias("a", "b")).select("a, b")

        :param func: user-defined table aggregate function.
        :return: The result table.

        .. versionadded:: 1.13.0
        """
        if isinstance(func, str):
            return FlatAggregateTable(self._j_table.flatAggregate(func), self._t_env)
        else:
            return FlatAggregateTable(self._j_table.flatAggregate(func._j_expr), self._t_env)

    def insert_into(self, table_path: str):
        """
        Writes the :class:`~pyflink.table.Table` to a :class:`~pyflink.table.TableSink` that was
        registered under the specified name. For the path resolution algorithm see
        :func:`~TableEnvironment.use_database`.

        Example:
        ::

            >>> tab.insert_into("sink")

        :param table_path: The path of the registered :class:`~pyflink.table.TableSink` to which
               the :class:`~pyflink.table.Table` is written.

        .. note:: Deprecated in 1.11. Use :func:`execute_insert` for single sink,
                  use :class:`TableTableEnvironment`#:func:`create_statement_set`
                  for multiple sinks.
        """
        warnings.warn("Deprecated in 1.11. Use execute_insert for single sink, "
                      "use TableTableEnvironment#create_statement_set for multiple sinks.",
                      DeprecationWarning)
        self._j_table.insertInto(table_path)

    def to_pandas(self):
        """
        Converts the table to a pandas DataFrame. It will collect the content of the table to
        the client side and so please make sure that the content of the table could fit in memory
        before calling this method.

        Example:
        ::

            >>> pdf = pd.DataFrame(np.random.rand(1000, 2))
            >>> table = table_env.from_pandas(pdf, ["a", "b"])
            >>> table.filter(table.a > 0.5).to_pandas()

        :return: the result pandas DataFrame.

        .. versionadded:: 1.11.0
        """
        self._t_env._before_execute()
        gateway = get_gateway()
        max_arrow_batch_size = self._j_table.getTableEnvironment().getConfig().getConfiguration()\
            .getInteger(gateway.jvm.org.apache.flink.python.PythonOptions.MAX_ARROW_BATCH_SIZE)
        batches = gateway.jvm.org.apache.flink.table.runtime.arrow.ArrowUtils\
            .collectAsPandasDataFrame(self._j_table, max_arrow_batch_size)
        if batches.hasNext():
            import pytz
            timezone = pytz.timezone(
                self._j_table.getTableEnvironment().getConfig().getLocalTimeZone().getId())
            serializer = ArrowSerializer(
                create_arrow_schema(self.get_schema().get_field_names(),
                                    self.get_schema().get_field_data_types()),
                self.get_schema().to_row_data_type(),
                timezone)
            import pyarrow as pa
            table = pa.Table.from_batches(serializer.load_from_iterator(batches))
            pdf = table.to_pandas()

            schema = self.get_schema()
            for field_name in schema.get_field_names():
                pdf[field_name] = tz_convert_from_internal(
                    pdf[field_name], schema.get_field_data_type(field_name), timezone)
            return pdf
        else:
            import pandas as pd
            return pd.DataFrame.from_records([], columns=self.get_schema().get_field_names())

    def get_schema(self) -> 'TableSchema':
        """
        Returns the :class:`~pyflink.table.TableSchema` of this table.

        :return: The schema of this table.
        """
        return TableSchema(j_table_schema=self._j_table.getSchema())

    def print_schema(self):
        """
        Prints the schema of this table to the console in a tree format.
        """
        self._j_table.printSchema()

    def execute_insert(self, table_path: str, overwrite: bool = False) -> 'TableResult':
        """
        Writes the :class:`~pyflink.table.Table` to a :class:`~pyflink.table.TableSink` that was
        registered under the specified name, and then execute the insert operation.
        For the path resolution algorithm see :func:`~TableEnvironment.use_database`.

        Example:
        ::

            >>> tab.execute_insert("sink")

        :param table_path: The path of the registered :class:`~pyflink.table.TableSink` to which
               the :class:`~pyflink.table.Table` is written.
        :param overwrite: The flag that indicates whether the insert should overwrite
               existing data or not.
        :return: The table result.

        .. versionadded:: 1.11.0
        """
        self._t_env._before_execute()
        return TableResult(self._j_table.executeInsert(table_path, overwrite))

    def execute(self) -> 'TableResult':
        """
        Collects the contents of the current table local client.

        Example:
        ::

            >>> tab.execute()

        :return: The content of the table.

        .. versionadded:: 1.11.0
        """
        self._t_env._before_execute()
        return TableResult(self._j_table.execute())

    def explain(self, *extra_details: ExplainDetail) -> str:
        """
        Returns the AST of this table and the execution plan.

        :param extra_details: The extra explain details which the explain result should include,
                              e.g. estimated cost, changelog mode for streaming
        :return: The statement for which the AST and execution plan will be returned.

        .. versionadded:: 1.11.0
        """
        j_extra_details = to_j_explain_detail_arr(extra_details)
        return self._j_table.explain(j_extra_details)


class GroupedTable(object):
    """
    A table that has been grouped on a set of grouping keys.
    """

    def __init__(self, java_table, t_env):
        self._j_table = java_table
        self._t_env = t_env

    def select(self, *fields: Union[str, Expression]) -> 'Table':
        """
        Performs a selection operation on a grouped table. Similar to an SQL SELECT statement.
        The field expressions can contain complex expressions and aggregations.

        Example:
        ::

            >>> tab.group_by(tab.key).select(tab.key, tab.value.avg.alias('average'))
            >>> tab.group_by("key").select("key, value.avg as average")


        :param fields: Expression string that contains group keys and aggregate function calls.
        :return: The result table.
        """
        if all(isinstance(f, Expression) for f in fields):
            return Table(self._j_table.select(to_expression_jarray(fields)), self._t_env)
        else:
            assert len(fields) == 1
            assert isinstance(fields[0], str)
            return Table(self._j_table.select(fields[0]), self._t_env)

    def aggregate(self, func: Union[str, Expression]) -> 'AggregatedTable':
        """
        Performs a aggregate operation with an aggregate function. You have to close the
        aggregate with a select statement.

        Example:
        ::

            >>> agg = udaf(lambda a: (a.mean(), a.max()),
            ...               result_type=DataTypes.ROW(
            ...                   [DataTypes.FIELD("a", DataTypes.FLOAT()),
            ...                    DataTypes.FIELD("b", DataTypes.INT())]),
            ...               func_type="pandas")
            >>> tab.group_by(tab.a).aggregate(agg(tab.b).alias("c", "d")).select("a, c, d")

        :param func: user-defined aggregate function.
        :return: The result table.

        .. versionadded:: 1.13.0
        """
        if isinstance(func, str):
            return AggregatedTable(self._j_table.aggregate(func), self._t_env)
        else:
            return AggregatedTable(self._j_table.aggregate(func._j_expr), self._t_env)

    def flat_aggregate(self, func: Union[str, Expression]) -> 'FlatAggregateTable':
        """
        Performs a flat_aggregate operation on a grouped table. flat_aggregate takes a
        :class:`~pyflink.table.TableAggregateFunction` which returns multiple rows. Use a selection
        after flatAggregate.

        Example:
        ::

            >>> table_agg = udtaf(MyTableAggregateFunction())
            >>> tab.group_by(tab.c).flat_aggregate(table_agg(tab.a).alias("a")).select("c, a")

        :param func: user-defined table aggregate function.
        :return: The result table.

        .. versionadded:: 1.13.0
        """
        if isinstance(func, str):
            return FlatAggregateTable(self._j_table.flatAggregate(func), self._t_env)
        else:
            return FlatAggregateTable(self._j_table.flatAggregate(func._j_expr), self._t_env)


class GroupWindowedTable(object):
    """
    A table that has been windowed for :class:`~pyflink.table.GroupWindow`.
    """

    def __init__(self, java_group_windowed_table, t_env):
        self._j_table = java_group_windowed_table
        self._t_env = t_env

    def group_by(self, *fields: Union[str, Expression]) -> 'WindowGroupedTable':
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

            >>> from pyflink.table import expressions as expr
            >>> tab.window(Tumble.over(expr.lit(10).minutes).on(tab.rowtime).alias('w')) \\
            ...     .group_by(col('w')) \\
            ...     .select(tab.a.sum.alias('a'),
            ...             col('w').start.alias('b'),
            ...             col('w').end.alias('c'),
            ...             col('w').rowtime.alias('d'))

        :param fields: Group keys.
        :return: A window grouped table.
        """
        if all(isinstance(f, Expression) for f in fields):
            return WindowGroupedTable(
                self._j_table.groupBy(to_expression_jarray(fields)), self._t_env)
        else:
            assert len(fields) == 1
            assert isinstance(fields[0], str)
            return WindowGroupedTable(self._j_table.groupBy(fields[0]), self._t_env)


class WindowGroupedTable(object):
    """
    A table that has been windowed and grouped for :class:`~pyflink.table.window.GroupWindow`.
    """

    def __init__(self, java_window_grouped_table, t_env):
        self._j_table = java_window_grouped_table
        self._t_env = t_env

    def select(self, *fields: Union[str, Expression]) -> 'Table':
        """
        Performs a selection operation on a window grouped table. Similar to an SQL SELECT
        statement.
        The field expressions can contain complex expressions and aggregations.

        Example:
        ::

            >>> window_grouped_table.select(col('key'),
            ...                             col('window').start,
            ...                             col('value').avg.alias('valavg'))
            >>> window_grouped_table.select("key, window.start, value.avg as valavg")

        :param fields: Expression string.
        :return: The result table.
        """
        if all(isinstance(f, Expression) for f in fields):
            return Table(self._j_table.select(to_expression_jarray(fields)), self._t_env)
        else:
            assert len(fields) == 1
            assert isinstance(fields[0], str)
            return Table(self._j_table.select(fields[0]), self._t_env)

    def aggregate(self, func: Union[str, Expression]) -> 'AggregatedTable':
        """
        Performs an aggregate operation on a window grouped table. You have to close the
        aggregate with a select statement.

        Example:
        ::

            >>> agg = udaf(lambda a: (a.mean(), a.max()),
            ...               result_type=DataTypes.ROW(
            ...                   [DataTypes.FIELD("a", DataTypes.FLOAT()),
            ...                    DataTypes.FIELD("b", DataTypes.INT())]),
            ...               func_type="pandas")
            >>> window_grouped_table.group_by("w") \
            ...     .aggregate(agg(window_grouped_table.b) \
            ...     .alias("c", "d")) \
            ...     .select("c, d")

        :param func: user-defined aggregate function.
        :return: The result table.

        .. versionadded:: 1.13.0
        """
        if isinstance(func, str):
            return AggregatedTable(self._j_table.aggregate(func), self._t_env)
        else:
            return AggregatedTable(self._j_table.aggregate(func._j_expr), self._t_env)


class OverWindowedTable(object):
    """
    A table that has been windowed for :class:`~pyflink.table.window.OverWindow`.

    Unlike group windows, which are specified in the GROUP BY clause, over windows do not collapse
    rows. Instead over window aggregates compute an aggregate for each input row over a range of
    its neighboring rows.
    """

    def __init__(self, java_over_windowed_table, t_env):
        self._j_table = java_over_windowed_table
        self._t_env = t_env

    def select(self, *fields: Union[str, Expression]) -> 'Table':
        """
        Performs a selection operation on a over windowed table. Similar to an SQL SELECT
        statement.
        The field expressions can contain complex expressions and aggregations.

        Example:
        ::

            >>> over_windowed_table.select(col('c'),
            ...                            col('b').count.over(col('ow')),
            ...                            col('e').sum.over(col('ow')))
            >>> over_windowed_table.select("c, b.count over ow, e.sum over ow")

        :param fields: Expression string.
        :return: The result table.
        """
        if all(isinstance(f, Expression) for f in fields):
            return Table(self._j_table.select(to_expression_jarray(fields)), self._t_env)
        else:
            assert len(fields) == 1
            assert isinstance(fields[0], str)
            return Table(self._j_table.select(fields[0]), self._t_env)


class AggregatedTable(object):
    """
    A table that has been performed on the aggregate function.
    """

    def __init__(self, java_table, t_env):
        self._j_table = java_table
        self._t_env = t_env

    def select(self, *fields: Union[str, Expression]) -> 'Table':
        """
        Performs a selection operation after an aggregate operation. The field expressions
        cannot contain table functions and aggregations.

        Example:
        ""

            >>> agg = udaf(lambda a: (a.mean(), a.max()),
            ...               result_type=DataTypes.ROW(
            ...                   [DataTypes.FIELD("a", DataTypes.FLOAT()),
            ...                    DataTypes.FIELD("b", DataTypes.INT())]),
            ...               func_type="pandas")
            >>> tab.aggregate(agg(tab.a).alias("a", "b")).select("a, b")

        :param fields: Expression string.
        :return: The result table.
        """
        if all(isinstance(f, Expression) for f in fields):
            return Table(self._j_table.select(to_expression_jarray(fields)), self._t_env)
        else:
            assert len(fields) == 1
            assert isinstance(fields[0], str)
            return Table(self._j_table.select(fields[0]), self._t_env)


class FlatAggregateTable(object):
    """
    A table that performs flatAggregate on a :class:`~pyflink.table.Table`, a
    :class:`~pyflink.table.GroupedTable` or a :class:`~pyflink.table.WindowGroupedTable`
    """

    def __init__(self, java_table, t_env):
        self._j_table = java_table
        self._t_env = t_env

    def select(self, *fields: Union[str, Expression]) -> 'Table':
        """
        Performs a selection operation on a FlatAggregateTable. Similar to a SQL SELECT statement.
        The field expressions can contain complex expressions.

        Example:
        ::

            >>> table_agg = udtaf(MyTableAggregateFunction())
            >>> tab.flat_aggregate(table_agg(tab.a).alias("a", "b")).select("a, b")

        :param fields: Expression string.
        :return: The result table.
        """
        if all(isinstance(f, Expression) for f in fields):
            return Table(self._j_table.select(to_expression_jarray(fields)), self._t_env)
        else:
            assert len(fields) == 1
            assert isinstance(fields[0], str)
            return Table(self._j_table.select(fields[0]), self._t_env)
