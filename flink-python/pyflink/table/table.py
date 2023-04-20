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
from typing import Union

from pyflink.java_gateway import get_gateway
from pyflink.table import ExplainDetail
from pyflink.table.expression import Expression, _get_java_expression
from pyflink.table.expressions import col, with_columns, without_columns
from pyflink.table.serializers import ArrowSerializer
from pyflink.table.table_descriptor import TableDescriptor
from pyflink.table.table_result import TableResult
from pyflink.table.table_schema import TableSchema
from pyflink.table.types import create_arrow_schema
from pyflink.table.udf import UserDefinedScalarFunctionWrapper, \
    UserDefinedAggregateFunctionWrapper, UserDefinedTableFunctionWrapper
from pyflink.table.utils import tz_convert_from_internal, to_expression_jarray
from pyflink.table.window import OverWindow, GroupWindow

from pyflink.util.java_utils import to_jarray
from pyflink.util.java_utils import to_j_explain_detail_arr

__all__ = ['Table', 'GroupedTable', 'GroupWindowedTable', 'OverWindowedTable', 'WindowGroupedTable']


class Table(object):
    """
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

    def select(self, *fields: Expression) -> 'Table':
        """
        Performs a selection operation. Similar to a SQL SELECT statement. The field expressions
        can contain complex expressions.

        Example:
        ::

            >>> from pyflink.table.expressions import col, concat
            >>> tab.select(tab.key, concat(tab.value, 'hello'))
            >>> tab.select(col('key'), concat(col('value'), 'hello'))

        :return: The result table.
        """
        return Table(self._j_table.select(to_expression_jarray(fields)), self._t_env)

    def alias(self, field: str, *fields: str) -> 'Table':
        """
        Renames the fields of the expression result. Use this to disambiguate fields before
        joining two tables.

        Example:
        ::

            >>> tab.alias("a", "b", "c")

        :param field: Field alias.
        :param fields: Additional field aliases.
        :return: The result table.
        """
        gateway = get_gateway()
        extra_fields = to_jarray(gateway.jvm.String, fields)
        return Table(get_method(self._j_table, "as")(field, extra_fields), self._t_env)

    def filter(self, predicate: Expression[bool]) -> 'Table':
        """
        Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
        clause.

        Example:
        ::

            >>> tab.filter(col('name') == 'Fred')

        :param predicate: Predicate expression string.
        :return: The result table.
        """
        return Table(self._j_table.filter(_get_java_expression(predicate)), self._t_env)

    def where(self, predicate: Expression[bool]) -> 'Table':
        """
        Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
        clause.

        Example:
        ::

            >>> tab.where(col('name') == 'Fred')

        :param predicate: Predicate expression string.
        :return: The result table.
        """
        return Table(self._j_table.where(_get_java_expression(predicate)), self._t_env)

    def group_by(self, *fields: Expression) -> 'GroupedTable':
        """
        Groups the elements on some grouping keys. Use this before a selection with aggregations
        to perform the aggregation on a per-group basis. Similar to a SQL GROUP BY statement.

        Example:
        ::

            >>> tab.group_by(col('key')).select(col('key'), col('value').avg)

        :param fields: Group keys.
        :return: The grouped table.
        """
        return GroupedTable(self._j_table.groupBy(to_expression_jarray(fields)), self._t_env)

    def distinct(self) -> 'Table':
        """
        Removes duplicate values and returns only distinct (different) values.

        Example:
        ::

            >>> tab.select(col('key'), col('value')).distinct()

        :return: The result table.
        """
        return Table(self._j_table.distinct(), self._t_env)

    def join(self, right: 'Table', join_predicate: Expression[bool] = None):
        """
        Joins two :class:`~pyflink.table.Table`. Similar to a SQL join. The fields of the two joined
        operations must not overlap, use :func:`~pyflink.table.Table.alias` to rename fields if
        necessary. You can use where and select clauses after a join to further specify the
        behaviour of the join.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment` .

        Example:
        ::

            >>> left.join(right).where((col('a') == col('b')) && (col('c') > 3))
            >>> left.join(right, col('a') == col('b'))

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
                        join_predicate: Expression[bool] = None) -> 'Table':
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
            >>> left.left_outer_join(right, col('a') == col('b'))

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
                         join_predicate: Expression[bool]) -> 'Table':
        """
        Joins two :class:`~pyflink.table.Table`. Similar to a SQL right outer join. The fields of
        the two joined operations must not overlap, use :func:`~pyflink.table.Table.alias` to
        rename fields if necessary.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment` and its
            :class:`~pyflink.table.TableConfig` must have null check enabled (default).

        Example:
        ::

            >>> left.right_outer_join(right, col('a') == col('b'))

        :param right: Right table.
        :param join_predicate: The join predicate expression string.
        :return: The result table.
        """
        return Table(self._j_table.rightOuterJoin(
            right._j_table, _get_java_expression(join_predicate)), self._t_env)

    def full_outer_join(self,
                        right: 'Table',
                        join_predicate: Expression[bool]) -> 'Table':
        """
        Joins two :class:`~pyflink.table.Table`. Similar to a SQL full outer join. The fields of
        the two joined operations must not overlap, use :func:`~pyflink.table.Table.alias` to
        rename fields if necessary.

        .. note::

            Both tables must be bound to the same :class:`~pyflink.table.TableEnvironment` and its
            :class:`~pyflink.table.TableConfig` must have null check enabled (default).

        Example:
        ::

            >>> left.full_outer_join(right, col('a') == col('b'))

        :param right: Right table.
        :param join_predicate: The join predicate expression string.
        :return: The result table.
        """
        return Table(self._j_table.fullOuterJoin(
            right._j_table, _get_java_expression(join_predicate)), self._t_env)

    def join_lateral(self,
                     table_function_call: Union[Expression, UserDefinedTableFunctionWrapper],
                     join_predicate: Expression[bool] = None) -> 'Table':
        """
        Joins this Table with an user-defined TableFunction. This join is similar to a SQL inner
        join but works with a table function. Each row of the table is joined with the rows
        produced by the table function.

        Example:
        ::

            >>> from pyflink.table.expressions import *
            >>> t_env.create_java_temporary_system_function("split",
            ...     "java.table.function.class.name")
            >>> tab.join_lateral(call('split', ' ').alias('b'), col('a') == col('b'))
            >>> # take all the columns as inputs
            >>> @udtf(result_types=[DataTypes.INT(), DataTypes.STRING()])
            ... def split_row(row: Row):
            ...     for s in row[1].split(","):
            ...         yield row[0], s
            >>> tab.join_lateral(split_row.alias("a", "b"))

        :param table_function_call: An expression representing a table function call.
        :param join_predicate: Optional, The join predicate expression string, join ON TRUE if not
                               exist.
        :return: The result Table.
        """
        if isinstance(table_function_call, UserDefinedTableFunctionWrapper):
            table_function_call._set_takes_row_as_input()
            if hasattr(table_function_call, "_alias_names"):
                alias_names = getattr(table_function_call, "_alias_names")
                table_function_call = table_function_call(with_columns(col("*"))) \
                    .alias(*alias_names)
            else:
                raise AttributeError('table_function_call must be followed by a alias function'
                                     'e.g. table_function.alias("a", "b")')
        if join_predicate is None:
            return Table(self._j_table.joinLateral(
                _get_java_expression(table_function_call)), self._t_env)
        else:
            return Table(self._j_table.joinLateral(
                _get_java_expression(table_function_call),
                _get_java_expression(join_predicate)),
                self._t_env)

    def left_outer_join_lateral(self,
                                table_function_call: Union[Expression,
                                                           UserDefinedTableFunctionWrapper],
                                join_predicate: Expression[bool] = None) -> 'Table':
        """
        Joins this Table with an user-defined TableFunction. This join is similar to
        a SQL left outer join but works with a table function. Each row of the table is joined
        with all rows produced by the table function. If the join does not produce any row, the
        outer row is padded with nulls.

        Example:
        ::

            >>> t_env.create_java_temporary_system_function("split",
            ...     "java.table.function.class.name")
            >>> from pyflink.table.expressions import *
            >>> tab.left_outer_join_lateral(call('split', ' ').alias('b'))

            >>> # take all the columns as inputs
            >>> @udtf(result_types=[DataTypes.INT(), DataTypes.STRING()])
            ... def split_row(row: Row):
            ...     for s in row[1].split(","):
            ...         yield row[0], s
            >>> tab.left_outer_join_lateral(split_row.alias("a", "b"))

        :param table_function_call: An expression representing a table function call.
        :param join_predicate: Optional, The join predicate expression string, join ON TRUE if not
                               exist.
        :return: The result Table.
        """
        if isinstance(table_function_call, UserDefinedTableFunctionWrapper):
            table_function_call._set_takes_row_as_input()
            if hasattr(table_function_call, "_alias_names"):
                alias_names = getattr(table_function_call, "_alias_names")
                table_function_call = table_function_call(with_columns(col("*"))) \
                    .alias(*alias_names)
            else:
                raise AttributeError('table_function_call must be followed by a alias function'
                                     'e.g. table_function.alias("a", "b")')
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

    def order_by(self, *fields: Expression) -> 'Table':
        """
        Sorts the given :class:`~pyflink.table.Table`. Similar to SQL ORDER BY.
        The resulting Table is sorted globally sorted across all parallel partitions.

        Example:
        ::

            >>> tab.order_by(col('name').desc)

        For unbounded tables, this operation requires a sorting on a time attribute or a subsequent
        fetch operation.

        :param fields: Order fields expression string.
        :return: The result table.
        """
        return Table(self._j_table.orderBy(to_expression_jarray(fields)), self._t_env)

    def offset(self, offset: int) -> 'Table':
        """
        Limits a (possibly sorted) result from an offset position.

        This method can be combined with a preceding :func:`~pyflink.table.Table.order_by` call for
        a deterministic order and a subsequent :func:`~pyflink.table.Table.fetch` call to return n
        rows after skipping the first o rows.

        Example:
        ::

            # skips the first 3 rows and returns all following rows.
            >>> tab.order_by(col('name').desc).offset(3)
            # skips the first 10 rows and returns the next 5 rows.
            >>> tab.order_by(col('name').desc).offset(10).fetch(5)

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

            >>> tab.order_by(col('name').desc).fetch(3)

        Skips the first 10 rows and returns the next 5 rows.
        ::

            >>> tab.order_by(col('name').desc).offset(10).fetch(5)

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

            >>> from pyflink.table.expressions import col, lit
            >>> tab.window(Tumble.over(lit(10).minutes).on(col('rowtime')).alias('w')) \\
            ...     .group_by(col('w')) \\
            ...     .select(col('a').sum.alias('a'),
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

            >>> from pyflink.table.expressions import col, lit
            >>> tab.over_window(Over.partition_by(col('c')).order_by(col('rowtime')) \\
            ...     .preceding(lit(10).seconds).alias("ow")) \\
            ...     .select(col('c'), col('b').count.over(col('ow'), col('e').sum.over(col('ow'))))

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

    def add_columns(self, *fields: Expression) -> 'Table':
        """
        Adds additional columns. Similar to a SQL SELECT statement. The field expressions
        can contain complex expressions, but can not contain aggregations. It will throw an
        exception if the added fields already exist.

        Example:
        ::

            >>> from pyflink.table.expressions import col, concat
            >>> tab.add_columns((col('a') + 1).alias('a1'), concat(col('b'), 'sunny').alias('b1'))

        :param fields: Column list string.
        :return: The result table.
        """
        return Table(self._j_table.addColumns(to_expression_jarray(fields)), self._t_env)

    def add_or_replace_columns(self, *fields: Expression) -> 'Table':
        """
        Adds additional columns. Similar to a SQL SELECT statement. The field expressions
        can contain complex expressions, but can not contain aggregations. Existing fields will be
        replaced if add columns name is the same as the existing column name. Moreover, if the added
        fields have duplicate field name, then the last one is used.

        Example:
        ::

            >>> from pyflink.table.expressions import col, concat
            >>> tab.add_or_replace_columns((col('a') + 1).alias('a1'),
            ...                            concat(col('b'), 'sunny').alias('b1'))

        :param fields: Column list string.
        :return: The result table.
        """
        return Table(self._j_table.addOrReplaceColumns(to_expression_jarray(fields)),
                     self._t_env)

    def rename_columns(self, *fields: Expression) -> 'Table':
        """
        Renames existing columns. Similar to a field alias statement. The field expressions
        should be alias expressions, and only the existing fields can be renamed.

        Example:
        ::

            >>> tab.rename_columns(col('a').alias('a1'), col('b').alias('b1'))

        :param fields: Column list string.
        :return: The result table.
        """
        return Table(self._j_table.renameColumns(to_expression_jarray(fields)),
                     self._t_env)

    def drop_columns(self, *fields: Expression) -> 'Table':
        """
        Drops existing columns. The field expressions should be field reference expressions.

        Example:
        ::

            >>> tab.drop_columns(col('a'), col('b'))

        :param fields: Column list string.
        :return: The result table.
        """
        return Table(self._j_table.dropColumns(to_expression_jarray(fields)),
                     self._t_env)

    def map(self, func: Union[Expression, UserDefinedScalarFunctionWrapper]) -> 'Table':
        """
        Performs a map operation with a user-defined scalar function.

        Example:
        ::

            >>> add = udf(lambda x: Row(x + 1, x * x), result_type=DataTypes.Row(
            ... [DataTypes.FIELD("a", DataTypes.INT()), DataTypes.FIELD("b", DataTypes.INT())]))
            >>> tab.map(add(col('a'))).alias("a", "b")
            >>> # take all the columns as inputs
            >>> identity = udf(lambda row: row, result_type=DataTypes.Row(
            ... [DataTypes.FIELD("a", DataTypes.INT()), DataTypes.FIELD("b", DataTypes.INT())]))
            >>> tab.map(identity)

        :param func: user-defined scalar function.
        :return: The result table.

        .. versionadded:: 1.13.0
        """
        if isinstance(func, Expression):
            return Table(self._j_table.map(func._j_expr), self._t_env)
        else:
            func._set_takes_row_as_input()
            return Table(self._j_table.map(func(with_columns(col("*")))._j_expr), self._t_env)

    def flat_map(self, func: Union[Expression, UserDefinedTableFunctionWrapper]) -> 'Table':
        """
        Performs a flatMap operation with a user-defined table function.

        Example:
        ::

            >>> @udtf(result_types=[DataTypes.INT(), DataTypes.STRING()])
            ... def split(x, string):
            ...     for s in string.split(","):
            ...         yield x, s
            >>> tab.flat_map(split(col('a'), col('b')))
            >>> # take all the columns as inputs
            >>> @udtf(result_types=[DataTypes.INT(), DataTypes.STRING()])
            ... def split_row(row: Row):
            ...     for s in row[1].split(","):
            ...         yield row[0], s
            >>> tab.flat_map(split_row)

        :param func: user-defined table function.
        :return: The result table.

        .. versionadded:: 1.13.0
        """
        if isinstance(func, Expression):
            return Table(self._j_table.flatMap(func._j_expr), self._t_env)
        else:
            func._set_takes_row_as_input()
            return Table(self._j_table.flatMap(func(with_columns(col("*")))._j_expr), self._t_env)

    def aggregate(self, func: Union[Expression, UserDefinedAggregateFunctionWrapper]) \
            -> 'AggregatedTable':
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
            >>> tab.aggregate(agg(col('a')).alias("a", "b")).select(col('a'), col('b'))
            >>> # take all the columns as inputs
            >>> # pd is a Pandas.DataFrame
            >>> agg_row = udaf(lambda pd: (pd.a.mean(), pd.a.max()),
            ...               result_type=DataTypes.ROW(
            ...                   [DataTypes.FIELD("a", DataTypes.FLOAT()),
            ...                    DataTypes.FIELD("b", DataTypes.INT())]),
            ...               func_type="pandas")
            >>> tab.aggregate(agg.alias("a", "b")).select(col('a'), col('b'))

        :param func: user-defined aggregate function.
        :return: The result table.

        .. versionadded:: 1.13.0
        """
        if isinstance(func, Expression):
            return AggregatedTable(self._j_table.aggregate(func._j_expr), self._t_env)
        else:
            func._set_takes_row_as_input()
            if hasattr(func, "_alias_names"):
                alias_names = getattr(func, "_alias_names")
                func = func(with_columns(col("*"))).alias(*alias_names)
            else:
                func = func(with_columns(col("*")))
            return AggregatedTable(self._j_table.aggregate(func._j_expr), self._t_env)

    def flat_aggregate(self, func: Union[Expression, UserDefinedAggregateFunctionWrapper]) \
            -> 'FlatAggregateTable':
        """
        Perform a global flat_aggregate without group_by. flat_aggregate takes a
        :class:`~pyflink.table.TableAggregateFunction` which returns multiple rows. Use a selection
        after the flat_aggregate.

        Example:
        ::

            >>> table_agg = udtaf(MyTableAggregateFunction())
            >>> tab.flat_aggregate(table_agg(col('a')).alias("a", "b")).select(col('a'), col('b'))
            >>> # take all the columns as inputs
            >>> class Top2(TableAggregateFunction):
            ...     def emit_value(self, accumulator):
            ...         yield Row(accumulator[0])
            ...         yield Row(accumulator[1])
            ...
            ...     def create_accumulator(self):
            ...         return [None, None]
            ...
            ...     def accumulate(self, accumulator, *args):
            ...         args[0] # type: Row
            ...         if args[0][0] is not None:
            ...             if accumulator[0] is None or args[0][0] > accumulator[0]:
            ...                 accumulator[1] = accumulator[0]
            ...                 accumulator[0] = args[0][0]
            ...             elif accumulator[1] is None or args[0][0] > accumulator[1]:
            ...                 accumulator[1] = args[0][0]
            ...
            ...     def get_accumulator_type(self):
            ...         return DataTypes.ARRAY(DataTypes.BIGINT())
            ...
            ...     def get_result_type(self):
            ...         return DataTypes.ROW(
            ...             [DataTypes.FIELD("a", DataTypes.BIGINT())])
            >>> top2 = udtaf(Top2())
            >>> tab.flat_aggregate(top2.alias("a", "b")).select(col('a'), col('b'))

        :param func: user-defined table aggregate function.
        :return: The result table.

        .. versionadded:: 1.13.0
        """
        if isinstance(func, Expression):
            return FlatAggregateTable(self._j_table.flatAggregate(func._j_expr), self._t_env)
        else:
            func._set_takes_row_as_input()
            if hasattr(func, "_alias_names"):
                alias_names = getattr(func, "_alias_names")
                func = func(with_columns(col("*"))).alias(*alias_names)
            else:
                func = func(with_columns(col("*")))
            return FlatAggregateTable(self._j_table.flatAggregate(func._j_expr), self._t_env)

    def to_pandas(self):
        """
        Converts the table to a pandas DataFrame. It will collect the content of the table to
        the client side and so please make sure that the content of the table could fit in memory
        before calling this method.

        Example:
        ::

            >>> pdf = pd.DataFrame(np.random.rand(1000, 2))
            >>> table = table_env.from_pandas(pdf, ["a", "b"])
            >>> table.filter(col('a') > 0.5).to_pandas()

        :return: the result pandas DataFrame.

        .. versionadded:: 1.11.0
        """
        self._t_env._before_execute()
        gateway = get_gateway()
        max_arrow_batch_size = self._j_table.getTableEnvironment().getConfig()\
            .get(gateway.jvm.org.apache.flink.python.PythonOptions.MAX_ARROW_BATCH_SIZE)
        batches_iterator = gateway.jvm.org.apache.flink.table.runtime.arrow.ArrowUtils\
            .collectAsPandasDataFrame(self._j_table, max_arrow_batch_size)
        if batches_iterator.hasNext():
            import pytz
            timezone = pytz.timezone(
                self._j_table.getTableEnvironment().getConfig().getLocalTimeZone().getId())
            serializer = ArrowSerializer(
                create_arrow_schema(self.get_schema().get_field_names(),
                                    self.get_schema().get_field_data_types()),
                self.get_schema().to_row_data_type(),
                timezone)
            import pyarrow as pa
            table = pa.Table.from_batches(serializer.load_from_iterator(batches_iterator))
            pdf = table.to_pandas()

            schema = self.get_schema()
            for field_name in schema.get_field_names():
                pdf[field_name] = tz_convert_from_internal(
                    pdf[field_name], schema.get_field_data_type(field_name), timezone)
            return pdf
        else:
            import pandas as pd
            return pd.DataFrame.from_records([], columns=self.get_schema().get_field_names())

    def get_schema(self) -> TableSchema:
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

    def execute_insert(self,
                       table_path_or_descriptor: Union[str, TableDescriptor],
                       overwrite: bool = False) -> TableResult:
        """
        1. When target_path_or_descriptor is a tale path:

            Writes the :class:`~pyflink.table.Table` to a :class:`~pyflink.table.TableSink` that was
            registered under the specified name, and then execute the insert operation. For the path
            resolution algorithm see :func:`~TableEnvironment.use_database`.

            Example:
            ::

                >>> tab.execute_insert("sink")

        2. When target_path_or_descriptor is a table descriptor:

            Declares that the pipeline defined by the given Table object should be written to a
            table (backed by a DynamicTableSink) expressed via the given TableDescriptor. It
            executes the insert operation.

            TableDescriptor is registered as an inline (i.e. anonymous) temporary catalog table
            (see :func:`~TableEnvironment.create_temporary_table`) using a unique identifier.
            Note that calling this method multiple times, even with the same descriptor, results
            in multiple sink tables being registered.

            This method allows to declare a :class:`~pyflink.table.Schema` for the sink descriptor.
            The declaration is similar to a {@code CREATE TABLE} DDL in SQL and allows to:

                1. overwrite automatically derived columns with a custom DataType
                2. add metadata columns next to the physical columns
                3. declare a primary key

            It is possible to declare a schema without physical/regular columns. In this case, those
            columns will be automatically derived and implicitly put at the beginning of the schema
            declaration.

            Examples:
            ::

                >>> schema = Schema.new_builder()
                ...      .column("f0", DataTypes.STRING())
                ...      .build()
                >>> table = table_env.from_descriptor(TableDescriptor.for_connector("datagen")
                ...      .schema(schema)
                ...      .build())
                >>> table.execute_insert(TableDescriptor.for_connector("blackhole")
                ...      .schema(schema)
                ...      .build())

            If multiple pipelines should insert data into one or more sink tables as part of a
            single execution, use a :class:`~pyflink.table.StatementSet`
            (see :func:`~TableEnvironment.create_statement_set`).

            By default, all insertion operations are executed asynchronously. Use
            :func:`~TableResult.await` or :func:`~TableResult.get_job_client` to monitor the
            execution.

            .. note:: execute_insert for a table descriptor (case 2.) was added from
                flink 1.14.0.

        :param table_path_or_descriptor: The path of the registered
            :class:`~pyflink.table.TableSink` or the descriptor describing the sink table into which
            data should be inserted to which the :class:`~pyflink.table.Table` is written.
        :param overwrite: Indicates whether the insert should overwrite existing data or not.
        :return: The table result.

        .. versionadded:: 1.11.0
        """
        self._t_env._before_execute()
        if isinstance(table_path_or_descriptor, str):
            return TableResult(self._j_table.executeInsert(table_path_or_descriptor, overwrite))
        else:
            return TableResult(self._j_table.executeInsert(
                table_path_or_descriptor._j_table_descriptor, overwrite))

    def execute(self) -> TableResult:
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
        TEXT = get_gateway().jvm.org.apache.flink.table.api.ExplainFormat.TEXT
        j_extra_details = to_j_explain_detail_arr(extra_details)
        return self._j_table.explain(TEXT, j_extra_details)


class GroupedTable(object):
    """
    A table that has been grouped on a set of grouping keys.
    """

    def __init__(self, java_table, t_env):
        self._j_table = java_table
        self._t_env = t_env

    def select(self, *fields: Expression) -> 'Table':
        """
        Performs a selection operation on a grouped table. Similar to an SQL SELECT statement.
        The field expressions can contain complex expressions and aggregations.

        Example:
        ::

            >>> tab.group_by(col('key')).select(col('key'), col('value').avg.alias('average'))

        :param fields: Expression string that contains group keys and aggregate function calls.
        :return: The result table.
        """
        return Table(self._j_table.select(to_expression_jarray(fields)), self._t_env)

    def aggregate(self, func: Union[Expression, UserDefinedAggregateFunctionWrapper]) \
            -> 'AggregatedTable':
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
            >>> tab.group_by(col('a')).aggregate(agg(col('b')).alias("c", "d")).select(
            ...     col('a'), col('c'), col('d'))
            >>> # take all the columns as inputs
            >>> # pd is a Pandas.DataFrame
            >>> agg_row = udaf(lambda pd: (pd.a.mean(), pd.b.max()),
            ...               result_type=DataTypes.ROW(
            ...                   [DataTypes.FIELD("a", DataTypes.FLOAT()),
            ...                    DataTypes.FIELD("b", DataTypes.INT())]),
            ...               func_type="pandas")
            >>> tab.group_by(col('a')).aggregate(agg.alias("a", "b")).select(col('a'), col('b'))

        :param func: user-defined aggregate function.
        :return: The result table.

        .. versionadded:: 1.13.0
        """
        if isinstance(func, Expression):
            return AggregatedTable(self._j_table.aggregate(func._j_expr), self._t_env)
        else:
            func._set_takes_row_as_input()
            if hasattr(func, "_alias_names"):
                alias_names = getattr(func, "_alias_names")
                func = func(with_columns(col("*"))).alias(*alias_names)
            else:
                func = func(with_columns(col("*")))
            return AggregatedTable(self._j_table.aggregate(func._j_expr), self._t_env)

    def flat_aggregate(self, func: Union[Expression, UserDefinedAggregateFunctionWrapper]) \
            -> 'FlatAggregateTable':
        """
        Performs a flat_aggregate operation on a grouped table. flat_aggregate takes a
        :class:`~pyflink.table.TableAggregateFunction` which returns multiple rows. Use a selection
        after flatAggregate.

        Example:
        ::

            >>> table_agg = udtaf(MyTableAggregateFunction())
            >>> tab.group_by(col('c')).flat_aggregate(table_agg(col('a')).alias("a")).select(
            ...     col('c'), col('a'))
            >>> # take all the columns as inputs
            >>> class Top2(TableAggregateFunction):
            ...     def emit_value(self, accumulator):
            ...         yield Row(accumulator[0])
            ...         yield Row(accumulator[1])
            ...
            ...     def create_accumulator(self):
            ...         return [None, None]
            ...
            ...     def accumulate(self, accumulator, *args):
            ...         args[0] # type: Row
            ...         if args[0][0] is not None:
            ...             if accumulator[0] is None or args[0][0] > accumulator[0]:
            ...                 accumulator[1] = accumulator[0]
            ...                 accumulator[0] = args[0][0]
            ...             elif accumulator[1] is None or args[0][0] > accumulator[1]:
            ...                 accumulator[1] = args[0][0]
            ...
            ...     def get_accumulator_type(self):
            ...         return DataTypes.ARRAY(DataTypes.BIGINT())
            ...
            ...     def get_result_type(self):
            ...         return DataTypes.ROW(
            ...             [DataTypes.FIELD("a", DataTypes.BIGINT())])
            >>> top2 = udtaf(Top2())
            >>> tab.group_by(col('c')) \\
            ...    .flat_aggregate(top2.alias("a", "b")) \\
            ...    .select(col('a'), col('b'))

        :param func: user-defined table aggregate function.
        :return: The result table.

        .. versionadded:: 1.13.0
        """
        if isinstance(func, Expression):
            return FlatAggregateTable(self._j_table.flatAggregate(func._j_expr), self._t_env)
        else:
            func._set_takes_row_as_input()
            if hasattr(func, "_alias_names"):
                alias_names = getattr(func, "_alias_names")
                func = func(with_columns(col("*"))).alias(*alias_names)
            else:
                func = func(with_columns(col("*")))
            return FlatAggregateTable(self._j_table.flatAggregate(func._j_expr), self._t_env)


class GroupWindowedTable(object):
    """
    A table that has been windowed for :class:`~pyflink.table.GroupWindow`.
    """

    def __init__(self, java_group_windowed_table, t_env):
        self._j_table = java_group_windowed_table
        self._t_env = t_env

    def group_by(self, *fields: Expression) -> 'WindowGroupedTable':
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

            >>> from pyflink.table.expressions import col, lit
            >>> tab.window(Tumble.over(lit(10).minutes).on(col('rowtime')).alias('w')) \\
            ...     .group_by(col('w')) \\
            ...     .select(col('a').sum.alias('a'),
            ...             col('w').start.alias('b'),
            ...             col('w').end.alias('c'),
            ...             col('w').rowtime.alias('d'))

        :param fields: Group keys.
        :return: A window grouped table.
        """
        return WindowGroupedTable(
            self._j_table.groupBy(to_expression_jarray(fields)), self._t_env)


class WindowGroupedTable(object):
    """
    A table that has been windowed and grouped for :class:`~pyflink.table.window.GroupWindow`.
    """

    def __init__(self, java_window_grouped_table, t_env):
        self._j_table = java_window_grouped_table
        self._t_env = t_env

    def select(self, *fields: Expression) -> 'Table':
        """
        Performs a selection operation on a window grouped table. Similar to an SQL SELECT
        statement.
        The field expressions can contain complex expressions and aggregations.

        Example:
        ::

            >>> window_grouped_table.select(col('key'),
            ...                             col('window').start,
            ...                             col('value').avg.alias('valavg'))

        :param fields: Expression string.
        :return: The result table.
        """
        if all(isinstance(f, Expression) for f in fields):
            return Table(self._j_table.select(to_expression_jarray(fields)), self._t_env)
        else:
            assert len(fields) == 1
            assert isinstance(fields[0], str)
            return Table(self._j_table.select(fields[0]), self._t_env)

    def aggregate(self, func: Union[Expression, UserDefinedAggregateFunctionWrapper]) \
            -> 'AggregatedTable':
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
            >>> window_grouped_table.group_by(col("w")) \
            ...     .aggregate(agg(col('b'))) \
            ...     .alias("c", "d") \
            ...     .select(col('c'), col('d'))
            >>> # take all the columns as inputs
            >>> # pd is a Pandas.DataFrame
            >>> agg_row = udaf(lambda pd: (pd.a.mean(), pd.b.max()),
            ...               result_type=DataTypes.ROW(
            ...                   [DataTypes.FIELD("a", DataTypes.FLOAT()),
            ...                    DataTypes.FIELD("b", DataTypes.INT())]),
            ...               func_type="pandas")
            >>> window_grouped_table.group_by(col("w"), col("a")).aggregate(agg_row)

        :param func: user-defined aggregate function.
        :return: The result table.

        .. versionadded:: 1.13.0
        """
        if isinstance(func, Expression):
            return AggregatedTable(self._j_table.aggregate(func._j_expr), self._t_env)
        else:
            func._set_takes_row_as_input()
            func = self._to_expr(func)
            return AggregatedTable(self._j_table.aggregate(func._j_expr), self._t_env)

    def _to_expr(self, func: UserDefinedAggregateFunctionWrapper) -> Expression:
        group_window_field = self._j_table.getClass().getDeclaredField("window")
        group_window_field.setAccessible(True)
        j_group_window = group_window_field.get(self._j_table)
        j_time_field = j_group_window.getTimeField()
        fields_without_window = without_columns(j_time_field)
        if hasattr(func, "_alias_names"):
            alias_names = getattr(func, "_alias_names")
            func_expression = func(fields_without_window).alias(*alias_names)
        else:
            func_expression = func(fields_without_window)
        return func_expression


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

    def select(self, *fields: Expression) -> 'Table':
        """
        Performs a selection operation on a over windowed table. Similar to an SQL SELECT
        statement.
        The field expressions can contain complex expressions and aggregations.

        Example:
        ::

            >>> over_windowed_table.select(col('c'),
            ...                            col('b').count.over(col('ow')),
            ...                            col('e').sum.over(col('ow')))

        :param fields: Expression string.
        :return: The result table.
        """
        return Table(self._j_table.select(to_expression_jarray(fields)), self._t_env)


class AggregatedTable(object):
    """
    A table that has been performed on the aggregate function.
    """

    def __init__(self, java_table, t_env):
        self._j_table = java_table
        self._t_env = t_env

    def select(self, *fields: Expression) -> 'Table':
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
            >>> tab.aggregate(agg(col('a')).alias("a", "b")).select(col('a'), col('b'))
            >>> # take all the columns as inputs
            >>> # pd is a Pandas.DataFrame
            >>> agg_row = udaf(lambda pd: (pd.a.mean(), pd.b.max()),
            ...               result_type=DataTypes.ROW(
            ...                   [DataTypes.FIELD("a", DataTypes.FLOAT()),
            ...                    DataTypes.FIELD("b", DataTypes.INT())]),
            ...               func_type="pandas")
            >>> tab.group_by(col('a')).aggregate(agg.alias("a", "b")).select(col('a'), col('b'))

        :param fields: Expression string.
        :return: The result table.
        """
        return Table(self._j_table.select(to_expression_jarray(fields)), self._t_env)


class FlatAggregateTable(object):
    """
    A table that performs flatAggregate on a :class:`~pyflink.table.Table`, a
    :class:`~pyflink.table.GroupedTable` or a :class:`~pyflink.table.WindowGroupedTable`
    """

    def __init__(self, java_table, t_env):
        self._j_table = java_table
        self._t_env = t_env

    def select(self, *fields: Expression) -> 'Table':
        """
        Performs a selection operation on a FlatAggregateTable. Similar to a SQL SELECT statement.
        The field expressions can contain complex expressions.

        Example:
        ::

            >>> table_agg = udtaf(MyTableAggregateFunction())
            >>> tab.flat_aggregate(table_agg(col('a')).alias("a", "b")).select(col('a'), col('b'))
            >>> # take all the columns as inputs
            >>> class Top2(TableAggregateFunction):
            ...     def emit_value(self, accumulator):
            ...         yield Row(accumulator[0])
            ...         yield Row(accumulator[1])
            ...
            ...     def create_accumulator(self):
            ...         return [None, None]
            ...
            ...     def accumulate(self, accumulator, *args):
            ...         args[0] # type: Row
            ...         if args[0][0] is not None:
            ...             if accumulator[0] is None or args[0][0] > accumulator[0]:
            ...                 accumulator[1] = accumulator[0]
            ...                 accumulator[0] = args[0][0]
            ...             elif accumulator[1] is None or args[0][0] > accumulator[1]:
            ...                 accumulator[1] = args[0][0]
            ...
            ...     def get_accumulator_type(self):
            ...         return DataTypes.ARRAY(DataTypes.BIGINT())
            ...
            ...     def get_result_type(self):
            ...         return DataTypes.ROW(
            ...             [DataTypes.FIELD("a", DataTypes.BIGINT())])
            >>> top2 = udtaf(Top2())
            >>> tab.group_by(col('c')) \\
            ...    .flat_aggregate(top2.alias("a", "b")) \\
            ...    .select(col('a'), col('b'))

        :param fields: Expression string.
        :return: The result table.
        """
        return Table(self._j_table.select(to_expression_jarray(fields)), self._t_env)
