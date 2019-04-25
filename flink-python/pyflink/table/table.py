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

__all__ = ['Table']


class Table(object):

    """
    A :class:`Table` is the core component of the Table API.
    Similar to how the batch and streaming APIs have DataSet and DataStream,
    the Table API is built around :class:`Table`.

    Use the methods of :class:`Table` to transform data.

    Example:
    ::
        >>> t_config = TableConfig.Builder().as_streaming_execution().set_parallelism(1).build()
        >>> t_env = TableEnvironment.get_table_environment(t_config)
        >>> ...
        >>> t_env.register_table_source("source", ...)
        >>> t = t_env.scan("source")
        >>> t.select(...)
        ...
        >>> t_env.register_table_sink("result", ...)
        >>> t.insert_into("result")
        >>> t_env.execute()

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
        :return: Result table.
        """
        return Table(self._j_table.select(fields))

    def alias(self, fields):
        """
        Renames the fields of the expression result. Use this to disambiguate fields before
        joining to operations.
        Example:
        ::
            >>> tab.alias("a, b")

        :param fields: Field list expression string.
        :return: Result table.
        """
        return Table(get_method(self._j_table, "as")(fields))

    def filter(self, predicate):
        """
        Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
        clause.
        Example:
        ::
            >>> tab.filter("name = 'Fred'")

        :param predicate: Predicate expression string.
        :return: Result table.
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
        :return: Result table.
        """
        return Table(self._j_table.where(predicate))

    def insert_into(self, table_name):
        """
        Writes the :class:`Table` to a :class:`TableSink` that was registered under the specified name.

        Example:
        ::
            >>> tab.insert_into("print")

        :param table_name: Name of the :class:`TableSink` to which the :class:`Table` is written.
        """
        self._j_table.insertInto(table_name)
