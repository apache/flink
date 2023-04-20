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

from pyflink.java_gateway import get_gateway
from pyflink.table import ExplainDetail
from pyflink.table.table_descriptor import TableDescriptor
from pyflink.table.table_result import TableResult
from pyflink.util.java_utils import to_j_explain_detail_arr

__all__ = ['StatementSet']


class StatementSet(object):
    """
    A :class:`~StatementSet` accepts pipelines defined by DML statements or :class:`~Table` objects.
    The planner can optimize all added statements together and then submit them as one job.

    The added statements will be cleared when calling the :func:`~StatementSet.execute` method.

    .. versionadded:: 1.11.0
    """

    def __init__(self, _j_statement_set, t_env):
        self._j_statement_set = _j_statement_set
        self._t_env = t_env

    def add_insert_sql(self, stmt: str) -> 'StatementSet':
        """
        add insert statement to the set.

        :param stmt: The statement to be added.
        :return: current StatementSet instance.

        .. versionadded:: 1.11.0
        """
        self._j_statement_set.addInsertSql(stmt)
        return self

    def attach_as_datastream(self):
        """
        Optimizes all statements as one entity and adds them as transformations to the underlying
        StreamExecutionEnvironment.

        Use :func:`~pyflink.datastream.StreamExecutionEnvironment.execute` to execute them.

        The added statements will be cleared after calling this method.

        .. versionadded:: 1.16.0
        """
        self._j_statement_set.attachAsDataStream()

    def add_insert(self,
                   target_path_or_descriptor: Union[str, TableDescriptor],
                   table,
                   overwrite: bool = False) -> 'StatementSet':
        """
        Adds a statement that the pipeline defined by the given Table object should be written to a
        table (backed by a DynamicTableSink) that was registered under the specified path or
        expressed via the given TableDescriptor.

        1. When target_path_or_descriptor is a tale path:

            See the documentation of :func:`~TableEnvironment.use_database` or
            :func:`~TableEnvironment.use_catalog` for the rules on the path resolution.

        2. When target_path_or_descriptor is a table descriptor:

            The given TableDescriptor is registered as an inline (i.e. anonymous) temporary catalog
            table (see :func:`~TableEnvironment.create_temporary_table`).

            Then a statement is added to the statement set that inserts the Table object's pipeline
            into that temporary table.

            This method allows to declare a Schema for the sink descriptor. The declaration is
            similar to a {@code CREATE TABLE} DDL in SQL and allows to:

                1. overwrite automatically derived columns with a custom DataType
                2. add metadata columns next to the physical columns
                3. declare a primary key

            It is possible to declare a schema without physical/regular columns. In this case, those
            columns will be automatically derived and implicitly put at the beginning of the schema
            declaration.

            Examples:
            ::

                >>> stmt_set = table_env.create_statement_set()
                >>> source_table = table_env.from_path("SourceTable")
                >>> sink_descriptor = TableDescriptor.for_connector("blackhole") \\
                ...     .schema(Schema.new_builder()
                ...         .build()) \\
                ...     .build()
                >>> stmt_set.add_insert(sink_descriptor, source_table)

            .. note:: add_insert for a table descriptor (case 2.) was added from
                flink 1.14.0.

        :param target_path_or_descriptor: The path of the registered
            :class:`~pyflink.table.TableSink` or the descriptor describing the sink table into which
            data should be inserted to which the :class:`~pyflink.table.Table` is written.
        :param table: The Table to add.
        :type table: pyflink.table.Table
        :param overwrite: Indicates whether the insert should overwrite existing data or not.
        :return: current StatementSet instance.

        .. versionadded:: 1.11.0
        """
        if isinstance(target_path_or_descriptor, str):
            self._j_statement_set.addInsert(target_path_or_descriptor, table._j_table, overwrite)
        else:
            self._j_statement_set.addInsert(
                target_path_or_descriptor._j_table_descriptor, table._j_table, overwrite)
        return self

    def explain(self, *extra_details: ExplainDetail) -> str:
        """
        returns the AST and the execution plan of all statements and Tables.

        :param extra_details: The extra explain details which the explain result should include,
                              e.g. estimated cost, changelog mode for streaming
        :return: All statements and Tables for which the AST and execution plan will be returned.

        .. versionadded:: 1.11.0
        """
        TEXT = get_gateway().jvm.org.apache.flink.table.api.ExplainFormat.TEXT
        j_extra_details = to_j_explain_detail_arr(extra_details)
        return self._j_statement_set.explain(TEXT, j_extra_details)

    def execute(self) -> TableResult:
        """
        execute all statements and Tables as a batch.

        .. note::
            The added statements and Tables will be cleared when executing this method.

        :return: execution result.

        .. versionadded:: 1.11.0
        """
        self._t_env._before_execute()
        return TableResult(self._j_statement_set.execute())
