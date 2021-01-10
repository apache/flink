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
from pyflink.table import ExplainDetail
from pyflink.table.table_result import TableResult
from pyflink.util.utils import to_j_explain_detail_arr

__all__ = ['StatementSet']


class StatementSet(object):
    """
    A StatementSet accepts DML statements or Tables,
    the planner can optimize all added statements and Tables together
    and then submit as one job.

    .. note::

        The added statements and Tables will be cleared
        when calling the `execute` method.

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

    def add_insert(self, target_path: str, table, overwrite: bool = False) -> 'StatementSet':
        """
        add Table with the given sink table name to the set.

        :param target_path: The path of the registered :class:`~pyflink.table.TableSink` to which
                            the :class:`~pyflink.table.Table` is written.
        :param table: The Table to add.
        :type table: pyflink.table.Table
        :param overwrite: The flag that indicates whether the insert
                          should overwrite existing data or not.
        :return: current StatementSet instance.

        .. versionadded:: 1.11.0
        """
        self._j_statement_set.addInsert(target_path, table._j_table, overwrite)
        return self

    def explain(self, *extra_details: ExplainDetail) -> str:
        """
        returns the AST and the execution plan of all statements and Tables.

        :param extra_details: The extra explain details which the explain result should include,
                              e.g. estimated cost, changelog mode for streaming
        :return: All statements and Tables for which the AST and execution plan will be returned.

        .. versionadded:: 1.11.0
        """
        j_extra_details = to_j_explain_detail_arr(extra_details)
        return self._j_statement_set.explain(j_extra_details)

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
