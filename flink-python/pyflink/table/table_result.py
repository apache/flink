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
from pyflink.common.job_client import JobClient
from pyflink.table.result_kind import ResultKind
from pyflink.table.table_schema import TableSchema

__all__ = ['TableResult']


class TableResult(object):
    """
    A :class:`~pyflink.table.TableResult` is the representation of the statement execution result.

    .. versionadded:: 1.11.0
    """

    def __init__(self, j_table_result):
        self._j_table_result = j_table_result

    def get_job_client(self):
        """
        For DML and DQL statement, return the JobClient which associates the submitted Flink job.
        For other statements (e.g.  DDL, DCL) return empty.

        :return: The job client, optional.
        :rtype: pyflink.common.JobClient

        .. versionadded:: 1.11.0
        """
        job_client = self._j_table_result.getJobClient()
        if job_client.isPresent():
            return JobClient(job_client.get())
        else:
            return None

    def get_table_schema(self):
        """
        Get the schema of result.

        The schema of DDL, USE, EXPLAIN:
        ::

            +-------------+-------------+----------+
            | column name | column type | comments |
            +-------------+-------------+----------+
            | result      | STRING      |          |
            +-------------+-------------+----------+

        The schema of SHOW:
        ::

            +---------------+-------------+----------+
            |  column name  | column type | comments |
            +---------------+-------------+----------+
            | <object name> | STRING      |          |
            +---------------+-------------+----------+
            The column name of `SHOW CATALOGS` is "catalog name",
            the column name of `SHOW DATABASES` is "database name",
            the column name of `SHOW TABLES` is "table name",
            the column name of `SHOW VIEWS` is "view name",
            the column name of `SHOW FUNCTIONS` is "function name".

        The schema of DESCRIBE:
        ::

            +------------------+-------------+-------------------------------------------------+
            | column name      | column type |                 comments                        |
            +------------------+-------------+-------------------------------------------------+
            | name             | STRING      | field name                                      |
            +------------------+-------------+-------------------------------------------------+
            | type             | STRING      | field type expressed as a String                |
            +------------------+-------------+-------------------------------------------------+
            | null             | BOOLEAN     | field nullability: true if a field is nullable, |
            |                  |             | else false                                      |
            +------------------+-------------+-------------------------------------------------+
            | key              | BOOLEAN     | key constraint: 'PRI' for primary keys,         |
            |                  |             | 'UNQ' for unique keys, else null                |
            +------------------+-------------+-------------------------------------------------+
            | computed column  | STRING      | computed column: string expression              |
            |                  |             | if a field is computed column, else null        |
            +------------------+-------------+-------------------------------------------------+
            | watermark        | STRING      | watermark: string expression if a field is      |
            |                  |             | watermark, else null                            |
            +------------------+-------------+-------------------------------------------------+

        The schema of INSERT: (one column per one sink)
        ::

            +----------------------------+-------------+-----------------------+
            | column name                | column type | comments              |
            +----------------------------+-------------+-----------------------+
            | (name of the insert table) | BIGINT      | the insert table name |
            +----------------------------+-------------+-----------------------+

        The schema of SELECT is the selected field names and types.

        :return: The schema of result.
        :rtype: pyflink.table.TableSchema

        .. versionadded:: 1.11.0
        """
        return TableSchema(j_table_schema=self._j_table_result.getTableSchema())

    def get_result_kind(self):
        """
        Return the ResultKind which represents the result type.

        For DDL operation and USE operation, the result kind is always SUCCESS.
        For other operations, the result kind is always SUCCESS_WITH_CONTENT.

        :return: The result kind.
        :rtype: pyflink.table.ResultKind

        .. versionadded:: 1.11.0
        """
        return ResultKind._from_j_result_kind(self._j_table_result.getResultKind())

    def print(self):
        """
        Print the result contents as tableau form to client console.

        NOTE: please make sure the result data to print should be small.
        Because all data will be collected to local first, and then print them to console.

        .. versionadded:: 1.11.0
        """
        self._j_table_result.print()
