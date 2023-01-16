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
from typing import Optional

from py4j.java_gateway import get_method
from pyflink.common.types import RowKind

from pyflink.common import Row
from pyflink.common.job_client import JobClient
from pyflink.java_gateway import get_gateway
from pyflink.table.result_kind import ResultKind
from pyflink.table.table_schema import TableSchema
from pyflink.table.types import _from_java_data_type
from pyflink.table.utils import pickled_bytes_to_python_converter

__all__ = ['TableResult', 'CloseableIterator']


class TableResult(object):
    """
    A :class:`~pyflink.table.TableResult` is the representation of the statement execution result.

    .. versionadded:: 1.11.0
    """

    def __init__(self, j_table_result):
        self._j_table_result = j_table_result

    def get_job_client(self) -> Optional[JobClient]:
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

    def wait(self, timeout_ms: int = None):
        """
        Wait if necessary for at most the given time (milliseconds) for the data to be ready.

        For a select operation, this method will wait until the first row can be accessed locally.
        For an insert operation, this method will wait for the job to finish,
        because the result contains only one row.
        For other operations, this method will return immediately,
        because the result is already available locally.

        .. versionadded:: 1.12.0
        """
        if timeout_ms:
            TimeUnit = get_gateway().jvm.java.util.concurrent.TimeUnit
            get_method(self._j_table_result, "await")(timeout_ms, TimeUnit.MILLISECONDS)
        else:
            get_method(self._j_table_result, "await")()

    def get_table_schema(self) -> TableSchema:
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
        return TableSchema(j_table_schema=self._get_java_table_schema())

    def get_result_kind(self) -> ResultKind:
        """
        Return the ResultKind which represents the result type.

        For DDL operation and USE operation, the result kind is always SUCCESS.
        For other operations, the result kind is always SUCCESS_WITH_CONTENT.

        :return: The result kind.

        .. versionadded:: 1.11.0
        """
        return ResultKind._from_j_result_kind(self._j_table_result.getResultKind())

    def collect(self) -> 'CloseableIterator':
        """
        Get the result contents as a closeable row iterator.

        Note:

        For SELECT operation, the job will not be finished unless all result data has been
        collected. So we should actively close the job to avoid resource leak through
        CloseableIterator#close method. Calling CloseableIterator#close method will cancel the job
        and release related resources.

        For DML operation, Flink does not support getting the real affected row count now. So the
        affected row count is always -1 (unknown) for every sink, and them will be returned until
        the job is finished.
        Calling CloseableIterator#close method will cancel the job.

        For other operations, no flink job will be submitted (get_job_client() is always empty), and
        the result is bounded. Do noting when calling CloseableIterator#close method.

        Recommended code to call CloseableIterator#close method looks like:

        >>> table_result = t_env.execute("select ...")
        >>> with table_result.collect() as results:
        >>>    for result in results:
        >>>        ...

        In order to fetch result to local, you can call either collect() and print(). But, they can
        not be called both on the same TableResult instance.

        :return: A CloseableIterator.

        .. versionadded:: 1.12.0
        """
        field_data_types = self._get_java_table_schema().getFieldDataTypes()

        j_iter = self._j_table_result.collect()

        return CloseableIterator(j_iter, field_data_types)

    def print(self):
        """
        Print the result contents as tableau form to client console.

        This method has slightly different behaviors under different checkpointing settings.

            - For batch jobs or streaming jobs without checkpointing,
              this method has neither exactly-once nor at-least-once guarantee.
              Query results are immediately accessible by the clients once they're produced,
              but exceptions will be thrown when the job fails and restarts.
            - For streaming jobs with exactly-once checkpointing,
              this method guarantees an end-to-end exactly-once record delivery.
              A result will be accessible by clients only after its corresponding checkpoint
              completes.
            - For streaming jobs with at-least-once checkpointing,
              this method guarantees an end-to-end at-least-once record delivery.
              Query results are immediately accessible by the clients once they're produced,
              but it is possible for the same result to be delivered multiple times.

        .. versionadded:: 1.11.0
        """
        self._j_table_result.print()

    def _get_java_table_schema(self):
        TableSchema = get_gateway().jvm.org.apache.flink.table.api.TableSchema
        return TableSchema.fromResolvedSchema(self._j_table_result.getResolvedSchema())


class CloseableIterator(object):
    """
    Representing an Iterator that is also auto closeable.
    """
    def __init__(self, j_closeable_iterator, field_data_types):
        self._j_closeable_iterator = j_closeable_iterator
        self._j_field_data_types = field_data_types
        self._data_types = [_from_java_data_type(j_field_data_type)
                            for j_field_data_type in self._j_field_data_types]

    def __iter__(self):
        return self

    def __next__(self):
        if not self._j_closeable_iterator.hasNext():
            raise StopIteration("No more data.")
        gateway = get_gateway()
        pickle_bytes = gateway.jvm.PythonBridgeUtils. \
            getPickledBytesFromRow(self._j_closeable_iterator.next(),
                                   self._j_field_data_types)
        row_kind = RowKind(int.from_bytes(pickle_bytes[0], byteorder='big', signed=False))
        pickle_bytes = list(pickle_bytes[1:])
        field_data = zip(pickle_bytes, self._data_types)
        fields = []
        for data, field_type in field_data:
            if len(data) == 0:
                fields.append(None)
            else:
                fields.append(pickled_bytes_to_python_converter(data, field_type))
        result_row = Row(*fields)
        result_row.set_row_kind(row_kind)
        return result_row

    def next(self):
        return self.__next__()

    def close(self):
        self._j_closeable_iterator.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
