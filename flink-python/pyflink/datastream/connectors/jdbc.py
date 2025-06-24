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
from pyflink.common.typeinfo import RowTypeInfo
from pyflink.datastream.functions import SinkFunction
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import to_jarray


__all__ = [
    'JdbcSink',
    'JdbcConnectionOptions',
    'JdbcExecutionOptions'
]


class JdbcSink(SinkFunction):

    def __init__(self, j_jdbc_sink):
        super(JdbcSink, self).__init__(sink_func=j_jdbc_sink)

    @staticmethod
    def sink(sql: str, type_info: RowTypeInfo, jdbc_connection_options: 'JdbcConnectionOptions',
             jdbc_execution_options: 'JdbcExecutionOptions' = None):
        """
        Create a JDBC sink.

        :param sql: arbitrary DML query (e.g. insert, update, upsert)
        :param type_info: A RowTypeInfo for query field types.
        :param jdbc_execution_options:  parameters of execution, such as batch size and maximum
                                        retries.
        :param jdbc_connection_options: parameters of connection, such as JDBC URL.
        :return: A JdbcSink.
        """
        sql_types = []
        gateway = get_gateway()
        JJdbcTypeUtil = gateway.jvm.org.apache.flink.connector.jdbc.utils.JdbcTypeUtil
        for field_type in type_info.get_field_types():
            sql_types.append(JJdbcTypeUtil
                             .typeInformationToSqlType(field_type.get_java_type_info()))
        j_sql_type = to_jarray(gateway.jvm.int, sql_types)
        output_format_clz = gateway.jvm.Class\
            .forName('org.apache.flink.connector.jdbc.internal.JdbcOutputFormat', False,
                     get_gateway().jvm.Thread.currentThread().getContextClassLoader())
        j_int_array_type = to_jarray(gateway.jvm.int, []).getClass()
        j_builder_method = output_format_clz.getDeclaredMethod('createRowJdbcStatementBuilder',
                                                               to_jarray(gateway.jvm.Class,
                                                                         [j_int_array_type]))
        j_builder_method.setAccessible(True)
        j_statement_builder = j_builder_method.invoke(None, to_jarray(gateway.jvm.Object,
                                                                      [j_sql_type]))

        jdbc_execution_options = jdbc_execution_options if jdbc_execution_options is not None \
            else JdbcExecutionOptions.defaults()
        j_jdbc_sink = gateway.jvm.org.apache.flink.connector.jdbc.JdbcSink\
            .sink(sql, j_statement_builder, jdbc_execution_options._j_jdbc_execution_options,
                  jdbc_connection_options._j_jdbc_connection_options)
        return JdbcSink(j_jdbc_sink=j_jdbc_sink)


class JdbcConnectionOptions(object):
    """
    JDBC connection options.
    """
    def __init__(self, j_jdbc_connection_options):
        self._j_jdbc_connection_options = j_jdbc_connection_options

    def get_db_url(self) -> str:
        return self._j_jdbc_connection_options.getDbURL()

    def get_driver_name(self) -> str:
        return self._j_jdbc_connection_options.getDriverName()

    def get_user_name(self) -> str:
        return self._j_jdbc_connection_options.getUsername()

    def get_password(self) -> str:
        return self._j_jdbc_connection_options.getPassword()

    class JdbcConnectionOptionsBuilder(object):
        """
        Builder for JdbcConnectionOptions.
        """
        def __init__(self):
            self._j_options_builder = get_gateway().jvm.org.apache.flink.connector\
                .jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder()

        def with_url(self, url: str) -> 'JdbcConnectionOptions.JdbcConnectionOptionsBuilder':
            self._j_options_builder.withUrl(url)
            return self

        def with_driver_name(self, driver_name: str) \
                -> 'JdbcConnectionOptions.JdbcConnectionOptionsBuilder':
            self._j_options_builder.withDriverName(driver_name)
            return self

        def with_user_name(self, user_name: str) \
                -> 'JdbcConnectionOptions.JdbcConnectionOptionsBuilder':
            self._j_options_builder.withUsername(user_name)
            return self

        def with_password(self, password: str) \
                -> 'JdbcConnectionOptions.JdbcConnectionOptionsBuilder':
            self._j_options_builder.withPassword(password)
            return self

        def build(self) -> 'JdbcConnectionOptions':
            return JdbcConnectionOptions(j_jdbc_connection_options=self._j_options_builder.build())


class JdbcExecutionOptions(object):
    """
    JDBC sink batch options.
    """
    def __init__(self, j_jdbc_execution_options):
        self._j_jdbc_execution_options = j_jdbc_execution_options

    def get_batch_interval_ms(self) -> int:
        return self._j_jdbc_execution_options.getBatchIntervalMs()

    def get_batch_size(self) -> int:
        return self._j_jdbc_execution_options.getBatchSize()

    def get_max_retries(self) -> int:
        return self._j_jdbc_execution_options.getMaxRetries()

    @staticmethod
    def defaults() -> 'JdbcExecutionOptions':
        return JdbcExecutionOptions(
            j_jdbc_execution_options=get_gateway().jvm
            .org.apache.flink.connector.jdbc.JdbcExecutionOptions.defaults())

    @staticmethod
    def builder() -> 'Builder':
        return JdbcExecutionOptions.Builder()

    class Builder(object):
        """
        Builder for JdbcExecutionOptions.
        """
        def __init__(self):
            self._j_builder = get_gateway().jvm\
                .org.apache.flink.connector.jdbc.JdbcExecutionOptions.builder()

        def with_batch_size(self, size: int) -> 'JdbcExecutionOptions.Builder':
            self._j_builder.withBatchSize(size)
            return self

        def with_batch_interval_ms(self, interval_ms: int) -> 'JdbcExecutionOptions.Builder':
            self._j_builder.withBatchIntervalMs(interval_ms)
            return self

        def with_max_retries(self, max_retries: int) -> 'JdbcExecutionOptions.Builder':
            self._j_builder.withMaxRetries(max_retries)
            return self

        def build(self) -> 'JdbcExecutionOptions':
            return JdbcExecutionOptions(j_jdbc_execution_options=self._j_builder.build())
