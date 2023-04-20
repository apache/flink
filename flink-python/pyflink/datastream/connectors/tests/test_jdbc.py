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
from pyflink.common import Types
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase
from pyflink.util.java_utils import get_field_value


class FlinkJdbcSinkTest(PyFlinkStreamingTestCase):

    def test_jdbc_sink(self):
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        jdbc_connection_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder()\
            .with_driver_name('com.mysql.jdbc.Driver')\
            .with_user_name('root')\
            .with_password('password')\
            .with_url('jdbc:mysql://server-name:server-port/database-name').build()

        jdbc_execution_options = JdbcExecutionOptions.builder().with_batch_interval_ms(2000)\
            .with_batch_size(100).with_max_retries(5).build()
        jdbc_sink = JdbcSink.sink("insert into test table", ds.get_type(), jdbc_connection_options,
                                  jdbc_execution_options)

        ds.add_sink(jdbc_sink).name('jdbc sink')
        plan = eval(self.env.get_execution_plan())
        self.assertEqual('Sink: jdbc sink', plan['nodes'][1]['type'])
        j_output_format = get_field_value(jdbc_sink.get_java_function(), 'outputFormat')

        connection_options = JdbcConnectionOptions(
            get_field_value(get_field_value(j_output_format, 'connectionProvider'),
                            'jdbcOptions'))
        self.assertEqual(jdbc_connection_options.get_db_url(), connection_options.get_db_url())
        self.assertEqual(jdbc_connection_options.get_driver_name(),
                         connection_options.get_driver_name())
        self.assertEqual(jdbc_connection_options.get_password(), connection_options.get_password())
        self.assertEqual(jdbc_connection_options.get_user_name(),
                         connection_options.get_user_name())

        exec_options = JdbcExecutionOptions(get_field_value(j_output_format, 'executionOptions'))
        self.assertEqual(jdbc_execution_options.get_batch_interval_ms(),
                         exec_options.get_batch_interval_ms())
        self.assertEqual(jdbc_execution_options.get_batch_size(),
                         exec_options.get_batch_size())
        self.assertEqual(jdbc_execution_options.get_max_retries(),
                         exec_options.get_max_retries())
