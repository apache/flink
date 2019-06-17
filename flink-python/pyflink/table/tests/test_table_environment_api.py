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
# #  distributed under the License is distributed on an "AS IS" BASIS,
# #  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# #  See the License for the specific language governing permissions and
# # limitations under the License.
################################################################################
import datetime
import os

from py4j.compat import unicode
from pyflink.table.table_environment import TableEnvironment
from pyflink.table.table_config import TableConfig
from pyflink.table.types import DataTypes, RowType
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase


class StreamTableEnvironmentTests(PyFlinkStreamTableTestCase):

    def test_register_table_source_scan(self):
        t_env = self.t_env
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        csv_source = self.prepare_csv_source(source_path, [], field_types, field_names)
        t_env.register_table_source("Source", csv_source)

        result = t_env.scan("Source")
        self.assertEqual(
            'CatalogTable: (path: [default_catalog, default_database, Source], fields: [a, b, c])',
            result._j_table.getQueryOperation().asSummaryString())

    def test_register_table_sink(self):
        t_env = self.t_env
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Sinks",
            field_names, field_types, source_sink_utils.TestAppendSink())

        t_env.from_elements([(1, "Hi", "Hello")], ["a", "b", "c"]).insert_into("Sinks")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['1,Hi,Hello']
        self.assert_equals(actual, expected)

    def test_from_table_source(self):
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        csv_source = self.prepare_csv_source(source_path, [], field_types, field_names)

        result = self.t_env.from_table_source(csv_source)
        self.assertEqual(
            'TableSource: (fields: [a, b, c])',
            result._j_table.getQueryOperation().asSummaryString())

    def test_list_tables(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]
        data = []
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        t_env = self.t_env
        t_env.register_table_source("Orders", csv_source)
        t_env.register_table_sink(
            "Sinks",
            field_names, field_types, source_sink_utils.TestAppendSink())
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestAppendSink())

        actual = t_env.list_tables()

        expected = ['Orders', 'Results', 'Sinks']
        self.assert_equals(actual, expected)

    def test_explain(self):
        schema = RowType()\
            .add('a', DataTypes.INT())\
            .add('b', DataTypes.STRING())\
            .add('c', DataTypes.STRING())
        t_env = self.t_env
        t = t_env.from_elements([], schema)
        result = t.select("1 + a, b, c")

        actual = t_env.explain(result)

        assert isinstance(actual, str) or isinstance(actual, unicode)

    def test_sql_query(self):
        t_env = self.t_env
        source = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "sinks",
            field_names, field_types, source_sink_utils.TestAppendSink())

        result = t_env.sql_query("select a + 1, b, c from %s" % source)
        result.insert_into("sinks")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['2,Hi,Hello', '3,Hello,Hello']
        self.assert_equals(actual, expected)

    def test_sql_update(self):
        t_env = self.t_env
        source = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "sinks",
            field_names, field_types, source_sink_utils.TestAppendSink())

        t_env.sql_update("insert into sinks select * from %s" % source)
        t_env.execute("test_sql_job")

        actual = source_sink_utils.results()
        expected = ['1,Hi,Hello', '2,Hello,Hello']
        self.assert_equals(actual, expected)

    def test_sql_update_with_query_config(self):
        t_env = self.t_env
        source = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "sinks",
            field_names, field_types, source_sink_utils.TestAppendSink())
        query_config = t_env.query_config()
        query_config.with_idle_state_retention_time(
            datetime.timedelta(days=1), datetime.timedelta(days=2))

        t_env.sql_update("insert into sinks select * from %s" % source, query_config)
        t_env.execute("test_sql_job")

        actual = source_sink_utils.results()
        expected = ['1,Hi,Hello', '2,Hello,Hello']
        self.assert_equals(actual, expected)

    def test_query_config(self):
        query_config = self.t_env.query_config()

        query_config.with_idle_state_retention_time(
            datetime.timedelta(days=1), datetime.timedelta(days=2))

        assert query_config.get_max_idle_state_retention_time() == 2 * 24 * 3600 * 1000
        assert query_config.get_min_idle_state_retention_time() == 24 * 3600 * 1000

    @staticmethod
    def test_table_config():

        table_config = TableConfig.Builder()\
            .as_streaming_execution()\
            .set_timezone("Asia/Shanghai")\
            .set_max_generated_code_length(64000)\
            .set_null_check(True)\
            .set_parallelism(4).build()

        assert table_config.parallelism() == 4
        assert table_config.null_check() is True
        assert table_config.max_generated_code_length() == 64000
        assert table_config.timezone() == "Asia/Shanghai"
        assert table_config.is_stream() is True

    @staticmethod
    def test_create_table_environment():
        table_config = TableConfig.Builder()\
            .set_parallelism(2)\
            .set_max_generated_code_length(32000)\
            .set_null_check(False)\
            .set_timezone("Asia/Shanghai")\
            .as_streaming_execution()\
            .build()

        t_env = TableEnvironment.create(table_config)

        readed_table_config = t_env.get_config()
        assert readed_table_config.parallelism() == 2
        assert readed_table_config.null_check() is False
        assert readed_table_config.max_generated_code_length() == 32000
        assert readed_table_config.timezone() == "Asia/Shanghai"
        assert readed_table_config.is_stream() is True
