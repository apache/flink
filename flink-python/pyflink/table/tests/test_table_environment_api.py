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
import os

from py4j.compat import unicode

from pyflink.dataset import ExecutionEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, CsvTableSink, StreamTableEnvironment
from pyflink.table.table_config import TableConfig
from pyflink.table.table_environment import BatchTableEnvironment
from pyflink.table.types import RowType
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, PyFlinkBatchTableTestCase
from pyflink.util.exceptions import TableException


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
            source_sink_utils.TestAppendSink(field_names, field_types))

        t_env.from_elements([(1, "Hi", "Hello")], ["a", "b", "c"]).insert_into("Sinks")
        self.t_env.execute("test")
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
            source_sink_utils.TestAppendSink(field_names, field_types))
        t_env.register_table_sink(
            "Results",
            source_sink_utils.TestAppendSink(field_names, field_types))

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

    def test_explain_with_extended(self):
        schema = RowType() \
            .add('a', DataTypes.INT()) \
            .add('b', DataTypes.STRING()) \
            .add('c', DataTypes.STRING())
        t_env = self.t_env
        t = t_env.from_elements([], schema)
        result = t.select("1 + a, b, c")

        actual = t_env.explain(result, True)

        assert isinstance(actual, str) or isinstance(actual, unicode)

    def test_explain_with_multi_sinks(self):
        t_env = self.t_env
        source = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "sink1",
            source_sink_utils.TestAppendSink(field_names, field_types))
        t_env.register_table_sink(
            "sink2",
            source_sink_utils.TestAppendSink(field_names, field_types))

        t_env.sql_update("insert into sink1 select * from %s where a > 100" % source)
        t_env.sql_update("insert into sink2 select * from %s where a < 100" % source)

        actual = t_env.explain(extended=True)

        assert isinstance(actual, str) or isinstance(actual, unicode)

    def test_sql_query(self):
        t_env = self.t_env
        source = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "sinks",
            source_sink_utils.TestAppendSink(field_names, field_types))

        result = t_env.sql_query("select a + 1, b, c from %s" % source)
        result.insert_into("sinks")
        self.t_env.execute("test")
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
            source_sink_utils.TestAppendSink(field_names, field_types))

        t_env.sql_update("insert into sinks select * from %s" % source)
        self.t_env.execute("test_sql_job")

        actual = source_sink_utils.results()
        expected = ['1,Hi,Hello', '2,Hello,Hello']
        self.assert_equals(actual, expected)

    def test_create_table_environment(self):
        table_config = TableConfig()
        table_config.set_max_generated_code_length(32000)
        table_config.set_null_check(False)
        table_config.set_local_timezone("Asia/Shanghai")

        env = StreamExecutionEnvironment.get_execution_environment()
        t_env = StreamTableEnvironment.create(env, table_config)

        readed_table_config = t_env.get_config()

        self.assertFalse(readed_table_config.get_null_check())
        self.assertEqual(readed_table_config.get_max_generated_code_length(), 32000)
        self.assertEqual(readed_table_config.get_local_timezone(), "Asia/Shanghai")


class BatchTableEnvironmentTests(PyFlinkBatchTableTestCase):

    def test_explain(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]
        data = []
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        t_env = self.t_env
        t_env.register_table_source("Source", csv_source)
        source = t_env.scan("Source")
        result = source.alias("a, b, c").select("1 + a, b, c")

        actual = t_env.explain(result)

        self.assertIsInstance(actual, (str, unicode))

    def test_explain_with_extended(self):
        schema = RowType() \
            .add('a', DataTypes.INT()) \
            .add('b', DataTypes.STRING()) \
            .add('c', DataTypes.STRING())
        t_env = self.t_env
        t = t_env.from_elements([], schema)
        result = t.select("1 + a, b, c")

        actual = t_env.explain(result, True)

        assert isinstance(actual, str) or isinstance(actual, unicode)

    def test_explain_with_multi_sinks(self):
        t_env = self.t_env
        source = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "sink1",
            CsvTableSink(field_names, field_types, "path1"))
        t_env.register_table_sink(
            "sink2",
            CsvTableSink(field_names, field_types, "path2"))

        t_env.sql_update("insert into sink1 select * from %s where a > 100" % source)
        t_env.sql_update("insert into sink2 select * from %s where a < 100" % source)

        with self.assertRaises(TableException):
            t_env.explain(extended=True)

    def test_create_table_environment(self):
        table_config = TableConfig()
        table_config.set_max_generated_code_length(32000)
        table_config.set_null_check(False)
        table_config.set_local_timezone("Asia/Shanghai")

        env = ExecutionEnvironment.get_execution_environment()
        t_env = BatchTableEnvironment.create(env, table_config)

        readed_table_config = t_env.get_config()

        self.assertFalse(readed_table_config.get_null_check())
        self.assertEqual(readed_table_config.get_max_generated_code_length(), 32000)
        self.assertEqual(readed_table_config.get_local_timezone(), "Asia/Shanghai")
