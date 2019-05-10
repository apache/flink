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
import tempfile

from py4j.compat import unicode
from pyflink.table.table_environment import TableEnvironment
from pyflink.table.table_config import TableConfig
from pyflink.table.table_sink import CsvTableSink
from pyflink.table.types import DataTypes
from pyflink.testing.test_case_utils import PyFlinkBatchTableTestCase


class BatchTableEnvironmentTests(PyFlinkBatchTableTestCase):

    def test_register_scan(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        t_env = self.t_env
        t_env.register_table_source("Source", csv_source)
        result = t_env.scan("Source")

        actual = self.collect(result)
        expected = ['1,Hi,Hello', '2,Hello,Hello']
        self.assert_equals(actual, expected)

    def test_register_table_source_sink(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        t_env = self.t_env
        t_env.register_table_source("Orders", csv_source)

        tmp_dir = tempfile.gettempdir()
        tmp_csv = tmp_dir + '/streaming2.csv'
        if os.path.isfile(tmp_csv):
            os.remove(tmp_csv)

        t_env.register_table_sink(
            "Results",
            field_names, field_types, CsvTableSink(tmp_csv))

        t_env.scan("Orders").insert_into("Results")

        t_env.execute()
        with open(tmp_csv, 'r') as f:
            lines = f.read()
            assert lines == '1,Hi,Hello\n'

    def test_from_table_source(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hi", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        t_env = self.t_env
        source = t_env.from_table_source(csv_source)

        actual = self.collect(source)
        expected = ['1,Hi,Hello', '2,Hi,Hello']
        self.assert_equals(actual, expected)

    def test_list_tables(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = []
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        t_env = self.t_env
        t_env.register_table_source("Orders", csv_source)

        tmp_dir = tempfile.gettempdir()
        tmp_csv = tmp_dir + '/streaming2.csv'

        t_env.register_table_sink(
            "Sinks",
            field_names, field_types, CsvTableSink(tmp_csv))

        t_env.register_table_sink(
            "Results",
            field_names, field_types, CsvTableSink(tmp_csv))

        actual = t_env.list_tables()
        expected = ['Orders', 'Results', 'Sinks']
        self.assert_equals(actual, expected)

    def test_explain(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = []
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        t_env = self.t_env
        t_env.register_table_source("Source", csv_source)
        source = t_env.scan("Source")
        result = source.alias("a, b, c").select("1 + a, b, c")

        actual = t_env.explain(result)
        assert isinstance(actual, str) or isinstance(actual, unicode)

    def test_sql_query_update(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        t_env = self.t_env
        t_env.register_table_source("Source", csv_source)
        source = t_env.scan("Source")
        result = t_env.sql_query("select a + 1, b, c from %s" % source)
        t_env.register_table("results", result)

        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        tmp_dir = tempfile.gettempdir()
        tmp_csv = tmp_dir + '/streaming2.csv'
        if os.path.isfile(tmp_csv):
            os.remove(tmp_csv)

        t_env.register_table_sink(
            "sinks",
            field_names, field_types, CsvTableSink(tmp_csv))

        query_config = t_env.query_config()

        t_env.sql_update("insert into sinks select * from results", query_config)
        t_env.execute("test_sql_job")

        with open(tmp_csv, 'r') as f:
            lines = f.read()
            print(lines)
            assert lines == '2,Hi,Hello\n' + '3,Hello,Hello\n'

        if os.path.isfile(tmp_csv):
            os.remove(tmp_csv)
        t_env.sql_update("insert into sinks select * from results")
        t_env.execute()

        with open(tmp_csv, 'r') as f:
            lines = f.read()
            print(lines)
            assert lines == '2,Hi,Hello\n' + '3,Hello,Hello\n'

    def test_table_config(self):
        t_env = self.t_env
        table_config = t_env.get_config()
        table_config.parallelism = 4
        table_config.null_check = True
        table_config.max_generated_code_length = 64000
        table_config.timezone = "Asia/Shanghai"
        assert table_config.parallelism == 4
        assert table_config.null_check is True
        assert table_config.max_generated_code_length == 64000
        assert table_config.timezone == "Asia/Shanghai"
        assert table_config.is_stream is False

    def test_create_table_environment(self):
        table_config = TableConfig.Builder()\
            .set_parallelism(2)\
            .set_max_generated_code_length(32000)\
            .set_null_check(False)\
            .set_timezone("Asia/Shanghai")\
            .as_batch_execution()\
            .build()
        t_env = TableEnvironment.create(table_config)
        readed_table_config = t_env.get_config()
        assert readed_table_config.parallelism == 2
        assert readed_table_config.null_check is False
        assert readed_table_config.max_generated_code_length == 32000
        assert readed_table_config.timezone == "Asia/Shanghai"
        assert readed_table_config.is_stream is False
