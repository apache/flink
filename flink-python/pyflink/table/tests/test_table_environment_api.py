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
import glob
import os
import pathlib
import sys

from py4j.protocol import Py4JJavaError

from pyflink.find_flink_home import _find_flink_source_root
from pyflink.java_gateway import get_gateway

from pyflink.dataset import ExecutionEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, CsvTableSink, StreamTableEnvironment, EnvironmentSettings
from pyflink.table.descriptors import FileSystem, OldCsv, Schema
from pyflink.table.table_config import TableConfig
from pyflink.table.table_environment import BatchTableEnvironment
from pyflink.table.types import RowType
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, PyFlinkBatchTableTestCase, \
    PyFlinkBlinkBatchTableTestCase
from pyflink.util.utils import get_j_env_configuration
from pyflink.table.explain_detail import ExplainDetail


class TableEnvironmentTest(object):

    def test_set_sys_executable_for_local_mode(self):
        jvm = get_gateway().jvm
        actual_executable = get_j_env_configuration(self.t_env) \
            .getString(jvm.PythonOptions.PYTHON_EXECUTABLE.key(), None)
        self.assertEqual(sys.executable, actual_executable)

    def test_explain(self):
        schema = RowType()\
            .add('a', DataTypes.INT())\
            .add('b', DataTypes.STRING())\
            .add('c', DataTypes.STRING())
        t_env = self.t_env
        t = t_env.from_elements([], schema)
        result = t.select("1 + a, b, c")

        actual = t_env.explain(result)

        assert isinstance(actual, str)

    def test_explain_with_extended(self):
        schema = RowType() \
            .add('a', DataTypes.INT()) \
            .add('b', DataTypes.STRING()) \
            .add('c', DataTypes.STRING())
        t_env = self.t_env
        t = t_env.from_elements([], schema)
        result = t.select("1 + a, b, c")

        actual = t_env.explain(result, True)

        assert isinstance(actual, str)

    def test_register_java_function(self):
        t_env = self.t_env

        t_env.register_java_function("scalar_func",
                                     "org.apache.flink.table.expressions.utils.RichFunc0")
        t_env.register_java_function(
            "agg_func", "org.apache.flink.table.functions.aggfunctions.ByteMaxAggFunction")
        t_env.register_java_function("table_func", "org.apache.flink.table.utils.TableFunc1")

        actual = t_env.list_user_defined_functions()
        expected = ['scalar_func', 'agg_func', 'table_func']
        self.assert_equals(actual, expected)


class StreamTableEnvironmentTests(TableEnvironmentTest, PyFlinkStreamTableTestCase):

    def test_register_table_source_scan(self):
        t_env = self.t_env
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        csv_source = self.prepare_csv_source(source_path, [], field_types, field_names)
        t_env.register_table_source("Source", csv_source)

        result = t_env.scan("Source")
        self.assertEqual(
            'CatalogTable: (identifier: [`default_catalog`.`default_database`.`Source`]'
            ', fields: [a, b, c])',
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

    def test_temporary_tables(self):
        t_env = self.t_env
        t_env.connect(FileSystem().path(os.path.join(self.tempdir + '/temp_1.csv'))) \
            .with_format(OldCsv()
                         .field_delimiter(',')
                         .field("a", DataTypes.INT())
                         .field("b", DataTypes.STRING())) \
            .with_schema(Schema()
                         .field("a", DataTypes.INT())
                         .field("b", DataTypes.STRING())) \
            .create_temporary_table("temporary_table_1")

        t_env.connect(FileSystem().path(os.path.join(self.tempdir + '/temp_2.csv'))) \
            .with_format(OldCsv()
                         .field_delimiter(',')
                         .field("a", DataTypes.INT())
                         .field("b", DataTypes.STRING())) \
            .with_schema(Schema()
                         .field("a", DataTypes.INT())
                         .field("b", DataTypes.STRING())) \
            .create_temporary_table("temporary_table_2")

        actual = t_env.list_temporary_tables()
        expected = ['temporary_table_1', 'temporary_table_2']
        self.assert_equals(actual, expected)

        t_env.drop_temporary_table("temporary_table_1")
        actual = t_env.list_temporary_tables()
        expected = ['temporary_table_2']
        self.assert_equals(actual, expected)

    def test_temporary_views(self):
        t_env = self.t_env
        t_env.create_temporary_view(
            "temporary_view_1",
            t_env.from_elements([(1, 'Hi', 'Hello')], ['a', 'b', 'c']))
        t_env.create_temporary_view(
            "temporary_view_2",
            t_env.from_elements([(1, 'Hi')], ['a', 'b']))

        actual = t_env.list_temporary_views()
        expected = ['temporary_view_1', 'temporary_view_2']
        self.assert_equals(actual, expected)

        t_env.drop_temporary_view("temporary_view_1")
        actual = t_env.list_temporary_views()
        expected = ['temporary_view_2']
        self.assert_equals(actual, expected)

    def test_from_path(self):
        t_env = self.t_env
        t_env.create_temporary_view(
            "temporary_view_1",
            t_env.from_elements([(1, 'Hi', 'Hello')], ['a', 'b', 'c']))
        result = t_env.from_path("temporary_view_1")
        self.assertEqual(
            'CatalogTable: (identifier: [`default_catalog`.`default_database`.`temporary_view_1`]'
            ', fields: [a, b, c])',
            result._j_table.getQueryOperation().asSummaryString())

    def test_insert_into(self):
        t_env = self.t_env
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Sinks",
            source_sink_utils.TestAppendSink(field_names, field_types))

        t_env.insert_into("Sinks", t_env.from_elements([(1, "Hi", "Hello")], ["a", "b", "c"]))
        self.t_env.execute("test")

        actual = source_sink_utils.results()
        expected = ['1,Hi,Hello']
        self.assert_equals(actual, expected)

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
        assert isinstance(actual, str)

    def test_explain_sql_without_explain_detail(self):
        t_env = self.t_env
        source = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "sinks",
            source_sink_utils.TestAppendSink(field_names, field_types))

        result = t_env.explain_sql("select a + 1, b, c from %s" % source)

        assert isinstance(result, str)

    def test_explain_sql_with_explain_detail(self):
        t_env = self.t_env
        source = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "sinks",
            source_sink_utils.TestAppendSink(field_names, field_types))

        result = t_env.explain_sql(
            "select a + 1, b, c from %s" % source, ExplainDetail.CHANGELOG_MODE)

        assert isinstance(result, str)

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

    def test_create_table_environment_with_blink_planner(self):
        t_env = StreamTableEnvironment.create(
            self.env,
            environment_settings=EnvironmentSettings.new_instance().use_blink_planner().build())

        planner = t_env._j_tenv.getPlanner()

        self.assertEqual(
            planner.getClass().getName(),
            "org.apache.flink.table.planner.delegation.StreamPlanner")

        t_env = StreamTableEnvironment.create(
            environment_settings=EnvironmentSettings.new_instance().build())

        planner = t_env._j_tenv.getPlanner()

        self.assertEqual(
            planner.getClass().getName(),
            "org.apache.flink.table.planner.delegation.StreamPlanner")

        t_env = StreamTableEnvironment.create(
            environment_settings=EnvironmentSettings.new_instance().use_old_planner().build())

        planner = t_env._j_tenv.getPlanner()

        self.assertEqual(
            planner.getClass().getName(),
            "org.apache.flink.table.planner.StreamPlanner")

    def test_table_environment_with_blink_planner(self):
        self.env.set_parallelism(1)
        t_env = StreamTableEnvironment.create(
            self.env,
            environment_settings=EnvironmentSettings.new_instance().use_blink_planner().build())

        source_path = os.path.join(self.tempdir + '/streaming.csv')
        sink_path = os.path.join(self.tempdir + '/result.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]
        data = [(1, 'hi', 'hello'), (2, 'hello', 'hello')]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        t_env.register_table_source("source", csv_source)

        t_env.register_table_sink(
            "sink",
            CsvTableSink(field_names, field_types, sink_path))
        source = t_env.scan("source")

        result = source.alias("a, b, c").select("1 + a, b, c")

        result.insert_into("sink")

        t_env.execute("blink_test")

        results = []
        with open(sink_path, 'r') as f:
            results.append(f.readline())
            results.append(f.readline())

        self.assert_equals(results, ['2,hi,hello\n', '3,hello,hello\n'])

    def test_set_jars(self):
        self.verify_set_java_dependencies("pipeline.jars")

    def test_set_classpaths(self):
        self.verify_set_java_dependencies("pipeline.classpaths")

    def verify_set_java_dependencies(self, config_key):
        original_class_loader = \
            get_gateway().jvm.Thread.currentThread().getContextClassLoader()
        try:
            jar_urls = []
            func1_class_name = "org.apache.flink.python.util.TestScalarFunction1"
            func2_class_name = "org.apache.flink.python.util.TestScalarFunction2"
            func1_jar_pattern = "flink-python/target/func1/flink-python*-tests.jar"
            func2_jar_pattern = "flink-python/target/func2/flink-python*-tests.jar"
            self.ensure_jar_not_loaded(func1_class_name, func1_jar_pattern)
            self.ensure_jar_not_loaded(func2_class_name, func2_jar_pattern)
            jar_urls.extend(self.get_jar_url(func1_jar_pattern))
            jar_urls.extend(self.get_jar_url(func2_jar_pattern))

            # test set the "pipeline.jars" multiple times
            self.t_env.get_config().get_configuration().set_string(config_key, ";".join(jar_urls))
            first_class_loader = get_gateway().jvm.Thread.currentThread().getContextClassLoader()

            self.t_env.get_config().get_configuration().set_string(config_key, jar_urls[0])
            self.t_env.get_config().get_configuration().set_string(config_key, ";".join(jar_urls))
            second_class_loader = get_gateway().jvm.Thread.currentThread().getContextClassLoader()

            self.assertEqual(first_class_loader, second_class_loader)

            source = self.t_env.from_elements([(1, "Hi"), (2, "Hello")], ["a", "b"])
            self.t_env.register_java_function("func1", func1_class_name)
            self.t_env.register_java_function("func2", func2_class_name)
            table_sink = source_sink_utils.TestAppendSink(
                ["a", "b"], [DataTypes.STRING(), DataTypes.STRING()])
            self.t_env.register_table_sink("sink", table_sink)
            source.select("func1(a, b), func2(a, b)").insert_into("sink")
            self.t_env.execute("test")
            actual = source_sink_utils.results()
            expected = ['1 and Hi,1 or Hi', '2 and Hello,2 or Hello']
            self.assert_equals(actual, expected)
        finally:
            get_gateway().jvm.Thread.currentThread().setContextClassLoader(original_class_loader)

    def ensure_jar_not_loaded(self, func_class_name, jar_filename_pattern):
        test_jars = glob.glob(os.path.join(_find_flink_source_root(), jar_filename_pattern))
        if not test_jars:
            self.fail("'%s' is not available. Please compile the test jars first."
                      % jar_filename_pattern)
        try:
            self.t_env.register_java_function("func", func_class_name)
        except Py4JJavaError:
            pass
        else:
            self.fail("The scalar function '%s' should not be able to be loaded. Please remove "
                      "the '%s' from the classpath of the PythonGatewayServer process." %
                      (func_class_name, jar_filename_pattern))

    @staticmethod
    def get_jar_url(jar_filename_pattern):
        test_jars = glob.glob(os.path.join(_find_flink_source_root(), jar_filename_pattern))
        return [pathlib.Path(jar_path).as_uri() for jar_path in test_jars]


class BatchTableEnvironmentTests(TableEnvironmentTest, PyFlinkBatchTableTestCase):

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

        actual = t_env.explain(extended=True)

        assert isinstance(actual, str)

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

    def test_create_table_environment_with_blink_planner(self):
        t_env = BatchTableEnvironment.create(
            environment_settings=EnvironmentSettings.new_instance().in_batch_mode()
            .use_blink_planner().build())

        planner = t_env._j_tenv.getPlanner()

        self.assertEqual(
            planner.getClass().getName(),
            "org.apache.flink.table.planner.delegation.BatchPlanner")

    def test_table_environment_with_blink_planner(self):
        t_env = BatchTableEnvironment.create(
            environment_settings=EnvironmentSettings.new_instance().in_batch_mode()
            .use_blink_planner().build())

        source_path = os.path.join(self.tempdir + '/streaming.csv')
        sink_path = os.path.join(self.tempdir + '/results')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]
        data = [(1, 'hi', 'hello'), (2, 'hello', 'hello')]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        t_env.register_table_source("source", csv_source)

        t_env.register_table_sink(
            "sink",
            CsvTableSink(field_names, field_types, sink_path))
        source = t_env.scan("source")

        result = source.alias("a, b, c").select("1 + a, b, c")

        result.insert_into("sink")

        t_env.execute("blink_test")

        results = []
        for root, dirs, files in os.walk(sink_path):
            for sub_file in files:
                with open(os.path.join(root, sub_file), 'r') as f:
                    line = f.readline()
                    while line is not None and line != '':
                        results.append(line)
                        line = f.readline()

        self.assert_equals(results, ['2,hi,hello\n', '3,hello,hello\n'])


class BlinkBatchTableEnvironmentTests(PyFlinkBlinkBatchTableTestCase):

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

        actual = t_env.explain(extended=True)
        self.assertIsInstance(actual, str)

    def test_register_java_function(self):
        t_env = self.t_env

        t_env.register_java_function(
            "scalar_func", "org.apache.flink.table.expressions.utils.RichFunc0")

        t_env.register_java_function(
            "agg_func", "org.apache.flink.table.functions.aggfunctions.ByteMaxAggFunction")

        t_env.register_java_function(
            "table_func", "org.apache.flink.table.utils.TableFunc2")

        actual = t_env.list_user_defined_functions()
        expected = ['scalar_func', 'agg_func', 'table_func']
        self.assert_equals(actual, expected)
