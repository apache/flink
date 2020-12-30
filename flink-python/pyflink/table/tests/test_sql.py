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
import subprocess

from pyflink.find_flink_home import _find_flink_source_root
from pyflink.java_gateway import get_gateway
from pyflink.table import DataTypes, ResultKind
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, PyFlinkBatchTableTestCase, \
    PyFlinkTestCase


class SqlTests(object):

    def test_sql_ddl(self):
        self.t_env.execute_sql("create temporary function func1 as "
                               "'pyflink.table.tests.test_udf.add' language python")
        table = self.t_env.from_elements([(1, 2)]).alias("a, b").select("func1(a, b)")
        plan = table.explain()
        self.assertTrue(plan.find("PythonCalc(select=[add(f0, f1) AS _c0])") >= 0)


class StreamSqlTests(SqlTests, PyFlinkStreamTableTestCase):

    def test_sql_query(self):
        t_env = self.t_env
        source = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "sinks",
            source_sink_utils.TestAppendSink(field_names, field_types))

        result = t_env.sql_query("select a + 1, b, c from %s" % source)
        result.execute_insert("sinks").wait()
        actual = source_sink_utils.results()

        expected = ['2,Hi,Hello', '3,Hello,Hello']
        self.assert_equals(actual, expected)

    def test_execute_sql(self):
        t_env = self.t_env
        table_result = t_env.execute_sql("create table tbl"
                                         "("
                                         "   a bigint,"
                                         "   b int,"
                                         "   c varchar"
                                         ") with ("
                                         "  'connector' = 'COLLECTION',"
                                         "   'is-bounded' = 'false'"
                                         ")")
        self.assertIsNone(table_result.get_job_client())
        self.assert_equals(table_result.get_table_schema().get_field_names(), ["result"])
        self.assertEqual(table_result.get_result_kind(), ResultKind.SUCCESS)
        table_result.print()

        table_result = t_env.execute_sql("alter table tbl set ('k1' = 'a', 'k2' = 'b')")
        self.assertIsNone(table_result.get_job_client())
        self.assert_equals(table_result.get_table_schema().get_field_names(), ["result"])
        self.assertEqual(table_result.get_result_kind(), ResultKind.SUCCESS)
        table_result.print()

        field_names = ["k1", "k2", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.INT(), DataTypes.STRING()]
        t_env.register_table_sink(
            "sinks",
            source_sink_utils.TestAppendSink(field_names, field_types))
        table_result = t_env.execute_sql("insert into sinks select * from tbl")
        job_execution_result = table_result.get_job_client().get_job_execution_result().result()
        self.assertIsNotNone(job_execution_result.get_job_id())
        self.assert_equals(table_result.get_table_schema().get_field_names(),
                           ["default_catalog.default_database.sinks"])
        self.assertEqual(table_result.get_result_kind(), ResultKind.SUCCESS_WITH_CONTENT)
        table_result.print()

        table_result = t_env.execute_sql("drop table tbl")
        self.assertIsNone(table_result.get_job_client())
        self.assert_equals(table_result.get_table_schema().get_field_names(), ["result"])
        self.assertEqual(table_result.get_result_kind(), ResultKind.SUCCESS)
        table_result.print()

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


class BatchSqlTests(SqlTests, PyFlinkBatchTableTestCase):
    pass


class JavaSqlTests(PyFlinkTestCase):
    """
    We need to start these Java tests from python process to make sure that Python environment is
    available when the tests are running.
    """

    @staticmethod
    def get_classpath():
        return get_gateway().jvm.System.getProperties().get("java.class.path")

    @staticmethod
    def get_java_executable():
        return get_gateway().jvm.System.getProperty("java.home") + "/bin/java"

    def get_jar_path(self, jar_path_pattern):
        test_jar_path = glob.glob(os.path.join(_find_flink_source_root(), jar_path_pattern))
        if not test_jar_path:
            self.fail("'%s' is not available. Please compile the test jars first."
                      % jar_path_pattern)
        if len(test_jar_path) > 1:
            self.fail("There are multiple jars matches the pattern: %s, the jars are: %s"
                      % (jar_path_pattern, test_jar_path))
        return test_jar_path[0]

    def test_java_sql_ddl(self):
        test_class = "org.apache.flink.client.python.PythonFunctionFactoryTest"
        test_jar_pattern = "flink-python/target/javaDDL/flink-python*-tests.jar"
        test_jar_path = self.get_jar_path(test_jar_pattern)
        test_classpath = self.get_classpath() + os.pathsep + test_jar_path
        java_executable = self.get_java_executable()
        subprocess.check_output([java_executable, "-cp", test_classpath, test_class], shell=False)
