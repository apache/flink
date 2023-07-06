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
from pyflink.table import ResultKind, ExplainDetail
from pyflink.table import expressions as expr
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, \
    PyFlinkTestCase


class StreamSqlTests(PyFlinkStreamTableTestCase):

    def test_sql_ddl(self):
        self.t_env.execute_sql("create temporary function func1 as "
                               "'pyflink.table.tests.test_udf.add' language python")
        table = self.t_env.from_elements([(1, 2)]) \
            .alias("a", "b") \
            .select(expr.call("func1", expr.col("a"), expr.col("b")))
        plan = table.explain()
        self.assertGreaterEqual(plan.find("== Optimized Physical Plan =="), 0)
        self.assertGreaterEqual(plan.find("PythonCalc(select=[func1(f0, f1) AS _c0])"), 0)

        plan = table.explain(ExplainDetail.PLAN_ADVICE)
        self.assertGreaterEqual(plan.find("== Optimized Physical Plan With Advice =="), 0)
        self.assertGreaterEqual(plan.find("No available advice..."), 0)

    def test_sql_query(self):
        t_env = self.t_env
        source = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
        sink_table_ddl = """
        CREATE TABLE sinks_sql_query(a BIGINT, b STRING, c STRING) WITH ('connector'='test-sink')
        """
        t_env.execute_sql(sink_table_ddl)

        result = t_env.sql_query("select a + 1, b, c from %s" % source)
        result.execute_insert("sinks_sql_query").wait()
        actual = source_sink_utils.results()

        expected = ['+I[2, Hi, Hello]', '+I[3, Hello, Hello]']
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

        sink_table_ddl = """
        CREATE TABLE sinks(k1 BIGINT, k2 INT, c STRING) WITH ('connector'='test-sink')
        """
        t_env.execute_sql(sink_table_ddl)
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
        test_jar_pattern = "flink-python/target/artifacts/testJavaDdl.jar"
        test_jar_path = self.get_jar_path(test_jar_pattern)
        test_classpath = self.get_classpath() + os.pathsep + test_jar_path
        java_executable = self.get_java_executable()
        subprocess.check_output([java_executable,
                                 "-XX:+IgnoreUnrecognizedVMOptions",
                                 "--add-opens=java.base/java.lang=ALL-UNNAMED",
                                 "-cp", test_classpath, test_class], shell=False)
