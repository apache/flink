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
import unittest

from pyflink.table import DataTypes
from pyflink.table.udf import TableFunction, udtf, ScalarFunction, udf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, \
    PyFlinkBatchTableTestCase


class UserDefinedTableFunctionTests(object):

    def test_table_function(self):
        self._register_table_sink(
            ['a', 'b', 'c'],
            [DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()])

        multi_emit = udtf(MultiEmit(), result_types=[DataTypes.BIGINT(), DataTypes.BIGINT()])
        multi_num = udf(MultiNum(), result_type=DataTypes.BIGINT())

        t = self.t_env.from_elements([(1, 1, 3), (2, 1, 6), (3, 2, 9)], ['a', 'b', 'c'])
        t = t.join_lateral(multi_emit((t.a + t.a) / 2, multi_num(t.b)).alias('x', 'y'))
        t = t.left_outer_join_lateral(condition_multi_emit(t.x, t.y).alias('m')) \
            .select("x, y, m")
        t = t.left_outer_join_lateral(identity(t.m).alias('n')) \
            .select("x, y, n")
        actual = self._get_output(t)
        self.assert_equals(actual,
                           ["+I[1, 0, null]", "+I[1, 1, null]", "+I[2, 0, null]", "+I[2, 1, null]",
                            "+I[3, 0, 0]", "+I[3, 0, 1]", "+I[3, 0, 2]", "+I[3, 1, 1]",
                            "+I[3, 1, 2]", "+I[3, 2, 2]", "+I[3, 3, null]"])

    def test_table_function_with_sql_query(self):
        self._register_table_sink(
            ['a', 'b', 'c'],
            [DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()])

        self.t_env.create_temporary_system_function(
            "multi_emit", udtf(MultiEmit(), result_types=[DataTypes.BIGINT(), DataTypes.BIGINT()]))

        t = self.t_env.from_elements([(1, 1, 3), (2, 1, 6), (3, 2, 9)], ['a', 'b', 'c'])
        self.t_env.register_table("MyTable", t)
        t = self.t_env.sql_query(
            "SELECT a, x, y FROM MyTable LEFT JOIN LATERAL TABLE(multi_emit(a, b)) as T(x, y)"
            " ON TRUE")
        actual = self._get_output(t)
        self.assert_equals(actual, ["+I[1, 1, 0]", "+I[2, 2, 0]", "+I[3, 3, 0]", "+I[3, 3, 1]"])

    def _register_table_sink(self, field_names: list, field_types: list):
        table_sink = source_sink_utils.TestAppendSink(field_names, field_types)
        self.t_env.register_table_sink("Results", table_sink)

    def _get_output(self, t):
        t.execute_insert("Results").wait()
        return source_sink_utils.results()


class PyFlinkStreamUserDefinedFunctionTests(UserDefinedTableFunctionTests,
                                            PyFlinkStreamTableTestCase):
    def test_execute_from_json_plan(self):
        # create source file path
        tmp_dir = self.tempdir
        data = ['1,1', '3,2', '2,1']
        source_path = tmp_dir + '/test_execute_from_json_plan_input.csv'
        sink_path = tmp_dir + '/test_execute_from_json_plan_out'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        source_table = """
            CREATE TABLE source_table (
                a BIGINT,
                b BIGINT
            ) WITH (
                'connector' = 'filesystem',
                'path' = '%s',
                'format' = 'csv'
            )
        """ % source_path
        self.t_env.execute_sql(source_table)

        self.t_env.execute_sql("""
            CREATE TABLE sink_table (
                a BIGINT,
                b BIGINT,
                c BIGINT
            ) WITH (
                'connector' = 'filesystem',
                'path' = '%s',
                'format' = 'csv'
            )
        """ % sink_path)

        self.t_env.create_temporary_system_function(
            "multi_emit", udtf(MultiEmit(), result_types=[DataTypes.BIGINT(), DataTypes.BIGINT()]))

        json_plan = self.t_env._j_tenv.getJsonPlan("INSERT INTO sink_table "
                                                   "SELECT a, x, y FROM source_table "
                                                   "LEFT JOIN LATERAL TABLE(multi_emit(a, b))"
                                                   " as T(x, y)"
                                                   " ON TRUE")
        from py4j.java_gateway import get_method
        get_method(self.t_env._j_tenv.executeJsonPlan(json_plan), "await")()

        import glob
        lines = [line.strip() for file in glob.glob(sink_path + '/*') for line in open(file, 'r')]
        lines.sort()
        self.assertEqual(lines, ['1,1,0', '2,2,0', '3,3,0', '3,3,1'])


class PyFlinkBatchUserDefinedFunctionTests(UserDefinedTableFunctionTests,
                                           PyFlinkBatchTableTestCase):
    pass


class MultiEmit(TableFunction, unittest.TestCase):

    def open(self, function_context):
        mg = function_context.get_metric_group()
        self.counter = mg.add_group("key", "value").counter("my_counter")
        self.counter_sum = 0

    def eval(self, x, y):
        self.counter.inc(y)
        self.counter_sum += y
        for i in range(y):
            yield x, i


@udtf(result_types=[DataTypes.BIGINT()])
def identity(x):
    if x is not None:
        from pyflink.common import Row
        return Row(x)


# test specify the input_types
@udtf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()],
      result_types=DataTypes.BIGINT())
def condition_multi_emit(x, y):
    if x == 3:
        return range(y, x)


class MultiNum(ScalarFunction):
    def eval(self, x):
        return x * 2


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
