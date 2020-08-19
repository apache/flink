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
    PyFlinkBlinkStreamTableTestCase, PyFlinkBatchTableTestCase, PyFlinkBlinkBatchTableTestCase,\
    exec_insert_table


class UserDefinedTableFunctionTests(object):

    def test_table_function(self):
        self._register_table_sink(
            ['a', 'b', 'c'],
            [DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()])

        self.t_env.register_function(
            "multi_emit", udtf(MultiEmit(), result_types=[DataTypes.BIGINT(), DataTypes.BIGINT()]))

        self.t_env.register_function("condition_multi_emit", condition_multi_emit)

        self.t_env.register_function(
            "multi_num", udf(MultiNum(), result_type=DataTypes.BIGINT()))

        t = self.t_env.from_elements([(1, 1, 3), (2, 1, 6), (3, 2, 9)], ['a', 'b', 'c'])
        t = t.join_lateral("multi_emit(a, multi_num(b)) as (x, y)") \
            .left_outer_join_lateral("condition_multi_emit(x, y) as m") \
            .select("x, y, m")
        actual = self._get_output(t)
        self.assert_equals(actual,
                           ["1,0,null", "1,1,null", "2,0,null", "2,1,null", "3,0,0", "3,0,1",
                            "3,0,2", "3,1,1", "3,1,2", "3,2,2", "3,3,null"])

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
        self.assert_equals(actual, ["1,1,0", "2,2,0", "3,3,0", "3,3,1"])

    def _register_table_sink(self, field_names: list, field_types: list):
        table_sink = source_sink_utils.TestAppendSink(field_names, field_types)
        self.t_env.register_table_sink("Results", table_sink)

    def _get_output(self, t):
        exec_insert_table(t, "Results")
        return source_sink_utils.results()


class PyFlinkStreamUserDefinedTableFunctionTests(UserDefinedTableFunctionTests,
                                                 PyFlinkStreamTableTestCase):
    pass


class PyFlinkBlinkStreamUserDefinedFunctionTests(UserDefinedTableFunctionTests,
                                                 PyFlinkBlinkStreamTableTestCase):
    pass


class PyFlinkBlinkBatchUserDefinedFunctionTests(UserDefinedTableFunctionTests,
                                                PyFlinkBlinkBatchTableTestCase):
    pass


class PyFlinkBatchUserDefinedTableFunctionTests(UserDefinedTableFunctionTests,
                                                PyFlinkBatchTableTestCase):
    def _register_table_sink(self, field_names: list, field_types: list):
        pass

    def _get_output(self, t):
        return self.collect(t)


class MultiEmit(TableFunction, unittest.TestCase):

    def open(self, function_context):
        mg = function_context.get_metric_group()
        self.counter = mg.add_group("key", "value").counter("my_counter")
        self.counter_sum = 0

    def eval(self, x, y):
        self.counter.inc(y)
        self.counter_sum += y
        self.assertEqual(self.counter_sum, self.counter.get_count())
        for i in range(y):
            yield x, i


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
