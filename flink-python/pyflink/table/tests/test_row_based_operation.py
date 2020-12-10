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

from pyflink.common import Row
from pyflink.table.types import DataTypes
from pyflink.table.udf import udf, udtf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkBlinkBatchTableTestCase, \
    PyFlinkBlinkStreamTableTestCase


class RowBasedOperationTests(object):
    def test_map(self):
        t = self.t_env.from_elements(
            [(1, 2, 3), (2, 1, 3), (1, 5, 4), (1, 8, 6), (2, 3, 4)],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b", DataTypes.SMALLINT()),
                 DataTypes.FIELD("c", DataTypes.INT())]))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'],
            [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        func = udf(lambda x: Row(x + 1, x * x), result_type=DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT()),
             DataTypes.FIELD("b", DataTypes.BIGINT())]))

        t.map(func(t.b)).alias("a", "b") \
            .map(func(t.a)).alias("a", "b") \
            .execute_insert("Results") \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["4,9", "3,4", "7,36", "10,81", "5,16"])

    def test_map_with_pandas_udf(self):
        t = self.t_env.from_elements(
            [(1, Row(2, 3)), (2, Row(1, 3)), (1, Row(5, 4)), (1, Row(8, 6)), (2, Row(3, 4))],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b",
                                 DataTypes.ROW([DataTypes.FIELD("a", DataTypes.INT()),
                                                DataTypes.FIELD("b", DataTypes.INT())]))]))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'],
            [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        def func(x, y):
            import pandas as pd
            a = (x * 2).rename('b')
            res = pd.concat([a, x], axis=1) + y
            return res

        pandas_udf = udf(func,
                         result_type=DataTypes.ROW(
                             [DataTypes.FIELD("c", DataTypes.BIGINT()),
                              DataTypes.FIELD("d", DataTypes.BIGINT())]),
                         func_type='pandas')
        t.map(pandas_udf(t.a, t.b)).execute_insert("Results").wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["3,5", "3,7", "6,6", "9,8", "5,8"])

    def test_flat_map(self):
        t = self.t_env.from_elements(
            [(1, "2,3", 3), (2, "1", 3), (1, "5,6,7", 4)],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b", DataTypes.STRING()),
                 DataTypes.FIELD("c", DataTypes.INT())]))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'],
            [DataTypes.BIGINT(), DataTypes.STRING()])
        self.t_env.register_table_sink("Results", table_sink)

        @udtf(result_types=[DataTypes.INT(), DataTypes.STRING()])
        def split(x, string):
            for s in string.split(","):
                yield x, s

        t.flat_map(split(t.a, t.b)) \
            .alias("a, b") \
            .flat_map(split(t.a, t.b)) \
            .execute_insert("Results") \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["1,2", "1,3", "2,1", "1,5", "1,6", "1,7"])


class BatchRowBasedOperationITTests(RowBasedOperationTests, PyFlinkBlinkBatchTableTestCase):
    pass


class StreamRowBasedOperationITTests(RowBasedOperationTests, PyFlinkBlinkStreamTableTestCase):
    pass


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
