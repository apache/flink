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
from pandas.util.testing import assert_frame_equal

from pyflink.common import Row
from pyflink.table import expressions as expr
from pyflink.table.types import DataTypes
from pyflink.table.udf import udf, udtf, udaf, AggregateFunction
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
    def test_aggregate_with_pandas_udaf(self):
        t = self.t_env.from_elements(
            [(1, 2, 3), (2, 1, 3), (1, 5, 4), (1, 8, 6), (2, 3, 4)],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b", DataTypes.SMALLINT()),
                 DataTypes.FIELD("c", DataTypes.INT())]))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c'],
            [DataTypes.TINYINT(), DataTypes.FLOAT(), DataTypes.INT()])
        self.t_env.register_table_sink("Results", table_sink)
        pandas_udaf = udaf(lambda a: (a.mean(), a.max()),
                           result_type=DataTypes.ROW(
                               [DataTypes.FIELD("a", DataTypes.FLOAT()),
                                DataTypes.FIELD("b", DataTypes.INT())]),
                           func_type="pandas")
        t.group_by(t.a) \
            .aggregate(pandas_udaf(t.b).alias("c", "d")) \
            .select("a, c, d").execute_insert("Results") \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["1,5.0,8", "2,2.0,3"])

    def test_aggregate_with_pandas_udaf_without_keys(self):
        t = self.t_env.from_elements(
            [(1, 2, 3), (2, 1, 3), (1, 5, 4), (1, 8, 6), (2, 3, 4)],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b", DataTypes.SMALLINT()),
                 DataTypes.FIELD("c", DataTypes.INT())]))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'],
            [DataTypes.FLOAT(), DataTypes.INT()])
        self.t_env.register_table_sink("Results", table_sink)
        pandas_udaf = udaf(lambda a: Row(a.mean(), a.max()),
                           result_type=DataTypes.ROW(
                               [DataTypes.FIELD("a", DataTypes.FLOAT()),
                                DataTypes.FIELD("b", DataTypes.INT())]),
                           func_type="pandas")
        t.aggregate(pandas_udaf(t.b).alias("c", "d")) \
            .select("c, d").execute_insert("Results") \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["3.8,8"])

    def test_window_aggregate_with_pandas_udaf(self):
        import datetime
        from pyflink.table.window import Tumble
        t = self.t_env.from_elements(
            [
                (1, 2, 3, datetime.datetime(2018, 3, 11, 3, 10, 0, 0)),
                (3, 2, 4, datetime.datetime(2018, 3, 11, 3, 10, 0, 0)),
                (2, 1, 2, datetime.datetime(2018, 3, 11, 3, 10, 0, 0)),
                (1, 3, 1, datetime.datetime(2018, 3, 11, 3, 40, 0, 0)),
                (1, 8, 5, datetime.datetime(2018, 3, 11, 4, 20, 0, 0)),
                (2, 3, 6, datetime.datetime(2018, 3, 11, 3, 30, 0, 0))
            ],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b", DataTypes.SMALLINT()),
                 DataTypes.FIELD("c", DataTypes.INT()),
                 DataTypes.FIELD("rowtime", DataTypes.TIMESTAMP(3))]))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c'],
            [
                DataTypes.TIMESTAMP(3),
                DataTypes.FLOAT(),
                DataTypes.INT()
            ])
        self.t_env.register_table_sink("Results", table_sink)
        pandas_udaf = udaf(lambda a: (a.mean(), a.max()),
                           result_type=DataTypes.ROW(
                               [DataTypes.FIELD("a", DataTypes.FLOAT()),
                                DataTypes.FIELD("b", DataTypes.INT())]),
                           func_type="pandas")
        tumble_window = Tumble.over(expr.lit(1).hours) \
            .on(expr.col("rowtime")) \
            .alias("w")
        t.window(tumble_window) \
            .group_by("w") \
            .aggregate(pandas_udaf(t.b).alias("d", "e")) \
            .select("w.rowtime, d, e") \
            .execute_insert("Results") \
            .wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual,
                           ["2018-03-11 03:59:59.999,2.2,3",
                            "2018-03-11 04:59:59.999,8.0,8"])


class StreamRowBasedOperationITTests(RowBasedOperationTests, PyFlinkBlinkStreamTableTestCase):
    def test_aggregate(self):
        import pandas as pd
        t = self.t_env.from_elements(
            [(1, 2, 3), (2, 1, 3), (1, 5, 4), (1, 8, 6), (2, 3, 4)],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.BIGINT()),
                 DataTypes.FIELD("b", DataTypes.SMALLINT()),
                 DataTypes.FIELD("c", DataTypes.INT())]))

        function = CountAndSumAggregateFunction()
        agg = udaf(function,
                   result_type=function.get_result_type(),
                   accumulator_type=function.get_accumulator_type(),
                   name=str(function.__class__.__name__))
        result = t.group_by(t.a) \
            .aggregate(agg(t.b).alias("c", "d")) \
            .select("a, c, d") \
            .to_pandas()
        assert_frame_equal(result, pd.DataFrame([[1, 3, 15], [2, 2, 4]], columns=['a', 'c', 'd']))


class CountAndSumAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        from pyflink.common import Row
        return Row(accumulator[0], accumulator[1])

    def create_accumulator(self):
        from pyflink.common import Row
        return Row(0, 0)

    def accumulate(self, accumulator, *args):
        accumulator[0] += 1
        accumulator[1] += args[0]

    def retract(self, accumulator, *args):
        accumulator[0] -= 1
        accumulator[1] -= args[0]

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            accumulator[0] += other_acc[0]
            accumulator[1] += other_acc[1]

    def get_accumulator_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT()),
             DataTypes.FIELD("b", DataTypes.BIGINT())])

    def get_result_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT()),
             DataTypes.FIELD("b", DataTypes.BIGINT())])


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
