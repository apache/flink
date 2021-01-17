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
from pyflink.table import expressions as expr, ListView
from pyflink.table.types import DataTypes
from pyflink.table.udf import udf, udtf, udaf, AggregateFunction, TableAggregateFunction, udtaf
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
        self.assert_equals(
            actual, ["+I[4, 9]", "+I[3, 4]", "+I[7, 36]", "+I[10, 81]", "+I[5, 16]"])

    def test_map_with_pandas_udf(self):
        t = self.t_env.from_elements(
            [(1, Row(2, 3)), (2, Row(1, 3)), (1, Row(5, 4)), (1, Row(8, 6)), (2, Row(3, 4))],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b",
                                 DataTypes.ROW([DataTypes.FIELD("c", DataTypes.INT()),
                                                DataTypes.FIELD("d", DataTypes.INT())]))]))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'],
            [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        def func(x):
            import pandas as pd
            res = pd.concat([x.a, x.c + x.d], axis=1)
            return res

        def func2(x):
            return x * 2

        pandas_udf = udf(func,
                         result_type=DataTypes.ROW(
                             [DataTypes.FIELD("c", DataTypes.BIGINT()),
                              DataTypes.FIELD("d", DataTypes.BIGINT())]),
                         func_type='pandas')

        pandas_udf_2 = udf(func2,
                           result_type=DataTypes.ROW(
                               [DataTypes.FIELD("c", DataTypes.BIGINT()),
                                DataTypes.FIELD("d", DataTypes.BIGINT())]),
                           func_type='pandas')

        t.map(pandas_udf).map(pandas_udf_2).execute_insert("Results").wait()
        actual = source_sink_utils.results()
        self.assert_equals(
            actual,
            ["+I[4, 8]", "+I[2, 10]", "+I[2, 28]", "+I[2, 18]", "+I[4, 14]"])

    def test_flat_map(self):
        t = self.t_env.from_elements(
            [(1, "2,3"), (2, "1"), (1, "5,6,7")],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b", DataTypes.STRING())]))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c', 'd', 'e', 'f'],
            [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.BIGINT(),
             DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.STRING()])
        self.t_env.register_table_sink("Results", table_sink)

        @udtf(result_types=[DataTypes.INT(), DataTypes.STRING()])
        def split(x):
            for s in x[1].split(","):
                yield x[0], s

        t.flat_map(split) \
            .flat_map(split) \
            .join_lateral(split.alias("a", "b")) \
            .left_outer_join_lateral(split.alias("c", "d")) \
            .execute_insert("Results") \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(
            actual,
            ["+I[1, 2, 1, 2, 1, 2]", "+I[1, 3, 1, 3, 1, 3]", "+I[2, 1, 2, 1, 2, 1]",
             "+I[1, 5, 1, 5, 1, 5]", "+I[1, 6, 1, 6, 1, 6]", "+I[1, 7, 1, 7, 1, 7]"])


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
        pandas_udaf = udaf(lambda pd: (pd.b.mean(), pd.a.max()),
                           result_type=DataTypes.ROW(
                               [DataTypes.FIELD("a", DataTypes.FLOAT()),
                                DataTypes.FIELD("b", DataTypes.INT())]),
                           func_type="pandas")
        t.select(t.a, t.b) \
            .group_by(t.a) \
            .aggregate(pandas_udaf) \
            .select("*") \
            .execute_insert("Results") \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[1, 5.0, 1]", "+I[2, 2.0, 2]"])

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
        pandas_udaf = udaf(lambda pd: Row(pd.b.mean(), pd.b.max()),
                           result_type=DataTypes.ROW(
                               [DataTypes.FIELD("a", DataTypes.FLOAT()),
                                DataTypes.FIELD("b", DataTypes.INT())]),
                           func_type="pandas")
        t.select(t.b) \
            .aggregate(pandas_udaf.alias("a", "b")) \
            .select("a, b") \
            .execute_insert("Results") \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[3.8, 8]"])

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
        pandas_udaf = udaf(lambda pd: (pd.b.mean(), pd.b.max()),
                           result_type=DataTypes.ROW(
                               [DataTypes.FIELD("a", DataTypes.FLOAT()),
                                DataTypes.FIELD("b", DataTypes.INT())]),
                           func_type="pandas")
        tumble_window = Tumble.over(expr.lit(1).hours) \
            .on(expr.col("rowtime")) \
            .alias("w")
        t.select(t.b, t.rowtime) \
            .window(tumble_window) \
            .group_by("w") \
            .aggregate(pandas_udaf.alias("d", "e")) \
            .select("w.rowtime, d, e") \
            .execute_insert("Results") \
            .wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual,
                           ["+I[2018-03-11 03:59:59.999, 2.2, 3]",
                            "+I[2018-03-11 04:59:59.999, 8.0, 8]"])


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
            .aggregate(agg.alias("c", "d")) \
            .select("a, c, d") \
            .to_pandas()
        assert_frame_equal(result.sort_values('a').reset_index(drop=True),
                           pd.DataFrame([[1, 3, 15], [2, 2, 4]], columns=['a', 'c', 'd']))

    def test_flat_aggregate(self):
        import pandas as pd
        mytop = udtaf(Top2())
        t = self.t_env.from_elements([(1, 'Hi', 'Hello'),
                                      (3, 'Hi', 'hi'),
                                      (5, 'Hi2', 'hi'),
                                      (7, 'Hi', 'Hello'),
                                      (2, 'Hi', 'Hello')], ['a', 'b', 'c'])
        result = t.select(t.a, t.c) \
            .group_by(t.c) \
            .flat_aggregate(mytop) \
            .select(t.a) \
            .flat_aggregate(mytop.alias("b")) \
            .select("b") \
            .to_pandas()

        assert_frame_equal(result, pd.DataFrame([[7], [5]], columns=['b']))

    def test_flat_aggregate_list_view(self):
        import pandas as pd
        my_concat = udtaf(ListViewConcatTableAggregateFunction())
        self.t_env.get_config().get_configuration().set_string(
            "python.fn-execution.bundle.size", "2")
        # trigger the cache eviction in a bundle.
        self.t_env.get_config().get_configuration().set_string(
            "python.state.cache-size", "2")
        t = self.t_env.from_elements([(1, 'Hi', 'Hello'),
                                      (3, 'Hi', 'hi'),
                                      (3, 'Hi2', 'hi'),
                                      (3, 'Hi', 'hi'),
                                      (2, 'Hi', 'Hello'),
                                      (1, 'Hi2', 'Hello'),
                                      (3, 'Hi3', 'hi'),
                                      (3, 'Hi2', 'Hello'),
                                      (3, 'Hi3', 'hi'),
                                      (2, 'Hi3', 'Hello')], ['a', 'b', 'c'])
        result = t.group_by(t.c) \
            .flat_aggregate(my_concat(t.b, ',').alias("b")) \
            .select(t.b, t.c) \
            .alias("a, c")
        assert_frame_equal(result.to_pandas().sort_values('c').reset_index(drop=True),
                           pd.DataFrame([["Hi,Hi,Hi2,Hi2,Hi3", "Hello"],
                                         ["Hi,Hi,Hi2,Hi2,Hi3", "Hello"],
                                         ["Hi,Hi2,Hi,Hi3,Hi3", "hi"],
                                         ["Hi,Hi2,Hi,Hi3,Hi3", "hi"]],
                                        columns=['a', 'c']))


class CountAndSumAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        from pyflink.common import Row
        return Row(accumulator[0], accumulator[1])

    def create_accumulator(self):
        from pyflink.common import Row
        return Row(0, 0)

    def accumulate(self, accumulator, *args):
        accumulator[0] += 1
        accumulator[1] += args[0][1]

    def retract(self, accumulator, *args):
        accumulator[0] -= 1
        accumulator[1] -= args[0][1]

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


class Top2(TableAggregateFunction):

    def emit_value(self, accumulator):
        yield Row(accumulator[0])
        yield Row(accumulator[1])

    def create_accumulator(self):
        return [None, None]

    def accumulate(self, accumulator, *args):
        if args[0][0] is not None:
            if accumulator[0] is None or args[0][0] > accumulator[0]:
                accumulator[1] = accumulator[0]
                accumulator[0] = args[0][0]
            elif accumulator[1] is None or args[0][0] > accumulator[1]:
                accumulator[1] = args[0][0]

    def retract(self, accumulator, *args):
        accumulator[0] = accumulator[0] - 1

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            self.accumulate(accumulator, other_acc[0])
            self.accumulate(accumulator, other_acc[1])

    def get_accumulator_type(self):
        return DataTypes.ARRAY(DataTypes.BIGINT())

    def get_result_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT())])


class ListViewConcatTableAggregateFunction(TableAggregateFunction):

    def emit_value(self, accumulator):
        result = accumulator[1].join(accumulator[0])
        yield Row(result)
        yield Row(result)

    def create_accumulator(self):
        return Row(ListView(), '')

    def accumulate(self, accumulator, *args):
        accumulator[1] = args[1]
        accumulator[0].add(args[0])

    def retract(self, accumulator, *args):
        raise NotImplementedError

    def get_accumulator_type(self):
        return DataTypes.ROW([
            DataTypes.FIELD("f0", DataTypes.LIST_VIEW(DataTypes.STRING())),
            DataTypes.FIELD("f1", DataTypes.BIGINT())])

    def get_result_type(self):
        return DataTypes.ROW([DataTypes.FIELD("a", DataTypes.STRING())])


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
