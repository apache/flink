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

from pandas.testing import assert_frame_equal

from pyflink.common import Row
from pyflink.table import expressions as expr, ListView
from pyflink.table.types import DataTypes
from pyflink.table.udf import udf, udtf, udaf, AggregateFunction, TableAggregateFunction, udtaf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkBatchTableTestCase, \
    PyFlinkStreamTableTestCase


class RowBasedOperationTests(object):
    def test_map(self):
        t = self.t_env.from_elements(
            [(1, 2, 3), (2, 1, 3), (1, 5, 4), (1, 8, 6), (2, 3, 4)],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b", DataTypes.SMALLINT()),
                 DataTypes.FIELD("c", DataTypes.INT())]))

        sink_table_ddl = """
        CREATE TABLE Results_test_map(a BIGINT, b BIGINT) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)

        func = udf(lambda x: Row(a=x + 1, b=x * x), result_type=DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT()),
             DataTypes.FIELD("b", DataTypes.BIGINT())]))

        func2 = udf(lambda x: Row(x.a + 1, x.b * 2), result_type=DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT()),
             DataTypes.FIELD("b", DataTypes.BIGINT())]))

        t.map(func(t.b)).alias("a", "b") \
            .map(func(t.a)) \
            .map(func2) \
            .execute_insert("Results_test_map") \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(
            actual, ["+I[5, 18]", "+I[4, 8]", "+I[8, 72]", "+I[11, 162]", "+I[6, 32]"])

    def test_map_with_pandas_udf(self):
        t = self.t_env.from_elements(
            [(1, Row(2, 3)), (2, Row(1, 3)), (1, Row(5, 4)), (1, Row(8, 6)), (2, Row(3, 4))],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b",
                                 DataTypes.ROW([DataTypes.FIELD("c", DataTypes.INT()),
                                                DataTypes.FIELD("d", DataTypes.INT())]))]))

        sink_table_ddl = """
            CREATE TABLE Results_test_map_with_pandas_udf(
                a BIGINT,
                b BIGINT
            ) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)

        def func(x):
            import pandas as pd
            res = pd.concat([x.a, x.c + x.d], axis=1)
            return res

        def func2(x):
            return x * 2

        def func3(x):
            assert isinstance(x, Row)
            return x

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

        general_udf = udf(func3,
                          result_type=DataTypes.ROW(
                              [DataTypes.FIELD("c", DataTypes.BIGINT()),
                               DataTypes.FIELD("d", DataTypes.BIGINT())]))

        t.map(pandas_udf).map(pandas_udf_2).map(general_udf).execute_insert(
            "Results_test_map_with_pandas_udf").wait()
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

        sink_table_ddl = """
            CREATE TABLE Results_test_flat_map(
                a BIGINT, b STRING, c BIGINT, d STRING, e BIGINT, f STRING
            ) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)

        @udtf(result_types=[DataTypes.INT(), DataTypes.STRING()])
        def split(x):
            for s in x.b.split(","):
                yield x.a, s

        t.flat_map(split).alias("a", "b") \
            .flat_map(split).alias("a", "b") \
            .join_lateral(split.alias("c", "d")) \
            .left_outer_join_lateral(split.alias("e", "f")) \
            .execute_insert("Results_test_flat_map") \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(
            actual,
            ["+I[1, 2, 1, 2, 1, 2]", "+I[1, 3, 1, 3, 1, 3]", "+I[2, 1, 2, 1, 2, 1]",
             "+I[1, 5, 1, 5, 1, 5]", "+I[1, 6, 1, 6, 1, 6]", "+I[1, 7, 1, 7, 1, 7]"])


class BatchRowBasedOperationITTests(RowBasedOperationTests, PyFlinkBatchTableTestCase):
    def test_aggregate_with_pandas_udaf(self):
        t = self.t_env.from_elements(
            [(1, 2, 3), (2, 1, 3), (1, 5, 4), (1, 8, 6), (2, 3, 4)],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b", DataTypes.SMALLINT()),
                 DataTypes.FIELD("c", DataTypes.INT())]))

        sink_table_ddl = """
            CREATE TABLE Results_test_aggregate_with_pandas_udaf(
                a TINYINT,
                b FLOAT,
                c INT
            ) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        pandas_udaf = udaf(lambda pd: (pd.b.mean(), pd.a.max()),
                           result_type=DataTypes.ROW(
                               [DataTypes.FIELD("a", DataTypes.FLOAT()),
                                DataTypes.FIELD("b", DataTypes.INT())]),
                           func_type="pandas")
        t.select(t.a, t.b) \
            .group_by(t.a) \
            .aggregate(pandas_udaf) \
            .select(expr.col("*")) \
            .execute_insert("Results_test_aggregate_with_pandas_udaf") \
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

        sink_table_ddl = """
            CREATE TABLE Results_test_aggregate_with_pandas_udaf_without_keys(
                a FLOAT,
                b INT
            ) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        pandas_udaf = udaf(lambda pd: Row(pd.b.mean(), pd.b.max()),
                           result_type=DataTypes.ROW(
                               [DataTypes.FIELD("a", DataTypes.FLOAT()),
                                DataTypes.FIELD("b", DataTypes.INT())]),
                           func_type="pandas")
        t.select(t.b) \
            .aggregate(pandas_udaf.alias("a", "b")) \
            .select(t.a, t.b) \
            .execute_insert("Results_test_aggregate_with_pandas_udaf_without_keys") \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[3.8, 8]"])

    @unittest.skip("Not supported yet")
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

        sink_table_ddl = """
            CREATE TABLE Results_test_window_aggregate_with_pandas_udaf(
                a TIMESTAMP(3),
                b FLOAT,
                c INT
            ) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        print(t.get_schema())
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
            .group_by(expr.col("w")) \
            .aggregate(pandas_udaf.alias("d", "e")) \
            .select(expr.col("w").rowtime, expr.col("d"), expr.col("e")) \
            .execute_insert("Results_test_window_aggregate_with_pandas_udaf") \
            .wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual,
                           ["+I[2018-03-11 03:59:59.999, 2.2, 3]",
                            "+I[2018-03-11 04:59:59.999, 8.0, 8]"])


class StreamRowBasedOperationITTests(RowBasedOperationTests, PyFlinkStreamTableTestCase):
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
            .select(t.a, t.c, expr.col("d")) \
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
            .flat_aggregate(mytop.alias('a')) \
            .select(t.a) \
            .flat_aggregate(mytop.alias("b")) \
            .select(t.b) \
            .to_pandas()

        assert_frame_equal(result, pd.DataFrame([[7], [5]], columns=['b']))

    def test_flat_aggregate_list_view(self):
        import pandas as pd
        my_concat = udtaf(ListViewConcatTableAggregateFunction())
        self.t_env.get_config().set(
            "python.fn-execution.bundle.size", "2")
        # trigger the cache eviction in a bundle.
        self.t_env.get_config().set(
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
            .alias("a", "c")
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

    def accumulate(self, accumulator, row: Row):
        accumulator[0] += 1
        accumulator[1] += row.b

    def retract(self, accumulator, row: Row):
        accumulator[0] -= 1
        accumulator[1] -= row.a

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
        accumulator.sort()
        accumulator.reverse()
        size = len(accumulator)
        if size > 1:
            yield accumulator[0]
        if size > 2:
            yield accumulator[1]

    def create_accumulator(self):
        return []

    def accumulate(self, accumulator, row: Row):
        accumulator.append(row.a)

    def retract(self, accumulator, row: Row):
        accumulator.remove(row.a)

    def get_accumulator_type(self):
        return DataTypes.ARRAY(DataTypes.BIGINT())

    def get_result_type(self):
        return DataTypes.BIGINT()


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
