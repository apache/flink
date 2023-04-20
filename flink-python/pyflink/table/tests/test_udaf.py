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
import collections
import datetime
import uuid
from decimal import Decimal

import pandas as pd
from pandas.testing import assert_frame_equal

from pyflink.common import Row, RowKind
from pyflink.fn_execution.state_impl import RemovableConcatIterator
from pyflink.table import DataTypes
from pyflink.table.data_view import ListView, MapView
from pyflink.table.expressions import col, call, lit, row_interval
from pyflink.table.udf import AggregateFunction, udaf
from pyflink.table.window import Tumble, Slide, Session
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase


def generate_random_table_name():
    return "Table{0}".format(str(uuid.uuid1()).replace("-", "_"))


class CountAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        return [0]

    def accumulate(self, accumulator, *args):
        accumulator[0] = accumulator[0] + 1

    def retract(self, accumulator, *args):
        accumulator[0] = accumulator[0] - 1

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            accumulator[0] = accumulator[0] + other_acc[0]

    def get_accumulator_type(self):
        return 'ARRAY<BIGINT>'

    def get_result_type(self):
        return 'BIGINT'


class SumAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        return [0]

    def accumulate(self, accumulator, *args):
        accumulator[0] = accumulator[0] + args[0]

    def retract(self, accumulator, *args):
        accumulator[0] = accumulator[0] - args[0]

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            accumulator[0] = accumulator[0] + other_acc[0]

    def get_accumulator_type(self):
        return 'ARRAY<BIGINT>'

    def get_result_type(self):
        return 'BIGINT'


class ConcatAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        str_list = [i for i in accumulator[0]]
        str_list.sort()
        return accumulator[1].join(str_list)

    def create_accumulator(self):
        return Row([], '')

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            accumulator[1] = args[1]
            accumulator[0].append(args[0])

    def retract(self, accumulator, *args):
        if args[0] is not None:
            accumulator[0].remove(args[0])

    def get_accumulator_type(self):
        return 'ROW<f0 STRING, f1 BIGINT>'

    def get_result_type(self):
        return 'STRING'


class ListViewConcatAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[1].join(accumulator[0])

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
        return DataTypes.STRING()


class CountDistinctAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[1]

    def create_accumulator(self):
        return Row(MapView(), 0)

    def accumulate(self, accumulator, *args):
        input_str = args[0]
        if accumulator[0].is_empty() or input_str not in accumulator[0] \
                or accumulator[0][input_str] is None:
            accumulator[0][input_str] = 1
            accumulator[1] += 1
        else:
            accumulator[0][input_str] += 1
        if input_str == "clear":
            accumulator[0].clear()
            accumulator[1] = 0

    def retract(self, accumulator, *args):
        input_str = args[0]
        if accumulator[0].is_empty() or input_str not in accumulator[0]:
            return
        accumulator[0].put_all({input_str: accumulator[0][input_str] - 1})
        if accumulator[0][input_str] <= 0:
            accumulator[1] -= 1
            accumulator[0][input_str] = None

    def get_accumulator_type(self):
        return 'ROW<f0 MAP<STRING, STRING>, f1 BIGINT>'

    def get_result_type(self):
        return 'BIGINT'


class CustomIterateAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        # test iterate keys
        key_set = [i for i in accumulator[0]]
        key_set.sort()
        # test iterate values
        value_set = [str(i) for i in accumulator[0].values()]
        value_set.sort()
        item_set = {}
        # test iterate items
        for key, value in accumulator[0].items():
            item_set[key] = value
        ordered_item_set = collections.OrderedDict()
        for key in key_set:
            ordered_item_set[key] = str(item_set[key])
        try:
            # test auto clear the cached iterators
            next(iter(accumulator[0].items()))
        except StopIteration:
            pass
        return Row(",".join(key_set),
                   ','.join(value_set),
                   ",".join([":".join(item) for item in ordered_item_set.items()]),
                   accumulator[1])

    def create_accumulator(self):
        return Row(MapView(), 0)

    def accumulate(self, accumulator, *args):
        input_str = args[0]
        if input_str not in accumulator[0]:
            accumulator[0][input_str] = 1
            accumulator[1] += 1
        else:
            accumulator[0][input_str] += 1

    def retract(self, accumulator, *args):
        input_str = args[0]
        if input_str not in accumulator[0]:
            return
        accumulator[0][input_str] -= 1
        if accumulator[0][input_str] == 0:
            # test removable iterator
            key_iter = iter(accumulator[0].keys())  # type: RemovableConcatIterator
            while True:
                try:
                    key = next(key_iter)
                    if key == input_str:
                        key_iter.remove()
                except StopIteration:
                    break
            accumulator[1] -= 1

    def get_accumulator_type(self):
        return DataTypes.ROW([
            DataTypes.FIELD("f0", DataTypes.MAP_VIEW(DataTypes.STRING(), DataTypes.BIGINT())),
            DataTypes.FIELD("f1", DataTypes.BIGINT())])

    def get_result_type(self):
        return DataTypes.ROW([
            DataTypes.FIELD("f0", DataTypes.STRING()),
            DataTypes.FIELD("f1", DataTypes.STRING()),
            DataTypes.FIELD("f2", DataTypes.STRING()),
            DataTypes.FIELD("f3", DataTypes.BIGINT())])


class StreamTableAggregateTests(PyFlinkStreamTableTestCase):

    @classmethod
    def setUpClass(cls):
        super(StreamTableAggregateTests, cls).setUpClass()
        cls.t_env.create_temporary_system_function("my_count", CountAggregateFunction())
        cls.t_env.create_temporary_function("my_sum", SumAggregateFunction())
        cls.t_env.create_temporary_system_function("concat", ConcatAggregateFunction())
        cls.t_env.create_temporary_system_function("my_count_distinct",
                                                   CountDistinctAggregateFunction())

    def test_double_aggregate(self):
        # trigger the finish bundle more frequently to ensure testing the communication
        # between RemoteKeyedStateBackend and the StateGrpcService.
        self.t_env.get_config().set(
            "python.fn-execution.bundle.size", "2")
        # trigger the cache eviction in a bundle.
        self.t_env.get_config().set(
            "python.state.cache-size", "1")
        t = self.t_env.from_elements([(1, 'Hi', 'Hello'),
                                      (3, 'Hi', 'hi'),
                                      (3, 'Hi2', 'hi'),
                                      (3, 'Hi', 'hi2'),
                                      (2, 'Hi', 'Hello')], ['a', 'b', 'c'])
        result = t.group_by(t.c) \
            .select(call("my_count", t.a).alias("a"),
                    call("my_sum", t.a).alias("b"), t.c) \
            .select(call("my_count", col("a")).alias("a"),
                    call("my_sum", col("b")).alias("b"),
                    call("sum0", col("b")).alias("c"),
                    call("sum0", col("b").cast(DataTypes.DOUBLE())).alias("d"))
        assert_frame_equal(result.to_pandas(),
                           pd.DataFrame([[3, 12, 12, 12.0]], columns=['a', 'b', 'c', 'd']))

    def test_mixed_with_built_in_functions_with_retract(self):
        self.t_env.get_config().set("parallelism.default", "1")
        t = self.t_env.from_elements(
            [(1, 'Hi_', 1),
             (1, 'Hi', 2),
             (2, 'Hi_', 3),
             (2, 'Hi', 4),
             (3, None, None),
             (3, None, None),
             (4, 'hello2_', 7),
             (4, 'hello2', 8),
             (5, 'hello_', 9),
             (5, 'hello', 10)], ['a', 'b', 'c'])
        self.t_env.create_temporary_view(
            "test_mixed_with_built_in_functions_with_retract_source", t)
        table_with_retract_message = self.t_env.sql_query(
            "select a, LAST_VALUE(b) as b, LAST_VALUE(c) as c from "
            "test_mixed_with_built_in_functions_with_retract_source group by a")
        self.t_env.create_temporary_view(
            "test_mixed_with_built_in_functions_with_retract_retract_table",
            table_with_retract_message)
        result_table = self.t_env.sql_query(
            "select concat(b, ',') as a, "
            "FIRST_VALUE(b) as b, "
            "LAST_VALUE(b) as c, "
            "COUNT(c) as d, "
            "COUNT(1) as e, "
            "LISTAGG(b) as f,"
            "LISTAGG(b, '|') as g,"
            "MAX(c) as h,"
            "MAX(cast(c as float) + 1) as i,"
            "MIN(c) as j,"
            "MIN(cast(c as decimal) + 1) as k,"
            "SUM(c) as l,"
            "SUM(cast(c as float) + 1) as m,"
            "AVG(c) as n,"
            "AVG(cast(c as double) + 1) as o,"
            "STDDEV_POP(cast(c as float)),"
            "STDDEV_SAMP(cast(c as float)),"
            "VAR_POP(cast(c as float)),"
            "VAR_SAMP(cast(c as float))"
            " from test_mixed_with_built_in_functions_with_retract_retract_table")
        result = [i for i in result_table.execute().collect()]
        expected = Row('Hi,Hi,hello,hello2', 'Hi', 'hello', 4, 5, 'Hi,Hi,hello2,hello',
                       'Hi|Hi|hello2|hello', 10, 11.0, 2, Decimal(3.0), 24, 28.0, 6, 7.0,
                       3.1622777, 3.6514838, 10.0, 13.333333)
        expected.set_row_kind(RowKind.UPDATE_AFTER)
        self.assertEqual(result[len(result) - 1], expected)

    def test_mixed_with_built_in_functions_without_retract(self):
        self.t_env.get_config().set("parallelism.default", "1")
        t = self.t_env.from_elements(
            [('Hi', 2),
             ('Hi', 4),
             (None, None),
             ('hello2', 8),
             ('hello', 10)], ['b', 'c'])
        self.t_env.create_temporary_view(
            "test_mixed_with_built_in_functions_without_retract_source", t)
        result_table = self.t_env.sql_query(
            "select concat(b, ',') as a, "
            "FIRST_VALUE(b) as b, "
            "LAST_VALUE(b) as c, "
            "COUNT(c) as d, "
            "COUNT(1) as e, "
            "LISTAGG(b) as f,"
            "LISTAGG(b, '|') as g,"
            "MAX(c) as h,"
            "MAX(cast(c as float) + 1) as i,"
            "MIN(c) as j,"
            "MIN(cast(c as decimal) + 1) as k,"
            "SUM(c) as l,"
            "SUM(cast(c as float) + 1) as m "
            "from test_mixed_with_built_in_functions_without_retract_source")
        result = [i for i in result_table.execute().collect()]
        expected = Row('Hi,Hi,hello,hello2', 'Hi', 'hello', 4, 5, 'Hi,Hi,hello2,hello',
                       'Hi|Hi|hello2|hello', 10, 11.0, 2, Decimal(3.0), 24, 28.0)
        expected.set_row_kind(RowKind.UPDATE_AFTER)
        self.assertEqual(result[len(result) - 1], expected)

    def test_using_decorator(self):
        my_count = udaf(CountAggregateFunction(),
                        accumulator_type=DataTypes.ARRAY(DataTypes.INT()),
                        result_type=DataTypes.INT())
        t = self.t_env.from_elements([(1, 'Hi', 'Hello')], ['a', 'b', 'c'])
        result = t.group_by(t.c) \
            .select(my_count(t.a).alias("a"), t.c.alias("b"))
        plan = result.explain()
        result_type = result.get_schema().get_field_data_type(0)
        self.assertTrue(plan.find("PythonGroupAggregate(groupBy=[c], ") >= 0)
        self.assertEqual(result_type, DataTypes.INT())

    def test_list_view(self):
        my_concat = udaf(ListViewConcatAggregateFunction())
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
        result = t.group_by(t.c).select(my_concat(t.b, ',').alias("a"), t.c)
        assert_frame_equal(result.to_pandas().sort_values('c').reset_index(drop=True),
                           pd.DataFrame([["Hi,Hi,Hi2,Hi2,Hi3", "Hello"],
                                         ["Hi,Hi2,Hi,Hi3,Hi3", "hi"]], columns=['a', 'c']))

    def test_map_view(self):
        my_count = udaf(CountDistinctAggregateFunction())
        self.t_env.get_config().set_idle_state_retention(datetime.timedelta(days=1))
        self.t_env.get_config().set(
            "python.fn-execution.bundle.size", "2")
        # trigger the cache eviction in a bundle.
        self.t_env.get_config().set(
            "python.state.cache-size", "1")
        self.t_env.get_config().set(
            "python.map-state.read-cache-size", "1")
        self.t_env.get_config().set(
            "python.map-state.write-cache-size", "1")
        t = self.t_env.from_elements(
            [(1, 'Hi_', 'hi'),
             (1, 'Hi', 'hi'),
             (2, 'hello', 'hello'),
             (3, 'Hi_', 'hi'),
             (3, 'Hi', 'hi'),
             (4, 'hello', 'hello'),
             (5, 'Hi2_', 'hi'),
             (5, 'Hi2', 'hi'),
             (6, 'hello2', 'hello'),
             (7, 'Hi', 'hi'),
             (8, 'hello', 'hello'),
             (9, 'Hi2', 'hi'),
             (13, 'Hi3', 'hi')], ['a', 'b', 'c'])
        self.t_env.create_temporary_view("test_map_view_source", t)
        table_with_retract_message = self.t_env.sql_query(
            "select LAST_VALUE(b) as b, LAST_VALUE(c) as c from test_map_view_source group by a")
        result = table_with_retract_message.group_by(t.c).select(my_count(t.b).alias("a"), t.c)
        assert_frame_equal(result.to_pandas().sort_values('c').reset_index(drop=True),
                           pd.DataFrame([[2, "hello"],
                                         [3, "hi"]], columns=['a', 'c']))

    def test_data_view_clear(self):
        my_count = udaf(CountDistinctAggregateFunction())
        self.t_env.get_config().set_idle_state_retention(datetime.timedelta(days=1))
        self.t_env.get_config().set(
            "python.fn-execution.bundle.size", "2")
        # trigger the cache eviction in a bundle.
        self.t_env.get_config().set(
            "python.state.cache-size", "1")
        t = self.t_env.from_elements(
            [(2, 'hello', 'hello'),
             (4, 'clear', 'hello'),
             (6, 'hello2', 'hello'),
             (8, 'hello', 'hello')], ['a', 'b', 'c'])
        result = t.group_by(t.c).select(my_count(t.b).alias("a"), t.c)
        assert_frame_equal(result.to_pandas(),
                           pd.DataFrame([[2, "hello"]], columns=['a', 'c']))

    def test_map_view_iterate(self):
        test_iterate = udaf(CustomIterateAggregateFunction())
        self.t_env.get_config().set_idle_state_retention(datetime.timedelta(days=1))
        self.t_env.get_config().set(
            "python.fn-execution.bundle.size", "2")
        # trigger the cache eviction in a bundle.
        self.t_env.get_config().set(
            "python.state.cache-size", "2")
        self.t_env.get_config().set(
            "python.map-state.read-cache-size", "2")
        self.t_env.get_config().set(
            "python.map-state.write-cache-size", "2")
        self.t_env.get_config().set(
            "python.map-state.iterate-response-batch-size", "2")
        t = self.t_env.from_elements(
            [(1, 'Hi_', 'hi'),
             (1, 'Hi', 'hi'),
             (2, 'hello', 'hello'),
             (3, 'Hi_', 'hi'),
             (3, 'Hi', 'hi'),
             (4, 'hello', 'hello'),
             (5, 'Hi2_', 'hi'),
             (5, 'Hi2', 'hi'),
             (6, 'hello2', 'hello'),
             (7, 'Hi', 'hi'),
             (8, 'hello', 'hello'),
             (9, 'Hi2', 'hi'),
             (13, 'Hi3', 'hi')], ['a', 'b', 'c'])
        self.t_env.create_temporary_view("test_map_view_iterate_source", t)
        table_with_retract_message = self.t_env.sql_query(
            "select LAST_VALUE(b) as b, LAST_VALUE(c) as c from test_map_view_iterate_source "
            "group by a")
        result = table_with_retract_message.group_by(t.c) \
            .select(test_iterate(t.b).alias("a"), t.c) \
            .select(col("a").get(0).alias("a"),
                    col("a").get(1).alias("b"),
                    col("a").get(2).alias("c"),
                    col("a").get(3).alias("d"),
                    t.c.alias("e"))
        assert_frame_equal(
            result.to_pandas().sort_values('c').reset_index(drop=True),
            pd.DataFrame([
                ["Hi,Hi2,Hi3", "1,2,3", "Hi:3,Hi2:2,Hi3:1", 3, "hi"],
                ["hello,hello2", "1,3", 'hello:3,hello2:1', 2, "hello"]],
                columns=['a', 'b', 'c', 'd', 'e']))

    def test_distinct_and_filter(self):
        t = self.t_env.from_elements(
            [(1, 'Hi_', 'hi'),
             (1, 'Hi', 'hi'),
             (2, 'hello', 'hello'),
             (3, 'Hi_', 'hi'),
             (3, 'Hi', 'hi'),
             (4, 'hello', 'hello'),
             (5, 'Hi2_', 'hi'),
             (5, 'Hi2', 'hi'),
             (6, 'hello2', 'hello'),
             (7, 'Hi', 'hi'),
             (8, 'hello', 'hello'),
             (9, 'Hi2', 'hi'),
             (13, 'Hi3', 'hi')], ['a', 'b', 'c'])
        self.t_env.create_temporary_view("source", t)
        table_with_retract_message = self.t_env.sql_query(
            "select LAST_VALUE(b) as b, LAST_VALUE(c) as c from source group by a")
        self.t_env.create_temporary_view("retract_table", table_with_retract_message)
        result = self.t_env.sql_query(
            "select concat(distinct b, '.') as a, "
            "concat(distinct b, ',') filter (where c = 'hi') as b, "
            "concat(distinct b, ',') filter (where c = 'hello') as c, "
            "c as d "
            "from retract_table group by c")
        assert_frame_equal(result.to_pandas().sort_values(by='a').reset_index(drop=True),
                           pd.DataFrame([["Hi.Hi2.Hi3", "Hi,Hi2,Hi3", "", "hi"],
                                         ["hello.hello2", "", "hello,hello2", "hello"]],
                                        columns=['a', 'b', 'c', 'd']))

    def test_clean_state(self):
        self.t_env.get_config().set("parallelism.default", "1")
        self.t_env.get_config().set(
            "python.fn-execution.bundle.size", "1")
        self.t_env.get_config().set(
            "python.state.cache-size", "0")
        self.t_env.get_config().set(
            "table.exec.state.ttl", "2ms")

        source_table = generate_random_table_name()
        self.t_env.execute_sql(f"""
            CREATE TABLE {source_table}(
                a BIGINT
            ) WITH (
              'connector' = 'datagen',
              'number-of-rows' = '5',
              'rows-per-second' = '1'
            )
        """)

        sink_table = generate_random_table_name()
        self.t_env.execute_sql(f"""
        CREATE TABLE {sink_table}(
            a BIGINT
        ) WITH ('connector' = 'blackhole')
        """)
        t = self.t_env.from_path(source_table)
        t.select(call("my_count", t.a).alias("a")).execute_insert(sink_table).wait()

    def test_tumbling_group_window_over_time(self):
        # create source file path
        tmp_dir = self.tempdir
        data = [
            '1,1,2,2018-03-11 03:10:00',
            '3,3,2,2018-03-11 03:10:00',
            '2,2,1,2018-03-11 03:10:00',
            '2,2,1,2018-03-11 03:30:00',
            '1,1,3,2018-03-11 03:40:00',
            '1,1,8,2018-03-11 04:20:00',
        ]
        source_path = tmp_dir + '/test_tumbling_group_window_over_time.csv'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        source_table_name = generate_random_table_name()
        source_table = f"""
            create table {source_table_name}(
                a TINYINT,
                b SMALLINT,
                c INT,
                rowtime TIMESTAMP(3),
                WATERMARK FOR rowtime AS rowtime - INTERVAL '60' MINUTE
            ) with(
                'connector.type' = 'filesystem',
                'format.type' = 'csv',
                'connector.path' = '{source_path}',
                'format.ignore-first-line' = 'false',
                'format.field-delimiter' = ','
            )
        """
        self.t_env.execute_sql(source_table)
        t = self.t_env.from_path(source_table_name)

        from pyflink.testing import source_sink_utils
        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
        CREATE TABLE {sink_table}(a TINYINT, b TIMESTAMP(3), c TIMESTAMP(3), d BIGINT, e BIGINT)
        WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        t.window(Tumble.over(lit(1).hours).on(t.rowtime).alias("w")) \
            .group_by(t.a, col("w")) \
            .select(t.a,
                    col("w").start,
                    col("w").end,
                    t.c.count.alias("c"),
                    call("my_count_distinct", t.c).alias("d")) \
            .execute_insert(sink_table) \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual,
                           ["+I[2, 2018-03-11T03:00, 2018-03-11T04:00, 2, 1]",
                            "+I[3, 2018-03-11T03:00, 2018-03-11T04:00, 1, 1]",
                            "+I[1, 2018-03-11T03:00, 2018-03-11T04:00, 2, 2]",
                            "+I[1, 2018-03-11T04:00, 2018-03-11T05:00, 1, 1]"])

    def test_tumbling_group_window_over_count(self):
        self.t_env.get_config().set("parallelism.default", "1")
        # create source file path
        tmp_dir = self.tempdir
        data = [
            '1,1,2,2018-03-11 03:10:00',
            '3,3,2,2018-03-11 03:10:00',
            '2,2,1,2018-03-11 03:10:00',
            '1,1,3,2018-03-11 03:40:00',
            '1,1,8,2018-03-11 04:20:00',
            '2,2,3,2018-03-11 03:30:00',
            '3,3,3,2018-03-11 03:30:00'
        ]
        source_path = tmp_dir + '/test_tumbling_group_window_over_count.csv'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        source_table_name = generate_random_table_name()
        source_table = f"""
            create table {source_table_name}(
                a TINYINT,
                b SMALLINT,
                c SMALLINT,
                protime as PROCTIME()
            ) with(
                'connector.type' = 'filesystem',
                'format.type' = 'csv',
                'connector.path' = '{source_path}',
                'format.ignore-first-line' = 'false',
                'format.field-delimiter' = ','
            )
        """
        self.t_env.execute_sql(source_table)
        t = self.t_env.from_path(source_table_name)

        from pyflink.testing import source_sink_utils
        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
        CREATE TABLE {sink_table}(a TINYINT, d BIGINT)
        WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        t.window(Tumble.over(row_interval(2)).on(t.protime).alias("w")) \
            .group_by(t.a, col("w")) \
            .select(t.a, call("my_sum", t.c).alias("b")) \
            .execute_insert(sink_table) \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[1, 5]", "+I[2, 4]", "+I[3, 5]"])

    def test_sliding_group_window_over_time(self):
        # create source file path
        tmp_dir = self.tempdir
        data = [
            '1,1,2,2018-03-11 03:10:00',
            '3,3,2,2018-03-11 03:10:00',
            '2,2,1,2018-03-11 03:10:00',
            '2,2,1,2018-03-11 03:30:00',
            '1,1,3,2018-03-11 03:40:00',
            '1,1,8,2018-03-11 04:20:00',
        ]
        source_path = tmp_dir + '/test_sliding_group_window_over_time.csv'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        source_table_name = generate_random_table_name()
        source_table = f"""
            create table {source_table_name}(
                a TINYINT,
                b SMALLINT,
                c INT,
                rowtime TIMESTAMP(3),
                WATERMARK FOR rowtime AS rowtime - INTERVAL '60' MINUTE
            ) with(
                'connector.type' = 'filesystem',
                'format.type' = 'csv',
                'connector.path' = '{source_path}',
                'format.ignore-first-line' = 'false',
                'format.field-delimiter' = ','
            )
        """
        self.t_env.execute_sql(source_table)
        t = self.t_env.from_path(source_table_name)

        from pyflink.testing import source_sink_utils
        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
        CREATE TABLE {sink_table}(a TINYINT, b TIMESTAMP(3), c TIMESTAMP(3), d BIGINT)
        WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        t.window(Slide.over(lit(1).hours)
                 .every(lit(30).minutes)
                 .on(t.rowtime)
                 .alias("w")) \
            .group_by(t.a, col("w")) \
            .select(t.a, col("w").start, col("w").end, call("my_sum", t.c).alias("c")) \
            .execute_insert(sink_table) \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual,
                           ["+I[1, 2018-03-11T02:30, 2018-03-11T03:30, 2]",
                            "+I[2, 2018-03-11T02:30, 2018-03-11T03:30, 1]",
                            "+I[3, 2018-03-11T02:30, 2018-03-11T03:30, 2]",
                            "+I[1, 2018-03-11T03:00, 2018-03-11T04:00, 5]",
                            "+I[3, 2018-03-11T03:00, 2018-03-11T04:00, 2]",
                            "+I[2, 2018-03-11T03:00, 2018-03-11T04:00, 2]",
                            "+I[2, 2018-03-11T03:30, 2018-03-11T04:30, 1]",
                            "+I[1, 2018-03-11T03:30, 2018-03-11T04:30, 11]",
                            "+I[1, 2018-03-11T04:00, 2018-03-11T05:00, 8]"])

    def test_sliding_group_window_over_count(self):
        self.t_env.get_config().set("parallelism.default", "1")
        # create source file path
        tmp_dir = self.tempdir
        data = [
            '1,1,2,2018-03-11 03:10:00',
            '3,3,2,2018-03-11 03:10:00',
            '2,2,1,2018-03-11 03:10:00',
            '1,1,3,2018-03-11 03:40:00',
            '1,1,8,2018-03-11 04:20:00',
            '2,2,3,2018-03-11 03:30:00',
            '3,3,3,2018-03-11 03:30:00'
        ]
        source_path = tmp_dir + '/test_sliding_group_window_over_count.csv'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        self.t_env.create_temporary_system_function("my_sum", SumAggregateFunction())

        source_table_name = generate_random_table_name()
        source_table = f"""
            create table {source_table_name}(
                a TINYINT,
                b SMALLINT,
                c SMALLINT,
                protime as PROCTIME()
            ) with(
                'connector.type' = 'filesystem',
                'format.type' = 'csv',
                'connector.path' = '{source_path}',
                'format.ignore-first-line' = 'false',
                'format.field-delimiter' = ','
            )
        """
        self.t_env.execute_sql(source_table)
        t = self.t_env.from_path(source_table_name)

        from pyflink.testing import source_sink_utils
        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
        CREATE TABLE {sink_table}(a TINYINT, d BIGINT) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        t.window(Slide.over(row_interval(2)).every(row_interval(1)).on(t.protime).alias("w")) \
            .group_by(t.a, col("w")) \
            .select(t.a, call("my_sum", t.c).alias("b")) \
            .execute_insert(sink_table) \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[1, 5]", "+I[1, 11]", "+I[2, 4]", "+I[3, 5]"])

    def test_session_group_window_over_time(self):
        # create source file path
        tmp_dir = self.tempdir
        data = [
            '1,1,2,2018-03-11 03:10:00',
            '3,3,2,2018-03-11 03:10:00',
            '2,2,1,2018-03-11 03:10:00',
            '1,1,3,2018-03-11 03:40:00',
            '1,1,8,2018-03-11 04:20:00',
            '2,2,3,2018-03-11 03:30:00'
        ]
        source_path = tmp_dir + '/test_session_group_window_over_time.csv'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        source_table_name = generate_random_table_name()
        source_table = f"""
            create table {source_table_name}(
                a TINYINT,
                b SMALLINT,
                c SMALLINT,
                rowtime TIMESTAMP(3),
                WATERMARK FOR rowtime AS rowtime - INTERVAL '60' MINUTE
            ) with(
                'connector.type' = 'filesystem',
                'format.type' = 'csv',
                'connector.path' = '{source_path}',
                'format.ignore-first-line' = 'false',
                'format.field-delimiter' = ','
            )
        """
        self.t_env.execute_sql(source_table)
        t = self.t_env.from_path(source_table_name)

        from pyflink.testing import source_sink_utils
        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
        CREATE TABLE {sink_table}(a TINYINT, b TIMESTAMP(3), c TIMESTAMP(3), d BIGINT)
        WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        t.window(Session.with_gap(lit(30).minutes).on(t.rowtime).alias("w")) \
            .group_by(t.a, t.b, col("w")) \
            .select(t.a, col("w").start, col("w").end, call("my_count", t.c).alias("c")) \
            .execute_insert(sink_table) \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual,
                           ["+I[3, 2018-03-11T03:10, 2018-03-11T03:40, 1]",
                            "+I[2, 2018-03-11T03:10, 2018-03-11T04:00, 2]",
                            "+I[1, 2018-03-11T03:10, 2018-03-11T04:10, 2]",
                            "+I[1, 2018-03-11T04:20, 2018-03-11T04:50, 1]"])

    def test_execute_group_aggregate_from_json_plan(self):
        # create source file path
        tmp_dir = self.tempdir
        data = ['1,1', '3,2', '1,3']
        source_path = tmp_dir + '/test_execute_group_aggregate_from_json_plan.csv'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        source_table_name = generate_random_table_name()
        source_table = f"""
            CREATE TABLE {source_table_name} (
                a BIGINT,
                b BIGINT
            ) WITH (
                'connector' = 'filesystem',
                'path' = '{source_path}',
                'format' = 'csv'
            )
        """
        self.t_env.execute_sql(source_table)

        sink_table = generate_random_table_name()
        self.t_env.execute_sql(f"""
            CREATE TABLE {sink_table} (
                a BIGINT,
                b BIGINT
            ) WITH (
                'connector' = 'blackhole'
            )
        """)

        json_plan = self.t_env._j_tenv.compilePlanSql(f"""
            INSERT INTO {sink_table}
            SELECT  a, my_sum(b) FROM {source_table_name}
            GROUP BY a
        """)
        from py4j.java_gateway import get_method
        get_method(json_plan.execute(), "await")()

    def test_execute_group_window_aggregate_from_json_plan(self):
        # create source file path
        tmp_dir = self.tempdir
        data = [
            '1,1,2,2018-03-11 03:10:00',
            '3,3,2,2018-03-11 03:10:00',
            '2,2,1,2018-03-11 03:10:00',
            '1,1,3,2018-03-11 03:40:00',
            '1,1,8,2018-03-11 04:20:00',
            '2,2,3,2018-03-11 03:30:00'
        ]
        source_path = tmp_dir + '/test_execute_group_window_aggregate_from_json_plan.csv'
        sink_path = tmp_dir + '/test_execute_group_window_aggregate_from_json_plan'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        source_table_name = generate_random_table_name()
        source_table = f"""
            CREATE TABLE {source_table_name} (
                a TINYINT,
                b SMALLINT,
                c SMALLINT,
                rowtime TIMESTAMP(3),
                WATERMARK FOR rowtime AS rowtime - INTERVAL '60' MINUTE
            ) WITH (
                'connector' = 'filesystem',
                'path' = '{source_path}',
                'format' = 'csv'
            )
        """
        self.t_env.execute_sql(source_table)

        sink_table = generate_random_table_name()
        self.t_env.execute_sql(f"""
            CREATE TABLE {sink_table} (
                a BIGINT,
                w_start TIMESTAMP(3),
                w_end TIMESTAMP(3),
                b BIGINT
            ) WITH (
                'connector' = 'filesystem',
                'path' = '{sink_path}',
                'format' = 'csv'
            )
        """)

        json_plan = self.t_env._j_tenv.compilePlanSql(f"""
            INSERT INTO {sink_table}
            SELECT a, SESSION_START(rowtime, INTERVAL '30' MINUTE),
            SESSION_END(rowtime, INTERVAL '30' MINUTE),
            my_count(c) FROM {source_table_name}
            GROUP BY a, b, SESSION(rowtime, INTERVAL '30' MINUTE)
        """)
        from py4j.java_gateway import get_method
        get_method(json_plan.execute(), "await")()

        import glob
        lines = [line.strip() for file in glob.glob(sink_path + '/*') for line in open(file, 'r')]
        lines.sort()
        self.assertEqual(lines,
                         ['1,"2018-03-11 03:10:00","2018-03-11 04:10:00",2',
                          '1,"2018-03-11 04:20:00","2018-03-11 04:50:00",1',
                          '2,"2018-03-11 03:10:00","2018-03-11 04:00:00",2',
                          '3,"2018-03-11 03:10:00","2018-03-11 03:40:00",1'])


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
