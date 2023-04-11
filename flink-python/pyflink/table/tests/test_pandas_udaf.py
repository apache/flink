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
import uuid

from pyflink.table.expressions import col, call, lit, row_interval
from pyflink.table.types import DataTypes
from pyflink.table.udf import udaf, udf, AggregateFunction
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkBatchTableTestCase, \
    PyFlinkStreamTableTestCase


def generate_random_table_name():
    return "Table{0}".format(str(uuid.uuid1()).replace("-", "_"))


class BatchPandasUDAFITTests(PyFlinkBatchTableTestCase):

    @classmethod
    def setUpClass(cls):
        super(BatchPandasUDAFITTests, cls).setUpClass()
        cls.t_env.create_temporary_system_function("max_add", udaf(MaxAdd(),
                                                                   result_type=DataTypes.INT(),
                                                                   func_type="pandas"))
        cls.t_env.create_temporary_system_function("mean_udaf", mean_udaf)

    def test_check_result_type(self):
        def pandas_udaf():
            pass

        with self.assertRaises(
                TypeError,
                msg="Invalid returnType: Pandas UDAF doesn't support DataType type MAP currently"):
            udaf(pandas_udaf, result_type=DataTypes.MAP(DataTypes.INT(), DataTypes.INT()),
                 func_type="pandas")

    def test_group_aggregate_function(self):
        t = self.t_env.from_elements(
            [(1, 2, 3), (3, 2, 3), (2, 1, 3), (1, 5, 4), (1, 8, 6), (2, 3, 4)],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b", DataTypes.SMALLINT()),
                 DataTypes.FIELD("c", DataTypes.INT())]))

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
            CREATE TABLE {sink_table}(
                a TINYINT,
                b FLOAT,
                c ROW<a INT, b INT>,
                d STRING
            ) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        # general udf
        add = udf(lambda a: a + 1, result_type=DataTypes.INT())
        # pandas udf
        substract = udf(lambda a: a - 1, result_type=DataTypes.INT(), func_type="pandas")
        max_udaf = udaf(lambda a: (a.max(), a.min()),
                        result_type=DataTypes.ROW(
                            [DataTypes.FIELD("a", DataTypes.INT()),
                             DataTypes.FIELD("b", DataTypes.INT())]),
                        func_type="pandas")

        @udaf(result_type=DataTypes.STRING(), func_type="pandas")
        def multiply_udaf(a, b):
            return len(a) * b[0]

        t.group_by(t.a) \
            .select(t.a, mean_udaf(add(t.b)), max_udaf(substract(t.c)), multiply_udaf(t.b, 'abc')) \
            .execute_insert(sink_table) \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(
            actual,
            ["+I[1, 6.0, +I[5, 2], abcabcabc]",
             "+I[2, 3.0, +I[3, 2], abcabc]",
             "+I[3, 3.0, +I[2, 2], abc]"])

    def test_group_aggregate_without_keys(self):
        t = self.t_env.from_elements(
            [(1, 2, 3), (3, 2, 3), (2, 1, 3), (1, 5, 4), (1, 8, 6), (2, 3, 4)],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b", DataTypes.SMALLINT()),
                 DataTypes.FIELD("c", DataTypes.INT())]))

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
            CREATE TABLE {sink_table}(a INT) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        min_add = udaf(lambda a, b, c: a.min() + b.min() + c.min(),
                       result_type=DataTypes.INT(), func_type="pandas")
        t.select(min_add(t.a, t.b, t.c)) \
            .execute_insert(sink_table) \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[5]"])

    def test_group_aggregate_with_aux_group(self):
        t = self.t_env.from_elements(
            [(1, 2, 3), (3, 2, 3), (2, 1, 3), (1, 5, 4), (1, 8, 6), (2, 3, 4)],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b", DataTypes.SMALLINT()),
                 DataTypes.FIELD("c", DataTypes.INT())]))

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
        CREATE TABLE {sink_table}(a TINYINT, b INT, c FLOAT, d INT) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        self.t_env.get_config().get_configuration().set_string('python.metric.enabled', 'true')
        self.t_env.get_config().set('python.metric.enabled', 'true')
        t.group_by(t.a) \
            .select(t.a,  (t.a + 1).alias("b"), (t.a + 2).alias("c")) \
            .group_by(t.a, t.b) \
            .select(t.a, t.b, mean_udaf(t.b), call("max_add", t.b, t.c, 1)) \
            .execute_insert(sink_table) \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[1, 2, 2.0, 6]", "+I[2, 3, 3.0, 8]", "+I[3, 4, 4.0, 10]"])

    def test_tumble_group_window_aggregate_function(self):
        from pyflink.table.window import Tumble
        # create source file path
        data = [
            '1,2,3,2018-03-11 03:10:00',
            '3,2,4,2018-03-11 03:10:00',
            '2,1,2,2018-03-11 03:10:00',
            '1,3,1,2018-03-11 03:40:00',
            '1,8,5,2018-03-11 04:20:00',
            '2,3,6,2018-03-11 03:30:00'
        ]
        source_path = self.tempdir + '/test_tumble_group_window_aggregate_function.csv'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        self.t_env.get_config().set(
            "pipeline.time-characteristic", "EventTime")
        source_table = generate_random_table_name()
        source_table_ddl = f"""
            create table {source_table}(
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
        self.t_env.execute_sql(source_table_ddl)
        t = self.t_env.from_path(source_table)
        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
            CREATE TABLE {sink_table}(
                a TIMESTAMP(3),
                b TIMESTAMP(3),
                c FLOAT
            ) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        tumble_window = Tumble.over(lit(1).hours) \
            .on(col("rowtime")) \
            .alias("w")
        t.window(tumble_window) \
            .group_by(col("w")) \
            .select(col("w").start, col("w").end, mean_udaf(t.b)) \
            .execute_insert(sink_table) \
            .wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual,
                           ["+I[2018-03-11T03:00, 2018-03-11T04:00, 2.2]",
                            "+I[2018-03-11T04:00, 2018-03-11T05:00, 8.0]"])

    def test_slide_group_window_aggregate_function(self):
        from pyflink.table.window import Slide

        # create source file path
        data = [
            '1,2,3,2018-03-11 03:10:00',
            '3,2,4,2018-03-11 03:10:00',
            '2,1,2,2018-03-11 03:10:00',
            '1,3,1,2018-03-11 03:40:00',
            '1,8,5,2018-03-11 04:20:00',
            '2,3,6,2018-03-11 03:30:00'
        ]
        source_path = self.tempdir + '/test_slide_group_window_aggregate_function.csv'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        self.t_env.get_config().set(
            "pipeline.time-characteristic", "EventTime")
        source_table = generate_random_table_name()
        source_table_ddl = f"""
            create table {source_table}(
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
        self.t_env.execute_sql(source_table_ddl)
        t = self.t_env.from_path(source_table)
        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
            CREATE TABLE {sink_table}(
                a TINYINT,
                b TIMESTAMP(3),
                c TIMESTAMP(3),
                d FLOAT,
                e INT
            ) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        slide_window = Slide.over(lit(1).hours) \
            .every(lit(30).minutes) \
            .on(col("rowtime")) \
            .alias("w")
        t.window(slide_window) \
            .group_by(t.a, col("w")) \
            .select(t.a,
                    col("w").start,
                    col("w").end,
                    mean_udaf(t.b),
                    call("max_add", t.b, t.c, 1)) \
            .execute_insert(sink_table) \
            .wait()
        actual = source_sink_utils.results()

        self.assert_equals(actual,
                           ["+I[1, 2018-03-11T02:30, 2018-03-11T03:30, 2.0, 6]",
                            "+I[1, 2018-03-11T03:00, 2018-03-11T04:00, 2.5, 7]",
                            "+I[1, 2018-03-11T03:30, 2018-03-11T04:30, 5.5, 14]",
                            "+I[1, 2018-03-11T04:00, 2018-03-11T05:00, 8.0, 14]",
                            "+I[2, 2018-03-11T02:30, 2018-03-11T03:30, 1.0, 4]",
                            "+I[2, 2018-03-11T03:00, 2018-03-11T04:00, 2.0, 10]",
                            "+I[2, 2018-03-11T03:30, 2018-03-11T04:30, 3.0, 10]",
                            "+I[3, 2018-03-11T03:00, 2018-03-11T04:00, 2.0, 7]",
                            "+I[3, 2018-03-11T02:30, 2018-03-11T03:30, 2.0, 7]"])

    def test_over_window_aggregate_function(self):
        import datetime
        t = self.t_env.from_elements(
            [
                (1, 2, 3, datetime.datetime(2018, 3, 11, 3, 10, 0, 0)),
                (3, 2, 1, datetime.datetime(2018, 3, 11, 3, 10, 0, 0)),
                (2, 1, 2, datetime.datetime(2018, 3, 11, 3, 10, 0, 0)),
                (1, 3, 1, datetime.datetime(2018, 3, 11, 3, 10, 0, 0)),
                (1, 8, 5, datetime.datetime(2018, 3, 11, 4, 20, 0, 0)),
                (2, 3, 6, datetime.datetime(2018, 3, 11, 3, 30, 0, 0))
            ],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b", DataTypes.SMALLINT()),
                 DataTypes.FIELD("c", DataTypes.INT()),
                 DataTypes.FIELD("rowtime", DataTypes.TIMESTAMP(3))]))
        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
            CREATE TABLE {sink_table}(
                a TINYINT,
                b FLOAT,
                c INT,
                d FLOAT,
                e FLOAT,
                f FLOAT,
                g FLOAT,
                h FLOAT,
                i FLOAT,
                j FLOAT
            ) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        self.t_env.create_temporary_view("T_test_over_window_aggregate_function", t)
        self.t_env.execute_sql(f"""
            insert into {sink_table}
            select a,
             mean_udaf(b)
             over (PARTITION BY a ORDER BY rowtime
             ROWS BETWEEN UNBOUNDED preceding AND UNBOUNDED FOLLOWING),
             max_add(b, c)
             over (PARTITION BY a ORDER BY rowtime
             ROWS BETWEEN UNBOUNDED preceding AND 0 FOLLOWING),
             mean_udaf(b)
             over (PARTITION BY a ORDER BY rowtime
             ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING),
             mean_udaf(c)
             over (PARTITION BY a ORDER BY rowtime
             ROWS BETWEEN 1 PRECEDING AND 0 FOLLOWING),
             mean_udaf(c)
             over (PARTITION BY a ORDER BY rowtime
             RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
             mean_udaf(b)
             over (PARTITION BY a ORDER BY rowtime
             RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
             mean_udaf(b)
             over (PARTITION BY a ORDER BY rowtime
             RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND UNBOUNDED FOLLOWING),
             mean_udaf(c)
             over (PARTITION BY a ORDER BY rowtime
             RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND UNBOUNDED FOLLOWING),
             mean_udaf(c)
             over (PARTITION BY a ORDER BY rowtime
             RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND CURRENT ROW)
            from T_test_over_window_aggregate_function
        """).wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual,
                           ["+I[1, 4.3333335, 5, 4.3333335, 3.0, 3.0, 2.5, 4.3333335, 3.0, 2.0]",
                            "+I[1, 4.3333335, 13, 5.5, 3.0, 3.0, 4.3333335, 8.0, 5.0, 5.0]",
                            "+I[1, 4.3333335, 6, 4.3333335, 2.0, 3.0, 2.5, 4.3333335, 3.0, 2.0]",
                            "+I[2, 2.0, 9, 2.0, 4.0, 4.0, 2.0, 2.0, 4.0, 4.0]",
                            "+I[2, 2.0, 3, 2.0, 2.0, 4.0, 1.0, 2.0, 4.0, 2.0]",
                            "+I[3, 2.0, 3, 2.0, 1.0, 1.0, 2.0, 2.0, 1.0, 1.0]"])


class StreamPandasUDAFITTests(PyFlinkStreamTableTestCase):

    @classmethod
    def setUpClass(cls):
        super(StreamPandasUDAFITTests, cls).setUpClass()
        cls.t_env.create_temporary_system_function("mean_udaf", mean_udaf)
        max_add_min_udaf = udaf(lambda a: a.max() + a.min(),
                                result_type='SMALLINT',
                                func_type='pandas')
        cls.t_env.create_temporary_system_function("max_add_min_udaf", max_add_min_udaf)

    def test_sliding_group_window_over_time(self):
        # create source file path
        import tempfile
        import os
        tmp_dir = tempfile.gettempdir()
        data = [
            '1,1,2,2018-03-11 03:10:00',
            '3,3,2,2018-03-11 03:10:00',
            '2,2,1,2018-03-11 03:10:00',
            '1,1,3,2018-03-11 03:40:00',
            '1,1,8,2018-03-11 04:20:00',
            '2,2,3,2018-03-11 03:30:00'
        ]
        source_path = tmp_dir + '/test_sliding_group_window_over_time.csv'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        from pyflink.table.window import Slide
        self.t_env.get_config().set(
            "pipeline.time-characteristic", "EventTime")

        source_table = generate_random_table_name()
        source_table_ddl = f"""
            create table {source_table}(
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
        self.t_env.execute_sql(source_table_ddl)
        t = self.t_env.from_path(source_table)
        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
            CREATE TABLE {sink_table}(a TINYINT, b TIMESTAMP(3), c TIMESTAMP(3), d FLOAT)
            WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        t.window(Slide.over(lit(1).hours)
                 .every(lit(30).minutes)
                 .on(col("rowtime"))
                 .alias("w")) \
            .group_by(t.a, t.b, col("w")) \
            .select(t.a, col("w").start, col("w").end, mean_udaf(t.c).alias("b")) \
            .execute_insert(sink_table) \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual,
                           ["+I[1, 2018-03-11T02:30, 2018-03-11T03:30, 2.0]",
                            "+I[1, 2018-03-11T03:00, 2018-03-11T04:00, 2.5]",
                            "+I[1, 2018-03-11T03:30, 2018-03-11T04:30, 5.5]",
                            "+I[1, 2018-03-11T04:00, 2018-03-11T05:00, 8.0]",
                            "+I[2, 2018-03-11T02:30, 2018-03-11T03:30, 1.0]",
                            "+I[2, 2018-03-11T03:00, 2018-03-11T04:00, 2.0]",
                            "+I[2, 2018-03-11T03:30, 2018-03-11T04:30, 3.0]",
                            "+I[3, 2018-03-11T03:00, 2018-03-11T04:00, 2.0]",
                            "+I[3, 2018-03-11T02:30, 2018-03-11T03:30, 2.0]"])
        os.remove(source_path)

    def test_sliding_group_window_over_count(self):
        self.t_env.get_config().set("parallelism.default", "1")
        # create source file path
        import tempfile
        import os
        tmp_dir = tempfile.gettempdir()
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

        from pyflink.table.window import Slide
        self.t_env.get_config().set(
            "pipeline.time-characteristic", "ProcessingTime")

        source_table = generate_random_table_name()
        source_table_ddl = f"""
            create table {source_table}(
                a TINYINT,
                b SMALLINT,
                c SMALLINT,
                protime as PROCTIME()
            ) with(
                'connector.type' = 'filesystem',
                'format.type' = 'csv',
                'connector.path' = '%s',
                'format.ignore-first-line' = 'false',
                'format.field-delimiter' = ','
            )
        """ % source_path
        self.t_env.execute_sql(source_table_ddl)
        t = self.t_env.from_path(source_table)

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
        CREATE TABLE {sink_table}(a TINYINT, d FLOAT) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        t.window(Slide.over(row_interval(2))
                 .every(row_interval(1))
                 .on(t.protime)
                 .alias("w")) \
            .group_by(t.a, t.b, col("w")) \
            .select(t.a, mean_udaf(t.c).alias("b")) \
            .execute_insert(sink_table) \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[1, 2.5]", "+I[1, 5.5]", "+I[2, 2.0]", "+I[3, 2.5]"])
        os.remove(source_path)

    def test_tumbling_group_window_over_time(self):
        # create source file path
        import tempfile
        import os
        tmp_dir = tempfile.gettempdir()
        data = [
            '1,1,2,2018-03-11 03:10:00',
            '3,3,2,2018-03-11 03:10:00',
            '2,2,1,2018-03-11 03:10:00',
            '1,1,3,2018-03-11 03:40:00',
            '1,1,8,2018-03-11 04:20:00',
            '2,2,3,2018-03-11 03:30:00'
        ]
        source_path = tmp_dir + '/test_tumbling_group_window_over_time.csv'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        from pyflink.table.window import Tumble
        self.t_env.get_config().set(
            "pipeline.time-characteristic", "EventTime")

        source_table = generate_random_table_name()
        source_table_ddl = f"""
            create table {source_table}(
                a TINYINT,
                b SMALLINT,
                c SMALLINT,
                rowtime TIMESTAMP(3),
                WATERMARK FOR rowtime AS rowtime - INTERVAL '60' MINUTE
            ) with(
                'connector.type' = 'filesystem',
                'format.type' = 'csv',
                'connector.path' = '%s',
                'format.ignore-first-line' = 'false',
                'format.field-delimiter' = ','
            )
        """ % source_path
        self.t_env.execute_sql(source_table_ddl)
        t = self.t_env.from_path(source_table)

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
        CREATE TABLE {sink_table}(
        a TINYINT, b TIMESTAMP(3), c TIMESTAMP(3), d TIMESTAMP(3), e FLOAT)
        WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        t.window(Tumble.over(lit(1).hours).on(t.rowtime).alias("w")) \
            .group_by(t.a, t.b, col("w")) \
            .select(t.a,
                    col("w").start,
                    col("w").end,
                    col("w").rowtime,
                    mean_udaf(t.c).alias("b")) \
            .execute_insert(sink_table) \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, [
            "+I[1, 2018-03-11T03:00, 2018-03-11T04:00, 2018-03-11T03:59:59.999, 2.5]",
            "+I[1, 2018-03-11T04:00, 2018-03-11T05:00, 2018-03-11T04:59:59.999, 8.0]",
            "+I[2, 2018-03-11T03:00, 2018-03-11T04:00, 2018-03-11T03:59:59.999, 2.0]",
            "+I[3, 2018-03-11T03:00, 2018-03-11T04:00, 2018-03-11T03:59:59.999, 2.0]",
        ])
        os.remove(source_path)

    def test_tumbling_group_window_over_count(self):
        self.t_env.get_config().set("parallelism.default", "1")
        # create source file path
        import tempfile
        import os
        tmp_dir = tempfile.gettempdir()
        data = [
            '1,1,2,2018-03-11 03:10:00',
            '3,3,2,2018-03-11 03:10:00',
            '2,2,1,2018-03-11 03:10:00',
            '1,1,3,2018-03-11 03:40:00',
            '1,1,8,2018-03-11 04:20:00',
            '2,2,3,2018-03-11 03:30:00',
            '3,3,3,2018-03-11 03:30:00',
            '1,1,4,2018-03-11 04:20:00',
        ]
        source_path = tmp_dir + '/test_group_window_aggregate_function_over_count.csv'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        from pyflink.table.window import Tumble
        self.t_env.get_config().set(
            "pipeline.time-characteristic", "ProcessingTime")

        source_table = generate_random_table_name()
        source_table_ddl = f"""
            create table {source_table}(
                a TINYINT,
                b SMALLINT,
                c SMALLINT,
                protime as PROCTIME()
            ) with(
                'connector.type' = 'filesystem',
                'format.type' = 'csv',
                'connector.path' = '%s',
                'format.ignore-first-line' = 'false',
                'format.field-delimiter' = ','
            )
        """ % source_path
        self.t_env.execute_sql(source_table_ddl)
        t = self.t_env.from_path(source_table)

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
        CREATE TABLE {sink_table}(a TINYINT, d FLOAT) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        t.window(Tumble.over(row_interval(2)).on(t.protime).alias("w")) \
            .group_by(t.a, t.b, col("w")) \
            .select(t.a, mean_udaf(t.c).alias("b")) \
            .execute_insert(sink_table) \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[1, 2.5]", "+I[1, 6.0]", "+I[2, 2.0]", "+I[3, 2.5]"])
        os.remove(source_path)

    def test_row_time_over_range_window_aggregate_function(self):
        # create source file path
        import tempfile
        import os
        tmp_dir = tempfile.gettempdir()
        data = [
            '1,1,2013-01-01 03:10:00',
            '3,2,2013-01-01 03:10:00',
            '2,1,2013-01-01 03:10:00',
            '1,5,2013-01-01 03:10:00',
            '1,8,2013-01-01 04:20:00',
            '2,3,2013-01-01 03:30:00'
        ]
        source_path = tmp_dir + '/test_over_range_window_aggregate_function.csv'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        self.t_env.get_config().set(
            "pipeline.time-characteristic", "EventTime")
        source_table = generate_random_table_name()
        source_table_ddl = f"""
            create table {source_table}(
                a TINYINT,
                b SMALLINT,
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
        self.t_env.execute_sql(source_table_ddl)
        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
        CREATE TABLE {sink_table}(a TINYINT, b FLOAT, c SMALLINT) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        self.t_env.execute_sql(f"""
            insert into {sink_table}
            select a,
             mean_udaf(b)
             over (PARTITION BY a ORDER BY rowtime
             RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND CURRENT ROW),
             max_add_min_udaf(b)
             over (PARTITION BY a ORDER BY rowtime
             RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND CURRENT ROW)
            from {source_table}
        """).wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual,
                           ["+I[1, 3.0, 6]",
                            "+I[1, 3.0, 6]",
                            "+I[1, 8.0, 16]",
                            "+I[2, 1.0, 2]",
                            "+I[2, 2.0, 4]",
                            "+I[3, 2.0, 4]"])
        os.remove(source_path)

    def test_row_time_over_rows_window_aggregate_function(self):
        # create source file path
        import tempfile
        import os
        tmp_dir = tempfile.gettempdir()
        data = [
            '1,1,2013-01-01 03:10:00',
            '3,2,2013-01-01 03:10:00',
            '2,1,2013-01-01 03:10:00',
            '1,5,2013-01-01 03:10:00',
            '1,8,2013-01-01 04:20:00',
            '2,3,2013-01-01 03:30:00'
        ]
        source_path = tmp_dir + '/test_over_rows_window_aggregate_function.csv'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        self.t_env.get_config().set(
            "pipeline.time-characteristic", "EventTime")
        source_table = generate_random_table_name()
        source_table_ddl = f"""
            create table {source_table}(
                a TINYINT,
                b SMALLINT,
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
        self.t_env.execute_sql(source_table_ddl)

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
        CREATE TABLE {sink_table}(a TINYINT, b FLOAT, c SMALLINT) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        self.t_env.execute_sql(f"""
            insert into {sink_table}
            select a,
             mean_udaf(b)
             over (PARTITION BY a ORDER BY rowtime
             ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
             max_add_min_udaf(b)
             over (PARTITION BY a ORDER BY rowtime
             ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
            from {source_table}
        """).wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual,
                           ["+I[1, 1.0, 2]",
                            "+I[1, 3.0, 6]",
                            "+I[1, 6.5, 13]",
                            "+I[2, 1.0, 2]",
                            "+I[2, 2.0, 4]",
                            "+I[3, 2.0, 4]"])
        os.remove(source_path)

    def test_proc_time_over_rows_window_aggregate_function(self):
        # create source file path
        data = [
            '1,1,2013-01-01 03:10:00',
            '3,2,2013-01-01 03:10:00',
            '2,1,2013-01-01 03:10:00',
            '1,5,2013-01-01 03:10:00',
            '1,8,2013-01-01 04:20:00',
            '2,3,2013-01-01 03:30:00'
        ]
        source_path = self.tempdir + '/test_proc_time_over_rows_window_aggregate_function.csv'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        self.t_env.get_config().set("parallelism.default", "1")
        self.t_env.get_config().set(
            "pipeline.time-characteristic", "ProcessingTime")
        source_table = generate_random_table_name()
        source_table_ddl = f"""
            create table {source_table}(
                a TINYINT,
                b SMALLINT,
                proctime as PROCTIME()
            ) with(
                'connector.type' = 'filesystem',
                'format.type' = 'csv',
                'connector.path' = '{source_path}',
                'format.ignore-first-line' = 'false',
                'format.field-delimiter' = ','
            )
        """
        self.t_env.execute_sql(source_table_ddl)
        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
        CREATE TABLE {sink_table}(a TINYINT, b FLOAT, c SMALLINT) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        self.t_env.execute_sql(f"""
            insert into {sink_table}
            select a,
             mean_udaf(b)
             over (PARTITION BY a ORDER BY proctime
             ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
             max_add_min_udaf(b)
             over (PARTITION BY a ORDER BY proctime
             ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
            from {source_table}
        """).wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual,
                           ["+I[1, 1.0, 2]",
                            "+I[1, 3.0, 6]",
                            "+I[1, 6.5, 13]",
                            "+I[2, 1.0, 2]",
                            "+I[2, 2.0, 4]",
                            "+I[3, 2.0, 4]"])

    def test_execute_over_aggregate_from_json_plan(self):
        # create source file path
        tmp_dir = self.tempdir
        data = [
            '1,1,2013-01-01 03:10:00',
            '3,2,2013-01-01 03:10:00',
            '2,1,2013-01-01 03:10:00',
            '1,5,2013-01-01 03:10:00',
            '1,8,2013-01-01 04:20:00',
            '2,3,2013-01-01 03:30:00'
        ]
        source_path = tmp_dir + '/test_execute_over_aggregate_from_json_plan.csv'
        sink_path = tmp_dir + '/test_execute_over_aggregate_from_json_plan'
        with open(source_path, 'w') as fd:
            for ele in data:
                fd.write(ele + '\n')

        source_table = generate_random_table_name()
        source_table_ddl = f"""
            CREATE TABLE {source_table} (
                a TINYINT,
                b SMALLINT,
                rowtime TIMESTAMP(3),
                WATERMARK FOR rowtime AS rowtime - INTERVAL '60' MINUTE
            ) WITH (
                'connector' = 'filesystem',
                'path' = '{source_path}',
                'format' = 'csv'
            )
        """
        self.t_env.execute_sql(source_table_ddl)

        sink_table = generate_random_table_name()
        self.t_env.execute_sql(f"""
            CREATE TABLE {sink_table} (
                a TINYINT,
                b FLOAT,
                c SMALLINT
            ) WITH (
                'connector' = 'filesystem',
                'path' = '{sink_path}',
                'format' = 'csv'
            )
        """)

        self.t_env.get_config().set(
            "pipeline.time-characteristic", "EventTime")

        json_plan = self.t_env._j_tenv.compilePlanSql(f"""
        insert into {sink_table}
            select a,
             mean_udaf(b)
             over (PARTITION BY a ORDER BY rowtime
             ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
             max_add_min_udaf(b)
             over (PARTITION BY a ORDER BY rowtime
             ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
            from {source_table}
        """)
        from py4j.java_gateway import get_method
        get_method(json_plan.execute(), "await")()

        import glob
        lines = [line.strip() for file in glob.glob(sink_path + '/*') for line in open(file, 'r')]
        lines.sort()
        self.assertEqual(lines, ['1,1.0,2', '1,3.0,6', '1,6.5,13', '2,1.0,2', '2,2.0,4', '3,2.0,4'])


@udaf(result_type=DataTypes.FLOAT(), func_type="pandas")
def mean_udaf(v):
    return v.mean()


class MaxAdd(AggregateFunction):

    def __init__(self):
        self.counter = None
        self.counter_sum = 0

    def open(self, function_context):
        mg = function_context.get_metric_group()
        self.counter = mg.add_group("key", "value").counter("my_counter")
        self.counter_sum = 0

    def get_value(self, accumulator):
        # counter
        self.counter.inc(10)
        self.counter_sum += 10
        return accumulator[0]

    def create_accumulator(self):
        return []

    def accumulate(self, accumulator, *args):
        result = 0
        for arg in args:
            result += arg.max()
        accumulator.append(result)


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
