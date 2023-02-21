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
import datetime
import os
import unittest
import uuid

import pytz

from pyflink.common import Row
from pyflink.table import DataTypes, expressions as expr
from pyflink.table.expressions import call
from pyflink.table.udf import ScalarFunction, udf, FunctionContext
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, \
    PyFlinkBatchTableTestCase


def generate_random_table_name():
    return "Table{0}".format(str(uuid.uuid1()).replace("-", "_"))


class UserDefinedFunctionTests(object):

    def test_scalar_function(self):
        # test metric disabled.
        self.t_env.get_config().set('python.metric.enabled', 'false')
        self.t_env.get_config().set('pipeline.global-job-parameters', 'subtract_value:2')
        # test lambda function
        add_one = udf(lambda i: i + 1, result_type=DataTypes.BIGINT())

        # test Python ScalarFunction
        subtract_one = udf(SubtractOne(), result_type=DataTypes.BIGINT())

        subtract_two = udf(SubtractWithParameters(), result_type=DataTypes.BIGINT())

        # test callable function
        add_one_callable = udf(CallablePlus(), result_type=DataTypes.BIGINT())

        def partial_func(col, param):
            return col + param

        # test partial function
        import functools
        add_one_partial = udf(functools.partial(partial_func, param=1),
                              result_type=DataTypes.BIGINT())

        # check memory limit is set
        @udf(result_type=DataTypes.BIGINT())
        def check_memory_limit(exec_mode):
            if exec_mode == "process":
                assert os.environ['_PYTHON_WORKER_MEMORY_LIMIT'] is not None
            return 1

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
            CREATE TABLE {sink_table}(a BIGINT, b BIGINT, c BIGINT, d BIGINT, e BIGINT, f BIGINT,
             g BIGINT, h BIGINT) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)

        execution_mode = self.t_env.get_config().get("python.execution-mode", "process")

        t = self.t_env.from_elements([(1, 2, 3), (2, 5, 6), (3, 1, 9)], ['a', 'b', 'c'])
        t.where(add_one(t.b) <= 3).select(
            add_one(t.a),
            subtract_one(t.b),
            subtract_two(t.b),
            add(t.a, t.c),
            add_one_callable(t.a),
            add_one_partial(t.a),
            check_memory_limit(execution_mode),
            t.a).execute_insert(sink_table).wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[2, 1, 0, 4, 2, 2, 1, 1]", "+I[4, 0, -1, 12, 4, 4, 1, 3]"])

    def test_chaining_scalar_function(self):
        add_one = udf(lambda i: i + 1, result_type=DataTypes.BIGINT())
        subtract_one = udf(SubtractOne(), result_type=DataTypes.BIGINT())

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
                CREATE TABLE {sink_table}(a BIGINT, b BIGINT, c INT) WITH ('connector'='test-sink')
                """
        self.t_env.execute_sql(sink_table_ddl)

        t = self.t_env.from_elements([(1, 2, 1), (2, 5, 2), (3, 1, 3)], ['a', 'b', 'c'])
        t.select(add(add_one(t.a), subtract_one(t.b)), t.c, expr.lit(1)) \
            .execute_insert(sink_table).wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[3, 1, 1]", "+I[7, 2, 1]", "+I[4, 3, 1]"])

    def test_udf_in_join_condition(self):
        t1 = self.t_env.from_elements([(2, "Hi")], ['a', 'b'])
        t2 = self.t_env.from_elements([(2, "Flink")], ['c', 'd'])

        f = udf(lambda i: i, result_type=DataTypes.BIGINT())

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
            CREATE TABLE {sink_table}(a BIGINT, b STRING, c BIGINT, d StRING)
            WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        t1.join(t2).where(f(t1.a) == t2.c).execute_insert(sink_table).wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[2, Hi, 2, Flink]"])

    def test_udf_in_join_condition_2(self):
        t1 = self.t_env.from_elements([(1, "Hi"), (2, "Hi")], ['a', 'b'])
        t2 = self.t_env.from_elements([(2, "Flink")], ['c', 'd'])

        f = udf(lambda i: i, result_type=DataTypes.BIGINT())

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
            CREATE TABLE {sink_table}(
                a BIGINT,
                b STRING,
                c BIGINT,
                d STRING
            ) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)

        t1.join(t2).where(f(t1.a) == f(t2.c)).execute_insert(sink_table).wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[2, Hi, 2, Flink]"])

    def test_udf_with_constant_params(self):
        def udf_with_constant_params(p, null_param, tinyint_param, smallint_param, int_param,
                                     bigint_param, decimal_param, float_param, double_param,
                                     boolean_param, str_param,
                                     date_param, time_param, timestamp_param):
            from decimal import Decimal
            import datetime

            assert null_param is None, 'null_param is wrong value %s' % null_param

            assert isinstance(tinyint_param, int), 'tinyint_param of wrong type %s !' \
                                                   % type(tinyint_param)
            p += tinyint_param
            assert isinstance(smallint_param, int), 'smallint_param of wrong type %s !' \
                                                    % type(smallint_param)
            p += smallint_param
            assert isinstance(int_param, int), 'int_param of wrong type %s !' \
                                               % type(int_param)
            p += int_param
            assert isinstance(bigint_param, int), 'bigint_param of wrong type %s !' \
                                                  % type(bigint_param)
            p += bigint_param
            assert decimal_param == Decimal('1.05'), \
                'decimal_param is wrong value %s ' % decimal_param

            p += int(decimal_param)

            assert isinstance(float_param, float) and float_equal(float_param, 1.23, 1e-06), \
                'float_param is wrong value %s ' % float_param

            p += int(float_param)
            assert isinstance(double_param, float) and float_equal(double_param, 1.98932, 1e-07), \
                'double_param is wrong value %s ' % double_param

            p += int(double_param)

            assert boolean_param is True, 'boolean_param is wrong value %s' % boolean_param

            assert str_param == 'flink', 'str_param is wrong value %s' % str_param

            assert date_param == datetime.date(year=2014, month=9, day=13), \
                'date_param is wrong value %s' % date_param

            assert time_param == datetime.time(hour=12, minute=0, second=0), \
                'time_param is wrong value %s' % time_param

            assert timestamp_param == datetime.datetime(1999, 9, 10, 5, 20, 10), \
                'timestamp_param is wrong value %s' % timestamp_param

            return p

        self.t_env.create_temporary_system_function("udf_with_constant_params",
                                                    udf(udf_with_constant_params,
                                                        result_type=DataTypes.BIGINT()))

        self.t_env.create_temporary_system_function(
            "udf_with_all_constant_params", udf(lambda i, j: i + j,
                                                result_type=DataTypes.BIGINT()))

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
                CREATE TABLE {sink_table}(a BIGINT, b BIGINT) WITH ('connector'='test-sink')
                """
        self.t_env.execute_sql(sink_table_ddl)

        t = self.t_env.from_elements([(1, 2, 3), (2, 5, 6), (3, 1, 9)], ['a', 'b', 'c'])
        self.t_env.create_temporary_view("test_table", t)
        self.t_env.sql_query("select udf_with_all_constant_params("
                             "cast (1 as BIGINT),"
                             "cast (2 as BIGINT)), "
                             "udf_with_constant_params(a, "
                             "cast (null as BIGINT),"
                             "cast (1 as TINYINT),"
                             "cast (1 as SMALLINT),"
                             "cast (1 as INT),"
                             "cast (1 as BIGINT),"
                             "cast (1.05 as DECIMAL),"
                             "cast (1.23 as FLOAT),"
                             "cast (1.98932 as DOUBLE),"
                             "true,"
                             "'flink',"
                             "cast ('2014-09-13' as DATE),"
                             "cast ('12:00:00' as TIME),"
                             "cast ('1999-9-10 05:20:10' as TIMESTAMP))"
                             " from test_table").execute_insert(sink_table).wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[3, 8]", "+I[3, 9]", "+I[3, 10]"])

    def test_overwrite_builtin_function(self):
        self.t_env.create_temporary_system_function(
            "plus", udf(lambda i, j: i + j - 1,
                        result_type=DataTypes.BIGINT()))

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
                        CREATE TABLE {sink_table}(a BIGINT) WITH ('connector'='test-sink')
                        """
        self.t_env.execute_sql(sink_table_ddl)

        t = self.t_env.from_elements([(1, 2, 3), (2, 5, 6), (3, 1, 9)], ['a', 'b', 'c'])
        t.select(t.a + t.b).execute_insert(sink_table).wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[2]", "+I[6]", "+I[3]"])

    def test_open(self):
        self.t_env.get_config().set('python.metric.enabled', 'true')
        execution_mode = self.t_env.get_config().get("python.execution-mode", None)

        if execution_mode == "process":
            subtract = udf(SubtractWithMetrics(), result_type=DataTypes.BIGINT())
        else:
            subtract = udf(Subtract(), result_type=DataTypes.BIGINT())

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
                        CREATE TABLE {sink_table}(a BIGINT, b BIGINT) WITH ('connector'='test-sink')
                        """
        self.t_env.execute_sql(sink_table_ddl)

        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 4)], ['a', 'b'])
        t.select(t.a, subtract(t.b)).execute_insert(sink_table).wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[1, 1]", "+I[2, 4]", "+I[3, 3]"])

    def test_udf_without_arguments(self):
        one = udf(lambda: 1, result_type=DataTypes.BIGINT(), deterministic=True)
        two = udf(lambda: 2, result_type=DataTypes.BIGINT(), deterministic=False)

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
                        CREATE TABLE {sink_table}(a BIGINT, b BIGINT) WITH ('connector'='test-sink')
                        """
        self.t_env.execute_sql(sink_table_ddl)

        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select(one(), two()).execute_insert(sink_table).wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[1, 2]", "+I[1, 2]", "+I[1, 2]"])

    def test_all_data_types_expression(self):
        @udf(result_type=DataTypes.BOOLEAN())
        def boolean_func(bool_param):
            assert isinstance(bool_param, bool), 'bool_param of wrong type %s !' \
                                                 % type(bool_param)
            return bool_param

        @udf(result_type=DataTypes.TINYINT())
        def tinyint_func(tinyint_param):
            assert isinstance(tinyint_param, int), 'tinyint_param of wrong type %s !' \
                                                   % type(tinyint_param)
            return tinyint_param

        @udf(result_type=DataTypes.SMALLINT())
        def smallint_func(smallint_param):
            assert isinstance(smallint_param, int), 'smallint_param of wrong type %s !' \
                                                    % type(smallint_param)
            assert smallint_param == 32767, 'smallint_param of wrong value %s' % smallint_param
            return smallint_param

        @udf(result_type=DataTypes.INT())
        def int_func(int_param):
            assert isinstance(int_param, int), 'int_param of wrong type %s !' \
                                               % type(int_param)
            assert int_param == -2147483648, 'int_param of wrong value %s' % int_param
            return int_param

        @udf(result_type=DataTypes.BIGINT())
        def bigint_func(bigint_param):
            assert isinstance(bigint_param, int), 'bigint_param of wrong type %s !' \
                                                  % type(bigint_param)
            return bigint_param

        @udf(result_type=DataTypes.BIGINT())
        def bigint_func_none(bigint_param):
            assert bigint_param is None, 'bigint_param %s should be None!' % bigint_param
            return bigint_param

        @udf(result_type=DataTypes.FLOAT())
        def float_func(float_param):
            assert isinstance(float_param, float) and float_equal(float_param, 1.23, 1e-6), \
                'float_param is wrong value %s !' % float_param
            return float_param

        @udf(result_type=DataTypes.DOUBLE())
        def double_func(double_param):
            assert isinstance(double_param, float) and float_equal(double_param, 1.98932, 1e-7), \
                'double_param is wrong value %s !' % double_param
            return double_param

        @udf(result_type=DataTypes.BYTES())
        def bytes_func(bytes_param):
            assert bytes_param == b'flink', \
                'bytes_param is wrong value %s !' % bytes_param
            return bytes_param

        @udf(result_type=DataTypes.STRING())
        def str_func(str_param):
            assert str_param == 'pyflink', \
                'str_param is wrong value %s !' % str_param
            return str_param

        @udf(result_type=DataTypes.DATE())
        def date_func(date_param):
            from datetime import date
            assert date_param == date(year=2014, month=9, day=13), \
                'date_param is wrong value %s !' % date_param
            return date_param

        @udf(result_type=DataTypes.TIME())
        def time_func(time_param):
            from datetime import time
            assert time_param == time(hour=12, minute=0, second=0, microsecond=123000), \
                'time_param is wrong value %s !' % time_param
            return time_param

        @udf(result_type=DataTypes.TIMESTAMP(3))
        def timestamp_func(timestamp_param):
            from datetime import datetime
            assert timestamp_param == datetime(2018, 3, 11, 3, 0, 0, 123000), \
                'timestamp_param is wrong value %s !' % timestamp_param
            return timestamp_param

        @udf(result_type=DataTypes.ARRAY(DataTypes.BIGINT()))
        def array_func(array_param):
            assert array_param == [[1, 2, 3]] or array_param == ((1, 2, 3),), \
                'array_param is wrong value %s !' % array_param
            return array_param[0]

        @udf(result_type=DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()))
        def map_func(map_param):
            assert map_param == {1: 'flink', 2: 'pyflink'}, \
                'map_param is wrong value %s !' % map_param
            return map_param

        @udf(result_type=DataTypes.DECIMAL(38, 18))
        def decimal_func(decimal_param):
            from decimal import Decimal
            assert decimal_param == Decimal('1000000000000000000.050000000000000000'), \
                'decimal_param is wrong value %s !' % decimal_param
            return decimal_param

        @udf(result_type=DataTypes.DECIMAL(38, 18))
        def decimal_cut_func(decimal_param):
            from decimal import Decimal
            assert decimal_param == Decimal('1000000000000000000.059999999999999999'), \
                'decimal_param is wrong value %s !' % decimal_param
            return decimal_param

        @udf(result_type=DataTypes.BINARY(5))
        def binary_func(binary_param):
            assert len(binary_param) == 5
            return binary_param

        @udf(result_type=DataTypes.CHAR(7))
        def char_func(char_param):
            assert len(char_param) == 7
            return char_param

        @udf(result_type=DataTypes.VARCHAR(10))
        def varchar_func(varchar_param):
            assert len(varchar_param) <= 10
            return varchar_param

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
            CREATE TABLE {sink_table}(
                a BIGINT,
                b BIGINT,
                c TINYINT,
                d BOOLEAN,
                e SMALLINT,
                f INT,
                g FLOAT,
                h DOUBLE,
                i BYTES,
                j STRING,
                k DATE,
                l TIME,
                m TIMESTAMP(3),
                n ARRAY<BIGINT>,
                o MAP<BIGINT, STRING>,
                p DECIMAL(38, 18),
                q DECIMAL(38, 18),
                r BINARY(5),
                s CHAR(7),
                t VARCHAR(10)
            ) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)

        import datetime
        import decimal
        t = self.t_env.from_elements(
            [(1, None, 1, True, 32767, -2147483648, 1.23, 1.98932,
              bytearray(b'flink'), 'pyflink', datetime.date(2014, 9, 13),
              datetime.time(hour=12, minute=0, second=0, microsecond=123000),
              datetime.datetime(2018, 3, 11, 3, 0, 0, 123000), [[1, 2, 3]],
              {1: 'flink', 2: 'pyflink'}, decimal.Decimal('1000000000000000000.05'),
              decimal.Decimal('1000000000000000000.05999999999999999899999999999'),
              bytearray(b'flink'), 'pyflink', 'pyflink')],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.BIGINT()),
                 DataTypes.FIELD("b", DataTypes.BIGINT()),
                 DataTypes.FIELD("c", DataTypes.TINYINT()),
                 DataTypes.FIELD("d", DataTypes.BOOLEAN()),
                 DataTypes.FIELD("e", DataTypes.SMALLINT()),
                 DataTypes.FIELD("f", DataTypes.INT()),
                 DataTypes.FIELD("g", DataTypes.FLOAT()),
                 DataTypes.FIELD("h", DataTypes.DOUBLE()),
                 DataTypes.FIELD("i", DataTypes.BYTES()),
                 DataTypes.FIELD("j", DataTypes.STRING()),
                 DataTypes.FIELD("k", DataTypes.DATE()),
                 DataTypes.FIELD("l", DataTypes.TIME()),
                 DataTypes.FIELD("m", DataTypes.TIMESTAMP(3)),
                 DataTypes.FIELD("n", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.BIGINT()))),
                 DataTypes.FIELD("o", DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING())),
                 DataTypes.FIELD("p", DataTypes.DECIMAL(38, 18)),
                 DataTypes.FIELD("q", DataTypes.DECIMAL(38, 18)),
                 DataTypes.FIELD("r", DataTypes.BINARY(5)),
                 DataTypes.FIELD("s", DataTypes.CHAR(7)),
                 DataTypes.FIELD("t", DataTypes.VARCHAR(10))]))

        t.select(
            bigint_func(t.a),
            bigint_func_none(t.b),
            tinyint_func(t.c),
            boolean_func(t.d),
            smallint_func(t.e),
            int_func(t.f),
            float_func(t.g),
            double_func(t.h),
            bytes_func(t.i),
            str_func(t.j),
            date_func(t.k),
            time_func(t.l),
            timestamp_func(t.m),
            array_func(t.n),
            map_func(t.o),
            decimal_func(t.p),
            decimal_cut_func(t.q),
            binary_func(t.r),
            char_func(t.s),
            varchar_func(t.t)) \
            .execute_insert(sink_table).wait()
        actual = source_sink_utils.results()
        # Currently the sink result precision of DataTypes.TIME(precision) only supports 0.
        self.assert_equals(actual,
                           ["+I[1, null, 1, true, 32767, -2147483648, 1.23, 1.98932, "
                            "[102, 108, 105, 110, 107], pyflink, 2014-09-13, 12:00:00.123, "
                            "2018-03-11T03:00:00.123, [1, 2, 3], "
                            "{1=flink, 2=pyflink}, 1000000000000000000.050000000000000000, "
                            "1000000000000000000.059999999999999999, [102, 108, 105, 110, 107], "
                            "pyflink, pyflink]"])

    def test_all_data_types(self):
        def boolean_func(bool_param):
            assert isinstance(bool_param, bool), 'bool_param of wrong type %s !' \
                                                 % type(bool_param)
            return bool_param

        def tinyint_func(tinyint_param):
            assert isinstance(tinyint_param, int), 'tinyint_param of wrong type %s !' \
                                                   % type(tinyint_param)
            return tinyint_param

        def smallint_func(smallint_param):
            assert isinstance(smallint_param, int), 'smallint_param of wrong type %s !' \
                                                    % type(smallint_param)
            assert smallint_param == 32767, 'smallint_param of wrong value %s' % smallint_param
            return smallint_param

        def int_func(int_param):
            assert isinstance(int_param, int), 'int_param of wrong type %s !' \
                                               % type(int_param)
            assert int_param == -2147483648, 'int_param of wrong value %s' % int_param
            return int_param

        def bigint_func(bigint_param):
            assert isinstance(bigint_param, int), 'bigint_param of wrong type %s !' \
                                                  % type(bigint_param)
            return bigint_param

        def bigint_func_none(bigint_param):
            assert bigint_param is None, 'bigint_param %s should be None!' % bigint_param
            return bigint_param

        def float_func(float_param):
            assert isinstance(float_param, float) and float_equal(float_param, 1.23, 1e-6), \
                'float_param is wrong value %s !' % float_param
            return float_param

        def double_func(double_param):
            assert isinstance(double_param, float) and float_equal(double_param, 1.98932, 1e-7), \
                'double_param is wrong value %s !' % double_param
            return double_param

        def bytes_func(bytes_param):
            assert bytes_param == b'flink', \
                'bytes_param is wrong value %s !' % bytes_param
            return bytes_param

        def str_func(str_param):
            assert str_param == 'pyflink', \
                'str_param is wrong value %s !' % str_param
            return str_param

        def date_func(date_param):
            from datetime import date
            assert date_param == date(year=2014, month=9, day=13), \
                'date_param is wrong value %s !' % date_param
            return date_param

        def time_func(time_param):
            from datetime import time
            assert time_param == time(hour=12, minute=0, second=0, microsecond=123000), \
                'time_param is wrong value %s !' % time_param
            return time_param

        def timestamp_func(timestamp_param):
            from datetime import datetime
            assert timestamp_param == datetime(2018, 3, 11, 3, 0, 0, 123000), \
                'timestamp_param is wrong value %s !' % timestamp_param
            return timestamp_param

        def array_func(array_param):
            assert array_param == [[1, 2, 3]] or array_param == ((1, 2, 3),), \
                'array_param is wrong value %s !' % array_param
            return array_param[0]

        def map_func(map_param):
            assert map_param == {1: 'flink', 2: 'pyflink'}, \
                'map_param is wrong value %s !' % map_param
            return map_param

        def decimal_func(decimal_param):
            from decimal import Decimal
            assert decimal_param == Decimal('1000000000000000000.050000000000000000'), \
                'decimal_param is wrong value %s !' % decimal_param
            return decimal_param

        def decimal_cut_func(decimal_param):
            from decimal import Decimal
            assert decimal_param == Decimal('1000000000000000000.059999999999999999'), \
                'decimal_param is wrong value %s !' % decimal_param
            return decimal_param

        self.t_env.create_temporary_system_function(
            "boolean_func", udf(boolean_func, result_type=DataTypes.BOOLEAN()))

        self.t_env.create_temporary_system_function(
            "tinyint_func", udf(tinyint_func, result_type=DataTypes.TINYINT()))

        self.t_env.create_temporary_system_function(
            "smallint_func", udf(smallint_func, result_type=DataTypes.SMALLINT()))

        self.t_env.create_temporary_system_function(
            "int_func", udf(int_func, result_type=DataTypes.INT()))

        self.t_env.create_temporary_system_function(
            "bigint_func", udf(bigint_func, result_type=DataTypes.BIGINT()))

        self.t_env.create_temporary_system_function(
            "bigint_func_none", udf(bigint_func_none, result_type=DataTypes.BIGINT()))

        self.t_env.create_temporary_system_function(
            "float_func", udf(float_func, result_type=DataTypes.FLOAT()))

        self.t_env.create_temporary_system_function(
            "double_func", udf(double_func, result_type=DataTypes.DOUBLE()))

        self.t_env.create_temporary_system_function(
            "bytes_func", udf(bytes_func, result_type=DataTypes.BYTES()))

        self.t_env.create_temporary_system_function(
            "str_func", udf(str_func, result_type=DataTypes.STRING()))

        self.t_env.create_temporary_system_function(
            "date_func", udf(date_func, result_type=DataTypes.DATE()))

        self.t_env.create_temporary_system_function(
            "time_func", udf(time_func, result_type=DataTypes.TIME()))

        self.t_env.create_temporary_system_function(
            "timestamp_func", udf(timestamp_func, result_type=DataTypes.TIMESTAMP(3)))

        self.t_env.create_temporary_system_function(
            "array_func", udf(array_func, result_type=DataTypes.ARRAY(DataTypes.BIGINT())))

        self.t_env.create_temporary_system_function(
            "map_func", udf(map_func,
                            result_type=DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING())))

        self.t_env.create_temporary_system_function(
            "decimal_func", udf(decimal_func, result_type=DataTypes.DECIMAL(38, 18)))

        self.t_env.create_temporary_system_function(
            "decimal_cut_func", udf(decimal_cut_func, result_type=DataTypes.DECIMAL(38, 18)))

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
            CREATE TABLE {sink_table}(
            a BIGINT, b BIGINT, c TINYINT, d BOOLEAN, e SMALLINT, f INT, g FLOAT, h DOUBLE,
            i BYTES, j STRING, k DATE, l TIME, m TIMESTAMP(3), n ARRAY<BIGINT>,
            o MAP<BIGINT, STRING>, p DECIMAL(38, 18), q DECIMAL(38, 18))
            WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)

        import datetime
        import decimal
        t = self.t_env.from_elements(
            [(1, None, 1, True, 32767, -2147483648, 1.23, 1.98932,
              bytearray(b'flink'), 'pyflink', datetime.date(2014, 9, 13),
              datetime.time(hour=12, minute=0, second=0, microsecond=123000),
              datetime.datetime(2018, 3, 11, 3, 0, 0, 123000), [[1, 2, 3]],
              {1: 'flink', 2: 'pyflink'}, decimal.Decimal('1000000000000000000.05'),
              decimal.Decimal('1000000000000000000.05999999999999999899999999999'))],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.BIGINT()),
                 DataTypes.FIELD("b", DataTypes.BIGINT()),
                 DataTypes.FIELD("c", DataTypes.TINYINT()),
                 DataTypes.FIELD("d", DataTypes.BOOLEAN()),
                 DataTypes.FIELD("e", DataTypes.SMALLINT()),
                 DataTypes.FIELD("f", DataTypes.INT()),
                 DataTypes.FIELD("g", DataTypes.FLOAT()),
                 DataTypes.FIELD("h", DataTypes.DOUBLE()),
                 DataTypes.FIELD("i", DataTypes.BYTES()),
                 DataTypes.FIELD("j", DataTypes.STRING()),
                 DataTypes.FIELD("k", DataTypes.DATE()),
                 DataTypes.FIELD("l", DataTypes.TIME()),
                 DataTypes.FIELD("m", DataTypes.TIMESTAMP(3)),
                 DataTypes.FIELD("n", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.BIGINT()))),
                 DataTypes.FIELD("o", DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING())),
                 DataTypes.FIELD("p", DataTypes.DECIMAL(38, 18)),
                 DataTypes.FIELD("q", DataTypes.DECIMAL(38, 18))]))

        t.select(call("bigint_func", t.a), call("bigint_func_none", t.b),
                 call("tinyint_func", t.c), call("boolean_func", t.d),
                 call("smallint_func", t.e), call("int_func", t.f),
                 call("float_func", t.g), call("double_func", t.h),
                 call("bytes_func", t.i), call("str_func", t.j),
                 call("date_func", t.k), call("time_func", t.l),
                 call("timestamp_func", t.m), call("array_func", t.n),
                 call("map_func", t.o), call("decimal_func", t.p),
                 call("decimal_cut_func", t.q)) \
            .execute_insert(sink_table).wait()
        actual = source_sink_utils.results()
        # Currently the sink result precision of DataTypes.TIME(precision) only supports 0.
        self.assert_equals(actual,
                           ["+I[1, null, 1, true, 32767, -2147483648, 1.23, 1.98932, "
                            "[102, 108, 105, 110, 107], pyflink, 2014-09-13, "
                            "12:00:00.123, 2018-03-11T03:00:00.123, [1, 2, 3], "
                            "{1=flink, 2=pyflink}, 1000000000000000000.050000000000000000, "
                            "1000000000000000000.059999999999999999]"])

    def test_create_and_drop_function(self):
        t_env = self.t_env

        t_env.create_temporary_system_function(
            "add_one_func", udf(lambda i: i + 1, result_type=DataTypes.BIGINT()))
        t_env.create_temporary_function(
            "subtract_one_func", udf(SubtractOne(), result_type=DataTypes.BIGINT()))
        self.assertTrue('add_one_func' in t_env.list_user_defined_functions())
        self.assertTrue('subtract_one_func' in t_env.list_user_defined_functions())

        t_env.drop_temporary_system_function("add_one_func")
        t_env.drop_temporary_function("subtract_one_func")
        self.assertTrue('add_one_func' not in t_env.list_user_defined_functions())
        self.assertTrue('subtract_one_func' not in t_env.list_user_defined_functions())


# decide whether two floats are equal
def float_equal(a, b, rel_tol=1e-09, abs_tol=0.0):
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


class PyFlinkStreamUserDefinedFunctionTests(UserDefinedFunctionTests,
                                            PyFlinkStreamTableTestCase):
    def test_deterministic(self):
        add_one = udf(lambda i: i + 1, result_type=DataTypes.BIGINT())
        self.assertTrue(add_one._deterministic)

        add_one = udf(lambda i: i + 1, result_type=DataTypes.BIGINT(), deterministic=False)
        self.assertFalse(add_one._deterministic)

        subtract_one = udf(SubtractOne(), result_type=DataTypes.BIGINT())
        self.assertTrue(subtract_one._deterministic)

        with self.assertRaises(ValueError, msg="Inconsistent deterministic: False and True"):
            udf(SubtractOne(), result_type=DataTypes.BIGINT(), deterministic=False)

        self.assertTrue(add._deterministic)

        @udf(result_type=DataTypes.BIGINT(), deterministic=False)
        def non_deterministic_udf(i):
            return i

        self.assertFalse(non_deterministic_udf._deterministic)

    def test_name(self):
        add_one = udf(lambda i: i + 1, result_type=DataTypes.BIGINT())
        self.assertEqual("<lambda>", add_one._name)

        add_one = udf(lambda i: i + 1, result_type=DataTypes.BIGINT(), name="add_one")
        self.assertEqual("add_one", add_one._name)

        subtract_one = udf(SubtractOne(), result_type=DataTypes.BIGINT())
        self.assertEqual("SubtractOne", subtract_one._name)

        subtract_one = udf(SubtractOne(), result_type=DataTypes.BIGINT(), name="subtract_one")
        self.assertEqual("subtract_one", subtract_one._name)

        self.assertEqual("add", add._name)

        @udf(result_type=DataTypes.BIGINT(), name="named")
        def named_udf(i):
            return i

        self.assertEqual("named", named_udf._name)

    def test_abc(self):
        class UdfWithoutEval(ScalarFunction):
            def open(self, function_context):
                pass

        with self.assertRaises(
                TypeError,
                msg="Can't instantiate abstract class UdfWithoutEval with abstract methods eval"):
            UdfWithoutEval()

    def test_invalid_udf(self):
        class Plus(object):
            def eval(self, col):
                return col + 1

        with self.assertRaises(
                TypeError,
                msg="Invalid function: not a function or callable (__call__ is not defined)"):
            # test non-callable function
            self.t_env.create_temporary_system_function(
                "non-callable-udf", udf(Plus(), DataTypes.BIGINT(), DataTypes.BIGINT()))

    def test_data_types(self):
        timezone = self.t_env.get_config().get_local_timezone()
        local_datetime = pytz.timezone(timezone).localize(
            datetime.datetime(1970, 1, 1, 0, 0, 0, 123000))

        @udf(result_type=DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
        def local_zoned_timestamp_func(local_zoned_timestamp_param):
            assert local_zoned_timestamp_param == local_datetime, \
                'local_zoned_timestamp_param is wrong value %s !' % local_zoned_timestamp_param
            return local_zoned_timestamp_param

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
        CREATE TABLE {sink_table}(a TIMESTAMP_LTZ(3)) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)

        t = self.t_env.from_elements(
            [(local_datetime,)],
            DataTypes.ROW([DataTypes.FIELD("a", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))]))

        t.select(local_zoned_timestamp_func(local_zoned_timestamp_func(t.a))) \
            .execute_insert(sink_table) \
            .wait()
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[1970-01-01T00:00:00.123Z]"])

    def test_execute_from_json_plan(self):
        # create source file path
        tmp_dir = self.tempdir
        data = ['1,1', '3,3', '2,2']
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
                id BIGINT,
                data BIGINT
            ) WITH (
                'connector' = 'filesystem',
                'path' = '%s',
                'format' = 'csv'
            )
        """ % sink_path)

        add_one = udf(lambda i: i + 1, result_type=DataTypes.BIGINT())
        self.t_env.create_temporary_system_function("add_one", add_one)

        json_plan = self.t_env._j_tenv.compilePlanSql("INSERT INTO sink_table SELECT "
                                                      "a, "
                                                      "add_one(b) "
                                                      "FROM source_table")
        from py4j.java_gateway import get_method
        get_method(json_plan.execute(), "await")()

        import glob
        lines = [line.strip() for file in glob.glob(sink_path + '/*') for line in open(file, 'r')]
        lines.sort()
        self.assertEqual(lines, ['1,2', '2,3', '3,4'])

    def test_udf_with_rowtime_arguments(self):
        from pyflink.common import WatermarkStrategy
        from pyflink.common.typeinfo import Types
        from pyflink.common.watermark_strategy import TimestampAssigner
        from pyflink.table import Schema

        class MyTimestampAssigner(TimestampAssigner):

            def extract_timestamp(self, value, record_timestamp) -> int:
                return int(value[0])

        ds = self.env.from_collection(
            [(1, 42, "a"), (2, 5, "a"), (3, 1000, "c"), (100, 1000, "c")],
            Types.ROW_NAMED(["a", "b", "c"], [Types.LONG(), Types.INT(), Types.STRING()]))

        ds = ds.assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps()
            .with_timestamp_assigner(MyTimestampAssigner()))

        table = self.t_env.from_data_stream(
            ds,
            Schema.new_builder()
                  .column_by_metadata("rowtime", "TIMESTAMP_LTZ(3)")
                  .watermark("rowtime", "SOURCE_WATERMARK()")
                  .build())

        @udf(result_type=DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.INT())]))
        def inc(input_row):
            return Row(input_row.b)

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
                    CREATE TABLE {sink_table}(
                        a INT
                    ) WITH ('connector'='test-sink')
                """
        self.t_env.execute_sql(sink_table_ddl)
        table.map(inc).execute_insert(sink_table).wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual, ['+I[42]', '+I[5]', '+I[1000]', '+I[1000]'])


class PyFlinkBatchUserDefinedFunctionTests(UserDefinedFunctionTests,
                                           PyFlinkBatchTableTestCase):
    pass


class PyFlinkEmbeddedThreadTests(UserDefinedFunctionTests, PyFlinkBatchTableTestCase):
    def setUp(self):
        super(PyFlinkEmbeddedThreadTests, self).setUp()
        self.t_env.get_config().set("python.execution-mode", "thread")

    def test_all_data_types_string(self):
        @udf(result_type='BOOLEAN')
        def boolean_func(bool_param):
            assert isinstance(bool_param, bool), 'bool_param of wrong type %s !' \
                                                 % type(bool_param)
            return bool_param

        @udf(result_type='TINYINT')
        def tinyint_func(tinyint_param):
            assert isinstance(tinyint_param, int), 'tinyint_param of wrong type %s !' \
                                                   % type(tinyint_param)
            return tinyint_param

        @udf(result_type='SMALLINT')
        def smallint_func(smallint_param):
            assert isinstance(smallint_param, int), 'smallint_param of wrong type %s !' \
                                                    % type(smallint_param)
            assert smallint_param == 32767, 'smallint_param of wrong value %s' % smallint_param
            return smallint_param

        @udf(result_type='INT')
        def int_func(int_param):
            assert isinstance(int_param, int), 'int_param of wrong type %s !' \
                                               % type(int_param)
            assert int_param == -2147483648, 'int_param of wrong value %s' % int_param
            return int_param

        @udf(result_type='BIGINT')
        def bigint_func(bigint_param):
            assert isinstance(bigint_param, int), 'bigint_param of wrong type %s !' \
                                                  % type(bigint_param)
            return bigint_param

        @udf(result_type='BIGINT')
        def bigint_func_none(bigint_param):
            assert bigint_param is None, 'bigint_param %s should be None!' % bigint_param
            return bigint_param

        @udf(result_type='FLOAT')
        def float_func(float_param):
            assert isinstance(float_param, float) and float_equal(float_param, 1.23, 1e-6), \
                'float_param is wrong value %s !' % float_param
            return float_param

        @udf(result_type='DOUBLE')
        def double_func(double_param):
            assert isinstance(double_param, float) and float_equal(double_param, 1.98932, 1e-7), \
                'double_param is wrong value %s !' % double_param
            return double_param

        @udf(result_type='BYTES')
        def bytes_func(bytes_param):
            assert bytes_param == b'flink', \
                'bytes_param is wrong value %s !' % bytes_param
            return bytes_param

        @udf(result_type='STRING')
        def str_func(str_param):
            assert str_param == 'pyflink', \
                'str_param is wrong value %s !' % str_param
            return str_param

        @udf(result_type='DATE')
        def date_func(date_param):
            from datetime import date
            assert date_param == date(year=2014, month=9, day=13), \
                'date_param is wrong value %s !' % date_param
            return date_param

        @udf(result_type='TIME')
        def time_func(time_param):
            from datetime import time
            assert time_param == time(hour=12, minute=0, second=0, microsecond=123000), \
                'time_param is wrong value %s !' % time_param
            return time_param

        @udf(result_type='TIMESTAMP(3)')
        def timestamp_func(timestamp_param):
            from datetime import datetime
            assert timestamp_param == datetime(2018, 3, 11, 3, 0, 0, 123000), \
                'timestamp_param is wrong value %s !' % timestamp_param
            return timestamp_param

        @udf(result_type='ARRAY<BIGINT>')
        def array_func(array_param):
            assert array_param == [[1, 2, 3]] or array_param == ((1, 2, 3),), \
                'array_param is wrong value %s !' % array_param
            return array_param[0]

        @udf(result_type='MAP<BIGINT, STRING>')
        def map_func(map_param):
            assert map_param == {1: 'flink', 2: 'pyflink'}, \
                'map_param is wrong value %s !' % map_param
            return map_param

        @udf(result_type='DECIMAL(38, 18)')
        def decimal_func(decimal_param):
            from decimal import Decimal
            assert decimal_param == Decimal('1000000000000000000.050000000000000000'), \
                'decimal_param is wrong value %s !' % decimal_param
            return decimal_param

        @udf(result_type='DECIMAL(38, 18)')
        def decimal_cut_func(decimal_param):
            from decimal import Decimal
            assert decimal_param == Decimal('1000000000000000000.059999999999999999'), \
                'decimal_param is wrong value %s !' % decimal_param
            return decimal_param

        sink_table = generate_random_table_name()
        sink_table_ddl = f"""
            CREATE TABLE {sink_table}(
            a BIGINT, b BIGINT, c TINYINT, d BOOLEAN, e SMALLINT, f INT, g FLOAT, h DOUBLE, i BYTES,
            j STRING, k DATE, l TIME, m TIMESTAMP(3), n ARRAY<BIGINT>, o MAP<BIGINT, STRING>,
            p DECIMAL(38, 18), q DECIMAL(38, 18)) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)

        import datetime
        import decimal
        t = self.t_env.from_elements(
            [(1, None, 1, True, 32767, -2147483648, 1.23, 1.98932,
              bytearray(b'flink'), 'pyflink', datetime.date(2014, 9, 13),
              datetime.time(hour=12, minute=0, second=0, microsecond=123000),
              datetime.datetime(2018, 3, 11, 3, 0, 0, 123000), [[1, 2, 3]],
              {1: 'flink', 2: 'pyflink'}, decimal.Decimal('1000000000000000000.05'),
              decimal.Decimal('1000000000000000000.05999999999999999899999999999'))],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.BIGINT()),
                 DataTypes.FIELD("b", DataTypes.BIGINT()),
                 DataTypes.FIELD("c", DataTypes.TINYINT()),
                 DataTypes.FIELD("d", DataTypes.BOOLEAN()),
                 DataTypes.FIELD("e", DataTypes.SMALLINT()),
                 DataTypes.FIELD("f", DataTypes.INT()),
                 DataTypes.FIELD("g", DataTypes.FLOAT()),
                 DataTypes.FIELD("h", DataTypes.DOUBLE()),
                 DataTypes.FIELD("i", DataTypes.BYTES()),
                 DataTypes.FIELD("j", DataTypes.STRING()),
                 DataTypes.FIELD("k", DataTypes.DATE()),
                 DataTypes.FIELD("l", DataTypes.TIME()),
                 DataTypes.FIELD("m", DataTypes.TIMESTAMP(3)),
                 DataTypes.FIELD("n", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.BIGINT()))),
                 DataTypes.FIELD("o", DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING())),
                 DataTypes.FIELD("p", DataTypes.DECIMAL(38, 18)),
                 DataTypes.FIELD("q", DataTypes.DECIMAL(38, 18))]))

        t.select(
            bigint_func(t.a),
            bigint_func_none(t.b),
            tinyint_func(t.c),
            boolean_func(t.d),
            smallint_func(t.e),
            int_func(t.f),
            float_func(t.g),
            double_func(t.h),
            bytes_func(t.i),
            str_func(t.j),
            date_func(t.k),
            time_func(t.l),
            timestamp_func(t.m),
            array_func(t.n),
            map_func(t.o),
            decimal_func(t.p),
            decimal_cut_func(t.q)) \
            .execute_insert(sink_table).wait()
        actual = source_sink_utils.results()
        # Currently the sink result precision of DataTypes.TIME(precision) only supports 0.
        self.assert_equals(actual,
                           ["+I[1, null, 1, true, 32767, -2147483648, 1.23, 1.98932, "
                            "[102, 108, 105, 110, 107], pyflink, 2014-09-13, 12:00:00.123, "
                            "2018-03-11T03:00:00.123, [1, 2, 3], "
                            "{1=flink, 2=pyflink}, 1000000000000000000.050000000000000000, "
                            "1000000000000000000.059999999999999999]"])


# test specify the input_types
@udf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_type=DataTypes.BIGINT())
def add(i, j):
    return i + j


class SubtractOne(ScalarFunction):

    def eval(self, i):
        return i - 1


class SubtractWithParameters(ScalarFunction):

    def open(self, function_context: FunctionContext):
        self.subtract_value = int(function_context.get_job_parameter("subtract_value", "1"))

    def eval(self, i):
        return i - self.subtract_value


class SubtractWithMetrics(ScalarFunction, unittest.TestCase):

    def open(self, function_context):
        self.subtracted_value = 1
        mg = function_context.get_metric_group()
        self.counter = mg.add_group("key", "value").counter("my_counter")
        self.counter_sum = 0

    def eval(self, i):
        # counter
        self.counter.inc(i)
        self.counter_sum += i
        return i - self.subtracted_value


class Subtract(ScalarFunction, unittest.TestCase):

    def open(self, function_context):
        self.subtracted_value = 1
        self.counter_sum = 0

    def eval(self, i):
        # counter
        self.counter_sum += i
        return i - self.subtracted_value


class CallablePlus(object):

    def __call__(self, col):
        return col + 1


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
