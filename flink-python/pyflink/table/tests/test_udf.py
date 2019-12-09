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
from pyflink.table import DataTypes
from pyflink.table.udf import ScalarFunction, udf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, \
    PyFlinkBlinkStreamTableTestCase, PyFlinkBlinkBatchTableTestCase


class UserDefinedFunctionTests(object):

    def test_scalar_function(self):
        # test lambda function
        self.t_env.register_function(
            "add_one", udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT()))

        # test Python ScalarFunction
        self.t_env.register_function(
            "subtract_one", udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT()))

        # test Python function
        self.t_env.register_function("add", add)

        # test callable function
        self.t_env.register_function(
            "add_one_callable", udf(CallablePlus(), DataTypes.BIGINT(), DataTypes.BIGINT()))

        def partial_func(col, param):
            return col + param

        # test partial function
        import functools
        self.t_env.register_function(
            "add_one_partial",
            udf(functools.partial(partial_func, param=1), DataTypes.BIGINT(), DataTypes.BIGINT()))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c', 'd', 'e', 'f'],
            [DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(),
             DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1, 2, 3), (2, 5, 6), (3, 1, 9)], ['a', 'b', 'c'])
        t.where("add_one(b) <= 3") \
            .select("add_one(a), subtract_one(b), add(a, c), add_one_callable(a), "
                    "add_one_partial(a), a") \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["2,1,4,2,2,1", "4,0,12,4,4,3"])

    def test_chaining_scalar_function(self):
        self.t_env.register_function(
            "add_one", udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT()))
        self.t_env.register_function(
            "subtract_one", udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT()))
        self.t_env.register_function("add", add)

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c'],
            [DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.INT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1, 2, 1), (2, 5, 2), (3, 1, 3)], ['a', 'b', 'c'])
        t.select("add(add_one(a), subtract_one(b)), c, 1") \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["3,1,1", "7,2,1", "4,3,1"])

    def test_udf_in_join_condition(self):
        t1 = self.t_env.from_elements([(2, "Hi")], ['a', 'b'])
        t2 = self.t_env.from_elements([(2, "Flink")], ['c', 'd'])

        self.t_env.register_function("f", udf(lambda i: i, DataTypes.BIGINT(), DataTypes.BIGINT()))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c', 'd'],
            [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.STRING()])
        self.t_env.register_table_sink("Results", table_sink)

        t1.join(t2).where("f(a) = c").insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["2,Hi,2,Flink"])

    def test_udf_in_join_condition_2(self):
        t1 = self.t_env.from_elements([(1, "Hi"), (2, "Hi")], ['a', 'b'])
        t2 = self.t_env.from_elements([(2, "Flink")], ['c', 'd'])

        self.t_env.register_function("f", udf(lambda i: i, DataTypes.BIGINT(), DataTypes.BIGINT()))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c', 'd'],
            [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.STRING()])
        self.t_env.register_table_sink("Results", table_sink)

        t1.join(t2).where("f(a) = f(c)").insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["2,Hi,2,Flink"])

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

        self.t_env.register_function("udf_with_constant_params",
                                     udf(udf_with_constant_params,
                                         input_types=[DataTypes.BIGINT(),
                                                      DataTypes.BIGINT(),
                                                      DataTypes.TINYINT(),
                                                      DataTypes.SMALLINT(),
                                                      DataTypes.INT(),
                                                      DataTypes.BIGINT(),
                                                      DataTypes.DECIMAL(20, 10),
                                                      DataTypes.FLOAT(),
                                                      DataTypes.DOUBLE(),
                                                      DataTypes.BOOLEAN(),
                                                      DataTypes.STRING(),
                                                      DataTypes.DATE(),
                                                      DataTypes.TIME(),
                                                      DataTypes.TIMESTAMP()],
                                         result_type=DataTypes.BIGINT()))

        self.t_env.register_function(
            "udf_with_all_constant_params", udf(lambda i, j: i + j,
                                                [DataTypes.BIGINT(), DataTypes.BIGINT()],
                                                DataTypes.BIGINT()))

        table_sink = source_sink_utils.TestAppendSink(['a', 'b'],
                                                      [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1, 2, 3), (2, 5, 6), (3, 1, 9)], ['a', 'b', 'c'])
        self.t_env.register_table("test_table", t)
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
                             " from test_table").insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["3,8", "3,9", "3,10"])

    def test_overwrite_builtin_function(self):
        self.t_env.register_function(
            "plus", udf(lambda i, j: i + j - 1,
                        [DataTypes.BIGINT(), DataTypes.BIGINT()], DataTypes.BIGINT()))

        table_sink = source_sink_utils.TestAppendSink(['a'], [DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1, 2, 3), (2, 5, 6), (3, 1, 9)], ['a', 'b', 'c'])
        t.select("plus(a, b)").insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["2", "6", "3"])

    def test_open(self):
        self.t_env.register_function(
            "subtract", udf(Subtract(), DataTypes.BIGINT(), DataTypes.BIGINT()))
        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'], [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 4)], ['a', 'b'])
        t.select("a, subtract(b)").insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["1,1", "2,4", "3,3"])

    def test_udf_without_arguments(self):
        self.t_env.register_function("one", udf(
            lambda: 1, input_types=[], result_type=DataTypes.BIGINT(), deterministic=True))
        self.t_env.register_function("two", udf(
            lambda: 2, input_types=[], result_type=DataTypes.BIGINT(), deterministic=False))

        table_sink = source_sink_utils.TestAppendSink(['a', 'b'],
                                                      [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select("one(), two()").insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["1,2", "1,2", "1,2"])

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
            assert array_param == [[1, 2, 3]], \
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

        self.t_env.register_function(
            "boolean_func", udf(boolean_func, [DataTypes.BOOLEAN()], DataTypes.BOOLEAN()))

        self.t_env.register_function(
            "tinyint_func", udf(tinyint_func, [DataTypes.TINYINT()], DataTypes.TINYINT()))

        self.t_env.register_function(
            "smallint_func", udf(smallint_func, [DataTypes.SMALLINT()], DataTypes.SMALLINT()))

        self.t_env.register_function(
            "int_func", udf(int_func, [DataTypes.INT()], DataTypes.INT()))

        self.t_env.register_function(
            "bigint_func", udf(bigint_func, [DataTypes.BIGINT()], DataTypes.BIGINT()))

        self.t_env.register_function(
            "bigint_func_none", udf(bigint_func_none, [DataTypes.BIGINT()], DataTypes.BIGINT()))

        self.t_env.register_function(
            "float_func", udf(float_func, [DataTypes.FLOAT()], DataTypes.FLOAT()))

        self.t_env.register_function(
            "double_func", udf(double_func, [DataTypes.DOUBLE()], DataTypes.DOUBLE()))

        self.t_env.register_function(
            "bytes_func", udf(bytes_func, [DataTypes.BYTES()], DataTypes.BYTES()))

        self.t_env.register_function(
            "str_func", udf(str_func, [DataTypes.STRING()], DataTypes.STRING()))

        self.t_env.register_function(
            "date_func", udf(date_func, [DataTypes.DATE()], DataTypes.DATE()))

        self.t_env.register_function(
            "time_func", udf(time_func, [DataTypes.TIME(3)], DataTypes.TIME(3)))

        self.t_env.register_function(
            "timestamp_func", udf(timestamp_func, [DataTypes.TIMESTAMP()], DataTypes.TIMESTAMP()))

        self.t_env.register_function(
            "array_func", udf(array_func, [DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.BIGINT()))],
                              DataTypes.ARRAY(DataTypes.BIGINT())))

        self.t_env.register_function(
            "map_func", udf(map_func, [DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING())],
                            DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING())))

        self.t_env.register_function(
            "decimal_func", udf(decimal_func, [DataTypes.DECIMAL(38, 18)],
                                DataTypes.DECIMAL(38, 18)))

        self.t_env.register_function(
            "decimal_cut_func", udf(decimal_cut_func, [DataTypes.DECIMAL(38, 18)],
                                    DataTypes.DECIMAL(38, 18)))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q'],
            [DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.TINYINT(),
             DataTypes.BOOLEAN(), DataTypes.SMALLINT(), DataTypes.INT(),
             DataTypes.FLOAT(), DataTypes.DOUBLE(), DataTypes.BYTES(),
             DataTypes.STRING(), DataTypes.DATE(), DataTypes.TIME(3),
             DataTypes.TIMESTAMP(), DataTypes.ARRAY(DataTypes.BIGINT()),
             DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()),
             DataTypes.DECIMAL(38, 18), DataTypes.DECIMAL(38, 18)])
        self.t_env.register_table_sink("Results", table_sink)

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
                 DataTypes.FIELD("l", DataTypes.TIME(3)),
                 DataTypes.FIELD("m", DataTypes.TIMESTAMP()),
                 DataTypes.FIELD("n", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.BIGINT()))),
                 DataTypes.FIELD("o", DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING())),
                 DataTypes.FIELD("p", DataTypes.DECIMAL(38, 18)),
                 DataTypes.FIELD("q", DataTypes.DECIMAL(38, 18))]))

        t.select("bigint_func(a), bigint_func_none(b),"
                 "tinyint_func(c), boolean_func(d),"
                 "smallint_func(e),int_func(f),"
                 "float_func(g),double_func(h),"
                 "bytes_func(i),str_func(j),"
                 "date_func(k),time_func(l),"
                 "timestamp_func(m),array_func(n),"
                 "map_func(o),decimal_func(p),"
                 "decimal_cut_func(q)") \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        # Currently the sink result precision of DataTypes.TIME(precision) only supports 0.
        self.assert_equals(actual,
                           ["1,null,1,true,32767,-2147483648,1.23,1.98932,"
                            "[102, 108, 105, 110, 107],pyflink,2014-09-13,"
                            "12:00:00,2018-03-11 03:00:00.123,[1, 2, 3],"
                            "{1=flink, 2=pyflink},1000000000000000000.050000000000000000,"
                            "1000000000000000000.059999999999999999"])


# decide whether two floats are equal
def float_equal(a, b, rel_tol=1e-09, abs_tol=0.0):
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


class PyFlinkStreamUserDefinedFunctionTests(UserDefinedFunctionTests,
                                            PyFlinkStreamTableTestCase):
    pass


class PyFlinkBlinkStreamUserDefinedFunctionTests(UserDefinedFunctionTests,
                                                 PyFlinkBlinkStreamTableTestCase):
    def test_deterministic(self):
        add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT())
        self.assertTrue(add_one._deterministic)

        add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT(), deterministic=False)
        self.assertFalse(add_one._deterministic)

        subtract_one = udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT())
        self.assertTrue(subtract_one._deterministic)

        with self.assertRaises(ValueError, msg="Inconsistent deterministic: False and True"):
            udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT(), deterministic=False)

        self.assertTrue(add._deterministic)

        @udf(input_types=DataTypes.BIGINT(), result_type=DataTypes.BIGINT(), deterministic=False)
        def non_deterministic_udf(i):
            return i

        self.assertFalse(non_deterministic_udf._deterministic)

    def test_name(self):
        add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT())
        self.assertEqual("<lambda>", add_one._name)

        add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT(), name="add_one")
        self.assertEqual("add_one", add_one._name)

        subtract_one = udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT())
        self.assertEqual("SubtractOne", subtract_one._name)

        subtract_one = udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT(),
                           name="subtract_one")
        self.assertEqual("subtract_one", subtract_one._name)

        self.assertEqual("add", add._name)

        @udf(input_types=DataTypes.BIGINT(), result_type=DataTypes.BIGINT(), name="named")
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
            self.t_env.register_function(
                "non-callable-udf", udf(Plus(), DataTypes.BIGINT(), DataTypes.BIGINT()))


class PyFlinkBlinkBatchUserDefinedFunctionTests(UserDefinedFunctionTests,
                                                PyFlinkBlinkBatchTableTestCase):
    pass


@udf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_type=DataTypes.BIGINT())
def add(i, j):
    return i + j


class SubtractOne(ScalarFunction):

    def eval(self, i):
        return i - 1


class Subtract(ScalarFunction):

    def __init__(self):
        self.subtracted_value = 0

    def open(self, function_context):
        self.subtracted_value = 1

    def eval(self, i):
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
