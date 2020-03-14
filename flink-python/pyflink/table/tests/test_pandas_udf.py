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
import decimal
import unittest

from pyflink.table import DataTypes
from pyflink.table.tests.test_udf import SubtractOne
from pyflink.table.udf import udf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, \
    PyFlinkBlinkBatchTableTestCase, PyFlinkBlinkStreamTableTestCase


class PandasUDFTests(unittest.TestCase):

    def test_non_exist_udf_type(self):
        with self.assertRaisesRegex(ValueError,
                                    'The udf_type must be one of \'general, pandas\''):
            udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT(), udf_type="non-exist")


class PandasUDFITTests(object):

    def test_basic_functionality(self):
        # pandas UDF
        self.t_env.register_function(
            "add_one",
            udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT(), udf_type="pandas"))

        self.t_env.register_function("add", add)

        # general Python UDF
        self.t_env.register_function(
            "subtract_one", udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT()))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c', 'd'],
            [DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1, 2, 3), (2, 5, 6), (3, 1, 9)], ['a', 'b', 'c'])
        t.where("add_one(b) <= 3") \
            .select("a, b + 1, add(a + 1, subtract_one(c)) + 2, add(add_one(a), 1L)") \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["1,3,6,3", "3,2,14,5"])

    def test_all_data_types(self):
        import pandas as pd
        import numpy as np

        def tinyint_func(tinyint_param):
            assert isinstance(tinyint_param, pd.Series)
            assert isinstance(tinyint_param[0], np.int8), \
                'tinyint_param of wrong type %s !' % type(tinyint_param[0])
            return tinyint_param

        def smallint_func(smallint_param):
            assert isinstance(smallint_param, pd.Series)
            assert isinstance(smallint_param[0], np.int16), \
                'smallint_param of wrong type %s !' % type(smallint_param[0])
            assert smallint_param[0] == 32767, 'smallint_param of wrong value %s' % smallint_param
            return smallint_param

        def int_func(int_param):
            assert isinstance(int_param, pd.Series)
            assert isinstance(int_param[0], np.int32), \
                'int_param of wrong type %s !' % type(int_param[0])
            assert int_param[0] == -2147483648, 'int_param of wrong value %s' % int_param
            return int_param

        def bigint_func(bigint_param):
            assert isinstance(bigint_param, pd.Series)
            assert isinstance(bigint_param[0], np.int64), \
                'bigint_param of wrong type %s !' % type(bigint_param[0])
            return bigint_param

        def boolean_func(boolean_param):
            assert isinstance(boolean_param, pd.Series)
            assert isinstance(boolean_param[0], np.bool_), \
                'boolean_param of wrong type %s !' % type(boolean_param[0])
            return boolean_param

        def float_func(float_param):
            assert isinstance(float_param, pd.Series)
            assert isinstance(float_param[0], np.float32), \
                'float_param of wrong type %s !' % type(float_param[0])
            return float_param

        def double_func(double_param):
            assert isinstance(double_param, pd.Series)
            assert isinstance(double_param[0], np.float64), \
                'double_param of wrong type %s !' % type(double_param[0])
            return double_param

        def varchar_func(varchar_param):
            assert isinstance(varchar_param, pd.Series)
            assert isinstance(varchar_param[0], str), \
                'varchar_param of wrong type %s !' % type(varchar_param[0])
            return varchar_param

        def varbinary_func(varbinary_param):
            assert isinstance(varbinary_param, pd.Series)
            assert isinstance(varbinary_param[0], bytes), \
                'varbinary_param of wrong type %s !' % type(varbinary_param[0])
            return varbinary_param

        def decimal_func(decimal_param):
            assert isinstance(decimal_param, pd.Series)
            assert isinstance(decimal_param[0], decimal.Decimal), \
                'decimal_param of wrong type %s !' % type(decimal_param[0])
            return decimal_param

        def date_func(date_param):
            assert isinstance(date_param, pd.Series)
            assert isinstance(date_param[0], datetime.date), \
                'date_param of wrong type %s !' % type(date_param[0])
            return date_param

        def time_func(time_param):
            assert isinstance(time_param, pd.Series)
            assert isinstance(time_param[0], datetime.time), \
                'time_param of wrong type %s !' % type(time_param[0])
            return time_param

        self.t_env.register_function(
            "tinyint_func",
            udf(tinyint_func, [DataTypes.TINYINT()], DataTypes.TINYINT(), udf_type="pandas"))

        self.t_env.register_function(
            "smallint_func",
            udf(smallint_func, [DataTypes.SMALLINT()], DataTypes.SMALLINT(), udf_type="pandas"))

        self.t_env.register_function(
            "int_func",
            udf(int_func, [DataTypes.INT()], DataTypes.INT(), udf_type="pandas"))

        self.t_env.register_function(
            "bigint_func",
            udf(bigint_func, [DataTypes.BIGINT()], DataTypes.BIGINT(), udf_type="pandas"))

        self.t_env.register_function(
            "boolean_func",
            udf(boolean_func, [DataTypes.BOOLEAN()], DataTypes.BOOLEAN(), udf_type="pandas"))

        self.t_env.register_function(
            "float_func",
            udf(float_func, [DataTypes.FLOAT()], DataTypes.FLOAT(), udf_type="pandas"))

        self.t_env.register_function(
            "double_func",
            udf(double_func, [DataTypes.DOUBLE()], DataTypes.DOUBLE(), udf_type="pandas"))

        self.t_env.register_function(
            "varchar_func",
            udf(varchar_func, [DataTypes.STRING()], DataTypes.STRING(), udf_type="pandas"))

        self.t_env.register_function(
            "varbinary_func",
            udf(varbinary_func, [DataTypes.BYTES()], DataTypes.BYTES(), udf_type="pandas"))

        self.t_env.register_function(
            "decimal_func",
            udf(decimal_func, [DataTypes.DECIMAL(38, 18)], DataTypes.DECIMAL(38, 18),
                udf_type="pandas"))

        self.t_env.register_function(
            "date_func",
            udf(date_func, [DataTypes.DATE()], DataTypes.DATE(), udf_type="pandas"))

        self.t_env.register_function(
            "time_func",
            udf(time_func, [DataTypes.TIME()],   DataTypes.TIME(), udf_type="pandas"))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o'],
            [DataTypes.TINYINT(), DataTypes.SMALLINT(), DataTypes.INT(), DataTypes.BIGINT(),
             DataTypes.BOOLEAN(), DataTypes.BOOLEAN(), DataTypes.FLOAT(), DataTypes.DOUBLE(),
             DataTypes.STRING(), DataTypes.STRING(), DataTypes.BYTES(), DataTypes.DECIMAL(38, 18),
             DataTypes.DECIMAL(38, 18), DataTypes.DATE(), DataTypes.TIME()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements(
            [(1, 32767, -2147483648, 1, True, False, 1.0, 1.0, 'hello', '中文',
              bytearray(b'flink'), decimal.Decimal('1000000000000000000.05'),
              decimal.Decimal('1000000000000000000.05999999999999999899999999999'),
              datetime.date(2014, 9, 13), datetime.time(hour=1, minute=0, second=1))],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b", DataTypes.SMALLINT()),
                 DataTypes.FIELD("c", DataTypes.INT()),
                 DataTypes.FIELD("d", DataTypes.BIGINT()),
                 DataTypes.FIELD("e", DataTypes.BOOLEAN()),
                 DataTypes.FIELD("f", DataTypes.BOOLEAN()),
                 DataTypes.FIELD("g", DataTypes.FLOAT()),
                 DataTypes.FIELD("h", DataTypes.DOUBLE()),
                 DataTypes.FIELD("i", DataTypes.STRING()),
                 DataTypes.FIELD("j", DataTypes.STRING()),
                 DataTypes.FIELD("k", DataTypes.BYTES()),
                 DataTypes.FIELD("l", DataTypes.DECIMAL(38, 18)),
                 DataTypes.FIELD("m", DataTypes.DECIMAL(38, 18)),
                 DataTypes.FIELD("n", DataTypes.DATE()),
                 DataTypes.FIELD("o", DataTypes.TIME())]))

        t.select("tinyint_func(a),"
                 "smallint_func(b),"
                 "int_func(c),"
                 "bigint_func(d),"
                 "boolean_func(e),"
                 "boolean_func(f),"
                 "float_func(g),"
                 "double_func(h),"
                 "varchar_func(i),"
                 "varchar_func(j),"
                 "varbinary_func(k),"
                 "decimal_func(l),"
                 "decimal_func(m),"
                 "date_func(n),"
                 "time_func(o)") \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual,
                           ["1,32767,-2147483648,1,true,false,1.0,1.0,hello,中文,"
                            "[102, 108, 105, 110, 107],1000000000000000000.050000000000000000,"
                            "1000000000000000000.059999999999999999,2014-09-13,01:00:01"])


class StreamPandasUDFITTests(PandasUDFITTests,
                             PyFlinkStreamTableTestCase):
    pass


class BlinkBatchPandasUDFITTests(PandasUDFITTests,
                                 PyFlinkBlinkBatchTableTestCase):
    pass


class BlinkStreamPandasUDFITTests(PandasUDFITTests,
                                  PyFlinkBlinkStreamTableTestCase):
    pass

@udf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_type=DataTypes.BIGINT(),
     udf_type='pandas')
def add(i, j):
    return i + j


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
