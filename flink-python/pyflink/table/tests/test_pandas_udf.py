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

import pytz

from pyflink.table import DataTypes, Row
from pyflink.table.tests.test_udf import SubtractOne
from pyflink.table.udf import udf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, \
    PyFlinkBlinkBatchTableTestCase, PyFlinkBlinkStreamTableTestCase, PyFlinkBatchTableTestCase


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

        timestamp_value = datetime.datetime(1970, 1, 2, 0, 0, 0, 123000)

        def timestamp_func(timestamp_param):
            assert isinstance(timestamp_param, pd.Series)
            assert isinstance(timestamp_param[0], datetime.datetime), \
                'timestamp_param of wrong type %s !' % type(timestamp_param[0])
            assert timestamp_param[0] == timestamp_value, \
                'timestamp_param is wrong value %s, should be %s!' % (timestamp_param[0],
                                                                      timestamp_value)
            return timestamp_param

        def array_func(array_param):
            assert isinstance(array_param, pd.Series)
            assert isinstance(array_param[0], np.ndarray), \
                'array_param of wrong type %s !' % type(array_param[0])
            return array_param

        def nested_array_func(nested_array_param):
            assert isinstance(nested_array_param, pd.Series)
            assert isinstance(nested_array_param[0], np.ndarray), \
                'nested_array_param of wrong type %s !' % type(nested_array_param[0])
            return pd.Series(nested_array_param[0])

        def row_func(row_param):
            assert isinstance(row_param, pd.Series)
            assert isinstance(row_param[0], dict), \
                'row_param of wrong type %s !' % type(row_param[0])
            return row_param

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

        self.t_env.register_function(
            "timestamp_func",
            udf(timestamp_func, [DataTypes.TIMESTAMP(3)], DataTypes.TIMESTAMP(3),
                udf_type="pandas"))

        self.t_env.register_function(
            "array_str_func",
            udf(array_func, [DataTypes.ARRAY(DataTypes.STRING())],
                DataTypes.ARRAY(DataTypes.STRING()), udf_type="pandas"))

        self.t_env.register_function(
            "array_timestamp_func",
            udf(array_func, [DataTypes.ARRAY(DataTypes.TIMESTAMP(3))],
                DataTypes.ARRAY(DataTypes.TIMESTAMP(3)), udf_type="pandas"))

        self.t_env.register_function(
            "array_int_func",
            udf(array_func, [DataTypes.ARRAY(DataTypes.INT())],
                DataTypes.ARRAY(DataTypes.INT()), udf_type="pandas"))

        self.t_env.register_function(
            "nested_array_func",
            udf(nested_array_func, [DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING()))],
                DataTypes.ARRAY(DataTypes.STRING()), udf_type="pandas"))

        row_type = DataTypes.ROW(
            [DataTypes.FIELD("f1", DataTypes.INT()),
             DataTypes.FIELD("f2", DataTypes.STRING()),
             DataTypes.FIELD("f3", DataTypes.TIMESTAMP(3)),
             DataTypes.FIELD("f4", DataTypes.ARRAY(DataTypes.INT()))])
        self.t_env.register_function(
            "row_func",
            udf(row_func, [row_type], row_type, udf_type="pandas"))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
             'r', 's', 't', 'u'],
            [DataTypes.TINYINT(), DataTypes.SMALLINT(), DataTypes.INT(), DataTypes.BIGINT(),
             DataTypes.BOOLEAN(), DataTypes.BOOLEAN(), DataTypes.FLOAT(), DataTypes.DOUBLE(),
             DataTypes.STRING(), DataTypes.STRING(), DataTypes.BYTES(), DataTypes.DECIMAL(38, 18),
             DataTypes.DECIMAL(38, 18), DataTypes.DATE(), DataTypes.TIME(), DataTypes.TIMESTAMP(3),
             DataTypes.ARRAY(DataTypes.STRING()), DataTypes.ARRAY(DataTypes.TIMESTAMP(3)),
             DataTypes.ARRAY(DataTypes.INT()),
             DataTypes.ARRAY(DataTypes.STRING()), row_type])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements(
            [(1, 32767, -2147483648, 1, True, False, 1.0, 1.0, 'hello', '中文',
              bytearray(b'flink'), decimal.Decimal('1000000000000000000.05'),
              decimal.Decimal('1000000000000000000.05999999999999999899999999999'),
              datetime.date(2014, 9, 13), datetime.time(hour=1, minute=0, second=1),
              timestamp_value, ['hello', '中文', None], [timestamp_value], [1, 2],
              [['hello', '中文', None]], Row(1, 'hello', timestamp_value, [1, 2]))],
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
                 DataTypes.FIELD("o", DataTypes.TIME()),
                 DataTypes.FIELD("p", DataTypes.TIMESTAMP(3)),
                 DataTypes.FIELD("q", DataTypes.ARRAY(DataTypes.STRING())),
                 DataTypes.FIELD("r", DataTypes.ARRAY(DataTypes.TIMESTAMP(3))),
                 DataTypes.FIELD("s", DataTypes.ARRAY(DataTypes.INT())),
                 DataTypes.FIELD("t", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING()))),
                 DataTypes.FIELD("u", row_type)]))

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
                 "time_func(o),"
                 "timestamp_func(p),"
                 "array_str_func(q),"
                 "array_timestamp_func(r),"
                 "array_int_func(s),"
                 "nested_array_func(t),"
                 "row_func(u)") \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual,
                           ["1,32767,-2147483648,1,true,false,1.0,1.0,hello,中文,"
                            "[102, 108, 105, 110, 107],1000000000000000000.050000000000000000,"
                            "1000000000000000000.059999999999999999,2014-09-13,01:00:01,"
                            "1970-01-02 00:00:00.123,[hello, 中文, null],[1970-01-02 00:00:00.123],"
                            "[1, 2],[hello, 中文, null],1,hello,1970-01-02 00:00:00.123,[1, 2]"])


class BlinkPandasUDFITTests(object):

    def test_data_types_only_supported_in_blink_planner(self):
        import pandas as pd

        timezone = self.t_env.get_config().get_local_timezone()
        local_datetime = pytz.timezone(timezone).localize(
            datetime.datetime(1970, 1, 2, 0, 0, 0, 123000))

        def local_zoned_timestamp_func(local_zoned_timestamp_param):
            assert isinstance(local_zoned_timestamp_param, pd.Series)
            assert isinstance(local_zoned_timestamp_param[0], datetime.datetime), \
                'local_zoned_timestamp_param of wrong type %s !' % type(
                    local_zoned_timestamp_param[0])
            assert local_zoned_timestamp_param[0] == local_datetime, \
                'local_zoned_timestamp_param is wrong value %s, %s!' % \
                (local_zoned_timestamp_param[0], local_datetime)
            return local_zoned_timestamp_param

        self.t_env.register_function(
            "local_zoned_timestamp_func",
            udf(local_zoned_timestamp_func,
                [DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)],
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                udf_type="pandas"))

        table_sink = source_sink_utils.TestAppendSink(
            ['a'], [DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements(
            [(local_datetime,)],
            DataTypes.ROW([DataTypes.FIELD("a", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))]))

        t.select("local_zoned_timestamp_func(local_zoned_timestamp_func(a))") \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["1970-01-02T00:00:00.123Z"])


class StreamPandasUDFITTests(PandasUDFITTests,
                             PyFlinkStreamTableTestCase):
    pass


class BatchPandasUDFITTests(PyFlinkBatchTableTestCase):

    def test_basic_functionality(self):
        self.t_env.register_function(
            "add_one",
            udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT(), udf_type="pandas"))

        self.t_env.register_function("add", add)

        # general Python UDF
        self.t_env.register_function(
            "subtract_one", udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT()))

        t = self.t_env.from_elements([(1, 2, 3), (2, 5, 6), (3, 1, 9)], ['a', 'b', 'c'])
        t = t.where("add_one(b) <= 3") \
            .select("a, b + 1, add(a + 1, subtract_one(c)) + 2, add(add_one(a), 1L)")
        result = self.collect(t)
        self.assert_equals(result, ["1,3,6,3", "3,2,14,5"])


class BlinkBatchPandasUDFITTests(PandasUDFITTests,
                                 BlinkPandasUDFITTests,
                                 PyFlinkBlinkBatchTableTestCase):
    pass


class BlinkStreamPandasUDFITTests(PandasUDFITTests,
                                  BlinkPandasUDFITTests,
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
