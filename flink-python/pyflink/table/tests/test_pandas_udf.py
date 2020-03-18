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

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c', 'd'],
            [DataTypes.TINYINT(), DataTypes.SMALLINT(), DataTypes.INT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements(
            [(1, 32767, -2147483648, 1)],
            DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b", DataTypes.SMALLINT()),
                 DataTypes.FIELD("c", DataTypes.INT()),
                 DataTypes.FIELD("d", DataTypes.BIGINT())]))

        t.select("tinyint_func(a),"
                 "smallint_func(b),"
                 "int_func(c),"
                 "bigint_func(d)") \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual,
                           ["1,32767,-2147483648,1"])


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
