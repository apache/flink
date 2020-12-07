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

from pyflink.common import Row
from pyflink.table.types import DataTypes
from pyflink.table.udf import udf
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


class BatchRowBasedOperationITTests(RowBasedOperationTests, PyFlinkBlinkBatchTableTestCase):
    pass


class StreamRowBasedOperationITTests(RowBasedOperationTests, PyFlinkBlinkStreamTableTestCase):
    pass


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
