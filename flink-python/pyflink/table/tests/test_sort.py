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

import os

from pyflink.table.types import DataTypes
from pyflink.testing.test_case_utils import PyFlinkBatchTableTestCase


class BatchTableSortTests(PyFlinkBatchTableTestCase):

    def test_order_by_offset_fetch(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b"]
        field_types = [DataTypes.INT(), DataTypes.STRING()]
        data = [(1, "Hello"), (2, "Hello"), (3, "Flink"), (4, "Python")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        t_env = self.t_env
        t_env.register_table_source("Source", csv_source)
        source = t_env.scan("Source")

        result = source.order_by("a.desc").offset(2).fetch(2).select("a, b")
        actual = self.collect(result)

        expected = ['2,Hello', '1,Hello']
        self.assert_equals(actual, expected)


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
