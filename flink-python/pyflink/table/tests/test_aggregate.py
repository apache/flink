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
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase


class StreamTableAggregateTests(PyFlinkStreamTableTestCase):

    def test_group_by(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]
        data = [(1, "Hi", "Hello"), (2, "Hello", "Hello"), (2, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        t_env = self.t_env
        t_env.register_table_source("Source", csv_source)
        source = t_env.scan("Source")
        field_names = ["a", "b"]
        field_types = [DataTypes.INT(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestRetractSink())

        result = source.group_by("c").select("a.sum, c as b")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['5,Hello']
        self.assert_equals(actual, expected)


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
