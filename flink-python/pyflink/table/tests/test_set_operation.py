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
# #  distributed under the License is distributed on an "AS IS" BASIS,
# #  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# #  See the License for the specific language governing permissions and
# # limitations under the License.
################################################################################

from pyflink.table.types import DataTypes
from pyflink.testing import source_sink_utils

from pyflink.testing.test_case_utils import PyFlinkBatchTableTestCase, PyFlinkStreamTableTestCase


class StreamTableSetOperationTests(PyFlinkStreamTableTestCase):

    def test_union_all(self):
        t_env = self.t_env
        t1 = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hi", "Hello"), (3, "Hello", "Hello")],
                                 ['a', 'b', 'c'])
        t2 = t_env.from_elements([(2, "Hi", "Hello"), (3, "Hello", "Python"), (4, "Hi", "Flink")],
                                 ['a', 'b', 'c'])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestAppendSink())

        result = t1.union_all(t2)
        result.insert_into("Results")
        t_env.execute()

        actual = source_sink_utils.results()
        expected = ['1,Hi,Hello',
                    '2,Hi,Hello',
                    '2,Hi,Hello',
                    '3,Hello,Hello',
                    '3,Hello,Python',
                    '4,Hi,Flink']
        self.assert_equals(actual, expected)


class BatchTableSetOperationTests(PyFlinkBatchTableTestCase):

    data1 = [(1, "Hi", "Hello"), (1, "Hi", "Hello"), (3, "Hello", "Hello")]
    data2 = [(3, "Hello", "Hello"), (3, "Hello", "Python"), (4, "Hi", "Flink")]
    schema = ["a", "b", "c"]

    def test_minus(self):
        t_env = self.t_env
        t1 = t_env.from_elements(self.data1, self.schema)
        t2 = t_env.from_elements(self.data2, self.schema)

        result = t1.minus(t2)
        actual = self.collect(result)

        expected = ['1,Hi,Hello']
        self.assert_equals(actual, expected)

    def test_minus_all(self):
        t_env = self.t_env
        t1 = t_env.from_elements(self.data1, self.schema)
        t2 = t_env.from_elements(self.data2, self.schema)

        result = t1.minus_all(t2)
        actual = self.collect(result)

        expected = ['1,Hi,Hello',
                    '1,Hi,Hello']
        self.assert_equals(actual, expected)

    def test_union(self):
        t_env = self.t_env
        t1 = t_env.from_elements(self.data1, self.schema)
        t2 = t_env.from_elements(self.data2, self.schema)

        result = t1.union(t2)
        actual = self.collect(result)

        expected = ['1,Hi,Hello',
                    '3,Hello,Hello',
                    '3,Hello,Python',
                    '4,Hi,Flink']
        self.assert_equals(actual, expected)

    def test_intersect(self):
        t_env = self.t_env
        t1 = t_env.from_elements(self.data1, self.schema)
        t2 = t_env.from_elements(self.data2, self.schema)

        result = t1.intersect(t2)
        actual = self.collect(result)

        expected = ['3,Hello,Hello']
        self.assert_equals(actual, expected)

    def test_intersect_all(self):
        t_env = self.t_env
        t1 = t_env.from_elements(self.data1, self.schema)
        t2 = t_env.from_elements(self.data2, self.schema)

        result = t1.intersect_all(t2)
        actual = self.collect(result)

        expected = ['3,Hello,Hello']
        self.assert_equals(actual, expected)


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
