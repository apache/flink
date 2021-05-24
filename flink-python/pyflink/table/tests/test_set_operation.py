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

from pyflink.testing.test_case_utils import PyFlinkOldBatchTableTestCase


class StreamTableSetOperationTests(PyFlinkOldBatchTableTestCase):

    data1 = [(1, "Hi", "Hello")]
    data2 = [(3, "Hello", "Hello")]
    schema = ["a", "b", "c"]

    def test_minus(self):
        t_env = self.t_env
        t1 = t_env.from_elements(self.data1, self.schema)
        t2 = t_env.from_elements(self.data2, self.schema)

        result = t1.minus(t2)

        self.assertEqual('MINUS', result._j_table.getQueryOperation().getType().toString())
        self.assertFalse(result._j_table.getQueryOperation().isAll())

    def test_minus_all(self):
        t_env = self.t_env
        t1 = t_env.from_elements(self.data1, self.schema)
        t2 = t_env.from_elements(self.data2, self.schema)

        result = t1.minus_all(t2)
        self.assertEqual('MINUS', result._j_table.getQueryOperation().getType().toString())
        self.assertTrue(result._j_table.getQueryOperation().isAll())

    def test_union(self):
        t_env = self.t_env
        t1 = t_env.from_elements(self.data1, self.schema)
        t2 = t_env.from_elements(self.data2, self.schema)

        result = t1.union(t2)
        self.assertEqual('UNION', result._j_table.getQueryOperation().getType().toString())
        self.assertFalse(result._j_table.getQueryOperation().isAll())

    def test_union_all(self):
        t_env = self.t_env
        t1 = t_env.from_elements(self.data1, self.schema)
        t2 = t_env.from_elements(self.data2, self.schema)

        result = t1.union_all(t2)
        self.assertEqual('UNION', result._j_table.getQueryOperation().getType().toString())
        self.assertTrue(result._j_table.getQueryOperation().isAll())

    def test_intersect(self):
        t_env = self.t_env
        t1 = t_env.from_elements(self.data1, self.schema)
        t2 = t_env.from_elements(self.data2, self.schema)

        result = t1.intersect(t2)
        self.assertEqual('INTERSECT', result._j_table.getQueryOperation().getType().toString())
        self.assertFalse(result._j_table.getQueryOperation().isAll())

    def test_intersect_all(self):
        t_env = self.t_env
        t1 = t_env.from_elements(self.data1, self.schema)
        t2 = t_env.from_elements(self.data2, self.schema)

        result = t1.intersect_all(t2)
        self.assertEqual('INTERSECT', result._j_table.getQueryOperation().getType().toString())
        self.assertTrue(result._j_table.getQueryOperation().isAll())


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
