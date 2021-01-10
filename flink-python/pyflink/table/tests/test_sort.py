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

from pyflink.testing.test_case_utils import PyFlinkBatchTableTestCase


class BatchTableSortTests(PyFlinkBatchTableTestCase):

    def test_order_by_offset_fetch(self):
        t = self.t_env.from_elements([(1, "Hello")], ["a", "b"])
        result = t.order_by(t.a.desc).offset(2).fetch(2)

        query_operation = result._j_table.getQueryOperation()
        self.assertEqual(2, query_operation.getOffset())
        self.assertEqual(2, query_operation.getFetch())
        self.assertEqual('[desc(a)]',
                         query_operation.getOrder().toString())

    def test_limit(self):
        t = self.t_env.from_elements([(1, "Hello")], ["a", "b"])
        result = t.limit(1)

        query_operation = result._j_table.getQueryOperation()
        self.assertEqual(0, query_operation.getOffset())
        self.assertEqual(1, query_operation.getFetch())

    def test_limit_with_offset(self):
        t = self.t_env.from_elements([(1, "Hello")], ["a", "b"])
        result = t.limit(1, 2)

        query_operation = result._j_table.getQueryOperation()
        self.assertEqual(2, query_operation.getOffset())
        self.assertEqual(1, query_operation.getFetch())


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
