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

from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase


class StreamTableJoinTests(PyFlinkStreamTableTestCase):

    def test_join_without_where(self):
        t_env = self.t_env
        t1 = t_env.from_elements([(1, "Hi", "Hello")], ['a', 'b', 'c'])
        t2 = t_env.from_elements([(2, "Flink")], ['d', 'e'])
        result = t1.join(t2, "a = d")

        query_operation = result._j_table.getQueryOperation()
        self.assertEqual('INNER', query_operation.getJoinType().toString())
        self.assertEqual('equals(a, d)',
                         query_operation.getCondition().toString())
        self.assertFalse(query_operation.isCorrelated())

    def test_join_with_where(self):
        t_env = self.t_env
        t1 = t_env.from_elements([(1, "Hi", "Hello")], ['a', 'b', 'c'])
        t2 = t_env.from_elements([(2, "Flink")], ['d', 'e'])
        result = t1.join(t2).where("a = d")

        query_operation = result._j_table.getQueryOperation().getChildren().get(0)
        self.assertEqual('INNER', query_operation.getJoinType().toString())
        self.assertEqual('true', query_operation.getCondition().toString())
        self.assertFalse(query_operation.isCorrelated())

    def test_left_outer_join_without_where(self):
        t_env = self.t_env
        t1 = t_env.from_elements([(1, "Hi", "Hello")], ['a', 'b', 'c'])
        t2 = t_env.from_elements([(2, "Flink")], ['d', 'e'])
        result = t1.left_outer_join(t2, "a = d")

        query_operation = result._j_table.getQueryOperation()
        self.assertEqual('LEFT_OUTER', query_operation.getJoinType().toString())
        self.assertEqual('equals(a, d)',
                         query_operation.getCondition().toString())
        self.assertFalse(query_operation.isCorrelated())

    def test_left_outer_join_with_where(self):
        t_env = self.t_env
        t1 = t_env.from_elements([(1, "Hi", "Hello")], ['a', 'b', 'c'])
        t2 = t_env.from_elements([(2, "Flink")], ['d', 'e'])
        result = t1.left_outer_join(t2).where("a = d")

        query_operation = result._j_table.getQueryOperation().getChildren().get(0)
        self.assertEqual('LEFT_OUTER', query_operation.getJoinType().toString())
        self.assertEqual('true', query_operation.getCondition().toString())
        self.assertFalse(query_operation.isCorrelated())

    def test_right_outer_join(self):
        t_env = self.t_env
        t1 = t_env.from_elements([(1, "Hi", "Hello")], ['a', 'b', 'c'])
        t2 = t_env.from_elements([(2, "Flink")], ['d', 'e'])
        result = t1.right_outer_join(t2, "a = d")

        query_operation = result._j_table.getQueryOperation()
        self.assertEqual('RIGHT_OUTER', query_operation.getJoinType().toString())
        self.assertEqual('equals(a, d)',
                         query_operation.getCondition().toString())
        self.assertFalse(query_operation.isCorrelated())

    def test_full_outer_join(self):
        t_env = self.t_env
        t1 = t_env.from_elements([(1, "Hi", "Hello")], ['a', 'b', 'c'])
        t2 = t_env.from_elements([(2, "Flink")], ['d', 'e'])

        result = t1.full_outer_join(t2, "a = d")
        query_operation = result._j_table.getQueryOperation()
        self.assertEqual('FULL_OUTER', query_operation.getJoinType().toString())
        self.assertEqual('equals(a, d)',
                         query_operation.getCondition().toString())
        self.assertFalse(query_operation.isCorrelated())


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
