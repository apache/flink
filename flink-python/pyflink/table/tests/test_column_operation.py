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


class StreamTableColumnsOperationTests(PyFlinkStreamTableTestCase):

    def test_add_columns(self):
        t = self.t_env.from_elements([(1, 'Hi', 'Hello')], ['a', 'b', 'c'])
        result = t.select(t.a).add_columns((t.a + 1).alias('b'), (t.a + 2).alias('c'))
        query_operation = result._j_table.getQueryOperation()
        self.assertEqual('[a, plus(a, 1), '
                         'plus(a, 2)]',
                         query_operation.getProjectList().toString())

    def test_add_or_replace_columns(self):
        t = self.t_env.from_elements([(1, 'Hi', 'Hello')], ['a', 'b', 'c'])
        result = t.select("a").add_or_replace_columns((t.a + 1).alias('b'), (t.a + 2).alias('a'))
        query_operation = result._j_table.getQueryOperation()
        self.assertEqual('[plus(a, 2), '
                         'plus(a, 1)]',
                         query_operation.getProjectList().toString())

    def test_rename_columns(self):
        t = self.t_env.from_elements([(1, 'Hi', 'Hello')], ['a', 'b', 'c'])
        result = t.select("a, b, c").rename_columns(t.a.alias('d'), t.c.alias('f'), t.b.alias('e'))
        resolved_schema = result._j_table.getQueryOperation().getResolvedSchema()
        self.assertEqual(['d', 'e', 'f'], list(resolved_schema.getColumnNames()))

    def test_drop_columns(self):
        t = self.t_env.from_elements([(1, 'Hi', 'Hello')], ['a', 'b', 'c'])
        result = t.drop_columns(t.a, t.c)
        query_operation = result._j_table.getQueryOperation()
        self.assertEqual('[b]', query_operation.getProjectList().toString())


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
