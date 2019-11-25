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


class CorrelateTests(PyFlinkStreamTableTestCase):

    def test_join_lateral(self):
        t_env = self.t_env
        t_env.register_java_function("split", "org.apache.flink.table.utils.TableFunc1")
        source = t_env.from_elements([("1", "1#3#5#7"), ("2", "2#4#6#8")], ["id", "words"])

        result = source.join_lateral("split(words) as (word)")

        query_operation = result._j_table.getQueryOperation()
        self.assertEqual('INNER', query_operation.getJoinType().toString())
        self.assertTrue(query_operation.isCorrelated())
        self.assertEqual('true', query_operation.getCondition().toString())

    def test_join_lateral_with_join_predicate(self):
        t_env = self.t_env
        t_env.register_java_function("split", "org.apache.flink.table.utils.TableFunc1")
        source = t_env.from_elements([("1", "1#3#5#7"), ("2", "2#4#6#8")], ["id", "words"])

        result = source.join_lateral("split(words) as (word)", "id = word")

        query_operation = result._j_table.getQueryOperation()
        self.assertEqual('INNER', query_operation.getJoinType().toString())
        self.assertTrue(query_operation.isCorrelated())
        self.assertEqual('equals(id, word)',
                         query_operation.getCondition().toString())

    def test_left_outer_join_lateral(self):
        t_env = self.t_env
        t_env.register_java_function("split", "org.apache.flink.table.utils.TableFunc1")
        source = t_env.from_elements([("1", "1#3#5#7"), ("2", "2#4#6#8")], ["id", "words"])

        result = source.left_outer_join_lateral("split(words) as (word)")

        query_operation = result._j_table.getQueryOperation()
        self.assertEqual('LEFT_OUTER', query_operation.getJoinType().toString())
        self.assertTrue(query_operation.isCorrelated())
        self.assertEqual('true', query_operation.getCondition().toString())

    def test_left_outer_join_lateral_with_join_predicate(self):
        t_env = self.t_env
        t_env.register_java_function("split", "org.apache.flink.table.utils.TableFunc1")
        source = t_env.from_elements([("1", "1#3#5#7"), ("2", "2#4#6#8")], ["id", "words"])

        # only support "true" as the join predicate currently
        result = source.left_outer_join_lateral("split(words) as (word)", "true")

        query_operation = result._j_table.getQueryOperation()
        self.assertEqual('LEFT_OUTER', query_operation.getJoinType().toString())
        self.assertTrue(query_operation.isCorrelated())
        self.assertEqual('true', query_operation.getCondition().toString())
