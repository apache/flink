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

from py4j.protocol import Py4JJavaError

from pyflink.table.window import Session, Slide, Tumble
from pyflink.table import Over
from pyflink.testing.test_case_utils import PyFlinkBatchTableTestCase, PyFlinkStreamTableTestCase


class StreamTableWindowTests(PyFlinkStreamTableTestCase):

    def test_over_window(self):
        t_env = self.t_env
        t = t_env.from_elements([(1, 1, "Hello")], ['a', 'b', 'c'])

        result = t.over_window(Over.partition_by("c").order_by("a")
                               .preceding("2.rows").following("current_row").alias("w"))

        self.assertRaisesRegex(
            Py4JJavaError, "Ordering must be defined on a time attribute",
            result.select, "b.sum over w")


class BatchTableWindowTests(PyFlinkBatchTableTestCase):

    def test_tumble_window(self):
        t = self.t_env.from_elements([(1, 1, "Hello")], ["a", "b", "c"])
        result = t.window(Tumble.over("2.rows").on("a").alias("w"))\
            .group_by("w, c").select("b.sum")

        query_operation = result._j_table.getQueryOperation().getChildren().get(0)
        self.assertEqual('[c]', query_operation.getGroupingExpressions().toString())
        self.assertEqual('TumbleWindow(field: [a], size: [2])',
                         query_operation.getGroupWindow().asSummaryString())

    def test_slide_window(self):
        t = self.t_env.from_elements([(1000, 1, "Hello")], ["a", "b", "c"])
        result = t.window(Slide.over("2.seconds").every("1.seconds").on("a").alias("w"))\
            .group_by("w, c").select("b.sum")

        query_operation = result._j_table.getQueryOperation().getChildren().get(0)
        self.assertEqual('[c]', query_operation.getGroupingExpressions().toString())
        self.assertEqual('SlideWindow(field: [a], slide: [1000], size: [2000])',
                         query_operation.getGroupWindow().asSummaryString())

    def test_session_window(self):
        t = self.t_env.from_elements([(1000, 1, "Hello")], ["a", "b", "c"])
        result = t.window(Session.with_gap("1.seconds").on("a").alias("w"))\
            .group_by("w, c").select("b.sum")

        query_operation = result._j_table.getQueryOperation().getChildren().get(0)
        self.assertEqual('[c]', query_operation.getGroupingExpressions().toString())
        self.assertEqual('SessionWindow(field: [a], gap: [1000])',
                         query_operation.getGroupWindow().asSummaryString())


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
