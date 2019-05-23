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

from py4j.protocol import Py4JJavaError

from pyflink.table.window import Session, Slide, Tumble
from pyflink.table import Over
from pyflink.table.types import DataTypes
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, PyFlinkBatchTableTestCase


class StreamTableWindowTests(PyFlinkStreamTableTestCase):

    def test_over_window(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.LONG, DataTypes.INT, DataTypes.STRING]
        data = [(1, 1, "Hello"), (2, 2, "Hello"), (3, 4, "Hello"), (4, 8, "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        t_env = self.t_env
        t_env.register_table_source("Source", csv_source)
        source = t_env.scan("Source")

        result = source.over_window(Over.partition_by("c").order_by("a")
                                    .preceding("2.rows").following("current_row").alias("w"))

        self.assertRaisesRegexp(
            Py4JJavaError, "Ordering must be defined on a time attribute",
            result.select, "b.sum over w")


class BatchTableWindowTests(PyFlinkBatchTableTestCase):

    def test_tumble_window(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.LONG, DataTypes.INT, DataTypes.STRING]
        data = [(1, 1, "Hello"), (2, 2, "Hello"), (3, 4, "Hello"), (4, 8, "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        t_env = self.t_env
        t_env.register_table_source("Source", csv_source)
        source = t_env.scan("Source")

        result = source.window(Tumble.over("2.rows").on("a").alias("w"))\
            .group_by("w, c").select("b.sum")
        actual = self.collect(result)

        expected = ['3', '12']
        self.assert_equals(actual, expected)

    def test_slide_window(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.LONG, DataTypes.INT, DataTypes.STRING]
        data = [(1000, 1, "Hello"), (2000, 2, "Hello"), (3000, 4, "Hello"), (4000, 8, "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        t_env = self.t_env
        t_env.register_table_source("Source", csv_source)
        source = t_env.scan("Source")

        result = source.window(Slide.over("2.seconds").every("1.seconds").on("a").alias("w"))\
            .group_by("w, c").select("b.sum")
        actual = self.collect(result)

        expected = ['1', '3', '6', '12', '8']
        self.assert_equals(actual, expected)

    def test_session_window(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.LONG, DataTypes.INT, DataTypes.STRING]
        data = [(1000, 1, "Hello"), (2000, 2, "Hello"), (4000, 4, "Hello"), (5000, 8, "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        t_env = self.t_env
        t_env.register_table_source("Source", csv_source)
        source = t_env.scan("Source")

        result = source.window(Session.with_gap("1.seconds").on("a").alias("w"))\
            .group_by("w, c").select("b.sum")
        actual = self.collect(result)

        expected = ['3', '12']
        self.assert_equals(actual, expected)


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
