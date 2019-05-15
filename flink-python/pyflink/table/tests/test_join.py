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


class StreamTableJoinTests(PyFlinkStreamTableTestCase):

    def test_join_without_where(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hi", "Hello"), (3, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        source_path2 = os.path.join(self.tempdir + '/streaming2.csv')
        field_names2 = ["d", "e"]
        field_types2 = [DataTypes.INT, DataTypes.STRING]
        data2 = [(2, "Flink"), (3, "Python"), (3, "Flink")]
        csv_source2 = self.prepare_csv_source(source_path2, data2, field_types2, field_names2)
        t_env = self.t_env
        t_env.register_table_source("Source1", csv_source)
        t_env.register_table_source("Source2", csv_source2)
        source1 = t_env.scan("Source1")
        source2 = t_env.scan("Source2")
        field_names = ["a", "b"]
        field_types = [DataTypes.INT, DataTypes.STRING]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestRetractSink())

        result = source1.join(source2, "a = d").select("a, b + e")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['2,HiFlink', '3,HelloPython', '3,HelloFlink']
        self.assert_equals(actual, expected)

    def test_join_with_where(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hi", "Hello"), (3, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        source_path2 = os.path.join(self.tempdir + '/streaming2.csv')
        field_names2 = ["d", "e"]
        field_types2 = [DataTypes.INT, DataTypes.STRING]
        data2 = [(2, "Flink"), (3, "Python"), (3, "Flink")]
        csv_source2 = self.prepare_csv_source(source_path2, data2, field_types2, field_names2)
        t_env = self.t_env
        t_env.register_table_source("Source1", csv_source)
        t_env.register_table_source("Source2", csv_source2)
        source1 = t_env.scan("Source1")
        source2 = t_env.scan("Source2")
        field_names = ["a", "b"]
        field_types = [DataTypes.INT, DataTypes.STRING]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestRetractSink())

        result = source1.join(source2).where("a = d").select("a, b + e")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['2,HiFlink', '3,HelloPython', '3,HelloFlink']
        self.assert_equals(actual, expected)

    def test_left_outer_join_without_where(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hi", "Hello"), (3, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        source_path2 = os.path.join(self.tempdir + '/streaming2.csv')
        field_names2 = ["d", "e"]
        field_types2 = [DataTypes.INT, DataTypes.STRING]
        data2 = [(2, "Flink"), (3, "Python"), (3, "Flink")]
        csv_source2 = self.prepare_csv_source(source_path2, data2, field_types2, field_names2)
        t_env = self.t_env
        t_env.register_table_source("Source1", csv_source)
        t_env.register_table_source("Source2", csv_source2)
        source1 = t_env.scan("Source1")
        source2 = t_env.scan("Source2")
        field_names = ["a", "b"]
        field_types = [DataTypes.INT, DataTypes.STRING]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestRetractSink())

        result = source1.left_outer_join(source2, "a = d").select("a, b + e")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['1,null', '2,HiFlink', '3,HelloPython', '3,HelloFlink']
        self.assert_equals(actual, expected)

    def test_left_outer_join_with_where(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hi", "Hello"), (3, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        source_path2 = os.path.join(self.tempdir + '/streaming2.csv')
        field_names2 = ["d", "e"]
        field_types2 = [DataTypes.INT, DataTypes.STRING]
        data2 = [(2, "Flink"), (3, "Python"), (3, "Flink")]
        csv_source2 = self.prepare_csv_source(source_path2, data2, field_types2, field_names2)
        t_env = self.t_env
        t_env.register_table_source("Source1", csv_source)
        t_env.register_table_source("Source2", csv_source2)
        source1 = t_env.scan("Source1")
        source2 = t_env.scan("Source2")
        field_names = ["a", "b"]
        field_types = [DataTypes.INT, DataTypes.STRING]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestRetractSink())

        result = source1.left_outer_join(source2).where("a = d").select("a, b + e")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['2,HiFlink', '3,HelloPython', '3,HelloFlink']
        self.assert_equals(actual, expected)

    def test_right_outer_join(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hi", "Hello"), (3, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        source_path2 = os.path.join(self.tempdir + '/streaming2.csv')
        field_names2 = ["d", "e"]
        field_types2 = [DataTypes.INT, DataTypes.STRING]
        data2 = [(2, "Flink"), (3, "Python"), (4, "Flink")]
        csv_source2 = self.prepare_csv_source(source_path2, data2, field_types2, field_names2)
        t_env = self.t_env
        t_env.register_table_source("Source1", csv_source)
        t_env.register_table_source("Source2", csv_source2)
        source1 = t_env.scan("Source1")
        source2 = t_env.scan("Source2")
        field_names = ["a", "b"]
        field_types = [DataTypes.INT, DataTypes.STRING]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestRetractSink())

        result = source1.right_outer_join(source2, "a = d").select("d, b + e")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['2,HiFlink', '3,HelloPython', '4,null']
        self.assert_equals(actual, expected)

    def test_full_outer_join(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hi", "Hello"), (3, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        source_path2 = os.path.join(self.tempdir + '/streaming2.csv')
        field_names2 = ["d", "e"]
        field_types2 = [DataTypes.INT, DataTypes.STRING]
        data2 = [(2, "Flink"), (3, "Python"), (4, "Flink")]
        csv_source2 = self.prepare_csv_source(source_path2, data2, field_types2, field_names2)
        t_env = self.t_env
        t_env.register_table_source("Source1", csv_source)
        t_env.register_table_source("Source2", csv_source2)
        source1 = t_env.scan("Source1")
        source2 = t_env.scan("Source2")
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.INT, DataTypes.STRING]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestRetractSink())

        result = source1.full_outer_join(source2, "a = d").select("a, d, b + e")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['1,null,null', '2,2,HiFlink', '3,3,HelloPython', 'null,4,null']
        self.assert_equals(actual, expected)


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
