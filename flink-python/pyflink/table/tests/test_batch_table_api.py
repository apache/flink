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
import os

from pyflink.table.types import DataTypes
from pyflink.table.window import Tumble, Slide, Session

from pyflink.testing.test_case_utils import PyFlinkBatchTableTestCase


class BatchTableTests(PyFlinkBatchTableTestCase):

    def test_select_alias(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        t_env = self.t_env
        t_env.register_table_source("Source", csv_source)
        source = t_env.scan("Source")
        result = source.alias("a, b, c").select("a + 1, b + c")

        actual = self.collect(result)
        expected = ['2,HiHello', '3,HelloHello']
        self.assert_equals(actual, expected)

    def test_where_filter(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        t_env = self.t_env
        t_env.register_table_source("Source", csv_source)
        source = t_env.scan("Source")
        result = source.where("a > 1").filter("c = 'Hello'").select("a + 1, b + c")

        actual = self.collect(result)
        expected = ['3,HelloHello']
        self.assert_equals(actual, expected)

    def test_group_by(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hello", "Hello"), (2, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        t_env = self.t_env
        t_env.register_table_source("Source", csv_source)
        source = t_env.scan("Source")
        result = source.group_by("c").select("a.sum, c as b")

        actual = self.collect(result)
        expected = ['5,Hello']
        self.assert_equals(actual, expected)

    def test_distinct(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hello", "Hello"), (2, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        t_env = self.t_env
        t_env.register_table_source("Source", csv_source)
        source = t_env.scan("Source")
        result = source.distinct().select("a, c as b")

        actual = self.collect(result)
        expected = ['1,Hello', '2,Hello']
        self.assert_equals(actual, expected)

    def test_join(self):
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
        result = source1.join(source2, "a = d").select("a, b + e")

        actual = self.collect(result)
        expected = ['2,HiFlink', '3,HelloPython', '3,HelloFlink']
        self.assert_equals(actual, expected)

        result2 = source1.join(source2).where("a = d").select("a, b + e")

        actual = self.collect(result2)
        expected = ['2,HiFlink', '3,HelloPython', '3,HelloFlink']
        self.assert_equals(actual, expected)

    def test_left_outer_join(self):
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
        result = source1.left_outer_join(source2, "a = d").select("a, b + e")

        actual = self.collect(result)
        expected = ['1,null', '2,HiFlink', '3,HelloPython', '3,HelloFlink']
        self.assert_equals(actual, expected)

        result2 = source1.left_outer_join(source2).where("a = d").select("a, b + e")

        actual = self.collect(result2)
        print(actual)
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
        result = source1.right_outer_join(source2, "a = d").select("d, b + e")

        actual = self.collect(result)
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
        result = source1.full_outer_join(source2, "a = d").select("a, d, b + e")

        actual = self.collect(result)
        expected = ['1,null,null', '2,2,HiFlink', '3,3,HelloPython', 'null,4,null']
        self.assert_equals(actual, expected)

    def test_minus(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (1, "Hi", "Hello"), (3, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        source_path2 = os.path.join(self.tempdir + '/streaming2.csv')
        data2 = [(3, "Hello", "Hello"), (3, "Hello", "Python"), (4, "Hi", "Flink")]
        csv_source2 = self.prepare_csv_source(source_path2, data2, field_types, field_names)

        t_env = self.t_env
        t_env.register_table_source("Source1", csv_source)
        t_env.register_table_source("Source2", csv_source2)

        source1 = t_env.scan("Source1")
        source2 = t_env.scan("Source2")
        result = source1.minus(source2)

        actual = self.collect(result)
        expected = ['1,Hi,Hello']
        self.assert_equals(actual, expected)

    def test_minus_all(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (1, "Hi", "Hello"), (3, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        source_path2 = os.path.join(self.tempdir + '/streaming2.csv')
        data2 = [(3, "Hello", "Hello"), (3, "Hello", "Python"), (4, "Hi", "Flink")]
        csv_source2 = self.prepare_csv_source(source_path2, data2, field_types, field_names)

        t_env = self.t_env
        t_env.register_table_source("Source1", csv_source)
        t_env.register_table_source("Source2", csv_source2)

        source1 = t_env.scan("Source1")
        source2 = t_env.scan("Source2")
        result = source1.minus_all(source2)

        actual = self.collect(result)
        expected = ['1,Hi,Hello',
                    '1,Hi,Hello']
        self.assert_equals(actual, expected)

    def test_union(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hi", "Hello"), (3, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        source_path2 = os.path.join(self.tempdir + '/streaming2.csv')
        data2 = [(2, "Hi", "Hello"), (3, "Hello", "Python"), (4, "Hi", "Flink")]
        csv_source2 = self.prepare_csv_source(source_path2, data2, field_types, field_names)

        t_env = self.t_env
        t_env.register_table_source("Source1", csv_source)
        t_env.register_table_source("Source2", csv_source2)

        source1 = t_env.scan("Source1")
        source2 = t_env.scan("Source2")
        result = source1.union(source2)

        actual = self.collect(result)
        expected = ['1,Hi,Hello',
                    '2,Hi,Hello',
                    '3,Hello,Hello',
                    '3,Hello,Python',
                    '4,Hi,Flink']
        self.assert_equals(actual, expected)

    def test_union_all(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hi", "Hello"), (3, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        source_path2 = os.path.join(self.tempdir + '/streaming2.csv')
        data2 = [(2, "Hi", "Hello"), (3, "Hello", "Python"), (4, "Hi", "Flink")]
        csv_source2 = self.prepare_csv_source(source_path2, data2, field_types, field_names)

        t_env = self.t_env
        t_env.register_table_source("Source1", csv_source)
        t_env.register_table_source("Source2", csv_source2)

        source1 = t_env.scan("Source1")
        source2 = t_env.scan("Source2")
        result = source1.union_all(source2)

        actual = self.collect(result)
        expected = ['1,Hi,Hello',
                    '2,Hi,Hello',
                    '2,Hi,Hello',
                    '3,Hello,Hello',
                    '3,Hello,Python',
                    '4,Hi,Flink']
        self.assert_equals(actual, expected)

    def test_intersect(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hi", "Hello"), (2, "Hi", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        source_path2 = os.path.join(self.tempdir + '/streaming2.csv')
        data2 = [(2, "Hi", "Hello"), (2, "Hi", "Hello"), (4, "Hi", "Flink")]
        csv_source2 = self.prepare_csv_source(source_path2, data2, field_types, field_names)

        t_env = self.t_env
        t_env.register_table_source("Source1", csv_source)
        t_env.register_table_source("Source2", csv_source2)

        source1 = t_env.scan("Source1")
        source2 = t_env.scan("Source2")
        result = source1.intersect(source2)

        actual = self.collect(result)
        expected = ['2,Hi,Hello']
        self.assert_equals(actual, expected)

    def test_intersect_all(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hi", "Hello"), (2, "Hi", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        source_path2 = os.path.join(self.tempdir + '/streaming2.csv')
        data2 = [(2, "Hi", "Hello"), (2, "Hi", "Hello"), (4, "Hi", "Flink")]
        csv_source2 = self.prepare_csv_source(source_path2, data2, field_types, field_names)

        t_env = self.t_env
        t_env.register_table_source("Source1", csv_source)
        t_env.register_table_source("Source2", csv_source2)

        source1 = t_env.scan("Source1")
        source2 = t_env.scan("Source2")
        result = source1.intersect_all(source2)

        actual = self.collect(result)
        expected = ['2,Hi,Hello', '2,Hi,Hello']
        self.assert_equals(actual, expected)

    def test_order_by_offset_fetch(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b"]
        field_types = [DataTypes.INT, DataTypes.STRING]
        data = [(1, "Hello"), (2, "Hello"), (3, "Flink"), (4, "Python")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)

        t_env = self.t_env
        t_env.register_table_source("Source", csv_source)
        source = t_env.scan("Source")
        result = source.order_by("a.desc").offset(2).fetch(2).select("a, b")

        actual = self.collect(result)
        expected = ['2,Hello', '1,Hello']
        self.assert_equals(actual, expected)

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
