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
from pyflink.table.types import DataTypes
from pyflink.table.table_schema import TableSchema
from pyflink.testing.test_case_utils import PyFlinkTestCase


class TableSchemaTests(PyFlinkTestCase):

    def test_init(self):
        schema = TableSchema(["a", "b", "c"],
                             [DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()])

        self.assertEqual(3, schema.get_field_count())
        self.assertEqual(["a", "b", "c"], schema.get_field_names())

        self.assertEqual([DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()],
                         schema.get_field_data_types())

    def test_copy(self):
        schema = TableSchema(["a", "b", "c"],
                             [DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()])

        copied_schema = schema.copy()

        self.assertEqual(schema, copied_schema)
        copied_schema._j_table_schema = None
        self.assertNotEqual(schema, copied_schema)

    def test_get_field_data_types(self):
        schema = TableSchema(["a", "b", "c"],
                             [DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()])

        types = schema.get_field_data_types()

        self.assertEqual([DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()], types)

    def test_get_field_data_type(self):
        schema = TableSchema(["a", "b", "c"],
                             [DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()])

        type_by_name = schema.get_field_data_type("b")
        type_by_index = schema.get_field_data_type(2)
        type_by_name_not_exist = schema.get_field_data_type("d")
        type_by_index_not_exist = schema.get_field_data_type(6)
        with self.assertRaises(TypeError):
            schema.get_field_data_type(None)

        self.assertEqual(DataTypes.BIGINT(), type_by_name)
        self.assertEqual(DataTypes.STRING(), type_by_index)
        self.assertIsNone(type_by_name_not_exist)
        self.assertIsNone(type_by_index_not_exist)

    def test_get_field_count(self):
        schema = TableSchema(["a", "b", "c"],
                             [DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()])

        count = schema.get_field_count()

        self.assertEqual(3, count)

    def test_get_field_names(self):
        schema = TableSchema(["a", "b", "c"],
                             [DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()])

        names = schema.get_field_names()

        self.assertEqual(["a", "b", "c"], names)

    def test_get_field_name(self):
        schema = TableSchema(["a", "b", "c"],
                             [DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()])

        field_name = schema.get_field_name(2)
        field_name_not_exist = schema.get_field_name(3)

        self.assertEqual("c", field_name)
        self.assertIsNone(field_name_not_exist)

    def test_to_row_data_type(self):
        schema = TableSchema(["a", "b", "c"],
                             [DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()])

        row_type = schema.to_row_data_type()

        expected = DataTypes.ROW([DataTypes.FIELD("a", DataTypes.INT()),
                                  DataTypes.FIELD("b", DataTypes.BIGINT()),
                                  DataTypes.FIELD("c", DataTypes.STRING())],
                                 nullable=False)
        self.assertEqual(expected, row_type)

    def test_hash(self):
        schema = TableSchema(["a", "b", "c"],
                             [DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()])
        schema2 = TableSchema(["a", "b", "c"],
                              [DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()])

        self.assertEqual(hash(schema2), hash(schema))

    def test_str(self):
        schema = TableSchema(["a", "b", "c"],
                             [DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()])

        expected = "root\n |-- a: INT\n |-- b: BIGINT\n |-- c: STRING\n"
        self.assertEqual(expected, str(schema))

    def test_repr(self):
        schema = TableSchema(["a", "b", "c"],
                             [DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()])

        expected = "root\n |-- a: INT\n |-- b: BIGINT\n |-- c: STRING\n"
        self.assertEqual(expected, repr(schema))

    def test_builder(self):
        schema_builder = TableSchema.builder()

        schema = schema_builder \
            .field("a", DataTypes.INT())\
            .field("b", DataTypes.BIGINT())\
            .field("c", DataTypes.STRING()).build()

        expected = TableSchema(["a", "b", "c"],
                               [DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()])
        self.assertEqual(expected, schema)
