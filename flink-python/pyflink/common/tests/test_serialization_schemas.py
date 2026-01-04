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
from pyflink.common.serialization import (
    SimpleStringSchema,
    ByteArraySchema,
    RowFieldExtractorSchema,
)
from pyflink.testing.test_case_utils import PyFlinkTestCase
from pyflink.java_gateway import get_gateway


class SimpleStringSchemaTests(PyFlinkTestCase):

    def test_simple_string_schema(self):
        expected_string = 'test string'
        simple_string_schema = SimpleStringSchema()
        self.assertEqual(expected_string.encode(encoding='utf-8'),
                         simple_string_schema._j_serialization_schema.serialize(expected_string))

        self.assertEqual(expected_string, simple_string_schema._j_deserialization_schema
                         .deserialize(expected_string.encode(encoding='utf-8')))


class SimpleByteSchemaTests(PyFlinkTestCase):
    def test_simple_byte_schema(self):
        expected_bytes = "test bytes".encode(encoding='utf-8')
        simple_byte_schema = ByteArraySchema()
        self.assertEqual(expected_bytes,
                         simple_byte_schema._j_serialization_schema.serialize(expected_bytes))
        self.assertEqual(expected_bytes, simple_byte_schema._j_deserialization_schema
                         .deserialize(expected_bytes))


class RowFieldExtractorSchemaTests(PyFlinkTestCase):
    """Tests for RowFieldExtractorSchema."""

    def test_row_field_extractor_schema_creation(self):
        """Test RowFieldExtractorSchema can be created with valid index."""
        schema = RowFieldExtractorSchema(0)
        self.assertIsNotNone(schema._j_serialization_schema)

    def test_serialize_string_field(self):
        """Test serializing a string field from a Row."""
        schema = RowFieldExtractorSchema(0)
        # Create a Java Row using constructor and setField
        gateway = get_gateway()
        j_row = gateway.jvm.org.apache.flink.types.Row(2)  # 2 fields
        j_row.setField(0, "test-value")
        j_row.setField(1, 123)
        result = schema._j_serialization_schema.serialize(j_row)
        expected = "test-value".encode('utf-8')
        self.assertEqual(expected, bytes(result))

    def test_serialize_integer_field(self):
        """Test serializing an integer field from a Row."""
        schema = RowFieldExtractorSchema(1)
        # Create a Java Row using constructor and setField
        gateway = get_gateway()
        j_row = gateway.jvm.org.apache.flink.types.Row(2)
        j_row.setField(0, "key")
        j_row.setField(1, 42)
        result = schema._j_serialization_schema.serialize(j_row)
        expected = "42".encode('utf-8')
        self.assertEqual(expected, bytes(result))

    def test_serialize_null_row(self):
        """Test serializing null Row returns empty byte array."""
        schema = RowFieldExtractorSchema(0)
        result = schema._j_serialization_schema.serialize(None)
        self.assertEqual(0, len(result))

    def test_serialize_null_field(self):
        """Test serializing Row with null field returns empty byte array."""
        schema = RowFieldExtractorSchema(0)
        # Create a Java Row with null first field
        gateway = get_gateway()
        j_row = gateway.jvm.org.apache.flink.types.Row(2)
        j_row.setField(0, None)  # null field
        j_row.setField(1, "value")
        result = schema._j_serialization_schema.serialize(j_row)
        self.assertEqual(0, len(result))

    def test_negative_field_index_raises_error(self):
        """Test that negative field index raises ValueError."""
        with self.assertRaises(ValueError) as context:
            RowFieldExtractorSchema(-1)
        self.assertIn("Field index must be non-negative", str(context.exception))

    def test_get_field_index(self):
        """Test that getFieldIndex returns correct value."""
        schema = RowFieldExtractorSchema(3)
        field_index = schema._j_serialization_schema.getFieldIndex()
        self.assertEqual(3, field_index)

    def test_multiple_schemas_with_different_indices(self):
        """Test creating multiple schemas with different field indices."""
        schema0 = RowFieldExtractorSchema(0)
        schema1 = RowFieldExtractorSchema(1)
        schema2 = RowFieldExtractorSchema(2)
        self.assertEqual(0, schema0._j_serialization_schema.getFieldIndex())
        self.assertEqual(1, schema1._j_serialization_schema.getFieldIndex())
        self.assertEqual(2, schema2._j_serialization_schema.getFieldIndex())

    def test_serialize_different_field_types(self):
        """Test serializing different data types from Row fields."""
        gateway = get_gateway()
        # Test with string
        schema_str = RowFieldExtractorSchema(0)
        j_row_str = gateway.jvm.org.apache.flink.types.Row(2)
        j_row_str.setField(0, "hello")
        j_row_str.setField(1, 100)
        result_str = schema_str._j_serialization_schema.serialize(j_row_str)
        self.assertEqual("hello".encode('utf-8'), bytes(result_str))
        # Test with integer
        schema_int = RowFieldExtractorSchema(1)
        j_row_int = gateway.jvm.org.apache.flink.types.Row(2)
        j_row_int.setField(0, "key")
        j_row_int.setField(1, 999)
        result_int = schema_int._j_serialization_schema.serialize(j_row_int)
        self.assertEqual("999".encode('utf-8'), bytes(result_int))

    def test_schema_equals(self):
        """Test that schemas with same field index are considered equal."""
        schema1 = RowFieldExtractorSchema(1)
        schema2 = RowFieldExtractorSchema(1)
        schema3 = RowFieldExtractorSchema(2)
        # Test via Java equals method
        self.assertTrue(schema1._j_serialization_schema.equals(schema2._j_serialization_schema))
        self.assertFalse(schema1._j_serialization_schema.equals(schema3._j_serialization_schema))

    def test_schema_hash_code(self):
        """Test that schemas with same field index have same hash code."""
        schema1 = RowFieldExtractorSchema(1)
        schema2 = RowFieldExtractorSchema(1)
        hash1 = schema1._j_serialization_schema.hashCode()
        hash2 = schema2._j_serialization_schema.hashCode()
        self.assertEqual(hash1, hash2)
