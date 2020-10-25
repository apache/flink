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
from pyflink.common.serialization import JsonRowSerializationSchema, \
    JsonRowDeserializationSchema, CsvRowSerializationSchema, CsvRowDeserializationSchema, \
    SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import PyFlinkTestCase


class TestRowSerializationSchemas(PyFlinkTestCase):

    def test_simple_string_schema(self):
        expected_string = 'test string'
        simple_string_schema = SimpleStringSchema()
        self.assertEqual(expected_string.encode(encoding='utf-8'),
                         simple_string_schema._j_serialization_schema.serialize(expected_string))

        self.assertEqual(expected_string, simple_string_schema._j_deserialization_schema
                         .deserialize(expected_string.encode(encoding='utf-8')))

    def test_json_row_serialization_deserialization_schema(self):
        jsons = ["{\"svt\":\"2020-02-24T12:58:09.209+0800\"}",
                 "{\"svt\":\"2020-02-24T12:58:09.209+0800\", "
                 "\"ops\":{\"id\":\"281708d0-4092-4c21-9233-931950b6eccf\"},\"ids\":[1, 2, 3]}",
                 "{\"svt\":\"2020-02-24T12:58:09.209+0800\"}"]
        expected_jsons = ["{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"ops\":null,\"ids\":null}",
                          "{\"svt\":\"2020-02-24T12:58:09.209+0800\","
                          "\"ops\":{\"id\":\"281708d0-4092-4c21-9233-931950b6eccf\"},"
                          "\"ids\":[1,2,3]}",
                          "{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"ops\":null,\"ids\":null}"]

        row_schema = Types.ROW_NAMED(["svt", "ops", "ids"],
                                     [Types.STRING(),
                                     Types.ROW_NAMED(['id'], [Types.STRING()]),
                                     Types.PRIMITIVE_ARRAY(Types.INT())])

        json_row_serialization_schema = JsonRowSerializationSchema.builder() \
            .with_type_info(row_schema).build()
        json_row_deserialization_schema = JsonRowDeserializationSchema.builder() \
            .type_info(row_schema).build()

        for i in range(len(jsons)):
            j_row = json_row_deserialization_schema._j_deserialization_schema\
                .deserialize(bytes(jsons[i], encoding='utf-8'))
            result = str(json_row_serialization_schema._j_serialization_schema.serialize(j_row),
                         encoding='utf-8')
            self.assertEqual(expected_jsons[i], result)

    def test_csv_row_serialization_schema(self):
        JRow = get_gateway().jvm.org.apache.flink.types.Row

        j_row = JRow(3)
        j_row.setField(0, "BEGIN")
        j_row.setField(2, "END")

        def field_assertion(field_info, csv_value, value, field_delimiter):
            row_info = Types.ROW([Types.STRING(), field_info, Types.STRING()])
            expected_csv = "BEGIN" + field_delimiter + csv_value + field_delimiter + "END\n"
            j_row.setField(1, value)

            csv_row_serialization_schema = CsvRowSerializationSchema.Builder(row_info)\
                .set_escape_character('*').set_quote_character('\'')\
                .set_array_element_delimiter(':').set_field_delimiter(';').build()
            csv_row_deserialization_schema = CsvRowDeserializationSchema.Builder(row_info)\
                .set_escape_character('*').set_quote_character('\'')\
                .set_array_element_delimiter(':').set_field_delimiter(';').build()

            serialized_bytes = csv_row_serialization_schema._j_serialization_schema.serialize(j_row)
            self.assertEqual(expected_csv, str(serialized_bytes, encoding='utf-8'))

            j_deserialized_row = csv_row_deserialization_schema._j_deserialization_schema\
                .deserialize(expected_csv.encode("utf-8"))
            self.assertTrue(j_row.equals(j_deserialized_row))

        field_assertion(Types.STRING(), "'123''4**'", "123'4*", ";")
        field_assertion(Types.STRING(), "'a;b''c'", "a;b'c", ";")
        field_assertion(Types.INT(), "12", 12, ";")

        test_j_row = JRow(2)
        test_j_row.setField(0, "1")
        test_j_row.setField(1, "hello")

        field_assertion(Types.ROW([Types.STRING(), Types.STRING()]), "'1:hello'", test_j_row, ";")
        test_j_row.setField(1, "hello world")
        field_assertion(Types.ROW([Types.STRING(), Types.STRING()]), "'1:hello world'", test_j_row,
                        ";")
        field_assertion(Types.STRING(), "null", "null", ";")
