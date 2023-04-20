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
from pyflink.common import Types
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import PyFlinkTestCase


class JsonSerializationSchemaTests(PyFlinkTestCase):

    def test_json_row_serialization_deserialization_schema(self):
        jvm = get_gateway().jvm
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
        json_row_serialization_schema._j_serialization_schema.open(
            jvm.org.apache.flink.connector.testutils.formats.DummyInitializationContext())
        json_row_deserialization_schema._j_deserialization_schema.open(
            jvm.org.apache.flink.connector.testutils.formats.DummyInitializationContext())

        for i in range(len(jsons)):
            j_row = json_row_deserialization_schema._j_deserialization_schema\
                .deserialize(bytes(jsons[i], encoding='utf-8'))
            result = str(json_row_serialization_schema._j_serialization_schema.serialize(j_row),
                         encoding='utf-8')
            self.assertEqual(expected_jsons[i], result)
