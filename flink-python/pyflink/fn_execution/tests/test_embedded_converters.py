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
import unittest
from types import SimpleNamespace
from unittest import mock

from pyflink.common import Row
from pyflink.fn_execution import flink_fn_execution_pb2 as proto


class EmbeddedConvertersTests(unittest.TestCase):
    def test_row_schema_converter_preserves_field_names(self):
        with mock.patch.dict("sys.modules", {"pemja": SimpleNamespace(findClass=lambda _: None)}):
            from pyflink.fn_execution.embedded.converters import from_row_schema_proto

        schema = proto.Schema(fields=[
            proto.Schema.Field(
                name="a",
                type=proto.Schema.FieldType(type_name=proto.Schema.BIGINT, nullable=True)),
            proto.Schema.Field(
                name="b",
                type=proto.Schema.FieldType(type_name=proto.Schema.BIGINT, nullable=True)),
        ])

        converter = from_row_schema_proto(schema)
        internal = converter.to_internal((0, (2, 4)))

        self.assertIsInstance(internal, Row)
        self.assertEqual((internal.a, internal.b), (2, 4))

        output_row = Row(3, 16)
        output_row.set_field_names(["a", "b"])
        self.assertEqual(converter.to_external(output_row), (0, (3, 16)))
