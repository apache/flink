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

import pyflink.dataframe as pf


class _CapturingEnvironment:
    def from_elements(self, rows, schema):
        self.rows = rows
        return object()


class FromRecordsTests(unittest.TestCase):
    def test_rejects_string_like_data_container(self):
        for data in ["ab", b"ab", bytearray(b"ab")]:
            with self.subTest(data_type=type(data)):
                with self.assertRaisesRegex(
                    TypeError, "data must be a non-string sequence of records"
                ):
                    pf.from_records(data, schema=["id"])

    def test_rejects_empty_data(self):
        with self.assertRaisesRegex(ValueError, "data must not be empty"):
            pf.from_records([], schema=["id"])

    def test_rejects_empty_schema(self):
        with self.assertRaisesRegex(ValueError, "schema must not be empty"):
            pf.from_records([(1,)], schema=[])

    def test_rejects_schema_that_is_not_a_list_of_strings(self):
        for schema in [0, False, "", (), ("id",), [1]]:
            with self.subTest(schema=schema):
                with self.assertRaisesRegex(
                    TypeError, "schema must be a list of strings"
                ):
                    pf.from_records([(1,)], schema=schema)

    def test_rejects_empty_schema_field_name(self):
        with self.assertRaisesRegex(ValueError, "schema field names must not be empty"):
            pf.from_records([(1,)], schema=[""])

    def test_rejects_duplicate_schema_field_names(self):
        with self.assertRaisesRegex(ValueError, "schema field names must be unique"):
            pf.from_records([(1, 2)], schema=["id", "id"])

    def test_requires_schema_for_sequence_records(self):
        with self.assertRaisesRegex(ValueError, "schema is required for sequence records"):
            pf.from_records([(1,)])

    def test_rejects_unsupported_record_type(self):
        with self.assertRaisesRegex(
            TypeError, "records must be mappings or non-string sequences"
        ):
            pf.from_records([1], schema=["id"])

    def test_rejects_record_with_wrong_arity(self):
        with self.assertRaisesRegex(
            ValueError, "record at index 1 has 1 values but schema has 2 fields"
        ):
            pf.from_records([(1, "Alice"), (2,)], schema=["id", "name"])

    def test_rejects_mapping_records_with_different_keys(self):
        previous_environment = pf.get_table_environment()
        self.addCleanup(pf.set_table_environment, previous_environment)
        pf.set_table_environment(_CapturingEnvironment())

        records_with_different_keys = [
            [{"a": 1}, {"a": 2, "b": 3}],
            [{"a": 1, "b": 2}, {"a": 3}],
        ]
        for records in records_with_different_keys:
            with self.subTest(records=records):
                with self.assertRaisesRegex(
                    ValueError,
                    "mapping record at index 1 must have the same keys as the first record",
                ):
                    pf.from_records(records)

    def test_rejects_schema_field_missing_from_mapping_records(self):
        previous_environment = pf.get_table_environment()
        self.addCleanup(pf.set_table_environment, previous_environment)
        pf.set_table_environment(_CapturingEnvironment())

        with self.assertRaisesRegex(
            ValueError, "schema field 'name' is not present in mapping records"
        ):
            pf.from_records([{"id": 1}], schema=["id", "name"])

    def test_passes_native_sequence_records_through_without_outer_copy(self):
        previous_environment = pf.get_table_environment()
        self.addCleanup(pf.set_table_environment, previous_environment)

        for records in [[(1,), (2,)], ((1,), (2,))]:
            with self.subTest(record_type=type(records)):
                environment = _CapturingEnvironment()
                pf.set_table_environment(environment)

                pf.from_records(records, schema=["id"])

                self.assertIs(environment.rows, records)


class FromDictTests(unittest.TestCase):
    def test_rejects_empty_data(self):
        with self.assertRaisesRegex(ValueError, "data must not be empty"):
            pf.from_dict({})

    def test_rejects_zero_rows(self):
        with self.assertRaisesRegex(ValueError, "data must contain at least one row"):
            pf.from_dict({"id": []})

    def test_rejects_string_like_column_values(self):
        for values in ["Alice", b"abc", bytearray(b"abc")]:
            with self.subTest(value_type=type(values)):
                with self.assertRaisesRegex(
                    TypeError, "column 'value' values must be a non-string sequence"
                ):
                    pf.from_dict({"value": values})

    def test_rejects_columns_with_different_lengths(self):
        with self.assertRaisesRegex(ValueError, "columns must have equal lengths"):
            pf.from_dict({"id": [1, 2], "name": ["Alice"]})

    def test_rejects_schema_column_missing_from_data(self):
        with self.assertRaisesRegex(ValueError, "column 'name' is not present in data"):
            pf.from_dict({"id": [1]}, schema=["id", "name"])

    def test_rejects_schema_that_is_not_a_list_of_strings(self):
        for schema in [0, False, "", (), ("id",), [1]]:
            with self.subTest(schema=schema):
                with self.assertRaisesRegex(
                    TypeError, "schema must be a list of strings"
                ):
                    pf.from_dict({"id": [1]}, schema=schema)

    def test_rejects_empty_schema_field_name(self):
        with self.assertRaisesRegex(ValueError, "schema field names must not be empty"):
            pf.from_dict({"id": [1]}, schema=[""])

    def test_rejects_duplicate_schema_field_names(self):
        with self.assertRaisesRegex(ValueError, "schema field names must be unique"):
            pf.from_dict({"id": [1]}, schema=["id", "id"])


if __name__ == "__main__":
    unittest.main()
