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
from datetime import datetime
from unittest.mock import Mock, patch
from typing import NamedTuple

import pandas as pd
import pyarrow as pa
import pyflink.dataframe as pf
import pyflink.dataframe.convert as dataframe_convert
from pyflink.table.types import BigIntType, RowType


class _Point(NamedTuple):
    x: int
    y: str


class _OtherPoint(NamedTuple):
    x: int
    z: str


class FromRecordsTests(unittest.TestCase):
    def test_rejects_scalar_sequence_data(self):
        for data in ["ab", b"ab", bytearray(b"ab"), memoryview(b"ab")]:
            with self.subTest(data_type=type(data)):
                with self.assertRaisesRegex(
                    TypeError,
                    "data must be a sequence of records, such as a list or tuple",
                ):
                    pf.from_records(data, schema=["value"])

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
        with self.assertRaises(TypeError) as error:
            pf.from_records([1], schema=["id"])
        self.assertEqual(str(error.exception), "invalid record at index 0")
        self.assertEqual(
            str(error.exception.__cause__),
            "record must be a mapping or a sequence of values, "
            "such as a list or tuple",
        )

    def test_rejects_scalar_sequence_records(self):
        for value in ["ab", b"ab", bytearray(b"ab"), memoryview(b"ab")]:
            for index, records, cause in [
                (
                    0,
                    [value],
                    "record must be a mapping or a sequence of values, "
                    "such as a list or tuple",
                ),
                (1, [(1, 2), value], "record must be a sequence"),
            ]:
                with self.subTest(value_type=type(value), index=index):
                    with self.assertRaises(TypeError) as error:
                        pf.from_records(records, schema=["left", "right"])
                    self.assertEqual(
                        str(error.exception), f"invalid record at index {index}"
                    )
                    self.assertEqual(str(error.exception.__cause__), cause)

    def test_rejects_record_with_wrong_arity(self):
        with self.assertRaises(ValueError) as error:
            pf.from_records([(1, "Alice"), (2,)], schema=["id", "name"])
        self.assertEqual(str(error.exception), "invalid record at index 1")
        self.assertEqual(
            str(error.exception.__cause__),
            "record has 1 values but schema has 2 fields",
        )

    def test_rejects_mapping_records_with_different_keys(self):
        records_with_different_keys = [
            (
                [{"a": 1}, {"a": 2, "b": 3}],
                "record has fields not present in schema: ['b']",
            ),
            (
                [{"a": 1, "b": 2}, {"a": 3}],
                "record is missing schema field 'b'",
            ),
        ]
        for records, cause in records_with_different_keys:
            with self.subTest(records=records):
                with self.assertRaises(ValueError) as error:
                    pf.from_records(records)
                self.assertEqual(str(error.exception), "invalid record at index 1")
                self.assertEqual(str(error.exception.__cause__), cause)

    def test_rejects_mixed_mapping_and_named_tuple_records_with_index(self):
        invalid_records = [
            (
                [{"id": 1}, (2,)],
                ["id"],
                "record must be a mapping",
            ),
            (
                [_Point(1, "a"), {"x": 2, "y": "b"}],
                ["x", "y"],
                "record must be a named tuple",
            ),
        ]
        for records, schema, cause in invalid_records:
            with self.subTest(records=records):
                with self.assertRaises(TypeError) as error:
                    pf.from_records(records, schema=schema)
                self.assertEqual(str(error.exception), "invalid record at index 1")
                self.assertEqual(str(error.exception.__cause__), cause)

    def test_rejects_schema_that_renames_named_tuple_fields(self):
        with self.assertRaises(ValueError) as error:
            pf.from_records([_Point(1, "a")], schema=["a", "b"])
        self.assertEqual(str(error.exception), "invalid record at index 0")
        self.assertEqual(
            str(error.exception.__cause__), "record is missing schema field 'a'"
        )

    def test_rejects_different_inferred_named_tuple_fields(self):
        with self.assertRaises(ValueError) as error:
            pf.from_records([_Point(1, "a"), _OtherPoint(2, "b")])
        self.assertEqual(str(error.exception), "invalid record at index 1")
        self.assertEqual(
            str(error.exception.__cause__),
            "record is missing schema field 'y'",
        )

    def test_rejects_schema_field_missing_from_mapping_records(self):
        with self.assertRaises(ValueError) as error:
            pf.from_records(
                [{"id": 1, "name": "Alice"}, {"id": 2}],
                schema=["id", "name"],
            )
        self.assertEqual(str(error.exception), "invalid record at index 1")
        self.assertEqual(
            str(error.exception.__cause__), "record is missing schema field 'name'"
        )


class FromDictTests(unittest.TestCase):
    def test_rejects_data_that_is_not_a_mapping(self):
        for data in [[], [("id", [1])]]:
            with self.subTest(data=data):
                with self.assertRaisesRegex(TypeError, "data must be a mapping"):
                    pf.from_dict(data)

    def test_rejects_scalar_sequence_column_values(self):
        for values in ["ab", b"ab", bytearray(b"ab"), memoryview(b"ab")]:
            with self.subTest(value_type=type(values)):
                with self.assertRaisesRegex(
                    TypeError,
                    "column 'value' values must be a sequence, "
                    "such as a list or tuple",
                ):
                    pf.from_dict({"value": values})

    def test_rejects_empty_data(self):
        with self.assertRaisesRegex(ValueError, "data must not be empty"):
            pf.from_dict({})

    def test_rejects_zero_rows(self):
        with self.assertRaisesRegex(ValueError, "data must contain at least one row"):
            pf.from_dict({"id": []})

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


class CreationValidationTests(unittest.TestCase):
    def test_parses_watermark_into_semantic_specification(self):
        watermark = dataframe_convert._WatermarkSpec.parse(
            ("ts", "ts - INTERVAL '5' SECOND")
        )

        self.assertEqual(watermark.column, "ts")
        self.assertEqual(watermark.expression, "ts - INTERVAL '5' SECOND")

    def test_watermark_spec_unpacks_column_before_expression(self):
        watermark = dataframe_convert._WatermarkSpec(
            "ts", "ts - INTERVAL '5' SECOND"
        )

        self.assertEqual(tuple(watermark), ("ts", "ts - INTERVAL '5' SECOND"))

    def test_rejects_invalid_watermarks(self):
        invalid_watermarks = [
            ("ts", "watermark must be a tuple"),
            (("ts",), "watermark must be a tuple"),
            (("ts", "ts", "extra"), "watermark must be a tuple"),
            (("", "ts"), "must be non-empty strings"),
            (("ts", ""), "must be non-empty strings"),
            ((1, "ts"), "must be non-empty strings"),
        ]
        for watermark, message in invalid_watermarks:
            with self.subTest(watermark=watermark):
                with self.assertRaisesRegex(TypeError, message):
                    pf.from_dict(
                        {"ts": [datetime(2026, 1, 1)]}, watermark=watermark
                    )

    def test_pandas_and_arrow_reject_invalid_positional_schemas(self):
        inputs = [
            (pf.from_pandas, pd.DataFrame({"left": [1], "right": [2]})),
            (pf.from_arrow, pa.table({"left": [1], "right": [2]})),
        ]
        invalid_schemas = [
            ("names", TypeError, "schema must be a list of strings"),
            (["left", 2], TypeError, "schema must be a list of strings"),
            (["left"], ValueError, "schema has 1 fields but data has 2 columns"),
            (["left", "left"], ValueError, "schema field names must be unique"),
        ]
        for creator, data in inputs:
            for schema, error_type, message in invalid_schemas:
                with self.subTest(creator=creator.__name__, schema=schema):
                    with self.assertRaisesRegex(error_type, message):
                        creator(data, schema=schema)

    def test_pandas_rejects_duplicate_columns_without_schema(self):
        pdf = pd.DataFrame([[1, 2]], columns=["value", "value"])

        with self.assertRaisesRegex(ValueError, "schema field names must be unique"):
            pf.from_pandas(pdf)

    def test_rejects_columnar_fields_containing_null_type(self):
        inputs = [
            (
                lambda: pf.from_pandas(pd.DataFrame({"value": [None]})),
                "columns with Arrow null types: 'value'",
            ),
            (
                lambda: pf.from_arrow(
                    pa.table({"left": pa.nulls(1), "right": pa.nulls(1)})
                ),
                "columns with Arrow null types: 'left', 'right'",
            ),
            (
                lambda: pf.from_arrow(
                    pa.table(
                        {
                            "value": pa.array(
                                [[None]], type=pa.list_(pa.null())
                            )
                        }
                    )
                ),
                "columns with Arrow null types: 'value'",
            ),
        ]
        for creator, message in inputs:
            with self.subTest(creator=creator):
                with self.assertRaisesRegex(TypeError, message):
                    creator()

    def test_rejects_invalid_table_and_columnar_inputs(self):
        invalid_inputs = [
            (pf.from_table, object(), "pyflink.table.Table"),
            (pf.from_pandas, object(), "pandas.DataFrame"),
            (pf.from_arrow, object(), "pyarrow.Table"),
        ]
        for creator, data, message in invalid_inputs:
            with self.subTest(creator=creator.__name__):
                with self.assertRaisesRegex(TypeError, message):
                    creator(data)


class RangeTests(unittest.TestCase):
    def test_matches_python_range_and_preserves_bigint_schema_when_empty(self):
        cases = [
            ((4,), [(0,), (1,), (2,), (3,)]),
            ((4, -1, -2), [(4,), (2,), (0,)]),
            ((2, 2), []),
            ((2**63 - 1, 2**63), [(2**63 - 1,)]),
            ((-(2**63), -(2**63) + 1), [(-(2**63),)]),
        ]
        for arguments, expected_rows in cases:
            table_environment = Mock()
            table_environment._from_elements.return_value = object()
            with self.subTest(arguments=arguments), patch(
                "pyflink.dataframe.convert.get_or_create_table_environment",
                return_value=table_environment,
            ):
                pf.range(*arguments)

            rows, row_type = table_environment._from_elements.call_args.args[:2]
            self.assertEqual([row[1:] for row in rows], expected_rows)
            self.assertIsInstance(row_type, RowType)
            self.assertEqual(row_type.field_names(), ["id"])
            self.assertIsInstance(row_type.field_types()[0], BigIntType)

    def test_rejects_values_outside_bigint_bounds(self):
        invalid_ranges = [
            (2**63, 2**63 + 1),
            (2**63 - 1, 2**63 + 2),
            (-(2**63) - 1, -(2**63) - 2, -1),
            (-(2**63), -(2**63) - 3, -1),
        ]
        table_environment = Mock()
        for arguments in invalid_ranges:
            with self.subTest(arguments=arguments), patch(
                "pyflink.dataframe.convert.get_or_create_table_environment",
                return_value=table_environment,
            ) as get_table_environment:
                with self.assertRaisesRegex(
                    ValueError, "range values must fit in signed BIGINT"
                ):
                    pf.range(*arguments)
                get_table_environment.assert_not_called()

    def test_rejects_invalid_arguments(self):
        invalid_arguments = [
            ((1.5,), TypeError, "start_or_end must be an integer"),
            ((0, 1.5), TypeError, "end must be an integer"),
            ((0, 1, 1.5), TypeError, "step must be an integer"),
            ((0, 1, 0), ValueError, "step must not be zero"),
        ]
        for arguments, error_type, message in invalid_arguments:
            with self.subTest(arguments=arguments):
                with self.assertRaisesRegex(error_type, message):
                    pf.range(*arguments)

if __name__ == "__main__":
    unittest.main()
