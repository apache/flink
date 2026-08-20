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

import datetime
import decimal
import sys
import unittest
from typing import Any, List, Optional, Union

import pyflink.dataframe as pf
from pyflink.table import DataTypes
from pyflink.testing.test_case_utils import PyFlinkTestCase


class DataTypeTests(unittest.TestCase):
    def test_public_api_surface(self):
        public_methods = {
            name for name in dir(pf.DataType) if not name.startswith("_")
        }

        self.assertEqual(
            public_methods,
            {
                "binary",
                "bool",
                "date",
                "decimal",
                "fixed_size_binary",
                "fixed_size_string",
                "float32",
                "float64",
                "int8",
                "int16",
                "int32",
                "int64",
                "list",
                "map",
                "not_null",
                "null",
                "nullable",
                "string",
                "struct",
                "time",
                "timestamp",
                "timestamp_ltz",
            },
        )

    def test_scalar_factories_map_to_table_types(self):
        expected_types = {
            "binary": DataTypes.BYTES(),
            "bool": DataTypes.BOOLEAN(),
            "date": DataTypes.DATE(),
            "float32": DataTypes.FLOAT(),
            "float64": DataTypes.DOUBLE(),
            "int8": DataTypes.TINYINT(),
            "int16": DataTypes.SMALLINT(),
            "int32": DataTypes.INT(),
            "int64": DataTypes.BIGINT(),
            "null": DataTypes.NULL(),
            "string": DataTypes.STRING(),
        }

        for factory_name, expected_type in expected_types.items():
            with self.subTest(factory_name=factory_name):
                data_type = getattr(pf.DataType, factory_name)()
                self.assertEqual(data_type._to_table_data_type(), expected_type)

    def test_parameterized_factories_map_to_table_types(self):
        test_cases = [
            (
                "decimal",
                pf.DataType.decimal,
                {"precision": 10, "scale": 3},
                DataTypes.DECIMAL(10, 3),
            ),
            (
                "fixed_size_binary",
                pf.DataType.fixed_size_binary,
                {"length": 16},
                DataTypes.BINARY(16),
            ),
            (
                "fixed_size_string",
                pf.DataType.fixed_size_string,
                {"length": 12},
                DataTypes.CHAR(12),
            ),
            ("time_default", pf.DataType.time, {}, DataTypes.TIME(0)),
            (
                "time_precision",
                pf.DataType.time,
                {"precision": 3},
                DataTypes.TIME(3),
            ),
            (
                "timestamp_default",
                pf.DataType.timestamp,
                {},
                DataTypes.TIMESTAMP(6),
            ),
            (
                "timestamp_precision",
                pf.DataType.timestamp,
                {"precision": 3},
                DataTypes.TIMESTAMP(3),
            ),
            (
                "timestamp_ltz_default",
                pf.DataType.timestamp_ltz,
                {},
                DataTypes.TIMESTAMP_LTZ(6),
            ),
            (
                "timestamp_ltz_precision",
                pf.DataType.timestamp_ltz,
                {"precision": 3},
                DataTypes.TIMESTAMP_LTZ(3),
            ),
        ]

        for factory_name, factory, arguments, expected_type in test_cases:
            with self.subTest(factory_name=factory_name):
                data_type = factory(**arguments)
                self.assertEqual(data_type._to_table_data_type(), expected_type)

    def test_logically_equal_types_compare_and_hash_equally(self):
        first_int = pf.DataType.int64()
        second_int = pf.DataType.int64()
        string = pf.DataType.string()

        self.assertEqual(first_int, second_int)
        self.assertNotEqual(first_int, string)
        self.assertEqual(len({first_int, second_int, string}), 2)

    def test_nullability_participates_in_equality_and_hashing(self):
        nullable = pf.DataType.int32()
        first_non_nullable = nullable.not_null()
        second_non_nullable = pf.DataType.int32().not_null()

        self.assertNotEqual(nullable, first_non_nullable)
        self.assertEqual(first_non_nullable, second_non_nullable)
        self.assertEqual(hash(first_non_nullable), hash(second_non_nullable))
        self.assertEqual(len({nullable, first_non_nullable}), 2)

    def test_nullability_modifiers_preserve_the_original_type(self):
        original = pf.DataType.int32()

        non_nullable = original.not_null()
        nullable_again = non_nullable.nullable()

        self.assertEqual(original._to_table_data_type(), DataTypes.INT())
        self.assertEqual(
            non_nullable._to_table_data_type(),
            DataTypes.INT().not_null(),
        )
        self.assertEqual(nullable_again._to_table_data_type(), DataTypes.INT())

    def test_null_type_cannot_be_made_non_nullable(self):
        with self.assertRaisesRegex(ValueError, "NULL"):
            pf.DataType.null().not_null()

    def test_list_preserves_its_element_type(self):
        list_type = pf.DataType.list(dtype=pf.DataType.int32().not_null())

        self.assertEqual(
            list_type._to_table_data_type(),
            DataTypes.ARRAY(DataTypes.INT().not_null()),
        )

    def test_map_preserves_its_key_and_value_types(self):
        map_type = pf.DataType.map(
            key_type=pf.DataType.string(),
            value_type=pf.DataType.int64(),
        )

        self.assertEqual(
            map_type._to_table_data_type(),
            DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()),
        )

    def test_struct_preserves_dict_insertion_order(self):
        struct_type = pf.DataType.struct(
            fields={
                "name": pf.DataType.string(),
                "age": pf.DataType.int32().not_null(),
            }
        )

        self.assertEqual(
            struct_type._to_table_data_type(),
            DataTypes.ROW(
                [
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("age", DataTypes.INT().not_null()),
                ]
            ),
        )

    def test_struct_preserves_list_field_order(self):
        struct_type = pf.DataType.struct(
            fields=[
                ("age", pf.DataType.int32().not_null()),
                ("name", pf.DataType.string()),
            ]
        )

        self.assertEqual(
            struct_type._to_table_data_type(),
            DataTypes.ROW(
                [
                    DataTypes.FIELD("age", DataTypes.INT().not_null()),
                    DataTypes.FIELD("name", DataTypes.STRING()),
                ]
            ),
        )

    def test_from_basic_python_type_hints(self):
        expected_types = {
            bool: pf.DataType.bool(),
            int: pf.DataType.int64(),
            float: pf.DataType.float64(),
            str: pf.DataType.string(),
            bytes: pf.DataType.binary(),
            bytearray: pf.DataType.binary(),
            decimal.Decimal: pf.DataType.decimal(38, 18),
            datetime.date: pf.DataType.date(),
            datetime.time: pf.DataType.time(),
            datetime.datetime: pf.DataType.timestamp(),
            Any: pf.DataType.string(),
        }

        for python_type, expected_type in expected_types.items():
            with self.subTest(python_type=python_type):
                self.assertEqual(
                    pf.DataType._from_type_hint(python_type),
                    expected_type,
                )

    def test_from_optional_type_hint(self):
        self.assertEqual(
            pf.DataType._from_type_hint(Optional[int]),
            pf.DataType.int64(),
        )

    @unittest.skipIf(
        sys.version_info < (3, 10),
        "PEP 604 union types require Python 3.10 or later",
    )
    def test_from_pep_604_union_type_hint(self):
        self.assertEqual(
            pf.DataType._from_type_hint(int | None),
            pf.DataType.int64(),
        )
        self.assertEqual(
            pf.DataType._from_type_hint(list[int | None]),
            pf.DataType.list(dtype=pf.DataType.int64()),
        )
        with self.assertRaises(TypeError):
            pf.DataType._from_type_hint(int | str)

    def test_from_list_type_hint(self):
        self.assertEqual(
            pf.DataType._from_type_hint(list[int]),
            pf.DataType.list(dtype=pf.DataType.int64()),
        )

    def test_from_dict_type_hint(self):
        self.assertEqual(
            pf.DataType._from_type_hint(dict[str, float]),
            pf.DataType.map(
                key_type=pf.DataType.string(),
                value_type=pf.DataType.float64(),
            ),
        )

    def test_from_type_hint_rejects_ambiguous_or_incomplete_hints(self):
        invalid_hints = [
            List,
            complex,
        ]

        for type_hint in invalid_hints:
            with self.subTest(type_hint=type_hint):
                with self.assertRaises(TypeError):
                    pf.DataType._from_type_hint(type_hint)

    def test_from_type_hint_reports_ambiguous_union_error(self):
        with self.assertRaises(TypeError) as context:
            pf.DataType._from_type_hint(Union[int, str])

        self.assertEqual(
            "Cannot infer DataType from type hint 'typing.Union[int, str]'. "
            "Please specify the data type explicitly.",
            str(context.exception),
        )

    def test_repr_preserves_type_parameters_and_nested_nullability(self):
        self.assertEqual(
            repr(
                pf.DataType.list(
                    pf.DataType.struct(
                        [
                            (
                                "amount",
                                pf.DataType.decimal(10, 2).not_null(),
                            )
                        ]
                    ).not_null()
                )
            ),
            "DataType(ArrayType("
            "RowType(RowField(amount, DecimalType(10, 2, false), ...), false), "
            "true))",
        )


class DataTypeExpressionTests(PyFlinkTestCase):
    def test_expression_casts_accept_dataframe_data_types(self):
        test_cases = [
            ("cast", "cast(value, DOUBLE)"),
            ("try_cast", "TRY_CAST(value, DOUBLE)"),
        ]

        for operation, expected_expression in test_cases:
            with self.subTest(operation=operation):
                expression = getattr(pf.col("value"), operation)(
                    pf.DataType.float64()
                )

                self.assertEqual(str(expression), expected_expression)


class DataTypeSqlTests(PyFlinkTestCase):
    def test_from_sql_parses_scalar_and_nested_types(self):
        test_cases = [
            ("INT", pf.DataType.int32()),
            (
                "DECIMAL(10, 3) NOT NULL",
                pf.DataType.decimal(10, 3).not_null(),
            ),
            (
                "ROW<name STRING, scores ARRAY<DOUBLE NOT NULL>>",
                pf.DataType.struct(
                    fields=[
                        ("name", pf.DataType.string()),
                        (
                            "scores",
                            pf.DataType.list(
                                dtype=pf.DataType.float64().not_null()
                            ),
                        ),
                    ]
                ),
            ),
        ]

        for sql_type, expected_type in test_cases:
            with self.subTest(sql_type=sql_type):
                self.assertEqual(pf.DataType._from_sql(sql_type), expected_type)

    def test_from_sql_preserves_timestamp_precision(self):
        self.assertEqual(
            pf.DataType._from_sql("TIMESTAMP(9)"),
            pf.DataType.timestamp(precision=9),
        )

    def test_from_sql_preserves_timestamp_ltz_precision(self):
        self.assertEqual(
            pf.DataType._from_sql("TIMESTAMP_LTZ(3)"),
            pf.DataType.timestamp_ltz(precision=3),
        )

    def test_from_sql_supports_null_type(self):
        self.assertEqual(
            pf.DataType._from_sql("NULL"),
            pf.DataType.null(),
        )

    def test_from_sql_reports_parser_errors_as_value_errors(self):
        with self.assertRaises(ValueError):
            pf.DataType._from_sql("VARCHAR(test)")


if __name__ == "__main__":
    unittest.main()
