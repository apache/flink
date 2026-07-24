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
from typing import NamedTuple

import pyflink.dataframe as pf
from py4j.protocol import Py4JJavaError
from pyflink.common import Row
from pyflink.table import DataTypes as TableDataTypes
from pyflink.table.expression import Expression
from pyflink.testing.test_case_utils import (
    PyFlinkDataFrameUTTestCase,
    PyFlinkStreamDataFrameTestCase,
)


class _Point(NamedTuple):
    x: int
    y: str


class _CloseableIterator:
    def __init__(self, values=None, error=None):
        self._values = iter(values or [])
        self._error = error
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._error is not None:
            raise self._error
        return next(self._values)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.closed = True


class _TableResult:
    def __init__(self, iterator):
        self._iterator = iterator

    def collect(self):
        return self._iterator


class _Table:
    def __init__(self, iterator):
        self._iterator = iterator

    def execute(self):
        return _TableResult(self._iterator)


class DataFrameCollectTests(unittest.TestCase):
    def test_collect_returns_all_rows_and_closes_iterator(self):
        iterator = _CloseableIterator([Row(1, "Alice")])
        dataframe = pf.DataFrame(_Table(iterator))

        self.assertEqual(dataframe.collect(), [Row(1, "Alice")])
        self.assertTrue(iterator.closed)

    def test_collect_closes_iterator_when_iteration_fails(self):
        iterator = _CloseableIterator(error=RuntimeError("iteration failed"))
        dataframe = pf.DataFrame(_Table(iterator))

        with self.assertRaisesRegex(RuntimeError, "iteration failed"):
            dataframe.collect()

        self.assertTrue(iterator.closed)


class DataFrameCreationTests(PyFlinkDataFrameUTTestCase):
    def test_from_dict_uses_insertion_order_without_schema(self):
        dataframe = pf.from_dict({"name": ["Alice"], "id": [1]})

        self.assert_dataframe_schema(
            dataframe,
            ["name", "id"],
            [TableDataTypes.STRING(), TableDataTypes.BIGINT()],
        )

    def test_from_dict_respects_explicit_schema_order_and_subset(self):
        dataframe = pf.from_dict(
            {
                "name": ["Alice"],
                "ignored": ["x"],
                "id": [1],
            },
            schema=["id", "name"],
        )

        self.assert_dataframe_schema(
            dataframe,
            ["id", "name"],
            [TableDataTypes.BIGINT(), TableDataTypes.STRING()],
        )

    def test_from_records_accepts_list_records(self):
        dataframe = pf.from_records(
            [[1, "Alice"], [2, "Bob"]],
            schema=["id", "name"],
        )

        self.assert_dataframe_schema(
            dataframe,
            ["id", "name"],
            [TableDataTypes.BIGINT(), TableDataTypes.STRING()],
        )

    def test_from_records_accepts_general_sequence_records(self):
        dataframe = pf.from_records(
            [range(2), range(2, 4)],
            schema=["left", "right"],
        )

        self.assert_dataframe_schema(
            dataframe,
            ["left", "right"],
            [TableDataTypes.BIGINT(), TableDataTypes.BIGINT()],
        )

    def test_from_records_infers_mapping_schema(self):
        dataframe = pf.from_records(
            [{"name": "Alice", "id": 1}, {"name": "Bob", "id": 2}]
        )

        self.assert_dataframe_schema(
            dataframe,
            ["name", "id"],
            [TableDataTypes.STRING(), TableDataTypes.BIGINT()],
        )

    def test_from_records_selects_mapping_fields_with_explicit_schema(self):
        dataframe = pf.from_records(
            [
                {"name": "Alice", "id": 1, "ignored": "x"},
                {"name": "Bob", "id": 2, "ignored": "y"},
            ],
            schema=["id", "name"],
        )

        self.assert_dataframe_schema(
            dataframe,
            ["id", "name"],
            [TableDataTypes.BIGINT(), TableDataTypes.STRING()],
        )

    def test_from_records_infers_named_tuple_schema(self):
        dataframe = pf.from_records([_Point(1, "a"), _Point(2, "b")])

        self.assert_dataframe_schema(
            dataframe,
            ["x", "y"],
            [TableDataTypes.BIGINT(), TableDataTypes.STRING()],
        )

    def test_from_records_selects_named_tuple_fields_with_explicit_schema(self):
        dataframe = pf.from_records(
            [_Point(1, "a"), _Point(2, "b")],
            schema=["y", "x"],
        )

        self.assert_dataframe_schema(
            dataframe,
            ["y", "x"],
            [TableDataTypes.STRING(), TableDataTypes.BIGINT()],
        )


class DataFrameSelectTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        self.dataframe = pf.from_records(
            [(1, "Alice"), (2, "Bob")],
            schema=["id", "name"],
        )

    def test_select_accepts_names_expressions_lists_and_named_projections(self):
        result = self.dataframe.select(
            ["name"],
            pf.col("id"),
            doubled=pf.col("id") * 2,
        )

        self.assert_dataframe_schema(
            result,
            ["name", "id", "doubled"],
            [
                TableDataTypes.STRING(),
                TableDataTypes.BIGINT(),
                TableDataTypes.BIGINT(),
            ],
        )

    def test_select_accepts_tuple_column_group(self):
        result = self.dataframe.select(("name", "id"))

        self.assert_dataframe_schema(
            result,
            ["name", "id"],
            [TableDataTypes.STRING(), TableDataTypes.BIGINT()],
        )

    def test_select_rejects_non_string_column(self):
        with self.assertRaisesRegex(TypeError, "columns must be strings"):
            self.dataframe.select(42)

    def test_select_rejects_non_expression_projection(self):
        with self.assertRaisesRegex(TypeError, "projections must be expressions"):
            self.dataframe.select(answer=42)


class DataFrameWithColumnTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        self.dataframe = pf.from_records(
            [(1, "Alice", 30)],
            schema=["id", "name", "age"],
        )

    def test_with_column_adds_callable_result(self):
        result = self.dataframe.with_column(
            "age_next_year",
            lambda current: current["age"] + 1,
        )

        self.assert_dataframe_schema(
            result,
            ["id", "name", "age", "age_next_year"],
            [
                TableDataTypes.BIGINT(),
                TableDataTypes.STRING(),
                TableDataTypes.BIGINT(),
                TableDataTypes.BIGINT(),
            ],
        )

    def test_with_column_replaces_existing_column(self):
        result = self.dataframe.with_column("age", pf.col("age") + 1)

        self.assert_dataframe_schema(
            result,
            ["id", "name", "age"],
            [
                TableDataTypes.BIGINT(),
                TableDataTypes.STRING(),
                TableDataTypes.BIGINT(),
            ],
        )

    def test_with_column_rejects_non_expression(self):
        with self.assertRaisesRegex(TypeError, "expr must be an Expression"):
            self.dataframe.with_column("answer", 42)

    def test_with_column_rejects_callable_returning_non_expression(self):
        with self.assertRaisesRegex(TypeError, "expr must be an Expression"):
            self.dataframe.with_column("answer", lambda df: 42)

    def test_with_column_rejects_expression_class(self):
        with self.assertRaisesRegex(TypeError, "expr must be an Expression"):
            self.dataframe.with_column("answer", Expression)

    def test_with_column_rejects_non_string_name(self):
        with self.assertRaisesRegex(TypeError, "name must be a string"):
            self.dataframe.with_column(42, object())


class DataFrameFilterTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        self.dataframe = pf.from_records(
            [(1, 0.9, "NYC", None), (2, 0.95, "SF", "Paris")],
            schema=["id", "score", "city", "destination"],
        )

    def assert_filter_schema(self, dataframe):
        self.assert_dataframe_schema(
            dataframe,
            ["id", "score", "city", "destination"],
            [
                TableDataTypes.BIGINT(),
                TableDataTypes.DOUBLE(),
                TableDataTypes.STRING(),
                TableDataTypes.STRING(),
            ],
        )

    def test_filter_accepts_expression(self):
        self.assert_filter_schema(self.dataframe.filter(pf.col("id") > 0))

    def test_filter_accepts_multiple_predicates_and_constraints(self):
        result = self.dataframe.filter(
            pf.col("id") > 0,
            pf.col("score") >= 0.8,
            city="NYC",
        )

        self.assert_filter_schema(result)

    def test_filter_accepts_none_constraint(self):
        self.assert_filter_schema(self.dataframe.filter(destination=None))

    def test_filter_accepts_sql_string_predicate(self):
        self.assert_filter_schema(self.dataframe.filter("id > 0"))

    def test_filter_accepts_callable_predicate(self):
        self.assert_filter_schema(
            self.dataframe.filter(lambda current: current["id"] > 0)
        )

    def test_filter_rejects_non_expression(self):
        with self.assertRaisesRegex(TypeError, "predicate must be an Expression"):
            self.dataframe.filter(True)

    def test_filter_requires_a_condition(self):
        with self.assertRaisesRegex(ValueError, "requires at least one predicate"):
            self.dataframe.filter()

    def test_filter_rejects_callable_returning_non_expression(self):
        with self.assertRaisesRegex(
            TypeError, "callable predicates must return an Expression"
        ):
            self.dataframe.filter(lambda df: True)

    def test_filter_rejects_expression_class(self):
        with self.assertRaisesRegex(TypeError, "predicate must be an Expression"):
            self.dataframe.filter(Expression)


class DataFrameGetItemTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        self.dataframe = pf.from_records(
            [(1, "Alice"), (2, "Bob")],
            schema=["id", "name"],
        )

    def test_getitem_returns_expression_for_column_name(self):
        self.assertIsInstance(self.dataframe["id"], Expression)

    def test_getitem_selects_list_projection(self):
        result = self.dataframe[["name", "id"]]

        self.assert_dataframe_schema(
            result,
            ["name", "id"],
            [TableDataTypes.STRING(), TableDataTypes.BIGINT()],
        )

    def test_getitem_selects_tuple_projection(self):
        result = self.dataframe[("name", "id")]

        self.assert_dataframe_schema(
            result,
            ["name", "id"],
            [TableDataTypes.STRING(), TableDataTypes.BIGINT()],
        )

    def test_getitem_filters_with_expression(self):
        result = self.dataframe[self.dataframe["id"] > 0]

        self.assert_dataframe_schema(
            result,
            ["id", "name"],
            [TableDataTypes.BIGINT(), TableDataTypes.STRING()],
        )

    def test_getitem_rejects_unsupported_key(self):
        with self.assertRaisesRegex(TypeError, "key must be a string, list"):
            self.dataframe[42]


class DataFrameLiteralTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        self.dataframe = pf.from_records([(1,)], schema=["id"])

    def test_lit_supports_inferred_and_explicit_types(self):
        result = self.dataframe.select(
            inferred_int=pf.lit(2),
            inferred_string=pf.lit("x"),
            explicit_int=pf.lit(3, pf.DataType.int64()),
            explicit_large_int=pf.lit(1 << 40, pf.DataType.int64()),
            explicit_string=pf.lit("y", pf.DataType.string()),
        )

        self.assert_dataframe_schema(
            result,
            [
                "inferred_int",
                "inferred_string",
                "explicit_int",
                "explicit_large_int",
                "explicit_string",
            ],
            [
                TableDataTypes.INT().not_null(),
                TableDataTypes.CHAR(1).not_null(),
                TableDataTypes.BIGINT().not_null(),
                TableDataTypes.BIGINT().not_null(),
                TableDataTypes.STRING().not_null(),
            ],
        )

    def test_lit_supports_explicitly_typed_nulls(self):
        result = self.dataframe.select(
            null_int=pf.lit(None, pf.DataType.int64()),
            null_string=pf.lit(None, pf.DataType.string()),
        )

        self.assert_dataframe_schema(
            result,
            ["null_int", "null_string"],
            [TableDataTypes.BIGINT(), TableDataTypes.STRING()],
        )

    def test_lit_supports_small_int_for_non_nullable_bigint(self):
        non_nullable_bigint = pf.DataType(TableDataTypes.BIGINT().not_null())
        result = self.dataframe.select(value=pf.lit(3, non_nullable_bigint))

        self.assert_dataframe_schema(
            result,
            ["value"],
            [TableDataTypes.BIGINT().not_null()],
        )

    def test_lit_rejects_values_incompatible_with_explicit_type(self):
        incompatible_values = [
            (3.14, pf.DataType.int64()),
            ("abc", pf.DataType.int64()),
            (42, pf.DataType.string()),
        ]
        for value, data_type in incompatible_values:
            with self.subTest(value=value, data_type=data_type):
                with self.assertRaises(Py4JJavaError):
                    pf.lit(value, data_type)

    def test_lit_rejects_non_dataframe_data_type(self):
        with self.assertRaisesRegex(
            TypeError, "data_type must be a pyflink.dataframe.DataType"
        ):
            pf.lit(1, object())


class DataFrameITTests(PyFlinkStreamDataFrameTestCase):
    def test_basic_functionality(self):
        df = pf.from_dict(
            {
                "name": [
                    "expression",
                    "Alice",
                    "sql",
                    "constraint",
                    "null_constraint",
                    "callable",
                ],
                "ignored": ["unused"],
                "id": [0, 1, 2, 3, 4, 6],
                "age": [20, 30, 40, 50, 60, 70],
                "score": [0.95, 0.95, 0.7, 0.95, 0.95, 0.95],
                "city": ["SF", "SF", "SF", "NYC", "SF", "SF"],
                "destination": [None, None, None, None, "Paris", None],
            },
            schema=["id", "name", "age", "score", "city", "destination"],
        )

        result = (
            df[df["id"] > 0]
            .filter(
                "score >= 0.9",
                lambda current: current["id"] < 6,
                city="SF",
                destination=None,
            )
            .with_column(
                "age_next_year",
                lambda current: current["age"] + 1,
            )
            .with_column("age", pf.col("age") + 1)
            .select(
                "id",
                "name",
                "age",
                age_next_year=pf.col("age_next_year"),
                inferred_int=pf.lit(2),
                inferred_string=pf.lit("x"),
                explicit_int=pf.lit(3, pf.DataType.int64()),
                explicit_large_int=pf.lit(1 << 40, pf.DataType.int64()),
                explicit_string=pf.lit("y", pf.DataType.string()),
                null_int=pf.lit(None, pf.DataType.int64()),
                null_string=pf.lit(None, pf.DataType.string()),
                non_nullable_int=pf.lit(
                    3,
                    pf.DataType(TableDataTypes.BIGINT().not_null()),
                ),
            )[
                (
                    "name",
                    "id",
                    "age",
                    "age_next_year",
                    "inferred_int",
                    "inferred_string",
                    "explicit_int",
                    "explicit_large_int",
                    "explicit_string",
                    "null_int",
                    "null_string",
                    "non_nullable_int",
                )
            ]
        )

        self.assertEqual(
            result.collect(),
            [Row("Alice", 1, 31, 31, 2, "x", 3, 1 << 40, "y", None, None, 3)],
        )


if __name__ == "__main__":
    unittest.main()
