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
from pyflink.common import Row
from pyflink.testing.test_case_utils import (
    PyFlinkStreamDataFrameTestCase,
    PyFlinkStreamTableTestCase,
)
from pyflink.util.api_stability_decorators import PublicEvolving


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


class DataFramePublicApiTests(unittest.TestCase):
    def test_exports_minimal_public_api(self):
        expected_exports = [
            "DataFrame",
            "DataType",
            "col",
            "lit",
            "from_dict",
            "from_records",
            "set_table_environment",
            "get_table_environment",
            "get_or_create_table_environment",
        ]

        self.assertEqual(pf.__all__, expected_exports)
        for name in expected_exports:
            with self.subTest(name=name):
                self.assertTrue(hasattr(pf, name))

    def test_exports_are_public_evolving_and_versioned(self):
        for name in pf.__all__:
            exported = getattr(pf, name)
            with self.subTest(name=name):
                self.assertIn(
                    PublicEvolving,
                    getattr(exported, "__stability_decorators", set()),
                )
                self.assertIn(".. versionadded:: 2.4.0", exported.__doc__)

    def test_public_methods_are_versioned(self):
        methods = [
            pf.DataFrame.filter,
            pf.DataFrame.with_column,
            pf.DataFrame.select,
            pf.DataFrame.__getitem__,
            pf.DataFrame.collect,
            pf.DataType.int64,
            pf.DataType.string,
        ]

        for method in methods:
            with self.subTest(method=method.__name__):
                self.assertIn(".. versionadded:: 2.4.0", method.__doc__)

    def test_dataframe_methods_are_public_evolving(self):
        methods = [
            pf.DataFrame.filter,
            pf.DataFrame.with_column,
            pf.DataFrame.select,
            pf.DataFrame.__getitem__,
            pf.DataFrame.collect,
            pf.DataType.int64,
            pf.DataType.string,
        ]

        for method in methods:
            with self.subTest(method=method.__name__):
                self.assertIn(
                    PublicEvolving,
                    getattr(method, "__stability_decorators", set()),
                )

    def test_public_api_docstrings_include_examples(self):
        documented_objects = [
            pf,
            pf.DataFrame,
            pf.DataType,
            pf.col,
            pf.lit,
            pf.from_dict,
            pf.from_records,
            pf.set_table_environment,
            pf.get_table_environment,
            pf.get_or_create_table_environment,
            pf.DataFrame.filter,
            pf.DataFrame.with_column,
            pf.DataFrame.select,
            pf.DataFrame.__getitem__,
            pf.DataFrame.collect,
            pf.DataType.int64,
            pf.DataType.string,
        ]

        for documented_object in documented_objects:
            with self.subTest(documented_object=documented_object):
                self.assertIn("Example::\n\n", documented_object.__doc__)

    def test_dataframe_public_method_surface(self):
        public_methods = {
            name for name in dir(pf.DataFrame) if not name.startswith("_")
        }

        self.assertEqual(
            public_methods, {"collect", "filter", "select", "with_column"}
        )


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


class DataFrameValidationTests(unittest.TestCase):
    def setUp(self):
        self.dataframe = pf.DataFrame(_Table(_CloseableIterator()))

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

    def test_with_column_rejects_non_expression(self):
        with self.assertRaisesRegex(TypeError, "expr must be an Expression"):
            self.dataframe.with_column("answer", 42)

    def test_with_column_rejects_callable_returning_non_expression(self):
        with self.assertRaisesRegex(TypeError, "expr must be an Expression"):
            self.dataframe.with_column("answer", lambda df: 42)

    def test_with_column_rejects_non_string_name(self):
        with self.assertRaisesRegex(TypeError, "name must be a string"):
            self.dataframe.with_column(42, object())

    def test_select_rejects_non_string_column(self):
        with self.assertRaisesRegex(TypeError, "columns must be strings"):
            self.dataframe.select("id", 42)

    def test_select_rejects_non_expression_projection(self):
        with self.assertRaisesRegex(TypeError, "projections must be expressions"):
            self.dataframe.select(answer=42)

    def test_getitem_rejects_unsupported_key(self):
        with self.assertRaisesRegex(TypeError, "key must be a string, list"):
            self.dataframe[42]

    def test_lit_rejects_non_dataframe_data_type(self):
        with self.assertRaisesRegex(
            TypeError, "data_type must be a pyflink.dataframe.DataType"
        ):
            pf.lit(1, object())


class DataFrameEndToEndTests(PyFlinkStreamDataFrameTestCase):
    def test_uses_current_test_environment(self):
        self.assertIs(pf.get_table_environment(), self.t_env)

    def test_complete_minimal_job(self):
        df = pf.from_records(
            [
                (1, "Alice", 30),
                (2, "Bob", 17),
            ],
            schema=["id", "name", "age"],
        )

        result = (
            df.filter(pf.col("age") >= 18)
            .with_column("age_next_year", pf.col("age") + 1)
            .select("id", "name", "age_next_year")
        )

        self.assertEqual(result.collect(), [Row(1, "Alice", 31)])

    def test_from_dict_respects_schema_order_and_subset(self):
        result = pf.from_dict(
            {
                "name": ["Alice", "Bob"],
                "ignored": ["x", "y"],
                "id": [1, 2],
            },
            schema=["id", "name"],
        )

        self.assertEqual(
            result.collect(), [Row(1, "Alice"), Row(2, "Bob")]
        )

    def test_from_dict_uses_insertion_order_without_schema(self):
        result = pf.from_dict({"name": ["Alice"], "id": [1]})

        self.assertEqual(result.collect(), [Row("Alice", 1)])

    def test_from_records_accepts_list_records(self):
        result = pf.from_records([[1, "Alice"], [2, "Bob"]], schema=["id", "name"])

        self.assertEqual(result.collect(), [Row(1, "Alice"), Row(2, "Bob")])

    def test_from_records_accepts_general_sequence_records(self):
        result = pf.from_records(
            [range(2), range(2, 4)], schema=["left", "right"]
        )

        self.assertEqual(result.collect(), [Row(0, 1), Row(2, 3)])

    def test_from_records_infers_schema_from_mapping_records(self):
        result = pf.from_records(
            [{"name": "Alice", "id": 1}, {"name": "Bob", "id": 2}]
        )

        self.assertEqual(result.collect(), [Row("Alice", 1), Row("Bob", 2)])

    def test_from_records_applies_schema_to_mapping_records(self):
        result = pf.from_records(
            [
                {"name": "Alice", "id": 1, "ignored": "x"},
                {"name": "Bob", "id": 2, "ignored": "y"},
            ],
            schema=["id", "name"],
        )

        self.assertEqual(result.collect(), [Row(1, "Alice"), Row(2, "Bob")])

    def test_with_column_replaces_existing_column_and_select_reorders(self):
        result = (
            pf.from_records([(1, "Alice", 30)], schema=["id", "name", "age"])
            .with_column("age", pf.col("age") + 1)
            .select("name", "age", "id")
        )

        self.assertEqual(result.collect(), [Row("Alice", 31, 1)])

    def test_with_column_accepts_callable(self):
        result = (
            pf.from_records([(1, 2), (3, 4)], schema=["left", "right"])
            .with_column("total", lambda df: df["left"] + df["right"])
            .select("total")
        )

        self.assertEqual(result.collect(), [Row(3), Row(7)])

    def test_select_accepts_expression_list_and_named_projection(self):
        result = pf.from_records(
            [(1, "Alice"), (2, "Bob")], schema=["id", "name"]
        ).select(["name"], pf.col("id"), doubled=pf.col("id") * 2)

        self.assertEqual(
            result.collect(),
            [Row("Alice", 1, 2), Row("Bob", 2, 4)],
        )

    def test_select_accepts_tuple_column_group(self):
        result = pf.from_records(
            [(1, "Alice"), (2, "Bob")], schema=["id", "name"]
        ).select(("name", "id"))

        self.assertEqual(result.collect(), [Row("Alice", 1), Row("Bob", 2)])

    def test_filter_combines_predicates_and_constraints(self):
        result = pf.from_records(
            [(1, 0.9, "NYC"), (2, 0.7, "NYC"), (3, 0.95, "SF")],
            schema=["age", "score", "city"],
        ).filter(pf.col("age") > 1, pf.col("score") >= 0.8, city="SF")

        self.assertEqual(result.collect(), [Row(3, 0.95, "SF")])

    def test_filter_accepts_sql_string_predicate(self):
        result = pf.from_records(
            [(1, "Alice"), (2, "Bob")], schema=["id", "name"]
        ).filter("id > 1")

        self.assertEqual(result.collect(), [Row(2, "Bob")])

    def test_filter_accepts_callable_predicate(self):
        result = pf.from_records(
            [(1, "Alice"), (2, "Bob")], schema=["id", "name"]
        ).filter(lambda df: df["id"] > 1)

        self.assertEqual(result.collect(), [Row(2, "Bob")])

    def test_getitem_selects_columns_and_filters_rows(self):
        df = pf.from_records(
            [(1, "Alice"), (2, "Bob")], schema=["id", "name"]
        )

        result = df[df["id"] > 1][["name", "id"]]

        self.assertEqual(result.collect(), [Row("Bob", 2)])

    def test_getitem_accepts_tuple_projection(self):
        df = pf.from_records(
            [(1, "Alice"), (2, "Bob")], schema=["id", "name"]
        )

        result = df[("name", "id")]

        self.assertEqual(result.collect(), [Row("Alice", 1), Row("Bob", 2)])

    def test_lit_supports_inferred_and_explicit_types(self):
        result = (
            pf.from_records([(1,)], schema=["id"])
            .with_column("inferred_int", pf.lit(2))
            .with_column("inferred_string", pf.lit("x"))
            .with_column("explicit_int", pf.lit(3, pf.DataType.int64()))
            .with_column("explicit_string", pf.lit("y", pf.DataType.string()))
            .select(
                "inferred_int",
                "inferred_string",
                "explicit_int",
                "explicit_string",
            )
        )

        self.assertEqual(result.collect(), [Row(2, "x", 3, "y")])

    def test_lit_supports_explicitly_typed_nulls(self):
        result = pf.from_records([(1,)], schema=["id"]).select(
            null_int=pf.lit(None, pf.DataType.int64()),
            null_string=pf.lit(None, pf.DataType.string()),
        )

        self.assertEqual(result.collect(), [Row(None, None)])


class DataFrameEnvironmentCleanupTests(PyFlinkStreamTableTestCase):
    def test_previous_test_environment_is_cleared(self):
        self.assertIsNone(pf.get_table_environment())


if __name__ == "__main__":
    unittest.main()
