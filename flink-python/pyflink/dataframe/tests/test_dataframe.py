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

import array
import decimal
import unittest
from datetime import date, datetime, time, timedelta, timezone
from typing import NamedTuple

import pandas as pd
import pyarrow as pa
import pyflink.dataframe as pf
from py4j.protocol import Py4JJavaError
from pyflink.common import Row
from pyflink.table import (
    DataTypes as TableDataTypes,
    EnvironmentSettings,
    TableEnvironment,
    TableSchema,
)
from pyflink.table.expression import Expression
from pyflink.table.types import LocalZonedTimestampType, TimestampType
from pyflink.testing.test_case_utils import (
    PyFlinkDataFrameUTTestCase,
    PyFlinkITTestCase,
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


class _PandasTable:
    def __init__(self, result=None, error=None):
        self._result = result
        self._error = error

    def to_pandas(self):
        if self._error is not None:
            raise self._error
        return self._result


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


class DataFrameConversionTests(unittest.TestCase):
    def test_to_table_returns_underlying_table(self):
        table = _PandasTable()

        self.assertIs(pf.DataFrame(table).to_table(), table)

    def test_to_pandas_delegates_to_underlying_table(self):
        expected = pd.DataFrame({"id": [1]})

        self.assertIs(pf.DataFrame(_PandasTable(expected)).to_pandas(), expected)

    def test_to_pandas_propagates_errors(self):
        with self.assertRaisesRegex(RuntimeError, "conversion failed"):
            pf.DataFrame(
                _PandasTable(error=RuntimeError("conversion failed"))
            ).to_pandas()


class DataFrameCompositionTests(unittest.TestCase):
    def test_pipe_forwards_dataframe_arguments_and_return_value(self):
        dataframe = pf.DataFrame(object())
        expected = object()

        def transform(current, value, *, label):
            self.assertIs(current, dataframe)
            self.assertEqual(value, 42)
            self.assertEqual(label, "answer")
            return expected

        self.assertIs(dataframe.pipe(transform, 42, label="answer"), expected)

    def test_aliases_reference_the_original_methods(self):
        self.assertIs(pf.DataFrame.where, pf.DataFrame.filter)
        self.assertIs(pf.DataFrame.drop, pf.DataFrame.drop_columns)
        self.assertIs(pf.DataFrame.rename, pf.DataFrame.rename_columns)


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

    def test_from_pandas_and_arrow_rename_columns_positionally(self):
        inputs = [
            pd.DataFrame(
                {"original_id": [1], "original_ts": [datetime(2026, 1, 1)]}
            ),
            pa.table(
                {
                    "original_id": pa.array([1], type=pa.int64()),
                    "original_ts": pa.array(
                        [datetime(2026, 1, 1)], type=pa.timestamp("us")
                    ),
                }
            ),
        ]
        for creator, data in zip((pf.from_pandas, pf.from_arrow), inputs):
            with self.subTest(creator=creator.__name__):
                dataframe = creator(data, schema=["id", "ts"])
                self.assert_dataframe_schema(dataframe, ["id", "ts"])

        duplicate_pdf = pd.DataFrame(
            [[1, "Alice"], [2, "Bob"]], columns=["value", "value"]
        )
        dataframe = pf.from_pandas(duplicate_pdf, schema=["id", "name"])
        self.assert_dataframe_schema(
            dataframe,
            ["id", "name"],
            [TableDataTypes.BIGINT(), TableDataTypes.STRING()],
        )

    def test_from_pandas_normalizes_inferred_column_names(self):
        dataframe = pf.from_pandas(pd.DataFrame([[1, 2]]))

        self.assert_dataframe_schema(
            dataframe,
            ["0", "1"],
            [TableDataTypes.BIGINT(), TableDataTypes.BIGINT()],
        )

    def test_empty_pandas_and_arrow_inputs_preserve_inferred_types(self):
        inputs = [
            (
                pf.from_pandas,
                pd.DataFrame({"id": pd.Series([], dtype="int64")}),
            ),
            (
                pf.from_arrow,
                pa.table({"id": pa.array([], type=pa.int64())}),
            ),
        ]
        for creator, data in inputs:
            with self.subTest(creator=creator.__name__):
                dataframe = creator(data)
                self.assert_dataframe_schema(
                    dataframe,
                    ["id"],
                    [TableDataTypes.BIGINT()],
                )

    def test_from_pandas_schema_inference(self):
        pdf = pd.DataFrame(
            {
                "original_id": [1.0, None],
                "original_name": ["Alice", None],
                "original_ts": pd.Series(
                    pd.to_datetime(
                        ["2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"]
                    )
                ),
            }
        )
        names = ["id", "name", "ts"]

        dataframe_schema = (
            pf.from_pandas(pdf, schema=names).to_table().get_resolved_schema()
        )
        table_schema = self.t_env.from_pandas(
            pdf, schema=names
        ).get_resolved_schema()

        self.assertEqual(
            table_schema.get_column_names(), dataframe_schema.get_column_names()
        )
        self.assertEqual(
            table_schema.get_column_data_types(),
            dataframe_schema.get_column_data_types(),
        )

        empty_pdf = pd.DataFrame(
            {
                "original_id": pd.Series([], dtype="float64"),
                "original_name": pd.Series([], dtype="string"),
                "original_ts": pd.Series([], dtype="datetime64[ns, UTC]"),
            }
        )
        empty_schema = pf.from_pandas(
            empty_pdf, schema=names
        ).to_table().get_resolved_schema()
        self.assertEqual(
            dataframe_schema.get_column_names(), empty_schema.get_column_names()
        )
        self.assertEqual(
            dataframe_schema.get_column_data_types(),
            empty_schema.get_column_data_types(),
        )

    def test_timezone_aware_creation_supports_java_timezone_ids(self):
        original_timezone = self.t_env.get_config().get_local_timezone()
        self.t_env.get_config().set_local_timezone("SystemV/PST8PDT")
        try:
            dataframe = pf.from_arrow(
                pa.table({
                    "ts": pa.array([0], type=pa.timestamp("ms", tz="UTC")),
                })
            )
            self.assert_dataframe_schema(
                dataframe,
                ["ts"],
                [TableDataTypes.TIMESTAMP(3)],
            )
        finally:
            self.t_env.get_config().set_local_timezone(original_timezone)

    def test_creators_attach_and_normalize_watermarks(self):
        timestamp = datetime(2026, 1, 1, 0, 0, 0, 123456)
        creators = [
            (
                lambda: pf.from_dict(
                    {"ts": [timestamp]},
                    watermark=("ts", "ts - INTERVAL '1' SECOND"),
                ),
                LocalZonedTimestampType,
            ),
            (
                lambda: pf.from_records(
                    [{"ts": timestamp}],
                    watermark=("ts", "ts - INTERVAL '1' SECOND"),
                ),
                LocalZonedTimestampType,
            ),
            (
                lambda: pf.from_pandas(
                    pd.DataFrame(
                        {
                            "ts": pd.Series(
                                [timestamp.replace(tzinfo=timezone.utc)],
                                dtype="datetime64[us, UTC]",
                            )
                        }
                    ),
                    watermark=("ts", "ts - INTERVAL '1' SECOND"),
                ),
                TimestampType,
            ),
            (
                lambda: pf.from_arrow(
                    pa.table(
                        {
                            "ts": pa.array(
                                [timestamp.replace(tzinfo=timezone.utc)],
                                type=pa.timestamp("us", tz="UTC"),
                            )
                        }
                    ),
                    watermark=("ts", "ts - INTERVAL '1' SECOND"),
                ),
                TimestampType,
            ),
        ]
        for creator, expected_type in creators:
            with self.subTest(creator=creator):
                resolved_schema = creator().to_table().get_resolved_schema()
                timestamp_type = resolved_schema.get_column_data_types()[0]
                self.assertIsInstance(timestamp_type, expected_type)
                self.assertEqual(timestamp_type.precision, 3)
                watermark_specs = resolved_schema.get_watermark_specs()
                self.assertEqual(len(watermark_specs), 1)
                self.assertEqual(watermark_specs[0].get_rowtime_attribute(), "ts")

    def test_watermark_requires_existing_timestamp_column(self):
        invalid_watermarks = [
            (("missing", "ts"), "watermark column 'missing' is not present"),
            (("id", "id"), "watermark column 'id' must have a timestamp type"),
        ]
        for watermark, message in invalid_watermarks:
            with self.subTest(watermark=watermark):
                with self.assertRaisesRegex(ValueError, message):
                    pf.from_records(
                        [{"id": 1, "ts": datetime(2026, 1, 1)}],
                        watermark=watermark,
                    )

    def test_from_table_and_to_table_preserve_identity(self):
        table = self.t_env.from_elements([(1,)], ["id"])

        self.assertIs(pf.from_table(table).to_table(), table)


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

    def test_with_columns_adds_and_replaces_positional_and_named_columns(self):
        result = self.dataframe.with_columns(
            (pf.col("id") + 1).alias("id"),
            (pf.col("age") + 2).alias("age_in_two_years"),
            age_next_year=pf.col("age") + 1,
            doubled_age=pf.col("age") * 2,
        )

        self.assert_dataframe_schema(
            result,
            [
                "id",
                "name",
                "age",
                "age_in_two_years",
                "age_next_year",
                "doubled_age",
            ],
            [
                TableDataTypes.BIGINT(),
                TableDataTypes.STRING(),
                TableDataTypes.BIGINT(),
                TableDataTypes.BIGINT(),
                TableDataTypes.BIGINT(),
                TableDataTypes.BIGINT(),
            ],
        )

    def test_with_columns_rejects_non_expressions(self):
        invalid_calls = [
            ("positional", lambda: self.dataframe.with_columns(42), "exprs"),
            (
                "named",
                lambda: self.dataframe.with_columns(answer=42),
                "named_exprs",
            ),
        ]
        for name, invalid_call, message in invalid_calls:
            with self.subTest(name=name):
                with self.assertRaisesRegex(TypeError, message):
                    invalid_call()


class DataFrameDropColumnsTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        self.dataframe = pf.from_records(
            [(1, "Alice", 30)],
            schema=["id", "name", "age"],
        )

    def test_drop_alias_accepts_names_and_expressions(self):
        result = self.dataframe.drop("name", pf.col("age"))

        self.assert_dataframe_schema(
            result,
            ["id"],
            [TableDataTypes.BIGINT()],
        )

    def test_drop_columns_handles_missing_names_and_no_op(self):
        with self.assertRaisesRegex(ValueError, "Column 'missing' not found"):
            self.dataframe.drop_columns("missing")

        self.assertIs(
            self.dataframe.drop_columns("missing", strict=False),
            self.dataframe,
        )
        self.assertIs(self.dataframe.drop_columns(), self.dataframe)

    def test_drop_columns_rejects_invalid_arguments(self):
        invalid_calls = [
            (
                "column",
                lambda: self.dataframe.drop_columns(42),
                "columns must be strings or expressions",
            ),
            (
                "strict",
                lambda: self.dataframe.drop_columns("id", strict="yes"),
                "strict must be a boolean",
            ),
        ]
        for name, invalid_call, message in invalid_calls:
            with self.subTest(name=name):
                with self.assertRaisesRegex(TypeError, message):
                    invalid_call()


class DataFrameRenameColumnsTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        self.dataframe = pf.from_records(
            [(1, "Alice", 30)],
            schema=["id", "name", "age"],
        )

    def test_rename_columns_supports_all_input_forms(self):
        cases = [
            (
                "mapping_alias",
                lambda: self.dataframe.rename(
                    {"id": "identifier", "missing": "ignored"}
                ),
                ["identifier", "name", "age"],
            ),
            (
                "mapping_keyword",
                lambda: self.dataframe.rename_columns(
                    mapping={"name": "customer"}
                ),
                ["id", "customer", "age"],
            ),
            (
                "callable",
                lambda: self.dataframe.rename_columns(str.upper),
                ["ID", "NAME", "AGE"],
            ),
            (
                "pairs",
                lambda: self.dataframe.rename_columns(
                    "id", "identifier", "age", "years"
                ),
                ["identifier", "name", "years"],
            ),
        ]
        for name, rename, expected_columns in cases:
            with self.subTest(name=name):
                self.assert_dataframe_schema(rename(), expected_columns)

    def test_rename_columns_returns_self_when_nothing_changes(self):
        self.assertIs(
            self.dataframe.rename_columns({"missing": "ignored"}),
            self.dataframe,
        )

    def test_rename_columns_rejects_invalid_arguments(self):
        invalid_calls = [
            (
                "missing_mapping",
                lambda: self.dataframe.rename_columns(),
                TypeError,
                "mapping must be a dictionary or callable",
            ),
            (
                "odd_pairs",
                lambda: self.dataframe.rename_columns("id", "identifier", "age"),
                ValueError,
                "must be old/new name pairs",
            ),
            (
                "non_string_pair",
                lambda: self.dataframe.rename_columns("id", 42),
                TypeError,
                "column names must be strings",
            ),
            (
                "non_string_mapping",
                lambda: self.dataframe.rename_columns({"id": 42}),
                TypeError,
                "mapping keys and values must be strings",
            ),
            (
                "invalid_callable_result",
                lambda: self.dataframe.rename_columns(lambda name: 42),
                TypeError,
                "callable must return a string",
            ),
            (
                "ambiguous_mapping",
                lambda: self.dataframe.rename_columns(
                    {"id": "identifier"}, mapping={"name": "customer"}
                ),
                ValueError,
                "either positional arguments or mapping",
            ),
        ]
        for name, invalid_call, error, message in invalid_calls:
            with self.subTest(name=name):
                with self.assertRaisesRegex(error, message):
                    invalid_call()


class DataFramePropertyTests(PyFlinkDataFrameUTTestCase):
    def test_schema_exposes_ordered_metadata(self):
        dataframe = pf.from_records(
            [(1, "Alice")],
            schema=["id", "name"],
        )

        self.assertIsInstance(dataframe.schema, TableSchema)
        self.assertEqual(dataframe.schema.get_field_names(), ["id", "name"])

    def test_columns_returns_defensive_ordered_list(self):
        dataframe = pf.from_records(
            [(1, "Alice")],
            schema=["id", "name"],
        )

        columns = dataframe.columns
        self.assertEqual(columns, ["id", "name"])
        columns.append("mutated")
        self.assertEqual(dataframe.columns, ["id", "name"])


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

    def test_lit_infers_supported_python_types(self):
        literal_values = {
            "inferred_bool": True,
            "inferred_int": 2,
            "inferred_bigint": 1 << 40,
            "inferred_float": 1.25,
            "inferred_string": "x",
            "inferred_bytes": b"x",
            "inferred_bytearray": bytearray(b"x"),
            "inferred_decimal": decimal.Decimal("1.25"),
            "inferred_date": date(2026, 8, 3),
            "inferred_time": time(1, 2, 3),
            "inferred_timestamp": datetime(2026, 8, 3, 1, 2, 3),
            "inferred_aware_timestamp": datetime(
                2026, 8, 3, 1, 2, 3, tzinfo=timezone.utc
            ),
            "inferred_timedelta": timedelta(days=1, seconds=2, microseconds=3000),
            "inferred_list": ["abc"],
            "inferred_nested_list": [[date(2026, 8, 3)]],
            "inferred_tuple": (1, 2),
            "inferred_array": array.array("h", [1, 2]),
        }
        result = self.dataframe.select(
            **{name: pf.lit(value) for name, value in literal_values.items()}
        )

        self.assert_dataframe_schema(
            result,
            list(literal_values),
            [
                TableDataTypes.BOOLEAN().not_null(),
                TableDataTypes.INT().not_null(),
                TableDataTypes.BIGINT().not_null(),
                TableDataTypes.DOUBLE().not_null(),
                TableDataTypes.CHAR(1).not_null(),
                TableDataTypes.BINARY(1).not_null(),
                TableDataTypes.BINARY(1).not_null(),
                TableDataTypes.DECIMAL(3, 2).not_null(),
                TableDataTypes.DATE().not_null(),
                TableDataTypes.TIME().not_null(),
                TableDataTypes.TIMESTAMP(0).not_null(),
                TableDataTypes.TIMESTAMP(0).not_null(),
                TableDataTypes.INTERVAL(
                    TableDataTypes.DAY(1), TableDataTypes.SECOND(3)
                ),
                TableDataTypes.ARRAY(TableDataTypes.CHAR(3)).not_null(),
                TableDataTypes.ARRAY(
                    TableDataTypes.ARRAY(TableDataTypes.DATE())
                ).not_null(),
                TableDataTypes.ARRAY(TableDataTypes.INT()).not_null(),
                TableDataTypes.ARRAY(TableDataTypes.SMALLINT()).not_null(),
            ],
        )

    def test_lit_supports_explicit_types(self):
        list_type = pf.DataType.list(pf.DataType.int16())
        map_type = pf.DataType.map(pf.DataType.int16(), pf.DataType.float32())
        struct_type = pf.DataType.struct(
            {
                "small_value": pf.DataType.int16(),
                "float_value": pf.DataType.float32(),
            }
        )
        result = self.dataframe.select(
            explicit_int8=pf.lit(3, pf.DataType.int8()),
            explicit_int16=pf.lit(3, pf.DataType.int16()),
            explicit_int32=pf.lit(3, pf.DataType.int32()),
            explicit_int64=pf.lit(3, pf.DataType.int64()),
            explicit_float32=pf.lit(1.25, pf.DataType.float32()),
            explicit_float64=pf.lit(1.25, pf.DataType.float64()),
            explicit_decimal=pf.lit(decimal.Decimal("1.25"), pf.DataType.decimal(3, 2)),
            explicit_bool=pf.lit(True, pf.DataType.bool()),
            explicit_string=pf.lit("y", pf.DataType.string()),
            explicit_fixed_string=pf.lit("y", pf.DataType.fixed_size_string(1)),
            explicit_binary=pf.lit(b"y", pf.DataType.binary()),
            explicit_fixed_binary=pf.lit(b"y", pf.DataType.fixed_size_binary(1)),
            explicit_date=pf.lit(date(2026, 8, 3), pf.DataType.date()),
            explicit_time=pf.lit(time(1, 2, 3, 4000), pf.DataType.time(6)),
            explicit_timestamp=pf.lit(
                datetime(2026, 8, 3, 1, 2, 3, 4000),
                pf.DataType.timestamp(6),
            ),
            explicit_timestamp_ltz=pf.lit(
                datetime(
                    2026, 8, 3, 1, 2, 3, 4000, tzinfo=timezone.utc
                ),
                pf.DataType.timestamp_ltz(6),
            ),
            explicit_list=pf.lit([1, 2], list_type),
            explicit_map=pf.lit({1: 1.25}, map_type),
            explicit_struct=pf.lit((1, 1.25), struct_type),
        )

        self.assert_dataframe_schema(
            result,
            [
                "explicit_int8",
                "explicit_int16",
                "explicit_int32",
                "explicit_int64",
                "explicit_float32",
                "explicit_float64",
                "explicit_decimal",
                "explicit_bool",
                "explicit_string",
                "explicit_fixed_string",
                "explicit_binary",
                "explicit_fixed_binary",
                "explicit_date",
                "explicit_time",
                "explicit_timestamp",
                "explicit_timestamp_ltz",
                "explicit_list",
                "explicit_map",
                "explicit_struct",
            ],
            [
                TableDataTypes.TINYINT().not_null(),
                TableDataTypes.SMALLINT().not_null(),
                TableDataTypes.INT().not_null(),
                TableDataTypes.BIGINT().not_null(),
                TableDataTypes.FLOAT().not_null(),
                TableDataTypes.DOUBLE().not_null(),
                TableDataTypes.DECIMAL(3, 2).not_null(),
                TableDataTypes.BOOLEAN().not_null(),
                TableDataTypes.STRING().not_null(),
                TableDataTypes.CHAR(1).not_null(),
                TableDataTypes.BYTES().not_null(),
                TableDataTypes.BINARY(1).not_null(),
                TableDataTypes.DATE().not_null(),
                TableDataTypes.TIME(6).not_null(),
                TableDataTypes.TIMESTAMP(6).not_null(),
                TableDataTypes.TIMESTAMP_LTZ(6).not_null(),
                list_type._to_table_data_type().not_null(),
                map_type._to_table_data_type().not_null(),
                struct_type._to_table_data_type().not_null(),
            ],
        )

    def test_lit_supports_explicitly_typed_nulls(self):
        result = self.dataframe.select(
            null_int=pf.lit(None, pf.DataType.int64()),
            null_string=pf.lit(None, pf.DataType.string()),
            null_list=pf.lit(None, pf.DataType.list(pf.DataType.int16())),
            null_map=pf.lit(
                None, pf.DataType.map(pf.DataType.int16(), pf.DataType.float32())
            ),
            null_struct=pf.lit(
                None, pf.DataType.struct({"value": pf.DataType.int16()})
            ),
        )

        self.assert_dataframe_schema(
            result,
            [
                "null_int",
                "null_string",
                "null_list",
                "null_map",
                "null_struct",
            ],
            [
                TableDataTypes.BIGINT(),
                TableDataTypes.STRING(),
                TableDataTypes.ARRAY(TableDataTypes.SMALLINT()),
                TableDataTypes.MAP(TableDataTypes.SMALLINT(), TableDataTypes.FLOAT()),
                TableDataTypes.ROW(
                    [TableDataTypes.FIELD("value", TableDataTypes.SMALLINT())]
                ),
            ],
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
            ([1.25], pf.DataType.list(pf.DataType.int16())),
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


class DataFrameAggregationTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        self.dataframe = pf.from_records(
            [
                ("engineering", "east", 10),
                ("engineering", "west", 20),
                ("sales", "east", 5),
            ],
            schema=["department", "region", "amount"],
        )

    def test_global_aggregation_preserves_positional_and_named_order(self):
        result = self.dataframe.agg(
            pf.col("amount").sum.alias("total_amount"),
            row_count=pf.col("amount").count,
        )

        self.assert_dataframe_schema(
            result,
            ["total_amount", "row_count"],
            [TableDataTypes.BIGINT(), TableDataTypes.BIGINT().not_null()],
        )

    def test_grouped_aggregation_emits_string_and_expression_keys_first(self):
        grouped = self.dataframe.group_by("department", pf.col("region"))

        self.assertIsInstance(grouped, pf.GroupedDataFrame)
        result = grouped.agg(
            pf.col("amount").sum.alias("total_amount"),
            row_count=pf.col("amount").count,
        )

        self.assert_dataframe_schema(
            result,
            ["department", "region", "total_amount", "row_count"],
            [
                TableDataTypes.STRING(),
                TableDataTypes.STRING(),
                TableDataTypes.BIGINT(),
                TableDataTypes.BIGINT().not_null(),
            ],
        )

    def test_group_by_requires_grouping_key(self):
        with self.assertRaisesRegex(ValueError, "requires at least one grouping key"):
            self.dataframe.group_by()

    def test_group_by_rejects_unsupported_key_type(self):
        with self.assertRaisesRegex(
            TypeError, "grouping keys must be strings or expressions"
        ):
            self.dataframe.group_by(42)

    def test_global_aggregation_requires_aggregation(self):
        with self.assertRaisesRegex(ValueError, "requires at least one aggregation"):
            self.dataframe.agg()

    def test_global_aggregation_rejects_unsupported_positional_type(self):
        with self.assertRaisesRegex(TypeError, "aggregations must be expressions"):
            self.dataframe.agg(42)

    def test_global_aggregation_rejects_unsupported_named_type(self):
        with self.assertRaisesRegex(TypeError, "aggregations must be expressions"):
            self.dataframe.agg(total=42)

    def test_grouped_aggregation_requires_aggregation(self):
        grouped = self.dataframe.group_by("department")
        with self.assertRaisesRegex(ValueError, "requires at least one aggregation"):
            grouped.agg()

    def test_grouped_aggregation_rejects_unsupported_positional_type(self):
        grouped = self.dataframe.group_by("department")
        with self.assertRaisesRegex(TypeError, "aggregations must be expressions"):
            grouped.agg(42)

    def test_grouped_aggregation_rejects_unsupported_named_type(self):
        grouped = self.dataframe.group_by("department")
        with self.assertRaisesRegex(TypeError, "aggregations must be expressions"):
            grouped.agg(total=42)

    def test_global_aggregation_delegates_expression_legality_to_planner(self):
        with self.assertRaisesRegex(Py4JJavaError, "ValidationException"):
            self.dataframe.agg(pf.col("amount"))

    def test_grouped_aggregation_delegates_ambiguous_output_to_planner(self):
        with self.assertRaisesRegex(Py4JJavaError, "ValidationException"):
            self.dataframe.group_by("department").agg(
                department=pf.col("amount").sum
            )


class DataFrameITTests(PyFlinkStreamDataFrameTestCase):
    def test_from_records(self):
        dataframe = pf.from_records(
            [(1, "Alice"), (2, "Bob")],
            schema=["id", "name"],
        )

        self.assertEqual(
            dataframe.collect(),
            [Row(1, "Alice"), Row(2, "Bob")],
        )

    def test_watermark_precision_normalization_floors_pre_epoch_timestamps(self):
        original_timezone = self.t_env.get_config().get_local_timezone()
        self.t_env.get_config().set_local_timezone("UTC")
        try:
            timestamp = datetime(1969, 12, 31, 23, 59, 59, 999999)
            creators = [
                (
                    "from_dict",
                    lambda: pf.from_dict(
                        {"ts": [timestamp]},
                        watermark=("ts", "ts - INTERVAL '1' SECOND"),
                    ),
                ),
                (
                    "from_records",
                    lambda: pf.from_records(
                        [{"ts": timestamp}],
                        watermark=("ts", "ts - INTERVAL '1' SECOND"),
                    ),
                ),
            ]

            for name, creator in creators:
                with self.subTest(creator=name):
                    result = creator().select(
                        ts=pf.col("ts").cast(TableDataTypes.STRING())
                    )
                    self.assertEqual(
                        result.collect(), [Row("1969-12-31 23:59:59.999")]
                    )
        finally:
            self.t_env.get_config().set_local_timezone(original_timezone)

    def test_pandas_to_pandas_round_trip(self):
        original_timezone = self.t_env.get_config().get_local_timezone()
        self.t_env.get_config().set_local_timezone("America/New_York")
        try:
            first_fold = pd.Timestamp("2026-11-01T05:30:00.123Z")
            second_fold = pd.Timestamp("2026-11-01T06:30:00.123Z")
            pdf = pd.DataFrame(
                {
                    "id": [0, 1, 2, 3],
                    "ts": pd.Series(
                        [None, first_fold, second_fold, None],
                        dtype="datetime64[ms, UTC]",
                    ),
                }
            )

            result = (
                pf.from_pandas(pdf)
                .filter(pf.col("id") > 0)
                .with_column("id_plus_one", pf.col("id") + 1)
                .select("id", "id_plus_one", "ts")
                .to_pandas()
                .sort_values("id")
                .reset_index(drop=True)
            )

            self.assertEqual(list(result.columns), ["id", "id_plus_one", "ts"])
            self.assertEqual(result["id"].tolist(), [1, 2, 3])
            self.assertEqual(result["id_plus_one"].tolist(), [2, 3, 4])
            self.assertEqual(result["ts"].isna().tolist(), [False, False, True])
            local_fold = pd.Timestamp("2026-11-01T01:30:00.123")
            self.assertEqual(
                result["ts"].tolist()[:2],
                [local_fold, local_fold],
            )
        finally:
            self.t_env.get_config().set_local_timezone(original_timezone)

    def test_lit_supports_inferred_and_explicit_types(self):
        dataframe = pf.from_records([(1,)], schema=["id"])
        map_type = pf.DataType.map(pf.DataType.int16(), pf.DataType.float32())
        struct_type = pf.DataType.struct(
            {
                "small_value": pf.DataType.int16(),
                "float_value": pf.DataType.float32(),
            }
        )

        result = dataframe.select(
            inferred_date=pf.lit(date(2026, 8, 3)),
            inferred_list=pf.lit(["abc"]),
            explicit_small_int=pf.lit(1, pf.DataType.int16()),
            explicit_float=pf.lit(1.25, pf.DataType.float32()),
            explicit_list=pf.lit(
                [1, 2],
                pf.DataType.list(pf.DataType.int16()),
            ),
            explicit_map=pf.lit({1: 1.25}, map_type),
            explicit_struct=pf.lit((1, 1.25), struct_type),
        )

        self.assertEqual(
            result.collect(),
            [
                Row(
                    date(2026, 8, 3),
                    ["abc"],
                    1,
                    1.25,
                    [1, 2],
                    {1: 1.25},
                    Row(1, 1.25),
                )
            ],
        )

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
            .where(
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
            .with_columns(
                (pf.col("age_next_year") + 1).alias("age_in_two_years"),
                score_percent=pf.col("score") * 100,
            )
            .drop("score", "city", "destination")
            .rename({"name": "customer_name"})
            .select(
                "id",
                "customer_name",
                "age",
                "age_next_year",
                "age_in_two_years",
                "score_percent",
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
                    "customer_name",
                    "id",
                    "age",
                    "age_next_year",
                    "age_in_two_years",
                    "score_percent",
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
            [
                Row(
                    "Alice",
                    1,
                    31,
                    31,
                    32,
                    95.0,
                    2,
                    "x",
                    3,
                    1 << 40,
                    "y",
                    None,
                    None,
                    3,
                )
            ],
        )


class DataFrameDropNullTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        self.dataframe = pf.from_records(
            [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": None, "age": 25},
                {"id": 3, "name": "Bob", "age": None},
                {"id": 4, "name": None, "age": None},
            ],
            schema=["id", "name", "age"],
        )
        self.expected_schema = [
            TableDataTypes.BIGINT(),
            TableDataTypes.STRING(),
            TableDataTypes.BIGINT(),
        ]

    def test_drop_null_without_subset_checks_all_columns(self):
        result = self.dataframe.drop_null()
        self.assert_dataframe_schema(result, ["id", "name", "age"], self.expected_schema)

    def test_drop_null_with_single_column_subset(self):
        result = self.dataframe.drop_null(subset=["age"])
        self.assert_dataframe_schema(result, ["id", "name", "age"], self.expected_schema)

    def test_drop_null_with_multiple_column_subset(self):
        result = self.dataframe.drop_null(subset=["name", "age"])
        self.assert_dataframe_schema(result, ["id", "name", "age"], self.expected_schema)

    def test_drop_null_with_empty_subset_returns_unchanged(self):
        result = self.dataframe.drop_null(subset=[])
        self.assert_dataframe_schema(result, ["id", "name", "age"], self.expected_schema)
        # Verify it's a no-op by checking the result is the same DataFrame
        self.assertEqual(result._table, self.dataframe._table)

    def test_drop_null_with_invalid_column_raises_error(self):
        with self.assertRaises(ValueError) as context:
            self.dataframe.drop_null(subset=["invalid_column"])
        self.assertIn("Columns not found in DataFrame", str(context.exception))

    def test_drop_null_with_non_list_subset_raises_error(self):
        with self.assertRaises(TypeError) as context:
            self.dataframe.drop_null(subset="age")
        self.assertIn("subset must be a list of strings", str(context.exception))


class DataFrameDropNanTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        self.dataframe = pf.from_records(
            [
                {"id": 1, "score": 0.95},
                {"id": 2, "score": float('nan')},
                {"id": 3, "score": 0.85},
            ],
            schema=["id", "score"],
        )
        self.expected_schema = [TableDataTypes.BIGINT(), TableDataTypes.DOUBLE()]

    def test_drop_nan_without_subset_checks_all_columns(self):
        result = self.dataframe.drop_nan()
        self.assert_dataframe_schema(result, ["id", "score"], self.expected_schema)

    def test_drop_nan_with_subset(self):
        result = self.dataframe.drop_nan(subset=["score"])
        self.assert_dataframe_schema(result, ["id", "score"], self.expected_schema)

    def test_drop_nan_with_multiple_column_subset(self):
        result = self.dataframe.drop_nan(subset=["id", "score"])
        self.assert_dataframe_schema(result, ["id", "score"], self.expected_schema)

    def test_drop_nan_with_empty_subset_returns_unchanged(self):
        result = self.dataframe.drop_nan(subset=[])
        self.assert_dataframe_schema(result, ["id", "score"], self.expected_schema)
        # Verify it's a no-op by checking the result is the same DataFrame
        self.assertEqual(result._table, self.dataframe._table)

    def test_drop_nan_with_invalid_column_raises_error(self):
        with self.assertRaises(ValueError) as context:
            self.dataframe.drop_nan(subset=["invalid_column"])
        self.assertIn("Columns not found in DataFrame", str(context.exception))

    def test_drop_nan_with_non_list_subset_raises_error(self):
        with self.assertRaises(TypeError) as context:
            self.dataframe.drop_nan(subset="score")
        self.assertIn("subset must be a list of strings", str(context.exception))


class DataFrameFillNullTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        self.dataframe = pf.from_records(
            [
                {"id": 1, "name": "Alice", "quantity": 10},
                {"id": 2, "name": None, "quantity": None},
                {"id": 3, "name": "Bob", "quantity": 5},
            ],
            schema=["id", "name", "quantity"],
        )
        self.expected_schema = [
            TableDataTypes.BIGINT(),
            TableDataTypes.STRING(),
            TableDataTypes.BIGINT(),
        ]

    def test_fill_null_with_numeric_value(self):
        result = self.dataframe.fill_null(0, subset=["quantity"])
        self.assert_dataframe_schema(result, ["id", "name", "quantity"], self.expected_schema)

    def test_fill_null_with_string_value(self):
        result = self.dataframe.fill_null("unknown", subset=["name"])
        self.assert_dataframe_schema(result, ["id", "name", "quantity"], self.expected_schema)

    def test_fill_null_without_subset_fills_all_columns(self):
        result = self.dataframe.fill_null(0)
        self.assert_dataframe_schema(result, ["id", "name", "quantity"], self.expected_schema)

    def test_fill_null_with_empty_subset_returns_unchanged(self):
        result = self.dataframe.fill_null(0, subset=[])
        self.assert_dataframe_schema(result, ["id", "name", "quantity"], self.expected_schema)
        # Verify it's a no-op by checking the result is the same DataFrame
        self.assertEqual(result._table, self.dataframe._table)

    def test_fill_null_with_invalid_column_raises_error(self):
        with self.assertRaises(ValueError) as context:
            self.dataframe.fill_null(0, subset=["invalid_column"])
        self.assertIn("Columns not found in DataFrame", str(context.exception))

    def test_fill_null_with_non_list_subset_raises_error(self):
        with self.assertRaises(TypeError) as context:
            self.dataframe.fill_null(0, subset="quantity")
        self.assertIn("subset must be a list of strings", str(context.exception))


class DataFrameFillNanTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        self.dataframe = pf.from_records(
            [
                {"id": 1, "score": 0.95},
                {"id": 2, "score": float('nan')},
                {"id": 3, "score": 0.85},
            ],
            schema=["id", "score"],
        )
        self.expected_schema = [TableDataTypes.BIGINT(), TableDataTypes.DOUBLE()]

    def test_fill_nan_with_numeric_value(self):
        result = self.dataframe.fill_nan(0.0, subset=["score"])
        self.assert_dataframe_schema(result, ["id", "score"], self.expected_schema)

    def test_fill_nan_without_subset_fills_all_columns(self):
        result = self.dataframe.fill_nan(0.0)
        self.assert_dataframe_schema(result, ["id", "score"], self.expected_schema)

    def test_fill_nan_with_multiple_column_subset(self):
        result = self.dataframe.fill_nan(0.0, subset=["id", "score"])
        self.assert_dataframe_schema(result, ["id", "score"], self.expected_schema)

    def test_fill_nan_with_empty_subset_returns_unchanged(self):
        result = self.dataframe.fill_nan(0.0, subset=[])
        self.assert_dataframe_schema(result, ["id", "score"], self.expected_schema)
        # Verify it's a no-op by checking the result is the same DataFrame
        self.assertEqual(result._table, self.dataframe._table)

    def test_fill_nan_with_invalid_column_raises_error(self):
        with self.assertRaises(ValueError) as context:
            self.dataframe.fill_nan(0.0, subset=["invalid_column"])
        self.assertIn("Columns not found in DataFrame", str(context.exception))

    def test_fill_nan_with_non_list_subset_raises_error(self):
        with self.assertRaises(TypeError) as context:
            self.dataframe.fill_nan(0.0, subset="score")
        self.assertIn("subset must be a list of strings", str(context.exception))


class DataFrameBatchITTests(PyFlinkITTestCase):
    def setUp(self):
        previous_environment = pf.get_table_environment()
        self.addCleanup(pf.set_table_environment, previous_environment)
        self.t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

    def test_from_records_with_batch_table_environment(self):
        pf.set_table_environment(self.t_env)

        result = pf.from_records(
            [(1, "Alice"), (2, "Bob")],
            schema=["id", "name"],
        ).filter(pf.col("id") > 1)

        self.assertEqual(result.collect(), [Row(2, "Bob")])

    def test_grouped_aggregation_with_batch_table_environment(self):
        pf.set_table_environment(self.t_env)

        result = pf.from_records(
            [
                ("engineering", 10),
                ("engineering", 20),
                ("sales", 5),
            ],
            schema=["department", "amount"],
        ).group_by("department").agg(
            total_amount=pf.col("amount").sum,
            row_count=pf.col("amount").count,
        )

        self.assertCountEqual(
            result.collect(),
            [Row("engineering", 30, 2), Row("sales", 5, 1)],
        )


class DataFrameNullNanITTests(PyFlinkStreamDataFrameTestCase):
    def test_fill_null_type_compatibility(self):
        # Create DataFrame with mixed types
        df = pf.from_records(
            [
                {"id": 1, "name": "Alice", "score": 0.95, "active": True},
                {"id": None, "name": None, "score": None, "active": None},
            ],
            schema=["id", "name", "score", "active"],
        )

        # Fill with int - should only fill numeric columns (id, score)
        result = df.fill_null(0)
        rows = result.collect()
        self.assertEqual(rows[1][0], 0)  # id filled
        self.assertIsNone(rows[1][1])  # name not filled (type mismatch)
        self.assertEqual(rows[1][2], 0.0)  # score filled
        self.assertIsNone(rows[1][3])  # active not filled (type mismatch)

        # Fill with string - should only fill string columns (name)
        result = df.fill_null("unknown")
        rows = result.collect()
        self.assertIsNone(rows[1][0])  # id not filled (type mismatch)
        self.assertEqual(rows[1][1], "unknown")  # name filled
        self.assertIsNone(rows[1][2])  # score not filled (type mismatch)
        self.assertIsNone(rows[1][3])  # active not filled (type mismatch)

        # Fill with bool - should only fill boolean columns (active)
        result = df.fill_null(False)
        rows = result.collect()
        self.assertIsNone(rows[1][0])  # id not filled (type mismatch)
        self.assertIsNone(rows[1][1])  # name not filled (type mismatch)
        self.assertIsNone(rows[1][2])  # score not filled (type mismatch)
        self.assertEqual(rows[1][3], False)  # active filled

    def test_drop_null_removes_rows_with_null_values(self):
        df = pf.from_records(
            [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": None, "age": 25},
                {"id": 3, "name": "Bob", "age": None},
                {"id": 4, "name": None, "age": None},
            ],
            schema=["id", "name", "age"],
        )

        result = df.drop_null().collect()
        self.assertEqual(result, [Row(1, "Alice", 30)])

    def test_drop_null_with_subset_removes_rows_with_null_in_specified_columns(self):
        df = pf.from_records(
            [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": None, "age": 25},
                {"id": 3, "name": "Bob", "age": None},
            ],
            schema=["id", "name", "age"],
        )

        result = df.drop_null(subset=["age"]).collect()
        self.assertEqual(result, [Row(1, "Alice", 30), Row(2, None, 25)])

    def test_drop_nan_removes_rows_with_nan_values(self):
        df = pf.from_records(
            [
                {"id": 1, "score": 0.95},
                {"id": 2, "score": float('nan')},
                {"id": 3, "score": 0.85},
            ],
            schema=["id", "score"],
        )

        result = df.drop_nan().collect()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0], 1)
        self.assertAlmostEqual(result[0][1], 0.95)
        self.assertEqual(result[1][0], 3)
        self.assertAlmostEqual(result[1][1], 0.85)

    def test_drop_nan_with_subset_removes_rows_with_nan_in_specified_columns(self):
        df = pf.from_records(
            [
                {"id": 1, "score": 0.95, "rating": 4.5},
                {"id": 2, "score": float('nan'), "rating": 3.0},
                {"id": 3, "score": 0.85, "rating": float('nan')},
            ],
            schema=["id", "score", "rating"],
        )

        result = df.drop_nan(subset=["score"]).collect()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0], 1)
        self.assertAlmostEqual(result[0][1], 0.95)
        self.assertEqual(result[1][0], 3)
        self.assertAlmostEqual(result[1][1], 0.85)

    def test_drop_nan_preserves_null_values(self):
        """Test that drop_nan preserves NULL values (critical bug fix)."""
        df = pf.from_records(
            [
                {"id": 1, "score": 0.95},
                {"id": 2, "score": None},  # NULL should be preserved
                {"id": 3, "score": float('nan')},  # NaN should be dropped
                {"id": 4, "score": 0.85},
            ],
            schema=["id", "score"],
        )

        result = df.drop_nan(subset=["score"]).collect()
        # Should have 3 rows: id=1, id=2 (NULL preserved), id=4
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0][0], 1)
        self.assertAlmostEqual(result[0][1], 0.95)
        self.assertEqual(result[1][0], 2)
        self.assertIsNone(result[1][1])  # NULL preserved
        self.assertEqual(result[2][0], 4)
        self.assertAlmostEqual(result[2][1], 0.85)


    def test_drop_nan_with_mixed_schema_auto_filters_to_float_columns(self):
        """Test that drop_nan with subset=None only checks FLOAT/DOUBLE columns."""
        df = pf.from_records(
            [
                {"id": 1, "name": "Alice", "score": 0.95, "rating": 4.5},
                {"id": 2, "name": "Bob", "score": float('nan'), "rating": 3.0},
                {"id": 3, "name": "Charlie", "score": 0.85, "rating": float('nan')},
            ],
            schema=["id", "name", "score", "rating"],
        )

        # Should only check score and rating (FLOAT/DOUBLE), not id (INT) or name (STRING)
        result = df.drop_nan().collect()
        # Only row with id=1 has no NaN in float columns
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], 1)
        self.assertEqual(result[0][1], "Alice")
        self.assertAlmostEqual(result[0][2], 0.95)
        self.assertAlmostEqual(result[0][3], 4.5)

    def test_drop_nan_with_no_float_columns_returns_unchanged(self):
        """Test that drop_nan with no FLOAT/DOUBLE columns returns unchanged DataFrame."""
        df = pf.from_records(
            [
                {"id": 1, "name": "Alice", "active": True},
                {"id": 2, "name": "Bob", "active": False},
            ],
            schema=["id", "name", "active"],
        )

        # No float columns, should return all rows unchanged
        result = df.drop_nan().collect()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], Row(1, "Alice", True))
        self.assertEqual(result[1], Row(2, "Bob", False))




    def test_fill_nan_with_mixed_schema_auto_filters_to_float_columns(self):
        """Test that fill_nan with subset=None only fills FLOAT/DOUBLE columns."""
        df = pf.from_records(
            [
                {"id": 1, "name": "Alice", "score": 0.95, "rating": 4.5},
                {"id": 2, "name": "Bob", "score": float('nan'), "rating": 3.0},
                {"id": 3, "name": "Charlie", "score": 0.85, "rating": float('nan')},
            ],
            schema=["id", "name", "score", "rating"],
        )

        # Should only fill score and rating (FLOAT/DOUBLE), not id (INT) or name (STRING)
        result = df.fill_nan(0.0).collect()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0][0], 1)
        self.assertEqual(result[0][1], "Alice")
        self.assertAlmostEqual(result[0][2], 0.95)
        self.assertAlmostEqual(result[0][3], 4.5)
        self.assertEqual(result[1][0], 2)
        self.assertEqual(result[1][1], "Bob")
        self.assertAlmostEqual(result[1][2], 0.0)  # NaN filled
        self.assertAlmostEqual(result[1][3], 3.0)
        self.assertEqual(result[2][0], 3)
        self.assertEqual(result[2][1], "Charlie")
        self.assertAlmostEqual(result[2][2], 0.85)
        self.assertAlmostEqual(result[2][3], 0.0)  # NaN filled

    def test_fill_nan_with_no_float_columns_returns_unchanged(self):
        """Test that fill_nan with no FLOAT/DOUBLE columns returns unchanged DataFrame."""
        df = pf.from_records(
            [
                {"id": 1, "name": "Alice", "active": True},
                {"id": 2, "name": "Bob", "active": False},
            ],
            schema=["id", "name", "active"],
        )

        # No float columns, should return all rows unchanged
        result = df.fill_nan(0.0).collect()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], Row(1, "Alice", True))
        self.assertEqual(result[1], Row(2, "Bob", False))

    def test_fill_null_skips_incompatible_array_column(self):
        """Test that fill_null with incompatible type (ARRAY) skips the column."""
        df = pf.from_records(
            [
                {"id": 1, "tags": ["python", "flink"]},
                {"id": 2, "tags": None},
            ],
            schema=["id", "tags"],
        )

        # Should skip ARRAY column when value is INT
        result = df.fill_null(0).collect()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0], 1)
        self.assertEqual(result[0][1], ["python", "flink"])
        self.assertEqual(result[1][0], 2)
        self.assertIsNone(result[1][1])  # Should remain NULL

    def test_fill_null_skips_incompatible_string_column(self):
        """Test that fill_null with numeric value skips STRING columns."""
        df = pf.from_records(
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": None},
            ],
            schema=["id", "name"],
        )

        # Numeric value should skip STRING column
        result = df.fill_null(0, subset=["name"]).collect()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], Row(1, "Alice"))
        self.assertIsNone(result[1][1])  # Should remain NULL


    def test_fill_null_replaces_null_with_specified_value(self):
        df = pf.from_records(
            [
                {"id": 1, "name": "Alice", "quantity": 10},
                {"id": 2, "name": None, "quantity": None},
                {"id": 3, "name": "Bob", "quantity": 5},
            ],
            schema=["id", "name", "quantity"],
        )

        result = df.fill_null(0, subset=["quantity"]).collect()
        self.assertEqual(result, [Row(1, "Alice", 10), Row(2, None, 0), Row(3, "Bob", 5)])

    def test_fill_null_with_string_value(self):
        df = pf.from_records(
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": None},
            ],
            schema=["id", "name"],
        )

        result = df.fill_null("unknown", subset=["name"]).collect()
        self.assertEqual(result, [Row(1, "Alice"), Row(2, "unknown")])

    def test_fill_nan_replaces_nan_with_specified_value(self):
        df = pf.from_records(
            [
                {"id": 1, "score": 0.95},
                {"id": 2, "score": float('nan')},
                {"id": 3, "score": 0.85},
            ],
            schema=["id", "score"],
        )

        result = df.fill_nan(0.0, subset=["score"]).collect()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0][0], 1)
        self.assertAlmostEqual(result[0][1], 0.95)
        self.assertEqual(result[1][0], 2)
        self.assertAlmostEqual(result[1][1], 0.0)
        self.assertEqual(result[2][0], 3)
        self.assertAlmostEqual(result[2][1], 0.85)


if __name__ == "__main__":
    unittest.main()
