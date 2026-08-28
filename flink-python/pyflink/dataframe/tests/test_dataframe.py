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

import inspect
import array
import decimal
import unittest
from py4j.protocol import Py4JJavaError
from datetime import date, datetime, time, timedelta, timezone
from typing import NamedTuple
from unittest.mock import Mock, patch

import pandas as pd
import pyarrow as pa
import pyflink.dataframe as pf
from pyflink.common import Row, RowKind
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


class DataFrameSlicingTests(unittest.TestCase):
    def setUp(self):
        self.table = Mock()
        self.dataframe = pf.DataFrame(self.table)

    def test_limit_is_lazy_and_returns_new_dataframe(self):
        limited_table = Mock()
        self.table.fetch.return_value = limited_table

        result = self.dataframe.limit(3)

        self.assertIsInstance(result, pf.DataFrame)
        self.assertIs(result.to_table(), limited_table)
        self.assertIs(self.dataframe.to_table(), self.table)
        self.table.fetch.assert_called_once_with(3)
        self.table.execute.assert_not_called()

    def test_offset_is_lazy_and_returns_new_dataframe(self):
        offset_table = Mock()
        self.table.offset.return_value = offset_table

        result = self.dataframe.offset(2)

        self.assertIsInstance(result, pf.DataFrame)
        self.assertIs(result.to_table(), offset_table)
        self.assertIs(self.dataframe.to_table(), self.table)
        self.table.offset.assert_called_once_with(2)
        self.table.execute.assert_not_called()

    def test_offset_and_limit_compose(self):
        offset_table = Mock()
        limited_table = Mock()
        self.table.offset.return_value = offset_table
        offset_table.fetch.return_value = limited_table

        result = self.dataframe.offset(2).limit(3)

        self.assertIs(result.to_table(), limited_table)
        self.table.offset.assert_called_once_with(2)
        offset_table.fetch.assert_called_once_with(3)
        self.table.execute.assert_not_called()

    def test_head_delegates_to_limit(self):
        expected = pf.DataFrame(Mock())

        with patch.object(pf.DataFrame, "limit", autospec=True) as limit:
            limit.return_value = expected

            result = self.dataframe.head(3)

        self.assertIs(result, expected)
        limit.assert_called_once_with(self.dataframe, 3)
        self.table.execute.assert_not_called()

    def test_zero_is_supported(self):
        limited_table = Mock()
        offset_table = Mock()
        self.table.fetch.return_value = limited_table
        self.table.offset.return_value = offset_table

        self.assertIs(self.dataframe.limit(0).to_table(), limited_table)
        self.assertIs(self.dataframe.head(0).to_table(), limited_table)
        self.assertIs(self.dataframe.offset(0).to_table(), offset_table)

        self.assertEqual(self.table.fetch.call_count, 2)
        self.table.fetch.assert_called_with(0)
        self.table.offset.assert_called_once_with(0)
        self.table.execute.assert_not_called()

    def test_rejects_negative_values(self):
        for method_name in ("limit", "offset", "head"):
            with self.subTest(method=method_name):
                with self.assertRaisesRegex(ValueError, "n must be non-negative"):
                    getattr(self.dataframe, method_name)(-1)

        self.table.fetch.assert_not_called()
        self.table.offset.assert_not_called()
        self.table.execute.assert_not_called()

    def test_rejects_unsupported_types(self):
        for method_name in ("limit", "offset", "head"):
            for value in (True, 1.5, "1", None):
                with self.subTest(method=method_name, value=value):
                    with self.assertRaisesRegex(TypeError, "n must be an integer"):
                        getattr(self.dataframe, method_name)(value)

        self.table.fetch.assert_not_called()
        self.table.offset.assert_not_called()
        self.table.execute.assert_not_called()


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


class DataFrameDropDuplicatesTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        self.dataframe = pf.from_records(
            [
                {"id": 1, "name": "a", "score": 10},
                {"id": 1, "name": "b", "score": 20},
                {"id": 2, "name": "c", "score": 30},
            ]
        )

    def test_sql_proctime_default_keep_first(self):
        self.assert_dataframe_sql(
            self.dataframe,
            "SELECT `id`, `name`, `score` FROM (\n"
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY PROCTIME() ASC)"
            " AS `__pf_row_number`\n"
            "  FROM `SRC`\n"
            ") WHERE `__pf_row_number` = 1",
            lambda: self.dataframe.drop_duplicates(subset="id"),
        )

    def test_sql_proctime_default_keep_last(self):
        self.assert_dataframe_sql(
            self.dataframe,
            "SELECT `id`, `name`, `score` FROM (\n"
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY PROCTIME() DESC)"
            " AS `__pf_row_number`\n"
            "  FROM `SRC`\n"
            ") WHERE `__pf_row_number` = 1",
            lambda: self.dataframe.drop_duplicates(subset="id", keep="last"),
        )

    def test_sql_multi_column_subset(self):
        self.assert_dataframe_sql(
            self.dataframe,
            "SELECT `id`, `name`, `score` FROM (\n"
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY `id`, `name` ORDER BY PROCTIME() ASC)"
            " AS `__pf_row_number`\n"
            "  FROM `SRC`\n"
            ") WHERE `__pf_row_number` = 1",
            lambda: self.dataframe.drop_duplicates(subset=["id", "name"]),
        )

    def test_sql_order_by_column_name(self):
        self.assert_dataframe_sql(
            self.dataframe,
            "SELECT `id`, `name`, `score` FROM (\n"
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `score` DESC)"
            " AS `__pf_row_number`\n"
            "  FROM `SRC`\n"
            ") WHERE `__pf_row_number` = 1",
            lambda: self.dataframe.drop_duplicates(subset="id", order_by="score", keep="last"),
        )

    def test_sql_nulls_first_per_key(self):
        self.assert_dataframe_sql(
            self.dataframe,
            "SELECT `id`, `name`, `score` FROM (\n"
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY `id`"
            " ORDER BY `score` ASC NULLS FIRST, `name` ASC NULLS LAST) AS `__pf_row_number`\n"
            "  FROM `SRC`\n"
            ") WHERE `__pf_row_number` = 1",
            lambda: self.dataframe.drop_duplicates(
                subset="id", order_by=["score", "name"], nulls_first=[True, False]
            ),
        )

    def test_sql_order_by_expression_is_materialized(self):
        self.assert_dataframe_sql(
            self.dataframe,
            "SELECT `id`, `name`, `score` FROM (\n"
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `__pf_order_0` ASC)"
            " AS `__pf_row_number`\n"
            "  FROM `SRC`\n"
            ") WHERE `__pf_row_number` = 1",
            lambda: self.dataframe.drop_duplicates(subset="id", order_by=pf.col("score")),
        )

    def test_sql_after_select_lists_only_source_columns(self):
        source = self.dataframe.select("id", "score")
        self.assert_dataframe_sql(
            source,
            "SELECT `id`, `score` FROM (\n"
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY PROCTIME() ASC)"
            " AS `__pf_row_number`\n"
            "  FROM `SRC`\n"
            ") WHERE `__pf_row_number` = 1",
            lambda: source.drop_duplicates(subset="id"),
        )

    def test_sql_rank_column_avoids_collision(self):
        dataframe = pf.from_records([{"__pf_row_number": 1, "value": 2}])
        self.assert_dataframe_sql(
            dataframe,
            "SELECT `__pf_row_number`, `value` FROM (\n"
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY `value` ORDER BY PROCTIME() ASC)"
            " AS `__pf_row_number_`\n"
            "  FROM `SRC`\n"
            ") WHERE `__pf_row_number_` = 1",
            lambda: dataframe.drop_duplicates(subset="value"),
        )

    def test_whole_row_produces_no_sql(self):
        # Whole-row deduplication uses distinct(), not a generated query.
        self.assert_dataframe_sql(
            self.dataframe, None, lambda: self.dataframe.drop_duplicates()
        )

    def test_schema_preserved(self):
        self.assert_dataframe_schema(
            self.dataframe.drop_duplicates("id", order_by="score", keep="last"),
            ["id", "name", "score"],
        )

    def test_whole_row_schema_preserved(self):
        self.assert_dataframe_schema(
            self.dataframe.drop_duplicates(), ["id", "name", "score"]
        )

    def test_rejects_invalid_keep(self):
        with self.assertRaises(ValueError) as error:
            self.dataframe.drop_duplicates("id", keep="middle")
        self.assertEqual(str(error.exception), 'keep must be "first" or "last"')

    def test_rejects_empty_subset_list(self):
        with self.assertRaises(ValueError) as error:
            self.dataframe.drop_duplicates([])
        self.assertEqual(str(error.exception), "subset must not be empty")

    def test_rejects_non_string_subset(self):
        with self.assertRaises(TypeError) as error:
            self.dataframe.drop_duplicates([1])
        self.assertEqual(
            str(error.exception), "subset must be a string or a list of strings"
        )

    def test_rejects_unknown_subset_column(self):
        # The message embeds the (py4j) column list, so match only the stable prefix.
        with self.assertRaisesRegex(ValueError, "subset column 'nope' does not exist"):
            self.dataframe.drop_duplicates("nope")

    def test_rejects_unknown_order_column(self):
        # The message embeds the (py4j) column list, so match only the stable prefix.
        with self.assertRaisesRegex(ValueError, "order_by column 'nope' does not exist"):
            self.dataframe.drop_duplicates("id", order_by="nope")

    def test_rejects_order_by_without_subset(self):
        with self.assertRaises(ValueError) as error:
            self.dataframe.drop_duplicates(order_by="score")
        self.assertEqual(
            str(error.exception),
            "order_by requires subset; whole-row duplicates cannot be ordered",
        )

    def test_rejects_nulls_first_without_order_by(self):
        with self.assertRaises(ValueError) as error:
            self.dataframe.drop_duplicates("id", nulls_first=True)
        self.assertEqual(str(error.exception), "nulls_first requires order_by")

    def test_rejects_nulls_first_length_mismatch(self):
        with self.assertRaises(ValueError) as error:
            self.dataframe.drop_duplicates(
                "id", order_by="score", nulls_first=[True, False]
            )
        self.assertEqual(
            str(error.exception), "nulls_first must have the same length as order_by"
        )

    def test_rejects_nulls_first_wrong_type(self):
        with self.assertRaises(TypeError) as error:
            self.dataframe.drop_duplicates("id", order_by="score", nulls_first=["x"])
        self.assertEqual(
            str(error.exception), "nulls_first must be a boolean or a list of booleans"
        )


class DataFrameDistinctTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        self.dataframe = pf.from_records(
            [{"id": 1, "score": 10}, {"id": 1, "score": 20}]
        )

    def test_shares_drop_duplicates_signature(self):
        self.assertEqual(
            inspect.signature(pf.DataFrame.distinct),
            inspect.signature(pf.DataFrame.drop_duplicates),
        )

    def test_produces_the_same_sql_as_drop_duplicates(self):
        self.assert_dataframe_sql(
            self.dataframe,
            "SELECT `id`, `score` FROM (\n"
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `score` ASC)"
            " AS `__pf_row_number`\n"
            "  FROM `SRC`\n"
            ") WHERE `__pf_row_number` = 1",
            lambda: self.dataframe.distinct(subset="id", order_by="score"),
        )


class DataFrameUniqueTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        self.dataframe = pf.from_records(
            [{"id": 1, "score": 10}, {"id": 1, "score": 20}]
        )

    def test_shares_drop_duplicates_signature(self):
        self.assertEqual(
            inspect.signature(pf.DataFrame.unique),
            inspect.signature(pf.DataFrame.drop_duplicates),
        )

    def test_produces_the_same_sql_as_drop_duplicates(self):
        self.assert_dataframe_sql(
            self.dataframe,
            "SELECT `id`, `score` FROM (\n"
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `score` ASC)"
            " AS `__pf_row_number`\n"
            "  FROM `SRC`\n"
            ") WHERE `__pf_row_number` = 1",
            lambda: self.dataframe.unique(subset="id", order_by="score"),
        )


class DataFrameDropDuplicatesITTests(PyFlinkStreamDataFrameTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.t_env.get_config().set("table.exec.resource.default-parallelism", "1")

    @staticmethod
    def _materialize(dataframe, key=None):
        # Fold the collected changelog into the final table so assertions read as the
        # resulting rows rather than the raw +I/-U/+U events.
        columns = dataframe._table.get_resolved_schema().get_column_names()
        if key is None:
            indices = list(range(len(columns)))
        else:
            indices = [columns.index(name) for name in key]

        state = {}
        for row in dataframe.collect():
            key_value = tuple(row[index] for index in indices)
            if row.get_row_kind() in (RowKind.INSERT, RowKind.UPDATE_AFTER):
                state[key_value] = tuple(row)
            else:
                state.pop(key_value, None)
        return sorted(state.values())

    def test_whole_row_removes_identical_rows(self):
        dataframe = pf.from_records(
            [(1, "a"), (1, "a"), (2, "b")],
            schema=["id", "name"],
        )

        self.assertEqual(
            self._materialize(dataframe.drop_duplicates()),
            [(1, "a"), (2, "b")],
        )

    def test_whole_row_keeps_rows_differing_in_any_column(self):
        dataframe = pf.from_records(
            [(1, "a"), (1, "b"), (1, "a")],
            schema=["id", "name"],
        )

        self.assertEqual(
            self._materialize(dataframe.drop_duplicates()),
            [(1, "a"), (1, "b")],
        )

    def test_distinct_alias_removes_identical_rows(self):
        dataframe = pf.from_records(
            [(1, "a"), (1, "a"), (2, "b")],
            schema=["id", "name"],
        )

        self.assertEqual(
            self._materialize(dataframe.distinct()),
            [(1, "a"), (2, "b")],
        )

    def test_unique_alias_removes_identical_rows(self):
        dataframe = pf.from_records(
            [(1, "a"), (1, "a"), (2, "b")],
            schema=["id", "name"],
        )

        self.assertEqual(
            self._materialize(dataframe.unique()),
            [(1, "a"), (2, "b")],
        )

    def test_from_dict_input(self):
        dataframe = pf.from_dict({"id": [1, 1, 2], "name": ["a", "a", "b"]})

        self.assertEqual(
            self._materialize(dataframe.drop_duplicates()),
            [(1, "a"), (2, "b")],
        )

    def test_subset_keep_first_by_order_column(self):
        dataframe = pf.from_records(
            [
                (1, "a", 10),
                (1, "b", 20),
                (2, "c", 30),
                (2, "d", 5),
                (3, "e", 7),
            ],
            schema=["id", "name", "score"],
        )

        result = dataframe.drop_duplicates("id", order_by="score", keep="first")

        self.assertEqual(
            self._materialize(result, key=["id"]),
            [(1, "a", 10), (2, "d", 5), (3, "e", 7)],
        )

    def test_subset_keep_last_by_order_column(self):
        dataframe = pf.from_records(
            [
                (1, "a", 10),
                (1, "b", 20),
                (2, "c", 30),
                (2, "d", 5),
                (3, "e", 7),
            ],
            schema=["id", "name", "score"],
        )

        result = dataframe.drop_duplicates("id", order_by="score", keep="last")

        self.assertEqual(
            self._materialize(result, key=["id"]),
            [(1, "b", 20), (2, "c", 30), (3, "e", 7)],
        )

    def test_multi_column_subset_keep_first_by_order_column(self):
        dataframe = pf.from_records(
            [
                (1, "a", 10),
                (1, "a", 20),
                (1, "b", 30),
                (2, "a", 40),
            ],
            schema=["id", "name", "score"],
        )

        result = dataframe.drop_duplicates(["id", "name"], order_by="score", keep="first")

        self.assertEqual(
            self._materialize(result, key=["id", "name"]),
            [(1, "a", 10), (1, "b", 30), (2, "a", 40)],
        )

    def test_subset_keep_first_by_arrival(self):
        dataframe = pf.from_records(
            [(1, "a"), (1, "b"), (2, "c")],
            schema=["id", "name"],
        )

        self.assertEqual(
            self._materialize(dataframe.drop_duplicates("id"), key=["id"]),
            [(1, "a"), (2, "c")],
        )

    def test_subset_keep_last_by_arrival(self):
        dataframe = pf.from_records(
            [(1, "a"), (1, "b"), (2, "c")],
            schema=["id", "name"],
        )

        result = dataframe.drop_duplicates("id", keep="last")

        self.assertEqual(
            self._materialize(result, key=["id"]),
            [(1, "b"), (2, "c")],
        )

    def test_dedup_after_select_and_filter(self):
        dataframe = pf.from_records(
            [
                (1, "a", 10),
                (1, "b", 20),
                (2, "c", 30),
                (3, "d", 40),
            ],
            schema=["id", "name", "score"],
        )

        result = (
            dataframe.filter(pf.col("id") > 1)
            .select("id", "score")
            .drop_duplicates("id", order_by="score", keep="last")
        )

        self.assertEqual(
            self._materialize(result, key=["id"]),
            [(2, 30), (3, 40)],
        )

    def test_filter_after_dedup(self):
        dataframe = pf.from_records(
            [
                (1, "a", 10),
                (1, "b", 20),
                (2, "c", 30),
            ],
            schema=["id", "name", "score"],
        )

        result = dataframe.drop_duplicates("id", order_by="score", keep="last").filter(
            pf.col("id") > 1
        )

        self.assertEqual(
            self._materialize(result, key=["id"]),
            [(2, "c", 30)],
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


class DataFrameBatchITTests(PyFlinkITTestCase):
    def setUp(self):
        previous_environment = pf.get_table_environment()
        self.addCleanup(pf.set_table_environment, previous_environment)
        self.t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

    def _ordered_dataframe(self):
        table = self.t_env.sql_query(
            "SELECT * FROM (VALUES (3, 'C'), (1, 'A'), (4, 'D'), (2, 'B')) "
            "AS T(id, name)"
        )
        return pf.from_table(table.order_by(table.id))

    def test_limit_returns_first_rows(self):
        self.assertEqual(
            self._ordered_dataframe().limit(2).collect(),
            [Row(1, "A"), Row(2, "B")],
        )

    def test_offset_and_limit_compose_for_pagination(self):
        self.assertEqual(
            self._ordered_dataframe().offset(1).limit(2).collect(),
            [Row(2, "B"), Row(3, "C")],
        )

    def test_head_and_limit_are_equivalent(self):
        dataframe = self._ordered_dataframe()

        self.assertEqual(dataframe.head(3).collect(), dataframe.limit(3).collect())

    def test_zero_slicing(self):
        dataframe = self._ordered_dataframe()

        self.assertEqual(dataframe.limit(0).collect(), [])
        self.assertEqual(dataframe.head(0).collect(), [])
        self.assertEqual(
            dataframe.offset(0).collect(),
            [Row(1, "A"), Row(2, "B"), Row(3, "C"), Row(4, "D")],
        )

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


if __name__ == "__main__":
    unittest.main()
