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

import time
import unittest

import pandas as pd
import pyarrow as pa
from py4j.protocol import Py4JJavaError

import pyflink.dataframe as pf
from pyflink.common import Row, RowKind
from pyflink.dataframe import DataType
from pyflink.dataframe.iteration import _build_arrow_schema, _CloseableIterator, _rows_to_batch
from pyflink.table import DataTypes
from pyflink.testing.test_case_utils import PyFlinkStreamDataFrameTestCase


def _row(*values, kind=RowKind.INSERT):
    row = Row(*values)
    row.set_row_kind(kind)
    return row


class CloseableIteratorTests(unittest.TestCase):
    def test_closes_once_exhausted(self):
        closes = []
        iterator = _CloseableIterator(iter([1, 2]), lambda: closes.append(True))

        self.assertEqual(list(iterator), [1, 2])
        self.assertEqual(closes, [True])

    def test_closes_and_reraises_when_iteration_fails(self):
        def failing():
            yield 1
            raise RuntimeError("fetch failed")

        closes = []
        iterator = _CloseableIterator(failing(), lambda: closes.append(True))

        self.assertEqual(next(iterator), 1)
        with self.assertRaisesRegex(RuntimeError, "fetch failed"):
            next(iterator)
        self.assertEqual(closes, [True])

    def test_context_manager_closes_early_and_close_is_idempotent(self):
        closes = []
        with _CloseableIterator(iter([1, 2, 3]), lambda: closes.append(True)) as iterator:
            self.assertEqual(next(iterator), 1)
        iterator.close()

        self.assertEqual(closes, [True])
        self.assertEqual(list(iterator), [])

    def test_row_kind_strings_match_the_documented_short_forms(self):
        # The docstrings promise these exact strings; they come from RowKind.__str__.
        self.assertEqual([str(kind) for kind in RowKind], ["+I", "-U", "+U", "-D"])


class RowsToBatchTests(unittest.TestCase):
    _COLUMNS = ["id", "name"]
    _TYPES = [DataTypes.BIGINT(), DataTypes.STRING()]

    def _batch(self, rows, batch_format="pyarrow", row_kind_field=None,
               columns=None, data_types=None):
        columns = columns or self._COLUMNS
        data_types = data_types or self._TYPES
        schema = _build_arrow_schema(columns, data_types, row_kind_field)
        return _rows_to_batch(rows, data_types, schema, batch_format, row_kind_field)

    def test_arrow_batch_uses_the_flink_schema(self):
        batch = self._batch([_row(1, "Alice")])

        self.assertIsInstance(batch, pa.Table)
        self.assertEqual(batch.schema.types, [pa.int64(), pa.utf8()])
        self.assertEqual(batch.to_pylist(), [{"id": 1, "name": "Alice"}])

    def test_pandas_batch_with_row_kind_column(self):
        batch = self._batch(
            [_row(1, "Alice"), _row(1, "Alice", kind=RowKind.DELETE)],
            batch_format="pandas",
            row_kind_field="op",
        )

        self.assertIsInstance(batch, pd.DataFrame)
        self.assertEqual(list(batch.columns), ["id", "name", "op"])
        self.assertEqual(list(batch["op"]), ["+I", "-D"])

    def test_empty_batch_keeps_the_schema(self):
        batch = self._batch([], row_kind_field="op")

        self.assertEqual(batch.schema.names, ["id", "name", "op"])
        self.assertEqual(batch.num_rows, 0)

    def test_nested_values_become_arrow_structs_lists_and_maps(self):
        batch = self._batch(
            [_row(Row(1, ["a"]), {"k": 2}), _row(None, None)],
            columns=["point", "counts"],
            data_types=[
                DataTypes.ROW([
                    DataTypes.FIELD("x", DataTypes.INT()),
                    DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())),
                ]),
                DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()),
            ],
        )

        self.assertEqual(
            batch.to_pylist(),
            [
                {"point": {"x": 1, "tags": ["a"]}, "counts": [("k", 2)]},
                {"point": None, "counts": None},
            ],
        )


class IterationTests(PyFlinkStreamDataFrameTestCase):
    def _people(self):
        return pf.from_records(
            [(i, f"name-{i}") for i in range(5)], schema=["id", "name"]
        )

    def _unbounded_source(self, rows_per_second):
        return pf.read_generic(
            "datagen",
            schema={"x": DataType.int64()},
            options={"rows-per-second": str(rows_per_second)},
        )

    def _failing_source(self):
        # Random datagen strings are not numbers, so the cast fails as soon as a row flows.
        self.t_env.execute_sql(
            "CREATE TEMPORARY TABLE IF NOT EXISTS random_strings (s STRING) "
            "WITH ('connector' = 'datagen', 'rows-per-second' = '10')"
        )
        return pf.from_table(
            self.t_env.sql_query("SELECT CAST(s AS INT) AS x FROM random_strings")
        )

    def _job_count(self):
        return len(self.resource.getMiniCluster().listJobs().get())

    def _assert_all_jobs_terminate(self, timeout_seconds=60):
        mini_cluster = self.resource.getMiniCluster()
        deadline = time.monotonic() + timeout_seconds
        while True:
            running = [
                job for job in mini_cluster.listJobs().get()
                if not job.getJobState().isGloballyTerminalState()
            ]
            if not running:
                return
            if time.monotonic() > deadline:
                self.fail(f"jobs still running: {[str(job.getJobId()) for job in running]}")
            time.sleep(0.1)

    # ------------------------------------------------------------------ iter_rows

    def test_iter_rows_yields_dicts_keyed_by_column(self):
        with self._people().iter_rows() as rows:
            collected = sorted(rows, key=lambda row: row["id"])

        self.assertEqual(collected, [{"id": i, "name": f"name-{i}"} for i in range(5)])

    def test_iter_rows_row_kind_field_is_only_checked_when_included(self):
        with self._people().iter_rows(row_kind_field="name") as rows:
            self.assertEqual(len(list(rows)), 5)

    def test_iter_rows_reraises_when_the_job_fails(self):
        with self._failing_source().iter_rows() as rows:
            with self.assertRaises(Py4JJavaError):
                list(rows)
        self._assert_all_jobs_terminate()

    def test_closing_iter_rows_early_cancels_an_unbounded_job(self):
        with self._unbounded_source(100).iter_rows() as rows:
            next(rows)

        self._assert_all_jobs_terminate()

    # ------------------------------------------------------------------ iter_batches

    def test_iter_batches_match_to_pandas(self):
        dataframe = pf.from_records(
            [(i, f"name-{i}", i * 1.5) for i in range(7)], schema=["id", "name", "score"]
        )

        with dataframe.iter_batches(batch_size=3) as batches:
            batches = list(batches)

        self.assertEqual([len(batch) for batch in batches], [3, 3, 1])
        pd.testing.assert_frame_equal(
            pd.concat(batches).sort_values("id").reset_index(drop=True),
            dataframe.to_pandas().sort_values("id").reset_index(drop=True),
        )

    def test_iter_batches_in_pyarrow_format(self):
        with self._people().iter_batches(batch_format="pyarrow") as batches:
            (batch,) = batches

        self.assertIsInstance(batch, pa.Table)
        self.assertEqual(batch.num_rows, 5)

    def test_iter_batches_reraises_when_the_job_fails(self):
        with self._failing_source().iter_batches(batch_size=1) as batches:
            with self.assertRaises(Py4JJavaError):
                next(batches)
        self._assert_all_jobs_terminate()

    # ------------------------------------------------------------------ take / take_batch

    def test_take_returns_first_n_rows(self):
        taken = self._people().take(2)

        self.assertEqual(len(taken), 2)
        self.assertTrue(all(row.keys() == {"id", "name"} for row in taken))

    def test_take_returns_all_rows_when_result_is_shorter(self):
        self.assertEqual(len(self._people().take(10)), 5)
        self.assertEqual(len(self._people().take(10, timeout=60)), 5)

    def test_take_zero_rows_does_not_execute(self):
        jobs_before = self._job_count()

        self.assertEqual(self._people().take(0), [])
        self.assertEqual(self._job_count(), jobs_before)

    def test_row_kinds_of_an_updating_result(self):
        dataframe = pf.from_records([(1, "a"), (2, "a")], schema=["id", "name"])
        counts = dataframe.group_by("name").agg(c=pf.col("id").count)

        self.assertEqual(
            counts.take(10, include_row_kind=True, row_kind_field="op"),
            [
                {"name": "a", "c": 1, "op": "+I"},
                {"name": "a", "c": 1, "op": "-U"},
                {"name": "a", "c": 2, "op": "+U"},
            ],
        )

    def test_take_cancels_an_unbounded_job(self):
        self.assertEqual(len(self._unbounded_source(100).take(3)), 3)

        self._assert_all_jobs_terminate()

    def test_take_with_timeout_returns_partial_rows_from_an_unbounded_job(self):
        rows = self._unbounded_source(1).take(1000, timeout=2)

        self.assertLess(len(rows), 1000)
        self._assert_all_jobs_terminate()

    def test_take_reraises_when_the_job_fails(self):
        with self.assertRaises(Py4JJavaError):
            self._failing_source().take(5)
        with self.assertRaises(Py4JJavaError):
            self._failing_source().take(5, timeout=60)
        self._assert_all_jobs_terminate()

    def test_take_batch_returns_first_n_rows_as_one_batch(self):
        batch = self._people().take_batch(3)

        self.assertIsInstance(batch, pd.DataFrame)
        self.assertEqual(len(batch), 3)

    def test_take_batch_with_timeout_returns_partial_rows_from_an_unbounded_job(self):
        batch = self._unbounded_source(1).take_batch(
            1000, timeout=2, batch_format="pyarrow", include_row_kind=True
        )

        self.assertLess(batch.num_rows, 1000)
        self.assertEqual(batch.schema.names, ["x", "__row_kind__"])
        self._assert_all_jobs_terminate()

    def test_take_batch_with_nested_types(self):
        dataframe = pf.from_table(self.t_env.sql_query(
            "SELECT ROW(1, 'a') AS point, ARRAY[1, 2] AS items, MAP['k', 3] AS counts"
        ))

        batch = dataframe.take_batch(1, batch_format="pyarrow")

        self.assertEqual(
            batch.to_pylist(),
            [{"point": {"EXPR$0": 1, "EXPR$1": "a"}, "items": [1, 2], "counts": [("k", 3)]}],
        )

    # ------------------------------------------------------------------ argument checks

    def test_invalid_arguments_are_rejected_before_executing(self):
        dataframe = self._people()
        multiset = pf.from_table(
            self.t_env.sql_query("SELECT COLLECT(name) AS tags FROM (VALUES ('a')) AS t(name)")
        )
        cases = [
            (ValueError, "conflicts", lambda: dataframe.iter_rows(
                include_row_kind=True, row_kind_field="name")),
            (ValueError, "row_kind_field", lambda: dataframe.iter_rows(
                include_row_kind=True, row_kind_field="")),
            (TypeError, "row_kind_field", lambda: dataframe.take(
                1, include_row_kind=True, row_kind_field=1)),
            (ValueError, "batch_size", lambda: dataframe.iter_batches(batch_size=0)),
            (TypeError, "batch_size", lambda: dataframe.iter_batches(batch_size=True)),
            (ValueError, "batch_format", lambda: dataframe.iter_batches(batch_format="arrow")),
            (ValueError, "batch_format", lambda: dataframe.take_batch(1, batch_format="polars")),
            (ValueError, "n must be non-negative", lambda: dataframe.take(-1)),
            (TypeError, "n must be an integer", lambda: dataframe.take(1.5)),
            (TypeError, "n must be an integer", lambda: dataframe.take(True)),
            (ValueError, "timeout", lambda: dataframe.take(1, timeout=-1)),
            (TypeError, "timeout", lambda: dataframe.take(1, timeout="1")),
            (TypeError, "timeout", lambda: dataframe.take_batch(1, timeout=True)),
            (ValueError, "not supported", lambda: multiset.iter_batches()),
            (ValueError, "not supported", lambda: multiset.take_batch(1)),
        ]
        jobs_before = self._job_count()

        for error, message, call in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(error, message):
                    call()
        self.assertEqual(self._job_count(), jobs_before)


if __name__ == "__main__":
    unittest.main()
