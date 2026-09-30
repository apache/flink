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

import threading
import time
import unittest

import pandas as pd
import pyarrow as pa

import pyflink.dataframe as pf
from pyflink.common import Row, RowKind
from pyflink.dataframe import DataType
from pyflink.table import DataTypes
from pyflink.testing.test_case_utils import PyFlinkStreamDataFrameTestCase


def _row(*values, kind=RowKind.INSERT):
    row = Row(*values)
    row.set_row_kind(kind)
    return row


class _ResultIterator:
    """Stands in for the Table API result iterator, optionally blocking like an unbounded job."""

    def __init__(self, rows, error=None, block_after_rows=False):
        self._rows = iter(rows)
        self._error = error
        self._block_after_rows = block_after_rows
        self._closed = threading.Event()
        self.close_count = 0

    @property
    def closed(self):
        return self._closed.is_set()

    def __iter__(self):
        return self

    def __next__(self):
        for row in self._rows:
            return row
        if self._error is not None:
            raise self._error
        if self._block_after_rows:
            self._closed.wait()
            raise RuntimeError("job cancelled")
        raise StopIteration

    def close(self):
        self.close_count += 1
        self._closed.set()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class _ResolvedSchema:
    def __init__(self, columns, data_types):
        self._columns = columns
        self._data_types = data_types

    def get_column_names(self):
        return list(self._columns)

    def get_column_data_types(self):
        return list(self._data_types)


class _TableResult:
    def __init__(self, iterator):
        self._iterator = iterator

    def collect(self):
        return self._iterator


class _Table:
    def __init__(self, iterator, columns=("id", "name"), data_types=None):
        self._iterator = iterator
        self._schema = _ResolvedSchema(
            columns, data_types or [DataTypes.BIGINT(), DataTypes.STRING()]
        )
        self.execute_count = 0

    def get_resolved_schema(self):
        return self._schema

    def execute(self):
        self.execute_count += 1
        return _TableResult(self._iterator)


def _dataframe(rows, **kwargs):
    iterator = kwargs.pop("iterator", None) or _ResultIterator(rows)
    table = _Table(iterator, **kwargs)
    return pf.DataFrame(table), table, iterator


class CloseableIteratorTests(unittest.TestCase):
    def test_closes_once_exhausted(self):
        closes = []
        iterator = pf.CloseableIterator(iter([1, 2]), lambda: closes.append(True))

        self.assertEqual(list(iterator), [1, 2])
        self.assertEqual(closes, [True])

    def test_closes_and_reraises_when_iteration_fails(self):
        def failing():
            yield 1
            raise RuntimeError("fetch failed")

        closes = []
        iterator = pf.CloseableIterator(failing(), lambda: closes.append(True))

        self.assertEqual(next(iterator), 1)
        with self.assertRaisesRegex(RuntimeError, "fetch failed"):
            next(iterator)
        self.assertEqual(closes, [True])

    def test_context_manager_closes_early_and_close_is_idempotent(self):
        closes = []
        with pf.CloseableIterator(iter([1, 2, 3]), lambda: closes.append(True)) as iterator:
            self.assertEqual(next(iterator), 1)
        iterator.close()

        self.assertEqual(closes, [True])
        self.assertEqual(list(iterator), [])


class IterRowsTests(unittest.TestCase):
    def test_yields_dicts_keyed_by_column_and_closes_at_end(self):
        dataframe, _, source = _dataframe([_row(1, "Alice"), _row(2, "Bob")])

        with dataframe.iter_rows() as rows:
            self.assertEqual(
                list(rows), [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
            )
        self.assertEqual(source.close_count, 1)

    def test_include_row_kind_adds_short_change_kind(self):
        dataframe, _, _ = _dataframe([
            _row(1, "Alice"),
            _row(1, "Alice", kind=RowKind.UPDATE_BEFORE),
            _row(1, "Alicia", kind=RowKind.UPDATE_AFTER),
            _row(1, "Alicia", kind=RowKind.DELETE),
        ])

        rows = list(dataframe.iter_rows(include_row_kind=True, row_kind_field="op"))

        self.assertEqual([row["op"] for row in rows], ["+I", "-U", "+U", "-D"])

    def test_rejects_row_kind_field_clash_before_executing(self):
        dataframe, table, _ = _dataframe([])

        with self.assertRaisesRegex(ValueError, "conflicts with an existing column"):
            dataframe.iter_rows(include_row_kind=True, row_kind_field="name")
        with self.assertRaises(TypeError):
            dataframe.iter_rows(include_row_kind=True, row_kind_field="")
        self.assertEqual(table.execute_count, 0)

    def test_row_kind_field_is_only_checked_when_included(self):
        dataframe, _, _ = _dataframe([_row(1, "Alice")])

        self.assertEqual(list(dataframe.iter_rows(row_kind_field="name")),
                         [{"id": 1, "name": "Alice"}])

    def test_row_kind_strings_match_the_documented_short_forms(self):
        # The docstrings promise these exact strings; they come from RowKind.__str__.
        self.assertEqual(
            [str(kind) for kind in RowKind],
            ["+I", "-U", "+U", "-D"],
        )


class IterBatchesTests(unittest.TestCase):
    def test_splits_rows_into_batches_of_batch_size(self):
        dataframe, _, source = _dataframe([_row(i, str(i)) for i in range(5)])

        batches = list(dataframe.iter_batches(batch_size=2))

        self.assertEqual([len(batch) for batch in batches], [2, 2, 1])
        self.assertTrue(all(isinstance(batch, pd.DataFrame) for batch in batches))
        self.assertEqual(list(pd.concat(batches)["id"]), [0, 1, 2, 3, 4])
        self.assertTrue(source.closed)

    def test_arrow_batches_use_the_flink_schema(self):
        dataframe, _, _ = _dataframe([_row(1, "Alice")])

        (batch,) = dataframe.iter_batches(batch_format="pyarrow")

        self.assertIsInstance(batch, pa.Table)
        self.assertEqual(batch.schema.types, [pa.int64(), pa.utf8()])
        self.assertEqual(batch.to_pylist(), [{"id": 1, "name": "Alice"}])

    def test_include_row_kind_appends_a_column(self):
        dataframe, _, _ = _dataframe(
            [_row(1, "Alice"), _row(1, "Alice", kind=RowKind.DELETE)]
        )

        (batch,) = dataframe.iter_batches(include_row_kind=True)

        self.assertEqual(list(batch.columns), ["id", "name", "__row_kind__"])
        self.assertEqual(list(batch["__row_kind__"]), ["+I", "-D"])

    def test_nested_values_become_arrow_structs_lists_and_maps(self):
        data_types = [
            DataTypes.ROW([
                DataTypes.FIELD("x", DataTypes.INT()),
                DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())),
            ]),
            DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()),
        ]
        dataframe, _, _ = _dataframe(
            [_row(Row(1, ["a"]), {"k": 2}), _row(None, None)],
            columns=("point", "counts"),
            data_types=data_types,
        )

        (batch,) = dataframe.iter_batches(batch_format="pyarrow")

        self.assertEqual(
            batch.to_pylist(),
            [
                {"point": {"x": 1, "tags": ["a"]}, "counts": [("k", 2)]},
                {"point": None, "counts": None},
            ],
        )

    def test_closes_and_reraises_when_the_source_fails(self):
        source = _ResultIterator([_row(1, "Alice")], error=RuntimeError("fetch failed"))
        dataframe, _, _ = _dataframe([], iterator=source)

        batches = dataframe.iter_batches(batch_size=1)
        self.assertEqual(len(next(batches)), 1)
        with self.assertRaisesRegex(RuntimeError, "fetch failed"):
            next(batches)
        self.assertEqual(source.close_count, 1)

    def test_rejects_invalid_arguments_before_executing(self):
        dataframe, table, _ = _dataframe([])

        with self.assertRaises(ValueError):
            dataframe.iter_batches(batch_size=0)
        with self.assertRaises(TypeError):
            dataframe.iter_batches(batch_size=True)
        with self.assertRaisesRegex(ValueError, "batch_format"):
            dataframe.iter_batches(batch_format="arrow")
        with self.assertRaisesRegex(ValueError, "conflicts"):
            dataframe.iter_batches(include_row_kind=True, row_kind_field="id")
        self.assertEqual(table.execute_count, 0)

    def test_rejects_unsupported_column_types_before_executing(self):
        dataframe, table, _ = _dataframe(
            [], columns=("tags",), data_types=[DataTypes.MULTISET(DataTypes.STRING())]
        )

        with self.assertRaisesRegex(ValueError, "not supported"):
            dataframe.iter_batches()
        with self.assertRaisesRegex(ValueError, "not supported"):
            dataframe.take_batch(1)
        self.assertEqual(table.execute_count, 0)


class TakeTests(unittest.TestCase):
    def test_returns_first_n_rows_and_closes(self):
        dataframe, _, source = _dataframe([_row(i, str(i)) for i in range(5)])

        self.assertEqual(
            dataframe.take(2), [{"id": 0, "name": "0"}, {"id": 1, "name": "1"}]
        )
        self.assertTrue(source.closed)

    def test_returns_all_rows_when_result_is_shorter(self):
        dataframe, _, _ = _dataframe([_row(1, "Alice")])

        self.assertEqual(dataframe.take(10), [{"id": 1, "name": "Alice"}])

    def test_zero_rows_does_not_execute(self):
        dataframe, table, _ = _dataframe([_row(1, "Alice")])

        self.assertEqual(dataframe.take(0), [])
        self.assertEqual(table.execute_count, 0)

    def test_include_row_kind(self):
        dataframe, _, _ = _dataframe([_row(1, "Alice", kind=RowKind.UPDATE_AFTER)])

        self.assertEqual(
            dataframe.take(1, include_row_kind=True),
            [{"id": 1, "name": "Alice", "__row_kind__": "+U"}],
        )

    def test_timeout_returns_rows_received_so_far_and_closes(self):
        source = _ResultIterator([_row(1, "Alice")], block_after_rows=True)
        dataframe, _, _ = _dataframe([], iterator=source)

        self.assertEqual(dataframe.take(5, timeout=0.2), [{"id": 1, "name": "Alice"}])
        self.assertTrue(source.closed)

    def test_timeout_returns_early_when_result_ends(self):
        dataframe, _, source = _dataframe([_row(1, "Alice")])

        self.assertEqual(dataframe.take(5, timeout=60), [{"id": 1, "name": "Alice"}])
        self.assertTrue(source.closed)

    def test_timeout_propagates_fetch_errors(self):
        source = _ResultIterator([_row(1, "Alice")], error=RuntimeError("fetch failed"))
        dataframe, _, _ = _dataframe([], iterator=source)

        with self.assertRaisesRegex(RuntimeError, "fetch failed"):
            dataframe.take(5, timeout=60)
        self.assertTrue(source.closed)

    def test_rejects_invalid_arguments_before_executing(self):
        dataframe, table, _ = _dataframe([])

        for n in (-1, 1.5, True):
            with self.assertRaises((TypeError, ValueError)):
                dataframe.take(n)
        for timeout in (-1, "1", True):
            with self.assertRaises((TypeError, ValueError)):
                dataframe.take(1, timeout=timeout)
        self.assertEqual(table.execute_count, 0)


class TakeBatchTests(unittest.TestCase):
    def test_returns_first_n_rows_as_one_batch(self):
        dataframe, _, source = _dataframe([_row(i, str(i)) for i in range(5)])

        batch = dataframe.take_batch(3)

        self.assertIsInstance(batch, pd.DataFrame)
        self.assertEqual(list(batch["id"]), [0, 1, 2])
        self.assertTrue(source.closed)

    def test_empty_batch_keeps_the_schema(self):
        dataframe, _, _ = _dataframe([])

        pandas_batch = dataframe.take_batch(0, include_row_kind=True)
        arrow_batch = dataframe.take_batch(0, batch_format="pyarrow")

        self.assertEqual(list(pandas_batch.columns), ["id", "name", "__row_kind__"])
        self.assertEqual(len(pandas_batch), 0)
        self.assertEqual(arrow_batch.schema.names, ["id", "name"])
        self.assertEqual(arrow_batch.num_rows, 0)

    def test_timeout_returns_the_rows_received_so_far_as_one_batch(self):
        source = _ResultIterator(
            [_row(1, "Alice"), _row(2, "Bob")], block_after_rows=True
        )
        dataframe, _, _ = _dataframe([], iterator=source)

        batch = dataframe.take_batch(5, timeout=0.2, batch_format="pyarrow")

        self.assertEqual(batch.to_pylist(), [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        self.assertTrue(source.closed)

    def test_rejects_invalid_batch_format_before_executing(self):
        dataframe, table, _ = _dataframe([])

        with self.assertRaisesRegex(ValueError, "batch_format"):
            dataframe.take_batch(1, batch_format="polars")
        self.assertEqual(table.execute_count, 0)


class IterationITTests(PyFlinkStreamDataFrameTestCase):
    def _unbounded_source(self, rows_per_second):
        return pf.read_generic(
            "datagen",
            schema={"x": DataType.int64()},
            options={"rows-per-second": str(rows_per_second)},
        )

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

    def test_iter_rows_and_take_on_bounded_input(self):
        dataframe = pf.from_records([(1, "Alice"), (2, "Bob")], schema=["id", "name"])

        with dataframe.iter_rows() as rows:
            collected = sorted(rows, key=lambda row: row["id"])
        taken = dataframe.take(1)

        self.assertEqual(collected, [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        self.assertEqual(len(taken), 1)
        self.assertIn(taken[0], collected)

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

    def test_batches_match_to_pandas(self):
        dataframe = pf.from_records(
            [(i, f"name-{i}", i * 1.5) for i in range(7)], schema=["id", "name", "score"]
        )

        with dataframe.iter_batches(batch_size=3) as batches:
            from_batches = pd.concat(list(batches))

        pd.testing.assert_frame_equal(
            from_batches.sort_values("id").reset_index(drop=True),
            dataframe.to_pandas().sort_values("id").reset_index(drop=True),
        )

    def test_take_batch_with_nested_types(self):
        dataframe = pf.from_table(self.t_env.sql_query(
            "SELECT ROW(1, 'a') AS point, ARRAY[1, 2] AS items, MAP['k', 3] AS counts"
        ))

        batch = dataframe.take_batch(1, batch_format="pyarrow")

        self.assertEqual(
            batch.to_pylist(),
            [{"point": {"EXPR$0": 1, "EXPR$1": "a"}, "items": [1, 2], "counts": [("k", 3)]}],
        )

    def test_take_cancels_an_unbounded_job(self):
        self.assertEqual(len(self._unbounded_source(100).take(3)), 3)

        self._assert_all_jobs_terminate()

    def test_take_with_timeout_returns_partial_rows_from_an_unbounded_job(self):
        rows = self._unbounded_source(1).take(1000, timeout=2)

        self.assertLess(len(rows), 1000)
        self._assert_all_jobs_terminate()

    def test_closing_iter_rows_early_cancels_an_unbounded_job(self):
        with self._unbounded_source(100).iter_rows() as rows:
            next(rows)

        self._assert_all_jobs_terminate()


if __name__ == "__main__":
    unittest.main()
