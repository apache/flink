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

import itertools
import queue
import threading
import time
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Protocol,
    TypeVar,
)

from pyflink.common import Row
from pyflink.dataframe.validation import _require_non_empty_str
from pyflink.table.types import ArrayType, DataType, MapType, RowType, create_arrow_schema
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ["CloseableIterator"]

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)

_BATCH_FORMATS = ("pandas", "pyarrow")


@PublicEvolving()
class CloseableIterator(Iterator[T_co], Protocol):
    """
    An iterator over the results of a running Flink job.

    Closing the iterator cancels the job if it is still running. The iterator closes itself once
    the results are exhausted or iteration raises an error, so only iterators abandoned before
    their end need an explicit :meth:`close`. Prefer a ``with`` block: on an unbounded source the
    job keeps running until the iterator is closed.

    Instances are returned by :meth:`DataFrame.iter_rows` and :meth:`DataFrame.iter_batches`.

    Example::

        >>> import pyflink.dataframe as pf
        >>> df = pf.from_records([{"id": 1}, {"id": 2}])
        >>> with df.iter_rows() as rows:
        ...     first = next(rows)

    .. versionadded:: 2.4.0
    """

    def close(self) -> None:
        """
        Close the iterator and cancel the job if it is still running. Closing twice is a no-op.
        """
        ...

    def __enter__(self) -> "CloseableIterator[T_co]":
        ...

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        ...


class _CloseableIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], close: Callable[[], None]):
        self._iterator = iterator
        self._close = close
        self._closed = False

    def __iter__(self) -> "_CloseableIterator[T]":
        return self

    def __next__(self) -> T:
        if self._closed:
            raise StopIteration
        try:
            return next(self._iterator)
        except BaseException:
            # Covers StopIteration and interrupts too: either way the job is no longer needed.
            self.close()
            raise

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._close()

    def __enter__(self) -> "_CloseableIterator[T]":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()


def _validate_row_kind_field(
    columns: List[str], include_row_kind: bool, row_kind_field: str
) -> None:
    if not include_row_kind:
        return
    _require_non_empty_str(row_kind_field, "row_kind_field")
    if row_kind_field in columns:
        raise ValueError(
            f"row_kind_field '{row_kind_field}' conflicts with an existing column; "
            "pass a different row_kind_field"
        )


def _row_to_dict(
    row: Row, columns: List[str], row_kind_field: Optional[str]
) -> Dict[str, Any]:
    result = dict(zip(columns, row))
    if row_kind_field is not None:
        result[row_kind_field] = str(row.get_row_kind())
    return result


def _iterate_rows(
    table, columns: List[str], row_kind_field: Optional[str]
) -> _CloseableIterator[Dict[str, Any]]:
    rows = table.execute().collect()
    return _CloseableIterator(
        (_row_to_dict(row, columns, row_kind_field) for row in rows), rows.close
    )


def _build_arrow_schema(
    columns: List[str], data_types: List[DataType], row_kind_field: Optional[str]
) -> Any:
    """
    Build the Arrow schema of a batch, failing on unsupported column types before any job runs.
    """
    import pyarrow as pa

    schema = create_arrow_schema(columns, data_types, allow_nested=True)
    if row_kind_field is not None:
        schema = schema.append(pa.field(row_kind_field, pa.utf8(), nullable=False))
    return schema


def _iterate_batches(
    table,
    batch_size: int,
    batch_format: str,
    data_types: List[DataType],
    arrow_schema: Any,
    row_kind_field: Optional[str],
) -> _CloseableIterator[Any]:
    rows = table.execute().collect()

    def batches():
        while True:
            chunk = list(itertools.islice(rows, batch_size))
            if not chunk:
                return
            yield _rows_to_batch(chunk, data_types, arrow_schema, batch_format, row_kind_field)

    return _CloseableIterator(batches(), rows.close)


def _take_rows(table, n: int, timeout: Optional[float]) -> List[Row]:
    if n == 0:
        return []
    rows = table.execute().collect()
    if timeout is None:
        with rows:
            return list(itertools.islice(rows, n))
    return _take_rows_until(rows, n, time.monotonic() + timeout)


def _take_rows_until(rows, n: int, deadline: float) -> List[Row]:
    """
    Read up to ``n`` rows, returning early with what has arrived when ``deadline`` passes.

    Fetching a row blocks inside the JVM and cannot be interrupted from Python, so a worker
    thread fetches while this thread waits on a queue with a timeout. Closing the iterator
    cancels the job; the JVM fetcher polls the job status between fetch attempts, so the worker
    unblocks within one retry interval, either with an end-of-results or with an error. The
    worker is a daemon thread and holds its own gateway connection, so it cannot stall exit if
    the cluster never answers.
    """
    results: "queue.Queue" = queue.Queue()
    closed = threading.Event()

    def fetch():
        try:
            for row in itertools.islice(rows, n):
                results.put(("row", row))
            results.put(("end", None))
        except BaseException as error:
            # After close() the job is being cancelled, so a failing fetch is the expected way
            # for the worker to unblock and nobody is waiting for its result any more. The
            # caller sets `closed` before calling close(), so an error caused by the
            # cancellation is never mistaken for a genuine fetch failure.
            if not closed.is_set():
                results.put(("error", error))

    taken: List[Row] = []
    try:
        threading.Thread(target=fetch, name="dataframe-take", daemon=True).start()
        while len(taken) < n:
            # Past the deadline the timeout is zero, which still drains rows already fetched.
            remaining = max(deadline - time.monotonic(), 0)
            try:
                kind, value = results.get(timeout=remaining)
            except queue.Empty:
                break
            if kind == "row":
                taken.append(value)
            elif kind == "end":
                break
            else:
                raise value
    finally:
        closed.set()
        rows.close()
    return taken


def _rows_to_batch(
    rows: List[Row],
    data_types: List[DataType],
    arrow_schema: Any,
    batch_format: str,
    row_kind_field: Optional[str],
) -> Any:
    import pyarrow as pa

    arrays = [
        pa.array(
            [_to_arrow_value(row[index], data_type) for row in rows],
            type=arrow_schema.field(index).type,
        )
        for index, data_type in enumerate(data_types)
    ]
    if row_kind_field is not None:
        arrays.append(pa.array([str(row.get_row_kind()) for row in rows], type=pa.utf8()))
    batch = pa.Table.from_arrays(arrays, schema=arrow_schema)
    return batch.to_pandas() if batch_format == "pandas" else batch


def _to_arrow_value(value: Any, data_type: DataType) -> Any:
    """
    Reshape a collected value into what pyarrow expects for ``data_type``.

    Collected nested rows are positional, while pyarrow builds structs from dicts keyed by
    field name. Maps become key-value pair lists, which every supported pyarrow version accepts.
    """
    if value is None:
        return None
    if isinstance(data_type, RowType):
        return {
            name: _to_arrow_value(field_value, field_type)
            for name, field_value, field_type in zip(
                data_type.field_names(), value, data_type.field_types()
            )
        }
    if isinstance(data_type, ArrayType):
        return [_to_arrow_value(element, data_type.element_type) for element in value]
    if isinstance(data_type, MapType):
        return [
            (
                _to_arrow_value(key, data_type.key_type),
                _to_arrow_value(item, data_type.value_type),
            )
            for key, item in value.items()
        ]
    return value
