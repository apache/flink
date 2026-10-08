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
import functools
import os
import unittest
from dataclasses import FrozenInstanceError
from typing import TypedDict

import cloudpickle
import pandas as pd
from py4j.protocol import Py4JJavaError
import pyflink.dataframe as pf
from pyflink.common import Row, Types
from pyflink.table import DataTypes, ListView, MapView, Schema
from pyflink.table.expressions import col, lit
from pyflink.table.udf import AggregateFunction, ScalarFunction, udaf as table_udaf
from pyflink.table.window import Session
from pyflink.testing.test_case_utils import (
    PyFlinkBatchTableTestCase, PyFlinkDataFrameUTTestCase, PyFlinkStreamDataFrameTestCase,
)
from pyflink.util.exceptions import TableException


class Sum(AggregateFunction):
    def create_accumulator(self):
        return [0]

    def accumulate(self, accumulator, value):
        if value is not None:
            accumulator[0] += value

    def retract(self, accumulator, value):
        if value is not None:
            accumulator[0] -= value

    def merge(self, accumulator, accumulators):
        for other in accumulators:
            accumulator[0] += other[0]

    def get_value(self, accumulator) -> int:
        return accumulator[0]

    def get_accumulator_type(self):
        return DataTypes.ARRAY(DataTypes.BIGINT())


class Statistics(TypedDict):
    total: int
    count: int


def _aggregate(declaration, *columns):
    adapter = cloudpickle.loads(cloudpickle.dumps(declaration._table_udf_wrapper._func))
    adapter.open(None)
    try:
        accumulator = adapter.create_accumulator()
        adapter.accumulate(accumulator, *columns)
        return adapter.get_value(accumulator)
    finally:
        adapter.close()


class DataFrameUDAFDeclarationTests(unittest.TestCase):
    def test_declaration_forms(self):
        for declaration in (pf.udaf(Sum), pf.udaf()(Sum), pf.udaf(return_dtype=int)(Sum),
                            pf.udaf(Sum())):
            self.assertEqual(declaration.return_dtype, pf.DataType.int64())
            self.assertEqual(declaration.__name__, "Sum")

    def test_general_requires_aggregate_function(self):
        class CallableSum:
            def __call__(self, values) -> int:
                return sum(values)

        for func in (sum, CallableSum, CallableSum()):
            with self.subTest(func=func), self.assertRaisesRegex(TypeError, "AggregateFunction"):
                pf.udaf(func, return_dtype=int, accumulator_type=list[int])

    def test_pandas_infers_scalar_result(self):
        def total(values) -> int:
            return values.sum()

        declaration = pf.udaf(total, func_type="pandas")
        self.assertEqual(declaration.return_dtype, pf.DataType.int64())

    def test_type_precedence_and_immutable_metadata(self):
        class TypedSum(Sum):
            def get_result_type(self):
                return DataTypes.INT()

        declaration = pf.udaf(TypedSum)
        self.assertEqual(declaration.return_dtype, pf.DataType.int32())
        self.assertEqual(declaration.accumulator_type, pf.DataType.list(pf.DataType.int64()))
        declaration = pf.udaf(TypedSum, return_dtype=float, accumulator_type=list[float])
        self.assertEqual(declaration.return_dtype, pf.DataType.float64())
        self.assertEqual(declaration.accumulator_type, pf.DataType.list(pf.DataType.float64()))
        with self.assertRaises(FrozenInstanceError):
            declaration.return_dtype = pf.DataType.string()
        self.assertIs(declaration._table_udf_wrapper, declaration._table_udf_wrapper)

        class ExplicitSum(Sum):
            def get_result_type(self):
                raise AssertionError("Explicit return_dtype must take precedence")

            def get_accumulator_type(self):
                raise AssertionError("Explicit accumulator_type must take precedence")

        self.assertEqual(pf.udaf(ExplicitSum, return_dtype=int,
                                 accumulator_type=list[int]).return_dtype, pf.DataType.int64())

    def test_return_annotation_is_resolved_independently(self):
        def total(values):
            return values.sum()

        total.__annotations__ = {"values": "UnknownInput", "return": int}
        self.assertEqual(pf.udaf(total, func_type="pandas").return_dtype, pf.DataType.int64())
        total.__annotations__["return"] = "UnknownOutput"
        self.assertEqual(pf.udaf(total, return_dtype=int, func_type="pandas").return_dtype,
                         pf.DataType.int64())

    def test_missing_and_invalid_types(self):
        class UntypedSum(Sum):
            get_accumulator_type = AggregateFunction.get_accumulator_type

            def create_accumulator(self) -> list[int]:
                raise AssertionError("Type resolution must not execute create_accumulator")

        with self.assertRaisesRegex(TypeError, "accumulator_type"):
            pf.udaf(UntypedSum)
        with self.assertRaisesRegex(TypeError, "return_dtype"):
            pf.udaf(lambda values: values.sum(), func_type="pandas")
        with self.assertRaisesRegex(TypeError, "MAP"):
            pf.udaf(lambda values: {}, return_dtype=dict[str, int], func_type="pandas")
        with self.assertRaises(TypeError):
            pf.udaf(Sum, return_dtype=object())

        class InvalidType(Sum):
            def get_result_type(self):
                return int

        with self.assertRaisesRegex(TypeError, "get_result_type"):
            pf.udaf(InvalidType)

    def test_invalid_functions_and_async_methods(self):
        class Scalar(ScalarFunction):
            def eval(self, value):
                return value

        class NeedsArgument(Sum):
            def __init__(self, argument):
                self.argument = argument

        for func in (42, Scalar, Scalar(), NeedsArgument):
            with self.subTest(func=func), self.assertRaises(TypeError):
                pf.udaf(func, return_dtype=int, func_type="pandas")
        with self.assertRaises(ValueError):
            pf.udaf(Sum, func_type="arrow")
        with self.assertRaises(TypeError):
            pf.udaf(Sum, deterministic=1)

        async def coroutine(values):
            return 1

        async def generator(values):
            yield 1

        @functools.wraps(generator)
        def wrapped(values):
            return generator(values)

        for func in (coroutine, generator, wrapped):
            with self.subTest(func=func), self.assertRaisesRegex(TypeError, "async"):
                pf.udaf(func, return_dtype=int, func_type="pandas")
        for method in ("accumulate", "get_value", "create_accumulator", "open", "merge"):
            cls = type("AsyncAggregate", (Sum,), {method: coroutine})
            with self.subTest(method=method), self.assertRaisesRegex(TypeError, "async"):
                pf.udaf(cls)

    def test_names_and_determinism(self):
        for name in (None, "", "my_sum"):
            self.assertEqual(pf.udaf(Sum, name=name).__name__, name or "Sum")

        class RandomSum(Sum):
            def is_deterministic(self):
                return False

        with self.assertRaisesRegex(ValueError, "Inconsistent deterministic"):
            pf.udaf(RandomSum)
        adapter = pf.udaf(RandomSum, deterministic=False)._table_udf_wrapper._func
        self.assertFalse(adapter.is_deterministic())

    def test_client_constructor_and_worker_lifecycle(self):
        events = []

        class LifecycleSum(Sum):
            def __init__(self):
                events.append("init")

            def open(self, context):
                events.append(("open", context))

            def create_accumulator(self):
                events.append("accumulator")
                return super().create_accumulator()

            def close(self):
                events.append("close")
                raise RuntimeError("close failure")

        declaration = pf.udaf(LifecycleSum)
        adapter = declaration._table_udf_wrapper._func
        self.assertEqual(events, ["init"])
        adapter.open("worker")
        accumulator = adapter.create_accumulator()
        adapter.accumulate(accumulator, 5)
        adapter.retract(accumulator, 2)
        adapter.merge(accumulator, [[7], [1]])
        self.assertEqual(accumulator, [11])
        self.assertEqual(adapter.get_value(accumulator), 11)
        with self.assertRaisesRegex(RuntimeError, "close failure"):
            adapter.close()
        self.assertEqual(events, ["init", ("open", "worker"), "accumulator", "close"])
        with self.assertRaisesRegex(RuntimeError, "before open"):
            adapter.get_value(accumulator)
        adapter.close()

    def test_callable_classes_construct_on_worker(self):
        events = []

        class Total:
            def __init__(self, offset=0):
                events.append("init")
                self.offset = offset

            def __call__(self, values) -> int:
                return int(values.sum()) + self.offset

        declaration = pf.udaf(Total, func_type="pandas")
        adapter = declaration._table_udf_wrapper._func
        self.assertEqual(events, [])
        adapter.open(None)
        try:
            acc = adapter.create_accumulator()
            adapter.accumulate(acc, pd.Series([1, 2]))
            self.assertEqual(adapter.get_value(acc), 3)
        finally:
            adapter.close()
        self.assertEqual(events, ["init"])
        for func in (Total(2), functools.partial(lambda offset, values: values.sum() + offset, 2)):
            self.assertEqual(_aggregate(pf.udaf(func, return_dtype=int, func_type="pandas"),
                                        pd.Series([1, 2])), 5)

    def test_struct_normalization_and_user_failures(self):
        def statistics(values) -> Statistics:
            return {"count": len(values), "total": int(values.sum())}

        self.assertEqual(_aggregate(pf.udaf(statistics, func_type="pandas"), pd.Series([1, 2])),
                         Row(total=3, count=2))
        dtype = pf.DataType.struct({
            "nested": pf.DataType.list(pf.DataType.struct({"value": pf.DataType.int64()})),
            "missing": pf.DataType.string(),
        })

        class Nested(Sum):
            def get_value(self, accumulator):
                return {"nested": [{"value": accumulator[0]}]}

        self.assertEqual(_aggregate(pf.udaf(Nested, return_dtype=dtype), 3),
                         Row(nested=[Row(value=3)], missing=None))

        def fail(values) -> int:
            raise RuntimeError("user failure")

        with self.assertRaisesRegex(RuntimeError, "user failure"):
            _aggregate(pf.udaf(fail, func_type="pandas"), pd.Series([1]))


class DataFrameUDAFPlanningTests(PyFlinkDataFrameUTTestCase):
    def test_sql_type_declarations(self):
        class SqlSum(Sum):
            def get_result_type(self):
                return "BIGINT"

            def get_accumulator_type(self):
                return "ARRAY<BIGINT>"

        for declaration in (pf.udaf(SqlSum), pf.udaf(
                Sum, return_dtype="BIGINT", accumulator_type="ARRAY<BIGINT>")):
            self.assertEqual(declaration.return_dtype, pf.DataType.int64())
            self.assertEqual(declaration.accumulator_type, pf.DataType.list(pf.DataType.int64()))

    def test_sql_registration_cleans_up_after_planning_error(self):
        total = pf.udaf(Sum)
        source = pf.from_dict({"value": [1]})
        with self.assertRaises(Py4JJavaError):
            pf.sql("SELECT total(missing) FROM src", auto_bind=False, src=source, total=total)
        self.assertNotIn("total", self.t_env.list_user_defined_functions())
        self.assertNotIn("src", self.t_env.list_temporary_views())


class DataFrameUDAFBatchTests(PyFlinkBatchTableTestCase):
    def setUp(self):
        previous = pf.get_table_environment()
        self.addCleanup(pf.set_table_environment, previous)
        pf.set_table_environment(self.t_env)

    def test_pandas_callables_classes_and_multiple_columns(self):
        client_pid = os.getpid()

        class Total:
            def __init__(self):
                if os.getpid() == client_pid:
                    raise AssertionError("Callable class must be constructed on the worker")

            def __call__(self, values, weights) -> int:
                return int((values * weights).sum())

        class WeightedSum(Sum):
            def __init__(self):
                self.pid = os.getpid()

            def open(self, context):
                if self.pid != client_pid:
                    raise AssertionError("AggregateFunction must be constructed on the client")
                self.opened = True

            def accumulate(self, accumulator, values, weights):
                if not self.opened:
                    raise AssertionError("open must be called")
                accumulator[0] += int((values * weights).sum())

        source = pf.from_records([("a", 1, 2), ("a", 3, 4), ("b", 2, 3)],
                                 schema=["category", "value", "weight"])
        weighted = pf.udaf(Total, func_type="pandas")
        result = source.group_by("category").agg(
            total=weighted(pf.col("value"), pf.col("weight")))
        self.assertEqual(result.columns, ["category", "total"])
        self.assertEqual(result.schema.get_field_data_types(),
                         [DataTypes.STRING(), DataTypes.BIGINT()])
        second = source.group_by("category").agg(
            total=pf.udaf(WeightedSum, func_type="pandas")(pf.col("value"), pf.col("weight")))
        self.assertCountEqual(result.union_all(second).collect(),
                              [Row("a", 14), Row("b", 6)] * 2)

    def test_sql_bindings_and_struct_fields(self):
        @pf.udaf(func_type="pandas")
        def stats(values) -> Statistics:
            return {"count": len(values), "total": int(values.sum())}

        source = pf.from_dict({"value": [1, 2, 3]})
        direct = source.agg(stats(pf.col("value")).alias("summary"))
        explicit = pf.sql("SELECT s(`value`) AS summary FROM src", auto_bind=False,
                          src=source, s=stats)
        automatic = pf.sql("SELECT stats(`value`) AS summary FROM source")
        for name in ("stats", "s"):
            self.assertNotIn(name, self.t_env.list_user_defined_functions())
        self.assertCountEqual(direct.union_all(explicit).union_all(automatic).collect(),
                              [Row(Row(6, 3))] * 3)
        projected = explicit.select(pf.col("summary").get("total"),
                                    pf.col("summary").get("count"))
        self.assertEqual(projected.collect(), [Row(6, 3)])

    def test_empty_nulls_and_table_udaf_compatibility(self):
        def total(values) -> int:
            return int(values.sum())

        source = pf.from_dict({"value": [1, None, 3]})
        declaration = pf.udaf(total, func_type="pandas")
        native = table_udaf(total, result_type=DataTypes.BIGINT(), func_type="pandas")
        self.assertEqual(source.agg(total=declaration(pf.col("value"))).collect(), [Row(4)])
        empty = source.filter(pf.col("value") < 0)
        self.assertEqual(empty.agg(total=declaration(pf.col("value"))).collect(),
                         empty.agg(total=native(pf.col("value"))).collect())
        with self.assertRaisesRegex(TableException, "non-Pandas UDAFs"):
            source.agg(total=pf.udaf(Sum)(pf.col("value"))).to_table().explain()


class DataFrameUDAFStreamTests(PyFlinkStreamDataFrameTestCase):
    def test_grouped_global_and_retracting_inputs(self):
        total = pf.udaf(Sum)
        source = pf.from_records([("a", 1), ("a", 2), ("b", 3), ("b", None)],
                                 schema=["category", "value"])
        grouped = source.group_by("category").agg(total=total(pf.col("value")))
        self.assertEqual(self._materialize(grouped, key=["category"]), [("a", 3), ("b", 3)])
        self.assertEqual(self._materialize(source.agg(total=total(pf.col("value"))), key=[]),
                         [(6,)])
        intermediate = source.group_by("category").agg(subtotal=pf.col("value").sum)
        result = intermediate.agg(total=total(pf.col("subtotal")))
        self.assertEqual(self._materialize(result, key=[]), [(6,)])
        with self.assertRaisesRegex(TableException, "Pandas UDAFs"):
            source.agg(total=pf.udaf(lambda values: values.sum(), return_dtype=int,
                                     func_type="pandas")(pf.col("value"))).to_table().explain()

    def test_general_sql_and_empty_input(self):
        total = pf.udaf(Sum())
        source = pf.from_dict({"value": [1, 2, 3]})
        for auto_bind in (False, True):
            bindings = {} if auto_bind else {"source": source, "total": total}
            result = pf.sql("SELECT total(`value`) AS total_value FROM source",
                            auto_bind=auto_bind, **bindings)
            self.assertEqual(self._materialize(result, key=[]), [(6,)])
        empty = source.filter(pf.col("value") < 0)
        native = table_udaf(Sum(), result_type=DataTypes.BIGINT())
        self.assertEqual(self._materialize(empty.agg(total(pf.col("value"))), key=[]),
                         self._materialize(empty.agg(native(pf.col("value"))), key=[]))

    def test_dataview_accumulator_is_preserved(self):
        class DistinctCount(AggregateFunction):
            def create_accumulator(self):
                return Row(MapView(), ListView())

            def accumulate(self, accumulator, value):
                if value not in accumulator[0]:
                    accumulator[0][value] = True
                    accumulator[1].add(value)

            def get_value(self, accumulator) -> int:
                return len(list(accumulator[1].get()))

            def get_accumulator_type(self):
                return DataTypes.ROW([
                    DataTypes.FIELD("seen", DataTypes.MAP_VIEW(DataTypes.STRING(),
                                                               DataTypes.BOOLEAN())),
                    DataTypes.FIELD("values", DataTypes.LIST_VIEW(DataTypes.STRING())),
                ])

        source = pf.from_records([("a", "x"), ("a", "x"), ("a", "y"), ("b", "x")],
                                 schema=["category", "value"])
        result = source.group_by("category").agg(count=pf.udaf(DistinctCount)(pf.col("value")))
        self.assertEqual(self._materialize(result, key=["category"]), [("a", 2), ("b", 1)])

    def test_session_window_merges_accumulators(self):
        data = self.env.from_collection([
            Row("a", 1, datetime.datetime(2020, 1, 1, 0, 0, 1)),
            Row("a", 3, datetime.datetime(2020, 1, 1, 0, 0, 3)),
            Row("a", 2, datetime.datetime(2020, 1, 1, 0, 0, 2)),
        ], type_info=Types.ROW_NAMED(["category", "value", "ts"],
                                     [Types.STRING(), Types.INT(), Types.SQL_TIMESTAMP()]))
        schema = Schema.new_builder().column("category", DataTypes.STRING()) \
            .column("value", DataTypes.INT()) \
            .column("ts", DataTypes.TIMESTAMP(3).bridged_to("java.sql.Timestamp")) \
            .watermark("ts", "ts - INTERVAL '10' SECOND").build()
        table = self.t_env.from_data_stream(data, schema)
        total = pf.udaf(Sum)
        result = table.window(Session.with_gap(lit(2).seconds).on(col("ts")).alias("w")) \
            .group_by(col("category"), col("w")) \
            .select(col("category"), total(col("value")).alias("total"))
        self.assertEqual(pf.from_table(result).collect(), [Row("a", 6)])


if __name__ == "__main__":
    unittest.main()
