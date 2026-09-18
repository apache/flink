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

import functools
import os
import threading
import unittest
from dataclasses import replace
from typing import (
    Annotated, Any, Dict, Generator, Iterable, Iterator, List, Mapping, MutableMapping,
    Optional, Tuple, TypedDict, Union,
)

import cloudpickle
import pyflink.dataframe as pf
from pyflink.common import Row
from pyflink.dataframe.udtf import _resolve_flat_map_udtf
from pyflink.table import DataTypes
from pyflink.table.udf import ScalarFunction, TableFunction
from pyflink.testing.test_case_utils import (
    PyFlinkBatchTableTestCase,
    PyFlinkDataFrameUTTestCase,
    PyFlinkStreamDataFrameTestCase,
)


class _Output(TypedDict):
    value: int
    label: str


def _eval_udtf(declaration, *args):
    adapter = declaration._create_table_wrapper()._func
    adapter.open(None)
    try:
        return list(adapter.eval(*args))
    finally:
        adapter.close()


class DataFrameUDTFDeclarationTests(unittest.TestCase):
    def test_decorator_forms_and_type_inference(self):
        def emit(value) -> Iterator[_Output]:
            yield {"value": value, "label": "ok"}

        expected = pf.DataType.struct({"value": pf.DataType.int64(), "label": pf.DataType.string()})
        for declaration in (pf.udtf(emit), pf.udtf()(emit), pf.udtf(return_dtype=expected)(emit),
                            pf.udtf(emit, return_dtype=_Output)):
            self.assertEqual(declaration.return_dtype, expected)
            self.assertEqual(declaration.__name__, "emit")

    def test_iterable_annotations_describe_one_emitted_item(self):
        hints = (Iterator[int], Iterable[int], Generator[int, None, None], List[int], list[int])
        for hint in hints:
            def emit(value):
                return [value]
            emit.__annotations__ = {"return": hint}
            with self.subTest(hint=hint):
                self.assertEqual(pf.udtf(emit).return_dtype, pf.DataType.int64())

    def test_explicit_type_does_not_resolve_annotations(self):
        def emit(value):
            return [value]
        emit.__annotations__ = {"value": "UnavailableInput", "return": "UnavailableOutput"}
        self.assertEqual(pf.udtf(emit, return_dtype=int).return_dtype, pf.DataType.int64())
        emit.__annotations__["return"] = Iterator[int]
        self.assertEqual(pf.udtf(emit).return_dtype, pf.DataType.int64())

    def test_callable_instance_and_partial(self):
        class Repeat:
            def __init__(self, count):
                self.count = count

            def __call__(self, value) -> Iterator[int]:
                yield from [value] * self.count

        def repeat(count, value) -> Iterator[int]:
            yield from [value] * count

        for func in (Repeat(2), functools.partial(repeat, 2)):
            self.assertEqual(_eval_udtf(pf.udtf(func), 3), [Row(3), Row(3)])

    def test_invalid_declarations(self):
        class Scalar(ScalarFunction):
            def eval(self, value):
                return value

        class NotCallable:
            pass

        class RequiresCallableArgument:
            def __init__(self, count):
                self.count = count

            def __call__(self, value):
                return [value] * self.count

        class RequiresArgument(TableFunction):
            def __init__(self, value):
                self.value = value

            def eval(self, value):
                return [value]

        for func in (123, Scalar(), Scalar, NotCallable,
                     RequiresCallableArgument, RequiresArgument):
            with self.subTest(func=func), self.assertRaises(TypeError):
                pf.udtf(func, return_dtype=int)
        for hint in (Iterator, Iterator[Tuple[int, ...]], Iterator[Tuple]):
            def emit(row):
                return []
            emit.__annotations__ = {"return": hint}
            with self.subTest(hint=hint), self.assertRaises(TypeError):
                pf.udtf(emit)
        with self.assertRaisesRegex(TypeError, "return_dtype"):
            pf.udtf(lambda row: [])
        with self.assertRaisesRegex(ValueError, "at least one"):
            pf.udtf(lambda row: [], return_dtype=pf.DataType.struct({}))

    def test_async_functions_are_rejected(self):
        async def coroutine(row):
            return [row]

        async def generator(row):
            yield row

        class AsyncCallable:
            async def __call__(self, row):
                yield row

        @functools.wraps(generator)
        def hidden_generator(row):
            return generator(row)

        for func in (coroutine, generator, AsyncCallable, AsyncCallable(), hidden_generator):
            with self.subTest(func=func), self.assertRaisesRegex(TypeError, "async"):
                pf.udtf(func, return_dtype=int)

    def test_names_follow_table_udtf_defaults(self):
        def emit(row) -> Iterator[int]:
            yield row[0]

        class Expand(TableFunction):
            def eval(self, row) -> Iterator[int]:
                yield row[0]

        class Repeat:
            def __call__(self, row) -> Iterator[int]:
                yield row[0]

        cases = (
            (emit, None, "emit"), (Expand, None, "Expand"), (Expand(), None, "Expand"),
            (Repeat(), None, "Repeat"), (emit, "", "emit"), (emit, "custom_name", "custom_name"),
        )
        for func, name, expected in cases:
            with self.subTest(func=func, name=name):
                declaration = pf.udtf(func, name=name)
                self.assertEqual(declaration.__name__, expected)
                self.assertEqual(declaration._create_table_wrapper()._name, expected)

    def test_determinism_validation(self):
        with self.assertRaises(TypeError):
            pf.udtf(lambda row: [], return_dtype=int, deterministic=1)

        class Random(TableFunction):
            def eval(self, value) -> Iterator[int]:
                yield value

            def is_deterministic(self):
                return False

        with self.assertRaisesRegex(ValueError, "Inconsistent deterministic"):
            pf.udtf(Random())
        adapter = pf.udtf(Random, deterministic=False, name="random")._create_table_wrapper()._func
        adapter.open(None)
        try:
            self.assertFalse(adapter.is_deterministic())
            self.assertEqual(list(adapter.eval(1)), [Row(1)])
        finally:
            adapter.close()
        with self.assertRaisesRegex(ValueError, "Inconsistent deterministic"):
            pf.udtf(Random)

    def test_table_function_class_constructs_on_client(self):
        events = []

        class Expand(TableFunction):
            def __init__(self):
                events.append("init")

            def open(self, context):
                events.append(("open", context))

            def eval(self, value) -> Iterator[int]:
                yield value

            def close(self):
                events.append("close")

        declaration = pf.udtf(Expand)
        self.assertEqual(events, ["init"])
        adapter = declaration._create_table_wrapper()._func
        self.assertEqual(events, ["init"])
        adapter.open("worker")
        self.assertEqual(list(adapter.eval(7)), [Row(7)])
        adapter.close()
        self.assertEqual(events, ["init", ("open", "worker"), "close"])
        with self.assertRaisesRegex(RuntimeError, "before open"):
            adapter.eval(7)

    def test_adapter_serializes_only_worker_metadata(self):
        def emit(value) -> Iterator[int]:
            yield value

        declaration = pf.udtf(emit)
        context = replace(declaration._declaration_context,
                          localns={"client_lock": threading.Lock()})
        declaration = replace(declaration, _declaration_context=context)
        adapter = cloudpickle.loads(cloudpickle.dumps(declaration._create_table_wrapper()._func))
        adapter.open(None)
        try:
            self.assertEqual(list(adapter.eval(5)), [Row(5)])
        finally:
            adapter.close()

    def test_table_function_constructor_failure_is_reported_on_client(self):
        class FailingConstructor(TableFunction):
            def __init__(self):
                raise RuntimeError("constructor failed")

            def eval(self, value) -> Iterator[int]:
                yield value

        with self.assertRaisesRegex(RuntimeError, "constructor failed"):
            pf.udtf(FailingConstructor)

    def test_table_function_close_failure_resets_adapter(self):
        class FailingClose(TableFunction):
            def eval(self, value) -> Iterator[int]:
                yield value

            def close(self):
                raise RuntimeError("close failed")

        adapter = pf.udtf(FailingClose)._create_table_wrapper()._func
        adapter.open(None)
        self.assertEqual(list(adapter.eval(1)), [Row(1)])
        with self.assertRaisesRegex(RuntimeError, "close failed"):
            adapter.close()
        with self.assertRaisesRegex(RuntimeError, "before open"):
            adapter.eval(1)
        adapter.close()

    def test_output_cardinality_and_lazy_iteration(self):
        events = []

        def emit(value):
            events.append(value)
            yield value
            events.append(value + 1)
            yield value + 1

        adapter = pf.udtf(emit, return_dtype=int)._create_table_wrapper()._func
        adapter.open(None)
        try:
            output = adapter.eval(2)
            self.assertEqual(events, [])
            self.assertEqual(next(output), Row(2))
            self.assertEqual(events, [2])
            self.assertEqual(list(output), [Row(3)])
        finally:
            adapter.close()

        cases = [(None, []), ([], []), (3, [Row(3)]), ((3,), [Row(3)]),
                 (Row(3), [Row(3)]), ([None, 3], [Row(None), Row(3)])]
        for result, expected in cases:
            self.assertEqual(_eval_udtf(pf.udtf(lambda: result, return_dtype=int)), expected)

    def test_struct_and_map_results(self):
        dtype = pf.DataType.struct({
            "value": pf.DataType.int64(),
            "nested": pf.DataType.list(pf.DataType.struct({"label": pf.DataType.string()})),
            "attributes": pf.DataType.map(pf.DataType.string(), pf.DataType.int64()),
        })
        result = {"attributes": {"x": 1}, "nested": [{"label": "ok"}], "value": 7}
        self.assertEqual(_eval_udtf(pf.udtf(lambda: result, return_dtype=dtype)), [
            Row(value=7, nested=[Row(label="ok")], attributes={"x": 1})])
        map_type = pf.DataType.map(pf.DataType.string(), pf.DataType.int64())
        for result in ({"x": 1}, Row({"x": 1}), ({"x": 1},)):
            self.assertEqual(_eval_udtf(pf.udtf(lambda: result, return_dtype=map_type)),
                             [Row({"x": 1})])

    def test_runtime_errors_are_propagated(self):
        dtype = pf.DataType.struct({"a": pf.DataType.int64(), "b": pf.DataType.int64()})
        for result, return_dtype in (((1,), dtype), ((1, 2), pf.DataType.int64())):
            declaration = pf.udtf(lambda: result, return_dtype=return_dtype)
            with self.assertRaises(ValueError):
                _eval_udtf(declaration)

        def fail(row):
            raise RuntimeError("user failure")
        with self.assertRaisesRegex(RuntimeError, "user failure"):
            _eval_udtf(pf.udtf(fail, return_dtype=int), Row(1))

    def test_single_named_field_accepts_scalar_items(self):
        cases = [
            (pf.DataType.string(), ["hello", "world"], [Row(word="hello"), Row(word="world")]),
            (pf.DataType.list(pf.DataType.int64()), [[1, 2], []], [Row(word=[1, 2]), Row(word=[])]),
        ]
        for field_type, result, expected in cases:
            dtype = pf.DataType.struct({"word": field_type})
            self.assertEqual(_eval_udtf(pf.udtf(lambda: result, return_dtype=dtype)), expected)

    def test_flat_map_validates_row_input_before_building_expression(self):
        def column(value: int) -> Iterator[int]:
            yield value

        def multiple(left, right) -> Iterator[int]:
            yield left + right

        def keyword(row, *, required) -> Iterator[int]:
            yield required

        for func in (column, multiple, keyword,
                     pf.udtf(column), pf.udtf(multiple), pf.udtf(keyword)):
            with self.subTest(func=func), self.assertRaisesRegex(ValueError, "row argument"):
                _resolve_flat_map_udtf(func, None, ["x"])
        with self.assertRaisesRegex(ValueError, "return_dtype"):
            _resolve_flat_map_udtf(pf.udtf(column), int, ["x"])
        with self.assertRaises(TypeError):
            _resolve_flat_map_udtf(None, int, ["x"])

    def test_incompatible_input_annotations(self):
        for hint in (Row, Tuple[int], Optional[int], Union[int, str],
                     Annotated[int, "column value"]):
            def expand(row) -> Iterator[int]:
                yield row
            expand.__annotations__["row"] = hint
            for func in (expand, pf.udtf(expand)):
                with self.subTest(hint=hint, func=func):
                    with self.assertRaisesRegex(ValueError, "row argument"):
                        _resolve_flat_map_udtf(func, None, ["x"])

    def test_flat_map_passes_dict_without_mutating_input(self):
        received = []

        @pf.udtf
        def expand(row: Dict[str, Any]) -> Iterator[int]:
            received.append(row.copy())
            yield row.pop("x")

        adapter = expand._create_table_wrapper(("z", "x"))._func
        adapter.open(None)
        self.addCleanup(adapter.close)
        original = Row(original_z=7, original_x=3)
        for value in ((7, 3), original):
            self.assertEqual(list(adapter.eval(value)), [Row(3)])
            self.assertEqual(received[-1], {"z": 7, "x": 3})
        self.assertEqual(original.as_dict(), {"original_z": 7, "original_x": 3})

    def test_callable_class_initializes_per_worker_adapter(self):
        events = []

        class Counter:
            def __init__(self):
                events.append("init")
                self.count = 0

            def __call__(self, value: int) -> Iterator[int]:
                self.count += 1
                yield value + self.count

        declaration = pf.udtf(Counter)
        adapters = [declaration._create_table_wrapper()._func for _ in range(2)]
        self.assertEqual(events, [])
        for adapter in adapters:
            adapter.open(None)
            self.addCleanup(adapter.close)
            self.assertEqual(list(adapter.eval(10)), [Row(11)])
            self.assertEqual(list(adapter.eval(10)), [Row(12)])
        self.assertEqual(events, ["init", "init"])
        adapters[0].close()
        with self.assertRaisesRegex(RuntimeError, "before open"):
            adapters[0].eval(10)

    def test_callable_class_constructor_failure_is_reported_on_worker(self):
        class FailingConstructor:
            def __init__(self):
                raise RuntimeError("constructor failed")

            def __call__(self, value) -> Iterator[int]:
                yield value

        adapter = pf.udtf(FailingConstructor)._create_table_wrapper()._func
        with self.assertRaisesRegex(RuntimeError, "constructor failed"):
            adapter.open(None)
        adapter.close()


class DataFrameUDTFPlanningTests(PyFlinkDataFrameUTTestCase):
    def test_compatible_input_annotations(self):
        class Input(TypedDict):
            x: int

        source = pf.from_dict({"x": [1]})
        for hint in (Input, Optional[Input], Optional[Dict[str, Any]],
                     Union[int, Dict[str, Any]], Mapping[str, Any], MutableMapping[str, Any],
                     Annotated[Dict[str, Any], "input row"]):
            def expand(row) -> Iterator[int]:
                yield row["x"]
            expand.__annotations__["row"] = hint
            for func in (expand, pf.udtf(expand)):
                with self.subTest(hint=hint, func=func):
                    self.assertEqual(source.flat_map(func).columns, ["f0"])

    def test_callable_class_signatures_and_annotations(self):
        class Expand:
            def __call__(self, row: Dict[str, Any], optional=0) -> Iterator[int]:
                yield row["x"] + optional

        class ClassMethod:
            @classmethod
            def __call__(cls, row: Dict[str, Any]) -> Iterator[int]:
                yield row["x"]

        class StaticMethod:
            @staticmethod
            def __call__(row: Dict[str, Any]) -> Iterator[int]:
                yield row["x"]

        class MultipleArguments:
            def __call__(self, left, right) -> Iterator[int]:
                yield left + right

        class ColumnArgument:
            def __call__(self, value: int) -> Iterator[int]:
                yield value

        source = pf.from_dict({"x": [1]})
        for cls in (Expand, ClassMethod, StaticMethod):
            for func in (cls, pf.udtf(cls)):
                with self.subTest(func=func):
                    self.assertEqual(source.flat_map(func).columns, ["f0"])
        for cls in (MultipleArguments, ColumnArgument):
            for func in (cls, pf.udtf(cls)):
                with self.subTest(func=func), self.assertRaisesRegex(ValueError, "row argument"):
                    source.flat_map(func)

    def test_wrapped_static_method_still_validates_input_annotations(self):
        def transparent(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper

        class Expand:
            @staticmethod
            @transparent
            def __call__(value: int) -> Iterator[int]:
                yield value

        source = pf.from_dict({"x": [1]})
        for func in (Expand, pf.udtf(Expand), Expand(), pf.udtf(Expand())):
            with self.subTest(func=func), self.assertRaisesRegex(ValueError, "dict row argument"):
                source.flat_map(func)

    def test_unnamed_multiple_fields_require_schema(self):
        def emit(row) -> Iterator[Tuple[int, str]]:
            yield row["x"], "ok"

        source = pf.from_dict({"x": [1]})
        with self.assertRaisesRegex(ValueError, "named output fields"):
            source.flat_map(emit)
        named = source.flat_map(emit, return_dtype="ROW<value BIGINT, label STRING>")
        self.assertEqual(named.columns, ["value", "label"])

    def test_call_metadata_aliases_and_shared_declaration(self):
        @pf.udtf
        def emit(value) -> Iterator[_Output]:
            yield {"value": value, "label": "ok"}

        source = pf.from_dict({"x": [1]})
        call = emit(pf.col("x"))
        aliased = call.alias("v", "l")
        self.assertIsNone(call.output_aliases)
        self.assertEqual(aliased.output_aliases, ("v", "l"))
        for names in (("one",), ("same", "same"), ("", "other")):
            with self.subTest(names=names), self.assertRaises(ValueError):
                call.alias(*names)
        table = source.to_table().join_lateral(aliased.expression)
        self.assertEqual(pf.from_table(table).columns, ["x", "v", "l"])

    def test_constructing_plan_does_not_reconstruct_user_class(self):
        events = []

        class Expand(TableFunction):
            def __init__(self):
                events.append("init")

            def eval(self, row: Dict[str, Any]) -> Iterator[int]:
                yield next(iter(row.values()))

        declaration = pf.udtf(Expand)
        self.assertEqual(events, ["init"])
        for name in ("x", "y"):
            result = pf.from_dict({name: [1]}).flat_map(declaration)
            self.assertEqual(result.columns, ["f0"])
        self.assertEqual(events, ["init"])


class _DataFrameFlatMapTests:
    def setUp(self):
        super().setUp()
        previous = pf.get_table_environment()
        self.addCleanup(pf.set_table_environment, previous)
        pf.set_table_environment(self.t_env)

    def test_typed_dict_input_and_named_output_pipeline(self):
        class Input(TypedDict):
            count: int
            label: str

        def expand(row: Input) -> Iterator[_Output]:
            for value in range(row["count"]):
                yield {"label": row["label"], "value": value}

        source = pf.from_records([(0, "empty"), (1, "a"), (2, "b")], schema=["count", "label"])
        result = source.flat_map(expand)
        self.assertEqual(result.columns, ["value", "label"])
        self.assertEqual(result.schema.get_field_data_types(),
                         [DataTypes.BIGINT(), DataTypes.STRING()])
        rows = result.filter(pf.col("value") >= 0).select("label", "value").collect()
        self.assertCountEqual(rows, [Row("a", 0), Row("b", 0), Row("b", 1)])

    def test_plain_and_decorated_functions_receive_dict(self):
        dtype = pf.DataType.struct({
            "is_dict": pf.DataType.bool(),
            "first": pf.DataType.int64(),
            "by_name": pf.DataType.int64(),
        })

        def expand(row: Dict[str, Any]):
            yield isinstance(row, dict), next(iter(row.values())), row["x"]

        source = pf.from_records([(7, 3)], schema=["z", "x"])
        plain = source.flat_map(expand, return_dtype=dtype)
        decorated = source.flat_map(pf.udtf(expand, return_dtype=dtype))
        self.assertEqual(plain.columns, ["is_dict", "first", "by_name"])
        self.assertEqual(decorated.columns, plain.columns)
        self.assertEqual(plain.union_all(decorated).collect(), [Row(True, 7, 3)] * 2)

    def test_wrapper_row_input_reuse_and_column_calls(self):
        @pf.udtf
        def expand(value) -> Iterator[int]:
            value = next(iter(value.values())) if isinstance(value, dict) else value
            yield value
            yield value + 1

        a = pf.from_dict({"x": [1]})
        b = pf.from_dict({"y": [3]})
        before = expand(pf.col("x")).alias("out")
        first, second = a.flat_map(expand), b.flat_map(expand)
        self.assertEqual(first.columns, ["f0"])
        after = expand(pf.col("x")).alias("out")
        combined = first.union_all(second)
        for call in (before, after):
            lateral = pf.from_table(a.to_table().join_lateral(call.expression)).select("out")
            combined = combined.union_all(lateral)
        self.assertCountEqual(combined.collect(), [Row(1), Row(2)] * 3 + [Row(3), Row(4)])

    def test_table_function_and_callable_classes_and_instances(self):
        client_pid = os.getpid()

        class Expand(TableFunction):
            def __init__(self):
                self.constructor_pid = os.getpid()

            def open(self, context):
                self.offset = 10

            def eval(self, row: Dict[str, Any]) -> Iterator[int]:
                if self.constructor_pid != client_pid:
                    raise AssertionError("TableFunction must be constructed on the client")
                yield row["x"] + self.offset

        class Repeat:
            def __call__(self, row: Dict[str, Any]) -> Iterator[int]:
                yield row["x"]
                yield row["x"]

        source = pf.from_dict({"x": [1]})
        first = source.flat_map(pf.udtf(Expand))
        second = source.flat_map(pf.udtf(Expand()))
        combined = first.union_all(second)
        for func in (Repeat, Repeat(), pf.udtf(Repeat), pf.udtf(Repeat())):
            combined = combined.union_all(source.flat_map(func))
        self.assertCountEqual(combined.collect(), [Row(11)] * 2 + [Row(1)] * 8)

    def test_wrapped_callable_class_methods(self):
        def transparent(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper

        class InstanceMethod:
            @transparent
            def __call__(self, row: Dict[str, Any]) -> Iterator[int]:
                yield row["x"]

        class StaticMethod:
            @staticmethod
            @transparent
            def __call__(row: Dict[str, Any]) -> Iterator[int]:
                yield row["x"]

        class ClassMethod:
            @classmethod
            @transparent
            def __call__(cls, row: Dict[str, Any]) -> Iterator[int]:
                yield row["x"]

        class InheritedStaticMethod(StaticMethod):
            pass

        def expand(row: Dict[str, Any]) -> Iterator[int]:
            yield row["x"]

        class WrappedFunction:
            @functools.wraps(expand)
            def __call__(self, *args, **kwargs):
                return expand(*args, **kwargs)

        source = pf.from_dict({"x": [1]})
        combined = None
        for cls in (InstanceMethod, StaticMethod, ClassMethod,
                    InheritedStaticMethod, WrappedFunction):
            self.assertEqual(list(cls()({"x": 1})), [1])
            for func in (cls(), pf.udtf(cls())):
                self.assertEqual(source.flat_map(func).columns, ["f0"])
            for func in (cls, pf.udtf(cls)):
                result = source.flat_map(func)
                combined = result if combined is None else combined.union_all(result)
        self.assertCountEqual(combined.collect(), [Row(1)] * 10)

    def test_nested_values_and_nulls(self):
        dtype = pf.DataType.struct({
            "payload": pf.DataType.struct({"label": pf.DataType.string()}),
            "values": pf.DataType.list(pf.DataType.int64()),
            "attributes": pf.DataType.map(pf.DataType.string(), pf.DataType.int64()),
            "missing": pf.DataType.string(),
        })

        def expand(row: Optional[Dict[str, Any]]):
            if row["x"] == 0:
                return None
            return {"payload": {"label": "ok"}, "values": [1, None], "attributes": {"a": 2}}

        result = pf.from_dict({"x": [0, 1]}).flat_map(expand, return_dtype=dtype)
        # Table.collect cannot decode NULL array elements, so inspect them in the JVM.
        projected = result.select(
            "payload", pf.col("values").cardinality, pf.col("values").at(1),
            pf.col("values").at(2).is_null, "attributes", "missing")
        self.assertEqual(projected.collect(), [Row(Row("ok"), 2, 1, True, {"a": 2}, None)])

    def test_map_output_is_one_column(self):
        def expand(row) -> Iterator[Dict[str, int]]:
            yield {"value": row["x"]}
            yield {"value": row["x"] + 1}

        result = pf.from_dict({"x": [1]}).flat_map(expand)
        self.assertEqual(result.columns, ["f0"])
        self.assertEqual(result.schema.get_field_data_types(),
                         [DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())])
        self.assertCountEqual(result.collect(), [Row({"value": 1}), Row({"value": 2})])


class DataFrameFlatMapStreamTests(_DataFrameFlatMapTests, PyFlinkStreamDataFrameTestCase):
    pass


class DataFrameFlatMapBatchTests(_DataFrameFlatMapTests, PyFlinkBatchTableTestCase):
    pass


class DataFrameFlatMapThreadTests(_DataFrameFlatMapTests, PyFlinkStreamDataFrameTestCase):
    def setUp(self):
        super().setUp()
        self.t_env.get_config().set("python.execution-mode", "thread")


if __name__ == "__main__":
    unittest.main()
