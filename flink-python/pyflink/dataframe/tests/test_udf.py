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

import asyncio
import functools
import inspect
import unittest
from dataclasses import dataclass
from typing import TypedDict

import pandas as pd
import pyarrow as pa
import pyflink.dataframe as pf
from pyflink.common import Row
from pyflink.table import DataTypes as TableDataTypes
from pyflink.table.types import RowType
from pyflink.table.udf import AsyncScalarFunction, ScalarFunction, TableFunction
from pyflink.testing.test_case_utils import (
    PyFlinkDataFrameUTTestCase,
    PyFlinkStreamDataFrameTestCase,
)


class DataFrameUDFDeclarationTests(unittest.TestCase):
    def test_function_declarations_return_types_and_metadata(self):
        class Details(TypedDict):
            label: str
            scores: list[int]

        class Result(TypedDict):
            id: int
            details: Details

        def add_one(value: int) -> int:
            """Add one to a value."""
            return value + 1

        def identity(value):
            return value

        def describe(value: int) -> Result:
            return {
                "id": value,
                "details": {"label": str(value), "scores": [value]},
            }

        decorated = pf.udf(add_one)

        from pyflink.dataframe.udf import DataFrameUDFWrapper

        self.assertIsInstance(decorated, DataFrameUDFWrapper)
        self.assertFalse(hasattr(pf, "DataFrameUDFWrapper"))
        self.assertEqual(decorated.return_dtype, pf.DataType.int64())
        self.assertEqual(decorated.__name__, "add_one")
        self.assertEqual(decorated.__doc__, "Add one to a value.")
        self.assertIs(decorated.__wrapped__, add_one)

        configured = pf.udf(return_dtype=pf.DataType.string())(
            lambda value: str(value)
        )
        direct = pf.udf(functools.partial(add_one), name="partial_add_one")

        self.assertEqual(configured.return_dtype, pf.DataType.string())
        self.assertEqual(direct.return_dtype, pf.DataType.int64())
        self.assertEqual(direct.__name__, "partial_add_one")

        declarations = [
            (
                "Python type",
                lambda: pf.udf(identity, return_dtype=int),
                pf.DataType.int64(),
            ),
            (
                "nested TypedDict annotation",
                lambda: pf.udf(describe),
                pf.DataType.struct(
                    {
                        "id": pf.DataType.int64(),
                        "details": pf.DataType.struct(
                            {
                                "label": pf.DataType.string(),
                                "scores": pf.DataType.list(pf.DataType.int64()),
                            }
                        ),
                    }
                ),
            ),
        ]
        for case_name, declare, expected in declarations:
            with self.subTest(case=case_name):
                self.assertEqual(declare().return_dtype, expected)

    def test_callable_classes_and_instances_infer_from_invocation_method(self):
        plain_constructor_calls = []
        scalar_constructor_calls = []

        class AddOne:
            def __init__(self):
                plain_constructor_calls.append("AddOne")

            def __call__(self, value: int) -> int:
                return value + 1

        class AddOffset:
            def __init__(self, offset):
                self.offset = offset

            def __call__(self, value: int) -> int:
                return value + self.offset

        class NamedCallable:
            __name__ = "configured_add"

            def __call__(self, value: int) -> int:
                return value + 1

        class Double(ScalarFunction):
            def __init__(self):
                scalar_constructor_calls.append("Double")

            def eval(self, *values: int) -> int:
                value, = values
                return value * 2

        class AsyncDouble(AsyncScalarFunction):
            def __init__(self):
                scalar_constructor_calls.append("AsyncDouble")

            async def eval(self, *values: int) -> int:
                value, = values
                return value * 2

        class AddScalarOffset(ScalarFunction):
            def __init__(self, offset):
                self.offset = offset

            def eval(self, *values: int) -> int:
                value, = values
                return value + self.offset

        named_callable = NamedCallable()
        double_instance = Double()
        async_double_instance = AsyncDouble()
        scalar_constructor_calls.clear()
        callables = [
            AddOne,
            AddOffset(2),
            named_callable,
            Double,
            double_instance,
            AsyncDouble,
            async_double_instance,
            AddScalarOffset(2),
        ]
        for source in callables:
            with self.subTest(source=source):
                decorated = pf.udf(source)
                self.assertEqual(decorated.return_dtype, pf.DataType.int64())

        self.assertEqual(plain_constructor_calls, [])
        self.assertEqual(scalar_constructor_calls, [])
        self.assertEqual(pf.udf(named_callable).__name__, "configured_add")

        decorated_class = pf.udf(Double)
        self.assertIs(decorated_class.__wrapped__, Double)
        self.assertEqual(decorated_class.__qualname__, Double.__qualname__)

    def test_func_type_resolution_and_async_detection(self):
        def pandas_add_one(values: pd.Series) -> pd.Series:
            return values + 1

        def with_pandas_context(context: pd.Series, value: int) -> int:
            return value

        def pandas_forward_reference(values):
            return values

        pandas_forward_reference.__annotations__["values"] = "pandas.Series"

        def mixed(values: pd.Series, offset: int):
            return values + offset

        def arrow_add_one(values: pa.Array) -> pa.Array:
            return pa.array([value.as_py() + 1 for value in values])

        async def async_add_one(value: int) -> int:
            return value + 1

        async def async_pandas(values: pd.Series) -> pd.Series:
            return values + 1

        class PandasCallable:
            def __call__(self, values: pd.Series) -> pd.Series:
                return values + 1

        class PandasScalarFunction(ScalarFunction):
            def eval(self, *values: pd.Series) -> pd.Series:
                value, = values
                return value + 1

        class AsyncScalarClass(AsyncScalarFunction):
            async def eval(self, *values: int) -> int:
                value, = values
                return value + 1

        declarations = [
            (
                "inferred pandas",
                lambda: pf.udf(pandas_add_one, return_dtype=pf.DataType.int64()),
                "pandas",
                False,
            ),
            (
                "bound pandas annotation is ignored",
                lambda: pf.udf(
                    functools.partial(with_pandas_context, pd.Series([1])),
                ),
                "general",
                False,
            ),
            (
                "pandas forward reference",
                lambda: pf.udf(
                    pandas_forward_reference,
                    return_dtype=pf.DataType.int64(),
                ),
                "pandas",
                False,
            ),
            (
                "any pandas annotation selects pandas",
                lambda: pf.udf(mixed, return_dtype=pf.DataType.int64()),
                "pandas",
                False,
            ),
            (
                "explicit general wins",
                lambda: pf.udf(
                    pandas_add_one,
                    return_dtype=pf.DataType.int64(),
                    func_type="general",
                ),
                "general",
                False,
            ),
            (
                "pyarrow annotations remain general",
                lambda: pf.udf(arrow_add_one, return_dtype=pf.DataType.int64()),
                "general",
                False,
            ),
            (
                "async general",
                lambda: pf.udf(async_add_one),
                "general",
                True,
            ),
            (
                "pandas callable class",
                lambda: pf.udf(
                    PandasCallable,
                    return_dtype=pf.DataType.int64(),
                ),
                "pandas",
                False,
            ),
            (
                "pandas scalar-function class",
                lambda: pf.udf(
                    PandasScalarFunction,
                    return_dtype=pf.DataType.int64(),
                ),
                "pandas",
                False,
            ),
            (
                "async scalar-function class",
                lambda: pf.udf(AsyncScalarClass),
                "general",
                True,
            ),
        ]
        for case_name, declare, expected_type, expected_async in declarations:
            with self.subTest(case=case_name):
                wrapped = declare()
                self.assertEqual(wrapped._func_type, expected_type)
                self.assertEqual(wrapped._source.is_async, expected_async)

        invalid_declarations = [
            (
                "async inferred pandas",
                lambda: pf.udf(async_pandas, return_dtype=pf.DataType.int64()),
                ValueError,
                "Async scalar functions",
            ),
            (
                "async explicit pandas",
                lambda: pf.udf(
                    async_add_one,
                    return_dtype=pf.DataType.int64(),
                    func_type="pandas",
                ),
                ValueError,
                "Async scalar functions",
            ),
        ]
        for case_name, declare, error_type, message in invalid_declarations:
            with self.subTest(case=case_name):
                with self.assertRaisesRegex(error_type, message):
                    declare()

    def test_determinism_and_name_metadata(self):
        class NonDeterministic(ScalarFunction):
            def eval(self, *values: int) -> int:
                value, = values
                return value

            def is_deterministic(self):
                return False

        class DefaultDeterministic(ScalarFunction):
            def eval(self, *values: int) -> int:
                value, = values
                return value

        instance = NonDeterministic()
        declarations = [
            (
                "matching instance metadata",
                lambda: pf.udf(instance, deterministic=False),
                False,
            ),
            ("class default", lambda: pf.udf(DefaultDeterministic), True),
            (
                "class matching metadata",
                lambda: pf.udf(NonDeterministic, deterministic=False),
                False,
            ),
        ]
        for case_name, declare, expected in declarations:
            with self.subTest(case=case_name):
                self.assertEqual(declare()._deterministic, expected)

        self.assertIs(
            inspect.signature(pf.udf).parameters["deterministic"].default,
            True,
        )
        with self.assertRaisesRegex(ValueError, "Inconsistent deterministic"):
            pf.udf(instance)
        self.assertTrue(pf.udf(NonDeterministic)._deterministic)

        named = pf.udf(instance, deterministic=False, name="identity")
        self.assertEqual(named.__name__, "identity")
        self.assertEqual(named._table_udf_wrapper._name, "identity")

    def test_general_structured_results_are_normalized_recursively(self):
        from pyflink.dataframe.udf import _normalize_user_value

        class Details:
            __slots__ = ("label", "scores")

            def __init__(self, label, scores):
                self.label = label
                self.scores = scores

        class ItemsOnly:
            def __init__(self, items):
                self._items = items

            def items(self):
                return self._items

        @dataclass
        class Result:
            id: int
            details: Details
            attributes: dict

        return_dtype = pf.DataType.struct(
            {
                "id": pf.DataType.int64(),
                "details": pf.DataType.struct(
                    {
                        "label": pf.DataType.string(),
                        "scores": pf.DataType.list(pf.DataType.int64()),
                    }
                ),
                "attributes": pf.DataType.map(
                    pf.DataType.string(), pf.DataType.int64()
                ),
            }
        )
        table_type = return_dtype._to_table_data_type()
        self.assertIsInstance(table_type, RowType)

        cases = [
            (
                "mapping",
                {
                    "id": 1,
                    "details": {"scores": (2, 3), "ignored": "extra"},
                    "attributes": [("answer", 42)],
                    "ignored": "extra",
                },
                Row(
                    id=1,
                    details=Row(label=None, scores=[2, 3]),
                    attributes={"answer": 42},
                ),
            ),
            (
                "named row",
                Row(
                    id=4,
                    details=Row(label="named", scores=[5]),
                    attributes={"count": 6},
                ),
                Row(
                    id=4,
                    details=Row(label="named", scores=[5]),
                    attributes={"count": 6},
                ),
            ),
            (
                "positional list and tuple",
                [7, ("positional", (8, 9)), {"count": 10}],
                Row(
                    id=7,
                    details=Row(label="positional", scores=[8, 9]),
                    attributes={"count": 10},
                ),
            ),
            (
                "dataclass and attribute objects",
                Result(
                    id=11,
                    details=Details(label="object", scores=[12]),
                    attributes=ItemsOnly([("count", 13)]),
                ),
                Row(
                    id=11,
                    details=Row(label="object", scores=[12]),
                    attributes={"count": 13},
                ),
            ),
        ]
        for case_name, value, expected in cases:
            with self.subTest(case=case_name):
                self.assertEqual(
                    _normalize_user_value(value, table_type), expected
                )

        with self.assertRaisesRegex(ValueError, "Expected 3 value"):
            _normalize_user_value((1, 2), table_type)
        with self.assertRaisesRegex(TypeError, "Expected a Mapping"):
            _normalize_user_value(object(), table_type)

    def test_invalid_declarations_fail_eagerly(self):
        def missing_return(value):
            return value

        def pandas_identity(values: pd.Series) -> pd.Series:
            return values

        class RequiresArgument:
            def __init__(self, value):
                self.value = value

            def __call__(self, other: int) -> int:
                return other + self.value

        class RequiresScalarArgument(ScalarFunction):
            def __init__(self, value):
                self.value = value

            def eval(self, *values: int) -> int:
                value, = values
                return value + self.value

        class RequiresAsyncScalarArgument(AsyncScalarFunction):
            def __init__(self, value):
                self.value = value

            async def eval(self, *values: int) -> int:
                value, = values
                return value + self.value

        class NotCallable:
            pass

        class NonScalarFunction(TableFunction):
            def eval(self, value):
                return value

        invalid_declarations = [
            (
                "not callable",
                lambda: pf.udf(42, return_dtype=pf.DataType.int64()),
                TypeError,
                "func must be callable",
            ),
            (
                "non-callable class",
                lambda: pf.udf(NotCallable, return_dtype=pf.DataType.int64()),
                TypeError,
                "func must be callable",
            ),
            (
                "non-scalar UDF class",
                lambda: pf.udf(
                    NonScalarFunction,
                    return_dtype=pf.DataType.int64(),
                ),
                TypeError,
                "func must be a scalar UDF",
            ),
            (
                "missing return",
                lambda: pf.udf(missing_return),
                TypeError,
                "Cannot infer return_dtype",
            ),
            (
                "Table return type",
                lambda: pf.udf(
                    missing_return, return_dtype=TableDataTypes.BIGINT()
                ),
                TypeError,
                "return_dtype must be",
            ),
            (
                "required constructor argument",
                lambda: pf.udf(RequiresArgument),
                TypeError,
                "zero-argument constructor",
            ),
            (
                "required scalar constructor argument",
                lambda: pf.udf(RequiresScalarArgument),
                TypeError,
                "zero-argument constructor",
            ),
            (
                "required async scalar constructor argument",
                lambda: pf.udf(RequiresAsyncScalarArgument),
                TypeError,
                "zero-argument constructor",
            ),
            (
                "invalid determinism",
                lambda: pf.udf(
                    missing_return,
                    return_dtype=pf.DataType.int64(),
                    deterministic=1,
                ),
                TypeError,
                "deterministic must be",
            ),
            (
                "invalid name",
                lambda: pf.udf(
                    missing_return,
                    return_dtype=pf.DataType.int64(),
                    name=1,
                ),
                TypeError,
                "name must be",
            ),
            (
                "empty name",
                lambda: pf.udf(
                    missing_return,
                    return_dtype=pf.DataType.int64(),
                    name="",
                ),
                ValueError,
                "name must not be empty",
            ),
            (
                "arrow func type",
                lambda: pf.udf(
                    missing_return,
                    return_dtype=pf.DataType.int64(),
                    func_type="arrow",
                ),
                ValueError,
                "func_type must be one of",
            ),
            (
                "pandas return type required",
                lambda: pf.udf(pandas_identity),
                TypeError,
                "return_dtype is required",
            ),
        ]
        for case_name, declare, error_type, message in invalid_declarations:
            with self.subTest(case=case_name):
                with self.assertRaisesRegex(error_type, message):
                    declare()


class DataFrameUDFAdapterTests(unittest.TestCase):
    def test_scalar_function_lifecycle_and_cleanup(self):
        from pyflink.dataframe.udf import (
            _DataFrameAsyncScalarFunctionAdapter,
            _DataFrameScalarFunctionAdapter,
            _UDFUsage,
            _resolve_udf_source,
        )

        events = []

        def create_adapter(source, deterministic=True, async_mode=False):
            adapter_type = (
                _DataFrameAsyncScalarFunctionAdapter
                if async_mode
                else _DataFrameScalarFunctionAdapter
            )
            return adapter_type(
                _resolve_udf_source(source),
                pf.DataType.int64(),
                deterministic,
                _UDFUsage.EXPRESSION,
                "general",
            )

        class LifecycleFunction(ScalarFunction):
            def __init__(self):
                events.append("init")

            def open(self, function_context):
                events.append(("open", function_context))

            def eval(self, value):
                return value + 1

            def close(self):
                events.append("close")

        context = object()
        adapter = create_adapter(LifecycleFunction)
        self.assertEqual(events, [])

        with self.assertRaisesRegex(RuntimeError, "before open"):
            adapter.eval(1)

        adapter.open(context)
        self.assertEqual(adapter.eval(1), 2)
        adapter.close()

        with self.assertRaisesRegex(RuntimeError, "before open"):
            adapter.eval(1)

        adapter.open(context)
        self.assertEqual(adapter.eval(2), 3)
        adapter.close()
        self.assertEqual(
            events,
            [
                "init",
                ("open", context),
                "close",
                "init",
                ("open", context),
                "close",
            ],
        )

        failed_lifecycle_events = []

        class NonDeterministicFunction(ScalarFunction):
            def __init__(self):
                failed_lifecycle_events.append("init")

            def eval(self, value):
                return value

            def is_deterministic(self):
                return False

            def close(self):
                failed_lifecycle_events.append("close")

        mismatched_adapter = create_adapter(NonDeterministicFunction)
        with self.assertRaisesRegex(ValueError, "Inconsistent deterministic"):
            mismatched_adapter.open(context)
        mismatched_adapter.close()
        self.assertEqual(failed_lifecycle_events, ["init"])

        async_events = []

        class AsyncLifecycleFunction(AsyncScalarFunction):
            def __init__(self):
                async_events.append("init")

            def open(self, function_context):
                async_events.append(("open", function_context))

            async def eval(self, value):
                return value + 1

            def close(self):
                async_events.append("close")

        async_adapter = create_adapter(
            AsyncLifecycleFunction,
            async_mode=True,
        )
        self.assertEqual(async_events, [])
        async_adapter.open(context)
        self.assertEqual(asyncio.run(async_adapter.eval(1)), 2)
        async_adapter.close()
        self.assertEqual(async_events, ["init", ("open", context), "close"])

        initialization_failure_events = []

        class ConstructorFailureFunction(ScalarFunction):
            def __init__(self):
                initialization_failure_events.append("init")
                raise RuntimeError("constructor failed")

            def eval(self, value):
                return value

        constructor_failure_adapter = create_adapter(ConstructorFailureFunction)
        with self.assertRaisesRegex(RuntimeError, "constructor failed"):
            constructor_failure_adapter.open(context)
        constructor_failure_adapter.close()
        self.assertEqual(initialization_failure_events, ["init"])

        class OpenFailureFunction(ScalarFunction):
            def __init__(self):
                initialization_failure_events.append("second init")

            def open(self, function_context):
                initialization_failure_events.append("open")
                raise RuntimeError("open failed")

            def eval(self, value):
                return value

            def close(self):
                initialization_failure_events.append("close")

        open_failure_adapter = create_adapter(OpenFailureFunction)
        with self.assertRaisesRegex(RuntimeError, "open failed"):
            open_failure_adapter.open(context)
        open_failure_adapter.close()
        self.assertEqual(
            initialization_failure_events,
            ["init", "second init", "open"],
        )

        deferred_constructor_calls = []

        class DeferredCallable:
            def __init__(self):
                deferred_constructor_calls.append("init")

            def __call__(self, value):
                return value + 1

        deferred_adapter = create_adapter(DeferredCallable)
        deferred_adapter.open(context)
        self.assertEqual(deferred_adapter.eval(1), 2)
        deferred_adapter.close()
        deferred_adapter.open(context)
        self.assertEqual(deferred_adapter.eval(2), 3)
        deferred_adapter.close()
        self.assertEqual(deferred_constructor_calls, ["init", "init"])

        class FailingCloseFunction(ScalarFunction):
            def eval(self, value):
                return value

            def close(self):
                raise RuntimeError("close failed")

        failing_adapter = create_adapter(FailingCloseFunction())
        failing_adapter.open(context)
        with self.assertRaisesRegex(RuntimeError, "close failed"):
            failing_adapter.close()
        with self.assertRaisesRegex(RuntimeError, "before open"):
            failing_adapter.eval(1)

    def test_binding_failure_closes_and_resets_deferred_scalar_class(self):
        from pyflink.dataframe.udf import (
            _DataFrameScalarFunctionAdapter,
            _UDFUsage,
            _resolve_udf_source,
        )

        events = []

        class BindingFailureFunction(ScalarFunction):
            def __init__(self):
                events.append("init")

            def open(self, function_context):
                events.append("open")

            def eval(self, value):
                return value

            def close(self):
                events.append("close")
                raise RuntimeError("close failed")

        adapter = _DataFrameScalarFunctionAdapter(
            _resolve_udf_source(BindingFailureFunction),
            pf.DataType.int64(),
            True,
            _UDFUsage.MAP,
            "general",
        )
        for _ in range(2):
            with self.assertRaisesRegex(NotImplementedError, "'map'"):
                adapter.open(object())
            adapter.close()

        self.assertEqual(
            events,
            ["init", "open", "close", "init", "open", "close"],
        )


class DataFrameUDFPlannerTests(PyFlinkDataFrameUTTestCase):
    def test_with_columns_binds_expressions_and_resolves_output_schema(self):
        sql_typed = pf.udf(lambda value: value, return_dtype="BIGINT")

        @pf.udf(name="render_value")
        def render(value: int, suffix: str) -> str:
            return f"{value}{suffix}"

        @pf.udf(
            return_dtype=pf.DataType.struct(
                {
                    "value": pf.DataType.int64(),
                    "tags": pf.DataType.list(pf.DataType.string()),
                }
            )
        )
        def describe(value):
            return {"value": value, "tags": [str(value)]}

        result = pf.from_records([(1,)], schema=["id"]).with_columns(
            rendered=render(pf.col("id"), "-literal"),
            description=describe(pf.col("id")),
            sql_value=sql_typed(pf.col("id")),
        )

        self.assert_dataframe_schema(
            result,
            ["id", "rendered", "description", "sql_value"],
            [
                TableDataTypes.BIGINT(),
                TableDataTypes.STRING(),
                TableDataTypes.ROW(
                    [
                        TableDataTypes.FIELD("value", TableDataTypes.BIGINT()),
                        TableDataTypes.FIELD(
                            "tags", TableDataTypes.ARRAY(TableDataTypes.STRING())
                        ),
                    ]
                ),
                TableDataTypes.BIGINT(),
            ],
        )


class DataFrameUDFITCase(PyFlinkStreamDataFrameTestCase):
    def test_supported_scalar_udfs_in_one_job(self):
        @dataclass
        class Details:
            doubled: int
            labels: list

        @pf.udf
        def add_one(value: int) -> int:
            return value + 1

        @pf.udf
        async def add_two(value: int) -> int:
            return value + 2

        @pf.udf(return_dtype=pf.DataType.int64(), func_type="pandas")
        def add_three(values: pd.Series) -> pd.Series:
            return values + 3

        @pf.udf(
            return_dtype=pf.DataType.struct(
                {
                    "doubled": pf.DataType.int64(),
                    "labels": pf.DataType.list(pf.DataType.string()),
                }
            )
        )
        def details(value):
            return Details(doubled=value * 2, labels=[str(value)])

        class DeferredCallable:
            def __call__(self, value: int) -> int:
                return value + 4

        class OpenedScalarFunction(ScalarFunction):
            def open(self, function_context):
                self._increment = 5

            def eval(self, *values: int) -> int:
                value, = values
                return value + self._increment

        class ClassNonDeterministic(ScalarFunction):
            def eval(self, *values: int) -> int:
                value, = values
                return value + 6

            def is_deterministic(self):
                return False

        deferred = pf.udf(DeferredCallable)
        opened_scalar_class = pf.udf(OpenedScalarFunction)
        scalar_class = pf.udf(ClassNonDeterministic, deterministic=False)

        result = (
            pf.from_records([(1,)], schema=["id"])
            .with_columns(async_value=add_two(pf.col("id")))
            .with_columns(
                sync_value=add_one(pf.col("id")),
                pandas_value=add_three(pf.col("id")),
                details=details(pf.col("id")),
                deferred_value=deferred(pf.col("id")),
                scalar_value=opened_scalar_class(pf.col("id")),
                scalar_class_value=scalar_class(pf.col("id")),
            )
        )

        self.assertEqual(
            result.collect(),
            [Row(1, 3, 2, 4, Row(2, ["1"]), 5, 6, 7)],
        )


if __name__ == "__main__":
    unittest.main()
