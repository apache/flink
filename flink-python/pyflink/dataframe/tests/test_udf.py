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
import importlib
import inspect
import operator
import types
import unittest
from dataclasses import dataclass
from typing import Any, Callable, NamedTuple, Optional, TypedDict, cast
from unittest import mock

import pandas as pd
import pyarrow as pa
import pyflink.dataframe as pf
from pyflink.common import Row, RowKind
from pyflink.table import DataTypes as TableDataTypes
from pyflink.table.expression import Expression
from pyflink.table.types import MapType, RowType
from pyflink.table.udf import AsyncScalarFunction, ScalarFunction, TableFunction
from pyflink.testing.test_case_utils import (
    PyFlinkDataFrameUTTestCase,
    PyFlinkStreamDataFrameTestCase,
)


def _return_dtype(declaration: Callable[..., Expression]) -> pf.DataType:
    return cast(Any, declaration).return_dtype


_UDF_TEST_ALIAS = int


def _module_alias_method(self, value: int) -> "_UDF_TEST_ALIAS":
    return value


def _module_alias_function(value: int) -> "_UDF_TEST_ALIAS":
    return value


def _call_module_alias_function(value):
    return _module_alias_function(value)


class DataFrameUDFDeclarationTests(unittest.TestCase):
    def test_arrow_annotation_inference_and_overrides(self):
        def arrow_identity(values: pa.Array) -> pa.ChunkedArray:
            return pa.chunked_array([values])

        def integer_array(values: pa.Int64Array) -> pa.Int64Array:
            return values

        def list_array(values: pa.ListArray):
            return values.flatten()

        def struct_array(values: "pa.StructArray"):
            return values.field("value")

        def concrete_return(values) -> pa.Int64Array:
            return values

        def mixed(values: pd.Series) -> pa.Int64Array:
            return pa.array(values)

        def captured(context: pd.Series, values: pa.Array) -> pa.Array:
            return values

        class ArrowCallable:
            def __call__(self, values: "pa.Array") -> "pa.Array":
                return values

        class ArrowScalar(ScalarFunction):
            def eval(self, values: pa.Array) -> pa.Array:
                return values

        for func in (arrow_identity, integer_array, list_array, struct_array, concrete_return,
                     ArrowCallable, ArrowCallable(), ArrowScalar, ArrowScalar(),
                     functools.partial(captured, pd.Series([1]))):
            with self.subTest(func=func):
                declaration = pf.udf(func, return_dtype=pf.DataType.int64())
                self.assertEqual(cast(Any, declaration)._func_type, "arrow")

        for mode in ("general", "pandas", "arrow"):
            with self.subTest(mode=mode):
                declaration = pf.udf(mixed, return_dtype=pf.DataType.int64(), func_type=mode)
                self.assertEqual(cast(Any, declaration)._func_type, mode)

        with self.assertRaisesRegex(ValueError, "pandas.*Arrow.*func_type"):
            pf.udf(mixed, return_dtype=pf.DataType.int64())
        with self.assertRaisesRegex(TypeError, "return_dtype is required for arrow"):
            pf.udf(arrow_identity)

    def test_explicit_arrow_declarations(self):
        from pyflink.table.udf import udf as table_udf

        def identity(values):
            return values

        declaration = pf.udf(identity, return_dtype=pf.DataType.string(), func_type="arrow")
        self.assertEqual(_return_dtype(declaration), pf.DataType.string())
        table_udf(identity, result_type=TableDataTypes.STRING(), func_type="arrow")

        with self.assertRaisesRegex(TypeError, "return_dtype is required for arrow"):
            pf.udf(identity, func_type="arrow")

        async def async_identity(values):
            return values

        class AsyncCallable:
            async def __call__(self, values):
                return values

        for declare in (
            lambda: pf.udf(async_identity, return_dtype=pf.DataType.string(), func_type="arrow"),
            lambda: table_udf(async_identity, result_type=TableDataTypes.STRING(),
                              func_type="arrow"),
            lambda: table_udf(AsyncCallable(), result_type=TableDataTypes.STRING(),
                              func_type="arrow"),
            lambda: table_udf(functools.partial(AsyncCallable()),
                              result_type=TableDataTypes.STRING(), func_type="arrow"),
        ):
            with self.subTest(declare=declare):
                with self.assertRaisesRegex(ValueError, "Async.*arrow"):
                    declare()

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

        def optional_return(value: int) -> Optional[int]:
            return value if value % 2 == 0 else None

        def identity(value):
            return value

        def describe(value: int) -> Result:
            return {
                "id": value,
                "details": {"label": str(value), "scores": [value]},
            }

        def concrete_return_with_unresolved_input(value):
            return value

        concrete_return_with_unresolved_input.__annotations__ = {
            "value": "UnavailableInput",
            "return": int,
        }

        def postponed_return_with_unresolved_input(value):
            return value

        postponed_return_with_unresolved_input.__annotations__ = {
            "value": "UnavailableInput",
            "return": "int",
        }

        decorated: Callable[..., Expression] = pf.udf(add_one)

        self.assertFalse(hasattr(pf, "DataFrameUDFWrapper"))
        udf_module = importlib.import_module("pyflink.dataframe.udf")
        self.assertFalse(hasattr(udf_module, "DataFrameUDFWrapper"))
        self.assertEqual(_return_dtype(decorated), pf.DataType.int64().not_null())
        self.assertEqual(decorated.__name__, "add_one")
        self.assertEqual(decorated.__qualname__, add_one.__qualname__)
        self.assertEqual(decorated.__module__, add_one.__module__)
        self.assertEqual(decorated.__doc__, "Add one to a value.")
        self.assertNotIn("__wrapped__", vars(decorated))
        self.assertNotIn("__signature__", vars(decorated))
        self.assertNotIn("__annotations__", vars(decorated))
        wrapper_signature = inspect.signature(decorated)
        parameters = tuple(wrapper_signature.parameters.values())
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].name, "args")
        self.assertIs(parameters[0].kind, inspect.Parameter.VAR_POSITIONAL)
        self.assertIs(parameters[0].annotation, Any)
        self.assertIs(wrapper_signature.return_annotation, Expression)

        configured: Callable[..., Expression] = pf.udf(
            return_dtype=pf.DataType.string()
        )(
            lambda value: str(value)
        )
        direct = pf.udf(functools.partial(add_one), name="partial_add_one")

        self.assertEqual(_return_dtype(configured), pf.DataType.string())
        self.assertEqual(_return_dtype(direct), pf.DataType.int64().not_null())
        self.assertEqual(direct.__name__, "partial_add_one")
        with mock.patch.object(
            pf.DataType,
            "_from_sql",
            return_value=pf.DataType.int64(),
        ) as from_sql:
            sql_typed = pf.udf(identity, return_dtype="BIGINT")
        self.assertEqual(_return_dtype(sql_typed), pf.DataType.int64())
        from_sql.assert_called_once_with("BIGINT")

        expected_result_dtype = pf.DataType.struct(
            {
                "id": pf.DataType.int64().not_null(),
                "details": pf.DataType.struct(
                    {
                        "label": pf.DataType.string().not_null(),
                        "scores": pf.DataType.list(
                            pf.DataType.int64().not_null()
                        ).not_null(),
                    }
                ).not_null(),
            }
        ).not_null()
        declarations = [
            (
                "Python type",
                lambda: pf.udf(identity, return_dtype=int),
                pf.DataType.int64().not_null(),
            ),
            (
                "Optional return annotation widens to nullable",
                lambda: pf.udf(optional_return),
                pf.DataType.int64(),
            ),
            (
                "nested TypedDict annotation",
                lambda: pf.udf(describe),
                expected_result_dtype,
            ),
            (
                "explicit nested TypedDict",
                lambda: pf.udf(identity, return_dtype=Result),
                expected_result_dtype,
            ),
            (
                "concrete return with unresolved input",
                lambda: pf.udf(concrete_return_with_unresolved_input),
                pf.DataType.int64().not_null(),
            ),
            (
                "postponed return with unresolved input",
                lambda: pf.udf(postponed_return_with_unresolved_input),
                pf.DataType.int64().not_null(),
            ),
        ]
        for case_name, declare, expected in declarations:
            with self.subTest(case=case_name):
                self.assertEqual(_return_dtype(declare()), expected)

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
                self.assertEqual(
                    _return_dtype(decorated), pf.DataType.int64().not_null()
                )

        self.assertEqual(plain_constructor_calls, [])
        self.assertEqual(scalar_constructor_calls, [])
        self.assertEqual(pf.udf(named_callable).__name__, "configured_add")

        decorated_class = pf.udf(Double)
        self.assertEqual(decorated_class.__qualname__, Double.__qualname__)

    def test_callable_class_resolves_class_local_return_annotation(self):
        class Describe:
            class Output(TypedDict):
                value: int

            def __call__(self, value: int) -> "Output":
                return {"value": value}

        expected = pf.DataType.struct({"value": pf.DataType.int64().not_null()}).not_null()
        for source in (Describe, Describe()):
            with self.subTest(source=source):
                self.assertEqual(_return_dtype(pf.udf(source)), expected)

    def test_callable_annotations_use_lexical_defining_class(self):
        class BoundMethodOwner:
            class Output(TypedDict):
                value: int

            def describe(self, value: int) -> "Output":
                return {"value": value}

        class InheritedMethodOwner:
            class Output(TypedDict):
                value: int

            def __call__(self, value: int) -> "Output":
                return {"value": value}

        class InheritedCallable(InheritedMethodOwner):
            pass

        class SelfQualified:
            class Output(TypedDict):
                value: int

            def __call__(self, value: int) -> "SelfQualified.Output":
                return {"value": value}

        expected = pf.DataType.struct({"value": pf.DataType.int64().not_null()}).not_null()
        bound_method = BoundMethodOwner().describe
        for source in (
            bound_method,
            functools.partial(bound_method),
            InheritedCallable,
            SelfQualified,
        ):
            with self.subTest(source=source):
                self.assertEqual(_return_dtype(pf.udf(source)), expected)

        class PandasCallable:
            Batch = pd.Series

            def __call__(self, values: "Batch") -> int:
                return len(values)

        pandas_declaration = pf.udf(PandasCallable, return_dtype=int)
        self.assertEqual(pandas_declaration._func_type, "pandas")

        class ReceivingCallable:
            _UDF_TEST_ALIAS = str
            __call__ = _module_alias_method

        self.assertEqual(
            _return_dtype(pf.udf(ReceivingCallable)),
            pf.DataType.int64().not_null(),
        )

        class OverriddenScalarFunction(ScalarFunction):
            _UDF_TEST_ALIAS = str

            def eval(self, *values: int) -> str:
                value, = values
                return str(value)

        overridden = OverriddenScalarFunction()
        overridden.eval = types.MethodType(_module_alias_method, overridden)
        self.assertEqual(
            _return_dtype(pf.udf(overridden)), pf.DataType.int64().not_null()
        )

    def test_wrapped_callable_annotations_and_partial_validation(self):
        def add(value: int, amount: int = 1) -> int:
            return value + amount

        def pandas_identity(values: pd.Series) -> pd.Series:
            return values

        class WrappedCallableClass:
            @functools.wraps(pandas_identity)
            def __call__(self, *args, **kwargs):
                return pandas_identity(*args, **kwargs)

        class WrappedClassMethodCallableClass:
            @classmethod
            @functools.wraps(pandas_identity)
            def __call__(cls, *args, **kwargs):
                return pandas_identity(*args, **kwargs)

        class WrappedScalarFunction(ScalarFunction):
            @functools.wraps(pandas_identity)
            def eval(self, *args, **kwargs):
                return pandas_identity(*args, **kwargs)

        with self.assertRaisesRegex(
            TypeError, "Invalid functools.partial UDF 'add'.*unexpected keyword"
        ):
            pf.udf(functools.partial(add, missing=1))

        uninspectable = pf.udf(operator.itemgetter(0), return_dtype=int)
        self.assertEqual(
            _return_dtype(uninspectable), pf.DataType.int64().not_null()
        )

        wrapped_callable_instance = WrappedCallableClass()
        for source in (
            WrappedCallableClass,
            wrapped_callable_instance,
            wrapped_callable_instance.__call__,
            WrappedClassMethodCallableClass,
            WrappedClassMethodCallableClass(),
            WrappedScalarFunction,
            WrappedScalarFunction(),
        ):
            with self.subTest(wrapped_source=source):
                declaration = pf.udf(
                    source, return_dtype=pf.DataType.int64()
                )
                self.assertEqual(declaration._func_type, "pandas")

        cross_namespace_wrapper = types.FunctionType(
            _call_module_alias_function.__code__,
            {
                "_module_alias_function": _module_alias_function,
                "_UDF_TEST_ALIAS": str,
            },
        )
        functools.update_wrapper(cross_namespace_wrapper, _module_alias_function)
        self.assertEqual(
            _return_dtype(pf.udf(cross_namespace_wrapper)),
            pf.DataType.int64().not_null(),
        )

    def test_func_type_resolution_and_async_detection(self):
        def method_decorator(method):
            @functools.wraps(method)
            def wrapper(*args, **kwargs):
                return method(*args, **kwargs)

            return wrapper

        def pandas_add_one(values: pd.Series) -> pd.Series:
            return values + 1

        def with_pandas_context(context: pd.Series, value: int) -> int:
            return value

        def pandas_forward_reference(values):
            return values

        pandas_forward_reference.__annotations__["values"] = "pandas.Series"

        def pandas_with_unresolved_annotation(
            values: pd.Series, context
        ) -> pd.Series:
            return values

        pandas_with_unresolved_annotation.__annotations__[
            "context"
        ] = "UnavailableContext"

        def pandas_after_missing_attribute(
            context: Any, values: pd.Series
        ) -> int:
            return len(values)

        pandas_after_missing_attribute.__annotations__[
            "context"
        ] = "pd.Missing"

        def only_missing_attribute(context: Any) -> int:
            return 1

        only_missing_attribute.__annotations__["context"] = "pd.Missing"

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

        class WrappedPandasContext:
            @method_decorator
            def __call__(self, context: pd.Series, value: int) -> int:
                return value

        wrapped_pandas_context = WrappedPandasContext()

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
                "bound wrapped pandas annotation is ignored",
                lambda: pf.udf(
                    functools.partial(
                        wrapped_pandas_context.__call__, pd.Series([1])
                    ),
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
                "unresolved annotation does not hide pandas annotation",
                lambda: pf.udf(
                    pandas_with_unresolved_annotation,
                    return_dtype=pf.DataType.int64(),
                ),
                "pandas",
                False,
            ),
            (
                "missing annotation attribute does not hide pandas annotation",
                lambda: pf.udf(
                    pandas_after_missing_attribute,
                    return_dtype=pf.DataType.int64(),
                ),
                "pandas",
                False,
            ),
            (
                "missing annotation attribute falls back to general",
                lambda: pf.udf(
                    only_missing_attribute,
                    return_dtype=pf.DataType.int64(),
                ),
                "general",
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
                "inferred arrow",
                lambda: pf.udf(arrow_add_one, return_dtype=pf.DataType.int64()),
                "arrow",
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
                self.assertEqual(
                    wrapped._runtime_source.is_async, expected_async
                )

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

    def test_sync_wrapper_around_async_target_is_rejected(self):
        async def async_add_one(value: int) -> int:
            return value + 1

        @functools.wraps(async_add_one)
        def sync_wrapper(*args, **kwargs):
            return async_add_one(*args, **kwargs)

        with self.assertRaisesRegex(TypeError, "async def"):
            pf.udf(sync_wrapper)

    def test_sync_async_scalar_eval_is_rejected(self):
        class SyncAsyncScalarFunction(AsyncScalarFunction):
            def eval(self, *values: int) -> int:
                value, = values
                return value + 1

        for source in (SyncAsyncScalarFunction, SyncAsyncScalarFunction()):
            with self.subTest(source=source):
                with self.assertRaisesRegex(
                    TypeError,
                    "AsyncScalarFunction 'SyncAsyncScalarFunction'.*async def",
                ):
                    pf.udf(source, return_dtype=pf.DataType.int64())

    def test_unrelated_methodtype_owner_requires_explicit_metadata(self):
        class MethodOwner:
            Batch = pd.Series

            class Output(TypedDict):
                value: int

            def eval(self, values: "Batch") -> "Output":
                return {"value": len(values)}

        class ReplacedScalarFunction(ScalarFunction):
            def eval(self, *values: int) -> int:
                value, = values
                return value

        replaced = ReplacedScalarFunction()
        replaced.eval = types.MethodType(MethodOwner.eval, replaced)

        with self.assertRaisesRegex(
            TypeError,
            r"Cannot infer return_dtype for 'ReplacedScalarFunction' from its "
            r"return annotation\.\nSpecify return_dtype explicitly\.",
        ):
            pf.udf(replaced)

        return_dtype = pf.DataType.struct({"value": pf.DataType.int64()})
        inferred_mode = pf.udf(replaced, return_dtype=return_dtype)
        self.assertEqual(inferred_mode._func_type, "general")
        explicit_mode = pf.udf(
            replaced,
            return_dtype=return_dtype,
            func_type="pandas",
        )
        self.assertEqual(explicit_mode._func_type, "pandas")

    def test_invalid_class_invocation_descriptors_fail_eagerly(self):
        class CallableBase:
            def __call__(self, value: int) -> int:
                return value

        class HiddenCallable(CallableBase):
            __call__ = None

        class ScalarBase(ScalarFunction):
            def eval(self, *values: int) -> int:
                value, = values
                return value

        class HiddenScalarFunction(ScalarBase):
            eval = None

        class InvalidStaticCallable:
            __call__ = staticmethod(None)

        class InvalidClassMethodCallable:
            __call__ = classmethod(None)

        invalid_classes = (
            (HiddenCallable, "Callable class", "__call__"),
            (HiddenScalarFunction, "Scalar UDF class", "eval"),
            (InvalidStaticCallable, "Callable class", "__call__"),
            (InvalidClassMethodCallable, "Callable class", "__call__"),
        )
        for source, source_kind, method_name in invalid_classes:
            with self.subTest(source=source):
                message = (
                    rf"{source_kind} '{source.__name__}' has an unsupported "
                    rf"{method_name} definition\.\nDefine {method_name} as an "
                    r"instance, class, or static method\."
                )
                with self.assertRaisesRegex(TypeError, message):
                    pf.udf(source, return_dtype=int)

    def test_descriptor_based_callable_classes_require_instances(self):
        class PartialMethodCallable:
            def invoke(self, offset: int, value: int) -> int:
                return offset + value

            __call__ = functools.partialmethod(invoke, 1)

        class PartialDescriptorCallable:
            __call__ = functools.partial(lambda: 1)

        for source in (PartialMethodCallable, PartialDescriptorCallable):
            with self.subTest(class_source=source):
                with self.assertRaisesRegex(
                    TypeError,
                    rf"Callable class '{source.__name__}' has an unsupported "
                    r"__call__ definition\.\nDefine __call__ as an instance, "
                    r"class, or static method\.",
                ):
                    pf.udf(source, return_dtype=int)

            with self.subTest(instance_source=source):
                declaration = pf.udf(
                    source(), return_dtype=int, func_type="general"
                )
                self.assertEqual(
                    _return_dtype(declaration), pf.DataType.int64().not_null()
                )

    def test_unresolved_typed_dict_fields_have_actionable_errors(self):
        class Describe:
            OuterAlias = int

            class Output(TypedDict):
                value: Any

            Output.__annotations__["value"] = "OuterAlias"

            def __call__(self, value: int) -> "Output":
                return {"value": value}

        with self.assertRaisesRegex(
            TypeError,
            r"Cannot infer return_dtype for 'Describe' from its return annotation\.\n"
            r"Specify return_dtype explicitly\.",
        ):
            pf.udf(Describe)

        with self.assertRaisesRegex(TypeError, "DataType or SQL"):
            pf.udf(lambda value: value, return_dtype=Describe.Output)

        class InvalidOutput(TypedDict):
            value: Any

        InvalidOutput.__annotations__["value"] = "list["

        def invalid_output(value: int) -> InvalidOutput:
            return {"value": value}

        with self.assertRaisesRegex(
            TypeError,
            r"Cannot infer return_dtype for 'invalid_output' from its return "
            r"annotation\.\nSpecify return_dtype explicitly\.",
        ):
            pf.udf(invalid_output)

        with self.assertRaisesRegex(TypeError, "DataType or SQL"):
            pf.udf(lambda value: value, return_dtype=InvalidOutput)

    def test_unresolvable_composite_return_dtype_throws_error(self):
        @dataclass
        class EmptyDataclass:
            pass

        class EmptyNamedTuple(NamedTuple):
            pass

        for return_dtype in (EmptyDataclass, EmptyNamedTuple):
            with self.subTest(return_dtype=return_dtype):
                with self.assertRaisesRegex(
                    TypeError,
                    f"Cannot resolve return_dtype from '{return_dtype.__name__}'",
                ):
                    pf.udf(lambda value: value, return_dtype=return_dtype)

    def test_malformed_forward_references_have_clean_inference_behavior(self):
        def pandas_after_malformed(
            context: Any, values: pd.Series
        ) -> int:
            return len(values)

        pandas_after_malformed.__annotations__["context"] = "list["
        self.assertEqual(
            pf.udf(
                pandas_after_malformed,
                return_dtype=pf.DataType.int64(),
            )._func_type,
            "pandas",
        )

        def only_malformed(context: Any) -> int:
            return 1

        only_malformed.__annotations__["context"] = "list["
        self.assertEqual(
            pf.udf(
                only_malformed, return_dtype=pf.DataType.int64()
            )._func_type,
            "general",
        )

        def malformed_return(value: int) -> int:
            return value

        malformed_return.__annotations__["return"] = "list["
        with self.assertRaisesRegex(
            TypeError,
            r"Cannot infer return_dtype for 'malformed_return' from its return "
            r"annotation\.\nSpecify return_dtype explicitly\.",
        ):
            pf.udf(malformed_return)

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
            ("matching class metadata", lambda: pf.udf(NonDeterministic, deterministic=False),
             False),
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
        named = pf.udf(instance, deterministic=False, name="identity")
        self.assertEqual(named.__name__, "identity")
        self.assertEqual(named._table_udf_wrapper._name, "identity")

    def test_scalar_function_classes_defer_custom_determinism_validation(self):
        for base in (ScalarFunction, AsyncScalarFunction):
            for deterministic in (True, False):
                constructed = []

                class CustomDeterminism(base):
                    def __init__(self):
                        constructed.append(True)

                    def is_deterministic(self):
                        return deterministic

                    if base is AsyncScalarFunction:
                        async def eval(self, value):
                            return value
                    else:
                        def eval(self, value):
                            return value

                class InheritedDeterminism(CustomDeterminism):
                    pass

                for source in (CustomDeterminism, InheritedDeterminism):
                    with self.subTest(base=base, deterministic=deterministic, source=source):
                        pf.udf(source, return_dtype=int, deterministic=deterministic)
                self.assertEqual(constructed, [])
                declaration = pf.udf(
                    CustomDeterminism(), return_dtype=int, deterministic=deterministic)
                self.assertEqual(declaration._deterministic, deterministic)

    def test_general_structured_results_are_normalized_recursively(self):
        from pyflink.dataframe.udf import _create_result_normalizer

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

        class PropertyDetails:
            def __init__(self, label, scores):
                self._label = label
                self.scores = scores

            @property
            def label(self):
                return self._label.upper()

        class MissingLabelDetails:
            def __init__(self, scores):
                self.scores = scores

        class FailingPropertyDetails:
            scores = [17]

            @property
            def label(self):
                raise AttributeError("label lookup failed")

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
        result_normalizer = _create_result_normalizer(table_type)
        self.assertIsNotNone(result_normalizer)

        named_row = Row(
            id=4,
            details=Row(label="named", scores=[5]),
            attributes={"count": 6},
        )
        named_row.set_row_kind(RowKind.DELETE)
        expected_named_row = Row(
            id=4,
            details=Row(label="named", scores=[5]),
            attributes={"count": 6},
        )
        expected_named_row.set_row_kind(RowKind.DELETE)

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
                named_row,
                expected_named_row,
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
            (
                "property attribute",
                Result(
                    id=14,
                    details=PropertyDetails(label="property", scores=[15]),
                    attributes={"count": 16},
                ),
                Row(
                    id=14,
                    details=Row(label="PROPERTY", scores=[15]),
                    attributes={"count": 16},
                ),
            ),
            (
                "missing object attribute",
                Result(
                    id=18,
                    details=MissingLabelDetails(scores=[19]),
                    attributes={"count": 20},
                ),
                Row(
                    id=18,
                    details=Row(label=None, scores=[19]),
                    attributes={"count": 20},
                ),
            ),
        ]
        for case_name, value, expected in cases:
            with self.subTest(case=case_name):
                self.assertEqual(
                    result_normalizer(value), expected
                )
        with self.assertRaisesRegex(ValueError, "Expected 3 value"):
            result_normalizer((1, 2))
        with self.assertRaisesRegex(TypeError, "Expected a Mapping"):
            result_normalizer(object())
        with self.assertRaisesRegex(AttributeError, "label lookup failed"):
            result_normalizer(
                {
                    "id": 21,
                    "details": FailingPropertyDetails(),
                    "attributes": {},
                },
            )

    def test_invalid_declarations_fail_eagerly(self):
        def missing_return(value):
            return value

        def unresolved_return(value):
            return value

        unresolved_return.__annotations__ = {
            "return": "UnavailableReturn"
        }

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

        class MissingCallableReturn:
            def __call__(self, value):
                return value

        class MissingScalarReturn(ScalarFunction):
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
                "add a return annotation",
            ),
            (
                "unresolved return",
                lambda: pf.udf(unresolved_return),
                TypeError,
                r"from its return annotation\.\nSpecify return_dtype explicitly\.",
            ),
            (
                "callable class missing return",
                lambda: pf.udf(MissingCallableReturn),
                TypeError,
                "add a return annotation",
            ),
            (
                "scalar function class missing return",
                lambda: pf.udf(MissingScalarReturn),
                TypeError,
                "add a return annotation",
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
                "unsupported func type",
                lambda: pf.udf(
                    missing_return,
                    return_dtype=pf.DataType.int64(),
                    func_type="unsupported",
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
    def pandas_map_output(self, adapter, dtype, *inputs):
        from pyflink.table.utils import create_pandas_table_row_schema, pandas_table_row_to_arrow
        import pytz

        row_type = dtype._to_table_data_type()
        schema = create_pandas_table_row_schema(TableDataTypes.ROW([
            TableDataTypes.FIELD("result", row_type)]))
        return pandas_table_row_to_arrow(
            schema, pytz.UTC, row_type, adapter.eval(*inputs)).column(0).to_pylist()

    def test_pandas_map_batches_returns_flattened_dataframe(self):
        from pyflink.dataframe.udf import _resolve_map_udf

        dtype = pf.DataType.struct({
            "v": pf.DataType.int64(),
            "payload": pf.DataType.struct({"v": pf.DataType.int64(),
                                           "tail": pf.DataType.int64()}),
            "other": pf.DataType.struct({"v": pf.DataType.string()}),
            "tail": pf.DataType.int64(),
        })
        input_type = TableDataTypes.ROW([
            TableDataTypes.FIELD("tag", TableDataTypes.STRING()),
            TableDataTypes.FIELD("id", TableDataTypes.BIGINT())])

        def transform(batch):
            self.assertEqual(batch["tag"].tolist(), ["x"] * len(batch["id"]))
            return {"tail": batch["id"] + 10,
                    "other": pd.DataFrame({"v": ["a", None]}, index=batch["id"].index),
                    "payload": pd.DataFrame({
                        "v": pd.Series([9007199254740993, None],
                                       index=batch["id"].index, dtype=object),
                        "tail": batch["id"] + 20}),
                    "v": batch["id"].sort_index(ascending=False)}

        adapter = _resolve_map_udf(transform, dtype, "pandas", input_type)._func
        adapter.open(object())
        try:
            result = adapter.eval("x", pd.Series([1, 2]))
            self.assertIsInstance(result, pd.DataFrame)
            pd.testing.assert_frame_equal(result, pd.concat([
                pd.Series([1, 2], name="v"),
                pd.Series([9007199254740993, None], name="v", dtype=object),
                pd.Series([21, 22], name="tail"),
                pd.Series(["a", None], name="v"),
                pd.Series([11, 12], name="tail"),
            ], axis=1))
            self.assertEqual(self.pandas_map_output(adapter, dtype, "x", pd.Series([1, 2])), [
                {"v": 1, "payload": {"v": 9007199254740993, "tail": 21},
                 "other": {"v": "a"}, "tail": 11},
                {"v": 2, "payload": {"v": None, "tail": 22},
                 "other": {"v": None}, "tail": 12},
            ])
        finally:
            adapter.close()

    def test_map_bindings_leave_other_uses_of_a_declaration_unchanged(self):
        from pyflink.dataframe.udf import _resolve_map_udf

        @pf.udf(return_dtype=pf.DataType.struct({"total": pf.DataType.int64()}))
        def total(value):
            return {"total": sum(value.values()) if isinstance(value, dict) else value}

        expression = total._table_udf_wrapper._func
        maps = [_resolve_map_udf(total, None, "general", TableDataTypes.ROW([
            TableDataTypes.FIELD(name, TableDataTypes.BIGINT()) for name in names
        ]))._func for names in (["left", "right"], ["value"])]
        for adapter in [expression] + maps:
            adapter.open(object())
            self.addCleanup(adapter.close)
        self.assertEqual(maps[0].eval(1, 2), Row(total=3))
        self.assertEqual(maps[1].eval(4), Row(total=4))
        self.assertEqual(maps[0].eval(5, 6), Row(total=11))
        self.assertEqual(expression.eval(7), Row(total=7))

    def test_map_requires_a_dictionary_result(self):
        from pyflink.dataframe.udf import _resolve_map_udf

        dtype = pf.DataType.struct({"value": pf.DataType.int64()})
        for result in (None, (1,), 1):
            with self.subTest(result=result):
                adapter = _resolve_map_udf(
                    lambda row: result, dtype, "general", dtype._to_table_data_type())._func
                adapter.open(object())
                try:
                    with self.assertRaisesRegex(TypeError, "dictionary"):
                        adapter.eval(1)
                finally:
                    adapter.close()

    def test_pandas_map_batches_preserves_row_columns_for_transported_and_fused_inputs(self):
        from pyflink.dataframe.udf import _resolve_map_udf
        from pyflink.table.types import create_arrow_schema
        from pyflink.table.utils import arrow_to_pandas
        import pytz

        input_type = TableDataTypes.ROW([
            TableDataTypes.FIELD("id", TableDataTypes.BIGINT()),
            TableDataTypes.FIELD("payload", TableDataTypes.ROW([
                TableDataTypes.FIELD("left", TableDataTypes.BIGINT()),
                TableDataTypes.FIELD("right", TableDataTypes.BIGINT())])),
            TableDataTypes.FIELD("tail", TableDataTypes.BIGINT()),
        ])
        dtype = pf.DataType.struct({"score": pf.DataType.int64()})

        def transform(batch):
            self.assertEqual(list(batch), ["id", "payload", "tail"])
            payload = batch["payload"]
            self.assertIsInstance(payload, pd.DataFrame)
            self.assertEqual(list(payload), ["left", "right"])
            pd.testing.assert_frame_equal(payload, pd.DataFrame(
                {"left": [2, None, 4], "right": [3, None, None]},
                index=payload.index))
            return {"score": batch["tail"] + batch["id"]}

        adapter = _resolve_map_udf(transform, dtype, "pandas", input_type)._func
        adapter.open(object())
        try:
            grouped = pd.DataFrame({
                "id": [1, 2, 3], "tail": [10, 20, 30], "payload": [
                    {"left": 2, "right": 3}, None,
                    {"left": 4, "right": None},
                ],
            })[["id", "payload", "tail"]]
            schema = create_arrow_schema(input_type.field_names(), input_type.field_types())
            batch = pa.RecordBatch.from_pandas(grouped, schema=schema)
            transported = arrow_to_pandas(pytz.UTC, input_type.field_types(), [batch])
            self.assertEqual(self.pandas_map_output(adapter, dtype, *transported),
                             [{"score": 11}, {"score": 22}, {"score": 33}])
            for names in (["unrelated1", "unrelated2"], ["right", "left"]):
                fused_payload = transported[1].set_axis(names, axis=1)
                self.assertEqual(self.pandas_map_output(
                    adapter, dtype, transported[0], fused_payload, transported[2]),
                    [{"score": 11}, {"score": 22}, {"score": 33}])
                self.assertEqual(list(fused_payload), names)
            grouped.index = [2, 0, 1]
            with self.assertRaisesRegex(TypeError, "ROW.*pandas.DataFrame"):
                adapter.eval(*(grouped[name] for name in input_type.field_names()))
            fused = [column.set_axis(grouped.index, axis=0) for column in transported]
            self.assertEqual(self.pandas_map_output(adapter, dtype, *fused),
                             [{"score": 11}, {"score": 22}, {"score": 33}])
        finally:
            adapter.close()

    def test_pandas_map_batches_preserves_nested_collections_with_repeated_names(self):
        import cloudpickle
        from pyflink.dataframe.udf import _resolve_map_udf
        from pyflink.fn_execution import flink_fn_execution_pb2 as proto
        from pyflink.fn_execution.table.operations import ScalarFunctionOperation
        from pyflink.table.udf import DelegatingScalarFunction
        from pyflink.table.utils import arrow_to_pandas
        import pytz

        row_type = TableDataTypes.ROW([TableDataTypes.FIELD(
            "payload", TableDataTypes.MAP(TableDataTypes.STRING(), TableDataTypes.BIGINT()))])
        input_type = TableDataTypes.ROW([TableDataTypes.FIELD("payload", row_type)])
        dtype = pf.DataType.struct({"size": pf.DataType.int64()})

        def transform(batch):
            self.assertEqual(batch["payload"]["payload"].tolist(), [[("x", 1), ("y", 2)], []])
            return {"size": batch["payload"]["payload"].map(len)}

        batch = pa.record_batch([pa.array([
            {"payload": [("x", 1), ("y", 2)]}, {"payload": []},
        ], type=pa.struct([("payload", pa.map_(pa.string(), pa.int64()))]))], names=["payload"])
        columns = arrow_to_pandas(pytz.UTC, [row_type], [batch])
        adapter = _resolve_map_udf(transform, dtype, "pandas", input_type)._func
        adapter.open(object())
        try:
            self.assertEqual(self.pandas_map_output(adapter, dtype, columns[0]),
                             [{"size": 2}, {"size": 0}])
            nested_dtype = pf.DataType.struct({"payload": pf.DataType.struct({
                "payload": pf.DataType.map(pf.DataType.string(), pf.DataType.int64())})})
            producer = _resolve_map_udf(
                lambda batch: {"payload": batch["payload"]},
                nested_dtype, "pandas", input_type)._func
            producer_proto = proto.UserDefinedFunction(
                payload=cloudpickle.dumps(producer), is_pandas_udf=True,
                inputs=[proto.Input(inputOffset=0)])
            producer.open(object())
            try:
                self.assertEqual(self.pandas_map_output(producer, nested_dtype, columns[0]),
                                 [{"payload": {"payload": [("x", 1), ("y", 2)]}},
                                  {"payload": {"payload": []}}])
            finally:
                producer.close()
            for values, expected in ((columns, [2, 0]),
                                     ([column.head(0) for column in columns], [])):
                for shared in (False, True):
                    with self.subTest(empty_batch=not expected, shared=shared):
                        consumer = proto.UserDefinedFunction(
                            payload=cloudpickle.dumps(DelegatingScalarFunction(
                                lambda frame: frame["payload"].map(len))),
                            is_pandas_udf=True, takes_row_as_input=True,
                            inputs=[proto.Input(refIndex=0)] if shared
                            else [proto.Input(udf=producer_proto)])
                        operation = ScalarFunctionOperation(proto.UserDefinedFunctions(
                            udfs=[producer_proto, consumer] if shared else [consumer],
                            output_indices=[1] if shared else []))
                        operation.open()
                        try:
                            self.assertEqual(operation.process_element(values)[0].tolist(),
                                             expected)
                        finally:
                            operation.close()
        finally:
            adapter.close()

    def test_pandas_map_batches_preserves_map_inputs(self):
        from pyflink.dataframe.udf import _resolve_map_udf
        from pyflink.table.types import create_arrow_schema
        from pyflink.table.utils import arrow_to_pandas, pandas_to_arrow
        import pytz

        lookup = TableDataTypes.MAP(TableDataTypes.STRING(), TableDataTypes.BIGINT())
        payload = TableDataTypes.ROW([
            TableDataTypes.FIELD("lookup", lookup),
        ])
        input_type = TableDataTypes.ROW([
            TableDataTypes.FIELD("lookup", lookup), TableDataTypes.FIELD("payload", payload)])
        dtype = pf.DataType.struct({"total": pf.DataType.int64()})

        def transform(batch):
            self.assertIs(batch["lookup"], inputs[0])
            self.assertIs(batch["payload"], inputs[1])
            return {"total": batch["lookup"].map(
                lambda value: 0 if value is None else sum(value.values())
                if isinstance(value, dict) else sum(item for _, item in value))}

        columns = [pd.Series([{"top": 1}, {}, None], dtype=object), pd.DataFrame({
            "lookup": [{"field": 2}, [("same", 3), ("same", 4)], None],
        })]
        schema = create_arrow_schema(input_type.field_names(), input_type.field_types())
        transported = arrow_to_pandas(pytz.UTC, input_type.field_types(), [pandas_to_arrow(
            schema, pytz.UTC, input_type.field_types(), columns)])
        adapter = _resolve_map_udf(transform, dtype, "pandas", input_type)._func
        adapter.open(object())
        try:
            for inputs in (columns, transported):
                self.assertEqual(self.pandas_map_output(adapter, dtype, *inputs),
                                 [{"total": 1}, {"total": 0}, {"total": 0}])
        finally:
            adapter.close()

    def test_arrow_map_batches_preserves_nested_and_chunked_columns(self):
        from pyflink.dataframe.udf import _resolve_map_udf
        from pyflink.fn_execution.utils.operation_utils import check_arrow_udf_result

        dtype = pf.DataType.struct({
            "id": pf.DataType.int64(),
            "payload": pf.DataType.struct({"x": pf.DataType.int64()}),
        })
        batch = pa.StructArray.from_arrays([
            pa.array([1, 2]), pa.array([{"x": None}, None], type=pa.struct([("x", pa.int64())])),
            pa.array([{"inner": {"v": 1}}, {"inner": {"v": 2}}],
                     type=pa.struct([("inner", pa.struct([("v", pa.int64())]))])),
            pa.array([[{"v": 1}], []], type=pa.list_(pa.struct([("v", pa.int64())])))
        ], names=["id", "payload", "deep", "items"])

        def transform(columns):
            self.assertEqual(columns["tag"].to_pylist(), ["x"] * len(columns["id"]))
            self.assertIsInstance(columns["payload"], pa.StructArray)
            self.assertEqual(columns["deep"].field("inner").field("v").to_pylist(),
                             [1, 2][:len(columns["id"])])
            self.assertEqual(columns["items"].to_pylist(),
                             [[{"v": 1}], []][:len(columns["id"])])
            return {"payload": columns["payload"], "ignored": pa.array([0, 0]),
                    "id": pa.chunked_array([columns["id"].slice(0, 1),
                                            columns["id"].slice(1)])}

        input_type = TableDataTypes.ROW([
            TableDataTypes.FIELD("tag", TableDataTypes.STRING())
        ] + dtype._to_table_data_type().fields + [
            TableDataTypes.FIELD("deep", TableDataTypes.ROW([
                TableDataTypes.FIELD("inner", TableDataTypes.ROW([
                    TableDataTypes.FIELD("v", TableDataTypes.BIGINT())]))])),
            TableDataTypes.FIELD("items", TableDataTypes.ARRAY(TableDataTypes.ROW([
                TableDataTypes.FIELD("v", TableDataTypes.BIGINT())]))),
        ])
        adapter = _resolve_map_udf(transform, dtype, "arrow", input_type)._func
        adapter.open(object())
        try:
            result = check_arrow_udf_result(adapter.eval, "x", *batch.flatten())
            self.assertEqual(result.to_pylist(),
                             [{"id": 1, "payload": {"x": None}}, {"id": 2, "payload": None}])
            self.assertEqual(adapter.eval("x", *batch.slice(0, 0).flatten()).to_pylist(), [])
        finally:
            adapter.close()

    def test_pandas_map_batches_aligns_indexes_and_checks_cardinality(self):
        from pyflink.dataframe.udf import _resolve_map_udf

        dtype = pf.DataType.struct({"score": pf.DataType.int64(), "id": pf.DataType.int64()})

        def align(batch):
            self.assertIsInstance(batch, dict)
            self.assertIsInstance(batch["id"], pd.Series)
            return {"score": pd.Series([100, 200], index=batch["id"].index[::-1]).rename_axis(None),
                    "id": batch["id"],
                    "ignored": pd.Series([0, 0])}

        def wrong_length(batch):
            return {"id": batch["id"], "score": pd.Series([100, 200], index=[1, 2])}

        def too_short(batch):
            return {"id": batch["id"].head(1), "score": batch["id"].head(1)}

        def same_order(batch):
            return {"score": pd.Series([200, 100], index=batch["id"].index).rename_axis(None),
                    "id": batch["id"]}

        def partial(batch):
            return {"score": pd.Series([9007199254740993], index=[0]), "id": batch["id"]}

        input_type = TableDataTypes.ROW([TableDataTypes.FIELD("id", TableDataTypes.BIGINT())])
        for source, index in ((align, pd.RangeIndex(2)), (align, pd.Index([2, 0], name="rows")),
                              (align, pd.to_datetime(["2024-01-03", "2024-01-01"])),
                              (same_order, pd.Index([2, 0], name="rows")),
                              (partial, pd.RangeIndex(2)),
                              (wrong_length, pd.RangeIndex(2)), (too_short, pd.RangeIndex(2)),
                              (functools.partial(too_short), pd.RangeIndex(2))):
            adapter = _resolve_map_udf(source, dtype, "pandas", input_type)._func
            adapter.open(object())
            try:
                batch = pd.DataFrame({"id": [10, 20]}, index=index)
                if source in (wrong_length, too_short, partial) \
                        or isinstance(source, functools.partial):
                    with self.assertRaisesRegex(ValueError, "length"):
                        adapter.eval(batch["id"])
                else:
                    self.assertEqual(self.pandas_map_output(adapter, dtype, batch["id"]),
                                     [{"score": 200, "id": 10}, {"score": 100, "id": 20}])
            finally:
                adapter.close()

    def test_pandas_map_batches_requires_row_fields_including_collections(self):
        from pyflink.dataframe.udf import _resolve_map_udf
        from pyflink.table.utils import create_pandas_table_row_schema, pandas_table_row_to_arrow
        import pytz

        details = pf.DataType.struct({"name": pf.DataType.string()})
        dtype = pf.DataType.struct({
            "details": details,
            "items": pf.DataType.list(
                pf.DataType.map(pf.DataType.string(), details)),
            "plain_items": pf.DataType.list(
                pf.DataType.map(pf.DataType.string(), pf.DataType.int64())),
            "lookup": pf.DataType.map(pf.DataType.string(), details),
        })
        input_type = TableDataTypes.ROW([TableDataTypes.FIELD("id", TableDataTypes.BIGINT())])
        valid = {"details": pd.DataFrame({"name": [None]}),
                 "items": [None, {"item key": {"name": "a"}}],
                 "plain_items": [None, {"plain key": 1}, {}],
                 "lookup": {"any key": {"name": None}}, "extra": 2}
        cases = [(valid, None), ({"items": [], "lookup": {}}, KeyError),
                 (dict(valid, details=pd.DataFrame(index=[0])), ValueError),
                 (dict(valid, details=pd.DataFrame({"name": [None], "extra": [1]})), ValueError),
                 (dict(valid, items=[{"item key": {}}]), KeyError),
                 (dict(valid, lookup={"any key": {}}), KeyError),
                 (dict(valid, lookup={"any key": types.SimpleNamespace()}), AttributeError),
                 (dict(valid, lookup={"any key": ()}), ValueError),
                 (dict(valid, lookup={"any key": (None, "extra")}), ValueError)]
        for result, error in cases:
            with self.subTest(result=result):
                adapter = _resolve_map_udf(
                    lambda batch: {name: value if isinstance(value, pd.DataFrame)
                                   else pd.Series([value]) for name, value in result.items()},
                    dtype, "pandas", input_type)._func
                adapter.open(object())
                try:
                    if result is valid:
                        output = adapter.eval(pd.Series([1]))
                        self.assertEqual(output["items"].iloc[0],
                                         [None, [("item key", {"name": "a"})]])
                        self.assertEqual(output["plain_items"].iloc[0],
                                         [None, [("plain key", 1)], []])
                        self.assertEqual(valid["items"], [None, {"item key": {"name": "a"}}])
                        self.assertEqual(valid["plain_items"], [None, {"plain key": 1}, {}])
                        row_type = dtype._to_table_data_type()
                        schema = create_pandas_table_row_schema(TableDataTypes.ROW([
                            TableDataTypes.FIELD("result", row_type)]))
                        batch = pandas_table_row_to_arrow(schema, pytz.UTC, row_type, output)
                        self.assertEqual(batch.to_pylist(), [{"result": {
                            "details": {"name": None},
                            "items": [None, [("item key", {"name": "a"})]],
                            "plain_items": [None, [("plain key", 1)], []],
                            "lookup": [("any key", {"name": None})]}}])
                    else:
                        with self.assertRaises(error):
                            adapter.eval(pd.Series([1]))
                finally:
                    adapter.close()

    def test_pandas_map_batches_preserves_nullable_structured_columns(self):
        import tracemalloc
        from pyflink.dataframe.udf import _create_pandas_result_normalizer, _resolve_map_udf

        row_type = TableDataTypes.ROW([TableDataTypes.FIELD("v", TableDataTypes.BIGINT())])
        for data_type, payload in (
            (TableDataTypes.ARRAY(row_type), [{"v": i} for i in range(10000)]),
            (TableDataTypes.MAP(TableDataTypes.STRING(), row_type),
             [(str(i), {"v": i}) for i in range(10000)]),
        ):
            normalize = _create_pandas_result_normalizer(data_type)
            normalize(payload)
            tracemalloc.start()
            try:
                normalized = normalize(payload)
                peak = tracemalloc.get_traced_memory()[1]
            finally:
                tracemalloc.stop()
            self.assertIs(normalized, payload)
            self.assertLess(peak, 16384)
            original = Row(v=1)
            if isinstance(data_type, MapType):
                source = [("same", payload[0][1]), ("changed", original)]
                converted = normalize(source)
                self.assertIs(converted[0][1], source[0][1])
                self.assertEqual(converted[1], ("changed", {"v": 1}))
                self.assertIs(source[1][1], original)
            else:
                source = [payload[0], original]
                converted = normalize(source)
                self.assertIs(converted[0], source[0])
                self.assertEqual(converted[1], {"v": 1})
                self.assertIs(source[1], original)

        dtype = pf.DataType.struct({
            "details": pf.DataType.struct({"name": pf.DataType.string()}),
            "items": pf.DataType.list(pf.DataType.int64()),
            "lookup": pf.DataType.map(pf.DataType.string(), pf.DataType.int64()),
        })
        input_type = TableDataTypes.ROW([TableDataTypes.FIELD("id", TableDataTypes.BIGINT())])

        class RowFrame(pd.DataFrame):
            pass

        for frame_type in (pd.DataFrame, RowFrame):
            with self.subTest(frame_type=frame_type):
                adapter = _resolve_map_udf(
                    lambda batch: {"details": frame_type({"name": [None, float("nan"), pd.NA]}),
                                   "items": pd.Series([None, float("nan"), pd.NA]),
                                   "lookup": pd.Series([None, float("nan"), pd.NA])},
                    dtype, "pandas", input_type)._func
                adapter.open(object())
                try:
                    self.assertEqual(self.pandas_map_output(adapter, dtype, pd.Series([1, 2, 3])),
                                     [{"details": {"name": None}, "items": None, "lookup": None}]
                                     * 3)
                finally:
                    adapter.close()

    def test_pandas_map_batches_requires_matching_output_containers(self):
        from pyflink.dataframe.udf import _resolve_map_udf

        input_type = TableDataTypes.ROW([TableDataTypes.FIELD("id", TableDataTypes.BIGINT())])
        for field_type, columns, container in (
            (pf.DataType.struct({"name": pf.DataType.string()}),
             (pd.Series([{"name": "a"}]), None, [1]), "DataFrame"),
            (pf.DataType.int64(), (pd.DataFrame({"inner": [1]}), None, [1]), "Series"),
        ):
            dtype = pf.DataType.struct({"payload": field_type})
            for column in columns:
                with self.subTest(column=column, field_type=field_type):
                    adapter = _resolve_map_udf(
                        lambda batch: {"payload": column}, dtype, "pandas", input_type)._func
                    adapter.open(object())
                    try:
                        with self.assertRaisesRegex(TypeError, f"pandas.{container}"):
                            adapter.eval(pd.Series([1]))
                    finally:
                        adapter.close()

    def test_pandas_map_batches_rejects_one_shot_structured_collections(self):
        from pyflink.dataframe.udf import _resolve_map_udf

        details = pf.DataType.struct({"name": pf.DataType.string()})
        input_type = TableDataTypes.ROW([TableDataTypes.FIELD("id", TableDataTypes.BIGINT())])
        for field_type, value in (
            (pf.DataType.list(pf.DataType.map(pf.DataType.string(), details)),
             iter([{"key": {"name": "a"}}])),
            (pf.DataType.map(pf.DataType.string(), details), iter([("key", {"name": "a"})])),
        ):
            with self.subTest(field_type=field_type):
                dtype = pf.DataType.struct({"value": field_type})
                adapter = _resolve_map_udf(
                    lambda batch: {"value": pd.Series([value])}, dtype, "pandas", input_type)._func
                adapter.open(object())
                try:
                    with self.assertRaisesRegex(TypeError, "iterator"):
                        adapter.eval(pd.Series([1]))
                finally:
                    adapter.close()

    def test_map_requires_output_fields_even_when_nullable(self):
        from pyflink.dataframe.udf import _resolve_map_udf

        class Output(TypedDict):
            value: Optional[str]

        def transform(row) -> Output:
            return {"value": None} if row["id"] else {}

        input_type = TableDataTypes.ROW([TableDataTypes.FIELD("id", TableDataTypes.BIGINT())])
        explicit = pf.DataType.struct({"value": pf.DataType.string()})
        for source, dtype in (
            (transform, None), (transform, Output),
            (pf.udf(transform), None), (transform, explicit),
        ):
            with self.subTest(source=source, dtype=dtype):
                adapter = _resolve_map_udf(source, dtype, "general", input_type)._func
                adapter.open(object())
                try:
                    self.assertEqual(adapter.eval(1), Row(value=None))
                    with self.assertRaisesRegex(KeyError, "value"):
                        adapter.eval(0)
                finally:
                    adapter.close()

    def test_map_requires_nested_row_fields_including_collections(self):
        from pyflink.dataframe.udf import _resolve_map_udf

        details = pf.DataType.struct({"name": pf.DataType.string()})
        dtype = pf.DataType.struct({
            "details": details,
            "items": pf.DataType.list(details),
            "lookup": pf.DataType.map(pf.DataType.string(), details),
        })
        input_type = TableDataTypes.ROW([TableDataTypes.FIELD("id", TableDataTypes.BIGINT())])
        valid = {"details": {"name": None, "extra": 1}, "items": [None, {"name": "a"}],
                 "lookup": {"any key": {"name": None}}, "extra": 2}
        for result in (valid, dict(valid, details={}), dict(valid, items=[{}]),
                       dict(valid, lookup={"any key": {}}),
                       dict(valid, details=types.SimpleNamespace())):
            def transform(row):
                return result

            async def transform_async(row):
                return result

            for source in (transform, transform_async):
                with self.subTest(result=result, source=source):
                    adapter = _resolve_map_udf(source, dtype, "general", input_type)._func
                    adapter.open(object())
                    try:
                        def invoke():
                            value = adapter.eval(1)
                            return asyncio.run(value) if inspect.isawaitable(value) else value

                        if result is valid:
                            self.assertEqual(invoke(), Row(
                                details=Row(name=None), items=[None, Row(name="a")],
                                lookup={"any key": Row(name=None)}))
                        else:
                            with self.assertRaisesRegex((KeyError, AttributeError), "name"):
                                invoke()
                    finally:
                        adapter.close()

    def test_row_maps_receive_named_fields_and_project_output_schema(self):
        from pyflink.dataframe.udf import (
            _DataFrameAsyncScalarFunctionAdapter,
            _DataFrameScalarFunctionAdapter,
            _UDFUsage,
            _resolve_udf,
        )

        def transform(row):
            self.assertEqual(row, {"value": 3, "label": "a"})
            return {"label": row["label"].upper(), "extra": 99, "value": row["value"] + 1}

        async def transform_async(row):
            return transform(row)

        dtype = pf.DataType.struct({"value": pf.DataType.int64(), "label": pf.DataType.string()})
        for source, adapter_type in (
            (transform, _DataFrameScalarFunctionAdapter),
            (transform_async, _DataFrameAsyncScalarFunctionAdapter),
        ):
            with self.subTest(source=source):
                adapter = adapter_type(
                    _resolve_udf(source).runtime_source, dtype, True, _UDFUsage.MAP, "general",
                    input_type=dtype._to_table_data_type(),
                )
                adapter.open(object())
                try:
                    result = adapter.eval(3, "a")
                    if inspect.isawaitable(result):
                        result = asyncio.run(result)
                    self.assertEqual(result, Row(value=4, label="A"))
                finally:
                    adapter.close()

    def test_row_maps_preserve_resolved_column_inputs(self):
        import cloudpickle
        from pyflink.dataframe.udf import _resolve_map_udf
        from pyflink.fn_execution import flink_fn_execution_pb2 as proto
        from pyflink.fn_execution.table.operations import ScalarFunctionOperation
        from pyflink.serializers import PickleSerializer
        from pyflink.table.udf import DelegatingScalarFunction

        literal = proto.Input(inputConstant=b"\x00" + PickleSerializer().dumps("xyz"))
        increment = proto.UserDefinedFunction(
            payload=cloudpickle.dumps(DelegatingScalarFunction(lambda value: value + 1)),
            inputs=[proto.Input(inputOffset=0)])
        for fields, inputs, values, expected in (
            ({"tag": pf.DataType.string()}, [literal], [], Row(tag="xyz")),
            ({"id": pf.DataType.int64(), "tag": pf.DataType.string()},
             [proto.Input(inputOffset=0), literal], [1], Row(id=1, tag="xyz")),
            ({"first": pf.DataType.int64(), "second": pf.DataType.int64()},
             [proto.Input(udf=increment), proto.Input(udf=increment)], [1],
             Row(first=2, second=2)),
            ({"payload": pf.DataType.struct({"tag": pf.DataType.string()})},
             [proto.Input(inputOffset=0)], [Row(tag="xyz")], Row(payload=Row(tag="xyz"))),
        ):
            with self.subTest(fields=fields):
                dtype = pf.DataType.struct(fields)
                function = _resolve_map_udf(
                    lambda row: row, dtype, "general", dtype._to_table_data_type())
                operation = ScalarFunctionOperation(proto.UserDefinedFunctions(udfs=[
                    proto.UserDefinedFunction(
                        payload=cloudpickle.dumps(function._func), inputs=inputs,
                        takes_row_as_input=function._takes_row_as_input)]))
                operation.open()
                try:
                    self.assertEqual(operation.process_element(values), [expected])
                finally:
                    operation.close()

    def test_general_result_normalizers_are_bound_by_return_type(self):
        from pyflink.dataframe.udf import (
            _DataFrameAsyncScalarFunctionAdapter,
            _DataFrameScalarFunctionAdapter,
            _UDFUsage,
            _resolve_udf,
        )

        return_dtype = pf.DataType.struct(
            {
                "value": pf.DataType.int64(),
                "labels": pf.DataType.list(pf.DataType.string()),
            }
        )

        def describe(value):
            return {"value": value, "labels": (str(value),)}

        async def describe_async(value):
            return {"value": value, "labels": (str(value),)}

        sync_adapter = _DataFrameScalarFunctionAdapter(
            _resolve_udf(describe).runtime_source,
            return_dtype,
            True,
            _UDFUsage.EXPRESSION,
            "general",
        )
        async_adapter = _DataFrameAsyncScalarFunctionAdapter(
            _resolve_udf(describe_async).runtime_source,
            return_dtype,
            True,
            _UDFUsage.EXPRESSION,
            "general",
        )
        sync_adapter.open(object())
        async_adapter.open(object())
        expected = Row(value=3, labels=["3"])
        self.assertEqual(sync_adapter.eval(3), expected)
        self.assertEqual(asyncio.run(async_adapter.eval(3)), expected)

        def identity(value):
            return value

        leaf_adapter = _DataFrameScalarFunctionAdapter(
            _resolve_udf(identity).runtime_source,
            pf.DataType.int64(),
            True,
            _UDFUsage.EXPRESSION,
            "general",
        )
        leaf_adapter.open(object())
        self.assertIs(leaf_adapter._invocation(), identity)

    def test_scalar_function_lifecycle_and_cleanup(self):
        from pyflink.dataframe.udf import (
            _DataFrameAsyncScalarFunctionAdapter,
            _DataFrameScalarFunctionAdapter,
            _UDFUsage,
            _resolve_udf,
        )

        events = []

        def create_adapter(source, deterministic=True, async_mode=False):
            adapter_type = (
                _DataFrameAsyncScalarFunctionAdapter
                if async_mode
                else _DataFrameScalarFunctionAdapter
            )
            return adapter_type(
                _resolve_udf(source).runtime_source,
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
            _resolve_udf,
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
            _resolve_udf(BindingFailureFunction).runtime_source,
            pf.DataType.int64(),
            True,
            _UDFUsage.MAP,
            "general",
        )
        for _ in range(2):
            with self.assertRaisesRegex(ValueError, "input schema"):
                adapter.open(object())
            adapter.close()

        self.assertEqual(
            events,
            ["init", "open", "close", "init", "open", "close"],
        )


class DataFrameUDFPlannerTests(PyFlinkDataFrameUTTestCase):
    def test_map_function_classes_require_explicit_custom_determinism(self):
        class DefaultDeterminism(ScalarFunction):
            def eval(self, value):
                return {"value": value["value"]}

        class CustomDeterminism(DefaultDeterminism):
            def is_deterministic(self):
                return False

        class InheritedDeterminism(CustomDeterminism):
            pass

        class DeferredDeterminism(CustomDeterminism):
            def __init__(self):
                raise AssertionError("Function classes must be constructed on the worker.")

        dataframe = pf.from_records([(1,)], schema=["value"])
        dtype = pf.DataType.struct({"value": pf.DataType.int64()})
        for mode in ("general", "pandas", "arrow"):
            def bind(source, **kwargs):
                return (dataframe.map(source, **kwargs) if mode == "general" else
                        dataframe.map_batches(source, batch_format=mode, **kwargs))

            for source in (CustomDeterminism, InheritedDeterminism, DeferredDeterminism):
                with self.subTest(mode=mode, source=source):
                    with self.assertRaisesRegex(TypeError, "deterministic=.*instance"):
                        bind(source, return_dtype=dtype)
                    declared = pf.udf(source, return_dtype=dtype, func_type=mode,
                                      deterministic=False)
                    self.assert_dataframe_schema(bind(declared), ["value"])
            for source in (DefaultDeterminism, CustomDeterminism()):
                self.assert_dataframe_schema(bind(source, return_dtype=dtype), ["value"])

        class CustomAsyncDeterminism(AsyncScalarFunction):
            async def eval(self, value):
                return {"value": value["value"]}

            def is_deterministic(self):
                return False

        class InheritedAsyncDeterminism(CustomAsyncDeterminism):
            pass

        for source in (CustomAsyncDeterminism, InheritedAsyncDeterminism):
            with self.assertRaisesRegex(TypeError, "deterministic=.*instance"):
                dataframe.map(source, return_dtype=dtype)
            declared = pf.udf(source, return_dtype=dtype, deterministic=False)
            self.assert_dataframe_schema(dataframe.map(declared), ["value"])
            self.assert_dataframe_schema(dataframe.map(source(), return_dtype=dtype), ["value"])

    def test_map_batches_requires_matching_format_and_struct_schema(self):
        dataframe = pf.from_records([(1,)], schema=["id"])
        dtype = pf.DataType.struct({"value": pf.DataType.int64()})

        def transform(batch):
            return {"value": batch["id"]}

        for source in (transform, pf.udf(transform, return_dtype=dtype, func_type="pandas")):
            result = dataframe.map_batches(
                source, batch_format="pandas",
                **({"return_dtype": dtype} if source is transform else {}),
            )
            self.assertEqual(result.columns, ["value"])
        for call, error, message in (
            (lambda: dataframe.map_batches(transform), TypeError, "batch_format"),
            (lambda: dataframe.map_batches(transform, batch_format="numpy"), ValueError, "format"),
            (lambda: dataframe.map_batches(transform, batch_format="pandas"), TypeError,
             "return_dtype"),
            (lambda: dataframe.map_batches(transform, batch_format="pandas", return_dtype=int),
             TypeError, "struct"),
            (lambda: dataframe.map_batches(
                pf.udf(transform, return_dtype=dtype), batch_format="pandas"), ValueError, "mode"),
        ):
            with self.subTest(message=message):
                with self.assertRaisesRegex(error, message):
                    call()

        for field_type in (
            pf.DataType.struct({"inner": pf.DataType.struct({"v": pf.DataType.int64()})}),
            pf.DataType.struct({"ts": pf.DataType.timestamp_ltz(3)}),
            pf.DataType.list(pf.DataType.struct({"v": pf.DataType.int64()})),
            pf.DataType.list(pf.DataType.timestamp_ltz(3)),
        ):
            with self.subTest(field_type=field_type):
                output_type = pf.DataType.struct({"value": field_type})
                with self.assertRaises((TypeError, ValueError)):
                    dataframe.map_batches(transform, batch_format="pandas",
                                          return_dtype=output_type)
                nested = dataframe.with_columns(value=pf.udf(
                    lambda value: value, return_dtype=field_type)(pf.col("id"))).select("value")
                with self.assertRaises((TypeError, ValueError)):
                    nested.map_batches(transform, batch_format="pandas", return_dtype=dtype)
                self.assertEqual(dataframe.map_batches(
                    transform, batch_format="arrow", return_dtype=output_type).columns, ["value"])

    def test_map_infers_schema_and_rejects_conflicting_declarations(self):
        class Output(TypedDict):
            label: str
            value: int

        def transform(row) -> Output:
            return {"value": row["id"] + 1, "label": "mapped"}

        dataframe = pf.from_records([(1,)], schema=["id"])
        declaration = pf.udf(transform)
        for source in (transform, declaration):
            result = dataframe.map(source)
            self.assertEqual(result.columns, ["label", "value"])
        with self.assertRaisesRegex(ValueError, "return_dtype"):
            dataframe.map(declaration, return_dtype=Output)
        with self.assertRaisesRegex(TypeError, "struct"):
            dataframe.map(lambda row: row["id"], return_dtype=int)
        with self.assertRaisesRegex(ValueError, "mode"):
            dataframe.map(pf.udf(transform, return_dtype=Output, func_type="pandas"))

    def test_arrow_calls_require_a_column_argument(self):
        from pyflink.table import ExplainDetail

        dataframe = pf.from_records([(1,)], schema=["id"])
        identity = pf.udf(lambda *values: values[0], return_dtype=pf.DataType.int64(),
                          func_type="arrow")
        for args in ((), (1,), (pf.lit(1),), (identity(),)):
            with self.subTest(args=args):
                with self.assertRaisesRegex(Exception, "at least one column-valued argument"):
                    result = dataframe.with_columns(
                        valid=identity(pf.col("id")), invalid=identity(*args))
                    result.to_table().explain(ExplainDetail.JSON_EXECUTION_PLAN)

    def test_with_columns_binds_expressions_and_resolves_output_schema(self):
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
        )

        self.assert_dataframe_schema(
            result,
            ["id", "rendered", "description"],
            [
                TableDataTypes.BIGINT(),
                TableDataTypes.STRING().not_null(),
                TableDataTypes.ROW(
                    [
                        TableDataTypes.FIELD("value", TableDataTypes.BIGINT()),
                        TableDataTypes.FIELD(
                            "tags", TableDataTypes.ARRAY(TableDataTypes.STRING())
                        ),
                    ]
                ),
            ],
        )


class DataFrameUDFITCase(PyFlinkStreamDataFrameTestCase):
    def test_row_maps_preserve_literal_and_fused_inputs(self):
        self.env.set_parallelism(1)

        def identity(row):
            return row

        async def async_identity(row):
            return row

        @pf.udf
        def increment(value: int) -> int:
            return value + 1

        source = pf.from_records([(1,), (2,)], schema=["id"])
        tagged = source.with_columns(tag=pf.lit("xyz"))
        for dataframe, fields, expected in (
            (tagged.select("tag"), {"tag": pf.DataType.string()},
             [Row("xyz"), Row("xyz")]),
            (tagged, {"id": pf.DataType.int64(), "tag": pf.DataType.string()},
             [Row(1, "xyz"), Row(2, "xyz")]),
            (source.select(increment(pf.col("id")).alias("first"),
                           increment(pf.col("id")).alias("second")),
             {"first": pf.DataType.int64(), "second": pf.DataType.int64()},
             [Row(2, 2), Row(3, 3)]),
        ):
            dtype = pf.DataType.struct(fields)
            for function in (identity, async_identity):
                with self.subTest(fields=fields, function=function):
                    result = dataframe.map(function, return_dtype=dtype).map(
                        identity, return_dtype=dtype)
                    self.assertEqual(result.collect(), expected)

    def test_batch_maps_broadcast_literal_columns(self):
        self.env.set_parallelism(1)
        self.t_env.get_config().set("python.fn-execution.bundle.size", "3")
        self.t_env.get_config().set("python.fn-execution.arrow.batch.size", "2")
        dtype = pf.DataType.struct({
            "tag": pf.DataType.string(), "empty": pf.DataType.int64(),
            "enabled": pf.DataType.bool(), "id": pf.DataType.int64()})
        dataframe = pf.from_records([(1,), (2,), (3,)], schema=["id"]).with_columns(
            tag=pf.lit("x"), empty=pf.lit(None, pf.DataType.int64()),
            enabled=pf.lit(True)).select("tag", "empty", "enabled", "id")
        for batch_format in ("pandas", "arrow"):
            with self.subTest(batch_format=batch_format):
                result = dataframe.map_batches(
                    lambda batch: batch, batch_format=batch_format, return_dtype=dtype)
                self.assertEqual(result.collect(),
                                 [Row("x", None, True, value) for value in (1, 2, 3)])

    def test_pandas_output_alignment_preserves_input_order_and_ltz(self):
        import datetime

        self.env.set_parallelism(1)
        self.t_env.get_config().set("python.fn-execution.bundle.size", "3")
        self.t_env.get_config().set("python.fn-execution.arrow.batch.size", "2")
        timestamp = datetime.datetime(2020, 1, 1, 12)

        def transform(batch):
            return {"score": (batch["id"] * 10).sort_index(ascending=False),
                    "id": batch["id"], "ts": batch["ts"]}

        timezone = self.t_env.get_config().get_local_timezone()
        try:
            for zone, epoch in [("UTC", 1577880000000), ("Asia/Shanghai", 1577851200000)]:
                with self.subTest(timezone=zone):
                    self.t_env.get_config().set_local_timezone(zone)
                    from pyflink.table.expressions import call_sql, col

                    source = self.t_env.from_elements(
                        [(10, epoch), (20, epoch), (30, None)], ["id", "epoch"])
                    dataframe = pf.from_table(source.select(
                        col("id"), call_sql("TO_TIMESTAMP_LTZ(epoch, 3)").alias("ts")))
                    result = dataframe.map_batches(
                        transform, batch_format="pandas", return_dtype=pf.DataType.struct({
                            "score": pf.DataType.int64(), "id": pf.DataType.int64(),
                            "ts": pf.DataType.timestamp_ltz(3)}))
                    result = result.select(pf.col("score"), pf.col("id"),
                                           pf.col("ts").cast(pf.DataType.timestamp(3)).alias("ts"))
                    self.assertEqual(result.collect(), [
                        Row(100, 10, timestamp), Row(200, 20, timestamp), Row(300, 30, None)])

        finally:
            self.t_env.get_config().set_local_timezone(timezone)

    def test_pandas_map_preserves_row_columns_across_execution_boundaries(self):
        from pyflink.table.udf import udf as table_udf

        self.env.set_parallelism(1)
        self.t_env.get_config().set("python.fn-execution.bundle.size", "3")
        self.t_env.get_config().set("python.fn-execution.arrow.batch.size", "2")
        payload_type = pf.DataType.struct({
            "lookup": pf.DataType.map(pf.DataType.string().not_null(), pf.DataType.int64()),
            "v": pf.DataType.int64(),
        })
        input_type = pf.DataType.struct({"id": pf.DataType.int64(), "payload": payload_type})
        value_type = pf.DataType.struct({"v": pf.DataType.int64()})
        output_type = pf.DataType.struct({"total": pf.DataType.int64(), "value": value_type})
        flat_type = pf.DataType.struct({"total": pf.DataType.int64(), "v": pf.DataType.int64()})

        def produce(values):
            rows = [
                {"lookup": [("long-key", int(value))], "v": 11} if value > 0 else
                {"lookup": [], "v": None} if value == 0 else
                {"lookup": None, "v": None}
                for value in values
            ]
            return pd.DataFrame({
                label: pd.Series([row[field] for row in rows], index=values.index[::-1],
                                 dtype=object)
                for label, field in zip(("unrelated1", "unrelated2"), ("lookup", "v"))
            })

        def consume(batch):
            assert isinstance(batch["payload"], pd.DataFrame)
            return {"total": batch["payload"]["lookup"].map(
                lambda pairs: -1 if pairs is None else sum(value for _, value in pairs)
            ) + batch["id"] * 10, "value": batch["payload"][["v"]].sort_index(ascending=False)}

        def extract(batch):
            return {"total": batch["total"], "v": batch["value"].field("v")}

        @table_udf(result_type=flat_type._to_table_data_type(), func_type="pandas")
        def table_extract(batch):
            assert isinstance(batch["total"], pd.Series)
            return pd.DataFrame({name: batch[name] for name in ("total", "v")})

        producer = pf.udf(produce, return_dtype=payload_type, func_type="pandas")
        for transported in (False, True):
            with self.subTest(transported=transported):
                dataframe = pf.from_records([(1,), (0,), (-1,)], schema=["id"]).with_columns(
                    payload=producer(pf.col("id")))
                if transported:
                    dataframe = dataframe.map_batches(
                        lambda batch: batch, return_dtype=input_type, batch_format="arrow")
                mapped = dataframe.map_batches(
                    consume, return_dtype=output_type, batch_format="pandas").map_batches(
                    lambda batch: batch, return_dtype=output_type, batch_format="pandas")
                for table_map in (False, True):
                    result = (pf.from_table(mapped.to_table().map(table_extract)) if table_map
                              else mapped.map_batches(extract, batch_format="arrow",
                                                      return_dtype=flat_type))
                    self.assertEqual(sorted(result.collect(), key=lambda row: row[0]),
                                     [Row(-11, None), Row(0, None), Row(11, 11)])

    def test_row_and_batch_maps_in_one_job(self):
        self.env.set_parallelism(1)
        self.t_env.get_config().set("python.fn-execution.bundle.size", "3")
        self.t_env.get_config().set("python.fn-execution.arrow.batch.size", "2")
        dtype = pf.DataType.struct({"id": pf.DataType.int64(), "value": pf.DataType.int64()})

        class Prepare(ScalarFunction):
            def eval(self, row):
                assert isinstance(row, dict)
                return {"value": row["id"] * 2, "id": row["id"], "ignored": 0}

            def is_deterministic(self):
                return False

        prepare = pf.udf(Prepare, return_dtype=dtype, deterministic=False)

        async def enrich(row):
            assert isinstance(row, dict)
            return {"id": row["id"], "value": row["value"] + 1}

        def pandas_add(batch):
            assert isinstance(batch, dict)
            assert all(isinstance(column, pd.Series) for column in batch.values())
            assert 1 <= len(batch["id"]) <= 2
            return {"id": batch["id"], "value": batch["value"] + 10}

        def arrow_double(batch):
            import pyarrow.compute as pc

            assert isinstance(batch, dict)
            assert all(isinstance(column, pa.Array) for column in batch.values())
            return {"id": pa.chunked_array([batch["id"].slice(0, 1), batch["id"].slice(1)]),
                    "value": pc.multiply(batch["value"], 2)}

        flat_dtype = pf.DataType.struct({
            "id": pf.DataType.int64(), "value": pf.DataType.int64(),
            "inner": pf.DataType.int64(),
        })
        nested_dtype = pf.DataType.struct({
            "id": pf.DataType.int64(),
            "payload": pf.DataType.struct({
                "value": pf.DataType.int64(),
                "inner": pf.DataType.struct({"value": pf.DataType.int64()}),
            }),
        })

        def pandas_prepare(batch):
            return {"id": batch["id"], "value": batch["value"],
                    "inner": pd.Series([None if identifier == 3 else 3
                                        for identifier in batch["id"]], dtype=object)}

        def arrow_nest(batch):
            return {"id": batch["id"], "payload": pa.array([
                None if identifier == 5 else {"value": value, "inner":
                    None if identifier == 4 else {"value": inner}}
                for identifier, value, inner in zip(
                    batch["id"].to_pylist(), batch["value"].to_pylist(), batch["inner"].to_pylist())
            ], type=pa.struct([("value", pa.int64()),
                              ("inner", pa.struct([("value", pa.int64())]))]))}

        def arrow_unnest(batch):
            assert isinstance(batch["payload"], pa.StructArray)
            return {"id": batch["id"], "value": pa.array([
                -1 if payload is None else -2 if payload["inner"] is None else -3
                if payload["inner"]["value"] is None
                else payload["value"] + payload["inner"]["value"]
                for payload in batch["payload"].to_pylist()
            ], type=pa.int64())}

        result = (pf.from_records([(1,), (2,), (3,), (4,), (5,)], schema=["id"])
                  .map(prepare).map(enrich, return_dtype=dtype)
                  .map_batches(pandas_add, return_dtype=dtype, batch_format="pandas")
                  .map_batches(arrow_double, return_dtype=dtype, batch_format="arrow")
                  .map_batches(pandas_prepare, return_dtype=flat_dtype, batch_format="pandas")
                  .map_batches(arrow_nest, return_dtype=nested_dtype, batch_format="arrow")
                  .map_batches(arrow_unnest, return_dtype=dtype, batch_format="arrow"))
        self.assertEqual(sorted(result.collect(), key=lambda row: row[0]),
                         [Row(1, 29), Row(2, 33), Row(3, -3), Row(4, -2), Row(5, -1)])

    def test_supported_scalar_udfs_in_one_job(self):
        import pyarrow.compute as pc

        self.env.set_parallelism(1)
        self.t_env.get_config().set("python.fn-execution.bundle.size", "3")
        self.t_env.get_config().set("python.fn-execution.arrow.batch.size", "2")

        @pf.udf(return_dtype=pf.DataType.string())
        def normalize_name(names: pa.Array) -> pa.Array:
            return pc.utf8_upper(names)

        @pf.udf(return_dtype=pf.DataType.struct({"value": pf.DataType.int64().not_null()}))
        def describe(values: pa.Array) -> pa.ChunkedArray:
            result = pa.StructArray.from_arrays([pc.multiply(values, 2)], names=["value"])
            return pa.chunked_array([result.slice(0, 1), result.slice(1)])

        @pf.udf(return_dtype=pf.DataType.int64())
        def struct_value(values: pa.Array) -> pa.Array:
            return pc.struct_field(values, "value")

        @dataclass
        class Details:
            doubled: int
            labels: list

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

        deferred = pf.udf(DeferredCallable)
        opened_scalar_class = pf.udf(OpenedScalarFunction)

        result = (
            pf.from_records([(1, "alice"), (2, None), (3, "Bob")], schema=["id", "name"])
            .with_columns(async_value=add_two(pf.col("id")))
            .with_columns(
                pandas_value=add_three(pf.col("id")),
                details=details(pf.col("id")),
                deferred_value=deferred(pf.col("id")),
                scalar_value=opened_scalar_class(pf.col("id")),
                normalized_name=normalize_name(pf.col("name")),
                arrow_details=describe(pf.col("id")),
                arrow_after_pandas=struct_value(describe(add_three(pf.col("id")))),
                pandas_after_arrow=add_three(struct_value(describe(pf.col("id")))),
            )
        )

        self.assertEqual(
            sorted(result.collect(), key=lambda row: row[0]),
            [
                Row(1, "alice", 3, 4, Row(2, ["1"]), 5, 6, "ALICE", Row(2), 8, 5),
                Row(2, None, 4, 5, Row(4, ["2"]), 6, 7, None, Row(4), 10, 7),
                Row(3, "Bob", 5, 6, Row(6, ["3"]), 7, 8, "BOB", Row(6), 12, 9),
            ],
        )


if __name__ == "__main__":
    unittest.main()
