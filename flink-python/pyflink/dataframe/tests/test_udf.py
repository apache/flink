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
from typing import Any, Callable, TypedDict, cast
from unittest import mock

import pandas as pd
import pyarrow as pa
import pyflink.dataframe as pf
from pyflink.common import Row, RowKind
from pyflink.table import DataTypes as TableDataTypes
from pyflink.table.expression import Expression
from pyflink.table.types import RowType
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
        self.assertEqual(_return_dtype(decorated), pf.DataType.int64())
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
        self.assertEqual(_return_dtype(direct), pf.DataType.int64())
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
                "id": pf.DataType.int64(),
                "details": pf.DataType.struct(
                    {
                        "label": pf.DataType.string(),
                        "scores": pf.DataType.list(pf.DataType.int64()),
                    }
                ),
            }
        )
        declarations = [
            (
                "Python type",
                lambda: pf.udf(identity, return_dtype=int),
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
                pf.DataType.int64(),
            ),
            (
                "postponed return with unresolved input",
                lambda: pf.udf(postponed_return_with_unresolved_input),
                pf.DataType.int64(),
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
                self.assertEqual(_return_dtype(decorated), pf.DataType.int64())

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

        expected = pf.DataType.struct({"value": pf.DataType.int64()})
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

        expected = pf.DataType.struct({"value": pf.DataType.int64()})
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
            _return_dtype(pf.udf(ReceivingCallable)), pf.DataType.int64()
        )

        class OverriddenScalarFunction(ScalarFunction):
            _UDF_TEST_ALIAS = str

            def eval(self, *values: int) -> str:
                value, = values
                return str(value)

        overridden = OverriddenScalarFunction()
        overridden.eval = types.MethodType(_module_alias_method, overridden)
        self.assertEqual(
            _return_dtype(pf.udf(overridden)), pf.DataType.int64()
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
        self.assertEqual(_return_dtype(uninspectable), pf.DataType.int64())

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
            pf.DataType.int64(),
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
                    _return_dtype(declaration), pf.DataType.int64()
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
            with self.assertRaisesRegex(NotImplementedError, "'map'"):
                adapter.open(object())
            adapter.close()

        self.assertEqual(
            events,
            ["init", "open", "close", "init", "open", "close"],
        )


class DataFrameUDFPlannerTests(PyFlinkDataFrameUTTestCase):
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
                TableDataTypes.STRING(),
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
    def test_supported_scalar_udfs_in_one_job(self):
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
            pf.from_records([(1,)], schema=["id"])
            .with_columns(async_value=add_two(pf.col("id")))
            .with_columns(
                pandas_value=add_three(pf.col("id")),
                details=details(pf.col("id")),
                deferred_value=deferred(pf.col("id")),
                scalar_value=opened_scalar_class(pf.col("id")),
            )
        )

        self.assertEqual(
            result.collect(),
            [Row(1, 3, 4, Row(2, ["1"]), 5, 6)],
        )


if __name__ == "__main__":
    unittest.main()
