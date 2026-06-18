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

"""
Tests for the CSE sequential execution logic in ScalarFunctionOperation.

When Python UDFs use refIndex (full-tree CSE), the execution switches from a
lambda-based approach to a sequential model where each UDF result is stored
in a `results` list for later reference.
"""

import unittest

import cloudpickle

from pyflink.fn_execution import flink_fn_execution_pb2
from pyflink.fn_execution.table.operations import ScalarFunctionOperation
from pyflink.table.udf import DelegatingScalarFunction


def _make_udf_payload(func):
    """Serialize a Python function into a UDF payload (cloudpickle of DelegatingScalarFunction)."""
    return cloudpickle.dumps(DelegatingScalarFunction(func))


def _build_serialized_fn(udf_protos):
    """
    Build a UserDefinedFunctions protobuf message from a list of UserDefinedFunction protos.
    """
    serialized_fn = flink_fn_execution_pb2.UserDefinedFunctions()
    serialized_fn.metric_enabled = False
    for udf_proto in udf_protos:
        serialized_fn.udfs.append(udf_proto)
    return serialized_fn


def _make_udf_proto(payload, inputs):
    """
    Create a UserDefinedFunction proto.

    :param payload: serialized UDF bytes
    :param inputs: list of Input protos
    """
    udf = flink_fn_execution_pb2.UserDefinedFunction()
    udf.payload = payload
    for inp in inputs:
        udf.inputs.append(inp)
    return udf


def _input_offset(offset):
    """Create an Input proto with inputOffset."""
    inp = flink_fn_execution_pb2.Input()
    inp.inputOffset = offset
    return inp


def _input_ref_index(index):
    """Create an Input proto with refIndex (CSE reference)."""
    inp = flink_fn_execution_pb2.Input()
    inp.refIndex = index
    return inp


def _input_udf(udf_proto):
    """Create an Input proto with a nested UDF (traditional chaining)."""
    inp = flink_fn_execution_pb2.Input()
    inp.udf.CopyFrom(udf_proto)
    return inp


class TestScalarFunctionCse(unittest.TestCase):
    """
    Tests for CSE sequential execution in ScalarFunctionOperation.

    Java side deduplicates Python UDF calls and encodes shared sub-expressions
    as refIndex=N. Python side detects this and generates a sequential function
    that stores intermediate results. Without refIndex, lambda codegen is used.
    """

    def test_cse_sequential_execution(self):
        """
        Verify refIndex triggers sequential codegen and produces correct results.

        Scenario: udf1(x,y), udf2(udf1(x,y)), udf3(udf1(x,y), udf2(udf1(x,y)))
        Flattened: [udf1(v[0],v[1]), udf2(results[0]), udf3(results[0],results[1])]
        """
        udf1_payload = _make_udf_payload(lambda x, y: x + y)
        udf2_payload = _make_udf_payload(lambda x: x * 3)
        udf3_payload = _make_udf_payload(lambda a, b: a + b)

        udf1_proto = _make_udf_proto(udf1_payload, [_input_offset(0), _input_offset(1)])
        udf2_proto = _make_udf_proto(udf2_payload, [_input_ref_index(0)])
        udf3_proto = _make_udf_proto(udf3_payload, [_input_ref_index(0), _input_ref_index(1)])

        serialized_fn = _build_serialized_fn([udf1_proto, udf2_proto, udf3_proto])

        op = ScalarFunctionOperation(serialized_fn)

        # Verify the function name confirms CSE codegen path
        self.assertEqual(op.func.__name__, '_sequential_execute')

        # Verify the actual generated code content from ScalarFunctionOperation
        generated_code = op._generated_code
        self.assertIn('def _sequential_execute(value):', generated_code)
        self.assertIn('results = [None] * 3', generated_code)
        # udf1 reads from input columns
        self.assertIn('results[0] = ', generated_code)
        self.assertIn('value[0]', generated_code)
        self.assertIn('value[1]', generated_code)
        # udf2 references udf1's result via results[0]
        self.assertIn('results[1] = ', generated_code)
        self.assertIn('results[0]', generated_code)
        # udf3 references both udf1 and udf2's results
        self.assertIn('results[2] = ', generated_code)
        self.assertIn('results[1]', generated_code)
        self.assertIn('return results', generated_code)

        # Verify computation results
        result = op.func([3, 4])
        # udf1(3, 4) = 3 + 4 = 7
        # udf2(results[0]) = udf2(7) = 7 * 3 = 21
        # udf3(results[0], results[1]) = udf3(7, 21) = 7 + 21 = 28
        self.assertEqual(result[0], 7)
        self.assertEqual(result[1], 21)
        self.assertEqual(result[2], 28)

        # Verify idempotency
        result2 = op.func([10, 5])
        # udf1(10, 5) = 15, udf2(15) = 45, udf3(15, 45) = 60
        self.assertEqual(result2[0], 15)
        self.assertEqual(result2[1], 45)
        self.assertEqual(result2[2], 60)

    def test_no_cse_lambda_execution(self):
        """
        Verify that without refIndex, traditional lambda codegen is used.

        Scenario 1: Independent UDFs with only inputOffset.
        Scenario 2: Traditional nested UDF encoding (udf as input, not refIndex).
        """
        # --- Scenario 1: Independent UDFs, no CSE ---
        udf1_payload = _make_udf_payload(lambda x: x * 2)
        udf2_payload = _make_udf_payload(lambda x: x + 10)

        udf1_proto = _make_udf_proto(udf1_payload, [_input_offset(0)])
        udf2_proto = _make_udf_proto(udf2_payload, [_input_offset(1)])

        serialized_fn = _build_serialized_fn([udf1_proto, udf2_proto])

        op = ScalarFunctionOperation(serialized_fn)

        # Verify the function name confirms lambda codegen path
        self.assertEqual(op.func.__name__, '<lambda>')

        # Verify the actual generated code is a lambda expression (no _sequential_execute)
        generated_code = op._generated_code
        self.assertTrue(generated_code.startswith('lambda value:'))
        self.assertNotIn('results[', generated_code)
        self.assertNotIn('_sequential_execute', generated_code)
        # Should contain direct value[N] references
        self.assertIn('value[0]', generated_code)
        self.assertIn('value[1]', generated_code)

        # Verify computation results
        result = op.func([5, 3])
        # udf1(5) = 10, udf2(3) = 13
        self.assertEqual(result[0], 10)
        self.assertEqual(result[1], 13)

        # --- Scenario 2: Traditional nested UDF encoding, no refIndex ---
        udf1_payload_nested = _make_udf_payload(lambda x: x * 2)
        udf2_payload_nested = _make_udf_payload(lambda x: x + 10)

        inner_udf1_proto = _make_udf_proto(udf1_payload_nested, [_input_offset(0)])
        outer_udf2_proto = _make_udf_proto(udf2_payload_nested, [_input_udf(inner_udf1_proto)])

        serialized_fn2 = _build_serialized_fn([outer_udf2_proto])

        op2 = ScalarFunctionOperation(serialized_fn2)

        # Verify the function name confirms lambda codegen path
        self.assertEqual(op2.func.__name__, '<lambda>')

        # Verify the actual generated code is a lambda with nested function call
        generated_code2 = op2._generated_code
        self.assertTrue(generated_code2.startswith('lambda value:'))
        self.assertNotIn('results[', generated_code2)
        # Should contain nested function call pattern (f_outer(f_inner(value[0])))
        self.assertIn('value[0]', generated_code2)

        # Verify computation results
        result2 = op2.func([5])
        # udf2(udf1(5)) = udf2(10) = 20
        self.assertEqual(result2[0], 20)

    def test_cse_with_takes_row_as_input(self):
        """
        Verify that when takesRowAsInput=True but the UDF receives its input
        via refIndex (CSE), the generated code uses results[N] instead of
        overriding with `value`.

        Scenario: func(value[0]) returns Row, func2 consumes func's result via refIndex.
        func2 has takesRowAsInput=True because in the original plan it received a Row.
        After CSE flattening, it should receive results[0] (func's output), not `value`
        (the original input row).

        This simulates the PythonMapMergeRule + CSE flattening for:
            .map(func(t.a)).map(func2)
        where func2 takes Row as input.
        """
        # func: returns Row(a, b)
        func_payload = _make_udf_payload(lambda x: type('Row', (), {'a': x + 1, 'b': x * x})())
        # Better: use actual Row
        from pyflink.common import Row
        func_payload = _make_udf_payload(lambda x: Row(a=x + 1, b=x * x))

        # func2: takes Row, returns Row
        func2_payload = _make_udf_payload(lambda x: Row(a=x.a + 1, b=x.b * 2))

        # func: takesRowAsInput=False, reads from value[0]
        func_proto = _make_udf_proto(func_payload, [_input_offset(0)])
        func_proto.takes_row_as_input = False

        # func2: takesRowAsInput=True, references func's result via refIndex=0
        func2_proto = _make_udf_proto(func2_payload, [_input_ref_index(0)])
        func2_proto.takes_row_as_input = True

        serialized_fn = _build_serialized_fn([func_proto, func2_proto])

        op = ScalarFunctionOperation(serialized_fn)

        # Verify the function name confirms CSE codegen path
        self.assertEqual(op.func.__name__, '_sequential_execute')

        # Verify the generated code
        generated_code = op._generated_code
        self.assertIn('def _sequential_execute(value):', generated_code)
        # func2 (results[1]) should receive results[0], NOT `value`
        line2 = [l for l in generated_code.split('\n') if 'results[1]' in l][0]
        self.assertIn('results[0]', line2, "func2 should receive results[0], not value")

        # Verify computation
        result = op.func([3])
        # func(3) = Row(a=4, b=9)
        # func2(Row(4, 9)) = Row(a=5, b=18)
        self.assertEqual(result[0].a, 4)
        self.assertEqual(result[0].b, 9)
        self.assertEqual(result[1].a, 5)
        self.assertEqual(result[1].b, 18)

        # Verify idempotency
        result2 = op.func([5])
        # func(5) = Row(a=6, b=25)
        # func2(Row(6, 25)) = Row(a=7, b=50)
        self.assertEqual(result2[0].a, 6)
        self.assertEqual(result2[0].b, 25)
        self.assertEqual(result2[1].a, 7)
        self.assertEqual(result2[1].b, 50)


if __name__ == '__main__':
    unittest.main()
