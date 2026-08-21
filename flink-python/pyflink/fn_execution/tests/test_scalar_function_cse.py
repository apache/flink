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
        # udf2/udf3 reference earlier results via results[N]
        self.assertIn('results[0]', generated_code)
        self.assertIn('results[1]', generated_code)
        self.assertIn('return results', generated_code)

        # Verify computation results
        result = op.func([3, 4])
        # udf1(3, 4) = 7, udf2(7) = 21, udf3(7, 21) = 28
        self.assertEqual(result[0], 7)
        self.assertEqual(result[1], 21)
        self.assertEqual(result[2], 28)

    def test_no_cse_lambda_execution(self):
        """
        Verify that without refIndex, traditional lambda codegen is used,
        both for independent UDFs and for traditional nested UDF encoding.
        """
        # --- Scenario 1: Independent UDFs, no CSE ---
        udf1_payload = _make_udf_payload(lambda x: x * 2)
        udf2_payload = _make_udf_payload(lambda x: x + 10)

        udf1_proto = _make_udf_proto(udf1_payload, [_input_offset(0)])
        udf2_proto = _make_udf_proto(udf2_payload, [_input_offset(1)])

        op = ScalarFunctionOperation(_build_serialized_fn([udf1_proto, udf2_proto]))

        self.assertEqual(op.func.__name__, '<lambda>')
        self.assertTrue(op._generated_code.startswith('lambda value:'))
        self.assertNotIn('results[', op._generated_code)

        result = op.func([5, 3])
        # udf1(5) = 10, udf2(3) = 13
        self.assertEqual(result[0], 10)
        self.assertEqual(result[1], 13)

        # --- Scenario 2: Traditional nested UDF encoding, no refIndex ---
        inner_udf1_proto = _make_udf_proto(udf1_payload, [_input_offset(0)])
        outer_udf2_proto = _make_udf_proto(udf2_payload, [_input_udf(inner_udf1_proto)])

        op2 = ScalarFunctionOperation(_build_serialized_fn([outer_udf2_proto]))

        self.assertEqual(op2.func.__name__, '<lambda>')
        self.assertNotIn('results[', op2._generated_code)

        # udf2(udf1(5)) = udf2(10) = 20
        self.assertEqual(op2.func([5])[0], 20)

    def test_cse_with_takes_row_as_input(self):
        """
        Verify that when takesRowAsInput=True but the UDF receives its input
        via refIndex (CSE), the generated code uses results[N] instead of
        overriding with `value`.
        """
        from pyflink.common import Row

        # func: reads value[0], returns Row(a, b)
        func_payload = _make_udf_payload(lambda x: Row(a=x + 1, b=x * x))
        # func2: takes Row, returns Row
        func2_payload = _make_udf_payload(lambda x: Row(a=x.a + 1, b=x.b * 2))

        func_proto = _make_udf_proto(func_payload, [_input_offset(0)])
        func_proto.takes_row_as_input = False

        # func2 references func's result via refIndex=0
        func2_proto = _make_udf_proto(func2_payload, [_input_ref_index(0)])
        func2_proto.takes_row_as_input = True

        op = ScalarFunctionOperation(_build_serialized_fn([func_proto, func2_proto]))

        self.assertEqual(op.func.__name__, '_sequential_execute')
        # func2 (results[1]) should receive results[0], NOT `value`
        line2 = [l for l in op._generated_code.split('\n') if 'results[1]' in l][0]
        self.assertIn('results[0]', line2, "func2 should receive results[0], not value")

        result = op.func([3])
        # func(3) = Row(a=4, b=9); func2(Row(4, 9)) = Row(a=5, b=18)
        self.assertEqual(result[0].a, 4)
        self.assertEqual(result[0].b, 9)
        self.assertEqual(result[1].a, 5)
        self.assertEqual(result[1].b, 18)


if __name__ == '__main__':
    unittest.main()
