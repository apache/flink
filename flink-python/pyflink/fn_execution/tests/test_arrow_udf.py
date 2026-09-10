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

"""Behavior of Arrow scalar operations without a Flink cluster."""

import pickle
import unittest

import cloudpickle
import pyarrow as pa
import pyarrow.compute as pc

from pyflink.fn_execution import flink_fn_execution_pb2 as proto
from pyflink.fn_execution.table.operations import ScalarFunctionOperation
from pyflink.table.udf import DelegatingScalarFunction, ScalarFunction


class Uppercase(ScalarFunction):
    def eval(self, values):
        return pc.utf8_upper(values)


class ArrowScalarOperationTests(unittest.TestCase):
    def operation(self, func, inputs):
        function = proto.UserDefinedFunction(
            payload=cloudpickle.dumps(DelegatingScalarFunction(func)),
            is_arrow_udf=True, inputs=inputs)
        operation = ScalarFunctionOperation(proto.UserDefinedFunctions(udfs=[function]))
        operation.open()
        self.addCleanup(operation.close)
        return operation

    def test_literals_columns_and_chunked_results(self):
        def add(offset, left, right):
            if not isinstance(offset, int) or not isinstance(left, pa.Array) \
                    or not isinstance(right, pa.Array):
                raise TypeError("Expected a scalar literal followed by two Arrow arrays.")
            values = pc.add(pc.add(left, right), offset)
            return pa.chunked_array([values.slice(0, 1), values.slice(1)])

        operation = self.operation(add, [
            proto.Input(inputConstant=b"\x00" + pickle.dumps(10)),
            proto.Input(inputOffset=0), proto.Input(inputOffset=1)])
        result = operation.process_element(pa.record_batch(
            [pa.array([1, None, 3]), pa.array([4, 5, 6])], names=["a", "b"]))
        self.assertEqual(result.column(0).to_pylist(), [15, None, 19])

    def test_invalid_scalar_results(self):
        batch = pa.record_batch([pa.array([1, 2, 3])], names=["value"])
        for result, error, message in (
            ([1, 2, 3], TypeError, "Array or pyarrow.ChunkedArray"),
            (None, TypeError, "NoneType"),
            (pa.scalar(1), TypeError, "Scalar"),
            (batch, TypeError, "RecordBatch"),
            (pa.Table.from_batches([batch]), TypeError, "Table"),
            (pa.array([1]), ValueError, "returned 1 rows, expected 3"),
            (pa.chunked_array([[1], [2]]), ValueError, "returned 2 rows, expected 3"),
        ):
            with self.subTest(result=result):
                operation = self.operation(lambda values: result, [proto.Input(inputOffset=0)])
                with self.assertRaisesRegex(error, message):
                    operation.process_element(batch)

    def test_invalid_intermediate_result_is_not_consumed(self):
        inner = proto.UserDefinedFunction(
            payload=cloudpickle.dumps(DelegatingScalarFunction(lambda values: values.slice(0, 1))),
            is_arrow_udf=True, inputs=[proto.Input(inputOffset=0)])
        # The outer result has the correct batch length, but must not hide the invalid inner result.
        operation = self.operation(lambda values: pa.array([1, 2, 3]), [proto.Input(udf=inner)])
        with self.assertRaisesRegex(ValueError, "returned 1 rows, expected 3"):
            operation.process_element(pa.record_batch([pa.array([1, 2, 3])], names=["value"]))

    def test_arrow_scalar_operation(self):
        function = proto.UserDefinedFunction(
            payload=cloudpickle.dumps(Uppercase()), is_arrow_udf=True,
            inputs=[proto.Input(inputOffset=0)])
        operation = ScalarFunctionOperation(
            proto.UserDefinedFunctions(udfs=[function]), one_arg_optimization=True)
        operation.open()
        self.addCleanup(operation.close)
        result = operation.process_element(pa.record_batch(
            [pa.array(["alice", None, "Bob"])], names=["name"]))
        self.assertIsInstance(result, pa.RecordBatch)
        self.assertEqual(result.column(0).to_pylist(), ["ALICE", None, "BOB"])

    def test_intermediate_schema_is_enforced(self):
        for values, error, message in (
            (pa.array(["wrong", "type"]), TypeError, "expected int64"),
            (pa.chunked_array([[1], [None]], type=pa.int64()), ValueError, "not nullable"),
        ):
            with self.subTest(values=values):
                inner = proto.UserDefinedFunction(
                    payload=cloudpickle.dumps(DelegatingScalarFunction(lambda column: values)),
                    is_arrow_udf=True, inputs=[proto.Input(inputOffset=0)],
                    output_type=proto.Schema.FieldType(type_name=proto.Schema.BIGINT,
                                                       nullable=False))
                operation = self.operation(lambda column: pa.array([1, 2]),
                                           [proto.Input(udf=inner)])
                with self.assertRaisesRegex(error, message):
                    operation.process_element(pa.record_batch([pa.array([1, 2])], names=["a"]))


if __name__ == "__main__":
    unittest.main()
