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

"""Tests common to all coder implementations."""
import logging
import unittest

from pyflink.fn_execution import coder_impl

try:
    from pyflink.fn_execution import fast_coder_impl

    have_cython = True
except ImportError:
    have_cython = False


@unittest.skipUnless(have_cython, "Uncompiled Cython Coder")
class CodersTest(unittest.TestCase):

    def check_cython_coder(self, python_field_coders, cython_field_coders, data):
        from apache_beam.coders.coder_impl import create_InputStream, create_OutputStream
        from pyflink.fn_execution.fast_coder_impl import InputStreamAndFunctionWrapper
        py_flatten_row_coder = coder_impl.FlattenRowCoderImpl(python_field_coders)
        internal = py_flatten_row_coder.encode(data)
        input_stream = create_InputStream(internal)
        output_stream = create_OutputStream()
        cy_flatten_row_coder = fast_coder_impl.FlattenRowCoderImpl(cython_field_coders)
        value = cy_flatten_row_coder.decode_from_stream(input_stream, False)
        wrapper_func_input_element = InputStreamAndFunctionWrapper(
            lambda v: [v[i] for i in range(len(v))], value)
        cy_flatten_row_coder.encode_to_stream(wrapper_func_input_element, output_stream, False)
        generator_result = py_flatten_row_coder.decode_from_stream(create_InputStream(
            output_stream.get()), False)
        result = []
        for item in generator_result:
            result.append(item)
        try:
            self.assertEqual(result, data)
        except AssertionError:
            self.assertEqual(len(result), len(data))
            self.assertEqual(len(result[0]), len(data[0]))
            for i in range(len(data[0])):
                if isinstance(data[0][i], float):
                    from pyflink.table.tests.test_udf import float_equal
                    assert float_equal(data[0][i], result[0][i], 1e-6)
                else:
                    self.assertEqual(data[0][i], result[0][i])

    # decide whether two floats are equal
    @staticmethod
    def float_equal(a, b, rel_tol=1e-09, abs_tol=0.0):
        return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)

    def test_cython_bigint_coder(self):
        data = [1, 100, -100, -1000]
        python_field_coders = [coder_impl.BigIntCoderImpl() for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.BigIntCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

    def test_cython_tinyint_coder(self):
        data = [1, 10, 127, -128]
        python_field_coders = [coder_impl.TinyIntCoderImpl() for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.TinyIntCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

    def test_cython_boolean_coder(self):
        data = [True, False]
        python_field_coders = [coder_impl.BooleanCoderImpl() for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.BooleanCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

    def test_cython_smallint_coder(self):
        data = [32767, -32768, 0]
        python_field_coders = [coder_impl.SmallIntCoderImpl() for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.SmallIntCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

    def test_cython_int_coder(self):
        data = [-2147483648, 2147483647]
        python_field_coders = [coder_impl.IntCoderImpl() for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.IntCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

    def test_cython_float_coder(self):
        data = [1.02, 1.32]
        python_field_coders = [coder_impl.FloatCoderImpl() for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.FloatCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

    def test_cython_double_coder(self):
        data = [-12.02, 1.98932]
        python_field_coders = [coder_impl.DoubleCoderImpl() for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.DoubleCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

    def test_cython_binary_coder(self):
        data = [b'pyflink', b'x\x00\x00\x00']
        python_field_coders = [coder_impl.BinaryCoderImpl() for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.BinaryCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

    def test_cython_char_coder(self):
        data = ['flink', '🐿']
        python_field_coders = [coder_impl.CharCoderImpl() for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.CharCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

    def test_cython_date_coder(self):
        import datetime
        data = [datetime.date(2019, 9, 10)]
        python_field_coders = [coder_impl.DateCoderImpl() for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.DateCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

    def test_cython_time_coder(self):
        import datetime
        data = [datetime.time(hour=11, minute=11, second=11, microsecond=123000)]
        python_field_coders = [coder_impl.TimeCoderImpl() for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.TimeCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

    def test_cython_decimal_coder(self):
        import decimal
        data = [decimal.Decimal('0.00001'), decimal.Decimal('1.23E-8')]
        python_field_coders = [coder_impl.DecimalCoderImpl(38, 18) for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.DecimalCoderImpl(38, 18) for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

        decimal.getcontext().prec = 2
        data = [decimal.Decimal('1.001')]
        python_field_coders = [coder_impl.DecimalCoderImpl(4, 3) for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.DecimalCoderImpl(4, 3) for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])
        self.assertEqual(decimal.getcontext().prec, 2)

    def test_cython_timestamp_coder(self):
        import datetime

        data = [datetime.datetime(2019, 9, 10, 18, 30, 20, 123000)]
        python_field_coders = [coder_impl.TimestampCoderImpl(3) for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.TimestampCoderImpl(3) for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

        data = [datetime.datetime(2019, 9, 10, 18, 30, 20, 123456)]
        python_field_coders = [coder_impl.TimestampCoderImpl(6) for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.TimestampCoderImpl(6) for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

    def test_cython_local_zoned_timestamp_coder(self):
        import datetime
        import pytz
        timezone = pytz.timezone("Asia/Shanghai")
        data = [timezone.localize(datetime.datetime(2019, 9, 10, 18, 30, 20, 123000))]
        python_field_coders = [coder_impl.LocalZonedTimestampCoderImpl(3, timezone)
                               for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.LocalZonedTimestampCoderImpl(3, timezone)
                               for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

        data = [timezone.localize(datetime.datetime(2019, 9, 10, 18, 30, 20, 123456))]
        python_field_coders = [coder_impl.LocalZonedTimestampCoderImpl(6, timezone)
                               for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.LocalZonedTimestampCoderImpl(6, timezone)
                               for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

    def test_cython_array_coder(self):
        data = [[1, 2, 3, None]]
        python_field_coders = [coder_impl.ArrayCoderImpl(coder_impl.BigIntCoderImpl())
                               for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.ArrayCoderImpl(fast_coder_impl.BigIntCoderImpl())
                               for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

    def test_cython_map_coder(self):
        data = [{'flink': 1, 'pyflink': 2, 'coder': None}]
        python_field_coders = [coder_impl.MapCoderImpl(coder_impl.CharCoderImpl(),
                                                       coder_impl.BigIntCoderImpl())
                               for _ in range(len(data))]
        cython_field_coders = [fast_coder_impl.MapCoderImpl(fast_coder_impl.CharCoderImpl(),
                                                            fast_coder_impl.BigIntCoderImpl())
                               for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])

    def test_cython_row_coder(self):
        from pyflink.table import Row
        field_count = 2
        data = [Row(*[None if i % 2 == 0 else i for i in range(field_count)])]
        python_field_coders = [coder_impl.RowCoderImpl([coder_impl.BigIntCoderImpl()
                                                        for _ in range(field_count)])]
        cython_field_coders = [fast_coder_impl.RowCoderImpl([fast_coder_impl.BigIntCoderImpl()
                                                             for _ in range(field_count)])]
        self.check_cython_coder(python_field_coders, cython_field_coders, [data])


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
