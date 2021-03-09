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

from pyflink.fn_execution.window import TimeWindow, CountWindow
from pyflink.testing.test_case_utils import PyFlinkTestCase

try:
    from pyflink.fn_execution import coder_impl_fast
    from pyflink.fn_execution.beam import beam_coder_impl_slow as coder_impl

    have_cython = True
except ImportError:
    have_cython = False


@unittest.skipUnless(have_cython, "Uncompiled Cython Coder")
class CodersTest(PyFlinkTestCase):

    def check_cython_coder(self, python_field_coders, cython_field_coders, data):
        from apache_beam.coders.coder_impl import create_InputStream, create_OutputStream
        from pyflink.fn_execution.beam.beam_stream import BeamInputStream, BeamOutputStream
        py_flatten_row_coder = coder_impl.FlattenRowCoderImpl(python_field_coders)
        internal = py_flatten_row_coder.encode(data)
        beam_input_stream = create_InputStream(internal)
        input_stream = BeamInputStream(beam_input_stream, beam_input_stream.size())
        beam_output_stream = create_OutputStream()
        cy_flatten_row_coder = coder_impl_fast.FlattenRowCoderImpl(cython_field_coders)
        value = cy_flatten_row_coder.decode_from_stream(input_stream)
        output_stream = BeamOutputStream(beam_output_stream)
        cy_flatten_row_coder.encode_to_stream(value, output_stream)
        output_stream.flush()
        generator_result = py_flatten_row_coder.decode_from_stream(create_InputStream(
            beam_output_stream.get()), False)
        result = []
        for item in generator_result:
            result.append(item)
        try:
            self.assertEqual(result, [data])
        except AssertionError:
            data = [data]
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
        cython_field_coders = [coder_impl_fast.BigIntCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_tinyint_coder(self):
        data = [1, 10, 127, -128]
        python_field_coders = [coder_impl.TinyIntCoderImpl() for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.TinyIntCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_boolean_coder(self):
        data = [True, False]
        python_field_coders = [coder_impl.BooleanCoderImpl() for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.BooleanCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_smallint_coder(self):
        data = [32767, -32768, 0]
        python_field_coders = [coder_impl.SmallIntCoderImpl() for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.SmallIntCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_int_coder(self):
        data = [-2147483648, 2147483647]
        python_field_coders = [coder_impl.IntCoderImpl() for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.IntCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_float_coder(self):
        data = [1.02, 1.32]
        python_field_coders = [coder_impl.FloatCoderImpl() for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.FloatCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_double_coder(self):
        data = [-12.02, 1.98932]
        python_field_coders = [coder_impl.DoubleCoderImpl() for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.DoubleCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_binary_coder(self):
        data = [b'pyflink', b'x\x00\x00\x00']
        python_field_coders = [coder_impl.BinaryCoderImpl() for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.BinaryCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_char_coder(self):
        data = ['flink', '🐿']
        python_field_coders = [coder_impl.CharCoderImpl() for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.CharCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_date_coder(self):
        import datetime
        data = [datetime.date(2019, 9, 10)]
        python_field_coders = [coder_impl.DateCoderImpl() for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.DateCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_time_coder(self):
        import datetime
        data = [datetime.time(hour=11, minute=11, second=11, microsecond=123000)]
        python_field_coders = [coder_impl.TimeCoderImpl() for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.TimeCoderImpl() for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_decimal_coder(self):
        import decimal
        data = [decimal.Decimal('0.00001'), decimal.Decimal('1.23E-8')]
        python_field_coders = [coder_impl.DecimalCoderImpl(38, 18) for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.DecimalCoderImpl(38, 18) for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

        decimal.getcontext().prec = 2
        data = [decimal.Decimal('1.001')]
        python_field_coders = [coder_impl.DecimalCoderImpl(4, 3) for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.DecimalCoderImpl(4, 3) for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)
        self.assertEqual(decimal.getcontext().prec, 2)

    def test_cython_timestamp_coder(self):
        import datetime

        data = [datetime.datetime(2019, 9, 10, 18, 30, 20, 123000)]
        python_field_coders = [coder_impl.TimestampCoderImpl(3) for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.TimestampCoderImpl(3) for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

        data = [datetime.datetime(2019, 9, 10, 18, 30, 20, 123456)]
        python_field_coders = [coder_impl.TimestampCoderImpl(6) for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.TimestampCoderImpl(6) for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_local_zoned_timestamp_coder(self):
        import datetime
        import pytz
        timezone = pytz.timezone("Asia/Shanghai")
        data = [timezone.localize(datetime.datetime(2019, 9, 10, 18, 30, 20, 123000))]
        python_field_coders = [coder_impl.LocalZonedTimestampCoderImpl(3, timezone)
                               for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.LocalZonedTimestampCoderImpl(3, timezone)
                               for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

        data = [timezone.localize(datetime.datetime(2019, 9, 10, 18, 30, 20, 123456))]
        python_field_coders = [coder_impl.LocalZonedTimestampCoderImpl(6, timezone)
                               for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.LocalZonedTimestampCoderImpl(6, timezone)
                               for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_array_coder(self):
        data = [[1, 2, 3, None]]
        python_field_coders = [coder_impl.BasicArrayCoderImpl(coder_impl.BigIntCoderImpl())
                               for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.BasicArrayCoderImpl(
            coder_impl_fast.BigIntCoderImpl()) for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_primitive_array_coder(self):
        data = [[1, 2, 3, 4]]
        python_field_coders = [coder_impl.PrimitiveArrayCoderImpl(coder_impl.BigIntCoderImpl())
                               for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.PrimitiveArrayCoderImpl(
            coder_impl_fast.BigIntCoderImpl()) for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_map_coder(self):
        data = [{'flink': 1, 'pyflink': 2, 'coder': None}]
        python_field_coders = [coder_impl.MapCoderImpl(coder_impl.CharCoderImpl(),
                                                       coder_impl.BigIntCoderImpl())
                               for _ in range(len(data))]
        cython_field_coders = [coder_impl_fast.MapCoderImpl(coder_impl_fast.CharCoderImpl(),
                                                            coder_impl_fast.BigIntCoderImpl())
                               for _ in range(len(data))]
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_row_coder(self):
        from pyflink.common import Row, RowKind
        field_count = 15
        field_names = ['f{}'.format(i) for i in range(field_count)]
        row = Row(**{field_names[i]: None if i % 2 == 0 else i for i in range(field_count)})
        data = [row]
        python_field_coders = [coder_impl.RowCoderImpl([coder_impl.BigIntCoderImpl()
                                                        for _ in range(field_count)],
                                                       row._fields)]
        cython_field_coders = [coder_impl_fast.RowCoderImpl([coder_impl_fast.BigIntCoderImpl()
                                                             for _ in range(field_count)],
                                                            row._fields)]
        row.set_row_kind(RowKind.INSERT)
        self.check_cython_coder(python_field_coders, cython_field_coders, data)
        row.set_row_kind(RowKind.UPDATE_BEFORE)
        self.check_cython_coder(python_field_coders, cython_field_coders, data)
        row.set_row_kind(RowKind.UPDATE_AFTER)
        self.check_cython_coder(python_field_coders, cython_field_coders, data)
        row.set_row_kind(RowKind.DELETE)
        self.check_cython_coder(python_field_coders, cython_field_coders, data)

    def test_cython_time_window_coder(self):
        fast_coder = coder_impl_fast.TimeWindowCoderImpl()
        slow_coder = coder_impl.TimeWindowCoderImpl()
        window = TimeWindow(100, 200)
        self.assertEqual(fast_coder.encode_nested(window), slow_coder.encode_nested(window))

    def test_cython_count_window_coder(self):
        fast_coder = coder_impl_fast.CountWindowCoderImpl()
        slow_coder = coder_impl.CountWindowCoderImpl()
        window = CountWindow(100)
        self.assertEqual(fast_coder.encode_nested(window), slow_coder.encode_nested(window))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
