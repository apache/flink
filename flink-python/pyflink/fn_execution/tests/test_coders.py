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
import decimal
import logging
import unittest

from pyflink.fn_execution.coders import BigIntCoder, TinyIntCoder, BooleanCoder, \
    SmallIntCoder, IntCoder, FloatCoder, DoubleCoder, BinaryCoder, CharCoder, DateCoder, \
    TimeCoder, TimestampCoder, BasicArrayCoder, MapCoder, DecimalCoder, FlattenRowCoder, RowCoder, \
    LocalZonedTimestampCoder, BigDecimalCoder, TupleCoder, PrimitiveArrayCoder, TimeWindowCoder, \
    CountWindowCoder
from pyflink.datastream.window import TimeWindow, CountWindow
from pyflink.testing.test_case_utils import PyFlinkTestCase

try:
    from pyflink.fn_execution import coder_impl_fast  # noqa # pylint: disable=unused-import

    have_cython = True
except ImportError:
    have_cython = False


@unittest.skipIf(have_cython,
                 "Found cython implementation, we don't need to test non-compiled implementation")
class CodersTest(PyFlinkTestCase):

    def check_coder(self, coder, *values):
        coder_impl = coder.get_impl()
        for v in values:
            if isinstance(v, float):
                from pyflink.table.tests.test_udf import float_equal
                assert float_equal(v, coder_impl.decode(coder_impl.encode(v)), 1e-6)
            else:
                self.assertEqual(v, coder_impl.decode(coder_impl.encode(v)))

    # decide whether two floats are equal
    @staticmethod
    def float_equal(a, b, rel_tol=1e-09, abs_tol=0.0):
        return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)

    def test_bigint_coder(self):
        coder = BigIntCoder()
        self.check_coder(coder, 1, 100, -100, -1000)

    def test_tinyint_coder(self):
        coder = TinyIntCoder()
        self.check_coder(coder, 1, 10, 127, -128)

    def test_boolean_coder(self):
        coder = BooleanCoder()
        self.check_coder(coder, True, False)

    def test_smallint_coder(self):
        coder = SmallIntCoder()
        self.check_coder(coder, 32767, -32768, 0)

    def test_int_coder(self):
        coder = IntCoder()
        self.check_coder(coder, -2147483648, 2147483647)

    def test_float_coder(self):
        coder = FloatCoder()
        self.check_coder(coder, 1.02, 1.32)

    def test_double_coder(self):
        coder = DoubleCoder()
        self.check_coder(coder, -12.02, 1.98932)

    def test_binary_coder(self):
        coder = BinaryCoder()
        self.check_coder(coder, b'pyflink')

    def test_char_coder(self):
        coder = CharCoder()
        self.check_coder(coder, 'flink', '🐿')

    def test_date_coder(self):
        import datetime
        coder = DateCoder()
        self.check_coder(coder, datetime.date(2019, 9, 10))

    def test_time_coder(self):
        import datetime
        coder = TimeCoder()
        self.check_coder(coder, datetime.time(hour=11, minute=11, second=11, microsecond=123000))

    def test_timestamp_coder(self):
        import datetime
        coder = TimestampCoder(3)
        self.check_coder(coder, datetime.datetime(2019, 9, 10, 18, 30, 20, 123000))
        coder = TimestampCoder(6)
        self.check_coder(coder, datetime.datetime(2019, 9, 10, 18, 30, 20, 123456))

    def test_local_zoned_timestamp_coder(self):
        import datetime
        import pytz
        timezone = pytz.timezone("Asia/Shanghai")
        coder = LocalZonedTimestampCoder(3, timezone)
        self.check_coder(coder,
                         timezone.localize(datetime.datetime(2019, 9, 10, 18, 30, 20, 123000)))
        coder = LocalZonedTimestampCoder(6, timezone)
        self.check_coder(coder,
                         timezone.localize(datetime.datetime(2019, 9, 10, 18, 30, 20, 123456)))

    def test_array_coder(self):
        element_coder = BigIntCoder()
        coder = BasicArrayCoder(element_coder)
        self.check_coder(coder, [1, 2, 3, None])

    def test_primitive_array_coder(self):
        element_coder = CharCoder()
        coder = PrimitiveArrayCoder(element_coder)
        self.check_coder(coder, ['hi', 'hello', 'flink'])

    def test_map_coder(self):
        key_coder = CharCoder()
        value_coder = BigIntCoder()
        coder = MapCoder(key_coder, value_coder)
        self.check_coder(coder, {'flink': 1, 'pyflink': 2, 'coder': None})

    def test_decimal_coder(self):
        import decimal
        coder = DecimalCoder(38, 18)
        self.check_coder(coder, decimal.Decimal('0.00001'), decimal.Decimal('1.23E-8'))
        coder = DecimalCoder(4, 3)
        decimal.getcontext().prec = 2
        self.check_coder(coder, decimal.Decimal('1.001'))
        self.assertEqual(decimal.getcontext().prec, 2)

    def test_flatten_row_coder(self):
        field_coder = BigIntCoder()
        field_count = 10
        coder = FlattenRowCoder([field_coder for _ in range(field_count)]).get_impl()
        v = [None if i % 2 == 0 else i for i in range(field_count)]
        generator_result = coder.decode(coder.encode(v))
        result = []
        for item in generator_result:
            result.append(item)
        self.assertEqual([v], result)

    def test_row_coder(self):
        from pyflink.common import Row, RowKind
        field_coder = BigIntCoder()
        field_count = 10
        field_names = ['f{}'.format(i) for i in range(field_count)]
        coder = RowCoder([field_coder for _ in range(field_count)], field_names)
        v = Row(**{field_names[i]: None if i % 2 == 0 else i for i in range(field_count)})
        v.set_row_kind(RowKind.INSERT)
        self.check_coder(coder, v)
        v.set_row_kind(RowKind.UPDATE_BEFORE)
        self.check_coder(coder, v)
        v.set_row_kind(RowKind.UPDATE_AFTER)
        self.check_coder(coder, v)
        v.set_row_kind(RowKind.DELETE)
        self.check_coder(coder, v)

    def test_basic_decimal_coder(self):
        basic_dec_coder = BigDecimalCoder()
        value = decimal.Decimal(1.200)
        self.check_coder(basic_dec_coder, value)

    def test_tuple_coder(self):
        field_coders = [IntCoder(), CharCoder(), CharCoder()]
        tuple_coder = TupleCoder(field_coders=field_coders)
        data = (1, "Hello", "Hi")
        self.check_coder(tuple_coder, data)

    def test_window_coder(self):
        coder = TimeWindowCoder()
        self.check_coder(coder, TimeWindow(100, 1000))
        coder = CountWindowCoder()
        self.check_coder(coder, CountWindow(100))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
