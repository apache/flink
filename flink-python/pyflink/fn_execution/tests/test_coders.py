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

import pyarrow as pa
import pytz

from pyflink.fn_execution.coders import BigIntCoder, TinyIntCoder, BooleanCoder, \
    SmallIntCoder, IntCoder, FloatCoder, DoubleCoder, BinaryCoder, CharCoder, DateCoder, \
    TimeCoder, TimestampCoder, GenericArrayCoder, MapCoder, DecimalCoder, FlattenRowCoder, \
    RowCoder, LocalZonedTimestampCoder, BigDecimalCoder, TupleCoder, PrimitiveArrayCoder, \
    TimeWindowCoder, CountWindowCoder, InstantCoder
from pyflink.datastream.window import TimeWindow, CountWindow
from pyflink.testing.test_case_utils import PyFlinkTestCase


class ArrowCodersTests(unittest.TestCase):
    from pyflink.fn_execution import coder_impl_slow as implementation

    def test_pandas_ltz_decode_preserves_instants(self):
        import pandas as pd
        from pandas.testing import assert_series_equal
        from pyflink.table import DataTypes
        from pyflink.table.types import create_arrow_schema

        epochs = [-1, 1704067200123, 1730611800123, 1730615400123, None]
        row_type = DataTypes.ROW([DataTypes.FIELD('ltz', DataTypes.TIMESTAMP_LTZ(3))])
        schema = create_arrow_schema(row_type.field_names(), row_type.field_types())
        for zone in ['UTC', 'Asia/Shanghai', 'America/New_York']:
            timezone = pytz.timezone(zone)
            for size in [0, len(epochs)]:
                with self.subTest(timezone=zone, size=size):
                    batch = pa.record_batch(
                        [pa.array(epochs[:size], type=pa.timestamp('ms'))], schema=schema)
                    with pa.BufferOutputStream() as output:
                        with pa.ipc.new_stream(output, schema) as writer:
                            writer.write_batch(batch)
                        data = output.getvalue().to_pybytes()
                    coder = self.implementation.ArrowCoderImpl(schema, row_type, timezone)
                    result = coder.decode(data)[0]
                    expected = pd.Series(pd.to_datetime(epochs[:size], unit='ms', utc=True),
                                         name='ltz') \
                        .dt.tz_convert(timezone)
                    self.assertEqual(zone, str(result.dt.tz))
                    assert_series_equal(result, expected.astype(result.dtype))

    def test_pandas_ltz_encode_preserves_epoch_values(self):
        import pandas as pd
        from pyflink.table import DataTypes
        from pyflink.table.types import create_arrow_schema

        epochs = [-1, 1704067200123, 1730611800123, 1730615400123, None]
        row_type = DataTypes.ROW([DataTypes.FIELD('ltz', DataTypes.TIMESTAMP_LTZ(3))])
        schema = create_arrow_schema(row_type.field_names(), row_type.field_types())
        aware = pd.Series(pd.to_datetime(epochs, unit='ms', utc=True)) \
            .dt.tz_convert('Asia/Shanghai')
        naive = pd.Series(pd.to_datetime(epochs, unit='ms'))
        for zone in ['UTC', 'Asia/Shanghai', 'America/New_York']:
            for values in [aware, naive]:
                for size in [0, len(epochs)]:
                    with self.subTest(timezone=zone, dtype=values.dtype, size=size):
                        coder = self.implementation.ArrowCoderImpl(
                            schema, row_type, pytz.timezone(zone))
                        encoded = coder.encode([values.iloc[:size]])
                        result = pa.ipc.open_stream(encoded).read_all().column(0)
                        self.assertEqual(epochs[:size], result.cast(pa.int64()).to_pylist())


try:
    from pyflink.fn_execution import coder_impl_fast
except ImportError:
    coder_impl_fast = None


@unittest.skipIf(coder_impl_fast is None, "Compiled coders are not installed")
class FastArrowCodersTests(ArrowCodersTests):
    implementation = coder_impl_fast


class LocalZonedTimestampCodersTests(unittest.TestCase):
    from pyflink.fn_execution import coder_impl_slow as implementation

    def test_timestamp_before_epoch(self):
        import datetime
        import struct

        for precision, micros in [(3, 999000), (6, 999456)]:
            with self.subTest(precision=precision):
                coder = self.implementation.TimestampCoderImpl(precision)
                value = datetime.datetime(1969, 12, 31, 23, 59, 59, micros)
                data = struct.pack('>q', -1)
                if precision == 6:
                    data += struct.pack('>i', 456000)
                self.assertEqual(data, coder.encode(value))
                self.assertEqual(value, coder.decode(data))

    def test_decode_preserves_instants(self):
        import datetime
        import struct

        for zone in ['UTC', 'Asia/Shanghai', 'America/New_York']:
            for precision in [3, 6]:
                for millis in [-1001, -1000, -1, 0, 1, 1704067200123, 1710052200123, 1710055800123,
                               1730611800123, 1730615400123]:
                    with self.subTest(zone=zone, precision=precision, millis=millis):
                        nanos = 456000 if precision == 6 else 0
                        data = struct.pack('>q', millis)
                        if precision == 6:
                            data += struct.pack('>i', nanos)
                        coder = self.implementation.LocalZonedTimestampCoderImpl(
                            precision, pytz.timezone(zone))
                        result = coder.decode(data)
                        expected = datetime.datetime(1970, 1, 1, tzinfo=pytz.UTC) + \
                            datetime.timedelta(milliseconds=millis, microseconds=nanos // 1000)
                        self.assertEqual(expected, result)
                        self.assertEqual(zone, str(result.tzinfo))

    def test_encode_preserves_epoch_values(self):
        import datetime
        import struct

        for zone in ['UTC', 'Asia/Shanghai', 'America/New_York']:
            for precision in [3, 6]:
                for millis in [-1001, -1000, -1, 0, 1, 1704067200123, 1730611800123, 1730615400123]:
                    nanos = 456000 if precision == 6 else 0
                    instant = datetime.datetime(1970, 1, 1, tzinfo=pytz.UTC) + \
                        datetime.timedelta(milliseconds=millis, microseconds=nanos // 1000)
                    expected = struct.pack('>q', millis)
                    if precision == 6:
                        expected += struct.pack('>i', nanos)
                    coder = self.implementation.LocalZonedTimestampCoderImpl(
                        precision, pytz.timezone(zone))
                    for value in [instant.astimezone(pytz.timezone('Asia/Shanghai')),
                                  instant.astimezone(pytz.timezone('America/New_York')),
                                  instant.replace(tzinfo=None)]:
                        with self.subTest(zone=zone, precision=precision, value=value):
                            self.assertEqual(expected, coder.encode(value))


@unittest.skipIf(coder_impl_fast is None, "Compiled coders are not installed")
class FastLocalZonedTimestampCodersTests(LocalZonedTimestampCodersTests):
    implementation = coder_impl_fast


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

    def test_instant_coder(self):
        from pyflink.common.time import Instant

        coder = InstantCoder()
        self.check_coder(coder, Instant(100, 2000), None, Instant(-9223372036854775808, 0))

    def test_array_coder(self):
        element_coder = BigIntCoder()
        coder = GenericArrayCoder(element_coder)
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
        self.assertEqual(v, result)

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

        coder = RowCoder([BigIntCoder(), CharCoder()], ['f1', 'f0'])
        v = Row(f0="flink", f1=11)
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

    def test_coder_with_unmatched_type(self):
        from pyflink.common import Row
        coder = FlattenRowCoder([BigIntCoder()])
        with self.assertRaises(TypeError, msg='Expected list, got Row'):
            self.check_coder(coder, Row(1))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
