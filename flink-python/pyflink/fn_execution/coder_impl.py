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

import datetime
import decimal
import struct
from apache_beam.coders.coder_impl import StreamCoderImpl


class RowCoderImpl(StreamCoderImpl):

    def __init__(self, field_coders):
        self._field_coders = field_coders

    def encode_to_stream(self, value, out_stream, nested):
        self.write_null_mask(value, out_stream)
        for i in range(len(self._field_coders)):
            if value[i] is not None:
                self._field_coders[i].encode_to_stream(value[i], out_stream, nested)

    def decode_from_stream(self, in_stream, nested):
        from pyflink.table import Row
        null_mask = self.read_null_mask(len(self._field_coders), in_stream)
        assert len(null_mask) == len(self._field_coders)
        return Row(*[None if null_mask[idx] else self._field_coders[idx].decode_from_stream(
            in_stream, nested) for idx in range(0, len(null_mask))])

    @staticmethod
    def write_null_mask(value, out_stream):
        field_pos = 0
        field_count = len(value)
        while field_pos < field_count:
            b = 0x00
            # set bits in byte
            num_pos = min(8, field_count - field_pos)
            byte_pos = 0
            while byte_pos < num_pos:
                b = b << 1
                # set bit if field is null
                if value[field_pos + byte_pos] is None:
                    b |= 0x01
                byte_pos += 1
            field_pos += num_pos
            # shift bits if last byte is not completely filled
            b <<= (8 - byte_pos)
            # write byte
            out_stream.write_byte(b)

    @staticmethod
    def read_null_mask(field_count, in_stream):
        null_mask = []
        field_pos = 0
        while field_pos < field_count:
            b = in_stream.read_byte()
            num_pos = min(8, field_count - field_pos)
            byte_pos = 0
            while byte_pos < num_pos:
                null_mask.append((b & 0x80) > 0)
                b = b << 1
                byte_pos += 1
            field_pos += num_pos
        return null_mask

    def __repr__(self):
        return 'RowCoderImpl[%s]' % ', '.join(str(c) for c in self._field_coders)


class ArrayCoderImpl(StreamCoderImpl):

    def __init__(self, elem_coder):
        self._elem_coder = elem_coder

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write_bigendian_int32(len(value))
        for elem in value:
            if elem is None:
                out_stream.write_byte(False)
            else:
                out_stream.write_byte(True)
                self._elem_coder.encode_to_stream(elem, out_stream, nested)

    def decode_from_stream(self, in_stream, nested):
        size = in_stream.read_bigendian_int32()
        elements = [self._elem_coder.decode_from_stream(in_stream, nested)
                    if in_stream.read_byte() else None for _ in range(size)]
        return elements

    def __repr__(self):
        return 'ArrayCoderImpl[%s]' % repr(self._elem_coder)


class MapCoderImpl(StreamCoderImpl):

    def __init__(self, key_coder, value_coder):
        self._key_coder = key_coder
        self._value_coder = value_coder

    def encode_to_stream(self, map_value, out_stream, nested):
        out_stream.write_bigendian_int32(len(map_value))
        for key in map_value:
            self._key_coder.encode_to_stream(key, out_stream, nested)
            value = map_value[key]
            if value is None:
                out_stream.write_byte(True)
            else:
                out_stream.write_byte(False)
                self._value_coder.encode_to_stream(map_value[key], out_stream, nested)

    def decode_from_stream(self, in_stream, nested):
        size = in_stream.read_bigendian_int32()
        map_value = {}
        for _ in range(size):
            key = self._key_coder.decode_from_stream(in_stream, nested)
            is_null = in_stream.read_byte()
            if is_null:
                map_value[key] = None
            else:
                value = self._value_coder.decode_from_stream(in_stream, nested)
                map_value[key] = value
        return map_value

    def __repr__(self):
        return 'MapCoderImpl[%s]' % ' : '.join([repr(self._key_coder), repr(self._value_coder)])


class BigIntCoderImpl(StreamCoderImpl):
    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write_bigendian_int64(value)

    def decode_from_stream(self, in_stream, nested):
        return in_stream.read_bigendian_int64()


class TinyIntCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write(struct.pack('b', value))

    def decode_from_stream(self, in_stream, nested):
        return struct.unpack('b', in_stream.read(1))[0]


class SmallIntImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write(struct.pack('>h', value))

    def decode_from_stream(self, in_stream, nested):
        return struct.unpack('>h', in_stream.read(2))[0]


class IntCoderImpl(StreamCoderImpl):
    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write_bigendian_int32(value)

    def decode_from_stream(self, in_stream, nested):
        return in_stream.read_bigendian_int32()


class BooleanCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write_byte(value)

    def decode_from_stream(self, in_stream, nested):
        return not not in_stream.read_byte()


class FloatCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write(struct.pack('>f', value))

    def decode_from_stream(self, in_stream, nested):
        return struct.unpack('>f', in_stream.read(4))[0]


class DoubleCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write_bigendian_double(value)

    def decode_from_stream(self, in_stream, nested):
        return in_stream.read_bigendian_double()


class DecimalCoderImpl(StreamCoderImpl):

    def __init__(self, precision, scale):
        self.context = decimal.Context(prec=precision)
        self.scale_format = decimal.Decimal(10) ** -scale

    def encode_to_stream(self, value, out_stream, nested):
        user_context = decimal.getcontext()
        decimal.setcontext(self.context)
        value = value.quantize(self.scale_format)
        bytes_value = str(value).encode("utf-8")
        out_stream.write_bigendian_int32(len(bytes_value))
        out_stream.write(bytes_value, False)
        decimal.setcontext(user_context)

    def decode_from_stream(self, in_stream, nested):
        user_context = decimal.getcontext()
        decimal.setcontext(self.context)
        size = in_stream.read_bigendian_int32()
        value = decimal.Decimal(in_stream.read(size).decode("utf-8")).quantize(self.scale_format)
        decimal.setcontext(user_context)
        return value


class BinaryCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write_bigendian_int32(len(value))
        out_stream.write(value, False)

    def decode_from_stream(self, in_stream, nested):
        size = in_stream.read_bigendian_int32()
        return in_stream.read(size)


class CharCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        bytes_value = value.encode("utf-8")
        out_stream.write_bigendian_int32(len(bytes_value))
        out_stream.write(bytes_value, False)

    def decode_from_stream(self, in_stream, nested):
        size = in_stream.read_bigendian_int32()
        return in_stream.read(size).decode("utf-8")


class DateCoderImpl(StreamCoderImpl):
    EPOCH_ORDINAL = datetime.datetime(1970, 1, 1).toordinal()

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write_bigendian_int32(self.date_to_internal(value))

    def decode_from_stream(self, in_stream, nested):
        value = in_stream.read_bigendian_int32()
        return self.internal_to_date(value)

    def date_to_internal(self, d):
        return d.toordinal() - self.EPOCH_ORDINAL

    def internal_to_date(self, v):
        return datetime.date.fromordinal(v + self.EPOCH_ORDINAL)


class TimeCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write_bigendian_int32(self.time_to_internal(value))

    def decode_from_stream(self, in_stream, nested):
        value = in_stream.read_bigendian_int32()
        return self.internal_to_time(value)

    def time_to_internal(self, t):
        milliseconds = (t.hour * 3600000
                        + t.minute * 60000
                        + t.second * 1000
                        + t.microsecond // 1000)
        return milliseconds

    def internal_to_time(self, v):
        seconds, milliseconds = divmod(v, 1000)
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        return datetime.time(hours, minutes, seconds, milliseconds * 1000)


class TimestampCoderImpl(StreamCoderImpl):

    def __init__(self, precision):
        self.precision = precision

    def is_compact(self):
        return self.precision <= 3

    def encode_to_stream(self, value, out_stream, nested):
        milliseconds, nanoseconds = self.timestamp_to_internal(value)
        if self.is_compact():
            assert nanoseconds == 0
            out_stream.write_bigendian_int64(milliseconds)
        else:
            out_stream.write_bigendian_int64(milliseconds)
            out_stream.write_bigendian_int32(nanoseconds)

    def decode_from_stream(self, in_stream, nested):
        if self.is_compact():
            milliseconds = in_stream.read_bigendian_int64()
            nanoseconds = 0
        else:
            milliseconds = in_stream.read_bigendian_int64()
            nanoseconds = in_stream.read_bigendian_int32()
        return self.internal_to_timestamp(milliseconds, nanoseconds)

    def timestamp_to_internal(self, timestamp):
        seconds = int(timestamp.replace(tzinfo=datetime.timezone.utc).timestamp())
        microseconds_of_second = timestamp.microsecond
        milliseconds = seconds * 1000 + microseconds_of_second // 1000
        nanoseconds = microseconds_of_second % 1000 * 1000
        return milliseconds, nanoseconds

    def internal_to_timestamp(self, milliseconds, nanoseconds):
        second, microsecond = (milliseconds // 1000,
                               milliseconds % 1000 * 1000 + nanoseconds // 1000)
        return datetime.datetime.utcfromtimestamp(second).replace(microsecond=microsecond)
