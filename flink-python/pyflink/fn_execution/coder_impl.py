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
        out_stream.write_var_int64(len(value))
        for elem in value:
            self._elem_coder.encode_to_stream(elem, out_stream, nested)

    def decode_from_stream(self, in_stream, nested):
        size = in_stream.read_var_int64()
        assert size >= 0
        elements = [self._elem_coder.decode_from_stream(in_stream, nested) for _ in range(size)]
        return elements

    def __repr__(self):
        return 'ArrayCoderImpl[%s]' % str(self._elem_coder)


class MapCoderImpl(StreamCoderImpl):

    def __init__(self, key_coder, value_coder):
        self._key_coder = key_coder
        self._value_coder = value_coder

    def encode_to_stream(self, map_value, out_stream, nested):
        out_stream.write_bigendian_int32(len(map_value))
        for key in map_value:
            self._key_coder.encode_to_stream(key, out_stream, nested)
            self._value_coder.encode_to_stream(map_value[key], out_stream, nested)

    def decode_from_stream(self, in_stream, nested):
        size = in_stream.read_bigendian_int32()
        assert size >= 0
        map_value = {}
        for _ in range(size):
            key = self._key_coder.decode_from_stream(in_stream, nested)
            value = self._value_coder.decode_from_stream(in_stream, nested)
            map_value[key] = value
        return map_value

    def __repr__(self):
        return 'MapCoderImpl[%s]' % ' : '.join([str(self._key_coder), str(self._value_coder)])


class MultiSetCoderImpl(StreamCoderImpl):

    def __init__(self, element_coder):
        self._element_coder = element_coder

    def encode_to_stream(self, value, out_stream, nested):
        dict_value = self.multiset_to_dict(value)
        out_stream.write_bigendian_int32(len(dict_value))
        for key in dict_value:
            self._element_coder.encode_to_stream(key, out_stream, nested)
            out_stream.write_var_int64(dict_value[key])

    def decode_from_stream(self, in_stream, nested):
        size = in_stream.read_bigendian_int32()
        assert size >= 0
        multiset_value = []
        for _ in range(size):
            element = self._element_coder.decode_from_stream(in_stream, nested)
            count = in_stream.read_var_int64()
            for i in range(count):
                multiset_value.append(element)
        return multiset_value

    def multiset_to_dict(self, multiset):
        dict_value = {}
        for ele in multiset:
            if ele in dict_value:
                dict_value[ele] += 1
            else:
                dict_value[ele] = 1
        return dict_value

    def __repr__(self):
        return 'MultiSetCoderImpl[%s]' % str(self._element_coder)


class ByteCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write_byte(value)

    def decode_from_stream(self, in_stream, nested):
        return int(in_stream.read_byte())

    def __repr__(self):
        return 'ByteCoderImpl'


class BooleanCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write_byte(value)

    def decode_from_stream(self, in_stream, nested):
        return not not in_stream.read_byte()

    def __repr__(self):
        return 'BooleanCoderImpl'


class ShortCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write(struct.pack('>h', value))

    def decode_from_stream(self, in_stream, nested):
        return struct.unpack('>h', in_stream.read(2))[0]

    def __repr__(self):
        return 'ShortCoderImpl'


class FloatCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write(struct.pack('>f', value))

    def decode_from_stream(self, in_stream, nested):
        return struct.unpack('>f', in_stream.read(4))[0]

    def __repr__(self):
        return 'FloatCoderImpl'


class DoubleCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write_bigendian_double(value)

    def decode_from_stream(self, in_stream, nested):
        return in_stream.read_bigendian_double()

    def __repr__(self):
        return 'DoubleCoderImpl'


class DecimalCoderImpl(StreamCoderImpl):

    def __init__(self, str_coder):
        self._str_coder = str_coder

    def encode_to_stream(self, value, out_stream, nested):
        self._str_coder.encode_to_stream(str(value), out_stream, True)

    def decode_from_stream(self, in_stream, nested):
        return decimal.Decimal(self._str_coder.decode_from_stream(in_stream, True))

    def __repr__(self):
        return 'DecimalCoderImpl'


class DateCoderImpl(StreamCoderImpl):

    EPOCH_ORDINAL = datetime.datetime(1970, 1, 1).toordinal()

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write_var_int64(self.date_to_internal(value))

    def decode_from_stream(self, in_stream, nested):
        value = in_stream.read_var_int64()
        return self.internal_to_date(value)

    def date_to_internal(self, d):
        return d.toordinal() - self.EPOCH_ORDINAL

    def internal_to_date(self, v):
        return datetime.date.fromordinal(v + self.EPOCH_ORDINAL)

    def __repr__(self):
        return "DateCoderImpl"


class TimeCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write_var_int64(self.time_to_internal(value))

    def decode_from_stream(self, in_stream, nested):
        value = in_stream.read_var_int64()
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

    def __repr__(self):
        return "TimeCoderImpl"


class DateTimeCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write_var_int64(self.timestamp_to_internal(value))

    def decode_from_stream(self, in_stream, nested):
        value = in_stream.read_var_int64()
        return self.internal_to_timestamp(value)

    def timestamp_to_internal(self, timestamp):
        from datetime import timezone
        return int(timestamp.replace(tzinfo=timezone.utc).timestamp() * 1000)

    def internal_to_timestamp(self, v):
        return datetime.datetime.utcfromtimestamp(v // 1000).replace(microsecond=v % 1000 * 1000)

    def __repr__(self):
        return "DateTimeCoderImpl"


class BinaryCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write(value, True)

    def decode_from_stream(self, in_stream, nested):
        return in_stream.read_all(True)

    def __repr__(self):
        return "BinaryCoderImpl"


class StringCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write(value.encode("utf-8"), True)

    def decode_from_stream(self, in_stream, nested):
        return in_stream.read_all(True).decode("utf-8")

    def __repr__(self):
        return "StringCoderImpl"
