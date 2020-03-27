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
from typing import Any
from typing import Generator
from typing import List

import pyarrow as pa
from apache_beam.coders.coder_impl import StreamCoderImpl, create_InputStream, create_OutputStream

from pyflink.fn_execution.ResettableIO import ResettableIO
from pyflink.table.types import Row


class FlattenRowCoderImpl(StreamCoderImpl):

    def __init__(self, field_coders):
        self._field_coders = field_coders
        self._field_count = len(field_coders)
        self._leading_complete_bytes_num = self._field_count // 8
        self._remaining_bits_num = self._field_count % 8
        self.null_mask_search_table = self.generate_null_mask_search_table()
        self.null_byte_search_table = (0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01)
        self.data_out_stream = create_OutputStream()

    @staticmethod
    def generate_null_mask_search_table():
        """
        Each bit of one byte represents if the column at the corresponding position is None or not,
        e.g. 0x84 represents the first column and the sixth column are None.
        """
        null_mask = []
        for b in range(256):
            every_num_null_mask = [(b & 0x80) > 0, (b & 0x40) > 0, (b & 0x20) > 0, (b & 0x10) > 0,
                                   (b & 0x08) > 0, (b & 0x04) > 0, (b & 0x02) > 0, (b & 0x01) > 0]
            null_mask.append(tuple(every_num_null_mask))

        return tuple(null_mask)

    def encode_to_stream(self, iter_value, out_stream, nested):
        field_coders = self._field_coders
        data_out_stream = self.data_out_stream
        for value in iter_value:
            self._write_null_mask(value, data_out_stream)
            for i in range(self._field_count):
                item = value[i]
                if item is not None:
                    field_coders[i].encode_to_stream(item, data_out_stream, nested)
            out_stream.write_var_int64(data_out_stream.size())
            out_stream.write(data_out_stream.get())
            data_out_stream._clear()

    def decode_from_stream(self, in_stream, nested):
        while in_stream.size() > 0:
            in_stream.read_var_int64()
            yield self._decode_one_row_from_stream(in_stream, nested)

    def _decode_one_row_from_stream(self, in_stream: create_InputStream, nested: bool) -> List:
        null_mask = self._read_null_mask(in_stream)
        return [None if null_mask[idx] else self._field_coders[idx].decode_from_stream(
            in_stream, nested) for idx in range(0, self._field_count)]

    def _write_null_mask(self, value, out_stream):
        field_pos = 0
        null_byte_search_table = self.null_byte_search_table
        remaining_bits_num = self._remaining_bits_num
        for _ in range(self._leading_complete_bytes_num):
            b = 0x00
            for i in range(0, 8):
                if value[field_pos + i] is None:
                    b |= null_byte_search_table[i]
            field_pos += 8
            out_stream.write_byte(b)

        if remaining_bits_num:
            b = 0x00
            for i in range(remaining_bits_num):
                if value[field_pos + i] is None:
                    b |= null_byte_search_table[i]
            out_stream.write_byte(b)

    def _read_null_mask(self, in_stream):
        null_mask = []
        null_mask_search_table = self.null_mask_search_table
        remaining_bits_num = self._remaining_bits_num
        for _ in range(self._leading_complete_bytes_num):
            b = in_stream.read_byte()
            null_mask.extend(null_mask_search_table[b])

        if remaining_bits_num:
            b = in_stream.read_byte()
            null_mask.extend(null_mask_search_table[b][0:remaining_bits_num])
        return null_mask

    def __repr__(self):
        return 'FlattenRowCoderImpl[%s]' % ', '.join(str(c) for c in self._field_coders)


class RowCoderImpl(FlattenRowCoderImpl):

    def __init__(self, field_coders):
        super(RowCoderImpl, self).__init__(field_coders)

    def encode_to_stream(self, value, out_stream, nested):
        field_coders = self._field_coders
        self._write_null_mask(value, out_stream)
        for i in range(self._field_count):
            item = value[i]
            if item is not None:
                field_coders[i].encode_to_stream(item, out_stream, nested)

    def decode_from_stream(self, in_stream, nested):
        return Row(*self._decode_one_row_from_stream(in_stream, nested))

    def __repr__(self):
        return 'RowCoderImpl[%s]' % ', '.join(str(c) for c in self._field_coders)


class TableFunctionRowCoderImpl(StreamCoderImpl):

    def __init__(self, flatten_row_coder):
        self._flatten_row_coder = flatten_row_coder
        self._field_count = flatten_row_coder._field_count

    def encode_to_stream(self, iter_value, out_stream, nested):
        for value in iter_value:
            if value:
                if self._field_count == 1:
                    value = self._create_tuple_result(value)
                self._flatten_row_coder.encode_to_stream(value, out_stream, nested)
            out_stream.write_var_int64(1)
            out_stream.write_byte(0x00)

    def decode_from_stream(self, in_stream, nested):
        return self._flatten_row_coder.decode_from_stream(in_stream, nested)

    @staticmethod
    def _create_tuple_result(value: List) -> Generator:
        for result in value:
            yield (result,)

    def __repr__(self):
        return 'TableFunctionRowCoderImpl[%s]' % repr(self._flatten_row_coder)


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


class LocalZonedTimestampCoderImpl(TimestampCoderImpl):

    def __init__(self, precision, timezone):
        super(LocalZonedTimestampCoderImpl, self).__init__(precision)
        self.timezone = timezone

    def internal_to_timestamp(self, milliseconds, nanoseconds):
        return self.timezone.localize(
            super(LocalZonedTimestampCoderImpl, self).internal_to_timestamp(
                milliseconds, nanoseconds))


class ArrowCoderImpl(StreamCoderImpl):

    def __init__(self, schema):
        self._schema = schema
        self._resettable_io = ResettableIO()
        self._batch_reader = ArrowCoderImpl._load_from_stream(self._resettable_io)
        self._batch_writer = pa.RecordBatchStreamWriter(self._resettable_io, self._schema)
        self.data_out_stream = create_OutputStream()
        self._resettable_io.set_output_stream(self.data_out_stream)

    def encode_to_stream(self, iter_cols, out_stream, nested):
        data_out_stream = self.data_out_stream
        for cols in iter_cols:
            self._batch_writer.write_batch(self._create_batch(cols))
            out_stream.write_var_int64(data_out_stream.size())
            out_stream.write(data_out_stream.get())
            data_out_stream._clear()

    def decode_from_stream(self, in_stream, nested):
        while in_stream.size() > 0:
            yield self._decode_one_batch_from_stream(in_stream)

    @staticmethod
    def _load_from_stream(stream):
        reader = pa.ipc.open_stream(stream)
        for batch in reader:
            yield batch

    def _create_batch(self, cols):
        def create_array(s, t):
            try:
                return pa.Array.from_pandas(s, mask=s.isnull(), type=t)
            except pa.ArrowException as e:
                error_msg = "Exception thrown when converting pandas.Series (%s) to " \
                            "pyarrow.Array (%s)."
                raise RuntimeError(error_msg % (s.dtype, t), e)

        arrays = [create_array(cols[i], self._schema.types[i]) for i in range(0, len(self._schema))]
        return pa.RecordBatch.from_arrays(arrays, self._schema)

    def _decode_one_batch_from_stream(self, in_stream: create_InputStream) -> List:
        self._resettable_io.set_input_bytes(in_stream.read_all(True))
        # there is only one arrow batch in the underlying input stream
        table = pa.Table.from_batches([next(self._batch_reader)])
        return [c.to_pandas(date_as_object=True) for c in table.itercolumns()]

    def __repr__(self):
        return 'ArrowCoderImpl[%s]' % self._schema


class PassThroughLengthPrefixCoderImpl(StreamCoderImpl):
    def __init__(self, value_coder):
        self._value_coder = value_coder

    def encode_to_stream(self, value, out: create_OutputStream, nested: bool) -> Any:
        self._value_coder.encode_to_stream(value, out, nested)

    def decode_from_stream(self, in_stream: create_InputStream, nested: bool) -> Any:
        return self._value_coder.decode_from_stream(in_stream, nested)

    def get_estimated_size_and_observables(self, value: Any, nested=False):
        return 0, []

    def __repr__(self):
        return 'PassThroughLengthPrefixCoderImpl[%s]' % self._value_coder
