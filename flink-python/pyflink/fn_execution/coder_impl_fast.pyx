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
# cython: language_level = 3
# cython: infer_types = True
# cython: profile=True
# cython: boundscheck=False, wraparound=False, initializedcheck=False, cdivision=True
from libc.stdint cimport int32_t, int64_t
from libc.stdlib cimport free, malloc

import datetime
import decimal
import pickle
from typing import List, Union

import cloudpickle
import avro.schema as avro_schema

from pyflink.common import Row, RowKind
from pyflink.common.time import Instant
from pyflink.datastream.window import CountWindow, TimeWindow, GlobalWindow
from pyflink.fn_execution.formats.avro import FlinkAvroDecoder, FlinkAvroDatumReader, \
    FlinkAvroBufferWrapper, FlinkAvroEncoder, FlinkAvroDatumWriter
from pyflink.fn_execution.ResettableIO import ResettableIO
from pyflink.table.utils import pandas_to_arrow, arrow_to_pandas

ROW_KIND_BIT_SIZE = 2

cdef class InternalRow:
    def __cinit__(self, list values, InternalRowKind row_kind):
        self.values = values
        self.row_kind = row_kind
        self.field_names = []

    cpdef object to_row(self):
        row = Row()
        row._values = self.values
        row.set_field_names(self.field_names)
        row.set_row_kind(RowKind(self.row_kind))
        return row

    @staticmethod
    def from_row(row: Row) -> InternalRow:
        cdef InternalRow internal_row
        internal_row = InternalRow(row._values, row.get_row_kind().value)
        internal_row.field_names = row._fields
        return internal_row

    cdef bint is_retract_msg(self):
        return self.row_kind == InternalRowKind.UPDATE_BEFORE or \
           self.row_kind == InternalRowKind.DELETE

    cdef bint is_accumulate_msg(self):
        return self.row_kind == InternalRowKind.UPDATE_AFTER or \
           self.row_kind == InternalRowKind.INSERT

    def __eq__(self, other):
        if not other:
            return False
        return self.values == other.values

    def __getitem__(self, item):
        return self.values[item]

    def __iter__(self):
        return iter(self.values)

    def __repr__(self):
        return "InternalRow(%s, %s)" % (self.row_kind, self.values)

cdef class MaskUtils:
    """
    A util class used to encode mask value.
    """

    def __cinit__(self, field_count):
        self._mask = <bint*> malloc((field_count + ROW_KIND_BIT_SIZE) * sizeof(bint))
        if self._mask == NULL:
            raise MemoryError()

        self._mask_byte_search_table = <unsigned char*> malloc(8 * sizeof(unsigned char))
        if self._mask_byte_search_table == NULL:
            raise MemoryError()
        self._mask_byte_search_table[0] = 0x80
        self._mask_byte_search_table[1] = 0x40
        self._mask_byte_search_table[2] = 0x20
        self._mask_byte_search_table[3] = 0x10
        self._mask_byte_search_table[4] = 0x08
        self._mask_byte_search_table[5] = 0x04
        self._mask_byte_search_table[6] = 0x02
        self._mask_byte_search_table[7] = 0x01

        self._row_kind_byte_table = <unsigned char*> malloc(8 * sizeof(unsigned char))
        if self._row_kind_byte_table == NULL:
            raise MemoryError()
        self._row_kind_byte_table[0] = 0x00
        self._row_kind_byte_table[1] = 0x80
        self._row_kind_byte_table[2] = 0x40
        self._row_kind_byte_table[3] = 0xC0

    def __init__(self, field_count):
        self._field_count = field_count
        self._leading_complete_bytes_num = (self._field_count + ROW_KIND_BIT_SIZE) // 8
        self._remaining_bits_num = (self._field_count + ROW_KIND_BIT_SIZE) % 8

    cdef void write_mask(self, list value, unsigned char row_kind_value,
                         OutputStream output_stream):
        cdef size_t field_pos, index, i
        cdef unsigned char*bit_map_byte_search_table
        cdef unsigned char b
        field_pos = 0
        bit_map_byte_search_table = self._mask_byte_search_table

        # first byte contains the row kind bits
        b = self._row_kind_byte_table[row_kind_value]
        for i in range(0, 8 - ROW_KIND_BIT_SIZE):
            if field_pos + i < self._field_count and value[field_pos + i] is None:
                b |= bit_map_byte_search_table[i + ROW_KIND_BIT_SIZE]
        field_pos += 8 - ROW_KIND_BIT_SIZE
        output_stream.write_byte(b)

        for _ in range(1, self._leading_complete_bytes_num):
            b = 0x00
            for i in range(8):
                if value[field_pos + i] is None:
                    b |= bit_map_byte_search_table[i]
            field_pos += 8
            output_stream.write_byte(b)

        if self._leading_complete_bytes_num >= 1 and self._remaining_bits_num:
            b = 0x00
            for i in range(self._remaining_bits_num):
                if value[field_pos + i] is None:
                    b |= bit_map_byte_search_table[i]
            output_stream.write_byte(b)

    cdef bint*read_mask(self, InputStream input_stream):
        cdef size_t field_pos, i
        cdef unsigned char b
        field_pos = 0
        for _ in range(self._leading_complete_bytes_num):
            b = input_stream.read_byte()
            for i in range(8):
                self._mask[field_pos] = (b & self._mask_byte_search_table[i]) > 0
                field_pos += 1

        if self._remaining_bits_num:
            b = input_stream.read_byte()
            for i in range(self._remaining_bits_num):
                self._mask[field_pos] = (b & self._mask_byte_search_table[i]) > 0
                field_pos += 1
        return self._mask

    def __dealloc__(self):
        if self._mask != NULL:
            free(self._mask)
        if self._mask_byte_search_table != NULL:
            free(self._mask_byte_search_table)
        if self._row_kind_byte_table != NULL:
            free(self._row_kind_byte_table)

cdef class LengthPrefixBaseCoderImpl:
    """
    LengthPrefixBaseCoderImpl will be used in Operations and other coders will be the field coder of
    LengthPrefixBaseCoderImpl.
    """

    def __init__(self, field_coder: FieldCoderImpl):
        self._field_coder = field_coder
        self._data_out_stream = OutputStream()

    cpdef encode_to_stream(self, value, LengthPrefixOutputStream output_stream):
        pass

    cpdef decode_from_stream(self, LengthPrefixInputStream input_stream):
        pass

    cdef void _write_data_to_output_stream(self, LengthPrefixOutputStream output_stream):
        cdef OutputStream data_out_stream
        data_out_stream = self._data_out_stream
        output_stream.write(data_out_stream.buffer, data_out_stream.pos)
        data_out_stream.pos = 0

cdef class FieldCoderImpl:
    cpdef encode_to_stream(self, value, OutputStream out_stream):
        """
        Encodes `value` to the output stream.

        :param value: The output data
        :param out_stream: Output Stream
        """
        pass

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        """
        Decodes data from the input stream.

        :param in_stream: Input Stream
        :param size: The size data of input stream will be decoded. If the size is set 0, it means
        the coder won't take use of the length to decode the data from input stream.
        :return: The decoded object.
        """
        pass

    cpdef bytes encode(self, value):
        """
        Encodes `value` to a bytes

        :param value: The output data
        :return: The encoded data.
        """
        cdef OutputStream out_stream
        out_stream = OutputStream()
        self.encode_to_stream(value, out_stream)
        return out_stream.buffer[0:out_stream.pos]

    cpdef decode(self, encoded):
        """
        Decodes an object from the bytes

        :param encoded: The bytes
        :return: The decoded object.
        """
        cdef InputStream input_stream
        input_stream = InputStream()
        input_stream._input_data = <char*> encoded
        return self.decode_from_stream(input_stream, len(encoded))

cdef class InputStreamWrapper:
    def __cinit__(self, value_coder: ValueCoderImpl, input_stream: LengthPrefixInputStream):
        self._value_coder = value_coder
        self._input_stream = input_stream

    cpdef bint has_next(self):
        return self._input_stream.available()

    cpdef next(self):
        return self._value_coder.decode_from_stream(self._input_stream)

cdef class IterableCoderImpl(LengthPrefixBaseCoderImpl):
    """
    Encodes iterable data to output stream. The output mode will decide whether write a special end
    message 0x00 to output stream after encoding data.
    """

    def __cinit__(self, *args, **kwargs):
        self._end_message = <char*> malloc(1)
        if self._end_message == NULL:
            raise MemoryError()
        self._end_message[0] = 0x00

    def __init__(self, field_coder: FieldCoderImpl, separated_with_end_message: bool):
        super(IterableCoderImpl, self).__init__(field_coder)
        self._separated_with_end_message = separated_with_end_message

    cpdef encode_to_stream(self, value, LengthPrefixOutputStream output_stream):
        if value:
            for item in value:
                self._field_coder.encode_to_stream(item, self._data_out_stream)
                self._write_data_to_output_stream(output_stream)

        # write end message
        if self._separated_with_end_message:
            output_stream.write(self._end_message, 1)

    cpdef decode_from_stream(self, LengthPrefixInputStream input_stream):
        return InputStreamWrapper(ValueCoderImpl(self._field_coder), input_stream)

    def __dealloc__(self):
        if self._end_message != NULL:
            free(self._end_message)

cdef class ValueCoderImpl(LengthPrefixBaseCoderImpl):
    """
    Encodes a single data to output stream.
    """

    def __init__(self, field_coder: FieldCoderImpl):
        super(ValueCoderImpl, self).__init__(field_coder)

    cpdef encode_to_stream(self, value, LengthPrefixOutputStream output_stream):
        self._field_coder.encode_to_stream(value, self._data_out_stream)
        self._write_data_to_output_stream(output_stream)

    cpdef decode_from_stream(self, LengthPrefixInputStream input_stream):
        cdef char*input_data
        cdef char*temp
        cdef size_t size
        cdef long long addr
        cdef InputStream data_input_stream
        # set input_data pointer to the input data
        size = input_stream.read(&input_data)

        # create InputStream
        data_input_stream = InputStream()
        data_input_stream._input_data = input_data

        return self._field_coder.decode_from_stream(data_input_stream, size)

cdef class FlattenRowCoderImpl(FieldCoderImpl):
    """
    A coder for flatten row (List) object (without field names and row kind value is 0).
    """

    def __init__(self, field_coders: List[FieldCoderImpl]):
        self._field_coders = field_coders  # type: List[FieldCoderImpl]
        self._field_count = len(self._field_coders)
        self._mask_utils = MaskUtils(self._field_count)
        self._reuse_flatten_row = [None for i in range(self._field_count)]

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        cdef list list_value, field_coders
        cdef size_t i
        cdef FieldCoderImpl field_coder

        list_value = <list?> value

        # encode mask value
        self._mask_utils.write_mask(list_value, 0, out_stream)

        # encode every field value
        field_coders = self._field_coders
        for i in range(self._field_count):
            item = list_value[i]
            if item is not None:
                field_coder = <FieldCoderImpl> field_coders[i]
                field_coder.encode_to_stream(item, out_stream)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        cdef bint*mask
        cdef size_t i
        cdef list flatten_row, field_coders
        cdef FieldCoderImpl field_coder

        # read mask
        mask = self._mask_utils.read_mask(in_stream)
        # skip ROW_KIND_BIT_SIZE to data mask
        (<bint**> &mask)[0] += ROW_KIND_BIT_SIZE

        # decode field data
        flatten_row = self._reuse_flatten_row
        field_coders = self._field_coders
        for i in range(self._field_count):
            if mask[i]:
                flatten_row[i] = None
            else:
                field_coder = <FieldCoderImpl> field_coders[i]
                flatten_row[i] = field_coder.decode_from_stream(in_stream, 0)
        return flatten_row

    def __repr__(self):
        return 'FlattenRowCoderImpl[%s]' % ', '.join(str(c) for c in self._field_coders)

cdef class RowCoderImpl(FieldCoderImpl):
    """
    A coder for `Row` or `InternalRow` object.
    """

    def __init__(self, field_coders: List[FieldCoderImpl], field_names: List[str]):
        self._field_coders = field_coders  # type: List[FieldCoderImpl]
        self._field_count = len(self._field_coders)
        self._field_names = field_names  # type: List[str]
        self._mask_utils = MaskUtils(self._field_count)

    cpdef encode_to_stream(self, value: Union[Row, InternalRow], OutputStream out_stream):
        cdef list list_values
        cdef size_t i
        cdef unsigned char row_kind_value
        cdef FieldCoderImpl field_coder

        if isinstance(value, InternalRow):
            # the type is InternalRow
            list_values = <list> value.values
            row_kind_value = <unsigned char> value.row_kind
        else:
            # the type is Row
            list_values = <list> value.get_fields_by_names(self._field_names)
            row_kind_value = <unsigned char> value.get_row_kind().value
        # encode mask value
        self._mask_utils.write_mask(list_values, row_kind_value, out_stream)

        # encode every field value
        for i in range(self._field_count):
            item = list_values[i]
            if item is not None:
                field_coder = <FieldCoderImpl> self._field_coders[i]
                field_coder.encode_to_stream(item, out_stream)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        cdef bint*mask
        cdef unsigned char row_kind_value = 0
        cdef size_t i
        cdef FieldCoderImpl field_coder
        cdef list field_values
        mask = self._mask_utils.read_mask(in_stream)
        field_values = []
        for i in range(self._field_count):
            if mask[i + ROW_KIND_BIT_SIZE]:
                field_values.append(None)
            else:
                field_coder = <FieldCoderImpl> self._field_coders[i]
                field_values.append(field_coder.decode_from_stream(in_stream, 0))
        row = Row()
        row._values = field_values
        for i in range(ROW_KIND_BIT_SIZE):
            row_kind_value += mask[i] * 2 ** i
        row.set_field_names(self._field_names)
        row.set_row_kind(RowKind(row_kind_value))
        return row

    def __repr__(self):
        return 'RowCoderImpl[%s, %s]' % \
               (', '.join(str(c) for c in self._field_coders), self._field_names)

cdef class ArrowCoderImpl(FieldCoderImpl):
    """
    A coder for arrow format data.
    """

    def __init__(self, schema, row_type, timezone):
        self._schema = schema
        self._field_types = row_type.field_types()
        self._timezone = timezone
        self._resettable_io = ResettableIO()
        self._batch_reader = self._load_from_stream(self._resettable_io)

    cpdef encode_to_stream(self, cols, OutputStream out_stream):
        import pyarrow as pa

        self._resettable_io.set_output_stream(out_stream)
        batch_writer = pa.RecordBatchStreamWriter(self._resettable_io, self._schema)
        batch_writer.write_batch(
            pandas_to_arrow(self._schema, self._timezone, self._field_types, cols))

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        return self.decode_one_batch_from_stream(in_stream, size)

    cdef list decode_one_batch_from_stream(self, InputStream in_stream, size_t size):
        self._resettable_io.set_input_bytes(in_stream.read(size))
        # there is only one arrow batch in the underlying input stream
        return arrow_to_pandas(self._timezone, self._field_types, [next(self._batch_reader)])

    def _load_from_stream(self, stream):
        import pyarrow as pa

        while stream.readable():
            reader = pa.ipc.open_stream(stream)
            yield reader.read_next_batch()

    def __repr__(self):
        return 'ArrowCoderImpl[%s]' % self._schema

cdef class OverWindowArrowCoderImpl(FieldCoderImpl):
    """
    A coder for over window with arrow format data.
    The data structure: [window data][arrow format data].
    """
    def __init__(self, arrow_coder_impl: ArrowCoderImpl):
        self._arrow_coder = arrow_coder_impl
        self._int_coder = IntCoderImpl()

    cpdef encode_to_stream(self, cols, OutputStream out_stream):
        self._arrow_coder.encode_to_stream(cols, out_stream)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        cdef int32_t window_num, window_size
        cdef list window_boundaries_and_arrow_data
        window_num = self._int_coder.decode_from_stream(in_stream, 0)
        size -= 4
        window_boundaries_and_arrow_data = []
        for _ in range(window_num):
            window_size = self._int_coder.decode_from_stream(in_stream, 0)
            size -= 4
            window_boundaries_and_arrow_data.append(
                [self._int_coder.decode_from_stream(in_stream, 0)
                 for _ in range(window_size)])
            size -= 4 * window_size
        window_boundaries_and_arrow_data.append(
            self._arrow_coder.decode_one_batch_from_stream(in_stream, size))
        return window_boundaries_and_arrow_data

    def __repr__(self):
        return 'OverWindowArrowCoderImpl[%s]' % self._arrow_coder


cdef class TinyIntCoderImpl(FieldCoderImpl):
    """
    A coder for tiny int value (from -128 to 127).
    """

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        out_stream.write_int8(value)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        return in_stream.read_int8()

cdef class SmallIntCoderImpl(FieldCoderImpl):
    """
    A coder for small int value (from -32,768 to 32,767).
    """

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        out_stream.write_int16(value)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        return in_stream.read_int16()

cdef class IntCoderImpl(FieldCoderImpl):
    """
    A coder for int value (from -2,147,483,648 to 2,147,483,647).
    """

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        out_stream.write_int32(value)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        return in_stream.read_int32()

cdef class BigIntCoderImpl(FieldCoderImpl):
    """
    A coder for big int value (from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807).
    """

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        out_stream.write_int64(value)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        return in_stream.read_int64()

cdef class BooleanCoderImpl(FieldCoderImpl):
    """
    A coder for a boolean value.
    """

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        out_stream.write_byte(value)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        return not not in_stream.read_byte()

cdef class FloatCoderImpl(FieldCoderImpl):
    """
    A coder for a float value (4-byte single precision floating point number).
    """

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        out_stream.write_float(value)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        return in_stream.read_float()

cdef class DoubleCoderImpl(FieldCoderImpl):
    """
    A coder for a double value (8-byte double precision floating point number).
    """

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        out_stream.write_double(value)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        return in_stream.read_double()

cdef class BinaryCoderImpl(FieldCoderImpl):
    """
    A coder for a bytes value.
    """

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        out_stream.write_bytes(value, len(value))

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        return in_stream.read_bytes()

cdef class CharCoderImpl(FieldCoderImpl):
    """
    A coder for a str value.
    """

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        cdef bytes value_bytes = value.encode('utf-8')
        out_stream.write_bytes(value_bytes, len(value_bytes))

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        return in_stream.read_bytes().decode("utf-8")

cdef class BigDecimalCoderImpl(FieldCoderImpl):
    """
    A coder for a big decimal value (without fixed precision and scale).
    """

    def __init__(self):
        self._value_coder = CharCoderImpl()

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        self._value_coder.encode_to_stream(str(value), out_stream)

    cpdef decode_from_stream(self, InputStream input_stream, size_t size):
        return decimal.Decimal(self._value_coder.decode_from_stream(input_stream, 0))

cdef class DecimalCoderImpl(FieldCoderImpl):
    """
    A coder for a decimal value (with fixed precision and scale).
    """

    def __init__(self, precision, scale):
        self._context = decimal.Context(prec=precision)
        self._scale_format = decimal.Decimal(10) ** -scale
        self._value_coder = CharCoderImpl()

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        user_context = decimal.getcontext()
        decimal.setcontext(self._context)
        self._value_coder.encode_to_stream(str(value.quantize(self._scale_format)), out_stream)
        decimal.setcontext(user_context)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        user_context = decimal.getcontext()
        decimal.setcontext(self._context)
        result = decimal.Decimal(self._value_coder.decode_from_stream(in_stream, 0)) \
            .quantize(self._scale_format)
        decimal.setcontext(user_context)
        return result

cdef class DateCoderImpl(FieldCoderImpl):
    """
    A coder for a datetime.date value.
    """

    def __init__(self):
        self._EPOCH_ORDINAL = datetime.datetime(1970, 1, 1).toordinal()

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        out_stream.write_int32(value.toordinal() - self._EPOCH_ORDINAL)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        return datetime.date.fromordinal(in_stream.read_int32() + self._EPOCH_ORDINAL)

cdef class TimeCoderImpl(FieldCoderImpl):
    """
    A coder for a datetime.time value.
    """

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        cdef int32_t hour, minute, seconds, microsecond, milliseconds
        hour = value.hour
        minute = value.minute
        seconds = value.second
        microsecond = value.microsecond
        milliseconds = hour * 3600000 + minute * 60000 + seconds * 1000 + microsecond // 1000
        out_stream.write_int32(milliseconds)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        cdef int32_t value, seconds, milliseconds, minutes, hours
        value = in_stream.read_int32()
        seconds = value // 1000
        milliseconds = value % 1000
        minutes = seconds // 60
        seconds %= 60
        hours = minutes // 60
        minutes %= 60
        return datetime.time(hours, minutes, seconds, milliseconds * 1000)

cdef class TimestampCoderImpl(FieldCoderImpl):
    """
    A coder for a datetime.datetime value.
    """

    def __init__(self, precision):
        self._is_compact = precision <= 3

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        cdef int32_t microseconds_of_second, nanoseconds
        cdef int64_t timestamp_seconds, timestamp_milliseconds
        timestamp_seconds = <int64_t> (value.replace(tzinfo=datetime.timezone.utc).timestamp())
        microseconds_of_second = value.microsecond
        timestamp_milliseconds = timestamp_seconds * 1000 + microseconds_of_second // 1000
        nanoseconds = microseconds_of_second % 1000 * 1000
        if self._is_compact:
            out_stream.write_int64(timestamp_milliseconds)
        else:
            out_stream.write_int64(timestamp_milliseconds)
            out_stream.write_int32(nanoseconds)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        return self._decode_timestamp_data_from_stream(in_stream)

    cdef _decode_timestamp_data_from_stream(self, InputStream in_stream):
        cdef int64_t milliseconds
        cdef int32_t nanoseconds, seconds, microseconds
        if self._is_compact:
            milliseconds = in_stream.read_int64()
            nanoseconds = 0
        else:
            milliseconds = in_stream.read_int64()
            nanoseconds = in_stream.read_int32()
        seconds = milliseconds // 1000
        microseconds = milliseconds % 1000 * 1000 + nanoseconds // 1000
        return datetime.datetime.utcfromtimestamp(seconds).replace(microsecond=microseconds)

cdef class LocalZonedTimestampCoderImpl(TimestampCoderImpl):
    """
    A coder for a datetime.datetime with time zone value.
    """

    def __init__(self, precision, timezone):
        super(LocalZonedTimestampCoderImpl, self).__init__(precision)
        self._timezone = timezone

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        return self._timezone.localize(self._decode_timestamp_data_from_stream(in_stream))

cdef class InstantCoderImpl(FieldCoderImpl):
    """
    A coder for Instant.
    """

    def __init__(self):
        self._null_seconds = -9223372036854775808
        self._null_nanos = -2147483648

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        if value is None:
            out_stream.write_int64(self._null_seconds)
            out_stream.write_int32(self._null_nanos)
        else:
            out_stream.write_int64(value.seconds)
            out_stream.write_int32(value.nanos)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        cdef int64_t seconds
        cdef int32_t nanos
        seconds = in_stream.read_int64()
        nanos = in_stream.read_int32()
        if seconds == self._null_seconds and nanos == self._null_nanos:
            return None
        else:
            return Instant(seconds, nanos)


cdef class CloudPickleCoderImpl(FieldCoderImpl):
    """
    A coder used with cloudpickle for all kinds of python object.
    """

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        cdef bytes pickled_bytes
        pickled_bytes = cloudpickle.dumps(value)
        out_stream.write_bytes(pickled_bytes, len(pickled_bytes))

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        cdef bytes pickled_bytes
        pickled_bytes = in_stream.read_bytes()
        return cloudpickle.loads(pickled_bytes)


cdef class PickleCoderImpl(FieldCoderImpl):
    """
    A coder used with pickle for all kinds of python object.
    """

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        cdef bytes pickled_bytes
        pickled_bytes = pickle.dumps(value)
        out_stream.write_bytes(pickled_bytes, len(pickled_bytes))

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        cdef bytes pickled_bytes
        pickled_bytes = in_stream.read_bytes()
        return pickle.loads(pickled_bytes)

cdef class GenericArrayCoderImpl(FieldCoderImpl):
    """
    A coder for basic array value (the element of array could be null).
    """

    def __init__(self, elem_coder: FieldCoderImpl):
        self._elem_coder = elem_coder

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        cdef int32_t length, i
        cdef list list_value = value
        length = len(value)
        out_stream.write_int32(length)
        for i in range(length):
            item = list_value[i]
            if item is None:
                out_stream.write_byte(False)
            else:
                out_stream.write_byte(True)
                self._elem_coder.encode_to_stream(item, out_stream)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        cdef int32_t length
        length = in_stream.read_int32()
        return [self._elem_coder.decode_from_stream(in_stream, 0) if in_stream.read_byte()
                else None for _ in range(length)]

cdef class PrimitiveArrayCoderImpl(FieldCoderImpl):
    """
    A coder for primitive array value (the element of array won't be null).
    """

    def __init__(self, elem_coder: FieldCoderImpl):
        self._elem_coder = elem_coder

    cpdef encode_to_stream(self, value, OutputStream output_stream):
        cdef int32_t length, i
        cdef list list_value = value
        length = len(value)
        output_stream.write_int32(length)
        for i in range(length):
            self._elem_coder.encode_to_stream(list_value[i], output_stream)

    cpdef decode_from_stream(self, InputStream input_stream, size_t size):
        cdef int32_t length
        length = input_stream.read_int32()
        return [self._elem_coder.decode_from_stream(input_stream, 0) for _ in range(length)]

cdef class MapCoderImpl(FieldCoderImpl):
    """
    A coder for map value (dict with same type key and same type value).
    """

    def __init__(self, key_coder: FieldCoderImpl, value_coder: FieldCoderImpl):
        self._key_coder = key_coder
        self._value_coder = value_coder

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        cdef int32_t length
        length = len(value)
        out_stream.write_int32(length)
        items = value.items()
        for k, v in items:
            self._key_coder.encode_to_stream(k, out_stream)
            if v is None:
                out_stream.write_byte(True)
            else:
                out_stream.write_byte(False)
                self._value_coder.encode_to_stream(v, out_stream)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        cdef int32_t length
        cdef dict map_value
        length = in_stream.read_int32()
        map_value = {}
        for _ in range(length):
            key = self._key_coder.decode_from_stream(in_stream, 0)
            if in_stream.read_byte():
                map_value[key] = None
            else:
                map_value[key] = self._value_coder.decode_from_stream(in_stream, 0)
        return map_value

cdef class TupleCoderImpl(FieldCoderImpl):
    """
    A coder for a tuple value.
    """

    def __init__(self, field_coders):
        self._field_coders = field_coders
        self._field_count = len(field_coders)

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        cdef tuple tuple_value = value
        for i in range(self._field_count):
            self._field_coders[i].encode_to_stream(tuple_value[i], out_stream)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        cdef int32_t i
        cdef list decoded_list
        decoded_list = [self._field_coders[i].decode_from_stream(in_stream, 0)
                        for i in range(self._field_count)]
        return tuple(decoded_list)

cdef class TimeWindowCoderImpl(FieldCoderImpl):
    """
    A coder for TimeWindow.
    """

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        out_stream.write_int64(value.start)
        out_stream.write_int64(value.end)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        cdef int64_t start, end
        start = in_stream.read_int64()
        end = in_stream.read_int64()
        return TimeWindow(start, end)

cdef class CountWindowCoderImpl(FieldCoderImpl):
    """
    A coder for CountWindow.
    """

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        out_stream.write_int64(value.id)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        return CountWindow(in_stream.read_int64())

cdef class GlobalWindowCoderImpl(FieldCoderImpl):
    """
    A coder for GlobalWindow.
    """

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        out_stream.write_byte(0)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        in_stream.read_byte()
        return GlobalWindow()

cdef class DataViewFilterCoderImpl(FieldCoderImpl):
    """
    A coder for CountWindow.
    """
    def __init__(self, udf_data_view_specs):
        self._udf_data_view_specs = udf_data_view_specs
        self._pickle_coder = PickleCoderImpl()

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        self._pickle_coder.encode_to_stream(self._filter_data_views(value), out_stream)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        return self._pickle_coder.decode_from_stream(in_stream, size)

    def _filter_data_views(self, row):
        i = 0
        for specs in self._udf_data_view_specs:
            for spec in specs:
                row[i][spec.field_index] = None
            i += 1
        return row

cdef class AvroCoderImpl(FieldCoderImpl):

    def __init__(self, schema_string: str):
        self._buffer_wrapper = FlinkAvroBufferWrapper()
        self._schema = avro_schema.parse(schema_string)
        self._decoder = FlinkAvroDecoder(self._buffer_wrapper)
        self._encoder = FlinkAvroEncoder(self._buffer_wrapper)
        self._reader = FlinkAvroDatumReader(writer_schema=self._schema, reader_schema=self._schema)
        self._writer = FlinkAvroDatumWriter(writer_schema=self._schema)

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        self._buffer_wrapper.switch_stream(out_stream)
        self._writer.write(value, self._encoder)

    cpdef decode_from_stream(self, InputStream in_stream, size_t size):
        self._buffer_wrapper.switch_stream(in_stream)
        return self._reader.read(self._decoder)

cdef class LocalDateCoderImpl(FieldCoderImpl):

    @staticmethod
    def _encode_to_stream(value, OutputStream out_stream):
        if value is None:
            out_stream.write_int32(0xFFFFFFFF)
            out_stream.write_int16(0xFFFF)
        else:
            out_stream.write_int32(value.year)
            out_stream.write_int8(value.month)
            out_stream.write_int8(value.day)

    @staticmethod
    def _decode_from_stream(InputStream in_stream):
        year = in_stream.read_int32()
        if year == 0xFFFFFFFF:
            in_stream.read(2)
            return None
        month = in_stream.read_int8()
        day = in_stream.read_int8()
        return datetime.date(year, month, day)

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        return self._encode_to_stream(value, out_stream)

    cpdef decode_from_stream(self, InputStream in_stream, size_t length):
        return self._decode_from_stream(in_stream)

cdef class LocalTimeCoderImpl(FieldCoderImpl):

    @staticmethod
    def _encode_to_stream(value, OutputStream out_stream):
        if value is None:
            out_stream.write_int8(0xFF)
            out_stream.write_int16(0xFFFF)
            out_stream.write_int32(0xFFFFFFFF)
        else:
            out_stream.write_int8(value.hour)
            out_stream.write_int8(value.minute)
            out_stream.write_int8(value.second)
            out_stream.write_int32(value.microsecond * 1000)

    @staticmethod
    def _decode_from_stream(InputStream in_stream):
        hour = in_stream.read_int8()
        if hour == 0xFF:
            in_stream.read(6)
            return None
        minute = in_stream.read_int8()
        second = in_stream.read_int8()
        nano = in_stream.read_int32()
        return datetime.time(hour, minute, second, nano // 1000)

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        return self._encode_to_stream(value, out_stream)

    cpdef decode_from_stream(self, InputStream in_stream, size_t length):
        return self._decode_from_stream(in_stream)

cdef class LocalDateTimeCoderImpl(FieldCoderImpl):

    cpdef encode_to_stream(self, value, OutputStream out_stream):
        if value is None:
            LocalDateCoderImpl._encode_to_stream(None, out_stream)
            LocalTimeCoderImpl._encode_to_stream(None, out_stream)
        else:
            LocalDateCoderImpl._encode_to_stream(value.date(), out_stream)
            LocalTimeCoderImpl._encode_to_stream(value.time(), out_stream)

    cpdef decode_from_stream(self, InputStream in_stream, size_t length):
        date = LocalDateCoderImpl._decode_from_stream(in_stream)
        time = LocalTimeCoderImpl._decode_from_stream(in_stream)
        if date is None or time is None:
            return None
        return datetime.datetime(date.year, date.month, date.day, time.hour, time.minute,
                                 time.second, time.microsecond)
