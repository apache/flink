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

cimport libc.stdlib
from libc.string cimport strlen

import datetime
import decimal
from pyflink.table import Row

cdef class InputStreamAndFunctionWrapper:
    def __cinit__(self, func, input_stream_wrapper):
        self.func = func
        self.input_stream_wrapper = input_stream_wrapper

cdef class PassThroughLengthPrefixCoderImpl(StreamCoderImpl):
    def __cinit__(self, value_coder):
        self._value_coder = value_coder

    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        self._value_coder.encode_to_stream(value, out_stream, nested)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        return self._value_coder.decode_from_stream(in_stream, nested)

    cpdef get_estimated_size_and_observables(self, value, bint nested=False):
        return 0, []

cdef class TableFunctionRowCoderImpl(FlattenRowCoderImpl):
    def __init__(self, flatten_row_coder):
        super(TableFunctionRowCoderImpl, self).__init__(flatten_row_coder._output_field_coders)

    cpdef encode_to_stream(self, input_stream_and_function_wrapper, OutputStream out_stream,
                           bint nested):
        self._prepare_encode(input_stream_and_function_wrapper, out_stream)
        while self._input_buffer_size > self._input_pos:
            self._decode_next_row()
            result = self.func(self.row)
            if result:
                for value in result:
                    if self._output_field_count == 1:
                        value = (value,)
                    self._encode_one_row(value)
                    self._maybe_flush(out_stream)
            self._encode_end_message()

        self._map_output_data_to_output_stream(out_stream)

    # write 0x00 as end message
    cdef void _encode_end_message(self):
        if self._output_buffer_size < self._output_pos + 2:
            self._extend(2)
        self._output_data[self._output_pos] = 0x01
        self._output_data[self._output_pos + 1] = 0x00
        self._output_pos += 2

cdef class FlattenRowCoderImpl(StreamCoderImpl):
    def __init__(self, field_coders):
        self._output_field_coders = field_coders
        self._output_field_count = len(self._output_field_coders)
        self._output_field_type = <TypeName*> libc.stdlib.malloc(
            self._output_field_count * sizeof(TypeName))
        self._output_coder_type = <CoderType*> libc.stdlib.malloc(
            self._output_field_count * sizeof(CoderType))
        self._output_leading_complete_bytes_num = self._output_field_count // 8
        self._output_remaining_bits_num = self._output_field_count % 8
        self._tmp_output_buffer_size = 1024
        self._tmp_output_pos = 0
        self._tmp_output_data = <char*> libc.stdlib.malloc(self._tmp_output_buffer_size)
        self._null_byte_search_table = <unsigned char*> libc.stdlib.malloc(
            8 * sizeof(unsigned char))
        self._init_attribute()

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        cdef InputStreamWrapper input_stream_wrapper
        input_stream_wrapper = self._wrap_input_stream(in_stream, in_stream.size())
        return input_stream_wrapper

    cpdef encode_to_stream(self, input_stream_and_function_wrapper, OutputStream out_stream,
                           bint nested):
        cdef list result
        self._prepare_encode(input_stream_and_function_wrapper, out_stream)
        while self._input_buffer_size > self._input_pos:
            self._decode_next_row()
            result = self.func(self.row)
            self._encode_one_row(result)
            self._maybe_flush(out_stream)
        self._map_output_data_to_output_stream(out_stream)

    cdef void _init_attribute(self):
        self._null_byte_search_table[0] = 0x80
        self._null_byte_search_table[1] = 0x40
        self._null_byte_search_table[2] = 0x20
        self._null_byte_search_table[3] = 0x10
        self._null_byte_search_table[4] = 0x08
        self._null_byte_search_table[5] = 0x04
        self._null_byte_search_table[6] = 0x02
        self._null_byte_search_table[7] = 0x01
        for i in range(self._output_field_count):
            self._output_field_type[i] = self._output_field_coders[i].type_name()
            self._output_coder_type[i] = self._output_field_coders[i].coder_type()

    cdef InputStreamWrapper _wrap_input_stream(self, InputStream input_stream, size_t size):
        # wrappers the input field coders and input_stream together
        # so that it can be transposed to operations
        cdef InputStreamWrapper input_stream_wrapper
        input_stream_wrapper = InputStreamWrapper()
        input_stream_wrapper.input_stream = input_stream
        input_stream_wrapper.input_field_coders = self._output_field_coders
        input_stream_wrapper.input_remaining_bits_num = self._output_remaining_bits_num
        input_stream_wrapper.input_leading_complete_bytes_num = \
            self._output_leading_complete_bytes_num
        input_stream_wrapper.input_field_count = self._output_field_count
        input_stream_wrapper.input_field_type = self._output_field_type
        input_stream_wrapper.input_coder_type = self._output_coder_type
        input_stream_wrapper.input_stream.pos = size
        input_stream_wrapper.input_buffer_size = size
        return input_stream_wrapper

    cdef void _encode_one_row(self, value):
        cdef libc.stdint.int32_t i
        self._write_null_mask(value, self._output_leading_complete_bytes_num,
                              self._output_remaining_bits_num)
        for i in range(self._output_field_count):
            item = value[i]
            if item is not None:
                if self._output_coder_type[i] == SIMPLE:
                    self._encode_field_simple(self._output_field_type[i], item)
                else:
                    self._encode_field_complex(self._output_field_type[i],
                                               self._output_field_coders[i], item)

        self._copy_to_output_buffer()

    cdef void _read_null_mask(self, bint*null_mask,
                              libc.stdint.int32_t input_leading_complete_bytes_num,
                              libc.stdint.int32_t input_remaining_bits_num):
        cdef libc.stdint.int32_t field_pos, i
        cdef unsigned char b
        field_pos = 0
        for _ in range(input_leading_complete_bytes_num):
            b = self._input_data[self._input_pos]
            self._input_pos += 1
            for i in range(8):
                null_mask[field_pos] = (b & self._null_byte_search_table[i]) > 0
                field_pos += 1

        if input_remaining_bits_num:
            b = self._input_data[self._input_pos]
            self._input_pos += 1
            for i in range(input_remaining_bits_num):
                null_mask[field_pos] = (b & self._null_byte_search_table[i]) > 0
                field_pos += 1

    cdef void _decode_next_row(self):
        cdef libc.stdint.int32_t i
        # skip prefix variable int length
        while self._input_data[self._input_pos] & 0x80:
            self._input_pos += 1
        self._input_pos += 1
        self._read_null_mask(self._null_mask, self._input_leading_complete_bytes_num,
                             self._input_remaining_bits_num)
        for i in range(self._input_field_count):
            if self._null_mask[i]:
                self.row[i] = None
            else:
                if self._input_coder_type[i] == SIMPLE:
                    self.row[i] = self._decode_field_simple(self._input_field_type[i])
                else:
                    self.row[i] = self._decode_field_complex(self._input_field_type[i],
                                                             self._input_field_coders[i])

    cdef object _decode_field(self, CoderType coder_type, TypeName field_type, BaseCoder field_coder):
        if coder_type == SIMPLE:
            return self._decode_field_simple(field_type)
        else:
            return self._decode_field_complex(field_type, field_coder)

    cdef object _decode_field_simple(self, TypeName field_type):
        cdef libc.stdint.int32_t value, minutes, seconds, hours
        cdef libc.stdint.int64_t milliseconds
        if field_type == TINYINT:
            # tinyint
            return self._decode_byte()
        elif field_type == SMALLINT:
            # smallint
            return self._decode_smallint()
        elif field_type == INT:
            # int
            return self._decode_int()
        elif field_type == BIGINT:
            # bigint
            return self._decode_bigint()
        elif field_type == BOOLEAN:
            # boolean
            return not not self._decode_byte()
        elif field_type == FLOAT:
            # float
            return self._decode_float()
        elif field_type == DOUBLE:
            # double
            return self._decode_double()
        elif field_type == BINARY:
            # bytes
            return self._decode_bytes()
        elif field_type == CHAR:
            # str
            return self._decode_bytes().decode("utf-8")
        elif field_type == DATE:
            # Date
            # EPOCH_ORDINAL = datetime.datetime(1970, 1, 1).toordinal()
            # The value of EPOCH_ORDINAL is 719163
            return datetime.date.fromordinal(self._decode_int() + 719163)
        elif field_type == TIME:
            # Time
            value = self._decode_int()
            seconds = value // 1000
            milliseconds = value % 1000
            minutes = seconds // 60
            seconds %= 60
            hours = minutes // 60
            minutes %= 60
            return datetime.time(hours, minutes, seconds, milliseconds * 1000)

    cdef object _decode_field_complex(self, TypeName field_type, BaseCoder field_coder):
        cdef libc.stdint.int32_t nanoseconds, microseconds, seconds, length
        cdef libc.stdint.int32_t i, row_field_count, leading_complete_bytes_num, remaining_bits_num
        cdef libc.stdint.int64_t milliseconds
        cdef bint*null_mask
        cdef BaseCoder value_coder, key_coder
        cdef TypeName value_type, key_type
        cdef CoderType value_coder_type, key_coder_type
        cdef list row_field_coders

        if field_type == DECIMAL:
            # decimal
            user_context = decimal.getcontext()
            decimal.setcontext((<DecimalCoderImpl> field_coder).context)
            result = decimal.Decimal((self._decode_bytes()).decode("utf-8")).quantize(
                (<DecimalCoderImpl> field_coder).scale_format)
            decimal.setcontext(user_context)
            return result
        elif field_type == TIMESTAMP:
            # Timestamp
            if (<TimeCoderImpl> field_coder).is_compact:
                milliseconds = self._decode_bigint()
                nanoseconds = 0
            else:
                milliseconds = self._decode_bigint()
                nanoseconds = self._decode_int()
            seconds, microseconds = (milliseconds // 1000,
                                     milliseconds % 1000 * 1000 + nanoseconds // 1000)
            return datetime.datetime.utcfromtimestamp(seconds).replace(microsecond=microseconds)
        elif field_type == LOCAL_ZONED_TIMESTAMP:
            # LOCAL_ZONED_TIMESTAMP
            if (<LocalZonedTimestampCoderImpl> field_coder).is_compact:
                milliseconds = self._decode_bigint()
                nanoseconds = 0
            else:
                milliseconds = self._decode_bigint()
                nanoseconds = self._decode_int()
            seconds, microseconds = (milliseconds // 1000,
                                     milliseconds % 1000 * 1000 + nanoseconds // 1000)
            return (<LocalZonedTimestampCoderImpl> field_coder).timezone.localize(
                datetime.datetime.utcfromtimestamp(seconds).replace(microsecond=microseconds))
        elif field_type == ARRAY:
            # Array
            length = self._decode_int()
            value_coder = (<ArrayCoderImpl> field_coder).elem_coder
            value_type = value_coder.type_name()
            value_coder_type = value_coder.coder_type()
            return [self._decode_field(value_coder_type, value_type, value_coder) if self._decode_byte()
                    else None for _ in range(length)]
        elif field_type == MAP:
            # Map
            key_coder = (<MapCoderImpl> field_coder).key_coder
            key_type = key_coder.type_name()
            key_coder_type = key_coder.coder_type()
            value_coder = (<MapCoderImpl> field_coder).value_coder
            value_type = value_coder.type_name()
            value_coder_type = value_coder.coder_type()
            length = self._decode_int()
            map_value = {}
            for _ in range(length):
                key = self._decode_field(key_coder_type, key_type, key_coder)
                if self._decode_byte():
                    map_value[key] = None
                else:
                    map_value[key] = self._decode_field(value_coder_type, value_type, value_coder)
            return map_value
        elif field_type == ROW:
            # Row
            row_field_coders = (<RowCoderImpl> field_coder).field_coders
            row_field_count = len(row_field_coders)
            null_mask = <bint*> libc.stdlib.malloc(row_field_count * sizeof(bint))
            leading_complete_bytes_num = row_field_count // 8
            remaining_bits_num = row_field_count % 8
            self._read_null_mask(null_mask, leading_complete_bytes_num, remaining_bits_num)
            row = Row(*[None if null_mask[i] else
                        self._decode_field(
                            row_field_coders[i].coder_type(),
                            row_field_coders[i].type_name(),
                            row_field_coders[i])
                        for i in range(row_field_count)])
            libc.stdlib.free(null_mask)
            return row

    cdef unsigned char _decode_byte(self) except? -1:
        self._input_pos += 1
        return <unsigned char> self._input_data[self._input_pos - 1]

    cdef libc.stdint.int16_t _decode_smallint(self) except? -1:
        self._input_pos += 2
        return (<unsigned char> self._input_data[self._input_pos - 1]
                | <libc.stdint.uint32_t> <unsigned char> self._input_data[self._input_pos - 2] << 8)

    cdef libc.stdint.int32_t _decode_int(self) except? -1:
        self._input_pos += 4
        return (<unsigned char> self._input_data[self._input_pos - 1]
                | <libc.stdint.uint32_t> <unsigned char> self._input_data[self._input_pos - 2] << 8
                | <libc.stdint.uint32_t> <unsigned char> self._input_data[self._input_pos - 3] << 16
                | <libc.stdint.uint32_t> <unsigned char> self._input_data[
                    self._input_pos - 4] << 24)

    cdef libc.stdint.int64_t _decode_bigint(self) except? -1:
        self._input_pos += 8
        return (<unsigned char> self._input_data[self._input_pos - 1]
                | <libc.stdint.uint64_t> <unsigned char> self._input_data[self._input_pos - 2] << 8
                | <libc.stdint.uint64_t> <unsigned char> self._input_data[self._input_pos - 3] << 16
                | <libc.stdint.uint64_t> <unsigned char> self._input_data[self._input_pos - 4] << 24
                | <libc.stdint.uint64_t> <unsigned char> self._input_data[self._input_pos - 5] << 32
                | <libc.stdint.uint64_t> <unsigned char> self._input_data[self._input_pos - 6] << 40
                | <libc.stdint.uint64_t> <unsigned char> self._input_data[self._input_pos - 7] << 48
                | <libc.stdint.uint64_t> <unsigned char> self._input_data[
                    self._input_pos - 8] << 56)

    cdef float _decode_float(self) except? -1:
        cdef libc.stdint.int32_t as_long = self._decode_int()
        return (<float*> <char*> &as_long)[0]

    cdef double _decode_double(self) except? -1:
        cdef libc.stdint.int64_t as_long = self._decode_bigint()
        return (<double*> <char*> &as_long)[0]

    cdef bytes _decode_bytes(self):
        cdef libc.stdint.int32_t size = self._decode_int()
        self._input_pos += size
        return self._input_data[self._input_pos - size: self._input_pos]

    cdef void _prepare_encode(self, InputStreamAndFunctionWrapper input_stream_and_function_wrapper,
                              OutputStream out_stream):
        cdef InputStreamWrapper input_stream_wrapper
        # get the data pointer of output_stream
        self._output_data = out_stream.data
        self._output_pos = out_stream.pos
        self._output_buffer_size = out_stream.buffer_size
        self._tmp_output_pos = 0

        input_stream_wrapper = input_stream_and_function_wrapper.input_stream_wrapper
        # get the data pointer of input_stream
        self._input_data = input_stream_wrapper.input_stream.allc
        self._input_buffer_size = input_stream_wrapper.input_buffer_size

        # get the infos of input coder which will be used to decode data from input_stream
        self._input_field_count = input_stream_wrapper.input_field_count
        self._input_leading_complete_bytes_num = input_stream_wrapper.input_leading_complete_bytes_num
        self._input_remaining_bits_num = input_stream_wrapper.input_remaining_bits_num
        self._input_field_type = input_stream_wrapper.input_field_type
        self._input_coder_type = input_stream_wrapper.input_coder_type
        self._input_field_coders = input_stream_wrapper.input_field_coders
        self._null_mask = <bint*> libc.stdlib.malloc(self._input_field_count * sizeof(bint))
        self._input_pos = 0

        # initial the result row and get the Python user-defined function
        self.row = [None for _ in range(self._input_field_count)]
        self.func = input_stream_and_function_wrapper.func

    cdef void _encode_field(self, CoderType coder_type, TypeName field_type, BaseCoder field_coder,
                          item):
        if coder_type == SIMPLE:
            self._encode_field_simple(field_type, item)
        else:
            self._encode_field_complex(field_type, field_coder, item)

    cdef void _encode_field_simple(self, TypeName field_type, item):
        cdef libc.stdint.int32_t hour, minute, seconds, microsecond, milliseconds
        if field_type == TINYINT:
            # tinyint
            self._encode_byte(item)
        elif field_type == SMALLINT:
            # smallint
            self._encode_smallint(item)
        elif field_type == INT:
            # int
            self._encode_int(item)
        elif field_type == BIGINT:
            # bigint
            self._encode_bigint(item)
        elif field_type == BOOLEAN:
            # boolean
            self._encode_byte(item)
        elif field_type == FLOAT:
            # float
            self._encode_float(item)
        elif field_type == DOUBLE:
            # double
            self._encode_double(item)
        elif field_type == BINARY:
            # bytes
            self._encode_bytes(item)
        elif field_type == CHAR:
            # str
            self._encode_bytes(item.encode('utf-8'))
        elif field_type == DATE:
            # Date
            # EPOCH_ORDINAL = datetime.datetime(1970, 1, 1).toordinal()
            # The value of EPOCH_ORDINAL is 719163
            self._encode_int(item.toordinal() - 719163)
        elif field_type == TIME:
            # Time
            hour = item.hour
            minute = item.minute
            seconds = item.second
            microsecond = item.microsecond
            milliseconds = hour * 3600000 + minute * 60000 + seconds * 1000 + microsecond // 1000
            self._encode_int(milliseconds)

    cdef void _encode_field_complex(self, TypeName field_type, BaseCoder field_coder, item):
        cdef libc.stdint.int32_t nanoseconds, microseconds_of_second, length, row_field_count
        cdef libc.stdint.int32_t leading_complete_bytes_num, remaining_bits_num
        cdef libc.stdint.int64_t timestamp_milliseconds, timestamp_seconds
        cdef BaseCoder value_coder, key_coder
        cdef TypeName value_type, key_type
        cdef CoderType value_coder_type, key_coder_type
        cdef BaseCoder row_field_coder
        cdef list row_field_coders, row_value

        if field_type == DECIMAL:
            # decimal
            user_context = decimal.getcontext()
            decimal.setcontext((<DecimalCoderImpl> field_coder).context)
            bytes_value = str(item.quantize((<DecimalCoderImpl> field_coder).scale_format)).encode(
                "utf-8")
            self._encode_bytes(bytes_value)
            decimal.setcontext(user_context)
        elif field_type == TIMESTAMP or field_type == LOCAL_ZONED_TIMESTAMP:
            # Timestamp
            timestamp_seconds = <libc.stdint.int64_t> (
                item.replace(tzinfo=datetime.timezone.utc).timestamp())
            microseconds_of_second = item.microsecond
            timestamp_milliseconds = timestamp_seconds * 1000 + microseconds_of_second // 1000
            nanoseconds = microseconds_of_second % 1000 * 1000
            if field_coder.is_compact:
                self._encode_bigint(timestamp_milliseconds)
            else:
                self._encode_bigint(timestamp_milliseconds)
                self._encode_int(nanoseconds)
        elif field_type == ARRAY:
            # Array
            length = len(item)
            value_coder = (<ArrayCoderImpl> field_coder).elem_coder
            value_type = value_coder.type_name()
            value_coder_type = value_coder.coder_type()
            self._encode_int(length)
            for i in range(length):
                value = item[i]
                if value is None:
                    self._encode_byte(False)
                else:
                    self._encode_byte(True)
                    self._encode_field(value_coder_type, value_type, value_coder, value)
        elif field_type == MAP:
            # Map
            length = len(item)
            self._encode_int(length)
            iter_items = item.items()
            key_coder = (<MapCoderImpl> field_coder).key_coder
            key_type = key_coder.type_name()
            key_coder_type = key_coder.coder_type()
            value_coder = (<MapCoderImpl> field_coder).value_coder
            value_type = value_coder.type_name()
            value_coder_type = value_coder.coder_type()
            for iter_item in iter_items:
                key = iter_item[0]
                value = iter_item[1]
                self._encode_field(key_coder_type, key_type, key_coder, key)
                if value is None:
                    self._encode_byte(True)
                else:
                    self._encode_byte(False)
                    self._encode_field(value_coder_type, value_type, value_coder, value)
        elif field_type == ROW:
            # Row
            row_field_coders = (<RowCoderImpl> field_coder).field_coders
            row_field_count = len(row_field_coders)
            leading_complete_bytes_num = row_field_count // 8
            remaining_bits_num = row_field_count % 8
            row_value = list(item)
            self._write_null_mask(row_value, leading_complete_bytes_num, remaining_bits_num)
            for i in range(row_field_count):
                field_item = row_value[i]
                row_field_coder = row_field_coders[i]
                if field_item is not None:
                    self._encode_field(row_field_coder.coder_type(), row_field_coder.type_name(),
                                     row_field_coder, field_item)

    cdef void _copy_to_output_buffer(self):
        cdef size_t size
        cdef size_t i
        cdef bint is_realloc
        cdef char bits
        # the length of the variable prefix length will be less than 9 bytes
        if self._output_buffer_size < self._output_pos + self._tmp_output_pos + 9:
            self._output_buffer_size += self._tmp_output_buffer_size + 9
            self._output_data = <char*> libc.stdlib.realloc(self._output_data,
                                                            self._output_buffer_size)
        size = self._tmp_output_pos
        # write variable prefix length
        while size:
            bits = size & 0x7F
            size >>= 7
            if size:
                bits |= 0x80
            self._output_data[self._output_pos] = bits
            self._output_pos += 1
        if self._tmp_output_pos < 8:
            # This is faster than memcpy when the string is short.
            for i in range(self._tmp_output_pos):
                self._output_data[self._output_pos + i] = self._tmp_output_data[i]
        else:
            libc.string.memcpy(self._output_data + self._output_pos, self._tmp_output_data,
                               self._tmp_output_pos)
        self._output_pos += self._tmp_output_pos
        self._tmp_output_pos = 0

    cdef void _maybe_flush(self, OutputStream out_stream):
        # Currently, it will trigger flushing when the size of buffer reach to 10_000_000
        if self._output_pos > 10_000_000:
            self._map_output_data_to_output_stream(out_stream)
            out_stream.flush()
            self._output_pos = 0

    cdef void _map_output_data_to_output_stream(self, OutputStream out_stream):
        out_stream.data = self._output_data
        out_stream.pos = self._output_pos
        out_stream.buffer_size = self._output_buffer_size

    cdef void _extend(self, size_t missing):
        while self._tmp_output_buffer_size < self._tmp_output_pos + missing:
            self._tmp_output_buffer_size *= 2
        self._tmp_output_data = <char*> libc.stdlib.realloc(self._tmp_output_data,
                                                            self._tmp_output_buffer_size)

    cdef void _encode_byte(self, unsigned char val):
        if self._tmp_output_buffer_size < self._tmp_output_pos + 1:
            self._extend(1)
        self._tmp_output_data[self._tmp_output_pos] = val
        self._tmp_output_pos += 1

    cdef void _encode_smallint(self, libc.stdint.int16_t v):
        if self._tmp_output_buffer_size < self._tmp_output_pos + 2:
            self._extend(2)
        self._tmp_output_data[self._tmp_output_pos] = <unsigned char> (v >> 8)
        self._tmp_output_data[self._tmp_output_pos + 1] = <unsigned char> v
        self._tmp_output_pos += 2

    cdef void _encode_int(self, libc.stdint.int32_t v):
        if self._tmp_output_buffer_size < self._tmp_output_pos + 4:
            self._extend(4)
        self._tmp_output_data[self._tmp_output_pos] = <unsigned char> (v >> 24)
        self._tmp_output_data[self._tmp_output_pos + 1] = <unsigned char> (v >> 16)
        self._tmp_output_data[self._tmp_output_pos + 2] = <unsigned char> (v >> 8)
        self._tmp_output_data[self._tmp_output_pos + 3] = <unsigned char> v
        self._tmp_output_pos += 4

    cdef void _encode_bigint(self, libc.stdint.int64_t v):
        if self._tmp_output_buffer_size < self._tmp_output_pos + 8:
            self._extend(8)
        self._tmp_output_data[self._tmp_output_pos] = <unsigned char> (v >> 56)
        self._tmp_output_data[self._tmp_output_pos + 1] = <unsigned char> (v >> 48)
        self._tmp_output_data[self._tmp_output_pos + 2] = <unsigned char> (v >> 40)
        self._tmp_output_data[self._tmp_output_pos + 3] = <unsigned char> (v >> 32)
        self._tmp_output_data[self._tmp_output_pos + 4] = <unsigned char> (v >> 24)
        self._tmp_output_data[self._tmp_output_pos + 5] = <unsigned char> (v >> 16)
        self._tmp_output_data[self._tmp_output_pos + 6] = <unsigned char> (v >> 8)
        self._tmp_output_data[self._tmp_output_pos + 7] = <unsigned char> v
        self._tmp_output_pos += 8

    cdef void _encode_float(self, float v):
        self._encode_int((<libc.stdint.int32_t*> <char*> &v)[0])

    cdef void _encode_double(self, double v):
        self._encode_bigint((<libc.stdint.int64_t*> <char*> &v)[0])

    cdef void _encode_bytes(self, char*b):
        cdef libc.stdint.int32_t length = strlen(b)
        self._encode_int(length)
        if self._tmp_output_buffer_size < self._tmp_output_pos + length:
            self._extend(length)
        if length < 8:
            # This is faster than memcpy when the string is short.
            for i in range(length):
                self._tmp_output_data[self._tmp_output_pos + i] = b[i]
        else:
            libc.string.memcpy(self._tmp_output_data + self._tmp_output_pos, b, length)
        self._tmp_output_pos += length

    cdef void _write_null_mask(self, value, libc.stdint.int32_t leading_complete_bytes_num,
                               libc.stdint.int32_t remaining_bits_num):
        cdef libc.stdint.int32_t field_pos, index
        cdef unsigned char*null_byte_search_table
        cdef unsigned char b, i
        field_pos = 0
        null_byte_search_table = self._null_byte_search_table
        for _ in range(leading_complete_bytes_num):
            b = 0x00
            for i in range(8):
                if value[field_pos + i] is None:
                    b |= null_byte_search_table[i]
            field_pos += 8
            self._encode_byte(b)

        if remaining_bits_num:
            b = 0x00
            for i in range(remaining_bits_num):
                if value[field_pos + i] is None:
                    b |= null_byte_search_table[i]
            self._encode_byte(b)

    def __dealloc__(self):
        if self.null_mask:
            libc.stdlib.free(self._null_mask)
        if self.null_byte_search_table:
            libc.stdlib.free(self._null_byte_search_table)
        if self._tmp_output_data:
            libc.stdlib.free(self._tmp_output_data)
        if self._output_field_type:
            libc.stdlib.free(self._output_field_type)
        if self._output_coder_type:
            libc.stdlib.free(self._output_coder_type)

cdef class BaseCoder:
    cpdef CoderType coder_type(self):
        return UNDEFINED

    cpdef TypeName type_name(self):
        return NONE

cdef class TinyIntCoderImpl(BaseCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE

    cpdef TypeName type_name(self):
        return TINYINT

cdef class SmallIntCoderImpl(BaseCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE
    cpdef TypeName type_name(self):
        return SMALLINT

cdef class IntCoderImpl(BaseCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE

    cpdef TypeName type_name(self):
        return INT

cdef class BigIntCoderImpl(BaseCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE
    cpdef TypeName type_name(self):
        return BIGINT

cdef class BooleanCoderImpl(BaseCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE
    cpdef TypeName type_name(self):
        return BOOLEAN

cdef class FloatCoderImpl(BaseCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE
    cpdef TypeName type_name(self):
        return FLOAT

cdef class DoubleCoderImpl(BaseCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE
    cpdef TypeName type_name(self):
        return DOUBLE

cdef class BinaryCoderImpl(BaseCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE
    cpdef TypeName type_name(self):
        return BINARY

cdef class CharCoderImpl(BaseCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE
    cpdef TypeName type_name(self):
        return CHAR

cdef class DateCoderImpl(BaseCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE

    cpdef TypeName type_name(self):
        return DATE

cdef class TimeCoderImpl(BaseCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE

    cpdef TypeName type_name(self):
        return TIME

cdef class DecimalCoderImpl(BaseCoder):
    def __cinit__(self, precision, scale):
        self.context = decimal.Context(prec=precision)
        self.scale_format = decimal.Decimal(10) ** -scale

    cpdef CoderType coder_type(self):
        return COMPLEX

    cpdef TypeName type_name(self):
        return DECIMAL

cdef class TimestampCoderImpl(BaseCoder):
    def __init__(self, precision):
        self.is_compact = precision <= 3

    cpdef CoderType coder_type(self):
        return COMPLEX

    cpdef TypeName type_name(self):
        return TIMESTAMP

cdef class LocalZonedTimestampCoderImpl(TimestampCoderImpl):
    def __init__(self, precision, timezone):
        super(LocalZonedTimestampCoderImpl, self).__init__(precision)
        self.timezone = timezone

    cpdef CoderType coder_type(self):
        return COMPLEX

    cpdef TypeName type_name(self):
        return LOCAL_ZONED_TIMESTAMP

cdef class ArrayCoderImpl(BaseCoder):
    def __cinit__(self, elem_coder):
        self.elem_coder = elem_coder

    cpdef CoderType coder_type(self):
        return COMPLEX

    cpdef TypeName type_name(self):
        return ARRAY

cdef class MapCoderImpl(BaseCoder):
    def __cinit__(self, key_coder, value_coder):
        self.key_coder = key_coder
        self.value_coder = value_coder

    cpdef CoderType coder_type(self):
        return COMPLEX

    cpdef TypeName type_name(self):
        return MAP

cdef class RowCoderImpl(BaseCoder):
    def __cinit__(self, field_coders):
        self.field_coders = field_coders

    cpdef CoderType coder_type(self):
        return COMPLEX

    cpdef TypeName type_name(self):
        return ROW