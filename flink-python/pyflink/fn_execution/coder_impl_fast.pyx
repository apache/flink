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

from libc.stdlib cimport free, malloc, realloc
from libc.string cimport memcpy

import datetime
import decimal
from pyflink.table import Row
from pyflink.table.types import RowKind

cdef class BaseCoderImpl:
    cpdef void encode_to_stream(self, value, LengthPrefixOutputStream output_stream):
        pass

    cpdef decode_from_stream(self, LengthPrefixInputStream input_stream):
        pass

cdef class TableFunctionRowCoderImpl(FlattenRowCoderImpl):
    def __init__(self, flatten_row_coder):
        super(TableFunctionRowCoderImpl, self).__init__(flatten_row_coder._field_coders)
        self._end_message = <char*> malloc(1)
        self._end_message[0] = 0x00

    cpdef void encode_to_stream(self, iter_value, LengthPrefixOutputStream output_stream):
        cdef is_row_or_tuple = False
        if iter_value:
            if isinstance(iter_value, (tuple, Row)):
                iter_value = [iter_value]
                is_row_or_tuple = True
            for value in iter_value:
                if self._field_count == 1 and not is_row_or_tuple:
                    value = (value,)
                self._encode_one_row(value, output_stream)
        # write 0x00 as end message
        output_stream.write(self._end_message, 1)

    def __dealloc__(self):
        if self._end_message:
            free(self._end_message)


cdef class AggregateFunctionRowCoderImpl(FlattenRowCoderImpl):

    def __init__(self, flatten_row_coder):
        super(AggregateFunctionRowCoderImpl, self).__init__(flatten_row_coder._field_coders)

    cpdef void encode_to_stream(self, iter_value, LengthPrefixOutputStream output_stream):
        if iter_value:
            for value in iter_value:
                self._encode_one_row_with_row_kind(
                    value, output_stream, value.get_row_kind().value)


cdef class DataStreamFlatMapCoderImpl(BaseCoderImpl):

    def __init__(self, field_coder):
        self._single_field_coder = field_coder
        self._end_message = <char*> malloc(1)
        self._end_message[0] = 0x00

    cpdef void encode_to_stream(self, iter_value, LengthPrefixOutputStream output_stream):
        if iter_value:
            for value in iter_value:
                self._single_field_coder.encode_to_stream(value, output_stream)
        output_stream.write(self._end_message, 1)

    cpdef object decode_from_stream(self, LengthPrefixInputStream input_stream):
        return self._single_field_coder.decode_from_stream(input_stream)

    def __dealloc__(self):
        if self._end_message:
            free(self._end_message)


cdef class DataStreamCoFlatMapCoderImpl(BaseCoderImpl):

    def __init__(self, field_coder):
        self._single_field_coder = field_coder

    cpdef void encode_to_stream(self, iter_value, LengthPrefixOutputStream output_stream):
        for value in iter_value:
            self._single_field_coder.encode_to_stream(value, output_stream)

    cpdef object decode_from_stream(self, LengthPrefixInputStream input_stream):
        return self._single_field_coder.decode_from_stream(input_stream)


cdef class DataStreamMapCoderImpl(FlattenRowCoderImpl):

    def __init__(self, field_coder):
        super(DataStreamMapCoderImpl, self).__init__([field_coder])
        self._single_field_coder = self._field_coders[0]

    cpdef void encode_to_stream(self, value, LengthPrefixOutputStream output_stream):
        coder_type = self._single_field_coder.coder_type()
        type_name = self._single_field_coder.type_name()
        self._encode_field(coder_type, type_name, self._single_field_coder, value)
        output_stream.write(self._tmp_output_data, self._tmp_output_pos)
        self._tmp_output_pos = 0

    cdef void _encode_field(self, CoderType coder_type, TypeName field_type, FieldCoder field_coder,
                        item):
        if coder_type == SIMPLE:
            self._encode_field_simple(field_type, item)
            self._encode_data_stream_field_simple(field_type, item)
        else:
            self._encode_field_complex(field_type, field_coder, item)
            self._encode_data_stream_field_complex(field_type, field_coder, item)

    cdef object _decode_field(self, CoderType coder_type, TypeName field_type,
                        FieldCoder field_coder):
        if coder_type == SIMPLE:
            decoded_obj = self._decode_field_simple(field_type)
            return decoded_obj if decoded_obj is not None \
                else self._decode_data_stream_field_simple(field_type)
        else:
            decoded_obj = self._decode_field_complex(field_type, field_coder)
            return decoded_obj if decoded_obj is not None \
                else self._decode_data_stream_field_complex(field_type, field_coder)

    cpdef object decode_from_stream(self, LengthPrefixInputStream input_stream):
        input_stream.read(&self._input_data)
        self._input_pos = 0
        coder_type = self._single_field_coder.coder_type()
        type_name = self._single_field_coder.type_name()
        decoded_obj = self._decode_field(coder_type, type_name, self._single_field_coder)
        return decoded_obj

    cdef void _encode_data_stream_field_simple(self, TypeName field_type, item):
        if field_type == PICKLED_BYTES:
            import pickle
            pickled_bytes = pickle.dumps(item)
            self._encode_bytes(pickled_bytes, len(pickled_bytes))
        elif field_type == BIG_DEC:
            item_bytes = str(item).encode('utf-8')
            self._encode_bytes(item_bytes, len(item_bytes))

    cdef void _encode_data_stream_field_complex(self, TypeName field_type, FieldCoder field_coder,
                                           item):
        if field_type == TUPLE:
            tuple_field_coders = (<TupleCoderImpl> field_coder).field_coders
            tuple_field_count = len(tuple_field_coders)
            tuple_value = list(item)
            for i in range(tuple_field_count):
                field_item = tuple_value[i]
                tuple_field_coder = tuple_field_coders[i]
                if field_item is not None:
                    self._encode_field(tuple_field_coder.coder_type(),
                                       tuple_field_coder.type_name(),
                                       tuple_field_coder,
                                       field_item)
    cdef object _decode_data_stream_field_simple(self, TypeName field_type):
        if field_type == PICKLED_BYTES:
            decoded_bytes = self._decode_bytes()
            import pickle
            return pickle.loads(decoded_bytes)
        elif field_type == BIG_DEC:
            return decimal.Decimal(self._decode_bytes().decode("utf-8"))

    cdef object _decode_data_stream_field_complex(self, TypeName field_type, FieldCoder field_coder):
        if field_type == TUPLE:
            tuple_field_coders = (<TupleCoderImpl> field_coder).field_coders
            tuple_field_count = len(tuple_field_coders)
            decoded_list = []
            for i in range(tuple_field_count):
                decoded_list.append(self._decode_field(
                    tuple_field_coders[i].coder_type(),
                    tuple_field_coders[i].type_name(),
                    tuple_field_coders[i]
                ))
            return (*decoded_list,)

ROW_KIND_BIT_SIZE = 2

cdef class FlattenRowCoderImpl(BaseCoderImpl):
    def __init__(self, field_coders):
        self._field_coders = field_coders
        self._field_count = len(self._field_coders)
        self._field_type = <TypeName*> malloc(self._field_count * sizeof(TypeName))
        self._field_coder_type = <CoderType*> malloc(
            self._field_count * sizeof(CoderType))
        self._leading_complete_bytes_num = (self._field_count + ROW_KIND_BIT_SIZE) // 8
        self._remaining_bits_num = (self._field_count + ROW_KIND_BIT_SIZE) % 8
        self._tmp_output_buffer_size = 1024
        self._tmp_output_pos = 0
        self._tmp_output_data = <char*> malloc(self._tmp_output_buffer_size)
        self._mask_byte_search_table = <unsigned char*> malloc(8 * sizeof(unsigned char))
        self._row_kind_byte_table = <unsigned char*> malloc(8 * sizeof(unsigned char))
        self._mask = <bint*> malloc((self._field_count + ROW_KIND_BIT_SIZE) * sizeof(bint))
        self._init_attribute()
        self.row = [None for _ in range(self._field_count)]

    cpdef void encode_to_stream(self, value, LengthPrefixOutputStream output_stream):
        self._encode_one_row(value, output_stream)

    cpdef decode_from_stream(self, LengthPrefixInputStream input_stream):
        self._decode_next_row(input_stream)
        return self.row

    cdef void _init_attribute(self):
        self._mask_byte_search_table[0] = 0x80
        self._mask_byte_search_table[1] = 0x40
        self._mask_byte_search_table[2] = 0x20
        self._mask_byte_search_table[3] = 0x10
        self._mask_byte_search_table[4] = 0x08
        self._mask_byte_search_table[5] = 0x04
        self._mask_byte_search_table[6] = 0x02
        self._mask_byte_search_table[7] = 0x01
        self._row_kind_byte_table[0] = 0x00
        self._row_kind_byte_table[1] = 0x80
        self._row_kind_byte_table[2] = 0x40
        self._row_kind_byte_table[3] = 0xC0
        for i in range(self._field_count):
            self._field_type[i] = self._field_coders[i].type_name()
            self._field_coder_type[i] = self._field_coders[i].coder_type()

    cdef void _encode_one_row_to_buffer(self, value, unsigned char row_kind_value):
        cdef size_t i
        self._write_mask(
            value,
            self._leading_complete_bytes_num,
            self._remaining_bits_num,
            row_kind_value,
            self._field_count)
        for i in range(self._field_count):
            item = value[i]
            if item is not None:
                if self._field_coder_type[i] == SIMPLE:
                    self._encode_field_simple(self._field_type[i], item)
                else:
                    self._encode_field_complex(self._field_type[i], self._field_coders[i], item)

    cpdef bytes encode_nested(self, value):
        self._encode_one_row_to_buffer(value, 0)
        cdef size_t pos
        pos = self._tmp_output_pos
        self._tmp_output_pos = 0
        return self._tmp_output_data[:pos]

    cdef void _encode_one_row(self, value, LengthPrefixOutputStream output_stream):
        self._encode_one_row_with_row_kind(value, output_stream, 0)

    cdef void _encode_one_row_with_row_kind(
            self, value, LengthPrefixOutputStream output_stream, unsigned char row_kind_value):
        self._encode_one_row_to_buffer(value, row_kind_value)
        output_stream.write(self._tmp_output_data, self._tmp_output_pos)
        self._tmp_output_pos = 0

    cdef void _read_mask(self, bint*mask,
                         size_t input_leading_complete_bytes_num,
                         size_t input_remaining_bits_num):
        cdef size_t field_pos, i
        cdef unsigned char b
        field_pos = 0
        for _ in range(input_leading_complete_bytes_num):
            b = self._input_data[self._input_pos]
            self._input_pos += 1
            for i in range(8):
                mask[field_pos] = (b & self._mask_byte_search_table[i]) > 0
                field_pos += 1

        if input_remaining_bits_num:
            b = self._input_data[self._input_pos]
            self._input_pos += 1
            for i in range(input_remaining_bits_num):
                mask[field_pos] = (b & self._mask_byte_search_table[i]) > 0
                field_pos += 1

    cdef void _decode_next_row(self, LengthPrefixInputStream input_stream):
        cdef size_t i
        cdef size_t length
        length = input_stream.read(&self._input_data)
        self._input_pos = 0
        self._read_mask(self._mask, self._leading_complete_bytes_num,
                        self._remaining_bits_num)
        for i in range(self._field_count):
            if self._mask[i + ROW_KIND_BIT_SIZE]:
                self.row[i] = None
            else:
                if self._field_coder_type[i] == SIMPLE:
                    self.row[i] = self._decode_field_simple(self._field_type[i])
                else:
                    self.row[i] = self._decode_field_complex(self._field_type[i],
                                                             self._field_coders[i])

    cdef object _decode_field(self, CoderType coder_type, TypeName field_type,
                              FieldCoder field_coder):
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

    cdef object _decode_field_complex(self, TypeName field_type, FieldCoder field_coder):
        cdef libc.stdint.int32_t nanoseconds, microseconds, seconds, length
        cdef libc.stdint.int32_t i, row_field_count, leading_complete_bytes_num, remaining_bits_num
        cdef libc.stdint.int64_t milliseconds
        cdef bint*null_mask
        cdef FieldCoder value_coder, key_coder
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
        elif field_type == BASIC_ARRAY:
            # Basic Array
            length = self._decode_int()
            value_coder = (<BasicArrayCoderImpl> field_coder).elem_coder
            value_type = value_coder.type_name()
            value_coder_type = value_coder.coder_type()
            return [
                self._decode_field(value_coder_type, value_type, value_coder) if self._decode_byte()
                else None for _ in range(length)]
        elif field_type == PRIMITIVE_ARRAY:
            # Primitive Array
            length = self._decode_int()
            value_coder = (<PrimitiveArrayCoderImpl> field_coder).elem_coder
            value_type = value_coder.type_name()
            value_coder_type = value_coder.coder_type()
            return [self._decode_field(value_coder_type, value_type, value_coder)
                    for _ in range(length)]
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
            row_field_names = (<RowCoderImpl> field_coder).field_names
            row_field_count = len(row_field_coders)
            mask = <bint*> malloc((row_field_count + ROW_KIND_BIT_SIZE) * sizeof(bint))
            leading_complete_bytes_num = (row_field_count + ROW_KIND_BIT_SIZE) // 8
            remaining_bits_num = (row_field_count + ROW_KIND_BIT_SIZE) % 8
            self._read_mask(mask, leading_complete_bytes_num, remaining_bits_num)
            row = Row(*[None if mask[i + ROW_KIND_BIT_SIZE] else
                        self._decode_field(
                            row_field_coders[i].coder_type(),
                            row_field_coders[i].type_name(),
                            row_field_coders[i])
                        for i in range(row_field_count)])
            row.set_field_names(row_field_names)
            row_kind_value = 0
            for i in range(ROW_KIND_BIT_SIZE):
                row_kind_value += mask[i] * 2 ** i
            row.set_row_kind(RowKind(row_kind_value))
            free(mask)
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

    cdef void _encode_field(self, CoderType coder_type, TypeName field_type, FieldCoder field_coder,
                            item):
        if coder_type == SIMPLE:
            self._encode_field_simple(field_type, item)
        else:
            self._encode_field_complex(field_type, field_coder, item)

    cdef void _encode_field_simple(self, TypeName field_type, item):
        cdef libc.stdint.int32_t hour, minute, seconds, microsecond, milliseconds
        cdef bytes item_bytes
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
            self._encode_bytes(item, len(item))
        elif field_type == CHAR:
            # str
            item_bytes = item.encode('utf-8')
            self._encode_bytes(item_bytes, len(item_bytes))
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

    cdef void _encode_field_complex(self, TypeName field_type, FieldCoder field_coder, item):
        cdef libc.stdint.int32_t nanoseconds, microseconds_of_second, length, row_field_count
        cdef libc.stdint.int32_t leading_complete_bytes_num, remaining_bits_num
        cdef libc.stdint.int64_t timestamp_milliseconds, timestamp_seconds
        cdef FieldCoder value_coder, key_coder
        cdef TypeName value_type, key_type
        cdef CoderType value_coder_type, key_coder_type
        cdef FieldCoder row_field_coder
        cdef list row_field_coders, row_value

        if field_type == DECIMAL:
            # decimal
            user_context = decimal.getcontext()
            decimal.setcontext((<DecimalCoderImpl> field_coder).context)
            bytes_value = str(item.quantize((<DecimalCoderImpl> field_coder).scale_format)).encode(
                "utf-8")
            self._encode_bytes(bytes_value, len(bytes_value))
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
        elif field_type == BASIC_ARRAY:
            # Basic Array
            length = len(item)
            value_coder = (<BasicArrayCoderImpl> field_coder).elem_coder
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
        elif field_type == PRIMITIVE_ARRAY:
            # Primitive Array
            length = len(item)
            value_coder = (<PrimitiveArrayCoderImpl> field_coder).elem_coder
            value_type = value_coder.type_name()
            value_coder_type = value_coder.coder_type()
            self._encode_int(length)
            for i in range(length):
                value = item[i]
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
            row_kind_value = item._row_kind.value
            self._write_mask(row_value, leading_complete_bytes_num, remaining_bits_num, row_kind_value, row_field_count)
            for i in range(row_field_count):
                field_item = row_value[i]
                row_field_coder = row_field_coders[i]
                if field_item is not None:
                    self._encode_field(row_field_coder.coder_type(), row_field_coder.type_name(),
                                       row_field_coder, field_item)

    cdef void _extend(self, size_t missing):
        while self._tmp_output_buffer_size < self._tmp_output_pos + missing:
            self._tmp_output_buffer_size *= 2
        self._tmp_output_data = <char*> realloc(self._tmp_output_data, self._tmp_output_buffer_size)

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

    cdef void _encode_bytes(self, char*b, size_t length):
        self._encode_int(length)
        if self._tmp_output_buffer_size < self._tmp_output_pos + length:
            self._extend(length)
        if length < 8:
            # This is faster than memcpy when the string is short.
            for i in range(length):
                self._tmp_output_data[self._tmp_output_pos + i] = b[i]
        else:
            memcpy(self._tmp_output_data + self._tmp_output_pos, b, length)
        self._tmp_output_pos += length

    cdef void _write_mask(self, value, size_t leading_complete_bytes_num,
                          size_t remaining_bits_num, unsigned char row_kind_value, size_t field_count):
        cdef size_t field_pos, index
        cdef unsigned char*bit_map_byte_search_table
        cdef unsigned char b, i
        field_pos = 0
        bit_map_byte_search_table = self._mask_byte_search_table

        # first byte contains the row kind bits
        b = self._row_kind_byte_table[row_kind_value]
        for i in range(0, 8 - ROW_KIND_BIT_SIZE):
            if field_pos + i < field_count and value[field_pos + i] is None:
                b |= bit_map_byte_search_table[i + ROW_KIND_BIT_SIZE]
        field_pos += 8 - ROW_KIND_BIT_SIZE
        self._encode_byte(b)

        for _ in range(1, leading_complete_bytes_num):
            b = 0x00
            for i in range(8):
                if value[field_pos + i] is None:
                    b |= bit_map_byte_search_table[i]
            field_pos += 8
            self._encode_byte(b)

        if leading_complete_bytes_num >= 1 and remaining_bits_num:
            b = 0x00
            for i in range(remaining_bits_num):
                if value[field_pos + i] is None:
                    b |= bit_map_byte_search_table[i]
            self._encode_byte(b)

    def __dealloc__(self):
        if self._mask:
            free(self._mask)
        if self._mask_byte_search_table:
            free(self._mask_byte_search_table)
        if self._tmp_output_data:
            free(self._tmp_output_data)
        if self._field_type:
            free(self._field_type)
        if self._field_coder_type:
            free(self._field_coder_type)

cdef class FieldCoder:
    cpdef CoderType coder_type(self):
        return UNDEFINED

    cpdef TypeName type_name(self):
        return NONE

cdef class TinyIntCoderImpl(FieldCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE

    cpdef TypeName type_name(self):
        return TINYINT

cdef class SmallIntCoderImpl(FieldCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE
    cpdef TypeName type_name(self):
        return SMALLINT

cdef class IntCoderImpl(FieldCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE

    cpdef TypeName type_name(self):
        return INT

cdef class BigIntCoderImpl(FieldCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE
    cpdef TypeName type_name(self):
        return BIGINT

cdef class BooleanCoderImpl(FieldCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE
    cpdef TypeName type_name(self):
        return BOOLEAN

cdef class FloatCoderImpl(FieldCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE
    cpdef TypeName type_name(self):
        return FLOAT

cdef class DoubleCoderImpl(FieldCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE
    cpdef TypeName type_name(self):
        return DOUBLE

cdef class BinaryCoderImpl(FieldCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE
    cpdef TypeName type_name(self):
        return BINARY

cdef class CharCoderImpl(FieldCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE
    cpdef TypeName type_name(self):
        return CHAR

cdef class DateCoderImpl(FieldCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE

    cpdef TypeName type_name(self):
        return DATE

cdef class TimeCoderImpl(FieldCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE

    cpdef TypeName type_name(self):
        return TIME

cdef class DecimalCoderImpl(FieldCoder):
    def __cinit__(self, precision, scale):
        self.context = decimal.Context(prec=precision)
        self.scale_format = decimal.Decimal(10) ** -scale

    cpdef CoderType coder_type(self):
        return COMPLEX

    cpdef TypeName type_name(self):
        return DECIMAL

cdef class TimestampCoderImpl(FieldCoder):
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

cdef class BasicArrayCoderImpl(FieldCoder):
    def __cinit__(self, elem_coder):
        self.elem_coder = elem_coder

    cpdef CoderType coder_type(self):
        return COMPLEX

    cpdef TypeName type_name(self):
        return BASIC_ARRAY

cdef class PrimitiveArrayCoderImpl(FieldCoder):
    def __cinit__(self, elem_coder):
        self.elem_coder = elem_coder

    cpdef CoderType coder_type(self):
        return COMPLEX

    cpdef TypeName type_name(self):
        return PRIMITIVE_ARRAY

cdef class MapCoderImpl(FieldCoder):
    def __cinit__(self, key_coder, value_coder):
        self.key_coder = key_coder
        self.value_coder = value_coder

    cpdef CoderType coder_type(self):
        return COMPLEX

    cpdef TypeName type_name(self):
        return MAP

cdef class RowCoderImpl(FieldCoder):
    def __cinit__(self, field_coders, field_names):
        self.field_coders = field_coders
        self.field_names = field_names

    cpdef CoderType coder_type(self):
        return COMPLEX

    cpdef TypeName type_name(self):
        return ROW

cdef class BigDecimalCoderImpl(FieldCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE

    cpdef TypeName type_name(self):
        return BIG_DEC

cdef class PickledBytesCoderImpl(FieldCoder):
    cpdef CoderType coder_type(self):
        return SIMPLE

    cpdef TypeName type_name(self):
        return PICKLED_BYTES

cdef class TupleCoderImpl(FieldCoder):
    def __cinit__(self, field_coders):
        self.field_coders = field_coders

    cpdef CoderType coder_type(self):
        return COMPLEX

    cpdef TypeName type_name(self):
        return TUPLE
