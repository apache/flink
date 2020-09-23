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
# cython: language_level=3

cimport libc.stdint

from pyflink.fn_execution.stream cimport LengthPrefixInputStream, LengthPrefixOutputStream

cdef class BaseCoderImpl:
    cpdef void encode_to_stream(self, value, LengthPrefixOutputStream output_stream)
    cpdef decode_from_stream(self, LengthPrefixInputStream input_stream)

cdef unsigned char ROW_KIND_BIT_SIZE

cdef class FlattenRowCoderImpl(BaseCoderImpl):
    cdef readonly list _field_coders
    cdef TypeName*_field_type
    cdef CoderType*_field_coder_type
    cdef size_t _field_count
    cdef size_t _leading_complete_bytes_num
    cdef size_t _remaining_bits_num

    cdef bint*_mask
    cdef unsigned char*_mask_byte_search_table
    cdef unsigned char*_row_kind_byte_table

    # the tmp char pointer used to store encoded data of every row
    cdef char*_tmp_output_data
    cdef size_t _tmp_output_buffer_size
    cdef size_t _tmp_output_pos

    # the char pointer used to map the decoded data of input_stream
    cdef char*_input_data
    cdef size_t _input_pos

    # used to store the result of Python user-defined function
    cdef list row

    # initial attribute
    cdef void _init_attribute(self)

    cdef void _write_mask(self, value, size_t leading_complete_bytes_num,
                             size_t remaining_bits_num, unsigned char row_kind_value)
    cdef void _read_mask(self, bint*null_mask, size_t leading_complete_bytes_num,
                            size_t remaining_bits_num)

    cpdef bytes encode_nested(self, value)
    # encode data to output_stream
    cdef void _encode_one_row_to_buffer(self, value, unsigned char row_kind_value)
    cdef void _encode_one_row(self, value, LengthPrefixOutputStream output_stream)
    cdef void _encode_one_row_with_row_kind(self, value, LengthPrefixOutputStream output_stream,
                                            unsigned char row_kind_value)
    cdef void _encode_field(self, CoderType coder_type, TypeName field_type, FieldCoder field_coder,
                            item)
    cdef void _encode_field_simple(self, TypeName field_type, item)
    cdef void _encode_field_complex(self, TypeName field_type, FieldCoder field_coder, item)
    cdef void _extend(self, size_t missing)
    cdef void _encode_byte(self, unsigned char val)
    cdef void _encode_smallint(self, libc.stdint.int16_t v)
    cdef void _encode_int(self, libc.stdint.int32_t v)
    cdef void _encode_bigint(self, libc.stdint.int64_t v)
    cdef void _encode_float(self, float v)
    cdef void _encode_double(self, double v)
    cdef void _encode_bytes(self, char*b, size_t length)

    # decode data from input_stream
    cdef void _decode_next_row(self, LengthPrefixInputStream input_stream)
    cdef object _decode_field(self, CoderType coder_type, TypeName field_type,
                              FieldCoder field_coder)
    cdef object _decode_field_simple(self, TypeName field_type)
    cdef object _decode_field_complex(self, TypeName field_type, FieldCoder field_coder)
    cdef unsigned char _decode_byte(self) except? -1
    cdef libc.stdint.int16_t _decode_smallint(self) except? -1
    cdef libc.stdint.int32_t _decode_int(self) except? -1
    cdef libc.stdint.int64_t _decode_bigint(self) except? -1
    cdef float _decode_float(self) except? -1
    cdef double _decode_double(self) except? -1
    cdef bytes _decode_bytes(self)

cdef class AggregateFunctionRowCoderImpl(FlattenRowCoderImpl):
    pass

cdef class TableFunctionRowCoderImpl(FlattenRowCoderImpl):
    cdef char* _end_message

cdef class DataStreamStatelessMapCoderImpl(FlattenRowCoderImpl):
    cdef readonly FieldCoder _single_field_coder
    cdef object _decode_data_stream_field_simple(self, TypeName field_type)
    cdef object _decode_data_stream_field_complex(self, TypeName field_type, FieldCoder field_coder)
    cdef void _encode_data_stream_field_simple(self, TypeName field_type, item)
    cdef void _encode_data_stream_field_complex(self, TypeName field_type, FieldCoder field_coder,
                                                item)

cdef class DataStreamStatelessFlatMapCoderImpl(BaseCoderImpl):
    cdef readonly object _single_field_coder

cdef enum CoderType:
    UNDEFINED = -1
    SIMPLE = 0
    COMPLEX = 1

cdef enum TypeName:
    NONE = -1
    ROW = 0
    TINYINT = 1
    SMALLINT = 2
    INT = 3
    BIGINT = 4
    DECIMAL = 5
    FLOAT = 6
    DOUBLE = 7
    DATE = 8
    TIME = 9
    TIMESTAMP = 10
    BOOLEAN = 11
    BINARY = 12
    CHAR = 13
    BASIC_ARRAY = 14
    MAP = 15
    LOCAL_ZONED_TIMESTAMP = 16
    PICKLED_BYTES = 17
    BIG_DEC = 18
    TUPLE = 19
    PRIMITIVE_ARRAY = 20

cdef class FieldCoder:
    cpdef CoderType coder_type(self)
    cpdef TypeName type_name(self)

cdef class TinyIntCoderImpl(FieldCoder):
    pass

cdef class SmallIntCoderImpl(FieldCoder):
    pass

cdef class IntCoderImpl(FieldCoder):
    pass

cdef class BigIntCoderImpl(FieldCoder):
    pass

cdef class BooleanCoderImpl(FieldCoder):
    pass

cdef class FloatCoderImpl(FieldCoder):
    pass

cdef class DoubleCoderImpl(FieldCoder):
    pass

cdef class BinaryCoderImpl(FieldCoder):
    pass

cdef class CharCoderImpl(FieldCoder):
    pass

cdef class DateCoderImpl(FieldCoder):
    pass

cdef class TimeCoderImpl(FieldCoder):
    pass

cdef class PickledBytesCoderImpl(FieldCoder):
    pass

cdef class BigDecimalCoderImpl(FieldCoder):
    pass

cdef class DecimalCoderImpl(FieldCoder):
    cdef readonly object context
    cdef readonly object scale_format

cdef class TimestampCoderImpl(FieldCoder):
    cdef readonly bint is_compact

cdef class LocalZonedTimestampCoderImpl(TimestampCoderImpl):
    cdef readonly object timezone

cdef class BasicArrayCoderImpl(FieldCoder):
    cdef readonly FieldCoder elem_coder

cdef class PrimitiveArrayCoderImpl(FieldCoder):
    cdef readonly FieldCoder elem_coder

cdef class MapCoderImpl(FieldCoder):
    cdef readonly FieldCoder key_coder
    cdef readonly FieldCoder value_coder

cdef class RowCoderImpl(FieldCoder):
    cdef readonly list field_coders

cdef class TupleCoderImpl(FieldCoder):
    cdef readonly list field_coders
