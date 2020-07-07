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

from apache_beam.coders.coder_impl cimport StreamCoderImpl, OutputStream, InputStream

# InputStreamAndFunctionWrapper wraps the user-defined function
# and input_stream_wrapper in operations
cdef class InputStreamAndFunctionWrapper:
    # user-defined function
    cdef readonly object func
    cdef InputStreamWrapper input_stream_wrapper

# InputStreamWrapper wraps input_stream and related infos used to decode data
cdef class InputStreamWrapper:
    cdef InputStream input_stream
    cdef list input_field_coders
    cdef TypeName*input_field_type
    cdef CoderType*input_coder_type
    cdef libc.stdint.int32_t input_field_count
    cdef libc.stdint.int32_t input_leading_complete_bytes_num
    cdef libc.stdint.int32_t input_remaining_bits_num
    cdef size_t input_buffer_size

cdef class PassThroughLengthPrefixCoderImpl(StreamCoderImpl):
    cdef readonly StreamCoderImpl _value_coder

cdef class FlattenRowCoderImpl(StreamCoderImpl):
    # the input field coders and related args used to decode input_stream data
    cdef list _input_field_coders
    cdef TypeName*_input_field_type
    cdef CoderType*_input_coder_type
    cdef libc.stdint.int32_t _input_field_count
    cdef libc.stdint.int32_t _input_leading_complete_bytes_num
    cdef libc.stdint.int32_t _input_remaining_bits_num

    # the output field coders and related args used to encode data to output_stream
    cdef readonly list _output_field_coders
    cdef TypeName*_output_field_type
    cdef CoderType*_output_coder_type
    cdef libc.stdint.int32_t _output_field_count
    cdef libc.stdint.int32_t _output_leading_complete_bytes_num
    cdef libc.stdint.int32_t _output_remaining_bits_num

    cdef bint*_null_mask
    cdef unsigned char*_null_byte_search_table

    # the char pointer used to store encoded data of output_stream
    cdef char*_output_data
    cdef size_t _output_buffer_size
    cdef size_t _output_pos

    # the tmp char pointer used to store encoded data of every row
    cdef char*_tmp_output_data
    cdef size_t _tmp_output_buffer_size
    cdef size_t _tmp_output_pos

    # the char pointer used to map the decoded data of input_stream
    cdef char*_input_data
    cdef size_t _input_pos
    cdef size_t _input_buffer_size

    # used to store the result of Python user-defined function
    cdef list row

    # the Python user-defined function
    cdef object func

    # initial attribute
    cdef void _init_attribute(self)

    # wrap input_stream
    cdef InputStreamWrapper _wrap_input_stream(self, InputStream input_stream, size_t size)

    cdef void _write_null_mask(self, value, libc.stdint.int32_t leading_complete_bytes_num,
                               libc.stdint.int32_t remaining_bits_num)
    cdef void _read_null_mask(self, bint*null_mask, libc.stdint.int32_t leading_complete_bytes_num,
                              libc.stdint.int32_t remaining_bits_num)

    cdef void _prepare_encode(self, InputStreamAndFunctionWrapper input_stream_and_function_wrapper,
                              OutputStream out_stream)

    cdef void _maybe_flush(self, OutputStream out_stream)
    # Because output_buffer will be reallocated during encoding data, we need to remap output_buffer
    # to the data pointer of output_stream
    cdef void _map_output_data_to_output_stream(self, OutputStream out_stream)
    cdef void _copy_to_output_buffer(self)

    # encode data to output_stream
    cdef void _encode_one_row(self, value)
    cdef void _encode_field(self, CoderType coder_type, TypeName field_type, BaseCoder field_coder,
                          item)
    cdef void _encode_field_simple(self, TypeName field_type, item)
    cdef void _encode_field_complex(self, TypeName field_type, BaseCoder field_coder, item)
    cdef void _extend(self, size_t missing)
    cdef void _encode_byte(self, unsigned char val)
    cdef void _encode_smallint(self, libc.stdint.int16_t v)
    cdef void _encode_int(self, libc.stdint.int32_t v)
    cdef void _encode_bigint(self, libc.stdint.int64_t v)
    cdef void _encode_float(self, float v)
    cdef void _encode_double(self, double v)
    cdef void _encode_bytes(self, char*b, size_t length)

    # decode data from input_stream
    cdef void _decode_next_row(self)
    cdef object _decode_field(self, CoderType coder_type, TypeName field_type, BaseCoder field_coder)
    cdef object _decode_field_simple(self, TypeName field_type)
    cdef object _decode_field_complex(self, TypeName field_type, BaseCoder field_coder)
    cdef unsigned char _decode_byte(self) except? -1
    cdef libc.stdint.int16_t _decode_smallint(self) except? -1
    cdef libc.stdint.int32_t _decode_int(self) except? -1
    cdef libc.stdint.int64_t _decode_bigint(self) except? -1
    cdef float _decode_float(self) except? -1
    cdef double _decode_double(self) except? -1
    cdef bytes _decode_bytes(self)

cdef class TableFunctionRowCoderImpl(FlattenRowCoderImpl):
    cdef void _encode_end_message(self)

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
    ARRAY = 14
    MAP = 15
    LOCAL_ZONED_TIMESTAMP = 16

cdef class BaseCoder:
    cpdef CoderType coder_type(self)
    cpdef TypeName type_name(self)

cdef class TinyIntCoderImpl(BaseCoder):
    pass

cdef class SmallIntCoderImpl(BaseCoder):
    pass

cdef class IntCoderImpl(BaseCoder):
    pass

cdef class BigIntCoderImpl(BaseCoder):
    pass

cdef class BooleanCoderImpl(BaseCoder):
    pass

cdef class FloatCoderImpl(BaseCoder):
    pass

cdef class DoubleCoderImpl(BaseCoder):
    pass

cdef class BinaryCoderImpl(BaseCoder):
    pass

cdef class CharCoderImpl(BaseCoder):
    pass

cdef class DateCoderImpl(BaseCoder):
    pass

cdef class TimeCoderImpl(BaseCoder):
    pass

cdef class DecimalCoderImpl(BaseCoder):
    cdef readonly object context
    cdef readonly object scale_format

cdef class TimestampCoderImpl(BaseCoder):
    cdef readonly bint is_compact

cdef class LocalZonedTimestampCoderImpl(TimestampCoderImpl):
    cdef readonly object timezone

cdef class ArrayCoderImpl(BaseCoder):
    cdef readonly BaseCoder elem_coder

cdef class MapCoderImpl(BaseCoder):
    cdef readonly BaseCoder key_coder
    cdef readonly BaseCoder value_coder

cdef class RowCoderImpl(BaseCoder):
    cdef readonly list field_coders
