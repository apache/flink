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

from libc.stdint cimport int32_t, int64_t

from pyflink.fn_execution.stream_fast cimport LengthPrefixInputStream, LengthPrefixOutputStream, \
    InputStream, OutputStream

cdef enum InternalRowKind:
    INSERT = 0
    UPDATE_BEFORE = 1
    UPDATE_AFTER = 2
    DELETE = 3

cdef class InternalRow:
    cdef readonly list values
    cdef readonly InternalRowKind row_kind
    cdef list field_names
    cpdef object to_row(self)
    cdef bint is_retract_msg(self)
    cdef bint is_accumulate_msg(self)

cdef class MaskUtils:
    cdef bint*_mask
    cdef unsigned char*_mask_byte_search_table
    cdef unsigned char*_row_kind_byte_table
    cdef size_t _field_count
    cdef size_t _leading_complete_bytes_num
    cdef size_t _remaining_bits_num

    cdef void write_mask(self, list value, unsigned char row_kind_value, OutputStream output_stream)
    cdef bint*read_mask(self, InputStream input_stream)

cdef class LengthPrefixBaseCoderImpl:
    cdef FieldCoderImpl _field_coder
    cdef OutputStream _data_out_stream

    cpdef encode_to_stream(self, value, LengthPrefixOutputStream output_stream)
    cpdef decode_from_stream(self, LengthPrefixInputStream input_stream)
    cdef void _write_data_to_output_stream(self, LengthPrefixOutputStream output_stream)

cdef class FieldCoderImpl:
    cpdef encode_to_stream(self, value, OutputStream out_stream)
    cpdef decode_from_stream(self, InputStream in_stream, size_t size)
    cpdef bytes encode(self, value)
    cpdef decode(self, encoded)

cdef class InputStreamWrapper:
    cdef ValueCoderImpl _value_coder
    cdef LengthPrefixInputStream _input_stream

    cpdef bint has_next(self)
    cpdef next(self)

cdef class IterableCoderImpl(LengthPrefixBaseCoderImpl):
    cdef char*_end_message
    cdef bint _separated_with_end_message

cdef class ValueCoderImpl(LengthPrefixBaseCoderImpl):
    pass

cdef unsigned char ROW_KIND_BIT_SIZE

cdef class FlattenRowCoderImpl(FieldCoderImpl):
    cdef list _field_coders
    cdef list _reuse_flatten_row
    cdef size_t _field_count
    cdef MaskUtils _mask_utils

cdef class RowCoderImpl(FieldCoderImpl):
    cdef list _field_coders
    cdef size_t _field_count
    cdef list _field_names
    cdef MaskUtils _mask_utils

cdef class ArrowCoderImpl(FieldCoderImpl):
    cdef object _schema
    cdef list _field_types
    cdef object _timezone
    cdef object _resettable_io
    cdef object _batch_reader

    cdef list decode_one_batch_from_stream(self, InputStream in_stream, size_t size)

cdef class OverWindowArrowCoderImpl(FieldCoderImpl):
    cdef ArrowCoderImpl _arrow_coder
    cdef IntCoderImpl _int_coder

cdef class TinyIntCoderImpl(FieldCoderImpl):
    pass

cdef class SmallIntCoderImpl(FieldCoderImpl):
    pass

cdef class IntCoderImpl(FieldCoderImpl):
    pass

cdef class BigIntCoderImpl(FieldCoderImpl):
    pass

cdef class BooleanCoderImpl(FieldCoderImpl):
    pass

cdef class FloatCoderImpl(FieldCoderImpl):
    pass

cdef class DoubleCoderImpl(FieldCoderImpl):
    pass

cdef class BinaryCoderImpl(FieldCoderImpl):
    pass

cdef class CharCoderImpl(FieldCoderImpl):
    pass

cdef class BigDecimalCoderImpl(FieldCoderImpl):
    cdef CharCoderImpl _value_coder

cdef class DecimalCoderImpl(FieldCoderImpl):
    cdef object _context
    cdef object _scale_format
    cdef CharCoderImpl _value_coder

cdef class DateCoderImpl(FieldCoderImpl):
    cdef int _EPOCH_ORDINAL

cdef class TimeCoderImpl(FieldCoderImpl):
    pass

cdef class TimestampCoderImpl(FieldCoderImpl):
    cdef bint _is_compact

    cdef _decode_timestamp_data_from_stream(self, InputStream in_stream)

cdef class LocalZonedTimestampCoderImpl(TimestampCoderImpl):
    cdef object _timezone

cdef class InstantCoderImpl(FieldCoderImpl):
    cdef int64_t _null_seconds
    cdef int32_t _null_nanos

cdef class CloudPickleCoderImpl(FieldCoderImpl):
    pass

cdef class PickleCoderImpl(FieldCoderImpl):
    pass

cdef class GenericArrayCoderImpl(FieldCoderImpl):
    cdef FieldCoderImpl _elem_coder

cdef class PrimitiveArrayCoderImpl(FieldCoderImpl):
    cdef FieldCoderImpl _elem_coder

cdef class MapCoderImpl(FieldCoderImpl):
    cdef FieldCoderImpl _key_coder
    cdef FieldCoderImpl _value_coder

cdef class TupleCoderImpl(FieldCoderImpl):
    cdef list _field_coders
    cdef size_t _field_count

cdef class TimeWindowCoderImpl(FieldCoderImpl):
    pass

cdef class CountWindowCoderImpl(FieldCoderImpl):
    pass

cdef class DataViewFilterCoderImpl(FieldCoderImpl):
    cdef object _udf_data_view_specs
    cdef PickleCoderImpl _pickle_coder

cdef class AvroCoderImpl(FieldCoderImpl):
    cdef object _buffer_wrapper
    cdef object _schema
    cdef object _decoder
    cdef object _encoder
    cdef object _reader
    cdef object _writer

cdef class LocalDateCoderImpl(FieldCoderImpl):
    pass

cdef class LocalTimeCoderImpl(FieldCoderImpl):
    pass

cdef class LocalDateTimeCoderImpl(FieldCoderImpl):
    pass
