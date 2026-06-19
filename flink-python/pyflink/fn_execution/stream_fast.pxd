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
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t

cdef class LengthPrefixInputStream:
    cdef size_t read(self, char**data)
    cdef size_t available(self)

cdef class LengthPrefixOutputStream:
    cdef void write(self, char*data, size_t length)
    cpdef void flush(self)
    cpdef void close(self)

cdef class InputStream:
    cdef char*_input_data
    cdef size_t _input_pos

    cpdef bytes read(self, size_t size)
    cdef long read_byte(self) except? -1
    cdef int8_t read_int8(self) except? -1
    cdef int16_t read_int16(self) except? -1
    cdef int32_t read_int32(self) except? -1
    cdef int64_t read_int64(self) except? -1
    cdef float read_float(self) except? -1
    cdef double read_double(self) except? -1
    cdef bytes read_bytes(self)

cdef class OutputStream:
    cdef char*buffer
    cdef size_t buffer_size
    cdef size_t pos

    cpdef void write(self, bytes v)
    cdef void write_byte(self, unsigned char val)
    cdef void write_int8(self, int8_t v)
    cdef void write_int16(self, int16_t v)
    cdef void write_int32(self, int32_t v)
    cdef void write_int64(self, int64_t v)
    cdef void write_float(self, float v)
    cdef void write_double(self, double v)
    cdef void write_bytes(self, char*b, size_t length)
    cdef void _extend(self, size_t missing)
