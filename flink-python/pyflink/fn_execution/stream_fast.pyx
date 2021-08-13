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
from libc.stdint cimport uint32_t, uint64_t
from libc.stdlib cimport free, malloc, realloc
from libc.string cimport memcpy

cdef class LengthPrefixInputStream:
    cdef size_t read(self, char** data):
        pass
    cdef size_t available(self):
        pass

cdef class LengthPrefixOutputStream:
    cdef void write(self, char*data, size_t length):
        pass
    cpdef void flush(self):
        pass
    cpdef void close(self):
        pass

cdef class InputStream:
    def __init__(self):
        self._input_pos = 0

    cpdef bytes read(self, size_t size):
        self._input_pos += size
        return self._input_data[self._input_pos - size: self._input_pos]

    cdef long read_byte(self) except? -1:
        self._input_pos += 1
        return <long> <unsigned char> self._input_data[self._input_pos - 1]

    cdef int8_t read_int8(self) except? -1:
        self._input_pos += 1
        return <unsigned char> self._input_data[self._input_pos - 1]

    cdef int16_t read_int16(self) except? -1:
        self._input_pos += 2
        return (<unsigned char> self._input_data[self._input_pos - 1]
                | <uint32_t> <unsigned char> self._input_data[self._input_pos - 2] << 8)

    cdef int32_t read_int32(self) except? -1:
        self._input_pos += 4
        return (<unsigned char> self._input_data[self._input_pos - 1]
                | <uint32_t> <unsigned char> self._input_data[self._input_pos - 2] << 8
                | <uint32_t> <unsigned char> self._input_data[self._input_pos - 3] << 16
                | <uint32_t> <unsigned char> self._input_data[self._input_pos - 4] << 24)

    cdef int64_t read_int64(self) except? -1:
        self._input_pos += 8
        return (<unsigned char> self._input_data[self._input_pos - 1]
                | <uint64_t> <unsigned char> self._input_data[self._input_pos - 2] << 8
                | <uint64_t> <unsigned char> self._input_data[self._input_pos - 3] << 16
                | <uint64_t> <unsigned char> self._input_data[self._input_pos - 4] << 24
                | <uint64_t> <unsigned char> self._input_data[self._input_pos - 5] << 32
                | <uint64_t> <unsigned char> self._input_data[self._input_pos - 6] << 40
                | <uint64_t> <unsigned char> self._input_data[self._input_pos - 7] << 48
                | <uint64_t> <unsigned char> self._input_data[self._input_pos - 8] << 56)

    cdef float read_float(self) except? -1:
        cdef int32_t as_int = self.read_int32()
        return (<float*> <char*> &as_int)[0]

    cdef double read_double(self) except? -1:
        cdef int64_t as_long = self.read_int64()
        return (<double*> <char*> &as_long)[0]

    cdef bytes read_bytes(self):
        cdef int32_t size = self.read_int32()
        self._input_pos += size
        return self._input_data[self._input_pos - size: self._input_pos]

cdef class OutputStream:
    def __cinit__(self):
        self.buffer = <char*> malloc(1024)
        if self.buffer == NULL:
            raise MemoryError("OutputStream allocates buffer failed")

    def __dealloc__(self):
        if self.buffer != NULL:
            free(self.buffer)

    def __init__(self):
        self.buffer_size = 1024
        self.pos = 0

    cpdef void write(self, bytes v):
        cdef size_t length
        cdef char*b
        length = len(v)
        b = <char*> v
        if self.buffer_size < self.pos + length:
            self._extend(length)
        if length < 8:
            # This is faster than memcpy when the string is short.
            for i in range(length):
                self.buffer[self.pos + i] = b[i]
        else:
            memcpy(self.buffer + self.pos, b, length)
        self.pos += length

    cdef void write_byte(self, unsigned char v):
        if self.buffer_size < self.pos + 1:
            self._extend(1)
        self.buffer[self.pos] = v
        self.pos += 1

    cdef void write_int8(self, int8_t v):
        if self.buffer_size < self.pos + 1:
            self._extend(1)
        self.buffer[self.pos] = <unsigned char> v
        self.pos += 1

    cdef void write_int16(self, int16_t v):
        if self.buffer_size < self.pos + 2:
            self._extend(2)
        self.buffer[self.pos] = <unsigned char> (v >> 8)
        self.buffer[self.pos + 1] = <unsigned char> v
        self.pos += 2

    cdef void write_int32(self, int32_t v):
        if self.buffer_size < self.pos + 4:
            self._extend(4)
        self.buffer[self.pos] = <unsigned char> (v >> 24)
        self.buffer[self.pos + 1] = <unsigned char> (v >> 16)
        self.buffer[self.pos + 2] = <unsigned char> (v >> 8)
        self.buffer[self.pos + 3] = <unsigned char> v
        self.pos += 4

    cdef void write_int64(self, int64_t v):
        if self.buffer_size < self.pos + 8:
            self._extend(8)
        self.buffer[self.pos] = <unsigned char> (v >> 56)
        self.buffer[self.pos + 1] = <unsigned char> (v >> 48)
        self.buffer[self.pos + 2] = <unsigned char> (v >> 40)
        self.buffer[self.pos + 3] = <unsigned char> (v >> 32)
        self.buffer[self.pos + 4] = <unsigned char> (v >> 24)
        self.buffer[self.pos + 5] = <unsigned char> (v >> 16)
        self.buffer[self.pos + 6] = <unsigned char> (v >> 8)
        self.buffer[self.pos + 7] = <unsigned char> v
        self.pos += 8

    cdef void write_float(self, float v):
        self.write_int32((<int32_t*> <char*> &v)[0])

    cdef void write_double(self, double v):
        self.write_int64((<int64_t*> <char*> &v)[0])

    cdef void write_bytes(self, char*b, size_t length):
        cdef size_t i
        self.write_int32(length)
        if self.buffer_size < self.pos + length:
            self._extend(length)
        if length < 8:
            # This is faster than memcpy when the string is short.
            for i in range(length):
                self.buffer[self.pos + i] = b[i]
        else:
            memcpy(self.buffer + self.pos, b, length)
        self.pos += length

    cdef void _extend(self, size_t missing):
        while self.buffer_size < self.pos + missing:
            self.buffer_size *= 2
        self.buffer = <char*> realloc(self.buffer, self.buffer_size)
