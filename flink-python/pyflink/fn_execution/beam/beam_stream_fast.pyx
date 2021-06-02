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

from libc.stdlib cimport realloc
from libc.string cimport memcpy

cdef class BeamInputStream(LengthPrefixInputStream):
    def __cinit__(self, input_stream, size):
        self._input_buffer_size = size
        self._input_pos = 0
        self._parse_input_stream(input_stream)

    cdef size_t read(self, char** data):
        cdef size_t length = 0
        cdef bint has_prefix = True
        cdef size_t shift = 0
        cdef char bits
        # read the var-int size
        while has_prefix:
            bits = self._input_data[self._input_pos] & 0x7F
            length |= bits << shift
            shift += 7
            if not (self._input_data[self._input_pos] & 0x80):
                has_prefix = False
            self._input_pos += 1
        data[0] = self._input_data + self._input_pos
        self._input_pos += length
        return length

    cdef size_t available(self):
        return self._input_buffer_size - self._input_pos

    cdef void _parse_input_stream(self, BInputStream input_stream):
        self._input_data = input_stream.allc
        input_stream.pos = self._input_buffer_size

cdef class BeamOutputStream(LengthPrefixOutputStream):
    def __cinit__(self, output_stream):
        self._output_stream = output_stream
        self._parse_output_stream(output_stream)

    cdef void write(self, char*data, size_t length):
        cdef char bits
        cdef size_t size = length
        # the length of the variable prefix length will be less than 9 bytes
        if self._output_buffer_size < self._output_pos + length + 9:
            self._output_buffer_size += length + 9
            self._output_data = <char*> realloc(self._output_data,
                                                self._output_buffer_size)
        # write variable prefix length
        while size:
            bits = size & 0x7F
            size >>= 7
            if size:
                bits |= 0x80
            self._output_data[self._output_pos] = bits
            self._output_pos += 1

        if length < 8:
            # This is faster than memcpy when the string is short.
            for i in range(length):
                self._output_data[self._output_pos + i] = data[i]
        else:
            memcpy(self._output_data + self._output_pos, data, length)
        self._output_pos += length
        self._maybe_flush()

    cpdef void flush(self):
        cdef size_t i
        self._map_output_data_to_output_stream()
        self._output_stream.maybe_flush()

    cdef void _parse_output_stream(self, BOutputStream output_stream):
        self._output_data = output_stream.data
        self._output_pos = output_stream.pos
        self._output_buffer_size = output_stream.buffer_size

    cdef void _maybe_flush(self):
        if self._output_pos > 10_000_000:
            self._map_output_data_to_output_stream()
            self._output_stream.flush()
            self._output_pos = 0

    cdef void _map_output_data_to_output_stream(self):
        self._output_stream.data = self._output_data
        self._output_stream.pos = self._output_pos
        self._output_stream.buffer_size = self._output_buffer_size
