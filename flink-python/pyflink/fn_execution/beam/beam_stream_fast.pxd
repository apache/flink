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

from apache_beam.coders.coder_impl cimport InputStream as BInputStream
from apache_beam.coders.coder_impl cimport OutputStream as BOutputStream

from pyflink.fn_execution.stream_fast cimport LengthPrefixInputStream, LengthPrefixOutputStream

cdef class BeamInputStream(LengthPrefixInputStream):
    cdef char*_input_data
    cdef size_t _input_buffer_size
    cdef size_t _input_pos
    cdef BInputStream _input_stream
    cdef void _parse_input_stream(self, BInputStream input_stream)

cdef class BeamSizeBasedOutputStream(LengthPrefixOutputStream):
    cdef char*_output_data
    cdef size_t _output_pos
    cdef size_t _output_buffer_size
    cdef BOutputStream _output_stream

    cdef void reset_output_stream(self, BOutputStream output_stream)
    cpdef bint maybe_flush(self)

cdef class BeamTimeBasedOutputStream(BeamSizeBasedOutputStream):
    cdef bint _flush_event
    cdef object _periodic_flusher

    cpdef void notify_flush(self)
