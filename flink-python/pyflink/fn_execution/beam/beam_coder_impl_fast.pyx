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

from apache_beam.coders.coder_impl cimport InputStream as BInputStream
from apache_beam.coders.coder_impl cimport OutputStream as BOutputStream
from apache_beam.coders.coder_impl cimport StreamCoderImpl

from pyflink.fn_execution.beam.beam_stream_fast cimport BeamInputStream
from pyflink.fn_execution.beam.beam_stream_fast cimport BeamTimeBasedOutputStream
from pyflink.fn_execution.stream_fast cimport InputStream

cdef class PassThroughLengthPrefixCoderImpl(StreamCoderImpl):
    def __cinit__(self, value_coder):
        self._value_coder = value_coder

    cpdef encode_to_stream(self, value, BOutputStream out_stream, bint nested):
        self._value_coder.encode_to_stream(value, out_stream, nested)

    cpdef decode_from_stream(self, BInputStream in_stream, bint nested):
        return self._value_coder.decode_from_stream(in_stream, nested)

    cpdef get_estimated_size_and_observables(self, value, bint nested=False):
        return 0, []

cdef class FlinkFieldCoderBeamWrapper(StreamCoderImpl):
    """
    Bridge between Beam coder and Flink coder for the low-level FieldCoder.
    """
    def __cinit__(self, value_coder):
        self._value_coder = value_coder
        self._data_out_stream = OutputStream()

    cpdef encode_to_stream(self, value, BOutputStream out_stream, bint nested):
        self._value_coder.encode_to_stream(value, self._data_out_stream)
        self._write_data_output_stream(out_stream)

    cpdef decode_from_stream(self, BInputStream in_stream, bint nested):
        cdef size_t size
        cdef InputStream data_input_stream

        size = in_stream.size()

        # create InputStream
        data_input_stream = InputStream()
        data_input_stream._input_data = <char*?>in_stream.allc
        data_input_stream._input_pos = in_stream.pos

        result = self._value_coder.decode_from_stream(data_input_stream, size)
        in_stream.pos = data_input_stream._input_pos
        return result

    cdef void _write_data_output_stream(self, BOutputStream out_stream):
        cdef OutputStream data_out_stream
        cdef size_t size, i, pos
        data_out_stream = self._data_out_stream
        size = data_out_stream.pos
        pos = out_stream.pos
        # extend buffer size
        if pos + size > out_stream.buffer_size:
            out_stream.buffer_size += size
            out_stream.data = <char*> realloc(out_stream.data, out_stream.buffer_size)
        # copy data of data output stream to beam output stream
        for i in range(size):
            out_stream.data[pos + i] = data_out_stream.buffer[i]
        out_stream.pos = pos + size

        # clear data output stream
        self._data_out_stream.pos = 0


cdef class FlinkLengthPrefixCoderBeamWrapper(StreamCoderImpl):
    """
    Bridge between Beam coder and Flink coder for the top-level LengthPrefixCoder.
    """
    def __cinit__(self, value_coder):
        self._value_coder = value_coder
        self._output_stream = BeamTimeBasedOutputStream()

    cpdef encode_to_stream(self, value, BOutputStream out_stream, bint nested):
        self._output_stream.reset_output_stream(out_stream)
        self._value_coder.encode_to_stream(value, self._output_stream)

    cpdef decode_from_stream(self, BInputStream in_stream, bint nested):
        cdef BeamInputStream input_stream = BeamInputStream(in_stream, in_stream.size())
        return self._value_coder.decode_from_stream(input_stream)
