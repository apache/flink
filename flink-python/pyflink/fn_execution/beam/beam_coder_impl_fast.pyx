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

from apache_beam.coders.coder_impl cimport InputStream as BInputStream
from apache_beam.coders.coder_impl cimport OutputStream as BOutputStream
from apache_beam.coders.coder_impl cimport StreamCoderImpl

from pyflink.fn_execution.beam.beam_stream cimport BeamInputStream

cdef class PassThroughLengthPrefixCoderImpl(StreamCoderImpl):
    def __cinit__(self, value_coder):
        self._value_coder = value_coder

    cpdef encode_to_stream(self, value, BOutputStream out_stream, bint nested):
        self._value_coder.encode_to_stream(value, out_stream, nested)

    cpdef decode_from_stream(self, BInputStream in_stream, bint nested):
        return self._value_coder.decode_from_stream(in_stream, nested)

    cpdef get_estimated_size_and_observables(self, value, bint nested=False):
        return 0, []

cdef class BeamCoderImpl(StreamCoderImpl):
    def __cinit__(self, value_coder):
        self._value_coder = value_coder

    cpdef encode_to_stream(self, value, BOutputStream out_stream, bint nested):
        self._value_coder.encode(value, out_stream)

    cpdef decode_from_stream(self, BInputStream in_stream, bint nested):
        cdef BeamInputStream input_stream = BeamInputStream(in_stream, in_stream.size())
        cdef InputStreamWrapper input_stream_wrapper = InputStreamWrapper(self._value_coder,
                                                                          input_stream)
        return input_stream_wrapper

cdef class InputStreamWrapper:
    def __cinit__(self, value_coder, input_stream):
        self._value_coder = value_coder
        self._input_stream = input_stream
