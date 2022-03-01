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

from apache_beam.coders.coder_impl cimport OutputStream as BOutputStream
from apache_beam.coders.coder_impl cimport StreamCoderImpl

from pyflink.fn_execution.coder_impl_fast cimport LengthPrefixBaseCoderImpl, FieldCoderImpl
from pyflink.fn_execution.beam.beam_stream_fast cimport BeamTimeBasedOutputStream
from pyflink.fn_execution.stream_fast cimport OutputStream

cdef class PassThroughLengthPrefixCoderImpl(StreamCoderImpl):
    cdef readonly StreamCoderImpl _value_coder

cdef class FlinkFieldCoderBeamWrapper(StreamCoderImpl):
    cdef FieldCoderImpl _value_coder
    cdef OutputStream _data_out_stream

    cdef void _write_data_output_stream(self, BOutputStream out_stream)

cdef class FlinkLengthPrefixCoderBeamWrapper(StreamCoderImpl):
    cdef readonly LengthPrefixBaseCoderImpl _value_coder
    cdef readonly BeamTimeBasedOutputStream _output_stream
