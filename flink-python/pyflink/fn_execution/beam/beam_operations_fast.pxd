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

from apache_beam.runners.worker.operations cimport Operation
from apache_beam.utils.windowed_value cimport WindowedValue

from pyflink.fn_execution.beam.beam_coder_impl_fast cimport FlinkLengthPrefixCoderBeamWrapper
from pyflink.fn_execution.coder_impl_fast cimport InputStreamWrapper

cdef class InputProcessor:
    cpdef has_next(self)
    cpdef next(self)

cdef class NetworkInputProcessor(InputProcessor):
    cdef InputStreamWrapper _input_stream_wrapper

cdef class IntermediateInputProcessor(InputProcessor):
    cdef object _input_values
    cdef object _next_value

cdef class OutputProcessor:
    cdef Operation _consumer
    cpdef process_outputs(self, WindowedValue windowed_value, results)
    cpdef close(self)

cdef class NetworkOutputProcessor(OutputProcessor):
    cdef FlinkLengthPrefixCoderBeamWrapper _value_coder_impl

cdef class IntermediateOutputProcessor(OutputProcessor):
    pass

cdef class FunctionOperation(Operation):
    cdef OutputProcessor _output_processor
    cdef bint _is_python_coder
    cdef object process_element
    cdef object operation
    cdef object operation_cls
    cdef object _profiler
    cdef object generate_operation(self)

cdef class StatelessFunctionOperation(FunctionOperation):
    pass

cdef class StatefulFunctionOperation(FunctionOperation):
    cdef object _keyed_state_backend
    cdef WindowedValue _reusable_windowed_value
    cpdef void add_timer_info(self, timer_family_id, timer_info)
    cpdef process_timer(self, tag, timer_data)
