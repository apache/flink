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

from apache_beam.coders.coder_impl cimport StreamCoderImpl
from apache_beam.runners.worker.operations cimport Operation

from pyflink.fn_execution.coder_impl_fast cimport BaseCoderImpl

cdef class BeamStatelessFunctionOperation(Operation):
    cdef Operation consumer
    cdef StreamCoderImpl _value_coder_impl
    cdef BaseCoderImpl _output_coder
    cdef list user_defined_funcs
    cdef object func
    cdef bint _is_python_coder
    cdef bint _metric_enabled
    cdef object base_metric_group

    cdef void _update_gauge(self, base_metric_group)

cdef class BeamScalarFunctionOperation(BeamStatelessFunctionOperation):
    pass

cdef class BeamTableFunctionOperation(BeamStatelessFunctionOperation):
    pass

cdef class DataStreamStatelessFunctionOperation(BeamStatelessFunctionOperation):
    pass
