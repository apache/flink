#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# cython: language_level=3

cimport libc.stdint

from apache_beam.runners.worker.operations cimport Operation
from apache_beam.coders.coder_impl cimport StreamCoderImpl, CoderImpl, OutputStream, InputStream

cdef class StatelessFunctionOperation(Operation):
    cdef Operation consumer
    cdef StreamCoderImpl _value_coder_impl
    cdef dict variable_dict
    cdef list user_defined_funcs
    cdef libc.stdint.int32_t _func_num
    cdef libc.stdint.int32_t _constant_num
    cdef object func
    cdef bint _is_python_coder
    cdef bint _metric_enabled
    cdef object base_metric_group

    cdef generate_func(self, udfs)
    cdef str _extract_user_defined_function(self, user_defined_function_proto)
    cdef str _extract_user_defined_function_args(self, args)
    cdef str _parse_constant_value(self, constant_value)
    cdef void _update_gauge(self, base_metric_group)

cdef class ScalarFunctionOperation(StatelessFunctionOperation):
    pass

cdef class TableFunctionOperation(StatelessFunctionOperation):
    pass