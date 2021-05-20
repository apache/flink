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
cimport libc
from pyflink.fn_execution.table.aggregate_fast cimport RowKeySelector
from pyflink.fn_execution.coder_impl_fast cimport InternalRow

cdef class NamespaceAggsHandleFunctionBase:
    cdef void open(self, object state_data_view_store)
    cdef void accumulate(self, list input_data)
    cdef void retract(self, list input_data)
    cpdef void merge(self, object namespace, list accumulators)
    cpdef void set_accumulators(self, object namespace, list accumulators)
    cdef list get_accumulators(self)
    cpdef list create_accumulators(self)
    cpdef void cleanup(self, object namespace)
    cdef void close(self)

cdef class NamespaceAggsHandleFunction(NamespaceAggsHandleFunctionBase):
    cpdef list get_value(self, object namespace)

cdef class SimpleNamespaceAggsHandleFunction(NamespaceAggsHandleFunction):
    cdef list _udfs
    cdef size_t _udf_num
    cdef list _input_extractors
    cdef object _named_property_extractor
    cdef list _accumulators
    cdef list _udf_data_view_specs
    cdef list _udf_data_views
    cdef int*_filter_args
    cdef int*_distinct_indexes
    cdef dict _distinct_view_descriptors
    cdef dict _distinct_data_views
    cdef object named_property_extractor
    cdef size_t*_get_value_indexes
    cdef size_t _get_value_indexes_length

cdef class GroupWindowAggFunctionBase:
    cdef libc.stdint.int64_t _allowed_lateness
    cdef int _rowtime_index
    cdef str _shift_timezone
    cdef RowKeySelector _key_selector
    cdef object _state_value_coder
    cdef object _state_backend
    cdef object _window_assigner
    cdef NamespaceAggsHandleFunctionBase _window_aggregator
    cdef object _trigger
    cdef object _window_function
    cdef object _internal_timer_service
    cdef object _window_context
    cdef object _trigger_context
    cdef object _window_state

    cpdef void open(self, object function_context)
    cpdef void close(self)
    cpdef list process_element(self, InternalRow input_data)
    cpdef void process_watermark(self, libc.stdint.int64_t watermark)
    cpdef list on_event_time(self, object timer)
    cpdef list on_processing_time(self, object timer)
    cpdef list get_timers(self)
    cpdef libc.stdint.int64_t to_utc_timestamp_mills(self, libc.stdint.int64_t epoch_mills)
    cpdef libc.stdint.int64_t cleanup_time(self, object window)
    cdef void _register_cleanup_timer(self, object window)
    cdef InternalRow _emit_window_result(self, list key, object window)

cdef class GroupWindowAggFunction(GroupWindowAggFunctionBase):
    pass
