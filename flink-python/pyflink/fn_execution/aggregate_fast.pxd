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
from pyflink.fn_execution.coder_impl_fast cimport InternalRow, InternalRowKind

cdef class DistinctViewDescriptor:
    cdef object input_extractor
    cdef int*filter_args
    cdef size_t filter_length

cdef class RowKeySelector:
    cdef size_t*grouping
    cdef size_t length

    cdef list get_key(self, list data)

cdef class AggsHandleFunctionBase:
    cdef void open(self, object state_data_view_store)
    cdef void accumulate(self, list input_data)
    cdef void retract(self, list input_data)
    cdef void merge(self, list accumulators)
    cdef void set_accumulators(self, list accumulators)
    cdef list get_accumulators(self)
    cdef list create_accumulators(self)
    cdef void cleanup(self)
    cdef void close(self)
    cdef list get_value(self)
    cdef list emit_value(self, list current_key, bint is_retract)

cdef class SimpleAggsHandleFunctionBase(AggsHandleFunctionBase):
    cdef list _udfs
    cdef size_t _udf_num
    cdef list _input_extractors
    cdef list _accumulators
    cdef list _udf_data_view_specs
    cdef list _udf_data_views
    cdef int*_filter_args
    cdef int*_distinct_indexes
    cdef dict _distinct_view_descriptors
    cdef dict _distinct_data_views

cdef class SimpleAggsHandleFunction(SimpleAggsHandleFunctionBase):
    cdef size_t*_get_value_indexes
    cdef size_t _get_value_indexes_length

cdef class SimpleTableAggsHandleFunction(SimpleAggsHandleFunctionBase):
    pass

cdef class RecordCounter:
    cdef bint record_count_is_zero(self, list acc)

    @staticmethod
    cdef RecordCounter of(int index_of_count_star)

cdef class AccumulationRecordCounter(RecordCounter):
    pass

cdef class RetractionRecordCounter(RecordCounter):
    cdef int _index_of_count_star

cdef class GroupAggFunctionBase:
    cdef AggsHandleFunctionBase aggs_handle
    cdef bint generate_update_before
    cdef bint state_cleaning_enabled
    cdef RowKeySelector key_selector
    cdef object state_value_coder
    cdef object state_backend
    cdef RecordCounter record_counter
    cdef dict buffer

    cpdef void open(self, function_context)
    cpdef void close(self)
    cpdef void process_element(self, InternalRow input_data)
    cpdef list finish_bundle(self)
    cpdef void on_timer(self, InternalRow key)
    cdef bint is_retract_msg(self, InternalRowKind row_kind)
    cdef bint is_accumulate_msg(self, InternalRowKind row_kind)

cdef class GroupAggFunction(GroupAggFunctionBase):
    pass

cdef class GroupTableAggFunction(GroupAggFunctionBase):
    pass
