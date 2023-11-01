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
from libc.stdlib cimport free, malloc
from typing import List, Dict

from pyflink.common import Row
from pyflink.fn_execution.coders import PickleCoder
from pyflink.fn_execution.table.state_data_view import DataViewSpec, ListViewSpec, MapViewSpec, \
    PerKeyStateDataViewStore
from pyflink.table import AggregateFunction, TableAggregateFunction

cdef InternalRow join_row(list left, list right, InternalRowKind row_kind):
    return InternalRow(left.__add__(right), row_kind)

cdef class DistinctViewDescriptor:
    def __cinit__(self, input_extractor, filter_args):
        self.input_extractor = input_extractor
        self.filter_length = len(filter_args)
        self.filter_args = <int*> malloc(self.filter_length * sizeof(int))
        for i in range(self.filter_length):
            self.filter_args[i] = filter_args[i]

    def __dealloc__(self):
        if self.filter_args:
            free(self.filter_args)

cdef class RowKeySelector(object):
    """
    A simple key selector used to extract the current key from the input Row according to the
    group-by field indexes.
    """

    def __cinit__(self, list grouping):
        self.length = len(grouping)
        self.grouping = <size_t*> malloc(self.length * sizeof(size_t))
        for i in range(self.length):
            self.grouping[i] = grouping[i]

    cdef list get_key(self, list data):
        cdef size_t i
        return [data[self.grouping[i]] for i in range(self.length)]

    def __dealloc__(self):
        if self.grouping:
            free(self.grouping)

cdef class AggsHandleFunctionBase:
    """
    The base class for handling aggregate or table aggregate functions.
    """
    cdef void open(self, object state_data_view_store):
        """
        Initialization method for the function. It is called before the actual working methods.

        :param state_data_view_store: The object used to manage the DataView.
        """
        pass

    cdef void accumulate(self, InternalRow input_data):
        """
        Accumulates the input values to the accumulators.

        :param input_data: Input values bundled in a InternalRow.
        """
        pass

    cdef void retract(self, InternalRow input_data):
        """
        Retracts the input values from the accumulators.

        :param input_data: Input values bundled in a InternalRow.
        """
        pass

    cdef void merge(self, list accumulators):
        """
        Merges the other accumulators into current accumulators.

        :param accumulators: The other List of accumulators.
        """
        pass

    cdef void set_accumulators(self, list accumulators):
        """
        Set the current accumulators (saved in a List) which contains the current aggregated
        results.

        In streaming: accumulators are stored in the state, we need to restore aggregate buffers
        from state.

        In batch: accumulators are stored in the dict, we need to restore aggregate buffers from
        dict.

        :param accumulators: Current accumulators.
        """
        pass

    cdef list get_accumulators(self):
        """
        Gets the current accumulators (saved in a list) which contains the current
        aggregated results.

        :return: The current accumulators.
        """
        pass

    cdef list create_accumulators(self):
        """
        Initializes the accumulators and save them to an accumulators List.

        :return: A List of accumulators which contains the aggregated results.
        """
        pass

    cdef void cleanup(self):
        """
        Cleanup for the retired accumulators state.
        """
        pass

    cdef void close(self):
        """
        Tear-down method for this function. It can be used for clean up work.
        By default, this method does nothing.
        """
        pass

    cdef list get_value(self):
        """
        Gets the result of the aggregation from the current accumulators.

        :return: The final result (saved in a List) of the current accumulators.
        """
        pass

    cdef list emit_value(self, list current_key, bint is_retract):
        """
        Emit the result of the table aggregation.
        """
        pass

cdef class SimpleAggsHandleFunctionBase(AggsHandleFunctionBase):
    """
    A simple AggsHandleFunction implementation which provides the basic functionality.
    """

    def __init__(self,
                 udfs: List[AggregateFunction],
                 input_extractors: List,
                 udf_data_view_specs: List[List[DataViewSpec]],
                 filter_args: List[int],
                 distinct_indexes: List[int],
                 distinct_view_descriptors: Dict[int, DistinctViewDescriptor]):
        self._udfs = udfs
        self._udf_num = len(self._udfs)
        self._input_extractors = input_extractors
        self._accumulators = None
        self._udf_data_view_specs = udf_data_view_specs
        self._udf_data_views = []
        self._filter_args = <int*> malloc(self._udf_num * sizeof(int))
        self._distinct_indexes = <int*> malloc(self._udf_num * sizeof(int))
        for i in range(self._udf_num):
            self._filter_args[i] = filter_args[i]
            self._distinct_indexes[i] = distinct_indexes[i]
        self._distinct_view_descriptors = distinct_view_descriptors
        self._distinct_data_views = {}

    cdef void open(self, object state_data_view_store):
        cdef dict data_views
        for udf in self._udfs:
            udf.open(state_data_view_store.get_runtime_context())
        for data_view_specs in self._udf_data_view_specs:
            data_views = {}
            for data_view_spec in data_view_specs:
                if isinstance(data_view_spec, ListViewSpec):
                    data_views[data_view_spec.field_index] = \
                        state_data_view_store.get_state_list_view(
                            data_view_spec.state_id,
                            data_view_spec.element_coder)
                elif isinstance(data_view_spec, MapViewSpec):
                    data_views[data_view_spec.field_index] = \
                        state_data_view_store.get_state_map_view(
                            data_view_spec.state_id,
                            data_view_spec.key_coder,
                            data_view_spec.value_coder)
            self._udf_data_views.append(data_views)
        for key in self._distinct_view_descriptors.keys():
            self._distinct_data_views[key] = state_data_view_store.get_state_map_view(
                "agg%ddistinct" % key,
                PickleCoder(),
                PickleCoder())

    cdef void accumulate(self, InternalRow input_data):
        cdef size_t i, j, filter_length
        cdef int distinct_index, filter_arg
        cdef int*filter_args
        cdef bint filtered
        cdef DistinctViewDescriptor distinct_view_descriptor
        cdef object distinct_data_view
        cdef InternalRow internal_row
        for i in range(self._udf_num):
            if i in self._distinct_data_views:
                distinct_view_descriptor = self._distinct_view_descriptors[i]
                filter_length = distinct_view_descriptor.filter_length
                if filter_length == 0:
                    filtered = False
                else:
                    filtered = True
                    filter_args = distinct_view_descriptor.filter_args
                    for j in range(filter_length):
                        filter_arg = filter_args[j]
                        if input_data[filter_arg]:
                            filtered = False
                            break
                if not filtered:
                    args = distinct_view_descriptor.input_extractor(input_data)
                    distinct_data_view = self._distinct_data_views[i]
                    if args in distinct_data_view:
                        distinct_data_view[args] += 1
                    else:
                        distinct_data_view[args] = 1
            filter_arg = self._filter_args[i]
            if filter_arg >= 0 and not input_data[filter_arg]:
                continue
            args = self._input_extractors[i](input_data)
            distinct_index = self._distinct_indexes[i]
            if distinct_index >= 0:
                distinct_data_view = self._distinct_data_views[distinct_index]
                if args in distinct_data_view:
                    if distinct_data_view[args] > 1:
                        continue
                else:
                    raise Exception(
                        "The args are not in the distinct data view, this should not happen.")
            # Convert InternalRow to Row
            if len(args) == 1 and isinstance(args[0], InternalRow):
                internal_row = <InternalRow> args[0]
                args[0] = internal_row.to_row()
            self._udfs[i].accumulate(self._accumulators[i], *args)

    cdef void retract(self, InternalRow input_data):
        cdef size_t i, j, filter_length
        cdef int distinct_index, filter_arg
        cdef bint filtered
        cdef DistinctViewDescriptor distinct_view_descriptor
        cdef object distinct_data_view
        cdef InternalRow internal_row
        for i in range(self._udf_num):
            if i in self._distinct_data_views:
                distinct_view_descriptor = self._distinct_view_descriptors[i]
                filter_length = distinct_view_descriptor.filter_length
                if filter_length == 0:
                    filtered = False
                else:
                    filtered = True
                    for j in range(filter_length):
                        filter_arg = distinct_view_descriptor.filter_args[j]
                        if input_data[filter_arg]:
                            filtered = False
                            break
                if not filtered:
                    args = distinct_view_descriptor.input_extractor(input_data)
                    distinct_data_view = self._distinct_data_views[i]
                    if args in distinct_data_view:
                        distinct_data_view[args] -= 1
                        if distinct_data_view[args] == 0:
                            del distinct_data_view[args]
            filter_arg = self._filter_args[i]
            if filter_arg >= 0 and not input_data[filter_arg]:
                continue
            args = self._input_extractors[i](input_data)
            distinct_index = self._distinct_indexes[i]
            if distinct_index >= 0 and args in self._distinct_data_views[distinct_index]:
                continue
            # Convert InternalRow to Row
            if len(args) == 1 and isinstance(args[0], InternalRow):
                internal_row = <InternalRow> args[0]
                args[0] = internal_row.to_row()
            self._udfs[i].retract(self._accumulators[i], *args)

    cdef void merge(self, list accumulators):
        cdef size_t i
        for i in range(self._udf_num):
            self._udfs[i].merge(self._accumulators[i], [accumulators[i]])

    cdef void set_accumulators(self, list accumulators):
        cdef size_t i, index
        if self._udf_data_views:
            for i in range(len(self._udf_data_views)):
                for index, data_view in self._udf_data_views[i].items():
                    accumulators[i][index] = data_view
        self._accumulators = accumulators

    cdef list get_accumulators(self):
        return self._accumulators

    cdef list create_accumulators(self):
        return [udf.create_accumulator() for udf in self._udfs]

    cdef void cleanup(self):
        cdef size_t i
        for i in range(len(self._udf_data_views)):
            for data_view in self._udf_data_views[i].values():
                data_view.clear()

    cdef void close(self):
        for udf in self._udfs:
            udf.close()

    def __dealloc__(self):
        if self._filter_args:
            free(self._filter_args)
        if self._distinct_indexes:
            free(self._distinct_indexes)

cdef class SimpleAggsHandleFunction(SimpleAggsHandleFunctionBase):
    """
    A simple AggsHandleFunction implementation which provides the basic functionality.
    """

    def __init__(self,
                 udfs: List[AggregateFunction],
                 input_extractors: List,
                 index_of_count_star: int,
                 count_star_inserted: bool,
                 udf_data_view_specs: List[List[DataViewSpec]],
                 filter_args: List[int],
                 distinct_indexes: List[int],
                 distinct_view_descriptors: Dict[int, DistinctViewDescriptor]):
        super(SimpleAggsHandleFunction, self).__init__(
            udfs, input_extractors, udf_data_view_specs, filter_args, distinct_indexes,
            distinct_view_descriptors)
        temp = [i for i in range(len(udfs))]
        if index_of_count_star >= 0 and count_star_inserted:
            # The record count is used internally, should be ignored by the get_value method.
            temp.remove(index_of_count_star)
        self._get_value_indexes_length = len(temp)
        self._get_value_indexes = <size_t*> malloc(sizeof(size_t) * self._get_value_indexes_length)
        for i in range(self._get_value_indexes_length):
            self._get_value_indexes[i] = temp[i]

    cdef list get_value(self):
        cdef size_t i
        cdef size_t*get_value_indexes
        get_value_indexes = self._get_value_indexes
        return [self._udfs[get_value_indexes[i]].get_value(self._accumulators[get_value_indexes[i]])
                for i in range(self._get_value_indexes_length)]

cdef class SimpleTableAggsHandleFunction(SimpleAggsHandleFunctionBase):
    """
    A simple TableAggsHandleFunction implementation which provides the basic functionality.
    """

    def __init__(self,
                 udfs: List[TableAggregateFunction],
                 input_extractors: List,
                 udf_data_view_specs: List[List[DataViewSpec]],
                 filter_args: List[int],
                 distinct_indexes: List[int],
                 distinct_view_descriptors: Dict[int, DistinctViewDescriptor]):
        super(SimpleTableAggsHandleFunction, self).__init__(
            udfs, input_extractors, udf_data_view_specs, filter_args, distinct_indexes,
            distinct_view_descriptors)

    cdef list emit_value(self, list current_key, bint is_retract):
        cdef InternalRow result
        cdef list results
        udf = self._udfs[0]  # type: TableAggregateFunction
        results = []
        for x in udf.emit_value(self._accumulators[0]):
            if is_retract:
                result = join_row(current_key, self._convert_to_row(x), InternalRowKind.DELETE)
            else:
                result = join_row(current_key, self._convert_to_row(x), InternalRowKind.INSERT)
            results.append(result)
        return results

    cdef list _convert_to_row(self, data):
        if isinstance(data, Row):
            return data._values
        elif isinstance(data, tuple):
            return list(data)
        else:
            return [data]

cdef class RecordCounter:
    """
    The RecordCounter is used to count the number of input records under the current key.
    """

    cdef bint record_count_is_zero(self, list acc):
        pass

    @staticmethod
    cdef RecordCounter of(int index_of_count_star):
        if index_of_count_star >= 0:
            return RetractionRecordCounter(index_of_count_star)
        else:
            return AccumulationRecordCounter()

cdef class AccumulationRecordCounter(RecordCounter):
    cdef bint record_count_is_zero(self, list acc):
        # when all the inputs are accumulations, the count will never be zero
        return acc is None

cdef class RetractionRecordCounter(RecordCounter):
    def __cinit__(self, int index_of_count_star):
        self._index_of_count_star = index_of_count_star

    cdef bint record_count_is_zero(self, list acc):
        # We store the counter in the accumulator and the counter is never be null
        return acc is None or acc[self._index_of_count_star][0] == 0

cdef class GroupAggFunctionBase:
    def __init__(self,
                 aggs_handle: AggsHandleFunctionBase,
                 key_selector: RowKeySelector,
                 state_backend,
                 state_value_coder,
                 generate_update_before: bool,
                 state_cleaning_enabled: bool,
                 index_of_count_star: int):
        self.aggs_handle = aggs_handle
        self.generate_update_before = generate_update_before
        self.state_cleaning_enabled = state_cleaning_enabled
        self.key_selector = key_selector
        self.state_value_coder = state_value_coder
        self.state_backend = state_backend
        self.record_counter = RecordCounter.of(index_of_count_star)
        self.buffer = {}

    cpdef void open(self, object function_context):
        self.aggs_handle.open(PerKeyStateDataViewStore(function_context, self.state_backend))

    cpdef void close(self):
        self.aggs_handle.close()

    cpdef void on_timer(self, InternalRow key):
        if self.state_cleaning_enabled:
            self.state_backend.set_current_key(list(key.values))
            accumulator_state = self.state_backend.get_value_state(
                "accumulators", self.state_value_coder)
            accumulator_state.clear()
            self.aggs_handle.cleanup()

    cpdef void process_element(self, InternalRow input_data):
        cdef list input_value, key
        input_value = input_data.values
        key = self.key_selector.get_key(input_value)
        try:
            self.buffer[tuple(key)].append(input_data)
        except KeyError:
            self.buffer[tuple(key)] = [input_data]

    cpdef list finish_bundle(self):
        pass

cdef class GroupAggFunction(GroupAggFunctionBase):
    def __init__(self,
                 aggs_handle,
                 key_selector: RowKeySelector,
                 state_backend,
                 state_value_coder,
                 generate_update_before: bool,
                 state_cleaning_enabled: bool,
                 index_of_count_star: int):
        super(GroupAggFunction, self).__init__(
            aggs_handle, key_selector, state_backend, state_value_coder, generate_update_before,
            state_cleaning_enabled, index_of_count_star)

    cpdef list finish_bundle(self):
        cdef list results = []
        cdef bint first_row
        cdef list key, pre_agg_value, new_agg_value, accumulators, input_value, input_rows
        cdef InternalRow retract_row, result_row
        cdef SimpleAggsHandleFunction aggs_handle
        cdef InternalRowKind input_row_kind
        cdef tuple current_key
        cdef size_t input_rows_num, start_index, i
        cdef InternalRow input_data
        cdef object accumulator_state, state_backend
        aggs_handle = <SimpleAggsHandleFunction> self.aggs_handle
        state_backend = self.state_backend
        for current_key in self.buffer:
            input_rows = self.buffer[current_key]
            input_rows_num = len(input_rows)
            key = list(current_key)
            first_row = False
            state_backend.set_current_key(key)
            state_backend.clear_cached_iterators()
            accumulator_state = state_backend.get_value_state(
                "accumulators", self.state_value_coder)
            accumulators = accumulator_state.value()
            start_index = 0
            if accumulators is None:
                for i in range(input_rows_num):
                    input_data = input_rows[i]
                    if input_data.is_retract_msg():
                        start_index += 1
                    else:
                        break
                if start_index == input_rows_num:
                    continue
                accumulators = aggs_handle.create_accumulators()
                first_row = True

            # set accumulators to handler first
            aggs_handle.set_accumulators(accumulators)
            # get previous aggregate result
            pre_agg_value = aggs_handle.get_value()

            for i in range(start_index, input_rows_num):
                input_data = input_rows[i]
                # update aggregate result and set to the newRow
                if input_data.is_accumulate_msg():
                    # accumulate input
                    aggs_handle.accumulate(input_data)
                else:
                    # retract input
                    aggs_handle.retract(input_data)

            # get current aggregate result
            new_agg_value = aggs_handle.get_value()

            # get accumulator
            accumulators = aggs_handle.get_accumulators()

            if not self.record_counter.record_count_is_zero(accumulators):
                # we aggregated at least one record for this key

                # update the state
                accumulator_state.update(accumulators)

                # if this was not the first row and we have to emit retractions
                if not first_row:
                    if pre_agg_value != new_agg_value:
                        # retract previous result
                        if self.generate_update_before:
                            # prepare UPDATE_BEFORE message for previous row
                            retract_row = join_row(key, pre_agg_value,
                                                   InternalRowKind.UPDATE_BEFORE)
                            results.append(retract_row)
                        # prepare UPDATE_AFTER message for new row
                        result_row = join_row(key, new_agg_value, InternalRowKind.UPDATE_AFTER)
                        results.append(result_row)
                else:
                    # this is the first, output new result
                    # prepare INSERT message for new row
                    result_row = join_row(key, new_agg_value, InternalRowKind.INSERT)
                    results.append(result_row)
            else:
                # we retracted the last record for this key
                # sent out a delete message
                if not first_row:
                    # prepare delete message for previous row
                    result_row = join_row(key, pre_agg_value, InternalRowKind.DELETE)
                    results.append(result_row)
                # and clear all state
                accumulator_state.clear()
                # cleanup dataview under current key
                aggs_handle.cleanup()
        self.buffer = {}
        return results

cdef class GroupTableAggFunction(GroupAggFunctionBase):
    def __init__(self,
                 aggs_handle,
                 key_selector: RowKeySelector,
                 state_backend,
                 state_value_coder,
                 generate_update_before: bool,
                 state_cleaning_enabled: bool,
                 index_of_count_star: int):
        super(GroupTableAggFunction, self).__init__(
            aggs_handle, key_selector, state_backend, state_value_coder, generate_update_before,
            state_cleaning_enabled, index_of_count_star)

    cpdef list finish_bundle(self):
        cdef bint first_row
        cdef list key, accumulators, input_value, results
        cdef SimpleTableAggsHandleFunction aggs_handle
        cdef InternalRowKind input_row_kind
        cdef tuple current_key
        cdef InternalRow input_data
        cdef size_t start_index, i, input_rows_num
        cdef object state_backend, accumulator_state
        results = []
        aggs_handle = <SimpleTableAggsHandleFunction> self.aggs_handle
        state_backend = self.state_backend
        for current_key in self.buffer:
            input_rows = self.buffer[current_key]
            input_rows_num = len(input_rows)
            key = list(current_key)
            first_row = False
            state_backend.set_current_key(key)
            state_backend.clear_cached_iterators()
            accumulator_state = state_backend.get_value_state(
                "accumulators", self.state_value_coder)
            accumulators = accumulator_state.value()
            start_index = 0
            if accumulators is None:
                for i in range(input_rows_num):
                    input_data = input_rows[i]
                    if input_data.is_retract_msg():
                        start_index += 1
                    else:
                        break
                if start_index == input_rows_num:
                    continue
                accumulators = aggs_handle.create_accumulators()
                first_row = True

            # set accumulators to handler first
            aggs_handle.set_accumulators(accumulators)

            if not first_row and self.generate_update_before:
                results.extend(aggs_handle.emit_value(key, True))

            for i in range(start_index, input_rows_num):
                input_data = input_rows[i]
                # update aggregate result and set to the newRow
                if input_data.is_accumulate_msg():
                    # accumulate input
                    aggs_handle.accumulate(input_data)
                else:
                    # retract input
                    aggs_handle.retract(input_data)

            # get accumulator
            accumulators = aggs_handle.get_accumulators()

            if not self.record_counter.record_count_is_zero(accumulators):
                results.extend(aggs_handle.emit_value(key, False))
                accumulator_state.update(accumulators)
            else:
                # and clear all state
                accumulator_state.clear()
                # cleanup dataview under current key
                aggs_handle.cleanup()
        self.buffer = {}
        return results
