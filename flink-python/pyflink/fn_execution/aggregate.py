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
from abc import ABC, abstractmethod
from typing import List, Dict

from apache_beam.coders import PickleCoder, Coder

from pyflink.common import Row, RowKind
from pyflink.common.state import ListState, MapState
from pyflink.fn_execution.coders import from_proto
from pyflink.fn_execution.operation_utils import is_built_in_function, load_aggregate_function
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend
from pyflink.table import AggregateFunction, FunctionContext
from pyflink.table.data_view import ListView, MapView


def join_row(left: Row, right: Row):
    fields = []
    for value in left:
        fields.append(value)
    for value in right:
        fields.append(value)
    return Row(*fields)


def extract_data_view_specs_from_accumulator(current_index, accumulator):
    # for built in functions we extract the data view specs from their accumulator
    i = -1
    extracted_specs = []
    for field in accumulator:
        i += 1
        # TODO: infer the coder from the input types and output type of the built-in functions
        if isinstance(field, MapView):
            extracted_specs.append(MapViewSpec(
                "builtInAgg%df%d" % (current_index, i), i, PickleCoder(), PickleCoder()))
        elif isinstance(field, ListView):
            extracted_specs.append(ListViewSpec(
                "builtInAgg%df%d" % (current_index, i), i, PickleCoder()))
    return extracted_specs


def extract_data_view_specs(udfs):
    extracted_udf_data_view_specs = []
    current_index = -1
    for udf in udfs:
        current_index += 1
        udf_data_view_specs_proto = udf.specs
        if not udf_data_view_specs_proto:
            if is_built_in_function(udf.payload):
                built_in_function = load_aggregate_function(udf.payload)
                accumulator = built_in_function.create_accumulator()
                extracted_udf_data_view_specs.append(
                    extract_data_view_specs_from_accumulator(current_index, accumulator))
            else:
                extracted_udf_data_view_specs.append([])
        else:
            extracted_specs = []
            for spec_proto in udf_data_view_specs_proto:
                state_id = spec_proto.name
                field_index = spec_proto.field_index
                if spec_proto.HasField("list_view"):
                    element_coder = from_proto(spec_proto.list_view.element_type)
                    extracted_specs.append(ListViewSpec(state_id, field_index, element_coder))
                elif spec_proto.HasField("map_view"):
                    key_coder = from_proto(spec_proto.map_view.key_type)
                    value_coder = from_proto(spec_proto.map_view.value_type)
                    extracted_specs.append(
                        MapViewSpec(state_id, field_index, key_coder, value_coder))
                else:
                    raise Exception("Unsupported data view spec type: " + spec_proto.type)
            extracted_udf_data_view_specs.append(extracted_specs)
    if all([len(i) == 0 for i in extracted_udf_data_view_specs]):
        return []
    return extracted_udf_data_view_specs


class StateListView(ListView):

    def __init__(self, list_state: ListState):
        super().__init__()
        self._list_state = list_state

    def get(self):
        return self._list_state.get()

    def add(self, value):
        self._list_state.add(value)

    def add_all(self, values):
        self._list_state.add_all(values)

    def clear(self):
        self._list_state.clear()

    def __hash__(self) -> int:
        return hash([i for i in self.get()])


class StateMapView(MapView):

    def __init__(self, map_state: MapState):
        super().__init__()
        self._map_state = map_state

    def get(self, key):
        return self._map_state.get(key)

    def put(self, key, value) -> None:
        self._map_state.put(key, value)

    def put_all(self, dict_value) -> None:
        self._map_state.put_all(dict_value)

    def remove(self, key) -> None:
        self._map_state.remove(key)

    def contains(self, key) -> bool:
        return self._map_state.contains(key)

    def items(self):
        return self._map_state.items()

    def keys(self):
        return self._map_state.keys()

    def values(self):
        return self._map_state.values()

    def is_empty(self) -> bool:
        return self._map_state.is_empty()

    def clear(self) -> None:
        return self._map_state.clear()


class DataViewSpec(object):

    def __init__(self, state_id, field_index):
        self.state_id = state_id
        self.field_index = field_index


class ListViewSpec(DataViewSpec):

    def __init__(self, state_id, field_index, element_coder):
        super(ListViewSpec, self).__init__(state_id, field_index)
        self.element_coder = element_coder


class MapViewSpec(DataViewSpec):

    def __init__(self, state_id, field_index, key_coder, value_coder):
        super(MapViewSpec, self).__init__(state_id, field_index)
        self.key_coder = key_coder
        self.value_coder = value_coder


class DistinctViewDescriptor(object):

    def __init__(self, input_extractor, filter_args):
        self._input_extractor = input_extractor
        self._filter_args = filter_args

    def get_input_extractor(self):
        return self._input_extractor

    def get_filter_args(self):
        return self._filter_args


class RowKeySelector(object):
    """
    A simple key selector used to extract the current key from the input Row according to the
    group-by field indexes.
    """

    def __init__(self, grouping):
        self.grouping = grouping

    def get_key(self, data: Row):
        return Row(*[data[i] for i in self.grouping])


class StateDataViewStore(object):
    """
    The class used to manage the DataViews used in :class:`AggsHandleFunction`. Currently
    DataView is not supported so it is just a wrapper of the :class:`FunctionContext`.
    """

    def __init__(self,
                 function_context: FunctionContext,
                 keyed_state_backend: RemoteKeyedStateBackend):
        self._function_context = function_context
        self._keyed_state_backend = keyed_state_backend

    def get_runtime_context(self):
        return self._function_context

    def get_state_list_view(self, state_name, element_coder):
        return StateListView(self._keyed_state_backend.get_list_state(state_name, element_coder))

    def get_state_map_view(self, state_name, key_coder, value_coder):
        return StateMapView(
            self._keyed_state_backend.get_map_state(state_name, key_coder, value_coder))


class AggsHandleFunction(ABC):
    """
    The base class for handling aggregate functions.
    """

    @abstractmethod
    def open(self, state_data_view_store):
        """
        Initialization method for the function. It is called before the actual working methods.

        :param state_data_view_store: The object used to manage the DataView.
        """
        pass

    @abstractmethod
    def accumulate(self, input_data: Row):
        """
        Accumulates the input values to the accumulators.

        :param input_data: Input values bundled in a row.
        """
        pass

    @abstractmethod
    def retract(self, input_data: Row):
        """
        Retracts the input values from the accumulators.

        :param input_data: Input values bundled in a row.
        """

    @abstractmethod
    def merge(self, accumulators: Row):
        """
        Merges the other accumulators into current accumulators.

        :param accumulators: The other row of accumulators.
        """
        pass

    @abstractmethod
    def set_accumulators(self, accumulators: Row):
        """
        Set the current accumulators (saved in a row) which contains the current aggregated results.

        In streaming: accumulators are stored in the state, we need to restore aggregate buffers
        from state.

        In batch: accumulators are stored in the dict, we need to restore aggregate buffers from
        dict.

        :param accumulators: Current accumulators.
        """
        pass

    @abstractmethod
    def get_accumulators(self) -> Row:
        """
        Gets the current accumulators (saved in a row) which contains the current
        aggregated results.

        :return: The current accumulators.
        """
        pass

    @abstractmethod
    def create_accumulators(self) -> Row:
        """
        Initializes the accumulators and save them to an accumulators row.

        :return: A row of accumulators which contains the aggregated results.
        """
        pass

    @abstractmethod
    def cleanup(self):
        """
        Cleanup for the retired accumulators state.
        """
        pass

    @abstractmethod
    def get_value(self) -> Row:
        """
        Gets the result of the aggregation from the current accumulators.

        :return: The final result (saved in a row) of the current accumulators.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Tear-down method for this function. It can be used for clean up work.
        By default, this method does nothing.
        """
        pass


class SimpleAggsHandleFunction(AggsHandleFunction):
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
        self._udfs = udfs
        self._input_extractors = input_extractors
        self._accumulators = None  # type: Row
        self._get_value_indexes = [i for i in range(len(udfs))]
        if index_of_count_star >= 0 and count_star_inserted:
            # The record count is used internally, should be ignored by the get_value method.
            self._get_value_indexes.remove(index_of_count_star)
        self._udf_data_view_specs = udf_data_view_specs
        self._udf_data_views = []
        self._filter_args = filter_args
        self._distinct_indexes = distinct_indexes
        self._distinct_view_descriptors = distinct_view_descriptors
        self._distinct_data_views = {}

    def open(self, state_data_view_store):
        for udf in self._udfs:
            udf.open(state_data_view_store.get_runtime_context())
        self._udf_data_views = []
        for data_view_specs in self._udf_data_view_specs:
            data_views = {}
            for data_view_spec in data_view_specs:
                if isinstance(data_view_spec, ListViewSpec):
                    data_views[data_view_spec.field_index] = \
                        state_data_view_store.get_state_list_view(
                            data_view_spec.state_id,
                            PickleCoder())
                elif isinstance(data_view_spec, MapViewSpec):
                    data_views[data_view_spec.field_index] = \
                        state_data_view_store.get_state_map_view(
                            data_view_spec.state_id,
                            PickleCoder(),
                            PickleCoder())
            self._udf_data_views.append(data_views)
        for key in self._distinct_view_descriptors.keys():
            self._distinct_data_views[key] = state_data_view_store.get_state_map_view(
                "agg%ddistinct" % key,
                PickleCoder(),
                PickleCoder())

    def accumulate(self, input_data: Row):
        for i in range(len(self._udfs)):
            if i in self._distinct_data_views:
                if len(self._distinct_view_descriptors[i].get_filter_args()) == 0:
                    filtered = False
                else:
                    filtered = True
                    for filter_arg in self._distinct_view_descriptors[i].get_filter_args():
                        if input_data[filter_arg]:
                            filtered = False
                            break
                if not filtered:
                    input_extractor = self._distinct_view_descriptors[i].get_input_extractor()
                    args = input_extractor(input_data)
                    if args in self._distinct_data_views[i]:
                        self._distinct_data_views[i][args] += 1
                    else:
                        self._distinct_data_views[i][args] = 1
            if self._filter_args[i] >= 0 and not input_data[self._filter_args[i]]:
                continue
            input_extractor = self._input_extractors[i]
            args = input_extractor(input_data)
            if self._distinct_indexes[i] >= 0:
                if args in self._distinct_data_views[self._distinct_indexes[i]]:
                    if self._distinct_data_views[self._distinct_indexes[i]][args] > 1:
                        continue
                else:
                    raise Exception(
                        "The args are not in the distinct data view, this should not happen.")
            self._udfs[i].accumulate(self._accumulators[i], *args)

    def retract(self, input_data: Row):
        for i in range(len(self._udfs)):
            if i in self._distinct_data_views:
                if len(self._distinct_view_descriptors[i].get_filter_args()) == 0:
                    filtered = False
                else:
                    filtered = True
                    for filter_arg in self._distinct_view_descriptors[i].get_filter_args():
                        if input_data[filter_arg]:
                            filtered = False
                            break
                if not filtered:
                    input_extractor = self._distinct_view_descriptors[i].get_input_extractor()
                    args = input_extractor(input_data)
                    if args in self._distinct_data_views[i]:
                        self._distinct_data_views[i][args] -= 1
                        if self._distinct_data_views[i][args] == 0:
                            del self._distinct_data_views[i][args]
            if self._filter_args[i] >= 0 and not input_data[self._filter_args[i]]:
                continue
            input_extractor = self._input_extractors[i]
            args = input_extractor(input_data)
            if self._distinct_indexes[i] >= 0 and \
                    args in self._distinct_data_views[self._distinct_indexes[i]]:
                continue
            self._udfs[i].retract(self._accumulators[i], *args)

    def merge(self, accumulators: Row):
        for i in range(len(self._udfs)):
            self._udfs[i].merge(self._accumulators[i], [accumulators[i]])

    def set_accumulators(self, accumulators: Row):
        if self._udf_data_views:
            for i in range(len(self._udf_data_views)):
                for index, data_view in self._udf_data_views[i].items():
                    accumulators[i][index] = data_view
        self._accumulators = accumulators

    def get_accumulators(self):
        return self._accumulators

    def create_accumulators(self):
        return Row(*[udf.create_accumulator() for udf in self._udfs])

    def cleanup(self):
        for i in range(len(self._udf_data_views)):
            for data_view in self._udf_data_views[i].values():
                data_view.clear()

    def get_value(self):
        return Row(*[self._udfs[i].get_value(self._accumulators[i])
                     for i in self._get_value_indexes])

    def close(self):
        for udf in self._udfs:
            udf.close()


class RecordCounter(ABC):
    """
    The RecordCounter is used to count the number of input records under the current key.
    """

    @abstractmethod
    def record_count_is_zero(self, acc):
        pass

    @staticmethod
    def of(index_of_count_star):
        if index_of_count_star >= 0:
            return RetractionRecordCounter(index_of_count_star)
        else:
            return AccumulationRecordCounter()


class AccumulationRecordCounter(RecordCounter):

    def record_count_is_zero(self, acc):
        # when all the inputs are accumulations, the count will never be zero
        return acc is None


class RetractionRecordCounter(RecordCounter):

    def __init__(self, index_of_count_star):
        self._index_of_count_star = index_of_count_star

    def record_count_is_zero(self, acc):
        # We store the counter in the accumulator and the counter is never be null
        return acc is None or acc[self._index_of_count_star][0] == 0


class GroupAggFunction(object):

    def __init__(self,
                 aggs_handle: AggsHandleFunction,
                 key_selector: RowKeySelector,
                 state_backend: RemoteKeyedStateBackend,
                 state_value_coder: Coder,
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

    def open(self, function_context: FunctionContext):
        self.aggs_handle.open(StateDataViewStore(function_context, self.state_backend))

    def close(self):
        self.aggs_handle.close()

    def process_element(self, input_data: Row):
        key = self.key_selector.get_key(input_data)
        self.state_backend.set_current_key(key)
        self.state_backend.clear_cached_iterators()
        accumulator_state = self.state_backend.get_value_state(
            "accumulators", self.state_value_coder)
        accumulators = accumulator_state.value()
        if accumulators is None:
            if self.is_retract_msg(input_data):
                # Don't create a new accumulator for a retraction message. This might happen if the
                # retraction message is the first message for the key or after a state clean up.
                return
            first_row = True
            accumulators = self.aggs_handle.create_accumulators()
        else:
            first_row = False

        # set accumulators to handler first
        self.aggs_handle.set_accumulators(accumulators)
        # get previous aggregate result
        pre_agg_value = self.aggs_handle.get_value()

        # update aggregate result and set to the newRow
        if self.is_accumulate_msg(input_data):
            # accumulate input
            self.aggs_handle.accumulate(input_data)
        else:
            # retract input
            self.aggs_handle.retract(input_data)

        # get current aggregate result
        new_agg_value = self.aggs_handle.get_value()

        # get accumulator
        accumulators = self.aggs_handle.get_accumulators()

        if not self.record_counter.record_count_is_zero(accumulators):
            # we aggregated at least one record for this key

            # update the state
            accumulator_state.update(accumulators)

            # if this was not the first row and we have to emit retractions
            if not first_row:
                if not self.state_cleaning_enabled and pre_agg_value == new_agg_value:
                    # newRow is the same as before and state cleaning is not enabled.
                    # We do not emit retraction and acc message.
                    # If state cleaning is enabled, we have to emit messages to prevent too early
                    # state eviction of downstream operators.
                    return
                else:
                    # retract previous result
                    if self.generate_update_before:
                        # prepare UPDATE_BEFORE message for previous row
                        retract_row = join_row(key, pre_agg_value)
                        retract_row.set_row_kind(RowKind.UPDATE_BEFORE)
                        yield retract_row
                    # prepare UPDATE_AFTER message for new row
                    result_row = join_row(key, new_agg_value)
                    result_row.set_row_kind(RowKind.UPDATE_AFTER)
            else:
                # this is the first, output new result
                # prepare INSERT message for new row
                result_row = join_row(key, new_agg_value)
                result_row.set_row_kind(RowKind.INSERT)
            yield result_row
        else:
            # we retracted the last record for this key
            # sent out a delete message
            if not first_row:
                # prepare delete message for previous row
                result_row = join_row(key, pre_agg_value)
                result_row.set_row_kind(RowKind.DELETE)
                yield result_row
            # and clear all state
            accumulator_state.clear()
            # cleanup dataview under current key
            self.aggs_handle.cleanup()

    def on_timer(self, key):
        if self.state_cleaning_enabled:
            self.state_backend.set_current_key(key)
            accumulator_state = self.state_backend.get_value_state(
                "accumulators", self.state_value_coder)
            accumulator_state.clear()
            self.aggs_handle.cleanup()

    @staticmethod
    def is_retract_msg(data: Row):
        return data.get_row_kind() == RowKind.UPDATE_BEFORE \
            or data.get_row_kind() == RowKind.DELETE

    @staticmethod
    def is_accumulate_msg(data: Row):
        return data.get_row_kind() == RowKind.UPDATE_AFTER \
            or data.get_row_kind() == RowKind.INSERT
