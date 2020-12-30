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
from typing import List, Dict, Iterable

from apache_beam.coders import PickleCoder, Coder

from pyflink.common import Row, RowKind
from pyflink.fn_execution.aggregate import DataViewSpec, ListViewSpec, MapViewSpec, \
    StateDataViewStore
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend
from pyflink.table import AggregateFunction, FunctionContext, TableAggregateFunction
from pyflink.table.udf import ImperativeAggregateFunction


def join_row(left: List, right: List):
    return Row(*(left + right))


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
    A simple key selector used to extract the current key from the input List according to the
    group-by field indexes.
    """

    def __init__(self, grouping):
        self.grouping = grouping

    def get_key(self, data):
        return [data[i] for i in self.grouping]


class AggsHandleFunctionBase(ABC):
    """
    The base class for handling aggregate or table aggregate functions.
    """

    @abstractmethod
    def open(self, state_data_view_store):
        """
        Initialization method for the function. It is called before the actual working methods.

        :param state_data_view_store: The object used to manage the DataView.
        """
        pass

    @abstractmethod
    def accumulate(self, input_data: List):
        """
        Accumulates the input values to the accumulators.

        :param input_data: Input values bundled in a List.
        """
        pass

    @abstractmethod
    def retract(self, input_data: List):
        """
        Retracts the input values from the accumulators.

        :param input_data: Input values bundled in a List.
        """

    @abstractmethod
    def merge(self, accumulators: List):
        """
        Merges the other accumulators into current accumulators.

        :param accumulators: The other List of accumulators.
        """
        pass

    @abstractmethod
    def set_accumulators(self, accumulators: List):
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

    @abstractmethod
    def get_accumulators(self) -> List:
        """
        Gets the current accumulators (saved in a list) which contains the current
        aggregated results.

        :return: The current accumulators.
        """
        pass

    @abstractmethod
    def create_accumulators(self) -> List:
        """
        Initializes the accumulators and save them to an accumulators List.

        :return: A List of accumulators which contains the aggregated results.
        """
        pass

    @abstractmethod
    def cleanup(self):
        """
        Cleanup for the retired accumulators state.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Tear-down method for this function. It can be used for clean up work.
        By default, this method does nothing.
        """
        pass


class AggsHandleFunction(AggsHandleFunctionBase):
    """
    The base class for handling aggregate functions.
    """

    @abstractmethod
    def get_value(self) -> List:
        """
        Gets the result of the aggregation from the current accumulators.

        :return: The final result (saved in a row) of the current accumulators.
        """
        pass


class TableAggsHandleFunction(AggsHandleFunctionBase):
    """
    The base class for handling table aggregate functions.
    """

    @abstractmethod
    def emit_value(self, current_key: List, is_retract: bool) -> Iterable[Row]:
        """
        Emit the result of the table aggregation.
        """
        pass


class SimpleAggsHandleFunctionBase(AggsHandleFunctionBase):
    """
    A simple AggsHandleFunctionBase implementation which provides the basic functionality.
    """

    def __init__(self,
                 udfs: List[ImperativeAggregateFunction],
                 input_extractors: List,
                 udf_data_view_specs: List[List[DataViewSpec]],
                 filter_args: List[int],
                 distinct_indexes: List[int],
                 distinct_view_descriptors: Dict[int, DistinctViewDescriptor]):
        self._udfs = udfs
        self._input_extractors = input_extractors
        self._accumulators = None  # type: List
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

    def accumulate(self, input_data: List):
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

    def retract(self, input_data: List):
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

    def merge(self, accumulators: List):
        for i in range(len(self._udfs)):
            self._udfs[i].merge(self._accumulators[i], [accumulators[i]])

    def set_accumulators(self, accumulators: List):
        if self._udf_data_views:
            for i in range(len(self._udf_data_views)):
                for index, data_view in self._udf_data_views[i].items():
                    accumulators[i][index] = data_view
        self._accumulators = accumulators

    def get_accumulators(self):
        return self._accumulators

    def create_accumulators(self):
        return [udf.create_accumulator() for udf in self._udfs]

    def cleanup(self):
        for i in range(len(self._udf_data_views)):
            for data_view in self._udf_data_views[i].values():
                data_view.clear()

    def close(self):
        for udf in self._udfs:
            udf.close()


class SimpleAggsHandleFunction(SimpleAggsHandleFunctionBase, AggsHandleFunction):
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
        self._get_value_indexes = [i for i in range(len(udfs))]
        if index_of_count_star >= 0 and count_star_inserted:
            # The record count is used internally, should be ignored by the get_value method.
            self._get_value_indexes.remove(index_of_count_star)

    def get_value(self):
        return [self._udfs[i].get_value(self._accumulators[i]) for i in self._get_value_indexes]


class SimpleTableAggsHandleFunction(SimpleAggsHandleFunctionBase, TableAggsHandleFunction):
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

    def emit_value(self, current_key: List, is_retract: bool):
        udf = self._udfs[0]  # type: TableAggregateFunction
        results = udf.emit_value(self._accumulators[0])
        for x in results:
            result = join_row(current_key, x._values)
            if is_retract:
                result.set_row_kind(RowKind.DELETE)
            else:
                result.set_row_kind(RowKind.INSERT)
            yield result


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

    def record_count_is_zero(self, acc: List):
        # We store the counter in the accumulator and the counter is never be null
        return acc is None or acc[self._index_of_count_star][0] == 0


class GroupAggFunctionBase(object):

    def __init__(self,
                 aggs_handle: AggsHandleFunctionBase,
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
        self.buffer = {}

    def open(self, function_context: FunctionContext):
        self.aggs_handle.open(StateDataViewStore(function_context, self.state_backend))

    def close(self):
        self.aggs_handle.close()

    def process_element(self, input_data: Row):
        input_value = input_data._values
        key = self.key_selector.get_key(input_value)
        try:
            self.buffer[tuple(key)].append(input_data)
        except KeyError:
            self.buffer[tuple(key)] = [input_data]

    def on_timer(self, key):
        if self.state_cleaning_enabled:
            self.state_backend.set_current_key(key)
            accumulator_state = self.state_backend.get_value_state(
                "accumulators", self.state_value_coder)
            accumulator_state.clear()
            self.aggs_handle.cleanup()

    @staticmethod
    def is_retract_msg(data: Row):
        return data.get_row_kind() == RowKind.UPDATE_BEFORE or data.get_row_kind() == RowKind.DELETE

    @staticmethod
    def is_accumulate_msg(data: Row):
        return data.get_row_kind() == RowKind.UPDATE_AFTER or data.get_row_kind() == RowKind.INSERT

    @abstractmethod
    def finish_bundle(self):
        pass


class GroupAggFunction(GroupAggFunctionBase):

    def __init__(self,
                 aggs_handle: AggsHandleFunction,
                 key_selector: RowKeySelector,
                 state_backend: RemoteKeyedStateBackend,
                 state_value_coder: Coder,
                 generate_update_before: bool,
                 state_cleaning_enabled: bool,
                 index_of_count_star: int):
        super(GroupAggFunction, self).__init__(
            aggs_handle, key_selector, state_backend, state_value_coder, generate_update_before,
            state_cleaning_enabled, index_of_count_star)

    def finish_bundle(self):
        for current_key, input_rows in self.buffer.items():
            current_key = list(current_key)
            first_row = False
            self.state_backend.set_current_key(current_key)
            self.state_backend.clear_cached_iterators()
            accumulator_state = self.state_backend.get_value_state(
                "accumulators", self.state_value_coder)
            accumulators = accumulator_state.value()  # type: List
            start_index = 0
            if accumulators is None:
                for i in range(len(input_rows)):
                    if self.is_retract_msg(input_rows[i]):
                        start_index += 1
                    else:
                        break
                if start_index == len(input_rows):
                    return
                accumulators = self.aggs_handle.create_accumulators()
                first_row = True

            # set accumulators to handler first
            self.aggs_handle.set_accumulators(accumulators)

            # get previous aggregate result
            pre_agg_value = self.aggs_handle.get_value()  # type: List

            for input_row in input_rows[start_index:]:
                # update aggregate result and set to the newRow
                if self.is_accumulate_msg(input_row):
                    # accumulate input
                    self.aggs_handle.accumulate(input_row._values)
                else:
                    # retract input
                    self.aggs_handle.retract(input_row._values)

            # get current aggregate result
            new_agg_value = self.aggs_handle.get_value()  # type: List

            # get accumulator
            accumulators = self.aggs_handle.get_accumulators()

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
                            retract_row = join_row(current_key, pre_agg_value)
                            retract_row.set_row_kind(RowKind.UPDATE_BEFORE)
                            yield retract_row
                        # prepare UPDATE_AFTER message for new row
                        result_row = join_row(current_key, new_agg_value)
                        result_row.set_row_kind(RowKind.UPDATE_AFTER)
                        yield result_row
                else:
                    # this is the first, output new result
                    # prepare INSERT message for new row
                    result_row = join_row(current_key, new_agg_value)
                    result_row.set_row_kind(RowKind.INSERT)
                    yield result_row
            else:
                # we retracted the last record for this key
                # sent out a delete message
                if not first_row:
                    # prepare delete message for previous row
                    result_row = join_row(current_key, pre_agg_value)
                    result_row.set_row_kind(RowKind.DELETE)
                    yield result_row
                # and clear all state
                accumulator_state.clear()
                # cleanup dataview under current key
                self.aggs_handle.cleanup()
        self.buffer = {}


class GroupTableAggFunction(GroupAggFunctionBase):
    def __init__(self,
                 aggs_handle: TableAggsHandleFunction,
                 key_selector: RowKeySelector,
                 state_backend: RemoteKeyedStateBackend,
                 state_value_coder: Coder,
                 generate_update_before: bool,
                 state_cleaning_enabled: bool,
                 index_of_count_star: int):
        super(GroupTableAggFunction, self).__init__(
            aggs_handle, key_selector, state_backend, state_value_coder, generate_update_before,
            state_cleaning_enabled, index_of_count_star)

    def finish_bundle(self):
        for current_key, input_rows in self.buffer.items():
            current_key = list(current_key)
            first_row = False
            self.state_backend.set_current_key(current_key)
            self.state_backend.clear_cached_iterators()
            accumulator_state = self.state_backend.get_value_state(
                "accumulators", self.state_value_coder)
            accumulators = accumulator_state.value()
            start_index = 0
            if accumulators is None:
                for i in range(len(input_rows)):
                    if self.is_retract_msg(input_rows[i]):
                        start_index += 1
                    else:
                        break
                if start_index == len(input_rows):
                    return
                accumulators = self.aggs_handle.create_accumulators()
                first_row = True

            # set accumulators to handler first
            self.aggs_handle.set_accumulators(accumulators)

            if not first_row and self.generate_update_before:
                yield from self.aggs_handle.emit_value(current_key, True)

            for input_row in input_rows[start_index:]:
                # update aggregate result and set to the newRow
                if self.is_accumulate_msg(input_row):
                    # accumulate input
                    self.aggs_handle.accumulate(input_row._values)
                else:
                    # retract input
                    self.aggs_handle.retract(input_row._values)

            # get accumulator
            accumulators = self.aggs_handle.get_accumulators()

            if not self.record_counter.record_count_is_zero(accumulators):
                yield from self.aggs_handle.emit_value(current_key, False)
                accumulator_state.update(accumulators)
            else:
                # and clear all state
                accumulator_state.clear()
                # cleanup dataview under current key
                self.aggs_handle.cleanup()
        self.buffer = {}
