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
from typing import List

from apache_beam.coders import PickleCoder

from pyflink.common import Row, RowKind
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend
from pyflink.table import AggregateFunction, FunctionContext


def join_row(left: Row, right: Row):
    fields = []
    for value in left:
        fields.append(value)
    for value in right:
        fields.append(value)
    return Row(*fields)


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

    def __init__(self, function_context):
        self.function_context = function_context

    def get_runtime_context(self):
        return self.function_context


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
                 args_offsets_list: List[List[int]],
                 index_of_count_star: int):
        self._udfs = udfs
        self._args_offsets_list = args_offsets_list
        self._accumulators = None  # type: Row
        self._get_value_indexes = [i for i in range(len(udfs))]
        if index_of_count_star >= 0:
            # The record count is used internally, should be ignored by the get_value method.
            self._get_value_indexes.remove(index_of_count_star)

    def open(self, state_data_view_store):
        for udf in self._udfs:
            udf.open(state_data_view_store.get_runtime_context())

    def accumulate(self, input_data: Row):
        for i in range(len(self._udfs)):
            args_offset = self._args_offsets_list[i]
            args = [input_data[offset] for offset in args_offset]
            self._udfs[i].accumulate(self._accumulators[i], *args)

    def retract(self, input_data: Row):
        for i in range(len(self._udfs)):
            args_offset = self._args_offsets_list[i]
            args = [input_data[offset] for offset in args_offset]
            self._udfs[i].retract(self._accumulators[i], *args)

    def merge(self, accumulators: Row):
        for i in range(len(self._udfs)):
            self._udfs[i].merge(self._accumulators[i], [accumulators[i]])

    def set_accumulators(self, accumulators: Row):
        self._accumulators = accumulators

    def get_accumulators(self):
        return self._accumulators

    def create_accumulators(self):
        return Row(*[udf.create_accumulator() for udf in self._udfs])

    def cleanup(self):
        # Due to DataView is unsupported currently, there is nothing to do in this method.
        pass

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
                 generate_update_before: bool,
                 state_cleaning_enabled: bool,
                 index_of_count_star: int):
        self.aggs_handle = aggs_handle
        self.generate_update_before = generate_update_before
        self.state_cleaning_enabled = state_cleaning_enabled
        self.key_selector = key_selector
        # Currently we do not support user-defined type accumulator.
        # So any accumulators can be encoded by the PickleCoder.
        self.state_value_coder = PickleCoder()
        self.state_backend = state_backend
        self.record_counter = RecordCounter.of(index_of_count_star)

    def open(self, function_context: FunctionContext):
        self.aggs_handle.open(StateDataViewStore(function_context))

    def close(self):
        self.aggs_handle.close()

    def process_element(self, input_data: Row):
        key = self.key_selector.get_key(input_data)
        self.state_backend.set_current_key(key)
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
