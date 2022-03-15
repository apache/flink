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
import datetime
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List, Dict

import pytz

from pyflink.common import Row, RowKind
from pyflink.common.constants import MAX_LONG_VALUE
from pyflink.fn_execution.datastream.timerservice import InternalTimer
from pyflink.fn_execution.datastream.timerservice_impl import LegacyInternalTimerServiceImpl
from pyflink.fn_execution.coders import PickleCoder
from pyflink.fn_execution.table.aggregate_slow import DistinctViewDescriptor, RowKeySelector
from pyflink.fn_execution.table.state_data_view import DataViewSpec, ListViewSpec, MapViewSpec, \
    PerWindowStateDataViewStore
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend
from pyflink.fn_execution.table.window_assigner import WindowAssigner, PanedWindowAssigner, \
    MergingWindowAssigner
from pyflink.fn_execution.table.window_context import WindowContext, TriggerContext, K, W
from pyflink.fn_execution.table.window_process_function import GeneralWindowProcessFunction, \
    InternalWindowProcessFunction, PanedWindowProcessFunction, MergingWindowProcessFunction
from pyflink.fn_execution.table.window_trigger import Trigger
from pyflink.table.udf import ImperativeAggregateFunction, FunctionContext


N = TypeVar('N')


def join_row(left: List, right: List):
    return Row(*(left + right))


class NamespaceAggsHandleFunctionBase(Generic[N], ABC):
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

        :param input_data: Input values bundled in a Row.
        """
        pass

    @abstractmethod
    def retract(self, input_data: Row):
        """
        Retracts the input values from the accumulators.

        :param input_data: Input values bundled in a Row.
        """

    @abstractmethod
    def merge(self, namespace: N, accumulators: List):
        """
        Merges the other accumulators into current accumulators.
        """
        pass

    @abstractmethod
    def set_accumulators(self, namespace: N, accumulators: List):
        """
        Set the current accumulators (saved in a row) which contains the current aggregated results.
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
    def cleanup(self, namespace: N):
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


class NamespaceAggsHandleFunction(NamespaceAggsHandleFunctionBase[N], ABC):
    @abstractmethod
    def get_value(self, namespace: N) -> List:
        """
        Gets the result of the aggregation from the current accumulators and namespace properties
        (like window start).
        :param namespace: the namespace properties which should be calculated, such window start
        :return: the final result (saved in a List) of the current accumulators.
        """
        pass


class SimpleNamespaceAggsHandleFunction(NamespaceAggsHandleFunction[N]):

    def __init__(self,
                 udfs: List[ImperativeAggregateFunction],
                 input_extractors: List,
                 index_of_count_star: int,
                 count_star_inserted: bool,
                 named_property_extractor,
                 udf_data_view_specs: List[List[DataViewSpec]],
                 filter_args: List[int],
                 distinct_indexes: List[int],
                 distinct_view_descriptors: Dict[int, DistinctViewDescriptor]):
        self._udfs = udfs
        self._input_extractors = input_extractors
        self._named_property_extractor = named_property_extractor
        self._accumulators = None  # type: List
        self._udf_data_view_specs = udf_data_view_specs
        self._udf_data_views = []
        self._filter_args = filter_args
        self._distinct_indexes = distinct_indexes
        self._distinct_view_descriptors = distinct_view_descriptors
        self._distinct_data_views = {}
        self._get_value_indexes = [i for i in range(len(udfs))]
        if index_of_count_star >= 0 and count_star_inserted:
            # The record count is used internally, should be ignored by the get_value method.
            self._get_value_indexes.remove(index_of_count_star)

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

    def merge(self, namespace: N, accumulators: List):
        if self._udf_data_views:
            for i in range(len(self._udf_data_views)):
                for index, data_view in self._udf_data_views[i].items():
                    data_view.set_current_namespace(namespace)
                    accumulators[i][index] = data_view
        for i in range(len(self._udfs)):
            self._udfs[i].merge(self._accumulators[i], [accumulators[i]])

    def set_accumulators(self, namespace: N, accumulators: List):
        if self._udf_data_views and namespace is not None:
            for i in range(len(self._udf_data_views)):
                for index, data_view in self._udf_data_views[i].items():
                    data_view.set_current_namespace(namespace)
                    accumulators[i][index] = data_view
        self._accumulators = accumulators

    def get_accumulators(self) -> List:
        return self._accumulators

    def create_accumulators(self) -> List:
        return [udf.create_accumulator() for udf in self._udfs]

    def cleanup(self, namespace: N):
        for i in range(len(self._udf_data_views)):
            for data_view in self._udf_data_views[i].values():
                data_view.set_current_namespace(namespace)
                data_view.clear()

    def close(self):
        for udf in self._udfs:
            udf.close()

    def get_value(self, namespace: N) -> List:
        result = [self._udfs[i].get_value(self._accumulators[i]) for i in self._get_value_indexes]
        if self._named_property_extractor:
            result.extend(self._named_property_extractor(namespace))
        return result


class GroupWindowAggFunctionBase(Generic[K, W]):
    def __init__(self,
                 allowed_lateness: int,
                 key_selector: RowKeySelector,
                 state_backend: RemoteKeyedStateBackend,
                 state_value_coder,
                 window_assigner: WindowAssigner[W],
                 window_aggregator: NamespaceAggsHandleFunctionBase[W],
                 trigger: Trigger[W],
                 rowtime_index: int,
                 shift_timezone: str):
        self._allowed_lateness = allowed_lateness
        self._key_selector = key_selector
        self._state_backend = state_backend
        self._state_value_coder = state_value_coder
        self._window_assigner = window_assigner
        self._window_aggregator = window_aggregator
        self._rowtime_index = rowtime_index
        self._shift_timezone = shift_timezone
        self._window_function = None  # type: InternalWindowProcessFunction[K, W]
        self._internal_timer_service = None  # type: LegacyInternalTimerServiceImpl
        self._window_context = None  # type: WindowContext
        self._trigger = trigger
        self._trigger_context = None  # type: TriggerContext
        self._window_state = self._state_backend.get_value_state("window_state", state_value_coder)

    def open(self, function_context: FunctionContext):
        self._internal_timer_service = LegacyInternalTimerServiceImpl(self._state_backend)
        self._window_aggregator.open(
            PerWindowStateDataViewStore(function_context, self._state_backend))

        if isinstance(self._window_assigner, PanedWindowAssigner):
            self._window_function = PanedWindowProcessFunction(
                self._allowed_lateness, self._window_assigner, self._window_aggregator)
        elif isinstance(self._window_assigner, MergingWindowAssigner):
            self._window_function = MergingWindowProcessFunction(
                self._allowed_lateness, self._window_assigner, self._window_aggregator,
                self._state_backend)
        else:
            self._window_function = GeneralWindowProcessFunction(
                self._allowed_lateness, self._window_assigner, self._window_aggregator)
        self._trigger_context = TriggerContext(
            self._trigger, self._internal_timer_service, self._state_backend)
        self._trigger_context.open()
        self._window_context = WindowContext(
            self, self._trigger_context, self._state_backend, self._state_value_coder,
            self._internal_timer_service, self._window_assigner.is_event_time())
        self._window_function.open(self._window_context)

    def process_element(self, input_row: Row):
        input_value = input_row._values
        current_key = self._key_selector.get_key(input_value)
        self._state_backend.set_current_key(current_key)
        if self._window_assigner.is_event_time():
            timestamp = input_value[self._rowtime_index]
            seconds = int(timestamp.replace(tzinfo=datetime.timezone.utc).timestamp())
            microseconds_of_second = timestamp.microsecond
            milliseconds = seconds * 1000 + microseconds_of_second // 1000
            timestamp = milliseconds
        else:
            timestamp = self._internal_timer_service.current_processing_time()

        timestamp = self.to_utc_timestamp_mills(timestamp)

        # the windows which the input row should be placed into
        affected_windows = self._window_function.assign_state_namespace(input_value, timestamp)
        for window in affected_windows:
            self._window_state.set_current_namespace(window)
            acc = self._window_state.value()  # type: List
            if acc is None:
                acc = self._window_aggregator.create_accumulators()
            self._window_aggregator.set_accumulators(window, acc)

            if input_row._is_accumulate_msg():
                self._window_aggregator.accumulate(input_row)
            else:
                self._window_aggregator.retract(input_row)
            acc = self._window_aggregator.get_accumulators()
            self._window_state.update(acc)

        # the actual window which the input row is belongs to
        actual_windows = self._window_function.assign_actual_windows(input_value, timestamp)
        result = []
        for window in actual_windows:
            self._trigger_context.window = window
            trigger_result = self._trigger_context.on_element(input_row, timestamp)
            if trigger_result:
                result.append(self._emit_window_result(current_key, window))
            self._register_cleanup_timer(window)
        return result

    def process_watermark(self, watermark: int):
        self._internal_timer_service.advance_watermark(watermark)

    def on_event_time(self, timer: InternalTimer):
        result = []
        timestamp = timer.get_timestamp()
        key = timer.get_key()
        self._state_backend.set_current_key(key)
        window = timer.get_namespace()
        self._trigger_context.window = window
        if self._trigger_context.on_event_time(timestamp):
            # fire
            result.append(self._emit_window_result(key, window))

        if self._window_assigner.is_event_time():
            self._window_function.clean_window_if_needed(window, timestamp)
        return result

    def on_processing_time(self, timer: InternalTimer):
        result = []
        timestamp = timer.get_timestamp()
        key = timer.get_key()
        self._state_backend.set_current_key(key)
        window = timer.get_namespace()
        self._trigger_context.window = window
        if self._trigger_context.on_processing_time(timestamp):
            # fire
            result.append(self._emit_window_result(key, window))

        if not self._window_assigner.is_event_time():
            self._window_function.clean_window_if_needed(window, timestamp)
        return result

    def get_timers(self):
        yield from self._internal_timer_service.timers.keys()
        self._internal_timer_service.timers.clear()

    def to_utc_timestamp_mills(self, epoch_mills):
        if self._shift_timezone == "UTC":
            return epoch_mills
        else:
            timezone = pytz.timezone(self._shift_timezone)
            local_date_time = datetime.datetime.fromtimestamp(epoch_mills / 1000., timezone)\
                .replace(tzinfo=None)
            epoch = datetime.datetime.utcfromtimestamp(0)
            return int((local_date_time - epoch).total_seconds() * 1000.0)

    def close(self):
        self._window_aggregator.close()

    def _register_cleanup_timer(self, window: N):
        cleanup_time = self.cleanup_time(window)
        if cleanup_time == MAX_LONG_VALUE:
            return

        if self._window_assigner.is_event_time():
            self._trigger_context.register_event_time_timer(cleanup_time)
        else:
            self._trigger_context.register_processing_time_timer(cleanup_time)

    def cleanup_time(self, window: N) -> int:
        if self._window_assigner.is_event_time():
            cleanup_time = max(0, window.max_timestamp() + self._allowed_lateness)
            if cleanup_time >= window.max_timestamp():
                return cleanup_time
            else:
                return MAX_LONG_VALUE
        else:
            return max(0, window.max_timestamp())

    @abstractmethod
    def _emit_window_result(self, key: List, window: W):
        pass


class GroupWindowAggFunction(GroupWindowAggFunctionBase[K, W]):

    def __init__(self,
                 allowed_lateness: int,
                 key_selector: RowKeySelector,
                 state_backend: RemoteKeyedStateBackend,
                 state_value_coder,
                 window_assigner: WindowAssigner[W],
                 window_aggregator: NamespaceAggsHandleFunction[W],
                 trigger: Trigger[W],
                 rowtime_index: int,
                 shift_timezone: str):
        super(GroupWindowAggFunction, self).__init__(
            allowed_lateness, key_selector, state_backend, state_value_coder, window_assigner,
            window_aggregator, trigger, rowtime_index, shift_timezone)
        self._window_aggregator = window_aggregator

    def _emit_window_result(self, key: List, window: W):
        self._window_function.prepare_aggregate_accumulator_for_emit(window)
        agg_result = self._window_aggregator.get_value(window)
        result_row = join_row(key, agg_result)
        # send INSERT
        result_row.set_row_kind(RowKind.INSERT)
        return result_row
