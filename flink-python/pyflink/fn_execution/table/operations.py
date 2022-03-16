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
import abc
from functools import reduce
from itertools import chain
from typing import Tuple

from pyflink.fn_execution.coders import DataViewFilterCoder, PickleCoder
from pyflink.fn_execution.datastream.timerservice import InternalTimer
from pyflink.fn_execution.datastream.operations import Operation
from pyflink.fn_execution.datastream.timerservice_impl import TimerOperandType, InternalTimerImpl
from pyflink.fn_execution.table.state_data_view import extract_data_view_specs

from pyflink.fn_execution.table.window_assigner import TumblingWindowAssigner, \
    CountTumblingWindowAssigner, SlidingWindowAssigner, CountSlidingWindowAssigner, \
    SessionWindowAssigner
from pyflink.fn_execution.table.window_trigger import EventTimeTrigger, ProcessingTimeTrigger, \
    CountTrigger
from pyflink.fn_execution.utils import operation_utils
from pyflink.fn_execution.utils.operation_utils import extract_user_defined_aggregate_function

try:
    from pyflink.fn_execution.table.aggregate_fast import RowKeySelector, \
        SimpleAggsHandleFunction, GroupAggFunction, DistinctViewDescriptor, \
        SimpleTableAggsHandleFunction, GroupTableAggFunction
    from pyflink.fn_execution.table.window_aggregate_fast import \
        SimpleNamespaceAggsHandleFunction, GroupWindowAggFunction
    from pyflink.fn_execution.coder_impl_fast import InternalRow
    has_cython = True
except ImportError:
    from pyflink.fn_execution.table.aggregate_slow import RowKeySelector, \
        SimpleAggsHandleFunction, GroupAggFunction, DistinctViewDescriptor, \
        SimpleTableAggsHandleFunction, GroupTableAggFunction
    from pyflink.fn_execution.table.window_aggregate_slow import \
        SimpleNamespaceAggsHandleFunction, GroupWindowAggFunction
    has_cython = False

from pyflink.table import FunctionContext, Row

# UDF
SCALAR_FUNCTION_URN = "flink:transform:scalar_function:v1"

# UDTF
TABLE_FUNCTION_URN = "flink:transform:table_function:v1"

# UDAF
STREAM_GROUP_AGGREGATE_URN = "flink:transform:stream_group_aggregate:v1"
STREAM_GROUP_TABLE_AGGREGATE_URN = "flink:transform:stream_group_table_aggregate:v1"
STREAM_GROUP_WINDOW_AGGREGATE_URN = "flink:transform:stream_group_window_aggregate:v1"

# Pandas UDAF
PANDAS_AGGREGATE_FUNCTION_URN = "flink:transform:aggregate_function:arrow:v1"
PANDAS_BATCH_OVER_WINDOW_AGGREGATE_FUNCTION_URN = \
    "flink:transform:batch_over_window_aggregate_function:arrow:v1"


class BundleOperation(object):
    def finish_bundle(self):
        raise NotImplementedError


class BaseOperation(Operation):
    def __init__(self, serialized_fn):
        super(BaseOperation, self).__init__(serialized_fn)
        self.func, self.user_defined_funcs = self.generate_func(serialized_fn)

    def process_element(self, value):
        return self.func(value)

    def open(self):
        for user_defined_func in self.user_defined_funcs:
            if hasattr(user_defined_func, 'open'):
                user_defined_func.open(FunctionContext(self.base_metric_group))

    def close(self):
        for user_defined_func in self.user_defined_funcs:
            if hasattr(user_defined_func, 'close'):
                user_defined_func.close()

    @abc.abstractmethod
    def generate_func(self, serialized_fn) -> Tuple:
        pass


class ScalarFunctionOperation(BaseOperation):
    def __init__(self, serialized_fn, one_arg_optimization=False, one_result_optimization=False):
        self._one_arg_optimization = one_arg_optimization
        self._one_result_optimization = one_result_optimization
        super(ScalarFunctionOperation, self).__init__(serialized_fn)

    def generate_func(self, serialized_fn):
        """
        Generates a lambda function based on udfs.
        :param serialized_fn: serialized function which contains a list of the proto
                              representation of the Python :class:`ScalarFunction`
        :return: the generated lambda function
        """
        scalar_functions, variable_dict, user_defined_funcs = reduce(
            lambda x, y: (
                ','.join([x[0], y[0]]),
                dict(chain(x[1].items(), y[1].items())),
                x[2] + y[2]),
            [operation_utils.extract_user_defined_function(
                udf, one_arg_optimization=self._one_arg_optimization)
                for udf in serialized_fn.udfs])
        if self._one_result_optimization:
            func_str = 'lambda value: %s' % scalar_functions
        else:
            func_str = 'lambda value: [%s]' % scalar_functions
        generate_func = eval(func_str, variable_dict)
        return generate_func, user_defined_funcs


class TableFunctionOperation(BaseOperation):
    def __init__(self, serialized_fn):
        super(TableFunctionOperation, self).__init__(serialized_fn)

    def generate_func(self, serialized_fn):
        """
        Generates a lambda function based on udtfs.
        :param serialized_fn: serialized function which contains the proto representation of
                              the Python :class:`TableFunction`
        :return: the generated lambda function
        """
        table_function, variable_dict, user_defined_funcs = \
            operation_utils.extract_user_defined_function(serialized_fn.udfs[0])
        variable_dict['normalize_table_function_result'] = \
            operation_utils.normalize_table_function_result
        generate_func = eval('lambda value: normalize_table_function_result(%s)' % table_function,
                             variable_dict)
        return generate_func, user_defined_funcs


class PandasAggregateFunctionOperation(BaseOperation):
    def __init__(self, serialized_fn):
        super(PandasAggregateFunctionOperation, self).__init__(serialized_fn)

    def generate_func(self, serialized_fn):
        pandas_functions, variable_dict, user_defined_funcs = reduce(
            lambda x, y: (
                ','.join([x[0], y[0]]),
                dict(chain(x[1].items(), y[1].items())),
                x[2] + y[2]),
            [operation_utils.extract_user_defined_function(udf, True)
             for udf in serialized_fn.udfs])
        variable_dict['normalize_pandas_result'] = operation_utils.normalize_pandas_result
        generate_func = eval('lambda value: normalize_pandas_result([%s])' %
                             pandas_functions, variable_dict)
        return generate_func, user_defined_funcs


class PandasBatchOverWindowAggregateFunctionOperation(BaseOperation):
    def __init__(self, serialized_fn):
        super(PandasBatchOverWindowAggregateFunctionOperation, self).__init__(serialized_fn)
        self.windows = [window for window in serialized_fn.windows]
        # the index among all the bounded range over window
        self.bounded_range_window_index = [-1 for _ in range(len(self.windows))]
        # Whether the specified position window is a bounded range window.
        self.is_bounded_range_window = []

        from pyflink.fn_execution import flink_fn_execution_pb2

        window_types = flink_fn_execution_pb2.OverWindow

        bounded_range_window_nums = 0
        for i, window in enumerate(self.windows):
            window_type = window.window_type
            if (window_type is window_types.RANGE_UNBOUNDED_PRECEDING) or (
                    window_type is window_types.RANGE_UNBOUNDED_FOLLOWING) or (
                    window_type is window_types.RANGE_SLIDING):
                self.bounded_range_window_index[i] = bounded_range_window_nums
                self.is_bounded_range_window.append(True)
                bounded_range_window_nums += 1
            else:
                self.is_bounded_range_window.append(False)

    def generate_func(self, serialized_fn):
        user_defined_funcs = []
        self.window_indexes = []
        self.mapper = []
        for udf in serialized_fn.udfs:
            pandas_agg_function, variable_dict, user_defined_func, window_index = \
                operation_utils.extract_over_window_user_defined_function(udf)
            user_defined_funcs.extend(user_defined_func)
            self.window_indexes.append(window_index)
            self.mapper.append(eval('lambda value: %s' % pandas_agg_function, variable_dict))
        return self.wrapped_over_window_function, user_defined_funcs

    def wrapped_over_window_function(self, boundaries_series):
        import pandas as pd
        from pyflink.fn_execution import flink_fn_execution_pb2

        OverWindow = flink_fn_execution_pb2.OverWindow
        input_series = boundaries_series[-1]
        # the row number of the arrow format data
        input_cnt = len(input_series[0])
        results = []
        # loop every agg func
        for i in range(len(self.window_indexes)):
            window_index = self.window_indexes[i]
            # the over window which the agg function belongs to
            window = self.windows[window_index]
            window_type = window.window_type
            func = self.mapper[i]
            result = []
            if self.is_bounded_range_window[window_index]:
                window_boundaries = boundaries_series[
                    self.bounded_range_window_index[window_index]]
                if window_type is OverWindow.RANGE_UNBOUNDED_PRECEDING:
                    # range unbounded preceding window
                    for j in range(input_cnt):
                        end = window_boundaries[j]
                        series_slices = [s.iloc[:end] for s in input_series]
                        result.append(func(series_slices))
                elif window_type is OverWindow.RANGE_UNBOUNDED_FOLLOWING:
                    # range unbounded following window
                    for j in range(input_cnt):
                        start = window_boundaries[j]
                        series_slices = [s.iloc[start:] for s in input_series]
                        result.append(func(series_slices))
                else:
                    # range sliding window
                    for j in range(input_cnt):
                        start = window_boundaries[j * 2]
                        end = window_boundaries[j * 2 + 1]
                        series_slices = [s.iloc[start:end] for s in input_series]
                        result.append(func(series_slices))
            else:
                # unbounded range window or unbounded row window
                if (window_type is OverWindow.RANGE_UNBOUNDED) or (
                        window_type is OverWindow.ROW_UNBOUNDED):
                    series_slices = [s.iloc[:] for s in input_series]
                    func_result = func(series_slices)
                    result = [func_result for _ in range(input_cnt)]
                elif window_type is OverWindow.ROW_UNBOUNDED_PRECEDING:
                    # row unbounded preceding window
                    window_end = window.upper_boundary
                    for j in range(input_cnt):
                        end = min(j + window_end + 1, input_cnt)
                        series_slices = [s.iloc[: end] for s in input_series]
                        result.append(func(series_slices))
                elif window_type is OverWindow.ROW_UNBOUNDED_FOLLOWING:
                    # row unbounded following window
                    window_start = window.lower_boundary
                    for j in range(input_cnt):
                        start = max(j + window_start, 0)
                        series_slices = [s.iloc[start: input_cnt] for s in input_series]
                        result.append(func(series_slices))
                else:
                    # row sliding window
                    window_start = window.lower_boundary
                    window_end = window.upper_boundary
                    for j in range(input_cnt):
                        start = max(j + window_start, 0)
                        end = min(j + window_end + 1, input_cnt)
                        series_slices = [s.iloc[start: end] for s in input_series]
                        result.append(func(series_slices))
            results.append(pd.Series(result))
        return results


class BaseStatefulOperation(BaseOperation, abc.ABC):

    def __init__(self, serialized_fn, keyed_state_backend):
        self.keyed_state_backend = keyed_state_backend
        super(BaseStatefulOperation, self).__init__(serialized_fn)

    def finish(self):
        super().finish()
        if self.keyed_state_backend:
            self.keyed_state_backend.commit()


NORMAL_RECORD = 0
TRIGGER_TIMER = 1
REGISTER_EVENT_TIMER = 0
REGISTER_PROCESSING_TIMER = 1


class AbstractStreamGroupAggregateOperation(BaseStatefulOperation):

    def __init__(self, serialized_fn, keyed_state_backend):
        self.generate_update_before = serialized_fn.generate_update_before
        self.grouping = [i for i in serialized_fn.grouping]
        self.group_agg_function = None
        # If the upstream generates retract message, we need to add an additional count1() agg
        # to track current accumulated messages count. If all the messages are retracted, we need
        # to send a DELETE message to downstream.
        self.index_of_count_star = serialized_fn.index_of_count_star
        self.count_star_inserted = serialized_fn.count_star_inserted
        self.state_cache_size = serialized_fn.state_cache_size
        self.state_cleaning_enabled = serialized_fn.state_cleaning_enabled
        self.data_view_specs = extract_data_view_specs(serialized_fn.udfs)
        super(AbstractStreamGroupAggregateOperation, self).__init__(
            serialized_fn, keyed_state_backend)

    def open(self):
        self.group_agg_function.open(FunctionContext(self.base_metric_group))

    def close(self):
        self.group_agg_function.close()

    def generate_func(self, serialized_fn):
        user_defined_aggs = []
        input_extractors = []
        filter_args = []
        # stores the indexes of the distinct views which the agg functions used
        distinct_indexes = []
        # stores the indexes of the functions which share the same distinct view
        # and the filter args of them
        distinct_info_dict = {}
        for i in range(len(serialized_fn.udfs)):
            user_defined_agg, input_extractor, filter_arg, distinct_index = \
                extract_user_defined_aggregate_function(
                    i, serialized_fn.udfs[i], distinct_info_dict)
            user_defined_aggs.append(user_defined_agg)
            input_extractors.append(input_extractor)
            filter_args.append(filter_arg)
            distinct_indexes.append(distinct_index)
        distinct_view_descriptors = {}
        for agg_index_list, filter_arg_list in distinct_info_dict.values():
            if -1 in filter_arg_list:
                # If there is a non-filter call, we don't need to check filter or not before
                # writing the distinct data view.
                filter_arg_list = []
            # use the agg index of the first function as the key of shared distinct view
            distinct_view_descriptors[agg_index_list[0]] = DistinctViewDescriptor(
                input_extractors[agg_index_list[0]], filter_arg_list)

        key_selector = RowKeySelector(self.grouping)
        if len(self.data_view_specs) > 0:
            state_value_coder = DataViewFilterCoder(self.data_view_specs)
        else:
            state_value_coder = PickleCoder()

        self.group_agg_function = self.create_process_function(
            user_defined_aggs, input_extractors, filter_args, distinct_indexes,
            distinct_view_descriptors, key_selector, state_value_coder)

        return self.process_element_or_timer, []

    def process_element_or_timer(self, input_data: Tuple[int, Row, int, Row]):
        # the structure of the input data:
        # [element_type, element(for process_element), timestamp(for timer), key(for timer)]
        # all the fields are nullable except the "element_type"
        if input_data[0] == NORMAL_RECORD:
            if has_cython:
                row = InternalRow.from_row(input_data[1])
            else:
                row = input_data[1]
            self.group_agg_function.process_element(row)
        else:
            if has_cython:
                timer = InternalRow.from_row(input_data[3])
            else:
                timer = input_data[3]
            self.group_agg_function.on_timer(timer)

    @abc.abstractmethod
    def create_process_function(self, user_defined_aggs, input_extractors, filter_args,
                                distinct_indexes, distinct_view_descriptors, key_selector,
                                state_value_coder):
        pass


class StreamGroupAggregateOperation(AbstractStreamGroupAggregateOperation, BundleOperation):

    def __init__(self, serialized_fn, keyed_state_backend):
        super(StreamGroupAggregateOperation, self).__init__(serialized_fn, keyed_state_backend)

    def finish_bundle(self):
        return self.group_agg_function.finish_bundle()

    def create_process_function(self, user_defined_aggs, input_extractors, filter_args,
                                distinct_indexes, distinct_view_descriptors, key_selector,
                                state_value_coder):
        aggs_handler_function = SimpleAggsHandleFunction(
            user_defined_aggs,
            input_extractors,
            self.index_of_count_star,
            self.count_star_inserted,
            self.data_view_specs,
            filter_args,
            distinct_indexes,
            distinct_view_descriptors)

        return GroupAggFunction(
            aggs_handler_function,
            key_selector,
            self.keyed_state_backend,
            state_value_coder,
            self.generate_update_before,
            self.state_cleaning_enabled,
            self.index_of_count_star)


class StreamGroupTableAggregateOperation(AbstractStreamGroupAggregateOperation, BundleOperation):
    def __init__(self, serialized_fn, keyed_state_backend):
        super(StreamGroupTableAggregateOperation, self).__init__(serialized_fn, keyed_state_backend)

    def finish_bundle(self):
        return self.group_agg_function.finish_bundle()

    def create_process_function(self, user_defined_aggs, input_extractors, filter_args,
                                distinct_indexes, distinct_view_descriptors, key_selector,
                                state_value_coder):
        aggs_handler_function = SimpleTableAggsHandleFunction(
            user_defined_aggs,
            input_extractors,
            self.data_view_specs,
            filter_args,
            distinct_indexes,
            distinct_view_descriptors)
        return GroupTableAggFunction(
            aggs_handler_function,
            key_selector,
            self.keyed_state_backend,
            state_value_coder,
            self.generate_update_before,
            self.state_cleaning_enabled,
            self.index_of_count_star)


class StreamGroupWindowAggregateOperation(AbstractStreamGroupAggregateOperation):
    def __init__(self, serialized_fn, keyed_state_backend):
        self._window = serialized_fn.group_window
        self._named_property_extractor = self._create_named_property_function()
        self._is_time_window = None
        self._reuse_timer_data = Row()
        self._reuse_key_data = Row()
        super(StreamGroupWindowAggregateOperation, self).__init__(
            serialized_fn, keyed_state_backend)

    def create_process_function(self, user_defined_aggs, input_extractors, filter_args,
                                distinct_indexes, distinct_view_descriptors, key_selector,
                                state_value_coder):
        from pyflink.fn_execution import flink_fn_execution_pb2

        self._is_time_window = self._window.is_time_window
        self._namespace_coder = self.keyed_state_backend._namespace_coder_impl
        if self._window.window_type == flink_fn_execution_pb2.GroupWindow.TUMBLING_GROUP_WINDOW:
            if self._is_time_window:
                window_assigner = TumblingWindowAssigner(
                    self._window.window_size, 0, self._window.is_row_time)
            else:
                window_assigner = CountTumblingWindowAssigner(self._window.window_size)
        elif self._window.window_type == flink_fn_execution_pb2.GroupWindow.SLIDING_GROUP_WINDOW:
            if self._is_time_window:
                window_assigner = SlidingWindowAssigner(
                    self._window.window_size, self._window.window_slide, 0,
                    self._window.is_row_time)
            else:
                window_assigner = CountSlidingWindowAssigner(
                    self._window.window_size, self._window.window_slide)
        else:
            window_assigner = SessionWindowAssigner(
                self._window.window_gap, self._window.is_row_time)
        if self._is_time_window:
            if self._window.is_row_time:
                trigger = EventTimeTrigger()
            else:
                trigger = ProcessingTimeTrigger()
        else:
            trigger = CountTrigger(self._window.window_size)

        window_aggregator = SimpleNamespaceAggsHandleFunction(
            user_defined_aggs,
            input_extractors,
            self.index_of_count_star,
            self.count_star_inserted,
            self._named_property_extractor,
            self.data_view_specs,
            filter_args,
            distinct_indexes,
            distinct_view_descriptors)
        return GroupWindowAggFunction(
            self._window.allowedLateness,
            key_selector,
            self.keyed_state_backend,
            state_value_coder,
            window_assigner,
            window_aggregator,
            trigger,
            self._window.time_field_index,
            self._window.shift_timezone)

    def process_element_or_timer(self, input_data: Tuple[int, Row, int, int, Row]):
        if input_data[0] == NORMAL_RECORD:
            self.group_agg_function.process_watermark(input_data[3])
            if has_cython:
                input_row = InternalRow.from_row(input_data[1])
            else:
                input_row = input_data[1]
            result_datas = self.group_agg_function.process_element(input_row)
            for result_data in result_datas:
                yield [NORMAL_RECORD, result_data, None]
            timers = self.group_agg_function.get_timers()
            for timer in timers:
                timer_operand_type = timer[0]  # type: TimerOperandType
                internal_timer = timer[1]  # type: InternalTimer
                window = internal_timer.get_namespace()
                self._reuse_key_data._values = internal_timer.get_key()
                timestamp = internal_timer.get_timestamp()
                encoded_window = self._namespace_coder.encode(window)
                self._reuse_timer_data._values = \
                    [timer_operand_type.value, self._reuse_key_data, timestamp, encoded_window]
                yield [TRIGGER_TIMER, None, self._reuse_timer_data]
        else:
            timestamp = input_data[2]
            timer_data = input_data[4]
            key = list(timer_data[1])
            timer_type = timer_data[0]
            namespace = self._namespace_coder.decode(timer_data[2])
            timer = InternalTimerImpl(timestamp, key, namespace)
            if timer_type == REGISTER_EVENT_TIMER:
                result_datas = self.group_agg_function.on_event_time(timer)
            else:
                result_datas = self.group_agg_function.on_processing_time(timer)
            for result_data in result_datas:
                yield [NORMAL_RECORD, result_data, None]

    def _create_named_property_function(self):
        from pyflink.fn_execution import flink_fn_execution_pb2

        named_property_extractor_array = []
        for named_property in self._window.namedProperties:
            if named_property == flink_fn_execution_pb2.GroupWindow.WINDOW_START:
                named_property_extractor_array.append("value.start")
            elif named_property == flink_fn_execution_pb2.GroupWindow.WINDOW_END:
                named_property_extractor_array.append("value.end")
            elif named_property == flink_fn_execution_pb2.GroupWindow.ROW_TIME_ATTRIBUTE:
                named_property_extractor_array.append("value.end - 1")
            elif named_property == flink_fn_execution_pb2.GroupWindow.PROC_TIME_ATTRIBUTE:
                named_property_extractor_array.append("-1")
            else:
                raise Exception("Unexpected property %s" % named_property)
        named_property_extractor_str = ','.join(named_property_extractor_array)
        if named_property_extractor_str:
            return eval('lambda value: [%s]' % named_property_extractor_str)
        else:
            return None
