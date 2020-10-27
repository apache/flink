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
from functools import reduce
from itertools import chain

from apache_beam.coders import PickleCoder
from typing import Tuple

from pyflink.datastream.functions import RuntimeContext
from pyflink.fn_execution import flink_fn_execution_pb2, operation_utils
from pyflink.fn_execution.beam.beam_coders import DataViewFilterCoder
from pyflink.fn_execution.operation_utils import extract_user_defined_aggregate_function
from pyflink.fn_execution.aggregate import RowKeySelector, SimpleAggsHandleFunction, \
    GroupAggFunction, extract_data_view_specs
from pyflink.metrics.metricbase import GenericMetricGroup
from pyflink.table import FunctionContext, Row
from pyflink.table.functions import Count1AggFunction


class Operation(object):
    def __init__(self, spec):
        super(Operation, self).__init__()
        self.spec = spec
        self.func, self.user_defined_funcs = self.generate_func(self.spec.serialized_fn)
        if self.spec.serialized_fn.metric_enabled:
            self.base_metric_group = GenericMetricGroup(None, None)
        else:
            self.base_metric_group = None

    def open(self):
        for user_defined_func in self.user_defined_funcs:
            user_defined_func.open(FunctionContext(self.base_metric_group))

    def finish(self):
        self._update_gauge(self.base_metric_group)

    def close(self):
        for user_defined_func in self.user_defined_funcs:
            user_defined_func.close()

    def _update_gauge(self, base_metric_group):
        if base_metric_group is not None:
            for name in base_metric_group._flink_gauge:
                flink_gauge = base_metric_group._flink_gauge[name]
                beam_gauge = base_metric_group._beam_gauge[name]
                beam_gauge.set(flink_gauge())
            for sub_group in base_metric_group._sub_groups:
                self._update_gauge(sub_group)

    def generate_func(self, serialized_fn) -> Tuple:
        pass


class ScalarFunctionOperation(Operation):
    def __init__(self, spec):
        super(ScalarFunctionOperation, self).__init__(spec)

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
            [operation_utils.extract_user_defined_function(udf) for udf in serialized_fn.udfs])
        generate_func = eval('lambda value: [%s]' % scalar_functions, variable_dict)
        return generate_func, user_defined_funcs


class TableFunctionOperation(Operation):
    def __init__(self, spec):
        super(TableFunctionOperation, self).__init__(spec)

    def generate_func(self, serialized_fn):
        """
        Generates a lambda function based on udtfs.
        :param serialized_fn: serialized function which contains the proto representation of
                              the Python :class:`TableFunction`
        :return: the generated lambda function
        """
        table_function, variable_dict, user_defined_funcs = \
            operation_utils.extract_user_defined_function(serialized_fn.udfs[0])
        generate_func = eval('lambda value: %s' % table_function, variable_dict)
        return generate_func, user_defined_funcs


class DataStreamStatelessFunctionOperation(Operation):

    def __init__(self, spec):
        super(DataStreamStatelessFunctionOperation, self).__init__(spec)

    def open(self):
        for user_defined_func in self.user_defined_funcs:
            runtime_context = RuntimeContext(
                self.spec.serialized_fn.runtime_context.task_name,
                self.spec.serialized_fn.runtime_context.task_name_with_subtasks,
                self.spec.serialized_fn.runtime_context.number_of_parallel_subtasks,
                self.spec.serialized_fn.runtime_context.max_number_of_parallel_subtasks,
                self.spec.serialized_fn.runtime_context.index_of_this_subtask,
                self.spec.serialized_fn.runtime_context.attempt_number,
                {p.key: p.value for p in self.spec.serialized_fn.runtime_context.job_parameters})
            user_defined_func.open(runtime_context)

    def generate_func(self, serialized_fn):
        func, user_defined_func = operation_utils.extract_data_stream_stateless_funcs(serialized_fn)
        return func, [user_defined_func]


class PandasAggregateFunctionOperation(Operation):
    def __init__(self, spec):
        super(PandasAggregateFunctionOperation, self).__init__(spec)

    def generate_func(self, serialized_fn):
        pandas_functions, variable_dict, user_defined_funcs = reduce(
            lambda x, y: (
                ','.join([x[0], y[0]]),
                dict(chain(x[1].items(), y[1].items())),
                x[2] + y[2]),
            [operation_utils.extract_user_defined_function(udf, True)
             for udf in serialized_fn.udfs])
        variable_dict['wrap_pandas_result'] = operation_utils.wrap_pandas_result
        generate_func = eval('lambda value: wrap_pandas_result([%s])' %
                             pandas_functions, variable_dict)
        return generate_func, user_defined_funcs


class PandasBatchOverWindowAggregateFunctionOperation(Operation):
    def __init__(self, spec):
        super(PandasBatchOverWindowAggregateFunctionOperation, self).__init__(spec)
        self.windows = [window for window in self.spec.serialized_fn.windows]
        # the index among all the bounded range over window
        self.bounded_range_window_index = [-1 for _ in range(len(self.windows))]
        # Whether the specified position window is a bounded range window.
        self.is_bounded_range_window = []
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


class StatefulFunctionOperation(Operation):

    def __init__(self, spec, keyed_state_backend):
        self.keyed_state_backend = keyed_state_backend
        super(StatefulFunctionOperation, self).__init__(spec)

    def finish(self):
        super().finish()
        if self.keyed_state_backend:
            self.keyed_state_backend.commit()


TRIGGER_TIMER = 1


class StreamGroupAggregateOperation(StatefulFunctionOperation):

    def __init__(self, spec, keyed_state_backend):
        self.generate_update_before = spec.serialized_fn.generate_update_before
        self.grouping = [i for i in spec.serialized_fn.grouping]
        self.group_agg_function = None
        # If the upstream generates retract message, we need to add an additional count1() agg
        # to track current accumulated messages count. If all the messages are retracted, we need
        # to send a DELETE message to downstream.
        self.index_of_count_star = spec.serialized_fn.index_of_count_star
        self.state_cache_size = spec.serialized_fn.state_cache_size
        self.state_cleaning_enabled = spec.serialized_fn.state_cleaning_enabled
        self.data_view_specs = extract_data_view_specs(spec.serialized_fn.udfs)
        super(StreamGroupAggregateOperation, self).__init__(spec, keyed_state_backend)

    def open(self):
        self.group_agg_function.open(FunctionContext(self.base_metric_group))

    def generate_func(self, serialized_fn):
        user_defined_aggs = []
        input_extractors = []
        for i in range(len(serialized_fn.udfs)):
            if i != self.index_of_count_star:
                user_defined_agg, input_extractor = extract_user_defined_aggregate_function(
                    serialized_fn.udfs[i])
            else:
                user_defined_agg = Count1AggFunction()

                def dummy_input_extractor(value):
                    return []
                input_extractor = dummy_input_extractor
            user_defined_aggs.append(user_defined_agg)
            input_extractors.append(input_extractor)
        aggs_handler_function = SimpleAggsHandleFunction(
            user_defined_aggs,
            input_extractors,
            self.index_of_count_star,
            self.data_view_specs)
        key_selector = RowKeySelector(self.grouping)
        if len(self.data_view_specs) > 0:
            state_value_coder = DataViewFilterCoder(self.data_view_specs)
        else:
            state_value_coder = PickleCoder()
        self.group_agg_function = GroupAggFunction(
            aggs_handler_function,
            key_selector,
            self.keyed_state_backend,
            state_value_coder,
            self.generate_update_before,
            self.state_cleaning_enabled,
            self.index_of_count_star)
        return self.process_element_or_timer, []

    def process_element_or_timer(self, input_data: Tuple[int, Row, int, Row]):
        # the structure of the input data:
        # [element_type, element(for process_element), timestamp(for timer), key(for timer)]
        # all the fields are nullable except the "element_type"
        if input_data[0] != TRIGGER_TIMER:
            return self.group_agg_function.process_element(input_data[1])
        else:
            self.group_agg_function.on_timer(input_data[3])
            return []

    def close(self):
        if self.group_agg_function is not None:
            self.group_agg_function.close()
