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
from functools import reduce
from itertools import chain

from libc.stdint cimport *
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import operation_specs
from apache_beam.utils.windowed_value cimport WindowedValue

from pyflink.fn_execution.coder_impl_fast cimport BaseCoderImpl
from pyflink.fn_execution.beam.beam_stream cimport BeamInputStream, BeamOutputStream
from pyflink.fn_execution.beam.beam_coder_impl_fast cimport InputStreamWrapper
from typing import Tuple

from pyflink.fn_execution.coders import from_proto

from pyflink.common import Row
from pyflink.fn_execution import flink_fn_execution_pb2, operation_utils
from pyflink.fn_execution.operation_utils import extract_user_defined_aggregate_function
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend
from pyflink.fn_execution.aggregate import RowKeySelector, SimpleAggsHandleFunction, GroupAggFunction
from pyflink.metrics.metricbase import GenericMetricGroup
from pyflink.table import FunctionContext
from pyflink.table.functions import Count1AggFunction

cdef class BeamStatelessFunctionOperation(Operation):
    """
    Base class of stateless function operation that will execute ScalarFunction or TableFunction for
    each input element.
    """

    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(BeamStatelessFunctionOperation, self).__init__(name, spec, counter_factory, sampler)
        self.consumer = consumers['output'][0]
        self._value_coder_impl = self.consumer.windowed_coder.wrapped_value_coder.get_impl()._value_coder
        from pyflink.fn_execution.beam.beam_coder_impl_slow import ArrowCoderImpl, \
            OverWindowArrowCoderImpl

        if isinstance(self._value_coder_impl, ArrowCoderImpl) or \
                isinstance(self._value_coder_impl, OverWindowArrowCoderImpl):
            self._is_python_coder = True
        else:
            self._is_python_coder = False
            self._output_coder = self._value_coder_impl._value_coder

        self.func, self.user_defined_funcs = self.generate_func(self.spec.serialized_fn.udfs)
        self._metric_enabled = self.spec.serialized_fn.metric_enabled
        self.base_metric_group = None
        if self._metric_enabled:
            self.base_metric_group = GenericMetricGroup(None, None)
        self.open_func()

    def open_func(self):
        for user_defined_func in self.user_defined_funcs:
            user_defined_func.open(FunctionContext(self.base_metric_group))

    def generate_func(self, udfs) -> tuple:
        pass

    cpdef start(self):
        with self.scoped_start_state:
            super(BeamStatelessFunctionOperation, self).start()

    cpdef finish(self):
        with self.scoped_finish_state:
            super(BeamStatelessFunctionOperation, self).finish()
            self._update_gauge(self.base_metric_group)

    cpdef teardown(self):
        with self.scoped_finish_state:
            for user_defined_func in self.user_defined_funcs:
                user_defined_func.close()

    cpdef process(self, WindowedValue o):
        cdef InputStreamWrapper input_stream_wrapper
        cdef BeamInputStream input_stream
        cdef BaseCoderImpl input_coder
        cdef BeamOutputStream output_stream
        with self.scoped_process_state:
            if self._is_python_coder:
                self._value_coder_impl.encode_to_stream(
                    self.func(o.value), self.consumer.output_stream, True)
                self.consumer.output_stream.maybe_flush()
            else:
                input_stream_wrapper = o.value
                input_stream = input_stream_wrapper._input_stream
                input_coder = input_stream_wrapper._value_coder
                output_stream = BeamOutputStream(self.consumer.output_stream)
                while input_stream.available():
                    input_data = input_coder.decode_from_stream(input_stream)
                    result = self.func(input_data)
                    self._output_coder.encode_to_stream(result, output_stream)
                output_stream.flush()

    def progress_metrics(self):
        metrics = super(BeamStatelessFunctionOperation, self).progress_metrics()
        metrics.processed_elements.measured.output_element_counts.clear()
        tag = None
        receiver = self.receivers[0]
        metrics.processed_elements.measured.output_element_counts[
            str(tag)] = receiver.opcounter.element_counter.value()
        return metrics

    cpdef monitoring_infos(self, transform_id, tag_to_pcollection_id):
        """
        Only pass user metric to Java
        :param tag_to_pcollection_id: useless for user metric
        """
        return self.user_monitoring_infos(transform_id)

    cdef void _update_gauge(self, base_metric_group):
        if base_metric_group is not None:
            for name in base_metric_group._flink_gauge:
                flink_gauge = base_metric_group._flink_gauge[name]
                beam_gauge = base_metric_group._beam_gauge[name]
                beam_gauge.set(flink_gauge())
            for sub_group in base_metric_group._sub_groups:
                self._update_gauge(sub_group)

cdef class BeamScalarFunctionOperation(BeamStatelessFunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(BeamScalarFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers)

    def generate_func(self, udfs):
        """
        Generates a lambda function based on udfs.
        :param udfs: a list of the proto representation of the Python :class:`ScalarFunction`
        :return: the generated lambda function
        """
        scalar_functions, variable_dict, user_defined_funcs = reduce(
            lambda x, y: (
                ','.join([x[0], y[0]]),
                dict(chain(x[1].items(), y[1].items())),
                x[2] + y[2]),
            [operation_utils.extract_user_defined_function(udf) for udf in udfs])
        mapper = eval('lambda value: [%s]' % scalar_functions, variable_dict)
        if self._is_python_coder:
            generate_func = lambda it: map(mapper, it)
        else:
            generate_func = mapper
        return generate_func, user_defined_funcs

cdef class BeamTableFunctionOperation(BeamStatelessFunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(BeamTableFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers)

    def generate_func(self, udtfs):
        """
        Generates a lambda function based on udtfs.
        :param udtfs: a list of the proto representation of the Python :class:`TableFunction`
        :return: the generated lambda function
        """
        table_function, variable_dict, user_defined_funcs = \
            operation_utils.extract_user_defined_function(udtfs[0])
        generate_func = eval('lambda value: %s' % table_function, variable_dict)
        return generate_func, user_defined_funcs

cdef class DataStreamStatelessFunctionOperation(BeamStatelessFunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(DataStreamStatelessFunctionOperation, self).__init__(name, spec, counter_factory,
                                                                   sampler, consumers)

    def generate_func(self, udfs):
        func = operation_utils.extract_data_stream_stateless_funcs(udfs)
        return func, []


cdef class PandasAggregateFunctionOperation(BeamStatelessFunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(PandasAggregateFunctionOperation, self).__init__(name, spec, counter_factory,
                                                                   sampler, consumers)

    def generate_func(self, udfs):
        pandas_functions, variable_dict, user_defined_funcs = reduce(
            lambda x, y: (
                ','.join([x[0], y[0]]),
                dict(chain(x[1].items(), y[1].items())),
                x[2] + y[2]),
            [operation_utils.extract_user_defined_function(udf, True) for udf in udfs])
        variable_dict['wrap_pandas_result'] = operation_utils.wrap_pandas_result
        mapper = eval('lambda value: wrap_pandas_result([%s])' % pandas_functions, variable_dict)
        generate_func = lambda it: map(mapper, it)
        return generate_func, user_defined_funcs


cdef class PandasBatchOverWindowAggregateFunctionOperation(BeamStatelessFunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(PandasBatchOverWindowAggregateFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers)
        self.windows = [window for window in self.spec.serialized_fn.windows]
        self.bounded_range_window_index = [-1 for _ in range(len(self.windows))]
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

    def generate_func(self, udfs):
        user_defined_funcs = []
        self.window_indexes = []
        self.mapper = []
        for udf in udfs:
            pandas_agg_function, variable_dict, user_defined_func, window_index = \
                operation_utils.extract_over_window_user_defined_function(udf)
            user_defined_funcs.extend(user_defined_func)
            self.window_indexes.append(window_index)
            self.mapper.append(eval('lambda value: %s' % pandas_agg_function, variable_dict))
        return self.wrapped_over_window_function, user_defined_funcs

    def wrapped_over_window_function(self, it):
        import pandas as pd
        OverWindow = flink_fn_execution_pb2.OverWindow
        for boundaries_series in it:
            input_series = boundaries_series[len(boundaries_series) - 1]
            input_cnt = len(input_series[0])
            results = []
            # loop every agg func
            for i in range(len(self.window_indexes)):
                window_index = self.window_indexes[i]
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
            yield results


class BeamStatefulFunctionOperation(BeamStatelessFunctionOperation):

    def __init__(self, name, spec, counter_factory, sampler, consumers, keyed_state_backend):
        self.keyed_state_backend = keyed_state_backend
        super(BeamStatefulFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers)

    def finish(self):
        super().finish()
        with self.scoped_finish_state:
            if self.keyed_state_backend:
                self.keyed_state_backend.commit()

    def reset(self):
        super().reset()
        if self.keyed_state_backend:
            self.keyed_state_backend.reset()


TRIGGER_TIMER = 1


class BeamStreamGroupAggregateOperation(BeamStatefulFunctionOperation):

    def __init__(self, name, spec, counter_factory, sampler, consumers, keyed_state_backend):
        self.generate_update_before = spec.serialized_fn.generate_update_before
        self.grouping = [i for i in spec.serialized_fn.grouping]
        self.group_agg_function = None
        self.index_of_count_star = spec.serialized_fn.index_of_count_star
        self.state_cache_size = spec.serialized_fn.state_cache_size
        self.state_cleaning_enabled = spec.serialized_fn.state_cleaning_enabled
        super(BeamStreamGroupAggregateOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers, keyed_state_backend)

    def open_func(self):
        self.group_agg_function.open(FunctionContext(self.base_metric_group))

    def generate_func(self, udfs):
        user_defined_aggs = []
        input_offsets = []
        for i in range(len(udfs)):
            if i != self.index_of_count_star:
                user_defined_agg, input_offset = extract_user_defined_aggregate_function(udfs[i])
            else:
                user_defined_agg = Count1AggFunction()
                input_offset = []
            user_defined_aggs.append(user_defined_agg)
            input_offsets.append(input_offset)
        aggs_handler_function = SimpleAggsHandleFunction(
            user_defined_aggs, input_offsets, self.index_of_count_star)
        key_selector = RowKeySelector(self.grouping)
        self.group_agg_function = GroupAggFunction(
            aggs_handler_function,
            key_selector,
            self.keyed_state_backend,
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

    def teardown(self):
        if self.group_agg_function is not None:
            self.group_agg_function.close()
        super().teardown()


@bundle_processor.BeamTransformFactory.register_urn(
    operation_utils.SCALAR_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create_scalar_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter, BeamScalarFunctionOperation)

@bundle_processor.BeamTransformFactory.register_urn(
    operation_utils.TABLE_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create_table_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter, BeamTableFunctionOperation)

@bundle_processor.BeamTransformFactory.register_urn(
    operation_utils.DATA_STREAM_STATELESS_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedDataStreamFunctions)
def create_data_stream_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter, DataStreamStatelessFunctionOperation)

@bundle_processor.BeamTransformFactory.register_urn(
    operation_utils.PANDAS_AGGREGATE_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create_pandas_aggregate_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter, PandasAggregateFunctionOperation)

@bundle_processor.BeamTransformFactory.register_urn(
    operation_utils.PANDAS_BATCH_OVER_WINDOW_AGGREGATE_FUNCTION_URN,
    flink_fn_execution_pb2.UserDefinedFunctions)
def create_pandas_over_window_aggregate_function(
        factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        PandasBatchOverWindowAggregateFunctionOperation)

@bundle_processor.BeamTransformFactory.register_urn(
    operation_utils.STREAM_GROUP_AGGREGATE_URN,
    flink_fn_execution_pb2.UserDefinedAggregateFunctions)
def create_aggregate_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter, BeamStreamGroupAggregateOperation)

def _create_user_defined_function_operation(factory, transform_proto, consumers, udfs_proto,
                                            operation_cls):
    output_tags = list(transform_proto.outputs.keys())
    output_coders = factory.get_output_coders(transform_proto)
    spec = operation_specs.WorkerDoFn(
        serialized_fn=udfs_proto,
        output_tags=output_tags,
        input=None,
        side_inputs=None,
        output_coders=[output_coders[tag] for tag in output_tags])

    if hasattr(spec.serialized_fn, "key_type"):
        # keyed operation, need to create the KeyedStateBackend.
        key_row_coder = from_proto(spec.serialized_fn.key_type)
        keyed_state_backend = RemoteKeyedStateBackend(
            factory.state_handler,
            key_row_coder,
            spec.serialized_fn.state_cache_size)

        return operation_cls(
            transform_proto.unique_name,
            spec,
            factory.counter_factory,
            factory.state_sampler,
            consumers,
            keyed_state_backend)
    else:
        return operation_cls(
            transform_proto.unique_name,
            spec,
            factory.counter_factory,
            factory.state_sampler,
            consumers)
