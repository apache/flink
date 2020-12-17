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

from apache_beam.runners.worker import bundle_processor, operation_specs

from pyflink.fn_execution import flink_fn_execution_pb2
from pyflink.fn_execution.coders import from_proto, from_type_info_proto
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend

import pyflink.fn_execution.operations as operations

try:
    import pyflink.fn_execution.beam.beam_operations_fast as beam_operations
except ImportError:
    import pyflink.fn_execution.beam.beam_operations_slow as beam_operations


@bundle_processor.BeamTransformFactory.register_urn(
    operations.SCALAR_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create_scalar_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatelessFunctionOperation,
        operations.ScalarFunctionOperation)


@bundle_processor.BeamTransformFactory.register_urn(
    operations.TABLE_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create_table_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatelessFunctionOperation,
        operations.TableFunctionOperation)


@bundle_processor.BeamTransformFactory.register_urn(
    operations.DATA_STREAM_STATELESS_FUNCTION_URN,
    flink_fn_execution_pb2.UserDefinedDataStreamFunction)
def create_data_stream_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatelessFunctionOperation,
        operations.DataStreamStatelessFunctionOperation)


@bundle_processor.BeamTransformFactory.register_urn(
    operations.PANDAS_AGGREGATE_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create_pandas_aggregate_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatelessFunctionOperation,
        operations.PandasAggregateFunctionOperation)


@bundle_processor.BeamTransformFactory.register_urn(
    operations.PANDAS_BATCH_OVER_WINDOW_AGGREGATE_FUNCTION_URN,
    flink_fn_execution_pb2.UserDefinedFunctions)
def create_pandas_over_window_aggregate_function(
        factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatelessFunctionOperation,
        operations.PandasBatchOverWindowAggregateFunctionOperation)


@bundle_processor.BeamTransformFactory.register_urn(
    operations.STREAM_GROUP_AGGREGATE_URN,
    flink_fn_execution_pb2.UserDefinedAggregateFunctions)
def create_aggregate_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatefulFunctionOperation,
        operations.StreamGroupAggregateOperation)


@bundle_processor.BeamTransformFactory.register_urn(
    operations.PROCESS_FUNCTION_URN,
    flink_fn_execution_pb2.UserDefinedDataStreamFunction)
def create_data_stream_process_function(factory, transform_id, transform_proto, parameter,
                                        consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatelessFunctionOperation,
        operations.ProcessFunctionOperation)


@bundle_processor.BeamTransformFactory.register_urn(
    operations.KEYED_PROCESS_FUNCTION_URN,
    flink_fn_execution_pb2.UserDefinedDataStreamFunction)
def create_data_stream_keyed_process_function(factory, transform_id, transform_proto, parameter,
                                              consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatefulFunctionOperation,
        operations.KeyedProcessFunctionOperation)


@bundle_processor.BeamTransformFactory.register_urn(
    operations.STREAM_GROUP_TABLE_AGGREGATE_URN,
    flink_fn_execution_pb2.UserDefinedAggregateFunctions)
def create_table_aggregate_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatefulFunctionOperation,
        operations.StreamGroupTableAggregateOperation)


def _create_user_defined_function_operation(factory, transform_proto, consumers, udfs_proto,
                                            beam_operation_cls, internal_operation_cls):
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
            spec.serialized_fn.state_cache_size,
            spec.serialized_fn.map_state_read_cache_size,
            spec.serialized_fn.map_state_write_cache_size)

        return beam_operation_cls(
            transform_proto.unique_name,
            spec,
            factory.counter_factory,
            factory.state_sampler,
            consumers,
            internal_operation_cls,
            keyed_state_backend)
    elif internal_operation_cls == operations.KeyedProcessFunctionOperation:
        key_type_info = spec.serialized_fn.key_type_info
        key_row_coder = from_type_info_proto(key_type_info.field[0].type)
        keyed_state_backend = RemoteKeyedStateBackend(
            factory.state_handler,
            key_row_coder,
            1000,
            1000,
            1000)
        return beam_operation_cls(
            transform_proto.unique_name,
            spec,
            factory.counter_factory,
            factory.state_sampler,
            consumers,
            internal_operation_cls,
            keyed_state_backend)
    else:
        return beam_operation_cls(
            transform_proto.unique_name,
            spec,
            factory.counter_factory,
            factory.state_sampler,
            consumers,
            internal_operation_cls)
