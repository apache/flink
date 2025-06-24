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
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import common
from apache_beam.runners.worker import bundle_processor, operation_specs
from apache_beam.utils import proto_utils

from pyflink import fn_execution

if fn_execution.PYFLINK_CYTHON_ENABLED:
    import pyflink.fn_execution.beam.beam_operations_fast as beam_operations
else:
    import pyflink.fn_execution.beam.beam_operations_slow as beam_operations

from pyflink.fn_execution import flink_fn_execution_pb2
from pyflink.fn_execution.coders import from_proto, from_type_info_proto, TimeWindowCoder, \
    CountWindowCoder, FlattenRowCoder
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend, RemoteOperatorStateBackend

import pyflink.fn_execution.datastream.operations as datastream_operations
from pyflink.fn_execution.datastream.process import operations
import pyflink.fn_execution.table.operations as table_operations

# ----------------- UDF --------------------


@bundle_processor.BeamTransformFactory.register_urn(
    table_operations.SCALAR_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create_scalar_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatelessFunctionOperation,
        table_operations.ScalarFunctionOperation)


# ----------------- UDTF --------------------


@bundle_processor.BeamTransformFactory.register_urn(
    table_operations.TABLE_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create_table_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatelessFunctionOperation,
        table_operations.TableFunctionOperation)


# ----------------- UDAF --------------------


@bundle_processor.BeamTransformFactory.register_urn(
    table_operations.STREAM_GROUP_AGGREGATE_URN,
    flink_fn_execution_pb2.UserDefinedAggregateFunctions)
def create_aggregate_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatefulFunctionOperation,
        table_operations.StreamGroupAggregateOperation)


@bundle_processor.BeamTransformFactory.register_urn(
    table_operations.STREAM_GROUP_TABLE_AGGREGATE_URN,
    flink_fn_execution_pb2.UserDefinedAggregateFunctions)
def create_table_aggregate_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatefulFunctionOperation,
        table_operations.StreamGroupTableAggregateOperation)


@bundle_processor.BeamTransformFactory.register_urn(
    table_operations.STREAM_GROUP_WINDOW_AGGREGATE_URN,
    flink_fn_execution_pb2.UserDefinedAggregateFunctions)
def create_group_window_aggregate_function(factory, transform_id, transform_proto, parameter,
                                           consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatefulFunctionOperation,
        table_operations.StreamGroupWindowAggregateOperation)


# ----------------- Pandas UDAF --------------------


@bundle_processor.BeamTransformFactory.register_urn(
    table_operations.PANDAS_AGGREGATE_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create_pandas_aggregate_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatelessFunctionOperation,
        table_operations.PandasAggregateFunctionOperation)


@bundle_processor.BeamTransformFactory.register_urn(
    table_operations.PANDAS_BATCH_OVER_WINDOW_AGGREGATE_FUNCTION_URN,
    flink_fn_execution_pb2.UserDefinedFunctions)
def create_pandas_over_window_aggregate_function(
        factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        beam_operations.StatelessFunctionOperation,
        table_operations.PandasBatchOverWindowAggregateFunctionOperation)


# ----------------- DataStream --------------------


@bundle_processor.BeamTransformFactory.register_urn(
    common_urns.primitives.PAR_DO.urn, beam_runner_api_pb2.ParDoPayload)
def create_data_stream_keyed_process_function(factory, transform_id, transform_proto, parameter,
                                              consumers):
    urn = parameter.do_fn.urn
    payload = proto_utils.parse_Bytes(
        parameter.do_fn.payload, flink_fn_execution_pb2.UserDefinedDataStreamFunction)
    if urn == datastream_operations.DATA_STREAM_STATELESS_FUNCTION_URN:
        return _create_user_defined_function_operation(
            factory, transform_proto, consumers, payload,
            beam_operations.StatelessFunctionOperation,
            operations.StatelessOperation)
    else:
        return _create_user_defined_function_operation(
            factory, transform_proto, consumers, payload,
            beam_operations.StatefulFunctionOperation,
            operations.StatefulOperation)


# ----------------- Utilities --------------------


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
    name = common.NameContext(transform_proto.unique_name)
    serialized_fn = spec.serialized_fn

    if isinstance(serialized_fn, flink_fn_execution_pb2.UserDefinedDataStreamFunction):
        operator_state_backend = RemoteOperatorStateBackend(
            factory.state_handler,
            serialized_fn.state_cache_size,
            serialized_fn.map_state_read_cache_size,
            serialized_fn.map_state_write_cache_size,
        )
    else:
        operator_state_backend = None

    if hasattr(serialized_fn, "key_type"):
        # keyed operation, need to create the KeyedStateBackend.
        row_schema = serialized_fn.key_type.row_schema
        key_row_coder = FlattenRowCoder([from_proto(f.type) for f in row_schema.fields])
        if serialized_fn.HasField('group_window'):
            if serialized_fn.group_window.is_time_window:
                window_coder = TimeWindowCoder()
            else:
                window_coder = CountWindowCoder()
        else:
            window_coder = None
        keyed_state_backend = RemoteKeyedStateBackend(
            factory.state_handler,
            key_row_coder,
            window_coder,
            serialized_fn.state_cache_size,
            serialized_fn.map_state_read_cache_size,
            serialized_fn.map_state_write_cache_size)

        return beam_operation_cls(
            name,
            spec,
            factory.counter_factory,
            factory.state_sampler,
            consumers,
            internal_operation_cls,
            keyed_state_backend,
            operator_state_backend,
        )
    elif internal_operation_cls == operations.StatefulOperation:
        key_row_coder = from_type_info_proto(serialized_fn.key_type_info)
        keyed_state_backend = RemoteKeyedStateBackend(
            factory.state_handler,
            key_row_coder,
            None,
            serialized_fn.state_cache_size,
            serialized_fn.map_state_read_cache_size,
            serialized_fn.map_state_write_cache_size)
        return beam_operation_cls(
            name,
            spec,
            factory.counter_factory,
            factory.state_sampler,
            consumers,
            internal_operation_cls,
            keyed_state_backend,
            operator_state_backend,
        )
    else:
        return beam_operation_cls(
            name,
            spec,
            factory.counter_factory,
            factory.state_sampler,
            consumers,
            internal_operation_cls,
            operator_state_backend,
        )
