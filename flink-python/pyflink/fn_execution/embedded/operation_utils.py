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
from pyflink.fn_execution.embedded.operations import (OneInputFunctionOperation,
                                                      TwoInputFunctionOperation)

from pyflink.fn_execution.embedded.converters import from_type_info_proto, from_schema_proto


def pare_user_defined_data_stream_function_proto(proto):
    from pyflink.fn_execution import flink_fn_execution_pb2
    serialized_fn = flink_fn_execution_pb2.UserDefinedDataStreamFunction()
    serialized_fn.ParseFromString(proto)
    return serialized_fn


def parse_coder_proto(proto):
    from pyflink.fn_execution import flink_fn_execution_pb2

    coder = flink_fn_execution_pb2.CoderInfoDescriptor()
    coder.ParseFromString(proto)
    return coder


def parse_function_proto(proto):
    from pyflink.fn_execution import flink_fn_execution_pb2
    serialized_fn = flink_fn_execution_pb2.UserDefinedFunctions()
    serialized_fn.ParseFromString(proto)
    return serialized_fn


def create_scalar_operation_from_proto(proto,
                                       input_coder_info,
                                       output_coder_into,
                                       one_arg_optimization=False,
                                       one_result_optimization=False):
    from pyflink.fn_execution.table.operations import ScalarFunctionOperation

    serialized_fn = parse_function_proto(proto)

    input_data_converter = (
        from_schema_proto(
            parse_coder_proto(input_coder_info).flatten_row_type.schema,
            one_arg_optimization))

    output_data_converter = (
        from_schema_proto(
            parse_coder_proto(output_coder_into).flatten_row_type.schema,
            one_result_optimization))

    scalar_operation = ScalarFunctionOperation(
        serialized_fn, one_arg_optimization, one_result_optimization)

    process_element_func = scalar_operation.process_element

    def process_element(value):
        actual_value = input_data_converter.to_internal(value)
        result = process_element_func(actual_value)
        return output_data_converter.to_external(result)

    scalar_operation.process_element = process_element
    return scalar_operation


def create_table_operation_from_proto(proto, input_coder_info, output_coder_into):
    from pyflink.fn_execution.table.operations import TableFunctionOperation

    serialized_fn = parse_function_proto(proto)

    input_data_converter = (
        from_schema_proto(parse_coder_proto(input_coder_info).flatten_row_type.schema))
    output_data_converter = (
        from_schema_proto(parse_coder_proto(output_coder_into).flatten_row_type.schema))

    table_operation = TableFunctionOperation(serialized_fn)

    process_element_func = table_operation.process_element

    def process_element(value):
        actual_value = input_data_converter.to_internal(value)
        results = process_element_func(actual_value)
        for result in results:
            yield output_data_converter.to_external(result)

    table_operation.process_element = process_element

    return table_operation


def create_one_input_user_defined_data_stream_function_from_protos(
        function_infos, input_coder_info, output_coder_info, runtime_context,
        function_context, timer_context, side_output_context, job_parameters, keyed_state_backend,
        operator_state_backend):
    serialized_fns = [pare_user_defined_data_stream_function_proto(proto)
                      for proto in function_infos]
    input_data_converter = (
        from_type_info_proto(parse_coder_proto(input_coder_info).raw_type.type_info))
    output_data_converter = (
        from_type_info_proto(parse_coder_proto(output_coder_info).raw_type.type_info))

    function_operation = OneInputFunctionOperation(
        serialized_fns,
        input_data_converter,
        output_data_converter,
        runtime_context,
        function_context,
        timer_context,
        side_output_context,
        job_parameters,
        keyed_state_backend,
        operator_state_backend)

    return function_operation


def create_two_input_user_defined_data_stream_function_from_protos(
        function_infos, input_coder_info1, input_coder_info2, output_coder_info, runtime_context,
        function_context, timer_context, side_output_context, job_parameters, keyed_state_backend,
        operator_state_backend):
    serialized_fns = [pare_user_defined_data_stream_function_proto(proto)
                      for proto in function_infos]

    input_data_converter1 = (
        from_type_info_proto(parse_coder_proto(input_coder_info1).raw_type.type_info))

    input_data_converter2 = (
        from_type_info_proto(parse_coder_proto(input_coder_info2).raw_type.type_info))

    output_data_converter = (
        from_type_info_proto(parse_coder_proto(output_coder_info).raw_type.type_info))

    function_operation = TwoInputFunctionOperation(
        serialized_fns,
        input_data_converter1,
        input_data_converter2,
        output_data_converter,
        runtime_context,
        function_context,
        timer_context,
        side_output_context,
        job_parameters,
        keyed_state_backend,
        operator_state_backend)

    return function_operation
