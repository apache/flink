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
from enum import Enum
from functools import partial

from typing import Any, Tuple, Dict, List

from pyflink.common import Row
from pyflink.common.serializer import VoidNamespaceSerializer
from pyflink.datastream import RuntimeContext
from pyflink.datastream.time_domain import TimeDomain
from pyflink.fn_execution import flink_fn_execution_pb2, pickle
from pyflink.fn_execution.datastream.keyed_process_function import \
    InternalKeyedProcessFunctionOnTimerContext, InternalKeyedProcessFunctionContext
from pyflink.fn_execution.datastream.window_operator import WindowOperator
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend
from pyflink.fn_execution.timerservice_impl import TimerServiceImpl, InternalTimerImpl, \
    InternalTimerServiceImpl
from pyflink.fn_execution.utils.input_handler import TwoInputRowWithTimerHandler, \
    OneInputRowWithTimerHandler
from pyflink.fn_execution.utils.output_factory import RowWithTimerOutputFactory
from pyflink.serializers import PickleSerializer
from pyflink.table import functions
from pyflink.table.udf import DelegationTableFunction, DelegatingScalarFunction, \
    ImperativeAggregateFunction, PandasAggregateFunctionWrapper

_func_num = 0
_constant_num = 0


def wrap_pandas_result(it):
    import pandas as pd
    arrays = []
    for result in it:
        if isinstance(result, (Row, Tuple)):
            arrays.append(pd.concat([pd.Series([item]) for item in result], axis=1))
        else:
            arrays.append(pd.Series([result]))
    return arrays


def wrap_inputs_as_row(*args):
    from pyflink.common.types import Row
    import pandas as pd
    if type(args[0]) == pd.Series:
        return pd.concat(args, axis=1)
    elif len(args) == 1 and isinstance(args[0], (pd.DataFrame, Row, Tuple)):
        return args[0]
    else:
        return Row(*args)


def check_pandas_udf_result(f, *input_args):
    output = f(*input_args)
    import pandas as pd
    assert type(output) == pd.Series or type(output) == pd.DataFrame, \
        "The result type of Pandas UDF '%s' must be pandas.Series or pandas.DataFrame, got %s" \
        % (f.__name__, type(output))
    assert len(output) == len(input_args[0]), \
        "The result length '%d' of Pandas UDF '%s' is not equal to the input length '%d'" \
        % (len(output), f.__name__, len(input_args[0]))
    return output


def extract_over_window_user_defined_function(user_defined_function_proto):
    window_index = user_defined_function_proto.window_index
    return (*extract_user_defined_function(user_defined_function_proto, True), window_index)


def extract_user_defined_function(user_defined_function_proto, pandas_udaf=False)\
        -> Tuple[str, Dict, List]:
    """
    Extracts user-defined-function from the proto representation of a
    :class:`UserDefinedFunction`.

    :param user_defined_function_proto: the proto representation of the Python
    :param pandas_udaf: whether the user_defined_function_proto is pandas udaf
    :class:`UserDefinedFunction`
    """

    def _next_func_num():
        global _func_num
        _func_num = _func_num + 1
        return _func_num

    def _extract_input(args) -> Tuple[str, Dict, List]:
        local_variable_dict = {}
        local_funcs = []
        args_str = []
        for arg in args:
            if arg.HasField("udf"):
                # for chaining Python UDF input: the input argument is a Python ScalarFunction
                udf_arg, udf_variable_dict, udf_funcs = extract_user_defined_function(arg.udf)
                args_str.append(udf_arg)
                local_variable_dict.update(udf_variable_dict)
                local_funcs.extend(udf_funcs)
            elif arg.HasField("inputOffset"):
                # the input argument is a column of the input row
                args_str.append("value[%s]" % arg.inputOffset)
            else:
                # the input argument is a constant value
                constant_value_name, parsed_constant_value = \
                    _parse_constant_value(arg.inputConstant)
                args_str.append(constant_value_name)
                local_variable_dict[constant_value_name] = parsed_constant_value
        return ",".join(args_str), local_variable_dict, local_funcs

    variable_dict = {}
    user_defined_funcs = []

    user_defined_func = pickle.loads(user_defined_function_proto.payload)
    if pandas_udaf:
        user_defined_func = PandasAggregateFunctionWrapper(user_defined_func)
    func_name = 'f%s' % _next_func_num()
    if isinstance(user_defined_func, DelegatingScalarFunction) \
            or isinstance(user_defined_func, DelegationTableFunction):
        if user_defined_function_proto.is_pandas_udf:
            variable_dict[func_name] = partial(check_pandas_udf_result, user_defined_func.func)
        else:
            variable_dict[func_name] = user_defined_func.func
    else:
        variable_dict[func_name] = user_defined_func.eval
    user_defined_funcs.append(user_defined_func)

    func_args, input_variable_dict, input_funcs = _extract_input(user_defined_function_proto.inputs)
    variable_dict.update(input_variable_dict)
    user_defined_funcs.extend(input_funcs)
    if user_defined_function_proto.takes_row_as_input:
        variable_dict['wrap_inputs_as_row'] = wrap_inputs_as_row
        func_str = "%s(wrap_inputs_as_row(%s))" % (func_name, func_args)
    else:
        func_str = "%s(%s)" % (func_name, func_args)
    return func_str, variable_dict, user_defined_funcs


def _parse_constant_value(constant_value) -> Tuple[str, Any]:
    j_type = constant_value[0]
    serializer = PickleSerializer()
    pickled_data = serializer.loads(constant_value[1:])
    # the type set contains
    # TINYINT,SMALLINT,INTEGER,BIGINT,FLOAT,DOUBLE,DECIMAL,CHAR,VARCHAR,NULL,BOOLEAN
    # the pickled_data doesn't need to transfer to anther python object
    if j_type == 0:
        parsed_constant_value = pickled_data
    # the type is DATE
    elif j_type == 1:
        parsed_constant_value = \
            datetime.date(year=1970, month=1, day=1) + datetime.timedelta(days=pickled_data)
    # the type is TIME
    elif j_type == 2:
        seconds, milliseconds = divmod(pickled_data, 1000)
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        parsed_constant_value = datetime.time(hours, minutes, seconds, milliseconds * 1000)
    # the type is TIMESTAMP
    elif j_type == 3:
        parsed_constant_value = \
            datetime.datetime(year=1970, month=1, day=1, hour=0, minute=0, second=0) \
            + datetime.timedelta(milliseconds=pickled_data)
    else:
        raise Exception("Unknown type %s, should never happen" % str(j_type))

    def _next_constant_num():
        global _constant_num
        _constant_num = _constant_num + 1
        return _constant_num

    constant_value_name = 'c%s' % _next_constant_num()
    return constant_value_name, parsed_constant_value


def extract_user_defined_aggregate_function(
        current_index,
        user_defined_function_proto,
        distinct_info_dict: Dict[Tuple[List[str]], Tuple[List[int], List[int]]]):
    user_defined_agg = load_aggregate_function(user_defined_function_proto.payload)
    assert isinstance(user_defined_agg, ImperativeAggregateFunction)
    args_str = []
    local_variable_dict = {}
    for arg in user_defined_function_proto.inputs:
        if arg.HasField("inputOffset"):
            # the input argument is a column of the input row
            args_str.append("value[%s]" % arg.inputOffset)
        else:
            # the input argument is a constant value
            constant_value_name, parsed_constant_value = \
                _parse_constant_value(arg.inputConstant)
            for key, value in local_variable_dict.items():
                if value == parsed_constant_value:
                    constant_value_name = key
                    break
            if constant_value_name not in local_variable_dict:
                local_variable_dict[constant_value_name] = parsed_constant_value
            args_str.append(constant_value_name)

    if user_defined_function_proto.distinct:
        if tuple(args_str) in distinct_info_dict:
            distinct_info_dict[tuple(args_str)][0].append(current_index)
            distinct_info_dict[tuple(args_str)][1].append(user_defined_function_proto.filter_arg)
            distinct_index = distinct_info_dict[tuple(args_str)][0][0]
        else:
            distinct_info_dict[tuple(args_str)] = \
                ([current_index], [user_defined_function_proto.filter_arg])
            distinct_index = current_index
    else:
        distinct_index = -1
    if user_defined_function_proto.takes_row_as_input:
        local_variable_dict['wrap_inputs_as_row'] = wrap_inputs_as_row
        func_str = "lambda value : [wrap_inputs_as_row(%s)]" % ",".join(args_str)
    else:
        func_str = "lambda value : (%s,)" % ",".join(args_str)
    return user_defined_agg, \
        eval(func_str, local_variable_dict) \
        if args_str else lambda v: tuple(), \
        user_defined_function_proto.filter_arg, \
        distinct_index


def is_built_in_function(payload):
    # The payload may be a pickled bytes or the class name of the built-in functions.
    # If it represents a built-in function, it will start with 0x00.
    # If it is a pickled bytes, it will start with 0x80.
    return payload[0] == 0


def load_aggregate_function(payload):
    if is_built_in_function(payload):
        built_in_function_class_name = payload[1:].decode("utf-8")
        cls = getattr(functions, built_in_function_class_name)
        return cls()
    else:
        return pickle.loads(payload)


def extract_data_stream_stateless_function(udf_proto, runtime_context):
    """
    Extracts user-defined-function from the proto representation of a
    :class:`Function`.

    :param udf_proto: the proto representation of the Python :class:`Function`
    :param runtime_context: the streaming runtime context
    """
    func_type = udf_proto.function_type
    UserDefinedDataStreamFunction = flink_fn_execution_pb2.UserDefinedDataStreamFunction
    func = None

    user_defined_func = pickle.loads(udf_proto.payload)
    if func_type == UserDefinedDataStreamFunction.MAP:
        func = user_defined_func.map
    elif func_type == UserDefinedDataStreamFunction.FLAT_MAP:
        func = user_defined_func.flat_map
    elif func_type == UserDefinedDataStreamFunction.CO_MAP:
        co_map_func = user_defined_func

        def wrapped_func(value):
            # value in format of: [INPUT_FLAG, REAL_VALUE]
            # INPUT_FLAG value of True for the left stream, while False for the right stream
            return co_map_func.map1(value[1]) if value[0] else co_map_func.map2(value[2])
        func = wrapped_func
    elif func_type == UserDefinedDataStreamFunction.CO_FLAT_MAP:
        co_flat_map_func = user_defined_func

        def wrapped_func(value):
            if value[0]:
                yield from co_flat_map_func.flat_map1(value[1])
            else:
                yield from co_flat_map_func.flat_map2(value[2])
        func = wrapped_func

    elif func_type == UserDefinedDataStreamFunction.TIMESTAMP_ASSIGNER:
        extract_timestamp = user_defined_func.extract_timestamp

        def wrapped_func(value):
            pre_timestamp = value[0]
            real_data = value[1]
            return extract_timestamp(real_data, pre_timestamp)
        func = wrapped_func

    def open_func():
        if hasattr(user_defined_func, "open"):
            user_defined_func.open(runtime_context)

    def close_func():
        if hasattr(user_defined_func, "close"):
            user_defined_func.close()

    return func, open_func, close_func


def extract_process_function(user_defined_function_proto, ctx, runtime_context):
    process_function = pickle.loads(user_defined_function_proto.payload)
    process_element = process_function.process_element

    def wrapped_process_function(value):
        # VALUE[CURRENT_TIMESTAMP, CURRENT_WATERMARK, NORMAL_DATA]
        ctx.set_timestamp(value[0])
        ctx.timer_service().advance_watermark(value[1])
        output_result = process_element(value[2], ctx)
        return output_result

    def open_func():
        if hasattr(process_function, "open"):
            process_function.open(runtime_context)

    def close_func():
        if hasattr(process_function, "close"):
            process_function.close()

    return wrapped_process_function, open_func, close_func


def extract_keyed_stateful_function(user_defined_function_proto,
                                    keyed_state_backend: RemoteKeyedStateBackend,
                                    runtime_context: RuntimeContext):
    func_type = user_defined_function_proto.function_type
    UserDefinedDataStreamFunction = flink_fn_execution_pb2.UserDefinedDataStreamFunction
    payload = pickle.loads(user_defined_function_proto.payload)
    internal_timer_service = InternalTimerServiceImpl(keyed_state_backend)

    def state_key_selector(normal_data):
        return Row(normal_data[0])

    def user_key_selector(normal_data):
        return normal_data[0]

    def input_selector(normal_data):
        return normal_data[1]

    if func_type == UserDefinedDataStreamFunction.KEYED_PROCESS or \
            func_type == UserDefinedDataStreamFunction.KEYED_CO_PROCESS:
        timer_service = TimerServiceImpl(internal_timer_service)
        on_timer_ctx = InternalKeyedProcessFunctionOnTimerContext(timer_service)
        ctx = InternalKeyedProcessFunctionContext(timer_service)
        process_function = payload
        output_factory = RowWithTimerOutputFactory(VoidNamespaceSerializer())

        def open_func():
            if hasattr(process_function, "open"):
                process_function.open(runtime_context)

        def close_func():
            if hasattr(process_function, "close"):
                process_function.close()

        if func_type == UserDefinedDataStreamFunction.KEYED_PROCESS:

            def process_element(normal_data, timestamp: int):
                ctx.set_timestamp(timestamp)
                user_current_key = user_key_selector(normal_data)
                ctx.set_current_key(user_current_key)
                return process_function.process_element(input_selector(normal_data), ctx)

            def on_event_time(internal_timer: InternalTimerImpl):
                timestamp = internal_timer.get_timestamp()
                state_current_key = internal_timer.get_key()
                user_current_key = user_key_selector(state_current_key)

                on_timer_ctx.set_current_key(user_current_key)
                on_timer_ctx.set_time_domain(TimeDomain.EVENT_TIME)

                return process_function.on_timer(timestamp, on_timer_ctx)

            def on_processing_time(internal_timer: InternalTimerImpl):
                timestamp = internal_timer.get_timestamp()
                state_current_key = internal_timer.get_key()
                user_current_key = user_key_selector(state_current_key)

                on_timer_ctx.set_current_key(user_current_key)
                on_timer_ctx.set_time_domain(TimeDomain.PROCESSING_TIME)

                return process_function.on_timer(timestamp, on_timer_ctx)

            input_handler = OneInputRowWithTimerHandler(
                internal_timer_service,
                keyed_state_backend,
                state_key_selector,
                process_element,
                on_event_time,
                on_processing_time,
                output_factory)

            process_element_func = input_handler.accept
        elif func_type == UserDefinedDataStreamFunction.KEYED_CO_PROCESS:
            input_handler = TwoInputRowWithTimerHandler(
                ctx,
                on_timer_ctx,
                timer_service,
                keyed_state_backend,
                process_function,
                output_factory)

            process_element_func = input_handler.accept
        else:
            raise Exception("Unsupported func_type: " + str(func_type))
    elif func_type == UserDefinedDataStreamFunction.WINDOW:
        window_operation_descriptor = payload
        window_assigner = window_operation_descriptor.assigner
        window_trigger = window_operation_descriptor.trigger
        allowed_lateness = window_operation_descriptor.allowed_lateness
        window_state_descriptor = window_operation_descriptor.window_state_descriptor
        internal_window_function = window_operation_descriptor.internal_window_function
        window_serializer = window_operation_descriptor.window_serializer
        keyed_state_backend._namespace_coder_impl = window_serializer._get_coder()
        window_operator = WindowOperator(
            window_assigner,
            keyed_state_backend,
            user_key_selector,
            window_state_descriptor,
            internal_window_function,
            window_trigger,
            allowed_lateness)
        output_factory = RowWithTimerOutputFactory(window_serializer)

        def open_func():
            window_operator.open(runtime_context, internal_timer_service)

        def close_func():
            window_operator.close()

        input_handler = OneInputRowWithTimerHandler(
            internal_timer_service,
            keyed_state_backend,
            state_key_selector,
            lambda n, t: window_operator.process_element(input_selector(n), t),
            window_operator.on_event_time,
            window_operator.on_processing_time,
            output_factory)

        process_element_func = input_handler.accept
    else:
        raise Exception("Unsupported func_type: " + str(func_type))

    return process_element_func, open_func, close_func


"""
All these Enum Classes MUST be in sync with
org.apache.flink.streaming.api.utils.PythonOperatorUtils if there are any changes.
"""


class KeyedProcessFunctionInputFlag(Enum):
    EVENT_TIME_TIMER = 0
    PROC_TIME_TIMER = 1
    NORMAL_DATA = 2
