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

from typing import Any, Tuple, Dict, List

from pyflink.common import Row
from pyflink.datastream.time_domain import TimeDomain
from pyflink.fn_execution import flink_fn_execution_pb2, pickle
from pyflink.serializers import PickleSerializer
from pyflink.table import functions
from pyflink.table.udf import DelegationTableFunction, DelegatingScalarFunction, \
    AggregateFunction, PandasAggregateFunctionWrapper

_func_num = 0
_constant_num = 0


def wrap_pandas_result(it):
    import pandas as pd
    return [pd.Series([result]) for result in it]


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
        variable_dict[func_name] = user_defined_func.func
    else:
        variable_dict[func_name] = user_defined_func.eval
    user_defined_funcs.append(user_defined_func)

    func_args, input_variable_dict, input_funcs = _extract_input(user_defined_function_proto.inputs)
    variable_dict.update(input_variable_dict)
    user_defined_funcs.extend(input_funcs)
    return "%s(%s)" % (func_name, func_args), variable_dict, user_defined_funcs


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
    assert isinstance(user_defined_agg, AggregateFunction)
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
    return user_defined_agg, \
        eval("lambda value : (%s,)" % ",".join(args_str), local_variable_dict) \
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


def extract_data_stream_stateless_function(udf_proto):
    """
    Extracts user-defined-function from the proto representation of a
    :class:`Function`.

    :param udf_proto: the proto representation of the Python :class:`Function`
    """
    func_type = udf_proto.function_type
    UserDefinedDataStreamFunction = flink_fn_execution_pb2.UserDefinedDataStreamFunction
    func = None
    # import pyflink.datastream.tests.test_data_stream
    # from pyflink.datastream.tests.test_data_stream import MyKeySelector
    user_defined_func = pickle.loads(udf_proto.payload)
    if func_type == UserDefinedDataStreamFunction.MAP:
        func = user_defined_func.map
    elif func_type == UserDefinedDataStreamFunction.FLAT_MAP:
        func = user_defined_func.flat_map
    elif func_type == UserDefinedDataStreamFunction.REDUCE:
        reduce_func = user_defined_func.reduce

        def wrap_func(value):
            return reduce_func(value[0], value[1])
        func = wrap_func
    elif func_type == UserDefinedDataStreamFunction.CO_MAP:
        co_map_func = user_defined_func

        def wrap_func(value):
            return co_map_func.map1(value[1]) if value[0] else co_map_func.map2(value[2])
        func = wrap_func
    elif func_type == UserDefinedDataStreamFunction.CO_FLAT_MAP:
        co_flat_map_func = user_defined_func

        def wrap_func(value):
            return co_flat_map_func.flat_map1(value[1]) if value[0] else \
                co_flat_map_func.flat_map2(value[2])
        func = wrap_func

    elif func_type == UserDefinedDataStreamFunction.TIMESTAMP_ASSIGNER:
        extract_timestamp = user_defined_func.extract_timestamp

        def wrap_func(value):
            pre_timestamp = value[0]
            real_data = value[1]
            new_timestamp = extract_timestamp(real_data, pre_timestamp)
            return Row(new_timestamp, real_data)
        func = wrap_func

    return func, user_defined_func


def extract_process_function(user_defined_function_proto, ctx, on_timer_ctx,
                             collector, keyed_state_backend):
    process_function = pickle.loads(user_defined_function_proto.payload)
    process_element = process_function.process_element
    on_timer_func = process_function.on_timer

    def wrapped_func(value):
        # VALUE[TIMER_FLAG, TIMER_VALUE, CURRENT_WATERMARK, TIMER_KEY, NORMAL_DATA]
        current_watermark = value[2]
        ctx.timer_service()._current_watermark = current_watermark
        on_timer_ctx.timer_service()._current_watermark = current_watermark
        # it is timer data
        if value[0] is not None:
            on_timer_ctx._timestamp = value[1]
            timer_key = value[3]
            keyed_state_backend.set_current_key(timer_key)
            if value[0] == 0:
                time_domain = TimeDomain.EVENT_TIME
            elif value[0] == 1:
                time_domain = TimeDomain.PROCESSING_TIME
            else:
                raise TypeError("TimeCharacteristic[%s] is not supported." % str(value[0]))
            on_timer_ctx._time_domain = time_domain
            on_timer_func(value[1], on_timer_ctx, collector)
        else:
            # it is normal data
            # VALUE[TIMER_FLAG, CURRENT_TIMESTAMP, CURRENT_WATERMARK, TIMER_KEY, NORMAL_DATA]
            # NORMAL_DATA[CURRENT_KEY, DATA]
            ctx._timestamp = value[1]
            current_key = Row(value[4][0])
            keyed_state_backend.set_current_key(current_key)

            real_data = value[4][1]
            process_element(real_data, ctx, collector)

        for a in collector.buf:
            # 0: proc time timer data
            # 1: event time timer data
            # 2: normal data
            # result_row: [TIMER_FLAG, TIMER TYPE, TIMER_KEY, RESULT_DATA]
            if a[0] == 2:
                yield Row(None, None, None, a[1])
            else:
                yield Row(a[0], a[1], a[2], None)
        collector.clear()
    return wrapped_func, process_function
