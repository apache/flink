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

import cloudpickle

from pyflink.serializers import PickleSerializer
from pyflink.table.udf import DelegationTableFunction, DelegatingScalarFunction

SCALAR_FUNCTION_URN = "flink:transform:scalar_function:v1"
TABLE_FUNCTION_URN = "flink:transform:table_function:v1"


class FunctionFactory:
    def __init__(self):
        self.variable_dict = {}
        self.user_defined_funcs = []

    def extract_user_defined_function(self, user_defined_function_proto):
        """
        Extracts user-defined-function from the proto representation of a
        :class:`UserDefinedFunction`.

        :param user_defined_function_proto: the proto representation of the Python
        :class:`UserDefinedFunction`
        """

        def _next_func_num():
            if not hasattr(self, "_func_num"):
                self._func_num = 0
            else:
                self._func_num += 1
            return self._func_num

        user_defined_func = cloudpickle.loads(user_defined_function_proto.payload)
        func_name = 'f%s' % _next_func_num()
        if isinstance(user_defined_func, DelegatingScalarFunction) \
                or isinstance(user_defined_func, DelegationTableFunction):
            self.variable_dict[func_name] = user_defined_func.func
        else:
            self.variable_dict[func_name] = user_defined_func.eval
        self.user_defined_funcs.append(user_defined_func)
        func_args = self._extract_user_defined_function_args(user_defined_function_proto.inputs)
        return "%s(%s)" % (func_name, func_args)

    def _extract_user_defined_function_args(self, args):
        args_str = []
        for arg in args:
            if arg.HasField("udf"):
                # for chaining Python UDF input: the input argument is a Python ScalarFunction
                args_str.append(self.extract_user_defined_function(arg.udf))
            elif arg.HasField("inputOffset"):
                # the input argument is a column of the input row
                args_str.append("value[%s]" % arg.inputOffset)
            else:
                # the input argument is a constant value
                args_str.append(self._parse_constant_value(arg.inputConstant))
        return ",".join(args_str)

    def _parse_constant_value(self, constant_value):
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
            if not hasattr(self, "_constant_num"):
                self._constant_num = 0
            else:
                self._constant_num += 1
            return self._constant_num

        constant_value_name = 'c%s' % _next_constant_num()
        self.variable_dict[constant_value_name] = parsed_constant_value
        return constant_value_name
