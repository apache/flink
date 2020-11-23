################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import ast
import datetime
import pickle

from pyflink.common import typeinfo
from pyflink.common.typeinfo import WrapperTypeInfo, RowTypeInfo, Types, BasicArrayTypeInfo, \
    PrimitiveArrayTypeInfo
from pyflink.java_gateway import get_gateway


def convert_to_python_obj(data, type_info):
    if type_info is None:
        return pickle.loads(data)
    else:
        gateway = get_gateway()
        pickle_bytes = gateway.jvm.PythonBridgeUtils. \
            getPickledBytesFromJavaObject(data, type_info.get_java_type_info())
        pickle_bytes = list(pickle_bytes[1:])
        field_data = zip(pickle_bytes, type_info.types)
        fields = []
        for data, field_type in field_data:
            if len(data) == 0:
                fields.append(None)
            else:
                fields.append(java_to_python_converter(data, field_type))
        return tuple(fields)


def java_to_python_converter(data, field_type: WrapperTypeInfo):
    if isinstance(field_type, RowTypeInfo):
        data = zip(list(data[1:]), field_type.get_field_types())
        fields = []
        for d, d_type in data:
            fields.append(java_to_python_converter(d, d_type))
        return tuple(fields)
    else:
        data = pickle.loads(data)
        if field_type == Types.SQL_TIME():
            seconds, microseconds = divmod(data, 10 ** 6)
            minutes, seconds = divmod(seconds, 60)
            hours, minutes = divmod(minutes, 60)
            return datetime.time(hours, minutes, seconds, microseconds)
        elif field_type == Types.SQL_DATE():
            return field_type.from_internal_type(data)
        elif field_type == Types.SQL_TIMESTAMP():
            return field_type.from_internal_type(int(data.timestamp() * 10 ** 6))
        elif field_type == Types.FLOAT():
            return field_type.from_internal_type(ast.literal_eval(data))
        elif BasicArrayTypeInfo.is_basic_array_type_info(field_type) or \
                PrimitiveArrayTypeInfo.is_primitive_array_type_info(field_type):
            element_type = typeinfo._from_java_type(
                field_type.get_java_type_info().getComponentInfo())
            elements = []
            for element_bytes in data:
                elements.append(java_to_python_converter(element_bytes, element_type))
            return elements
        else:
            return field_type.from_internal_type(data)
