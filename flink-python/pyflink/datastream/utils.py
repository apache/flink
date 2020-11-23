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
from pyflink.common.typeinfo import RowTypeInfo, TupleTypeInfo, Types, \
    is_primitive_array_type_info, is_basic_array_type_info
from pyflink.java_gateway import get_gateway


def convert_to_python_obj(data, type_info):
    if type_info == Types.PICKLED_BYTE_ARRAY():
        return pickle.loads(data)
    else:
        gateway = get_gateway()
        pickle_bytes = gateway.jvm.PythonBridgeUtils. \
            getPickledBytesFromJavaObject(data, type_info.get_java_type_info())
        if isinstance(type_info, RowTypeInfo) or isinstance(type_info, TupleTypeInfo):
            field_data = zip(list(pickle_bytes[1:]), type_info.get_field_types())
            fields = []
            for data, field_type in field_data:
                if len(data) == 0:
                    fields.append(None)
                else:
                    fields.append(pickled_bytes_to_python_converter(data, field_type))
            return tuple(fields)
        else:
            return pickled_bytes_to_python_converter(pickle_bytes, type_info)


def pickled_bytes_to_python_converter(data, field_type):
    if isinstance(field_type, RowTypeInfo):
        data = zip(list(data[1:]), field_type.get_field_types())
        fields = []
        for d, d_type in data:
            fields.append(pickled_bytes_to_python_converter(d, d_type))
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
        elif is_basic_array_type_info(field_type) or is_primitive_array_type_info(field_type):
            element_type = typeinfo._from_java_type(
                field_type.get_java_type_info().getComponentInfo())
            elements = []
            for element_bytes in data:
                elements.append(pickled_bytes_to_python_converter(element_bytes, element_type))
            return elements
        else:
            return field_type.from_internal_type(data)
