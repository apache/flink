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
import ast
import datetime
import pickle
from abc import abstractmethod

from pyflink.common import Row, RowKind, Configuration
from pyflink.common.typeinfo import (RowTypeInfo, TupleTypeInfo, Types, BasicArrayTypeInfo,
                                     PrimitiveArrayTypeInfo, MapTypeInfo, ListTypeInfo,
                                     ObjectArrayTypeInfo, ExternalTypeInfo, TypeInformation)
from pyflink.java_gateway import get_gateway


class ResultTypeQueryable(object):

    @abstractmethod
    def get_produced_type(self) -> TypeInformation:
        pass


def create_hadoop_configuration(config: Configuration):
    jvm = get_gateway().jvm
    hadoop_config = jvm.org.apache.hadoop.conf.Configuration()
    for k, v in config.to_dict().items():
        hadoop_config.set(k, v)
    return hadoop_config


def create_java_properties(config: Configuration):
    jvm = get_gateway().jvm
    properties = jvm.java.util.Properties()
    for k, v in config.to_dict().items():
        properties.put(k, v)
    return properties


def convert_to_python_obj(data, type_info):
    if type_info == Types.PICKLED_BYTE_ARRAY():
        return pickle.loads(data)
    elif isinstance(type_info, ExternalTypeInfo):
        return convert_to_python_obj(data, type_info._type_info)
    else:
        gateway = get_gateway()
        pickled_bytes = gateway.jvm.PythonBridgeUtils. \
            getPickledBytesFromJavaObject(data, type_info.get_java_type_info())
        return pickled_bytes_to_python_obj(pickled_bytes, type_info)


def pickled_bytes_to_python_obj(data, type_info):
    if isinstance(type_info, RowTypeInfo):
        row_kind = RowKind(int.from_bytes(data[0], 'little'))
        field_data_with_types = zip(list(data[1:]), type_info.get_field_types())
        fields = []
        for field_data, field_type in field_data_with_types:
            if len(field_data) == 0:
                fields.append(None)
            else:
                fields.append(pickled_bytes_to_python_obj(field_data, field_type))
        row = Row.of_kind(row_kind, *fields)
        row.set_field_names(type_info.get_field_names())
        return row
    elif isinstance(type_info, TupleTypeInfo):
        field_data_with_types = zip(data, type_info.get_field_types())
        fields = []
        for field_data, field_type in field_data_with_types:
            if len(field_data) == 0:
                fields.append(None)
            else:
                fields.append(pickled_bytes_to_python_obj(field_data, field_type))
        return tuple(fields)
    else:
        data = pickle.loads(data)
        if type_info == Types.SQL_TIME():
            seconds, microseconds = divmod(data, 10 ** 6)
            minutes, seconds = divmod(seconds, 60)
            hours, minutes = divmod(minutes, 60)
            return datetime.time(hours, minutes, seconds, microseconds)
        elif type_info == Types.SQL_DATE():
            return type_info.from_internal_type(data)
        elif type_info == Types.SQL_TIMESTAMP():
            return type_info.from_internal_type(int(data.timestamp() * 10 ** 6))
        elif type_info == Types.FLOAT():
            return type_info.from_internal_type(ast.literal_eval(data))
        elif isinstance(type_info,
                        (BasicArrayTypeInfo, PrimitiveArrayTypeInfo, ObjectArrayTypeInfo)):
            element_type = type_info._element_type
            elements = []
            for element_bytes in data:
                elements.append(pickled_bytes_to_python_obj(element_bytes, element_type))
            return elements
        elif isinstance(type_info, MapTypeInfo):
            key_type = type_info._key_type_info
            value_type = type_info._value_type_info
            zip_kv = zip(data[0], data[1])
            return dict((pickled_bytes_to_python_obj(k, key_type),
                         pickled_bytes_to_python_obj(v, value_type))
                        for k, v in zip_kv)
        elif isinstance(type_info, ListTypeInfo):
            element_type = type_info.elem_type
            elements = []
            for element_bytes in data:
                elements.append(pickled_bytes_to_python_obj(element_bytes, element_type))
            return elements
        else:
            return type_info.from_internal_type(data)
