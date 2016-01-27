# ###############################################################################
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
from struct import pack
from flink.connection.Constants import Types


#=====Compatibility====================================================================================================
import sys
PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY2:
    stringtype = basestring
else:
    stringtype = str


#=====Collector========================================================================================================
class Collector(object):
    def __init__(self, con, env, info):
        self._connection = con
        self._serializer = None
        self._env = env
        self._as_array = isinstance(info.types, bytearray)

    def _close(self):
        self._connection.send_end_signal()

    def collect(self, value):
        self._serializer = ArraySerializer(value, self._env._types) if self._as_array else KeyValuePairSerializer(value, self._env._types)
        self.collect = self._collect
        self.collect(value)

    def _collect(self, value):
        serialized_value = self._serializer.serialize(value)
        self._connection.write(serialized_value)


class PlanCollector(object):
    def __init__(self, con, env):
        self._connection = con
        self._env = env

    def _close(self):
        self._connection.send_end_signal()

    def collect(self, value):
        type = _get_type_info(value, self._env._types)
        serializer = _get_serializer(value, self._env._types)
        self._connection.write(b"".join([type, serializer.serialize(value)]))


#=====Serializer=======================================================================================================
class Serializer(object):
    def serialize(self, value):
        pass


class KeyValuePairSerializer(Serializer):
    def __init__(self, value, custom_types):
        self._typeK = [_get_type_info(key, custom_types) for key in value[0]]
        self._typeV = _get_type_info(value[1], custom_types)
        self._typeK_length = [len(type) for type in self._typeK]
        self._typeV_length = len(self._typeV)
        self._serializerK = [_get_serializer(key, custom_types) for key in value[0]]
        self._serializerV = _get_serializer(value[1], custom_types)

    def serialize(self, value):
        bits = [pack(">i", len(value[0]))[3:4]]
        for i in range(len(value[0])):
            x = self._serializerK[i].serialize(value[0][i])
            bits.append(pack(">i", len(x) + self._typeK_length[i]))
            bits.append(self._typeK[i])
            bits.append(x)
        v = self._serializerV.serialize(value[1])
        bits.append(pack(">i", len(v) + self._typeV_length))
        bits.append(self._typeV)
        bits.append(v)
        return b"".join(bits)


class ArraySerializer(Serializer):
    def __init__(self, value, custom_types):
        self._type = _get_type_info(value, custom_types)
        self._type_length = len(self._type)
        self._serializer = _get_serializer(value, custom_types)

    def serialize(self, value):
        serialized_value = self._serializer.serialize(value)
        return b"".join([pack(">i", len(serialized_value) + self._type_length), self._type, serialized_value])


def _get_type_info(value, custom_types):
    if isinstance(value, (list, tuple)):
        return b"".join([pack(">i", len(value))[3:4], b"".join([_get_type_info(field, custom_types) for field in value])])
    elif value is None:
        return Types.TYPE_NULL
    elif isinstance(value, stringtype):
        return Types.TYPE_STRING
    elif isinstance(value, bool):
        return Types.TYPE_BOOLEAN
    elif isinstance(value, int) or PY2 and isinstance(value, long):
        return Types.TYPE_LONG
    elif isinstance(value, bytearray):
        return Types.TYPE_BYTES
    elif isinstance(value, float):
        return Types.TYPE_DOUBLE
    else:
        for entry in custom_types:
            if isinstance(value, entry[1]):
                return entry[0]
        raise Exception("Unsupported Type encountered.")


def _get_serializer(value, custom_types):
    if isinstance(value, (list, tuple)):
        return TupleSerializer(value, custom_types)
    elif value is None:
        return NullSerializer()
    elif isinstance(value, stringtype):
        return StringSerializer()
    elif isinstance(value, bool):
        return BooleanSerializer()
    elif isinstance(value, int) or PY2 and isinstance(value, long):
        return LongSerializer()
    elif isinstance(value, bytearray):
        return ByteArraySerializer()
    elif isinstance(value, float):
        return FloatSerializer()
    else:
        for entry in custom_types:
            if isinstance(value, entry[1]):
                return CustomTypeSerializer(entry[0], entry[2])
        raise Exception("Unsupported Type encountered.")


class CustomTypeSerializer(Serializer):
    def __init__(self, id, serializer):
        self._id = id
        self._serializer = serializer

    def serialize(self, value):
        msg = self._serializer.serialize(value)
        return b"".join([pack(">i",len(msg)), msg])


class TupleSerializer(Serializer):
    def __init__(self, value, custom_types):
        self.serializer = [_get_serializer(field, custom_types) for field in value]

    def serialize(self, value):
        bits = []
        for i in range(len(value)):
            bits.append(self.serializer[i].serialize(value[i]))
        return b"".join(bits)


class BooleanSerializer(Serializer):
    def serialize(self, value):
        return pack(">?", value)


class FloatSerializer(Serializer):
    def serialize(self, value):
        return pack(">d", value)


class LongSerializer(Serializer):
    def serialize(self, value):
        return pack(">q", value)


class ByteArraySerializer(Serializer):
    def serialize(self, value):
        value = bytes(value)
        return pack(">I", len(value)) + value


class StringSerializer(Serializer):
    def serialize(self, value):
        value = value.encode("utf-8")
        return pack(">I", len(value)) + value


class NullSerializer(Serializer):
    def serialize(self, value):
        return b""