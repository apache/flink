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
from pemja import findClass

from pyflink.common.typeinfo import (TypeInformation, Types, BasicTypeInfo, BasicType,
                                     PrimitiveArrayTypeInfo, BasicArrayTypeInfo,
                                     ObjectArrayTypeInfo, MapTypeInfo)
from pyflink.datastream.state import (StateDescriptor, ValueStateDescriptor,
                                      ReducingStateDescriptor,
                                      AggregatingStateDescriptor, ListStateDescriptor,
                                      MapStateDescriptor, StateTtlConfig)

# Java Types Class
JTypes = findClass('org.apache.flink.api.common.typeinfo.Types')
JPrimitiveArrayTypeInfo = findClass('org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo')
JBasicArrayTypeInfo = findClass('org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo')
JPickledByteArrayTypeInfo = findClass('org.apache.flink.streaming.api.typeinfo.python.'
                                      'PickledByteArrayTypeInfo')
JMapTypeInfo = findClass('org.apache.flink.api.java.typeutils.MapTypeInfo')

# Java State Descriptor Class
JValueStateDescriptor = findClass('org.apache.flink.api.common.state.ValueStateDescriptor')
JListStateDescriptor = findClass('org.apache.flink.api.common.state.ListStateDescriptor')
JMapStateDescriptor = findClass('org.apache.flink.api.common.state.MapStateDescriptor')

# Java StateTtlConfig
JStateTtlConfig = findClass('org.apache.flink.api.common.state.StateTtlConfig')
JTime = findClass('org.apache.flink.api.common.time.Time')
JUpdateType = findClass('org.apache.flink.api.common.state.StateTtlConfig$UpdateType')
JStateVisibility = findClass('org.apache.flink.api.common.state.StateTtlConfig$StateVisibility')


def to_java_typeinfo(type_info: TypeInformation):
    if isinstance(type_info, BasicTypeInfo):
        basic_type = type_info._basic_type

        if basic_type == BasicType.STRING:
            j_typeinfo = JTypes.STRING
        elif basic_type == BasicType.BYTE:
            j_typeinfo = JTypes.LONG
        elif basic_type == BasicType.BOOLEAN:
            j_typeinfo = JTypes.BOOLEAN
        elif basic_type == BasicType.SHORT:
            j_typeinfo = JTypes.LONG
        elif basic_type == BasicType.INT:
            j_typeinfo = JTypes.LONG
        elif basic_type == BasicType.LONG:
            j_typeinfo = JTypes.LONG
        elif basic_type == BasicType.FLOAT:
            j_typeinfo = JTypes.DOUBLE
        elif basic_type == BasicType.DOUBLE:
            j_typeinfo = JTypes.DOUBLE
        elif basic_type == BasicType.CHAR:
            j_typeinfo = JTypes.STRING
        elif basic_type == BasicType.BIG_INT:
            j_typeinfo = JTypes.BIG_INT
        elif basic_type == BasicType.BIG_DEC:
            j_typeinfo = JTypes.BIG_DEC
        elif basic_type == BasicType.INSTANT:
            j_typeinfo = JTypes.INSTANT
        else:
            raise TypeError("Invalid BasicType %s." % basic_type)

    elif isinstance(type_info, PrimitiveArrayTypeInfo):
        element_type = type_info._element_type

        if element_type == Types.BOOLEAN():
            j_typeinfo = JPrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO
        elif element_type == Types.BYTE():
            j_typeinfo = JPrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
        elif element_type == Types.SHORT():
            j_typeinfo = JPrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO
        elif element_type == Types.INT():
            j_typeinfo = JPrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO
        elif element_type == Types.LONG():
            j_typeinfo = JPrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO
        elif element_type == Types.FLOAT():
            j_typeinfo = JPrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO
        elif element_type == Types.DOUBLE():
            j_typeinfo = JPrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO
        elif element_type == Types.CHAR():
            j_typeinfo = JPrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO
        else:
            raise TypeError("Invalid element type for a primitive array.")

    elif isinstance(type_info, BasicArrayTypeInfo):
        element_type = type_info._element_type

        if element_type == Types.BOOLEAN():
            j_typeinfo = JBasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO
        elif element_type == Types.BYTE():
            j_typeinfo = JBasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO
        elif element_type == Types.SHORT():
            j_typeinfo = JBasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO
        elif element_type == Types.INT():
            j_typeinfo = JBasicArrayTypeInfo.INT_ARRAY_TYPE_INFO
        elif element_type == Types.LONG():
            j_typeinfo = JBasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO
        elif element_type == Types.FLOAT():
            j_typeinfo = JBasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO
        elif element_type == Types.DOUBLE():
            j_typeinfo = JBasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO
        elif element_type == Types.CHAR():
            j_typeinfo = JBasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO
        elif element_type == Types.STRING():
            j_typeinfo = JBasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO
        else:
            raise TypeError("Invalid element type for a basic array.")

    elif isinstance(type_info, ObjectArrayTypeInfo):
        element_type = type_info._element_type

        j_typeinfo = JTypes.OBJECT_ARRAY(to_java_typeinfo(element_type))

    elif isinstance(type_info, MapTypeInfo):
        j_key_typeinfo = to_java_typeinfo(type_info._key_type_info)
        j_value_typeinfo = to_java_typeinfo(type_info._value_type_info)

        j_typeinfo = JMapTypeInfo(j_key_typeinfo, j_value_typeinfo)
    else:
        j_typeinfo = JPickledByteArrayTypeInfo.PICKLED_BYTE_ARRAY_TYPE_INFO

    return j_typeinfo


def to_java_state_ttl_config(ttl_config: StateTtlConfig):
    j_ttl_config_builder = JStateTtlConfig.newBuilder(
        JTime.milliseconds(ttl_config.get_ttl().to_milliseconds()))

    update_type = ttl_config.get_update_type()
    if update_type == StateTtlConfig.UpdateType.Disabled:
        j_ttl_config_builder.setUpdateType(JUpdateType.Disabled)
    elif update_type == StateTtlConfig.UpdateType.OnCreateAndWrite:
        j_ttl_config_builder.setUpdateType(JUpdateType.OnCreateAndWrite)
    elif update_type == StateTtlConfig.UpdateType.OnReadAndWrite:
        j_ttl_config_builder.setUpdateType(JUpdateType.OnReadAndWrite)

    state_visibility = ttl_config.get_state_visibility()
    if state_visibility == StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp:
        j_ttl_config_builder.setStateVisibility(JStateVisibility.ReturnExpiredIfNotCleanedUp)
    elif state_visibility == StateTtlConfig.StateVisibility.NeverReturnExpired:
        j_ttl_config_builder.setStateVisibility(JStateVisibility.NeverReturnExpired)

    cleanup_strategies = ttl_config.get_cleanup_strategies()
    if not cleanup_strategies.is_cleanup_in_background():
        j_ttl_config_builder.disableCleanupInBackground()

    if cleanup_strategies.in_full_snapshot():
        j_ttl_config_builder.cleanupFullSnapshot()

    incremental_cleanup_strategy = cleanup_strategies.get_incremental_cleanup_strategy()
    if incremental_cleanup_strategy:
        j_ttl_config_builder.cleanupIncrementally(
            incremental_cleanup_strategy.get_cleanup_size(),
            incremental_cleanup_strategy.run_cleanup_for_every_record())

    rocksdb_compact_filter_cleanup_strategy = \
        cleanup_strategies.get_rocksdb_compact_filter_cleanup_strategy()

    if rocksdb_compact_filter_cleanup_strategy:
        j_ttl_config_builder.cleanupInRocksdbCompactFilter(
            rocksdb_compact_filter_cleanup_strategy.get_query_time_after_num_entries(),
            rocksdb_compact_filter_cleanup_strategy.get_periodic_compaction_time())

    return j_ttl_config_builder.build()


def to_java_state_descriptor(state_descriptor: StateDescriptor):
    if isinstance(state_descriptor,
                  (ValueStateDescriptor, ReducingStateDescriptor, AggregatingStateDescriptor)):
        value_type_info = to_java_typeinfo(state_descriptor.type_info)
        j_state_descriptor = JValueStateDescriptor(state_descriptor.name, value_type_info)

    elif isinstance(state_descriptor, ListStateDescriptor):
        element_type_info = to_java_typeinfo(state_descriptor.type_info.elem_type)
        j_state_descriptor = JListStateDescriptor(state_descriptor.name, element_type_info)

    elif isinstance(state_descriptor, MapStateDescriptor):
        key_type_info = to_java_typeinfo(state_descriptor.type_info._key_type_info)
        value_type_info = to_java_typeinfo(state_descriptor.type_info._value_type_info)
        j_state_descriptor = JMapStateDescriptor(
            state_descriptor.name, key_type_info, value_type_info)
    else:
        raise Exception("Unknown supported state_descriptor {0}".format(state_descriptor))

    if state_descriptor._ttl_config:
        j_state_ttl_config = to_java_state_ttl_config(state_descriptor._ttl_config)
        j_state_descriptor.enableTimeToLive(j_state_ttl_config)

    return j_state_descriptor
