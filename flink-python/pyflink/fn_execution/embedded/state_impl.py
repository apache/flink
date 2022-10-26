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

from pyflink.datastream.state import (ValueStateDescriptor, ListStateDescriptor, MapStateDescriptor,
                                      StateDescriptor, ReducingStateDescriptor,
                                      AggregatingStateDescriptor)
from pyflink.fn_execution.datastream.embedded.state_impl import (ValueStateImpl, ListStateImpl,
                                                                 MapStateImpl, ReducingStateImpl,
                                                                 AggregatingStateImpl)
from pyflink.fn_execution.embedded.converters import from_type_info
from pyflink.fn_execution.embedded.java_utils import to_java_state_descriptor

JVoidNamespace = findClass('org.apache.flink.runtime.state.VoidNamespace')
JVoidNamespaceSerializer = findClass('org.apache.flink.runtime.state.VoidNamespaceSerializer')

JVoidNamespace_INSTANCE = JVoidNamespace.INSTANCE
JVoidNamespaceSerializer_INSTANCE = JVoidNamespaceSerializer.INSTANCE


class KeyedStateBackend(object):
    def __init__(self,
                 function_context,
                 keyed_state_backend,
                 window_serializer=JVoidNamespaceSerializer_INSTANCE,
                 window_converter=None):
        self._function_context = function_context
        self._keyed_state_backend = keyed_state_backend
        self._window_serializer = window_serializer
        self._window_converter = window_converter

    def get_current_key(self):
        return self._function_context.get_current_key()

    def get_value_state(self, state_descriptor: ValueStateDescriptor) -> ValueStateImpl:
        return ValueStateImpl(
            self._get_or_create_keyed_state(state_descriptor),
            from_type_info(state_descriptor.type_info),
            self._window_converter)

    def get_list_state(self, state_descriptor: ListStateDescriptor) -> ListStateImpl:
        return ListStateImpl(
            self._get_or_create_keyed_state(state_descriptor),
            from_type_info(state_descriptor.type_info),
            self._window_converter)

    def get_map_state(self, state_descriptor: MapStateDescriptor) -> MapStateImpl:
        return MapStateImpl(
            self._get_or_create_keyed_state(state_descriptor),
            from_type_info(state_descriptor.type_info),
            self._window_converter)

    def get_reducing_state(self, state_descriptor: ReducingStateDescriptor):
        return ReducingStateImpl(
            self._get_or_create_keyed_state(state_descriptor),
            from_type_info(state_descriptor.type_info),
            state_descriptor.get_reduce_function(),
            self._window_converter)

    def get_aggregating_state(self, state_descriptor: AggregatingStateDescriptor):
        return AggregatingStateImpl(
            self._get_or_create_keyed_state(state_descriptor),
            from_type_info(state_descriptor.type_info),
            state_descriptor.get_agg_function(),
            self._window_converter)

    def _get_or_create_keyed_state(self, state_descriptor: StateDescriptor):
        return self._keyed_state_backend.getPartitionedState(
            JVoidNamespace_INSTANCE,
            self._window_serializer,
            to_java_state_descriptor(state_descriptor))
