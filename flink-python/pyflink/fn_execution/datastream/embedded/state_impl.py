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
from abc import ABC
from typing import List, Iterable, Tuple, Dict, Collection

from pyflink.datastream import ReduceFunction, AggregateFunction
from pyflink.datastream.state import (T, IN, OUT, V, K, State)
from pyflink.fn_execution.embedded.converters import (DataConverter, DictDataConverter,
                                                      ListDataConverter)
from pyflink.fn_execution.internal_state import (InternalValueState, InternalKvState,
                                                 InternalListState, InternalReducingState,
                                                 InternalAggregatingState, InternalMapState,
                                                 N, InternalReadOnlyBroadcastState,
                                                 InternalBroadcastState)


class StateImpl(State, ABC):
    def __init__(self,
                 state,
                 value_converter: DataConverter):
        self._state = state
        self._value_converter = value_converter

    def clear(self):
        self._state.clear()


class KeyedStateImpl(StateImpl, InternalKvState, ABC):

    def __init__(self,
                 state,
                 value_converter: DataConverter,
                 window_converter: DataConverter = None):
        super(KeyedStateImpl, self).__init__(state, value_converter)
        self._window_converter = window_converter

    def set_current_namespace(self, namespace) -> None:
        j_window = self._window_converter.to_external(namespace)
        self._state.setCurrentNamespace(j_window)


class ValueStateImpl(KeyedStateImpl, InternalValueState):
    def __init__(self,
                 value_state,
                 value_converter: DataConverter,
                 window_converter: DataConverter = None):
        super(ValueStateImpl, self).__init__(value_state, value_converter, window_converter)

    def value(self) -> T:
        return self._value_converter.to_internal(self._state.value())

    def update(self, value: T) -> None:
        self._state.update(self._value_converter.to_external(value))


class ListStateImpl(KeyedStateImpl, InternalListState):

    def __init__(self,
                 list_state,
                 value_converter: ListDataConverter,
                 window_converter: DataConverter = None):
        super(ListStateImpl, self).__init__(list_state, value_converter, window_converter)
        self._element_converter = value_converter._field_converter

    def update(self, values: List[T]) -> None:
        self._state.update(self._value_converter.to_external(values))

    def add_all(self, values: List[T]) -> None:
        self._state.addAll(self._value_converter.to_external(values))

    def get(self) -> OUT:
        states = self._value_converter.to_internal(self._state.get())
        if states:
            yield from states

    def add(self, value: IN) -> None:
        self._state.add(self._element_converter.to_external(value))

    def merge_namespaces(self, target: N, sources: Collection[N]) -> None:
        j_target = self._window_converter.to_external(target)
        j_sources = [self._window_converter.to_external(window) for window in sources]
        self._state.mergeNamespaces(j_target, j_sources)


class ReducingStateImpl(KeyedStateImpl, InternalReducingState):

    def __init__(self,
                 value_state,
                 value_converter: DataConverter,
                 reduce_function: ReduceFunction,
                 window_converter: DataConverter = None):
        super(ReducingStateImpl, self).__init__(value_state, value_converter, window_converter)
        self._reduce_function = reduce_function

    def get(self) -> OUT:
        return self._value_converter.to_internal(self._state.value())

    def add(self, value: IN) -> None:
        if value is None:
            self.clear()
        else:
            current_value = self.get()

            if current_value is None:
                reduce_value = value
            else:
                reduce_value = self._reduce_function.reduce(current_value, value)

            self._state.update(self._value_converter.to_external(reduce_value))

    def merge_namespaces(self, target: N, sources: Collection[N]) -> None:
        merged = None
        for source in sources:
            self.set_current_namespace(source)
            source_state = self.get()

            if source_state is None:
                continue

            self.clear()

            if merged is None:
                merged = source_state
            else:
                merged = self._reduce_function.reduce(merged, source_state)

        if merged is not None:
            self.set_current_namespace(target)
            self._state.update(self._value_converter.to_external(merged))


class AggregatingStateImpl(KeyedStateImpl, InternalAggregatingState):
    def __init__(self,
                 value_state,
                 value_converter,
                 agg_function: AggregateFunction,
                 window_converter: DataConverter = None):
        super(AggregatingStateImpl, self).__init__(value_state, value_converter, window_converter)
        self._agg_function = agg_function

    def get(self) -> OUT:
        accumulator = self._value_converter.to_internal(self._state.value())

        if accumulator is None:
            return None
        else:
            return self._agg_function.get_result(accumulator)

    def add(self, value: IN) -> None:
        if value is None:
            self.clear()
        else:
            accumulator = self._value_converter.to_internal(self._state.value())

            if accumulator is None:
                accumulator = self._agg_function.create_accumulator()

            accumulator = self._agg_function.add(value, accumulator)
            self._state.update(self._value_converter.to_external(accumulator))

    def merge_namespaces(self, target: N, sources: Collection[N]) -> None:
        merged = None
        for source in sources:
            self.set_current_namespace(source)
            source_state = self.get()

            if source_state is None:
                continue

            self.clear()

            if merged is None:
                merged = source_state
            else:
                merged = self._agg_function.merge(merged, source_state)

        if merged is not None:
            self.set_current_namespace(target)
            self._state.update(self._value_converter.to_external(merged))


class MapStateImpl(KeyedStateImpl, InternalMapState):
    def __init__(self,
                 map_state,
                 map_converter: DictDataConverter,
                 window_converter: DataConverter = None):
        super(MapStateImpl, self).__init__(map_state, map_converter, window_converter)
        self._k_converter = map_converter._key_converter
        self._v_converter = map_converter._value_converter

    def get(self, key: K) -> V:
        return self._v_converter.to_internal(
            self._state.get(self._k_converter.to_external(key)))

    def put(self, key: K, value: V) -> None:
        self._state.put(self._k_converter.to_external(key), self._v_converter.to_external(value))

    def put_all(self, dict_value: Dict[K, V]) -> None:
        self._state.putAll(self._value_converter.to_external(dict_value))

    def remove(self, key: K) -> None:
        self._state.remove(self._k_converter.to_external(key))

    def contains(self, key: K) -> bool:
        return self._state.contains(self._k_converter.to_external(key))

    def items(self) -> Iterable[Tuple[K, V]]:
        entries = self._state.entries()
        if entries:
            for entry in entries:
                yield (self._k_converter.to_internal(entry.getKey()),
                       self._v_converter.to_internal(entry.getValue()))

    def keys(self) -> Iterable[K]:
        keys = self._state.keys()
        if keys:
            for k in keys:
                yield self._k_converter.to_internal(k)

    def values(self) -> Iterable[V]:
        values = self._state.values()
        if values:
            for v in values:
                yield self._v_converter.to_internal(v)

    def is_empty(self) -> bool:
        return self._state.isEmpty()


class ReadOnlyBroadcastStateImpl(StateImpl, InternalReadOnlyBroadcastState):

    def __init__(self,
                 map_state,
                 map_converter: DictDataConverter):
        super(ReadOnlyBroadcastStateImpl, self).__init__(map_state, map_converter)
        self._k_converter = map_converter._key_converter
        self._v_converter = map_converter._value_converter

    def get(self, key: K) -> V:
        return self._v_converter.to_internal(
            self._state.get(self._k_converter.to_external(key)))

    def contains(self, key: K) -> bool:
        return self._state.contains(self._k_converter.to_external(key))

    def items(self) -> Iterable[Tuple[K, V]]:
        entries = self._state.entries()
        for entry in entries:
            yield (self._k_converter.to_internal(entry.getKey()),
                   self._v_converter.to_internal(entry.getValue()))

    def keys(self) -> Iterable[K]:
        for k in self._state.keys():
            yield self._k_converter.to_internal(k)

    def values(self) -> Iterable[V]:
        for v in self._state.values():
            yield self._v_converter.to_internal(v)

    def is_empty(self) -> bool:
        return self._state.isEmpty()


class BroadcastStateImpl(ReadOnlyBroadcastStateImpl, InternalBroadcastState):
    def __init__(self,
                 map_state,
                 map_converter: DictDataConverter):
        super(BroadcastStateImpl, self).__init__(map_state, map_converter)
        self._map_converter = map_converter
        self._k_converter = map_converter._key_converter
        self._v_converter = map_converter._value_converter

    def to_read_only_broadcast_state(self) -> InternalReadOnlyBroadcastState[K, V]:
        return ReadOnlyBroadcastStateImpl(self._state, self._map_converter)

    def put(self, key: K, value: V) -> None:
        self._state.put(self._k_converter.to_external(key), self._v_converter.to_external(value))

    def put_all(self, dict_value: Dict[K, V]) -> None:
        self._state.putAll(self._value_converter.to_external(dict_value))

    def remove(self, key: K) -> None:
        self._state.remove(self._k_converter.to_external(key))
