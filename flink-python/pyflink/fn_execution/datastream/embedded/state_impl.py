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
from typing import List, Iterable, Tuple, Dict

from pyflink.datastream import ReduceFunction, AggregateFunction
from pyflink.datastream.state import (ValueState, T, State, ListState, IN, OUT, ReducingState,
                                      AggregatingState, MapState, V, K)
from pyflink.fn_execution.embedded.converters import (DataConverter, DictDataConverter,
                                                      ListDataConverter)


class StateImpl(State):
    def __init__(self, state, value_converter: DataConverter):
        self._state = state
        self._value_converter = value_converter

    def clear(self):
        self._state.clear()


class ValueStateImpl(StateImpl, ValueState):
    def __init__(self, value_state, value_converter: DataConverter):
        super(ValueStateImpl, self).__init__(value_state, value_converter)

    def value(self) -> T:
        return self._value_converter.to_internal(self._state.value())

    def update(self, value: T) -> None:
        self._state.update(self._value_converter.to_external(value))


class ListStateImpl(StateImpl, ListState):

    def __init__(self, list_state, value_converter: ListDataConverter):
        super(ListStateImpl, self).__init__(list_state, value_converter)
        self._element_converter = value_converter._field_converter

    def update(self, values: List[T]) -> None:
        self._state.update(self._value_converter.to_external(values))

    def add_all(self, values: List[T]) -> None:
        self._state.addAll(self._value_converter.to_external(values))

    def get(self) -> OUT:
        return self._value_converter.to_internal(self._state.get())

    def add(self, value: IN) -> None:
        self._state.add(self._element_converter.to_external(value))


class ReducingStateImpl(StateImpl, ReducingState):

    def __init__(self,
                 value_state,
                 value_converter: DataConverter,
                 reduce_function: ReduceFunction):
        super(ReducingStateImpl, self).__init__(value_state, value_converter)
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


class AggregatingStateImpl(StateImpl, AggregatingState):
    def __init__(self,
                 value_state,
                 value_converter,
                 agg_function: AggregateFunction):
        super(AggregatingStateImpl, self).__init__(value_state, value_converter)
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


class MapStateImpl(StateImpl, MapState):
    def __init__(self, map_state, map_converter: DictDataConverter):
        super(MapStateImpl, self).__init__(map_state, map_converter)
        self._k_converter = map_converter._key_converter
        self._v_converter = map_converter._value_converter

    def get(self, key: K) -> V:
        return self._value_converter.to_internal(
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
