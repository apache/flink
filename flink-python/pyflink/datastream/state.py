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
from abc import ABC, abstractmethod

from typing import TypeVar, Generic, Iterable, List, Iterator, Dict, Tuple

from pyflink.common.typeinfo import TypeInformation, Types, PickledBytesTypeInfo

__all__ = [
    'ValueStateDescriptor',
    'ValueState',
    'ListStateDescriptor',
    'ListState',
    'MapStateDescriptor',
    'MapState',
    'ReducingStateDescriptor',
    'ReducingState',
    'AggregatingStateDescriptor',
    'AggregatingState'
]

T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')
IN = TypeVar('IN')
OUT = TypeVar('OUT')


class State(ABC):
    """
    Interface that different types of partitioned state must implement.
    """

    @abstractmethod
    def clear(self) -> None:
        """
        Removes the value mapped under the current key.
        """
        pass


class ValueState(State, Generic[T]):
    """
    :class:`State` interface for partitioned single-value state. The value can be retrieved or
    updated.

    The state is accessed and modified by user functions, and checkpointed consistently by the
    system as part of the distributed snapshots.
    """

    @abstractmethod
    def value(self) -> T:
        """
        Returns the current value for the state. When the state is not partitioned the returned
        value is the same for all inputs in a given operator instance. If state partitioning is
        applied, the value returned depends on the current operator input, as the operator
        maintains an independent state for each partition.
        """
        pass

    @abstractmethod
    def update(self, value: T) -> None:
        """
        Updates the operator state accessible by :func:`value` to the given value. The next time
        :func:`value` is called (for the same state partition) the returned state will represent
        the updated value. When a partitioned state is updated with null, the state for the current
        key will be removed and the default value is returned on the next access.
        """
        pass


class AppendingState(State, Generic[IN, OUT]):
    """
    Base interface for partitioned state taht supports adding elements and inspecting the current
    state. Elements can either be kept in a buffer (list-like) or aggregated into one value.

    This state is accessed and modified by user functions, and checkpointed consistently by the
    system as part of the distributed snapshots.

    The state is only accessible by functions applied on a KeyedStream. The key is automatically
    supplied by the system, so the function always sees the value mapped to the key of the current
    element. That way, the system can handle stream and state partitioning consistently together.
    """

    @abstractmethod
    def get(self) -> OUT:
        """
        Returns the elements under the current key.
        """
        pass

    @abstractmethod
    def add(self, value: IN) -> None:
        """
        Adding the given value to the tail of this list state.
        """
        pass


class MergingState(AppendingState[IN, OUT]):
    """
    Extension of AppendingState that allows merging of state. That is, two instance of MergingState
    can be combined into a single instance that contains all the information of the two merged
    states.
    """
    pass


class ReducingState(MergingState[T, T]):
    """
    :class:`State` interface for reducing state. Elements can be added to the state, they will be
    combined using a reduce function. The current state can be inspected.

    The state is accessed and modified by user functions, and checkpointed consistently by the
    system as part of the distributed snapshots.

    The state is only accessible by functions applied on a KeyedStream. The key is automatically
    supplied by the system, so the function always sees the value mapped to the key of the current
    element. That way, the system can handle stream and state partitioning consistently together.
    """
    pass


class AggregatingState(MergingState[IN, OUT]):
    """
    :class:`State` interface for aggregating state, based on an
    :class:`~pyflink.datastream.functions.AggregateFunction`. Elements that are added to this type
    of state will be eagerly pre-aggregated using a given AggregateFunction.

    The state holds internally always the accumulator type of the AggregateFunction. When
    accessing the result of the state, the function's
    :func:`~pyflink.datastream.functions.AggregateFunction.get_result` method.

    The state is accessed and modified by user functions, and checkpointed consistently by the
    system as part of the distributed snapshots.

    The state is only accessible by functions applied on a KeyedStream. The key is automatically
    supplied by the system, so the function always sees the value mapped to the key of the current
    element. That way, the system can handle stream and state partitioning consistently together.
    """
    pass


class ListState(MergingState[T, Iterable[T]]):
    """
    :class:`State` interface for partitioned list state in Operations.
    The state is accessed and modified by user functions, and checkpointed consistently
    by the system as part of the distributed snapshots.

    Currently only keyed list state is supported.

    When it is a keyed list state, the state key is automatically supplied by the system, so the
    user function always sees the value mapped to the key of the current element. That way, the
    system can handle stream and state partitioning consistently together.
    """

    @abstractmethod
    def update(self, values: List[T]) -> None:
        """
        Updating existing values to to the given list of values.
        """
        pass

    @abstractmethod
    def add_all(self, values: List[T]) -> None:
        """
        Adding the given values to the tail of this list state.
        """
        pass

    def __iter__(self) -> Iterator[T]:
        return iter(self.get())


class MapState(State, Generic[K, V]):
    """
    :class:`State` interface for partitioned key-value state. The key-value pair can be added,
    updated and retrieved.
    The state is accessed and modified by user functions, and checkpointed consistently by the
    system as part of the distributed snapshots.

    The state key is automatically supplied by the system, so the function always sees the value
    mapped to the key of the current element. That way, the system can handle stream and state
    partitioning consistently together.
    """

    @abstractmethod
    def get(self, key: K) -> V:
        """
        Returns the current value associated with the given key.
        """
        pass

    @abstractmethod
    def put(self, key: K, value: V) -> None:
        """
        Associates a new value with the given key.
        """
        pass

    @abstractmethod
    def put_all(self, dict_value: Dict[K, V]) -> None:
        """
        Copies all of the mappings from the given map into the state.
        """
        pass

    @abstractmethod
    def remove(self, key: K) -> None:
        """
        Deletes the mapping of the given key.
        """
        pass

    @abstractmethod
    def contains(self, key: K) -> bool:
        """
        Returns whether there exists the given mapping.
        """
        pass

    @abstractmethod
    def items(self) -> Iterable[Tuple[K, V]]:
        """
        Returns all the mappings in the state.
        """
        pass

    @abstractmethod
    def keys(self) -> Iterable[K]:
        """
        Returns all the keys in the state.
        """
        pass

    @abstractmethod
    def values(self) -> Iterable[V]:
        """
        Returns all the values in the state.
        """
        pass

    @abstractmethod
    def is_empty(self) -> bool:
        """
        Returns true if this state contains no key-value mappings, otherwise false.
        """
        pass

    def __getitem__(self, key: K) -> V:
        return self.get(key)

    def __setitem__(self, key: K, value: V) -> None:
        self.put(key, value)

    def __delitem__(self, key: K) -> None:
        self.remove(key)

    def __contains__(self, key: K) -> bool:
        return self.contains(key)

    def __iter__(self) -> Iterator[K]:
        return iter(self.keys())


class StateDescriptor(ABC):
    """
    Base class for state descriptors. A StateDescriptor is used for creating partitioned State in
    stateful operations.
    """

    def __init__(self, name: str, type_info: TypeInformation):
        """
        Constructor for StateDescriptor.

        :param name: The name of the state
        :param type_info: The type information of the value.
        """
        self.name = name
        self.type_info = type_info

    def get_name(self) -> str:
        """
        Get the name of the state.

        :return: The name of the state.
        """
        return self.name


class ValueStateDescriptor(StateDescriptor):
    """
    StateDescriptor for ValueState. This can be used to create partitioned value state using
    RuntimeContext.get_state(ValueStateDescriptor).
    """

    def __init__(self, name: str, value_type_info: TypeInformation):
        """
        Constructor of the ValueStateDescriptor.

        :param name: The name of the state.
        :param value_type_info: the type information of the state.
        """
        if not isinstance(value_type_info, PickledBytesTypeInfo):
            raise ValueError("The type information of the value could only be PickledBytesTypeInfo "
                             "(created via Types.PICKLED_BYTE_ARRAY()) currently, got %s."
                             % type(value_type_info))
        super(ValueStateDescriptor, self).__init__(name, value_type_info)


class ListStateDescriptor(StateDescriptor):
    """
    StateDescriptor for ListState. This can be used to create state where the type is a list that
    can be appended and iterated over.
    """

    def __init__(self, name: str, elem_type_info: TypeInformation):
        """
        Constructor of the ListStateDescriptor.

        :param name: The name of the state.
        :param elem_type_info: the type information of the state element.
        """
        if not isinstance(elem_type_info, PickledBytesTypeInfo):
            raise ValueError("The type information of the element could only be "
                             "PickledBytesTypeInfo (created via Types.PICKLED_BYTE_ARRAY()) "
                             "currently, got %s" % type(elem_type_info))
        super(ListStateDescriptor, self).__init__(name, Types.LIST(elem_type_info))


class MapStateDescriptor(StateDescriptor):
    """
    StateDescriptor for MapState. This can be used to create state where the type is a map that can
    be updated and iterated over.
    """

    def __init__(self, name: str, key_type_info: TypeInformation, value_type_info: TypeInformation):
        """
        Constructor of the MapStateDescriptor.

        :param name: The name of the state.
        :param key_type_info: The type information of the key.
        :param value_type_info: the type information of the value.
        """
        if not isinstance(key_type_info, PickledBytesTypeInfo):
            raise ValueError("The type information of the key could only be PickledBytesTypeInfo "
                             "(created via Types.PICKLED_BYTE_ARRAY()) currently, got %s"
                             % type(key_type_info))
        if not isinstance(value_type_info, PickledBytesTypeInfo):
            raise ValueError("The type information of the value could only be PickledBytesTypeInfo "
                             "(created via Types.PICKLED_BYTE_ARRAY()) currently, got %s"
                             % type(value_type_info))
        super(MapStateDescriptor, self).__init__(name, Types.MAP(key_type_info, value_type_info))


class ReducingStateDescriptor(StateDescriptor):
    """
    StateDescriptor for ReducingState. This can be used to create partitioned reducing state using
    RuntimeContext.get_reducing_state(ReducingStateDescriptor).
    """

    def __init__(self,
                 name: str,
                 reduce_function,
                 type_info: TypeInformation):
        """
        Constructor of the ReducingStateDescriptor.

        :param name: The name of the state.
        :param reduce_function: The ReduceFunction used to aggregate the state.
        :param type_info: The type of the values in the state.
        """
        super(ReducingStateDescriptor, self).__init__(name, type_info)
        from pyflink.datastream.functions import ReduceFunction, ReduceFunctionWrapper
        if not isinstance(reduce_function, ReduceFunction):
            if callable(reduce_function):
                reduce_function = ReduceFunctionWrapper(reduce_function)  # type: ignore
            else:
                raise TypeError("The input must be a ReduceFunction or a callable function!")
        if not isinstance(type_info, PickledBytesTypeInfo):
            raise ValueError("The type information of the state could only be PickledBytesTypeInfo "
                             "(created via Types.PICKLED_BYTE_ARRAY()) currently, got %s"
                             % type(type_info))
        self._reduce_function = reduce_function

    def get_reduce_function(self):
        return self._reduce_function


class AggregatingStateDescriptor(StateDescriptor):
    """
    A StateDescriptor for AggregatingState.

    The type internally stored in the state is the type of the Accumulator of the
    :func:`~pyflink.datastream.functions.AggregateFunction`.
    """

    def __init__(self,
                 name: str,
                 agg_function,
                 state_type_info):
        super(AggregatingStateDescriptor, self).__init__(name, state_type_info)
        from pyflink.datastream.functions import AggregateFunction
        if not isinstance(agg_function, AggregateFunction):
            raise TypeError("The input must be a pyflink.datastream.functions.AggregateFunction!")
        if not isinstance(state_type_info, PickledBytesTypeInfo):
            raise ValueError("The type information of the state could only be PickledBytesTypeInfo "
                             "(created via Types.PICKLED_BYTE_ARRAY()) currently, got %s"
                             % type(state_type_info))
        self._agg_function = agg_function

    def get_agg_function(self):
        return self._agg_function
