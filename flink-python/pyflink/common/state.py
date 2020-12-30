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

T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')


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


class ListState(State, Generic[T]):
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
    def get(self) -> Iterable[T]:
        """
        Returns the elements under the current key.
        """
        pass

    @abstractmethod
    def add(self, value: T) -> None:
        """
        Adding the given value to the tail of this list state.
        """
        pass

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
