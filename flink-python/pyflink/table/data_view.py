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
from typing import TypeVar, Generic, Iterable, List, Any, Iterator, Dict, Tuple

T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')

__all__ = ['DataView', 'ListView', 'MapView']


class DataView(ABC):
    """
    A DataView is a collection type that can be used in the accumulator of an user defined
    :class:`pyflink.table.AggregateFunction`. Depending on the context in which the function
    is used, a DataView can be backed by a normal collection or a state backend.
    """

    @abstractmethod
    def clear(self) -> None:
        """
        Clears the DataView and removes all data.
        """
        pass


class ListView(DataView, Generic[T]):
    """
    A :class:`DataView` that provides list-like functionality in the accumulator of an
    AggregateFunction when large amounts of data are expected.
    """

    def __init__(self):
        self._list = []

    def get(self) -> Iterable[T]:
        """
        Returns an iterable of this list view.
        """
        return self._list

    def add(self, value: T) -> None:
        """
        Adds the given value to this list view.
        """
        self._list.append(value)

    def add_all(self, values: List[T]) -> None:
        """
        Adds all of the elements of the specified list to this list view.
        """
        self._list.extend(values)

    def clear(self) -> None:
        self._list = []

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ListView):
            iter_obj = other.get()
            self_iterator = iter(self)
            for value in iter_obj:
                try:
                    self_value = next(self_iterator)
                except StopIteration:
                    # this list view is shorter than another one
                    return False
                if self_value != value:
                    # the elements are not the same.
                    return False
            try:
                next(self_iterator)
            except StopIteration:
                # the length of this list view is the same as another one
                return True
            else:
                # this list view is longer than another one
                return False
        else:
            # the object is not a ListView
            return False

    def __hash__(self) -> int:
        return hash(self._list)

    def __iter__(self) -> Iterator[T]:
        return iter(self.get())


class MapView(Generic[K, V]):
    """
    A :class:`DataView` that provides dict-like functionality in the accumulator of an
    AggregateFunction when large amounts of data are expected.
    """

    def __init__(self):
        self._dict = dict()

    def get(self, key: K) -> V:
        """
        Return the value for the specified key.
        """
        return self._dict[key]

    def put(self, key: K, value: V) -> None:
        """
        Inserts a value for the given key into the map view.
        If the map view already contains a value for the key, the existing value is overwritten.
        """
        self._dict[key] = value

    def put_all(self, dict_value: Dict[K, V]) -> None:
        """
        Inserts all mappings from the specified map to this map view.
        """
        self._dict.update(dict_value)

    def remove(self, key: K) -> None:
        """
        Deletes the value for the given key.
        """
        del self._dict[key]

    def contains(self, key: K) -> bool:
        """
        Checks if the map view contains a value for a given key.
        """
        return key in self._dict

    def items(self) -> Iterable[Tuple[K, V]]:
        """
        Returns all entries of the map view.
        """
        return self._dict.items()

    def keys(self) -> Iterable[K]:
        """
        Returns all the keys in the map view.
        """
        return self._dict.keys()

    def values(self) -> Iterable[V]:
        """
        Returns all the values in the map view.
        """
        return self._dict.values()

    def is_empty(self) -> bool:
        """
        Returns true if the map view contains no key-value mappings, otherwise false.
        """
        return len(self._dict) == 0

    def clear(self) -> None:
        """
        Removes all entries of this map.
        """
        self._dict.clear()

    def __eq__(self, other: Any) -> bool:
        if other is None:
            return False
        if other.__class__ == MapView:
            return self._dict == other._dict
        else:
            # comparing the content of state backed map view is too expensive
            return other is self

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
