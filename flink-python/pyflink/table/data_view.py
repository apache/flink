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
from typing import TypeVar, Generic, Iterable, List, Any, Iterator

T = TypeVar('T')

__all__ = ['DataView', 'ListView']


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
