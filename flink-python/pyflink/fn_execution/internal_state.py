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
from typing import Generic, TypeVar, List, Iterable, Collection

from pyflink.datastream.state import State, ValueState, AppendingState, MergingState, ListState, \
    AggregatingState, ReducingState, MapState

N = TypeVar('N')
T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')
IN = TypeVar('IN')
OUT = TypeVar('OUT')


class InternalKvState(State, Generic[N]):
    """
    The :class:InternalKvState is the root of the internal state type hierarchy, similar to the
    :class:State being the root of the public API state hierarchy.

    The internal state classes give access to the namespace getters and setters and access to
    additional functionality, like raw value access or state merging.

    The public API state hierarchy is intended to be programmed against by Flink applications. The
    internal state hierarchy holds all the auxiliary methods that are used by the runtime and not
    intended to be used by user applications. These internal methods are considered of limited use
    to users and only confusing, and are usually not regarded as stable across releases.

    """

    @abstractmethod
    def set_current_namespace(self, namespace: N) -> None:
        """
        Sets the current namespace, which will be used when using the state access methods.

        :param namespace: The namespace.
        """
        pass


class InternalValueState(InternalKvState[N], ValueState[T], ABC):
    """
    The peer to the :class:ValueState in the internal state type hierarchy.
    """
    pass


class InternalAppendingState(InternalKvState[N], AppendingState[IN, OUT], ABC):
    """
    The peer to the :class:AppendingState in the internal state type hierarchy.
    """
    pass


class InternalMergingState(InternalAppendingState[N, IN, OUT], MergingState[IN, OUT]):
    """
    The peer to the :class:MergingState in the internal state type hierarchy.
    """

    @abstractmethod
    def merge_namespaces(self, target: N, sources: Collection[N]) -> None:
        """
        Merges the state of the current key for the given source namespaces into the state of the
        target namespace.

        :param target: The target namespace where the merged state should be stored.
        :param sources: The source namespaces whose state should be merged.
        """
        pass


class InternalListState(InternalMergingState[N, List[T], Iterable[T]], ListState[T], ABC):
    """
    The peer to the :class:ListState in the internal state type hierarchy.
    """
    pass


class InternalAggregatingState(InternalMergingState[N, IN, OUT], AggregatingState[IN, OUT], ABC):
    """
    The peer to the :class:AggregatingState in the internal state type hierarchy.
    """
    pass


class InternalReducingState(InternalMergingState[N, T, T], ReducingState[T], ABC):
    """
    The peer to the :class:ReducingState in the internal state type hierarchy.
    """
    pass


class InternalMapState(InternalKvState[N], MapState[K, V], ABC):
    """
    The peer to the :class:MapState in the internal state type hierarchy.
    """
    pass
