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

from typing import TypeVar, Generic

T = TypeVar('T')


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
