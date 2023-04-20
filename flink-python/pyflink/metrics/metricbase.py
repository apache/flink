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
from typing import Callable


class MetricGroup(ABC):
    """
    A MetricGroup is a named container for metrics and further metric subgroups.

    Instances of this class can be used to register new metrics with Flink and to create a nested
    hierarchy based on the group names.

    A MetricGroup is uniquely identified by it's place in the hierarchy and name.

    .. versionadded:: 1.11.0
    """

    @abstractmethod
    def add_group(self, name: str, extra: str = None) -> 'MetricGroup':
        """
        Creates a new MetricGroup and adds it to this groups sub-groups.

        If extra is not None, creates a new key-value MetricGroup pair.
        The key group is added to this group's sub-groups, while the value
        group is added to the key group's sub-groups. In this case,
        the value group will be returned and a user variable will be defined.

        .. versionadded:: 1.11.0
        """
        pass

    @abstractmethod
    def counter(self, name: str) -> 'Counter':
        """
        Registers a new `Counter` with Flink.

        .. versionadded:: 1.11.0
        """
        pass

    @abstractmethod
    def gauge(self, name: str, obj: Callable[[], int]) -> None:
        """
        Registers a new `Gauge` with Flink.

        .. versionadded:: 1.11.0
        """
        pass

    @abstractmethod
    def meter(self, name: str, time_span_in_seconds: int = 60) -> 'Meter':
        """
        Registers a new `Meter` with Flink.

        .. versionadded:: 1.11.0
        """
        pass

    @abstractmethod
    def distribution(self, name: str) -> 'Distribution':
        """
        Registers a new `Distribution` with Flink.

        .. versionadded:: 1.11.0
        """
        pass


class Metric(ABC):
    """
    Base interface of a metric object.

    .. versionadded:: 1.11.0
    """
    pass


class Counter(Metric, ABC):
    """
    Counter metric interface. Allows a count to be incremented/decremented
    during pipeline execution.

    .. versionadded:: 1.11.0
    """

    @abstractmethod
    def inc(self, n: int = 1):
        """
        Increment the current count by the given value.

        .. versionadded:: 1.11.0
        """
        pass

    @abstractmethod
    def dec(self, n: int = 1):
        """
        Decrement the current count by 1.

        .. versionadded:: 1.11.0
        """
        pass

    @abstractmethod
    def get_count(self) -> int:
        """
        Returns the current count.

        .. versionadded:: 1.11.0
        """
        pass


class Meter(Metric, ABC):
    """
    Meter Metric interface.

    Metric for measuring throughput.

    .. versionadded:: 1.11.0
    """

    @abstractmethod
    def mark_event(self, value: int = 1):
        """
        Mark occurrence of the specified number of events.

        .. versionadded:: 1.11.0
        """
        pass

    @abstractmethod
    def get_count(self) -> int:
        """
        Get number of events marked on the meter.

        .. versionadded:: 1.11.0
        """
        pass


class Distribution(Metric, ABC):
    """
    Distribution Metric interface.

    Allows statistics about the distribution of a variable to be collected during
    pipeline execution.

    .. versionadded:: 1.11.0
    """

    @abstractmethod
    def update(self, value):
        pass
