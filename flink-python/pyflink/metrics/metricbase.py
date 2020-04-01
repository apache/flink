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
import abc
import json
from enum import Enum
from typing import Callable


class MetricGroup(abc.ABC):

    def add_group(self, name: str, extra: str = None) -> 'MetricGroup':
        """
        Creates a new MetricGroup and adds it to this groups sub-groups.

        If extra is not None, creates a new key-value MetricGroup pair. The key group
        is added to this groups sub-groups, while the value group is added to the key
        group's sub-groups. This method returns the value group.

        The only difference between calling this method and
        `group.add_group(key).add_group(value)` is that get_all_variables()
        of the value group return an additional `"<key>"="value"` pair.
        """
        pass

    def counter(self, name: str) -> 'Counter':
        """
        Registers a new `Counter` with Flink.
        """
        pass

    def gauge(self, name: str, obj: Callable[[], int]) -> None:
        """
        Registers a new `Gauge` with Flink.
        """
        pass

    def meter(self, name: str, time_span_in_seconds: int = 60) -> 'Meter':
        """
        Registers a new `Meter` with Flink.
        """
        # There is no meter type in Beam, use counter to implement meter
        pass

    def distribution(self, name: str) -> 'Distribution':
        """
        Registers a new `Distribution` with Flink.
        """
        pass


class MetricGroupType(Enum):
    """
    Indicate the type of MetricGroup.
    """
    generic = 0
    key = 1
    value = 2


class GenericMetricGroup(MetricGroup):

    def __init__(
            self,
            parent,
            name,
            metric_group_type=MetricGroupType.generic):
        self._parent = parent
        self._sub_groups = []
        self._name = name
        self._metric_group_type = metric_group_type
        self._flink_gauge = {}
        self._beam_gauge = {}

    def _add_group(self, name: str, metric_group_type) -> 'MetricGroup':
        for group in self._sub_groups:
            if name == group._name and metric_group_type == group._metric_group_type:
                # we don't create same metric group repeatedly
                return group

        sub_group = GenericMetricGroup(
            self,
            name,
            metric_group_type)
        self._sub_groups.append(sub_group)
        return sub_group

    def add_group(self, name: str, extra: str = None) -> 'MetricGroup':
        if extra is None:
            return self._add_group(name, MetricGroupType.generic)
        else:
            return self._add_group(name, MetricGroupType.key)\
                ._add_group(extra, MetricGroupType.value)

    def counter(self, name: str) -> 'Counter':
        from apache_beam.metrics.metric import Metrics
        return Counter(Metrics.counter(self._get_namespace(), name))

    def gauge(self, name: str, obj: Callable[[], int]) -> None:
        from apache_beam.metrics.metric import Metrics
        self._flink_gauge[name] = obj
        self._beam_gauge[name] = Metrics.gauge(self._get_namespace(), name)

    def meter(self, name: str, time_span_in_seconds: int = 60) -> 'Meter':
        from apache_beam.metrics.metric import Metrics
        # There is no meter type in Beam, use counter to implement meter
        return Meter(Metrics.counter(self._get_namespace(time_span_in_seconds), name))

    def distribution(self, name: str) -> 'Distribution':
        from apache_beam.metrics.metric import Metrics
        return Distribution(Metrics.distribution(self._get_namespace(), name))

    def _get_metric_group_names_and_types(self) -> ([], []):
        if self._name is None:
            return [], []
        else:
            names, types = self._parent._get_metric_group_names_and_types()
            names.append(self._name)
            types.append(str(self._metric_group_type))
            return names, types

    def _get_namespace(self, time=None) -> str:
        names, metric_group_type = self._get_metric_group_names_and_types()
        names.extend(metric_group_type)
        if time is not None:
            names.append(str(time))
        return json.dumps(names)


class Metric(object):
    """Base interface of a metric object."""
    pass


class Counter(Metric):
    """Counter metric interface. Allows a count to be incremented/decremented
    during pipeline execution."""

    def __init__(self, inner_counter):
        self._inner_counter = inner_counter

    def inc(self, n=1):
        self._inner_counter.inc(n)

    def dec(self, n=1):
        self.inc(-n)

    def get_count(self):
        from apache_beam.metrics.execution import MetricsEnvironment
        container = MetricsEnvironment.current_container()
        return container.get_counter(self._inner_counter.metric_name).get_cumulative()


class Distribution(Metric):
    """Distribution Metric interface.

    Allows statistics about the distribution of a variable to be collected during
    pipeline execution."""

    def __init__(self, inner_distribution):
        self._inner_distribution = inner_distribution

    def update(self, value):
        self._inner_distribution.update(value)


class Meter(Metric):
    """Meter Metric interface.

    Metric for measuring throughput."""

    def __init__(self, inner_counter):
        self._inner_counter = inner_counter

    def make_event(self, value=1):
        self._inner_counter.inc(value)

    def get_count(self):
        from apache_beam.metrics.execution import MetricsEnvironment
        container = MetricsEnvironment.current_container()
        return container.get_counter(self._inner_counter.metric_name).get_cumulative()
