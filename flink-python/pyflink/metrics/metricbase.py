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
from enum import Enum


class MetricGroup(abc.ABC):

    def add_group(self, name: str, extra: str = None) -> 'MetricGroup':
        """
        if extra is not None, creates a new key-value MetricGroup pair. The key group
        is added to this groups sub-groups, while the value group is added to the key
        group's sub-groups. This method returns the value group.

        The only difference between calling this method and
        `group.add_group(key).add_group(value)` is that get_all_variables()
        of the value group return an additional `"<key>"="value"` pair.
        """
        pass

    def get_scope_components(self) -> []:
        """
        Gets the scope as an array of the scope components, for example
        `["host-7", "taskmanager-2", "window_word_count", "my-mapper"]`
        """
        pass

    def get_all_variables(self) -> map:
        """
        Returns a map of all variables and their associated value, for example
        `{"<host>"="host-7", "<tm_id>"="taskmanager-2"}`
        """
        pass

    def get_metric_identifier(self, metric_name: str) -> str:
        """
        Returns the fully qualified metric name, for example
        `host-7.taskmanager-2.window_word_count.my-mapper.metricName`
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
            variables,
            scope_components,
            delimiter,
            metric_group_type=MetricGroupType.generic):
        self._parent = parent
        self._sub_groups = []
        self._variables = variables
        self._scope_components = scope_components
        self._name = name
        self._metric_group_type = metric_group_type
        self._delimiter = delimiter

    def _add_group(self, name: str, metric_group_type) -> 'MetricGroup':
        for group in self._sub_groups:
            if name == group._name and metric_group_type == group._metric_group_type:
                # we don't create same metric group repeatedly
                return group

        # variables
        child_variables = {}
        child_variables.update(self._variables)
        if metric_group_type == MetricGroupType.value:
            child_variables[self._name] = name

        # add scope components
        child_compoents = self.get_scope_components().copy()
        child_compoents.append(name)

        sub_group = GenericMetricGroup(
            self,
            name,
            child_variables,
            child_compoents,
            self._delimiter,
            metric_group_type)
        self._sub_groups.append(sub_group)
        return sub_group

    def add_group(self, name: str, extra: str = None) -> 'MetricGroup':
        if extra is None:
            return self._add_group(name, MetricGroupType.generic)
        else:
            return self._add_group(name, MetricGroupType.key)\
                ._add_group(extra, MetricGroupType.value)

    def get_scope_components(self) -> []:
        return self._scope_components

    def get_all_variables(self) -> map:
        return self._variables

    def get_metric_identifier(self, metric_name: str) -> str:
        components = self._scope_components.copy()
        components.append(metric_name)
        return self._delimiter.join(components)
