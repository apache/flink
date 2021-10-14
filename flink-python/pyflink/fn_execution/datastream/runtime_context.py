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
from typing import Dict, Union

from pyflink.datastream import RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, ValueState, ListStateDescriptor, \
    ListState, MapStateDescriptor, MapState, ReducingStateDescriptor, ReducingState, \
    AggregatingStateDescriptor, AggregatingState
from pyflink.fn_execution.coders import from_type_info, MapCoder, GenericArrayCoder
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend
from pyflink.metrics import MetricGroup


class StreamingRuntimeContext(RuntimeContext):

    def __init__(self,
                 task_name: str,
                 task_name_with_subtasks: str,
                 number_of_parallel_subtasks: int,
                 max_number_of_parallel_subtasks: int,
                 index_of_this_subtask: int,
                 attempt_number: int,
                 job_parameters: Dict[str, str],
                 metric_group: MetricGroup,
                 keyed_state_backend: Union[RemoteKeyedStateBackend, None],
                 in_batch_execution_mode: bool):
        self._task_name = task_name
        self._task_name_with_subtasks = task_name_with_subtasks
        self._number_of_parallel_subtasks = number_of_parallel_subtasks
        self._max_number_of_parallel_subtasks = max_number_of_parallel_subtasks
        self._index_of_this_subtask = index_of_this_subtask
        self._attempt_number = attempt_number
        self._job_parameters = job_parameters
        self._metric_group = metric_group
        self._keyed_state_backend = keyed_state_backend
        self._in_batch_execution_mode = in_batch_execution_mode

    def get_task_name(self) -> str:
        """
        Returns the name of the task in which the UDF runs, as assigned during plan construction.
        """
        return self._task_name

    def get_number_of_parallel_subtasks(self) -> int:
        """
        Gets the parallelism with which the parallel task runs.
        """
        return self._number_of_parallel_subtasks

    def get_max_number_of_parallel_subtasks(self) -> int:
        """
        Gets the number of max-parallelism with which the parallel task runs.
        """
        return self._max_number_of_parallel_subtasks

    def get_index_of_this_subtask(self) -> int:
        """
        Gets the number of this parallel subtask. The numbering starts from 0 and goes up to
        parallelism-1 (parallelism as returned by
        :func:`~RuntimeContext.get_number_of_parallel_subtasks`).
        """
        return self._index_of_this_subtask

    def get_attempt_number(self) -> int:
        """
        Gets the attempt number of this parallel subtask. First attempt is numbered 0.
        """
        return self._attempt_number

    def get_task_name_with_subtasks(self) -> str:
        """
        Returns the name of the task, appended with the subtask indicator, such as "MyTask (3/6)",
        where 3 would be (:func:`~RuntimeContext.get_index_of_this_subtask` + 1), and 6 would be
        :func:`~RuntimeContext.get_number_of_parallel_subtasks`.
        """
        return self._task_name_with_subtasks

    def get_job_parameter(self, key: str, default_value: str):
        """
        Gets the global job parameter value associated with the given key as a string.
        """
        return self._job_parameters[key] if key in self._job_parameters else default_value

    def get_metrics_group(self) -> MetricGroup:
        """
        Gets the metric group.
        """
        return self._metric_group

    def get_state(self, state_descriptor: ValueStateDescriptor) -> ValueState:
        if self._keyed_state_backend:
            return self._keyed_state_backend.get_value_state(
                state_descriptor.name,
                from_type_info(state_descriptor.type_info),
                state_descriptor._ttl_config)
        else:
            raise Exception("This state is only accessible by functions executed on a KeyedStream.")

    def get_list_state(self, state_descriptor: ListStateDescriptor) -> ListState:
        if self._keyed_state_backend:
            array_coder = from_type_info(state_descriptor.type_info)  # type: GenericArrayCoder
            return self._keyed_state_backend.get_list_state(
                state_descriptor.name,
                array_coder._elem_coder,
                state_descriptor._ttl_config)
        else:
            raise Exception("This state is only accessible by functions executed on a KeyedStream.")

    def get_map_state(self, state_descriptor: MapStateDescriptor) -> MapState:
        if self._keyed_state_backend:
            map_coder = from_type_info(state_descriptor.type_info)  # type: MapCoder
            key_coder = map_coder._key_coder
            value_coder = map_coder._value_coder
            return self._keyed_state_backend.get_map_state(
                state_descriptor.name,
                key_coder,
                value_coder,
                state_descriptor._ttl_config)
        else:
            raise Exception("This state is only accessible by functions executed on a KeyedStream.")

    def get_reducing_state(self, state_descriptor: ReducingStateDescriptor) -> ReducingState:
        if self._keyed_state_backend:
            return self._keyed_state_backend.get_reducing_state(
                state_descriptor.get_name(),
                from_type_info(state_descriptor.type_info),
                state_descriptor.get_reduce_function(),
                state_descriptor._ttl_config)
        else:
            raise Exception("This state is only accessible by functions executed on a KeyedStream.")

    def get_aggregating_state(
            self, state_descriptor: AggregatingStateDescriptor) -> AggregatingState:
        if self._keyed_state_backend:
            return self._keyed_state_backend.get_aggregating_state(
                state_descriptor.get_name(),
                from_type_info(state_descriptor.type_info),
                state_descriptor.get_agg_function(),
                state_descriptor._ttl_config)
        else:
            raise Exception("This state is only accessible by functions executed on a KeyedStream.")

    @staticmethod
    def of(runtime_context_proto, metric_group, keyed_state_backend=None):
        return StreamingRuntimeContext(
            runtime_context_proto.task_name,
            runtime_context_proto.task_name_with_subtasks,
            runtime_context_proto.number_of_parallel_subtasks,
            runtime_context_proto.max_number_of_parallel_subtasks,
            runtime_context_proto.index_of_this_subtask,
            runtime_context_proto.attempt_number,
            {p.key: p.value for p in runtime_context_proto.job_parameters},
            metric_group,
            keyed_state_backend,
            runtime_context_proto.in_batch_execution_mode)
