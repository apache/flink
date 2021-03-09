/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.NO_MAPPINGS;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.NO_SUBTASKS;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Used by {@link StateAssignmentOperation} to store temporal information while creating {@link
 * OperatorSubtaskState}.
 */
class TaskStateAssignment {
    final ExecutionJobVertex executionJobVertex;
    final Map<OperatorID, OperatorState> oldState;
    final boolean hasState;
    final int newParallelism;
    final OperatorID inputOperatorID;
    final OperatorID outputOperatorID;

    final Map<OperatorInstanceID, List<OperatorStateHandle>> subManagedOperatorState;
    final Map<OperatorInstanceID, List<OperatorStateHandle>> subRawOperatorState;
    final Map<OperatorInstanceID, List<KeyedStateHandle>> subManagedKeyedState;
    final Map<OperatorInstanceID, List<KeyedStateHandle>> subRawKeyedState;

    final Map<OperatorInstanceID, List<InputChannelStateHandle>> inputChannelStates;
    final Map<OperatorInstanceID, List<ResultSubpartitionStateHandle>> resultSubpartitionStates;
    /** The subtask mapping when the output operator was rescaled. */
    Map<Integer, Set<Integer>> outputSubtaskMappings = emptyMap();
    /** The subtask mapping when the input operator was rescaled. */
    Map<Integer, Set<Integer>> inputSubtaskMappings = emptyMap();
    /**
     * The subpartitions mappings of the upstream task per input set when its output operator was
     * rescaled.
     */
    final Map<Integer, TaskStateAssignment> upstreamAssignments;
    /**
     * The input channel mappings of the downstream task per partition set when its input operator
     * was rescaled.
     */
    final Map<Integer, TaskStateAssignment> downstreamAssignments;

    public TaskStateAssignment(
            ExecutionJobVertex executionJobVertex, Map<OperatorID, OperatorState> oldState) {

        this.executionJobVertex = executionJobVertex;
        this.oldState = oldState;
        this.hasState =
                oldState.values().stream()
                        .anyMatch(operatorState -> operatorState.getNumberCollectedStates() > 0);

        newParallelism = executionJobVertex.getParallelism();
        final int expectedNumberOfSubtasks = newParallelism * oldState.size();

        subManagedOperatorState = new HashMap<>(expectedNumberOfSubtasks);
        subRawOperatorState = new HashMap<>(expectedNumberOfSubtasks);
        inputChannelStates = new HashMap<>(expectedNumberOfSubtasks);
        resultSubpartitionStates = new HashMap<>(expectedNumberOfSubtasks);
        subManagedKeyedState = new HashMap<>(expectedNumberOfSubtasks);
        subRawKeyedState = new HashMap<>(expectedNumberOfSubtasks);

        final List<OperatorIDPair> operatorIDs = executionJobVertex.getOperatorIDs();
        outputOperatorID = operatorIDs.get(0).getGeneratedOperatorID();
        inputOperatorID = operatorIDs.get(operatorIDs.size() - 1).getGeneratedOperatorID();

        upstreamAssignments = new HashMap<>(executionJobVertex.getInputs().size());
        downstreamAssignments = new HashMap<>(executionJobVertex.getProducedDataSets().length);
    }

    public OperatorSubtaskState getSubtaskState(OperatorInstanceID instanceID) {
        checkState(
                subManagedKeyedState.containsKey(instanceID)
                        || !subRawKeyedState.containsKey(instanceID),
                "If an operator has no managed key state, it should also not have a raw keyed state.");

        return OperatorSubtaskState.builder()
                .setManagedOperatorState(getState(instanceID, subManagedOperatorState))
                .setRawOperatorState(getState(instanceID, subRawOperatorState))
                .setManagedKeyedState(getState(instanceID, subManagedKeyedState))
                .setRawKeyedState(getState(instanceID, subRawKeyedState))
                .setInputChannelState(getState(instanceID, inputChannelStates))
                .setResultSubpartitionState(getState(instanceID, resultSubpartitionStates))
                .setInputRescalingDescriptor(
                        inputOperatorID.equals(instanceID.getOperatorId())
                                ? createRescalingDescriptor(
                                        instanceID,
                                        upstreamAssignments,
                                        assignment -> assignment.outputSubtaskMappings,
                                        inputSubtaskMappings)
                                : InflightDataRescalingDescriptor.NO_RESCALE)
                .setOutputRescalingDescriptor(
                        outputOperatorID.equals(instanceID.getOperatorId())
                                ? createRescalingDescriptor(
                                        instanceID,
                                        downstreamAssignments,
                                        assignment -> assignment.inputSubtaskMappings,
                                        outputSubtaskMappings)
                                : InflightDataRescalingDescriptor.NO_RESCALE)
                .build();
    }

    private InflightDataRescalingDescriptor createRescalingDescriptor(
            OperatorInstanceID instanceID,
            Map<Integer, TaskStateAssignment> assignments,
            Function<TaskStateAssignment, Map<Integer, Set<Integer>>> mappingRetriever,
            Map<Integer, Set<Integer>> subtaskMappings) {
        if (assignments.isEmpty() && subtaskMappings.isEmpty()) {
            return InflightDataRescalingDescriptor.NO_RESCALE;
        }

        final Set<Integer> oldTaskInstances =
                subtaskMappings.isEmpty()
                        ? NO_SUBTASKS
                        : subtaskMappings.get(instanceID.getSubtaskId());
        final Map<Integer, RescaledChannelsMapping> rescaledChannelsMappings =
                assignments.isEmpty()
                        ? NO_MAPPINGS
                        : assignments.entrySet().stream()
                                .collect(
                                        Collectors.toMap(
                                                Map.Entry::getKey,
                                                assignment ->
                                                        new RescaledChannelsMapping(
                                                                mappingRetriever.apply(
                                                                        assignment.getValue()))));
        return new InflightDataRescalingDescriptor(oldTaskInstances, rescaledChannelsMappings);
    }

    private <T extends StateObject> StateObjectCollection<T> getState(
            OperatorInstanceID instanceID,
            Map<OperatorInstanceID, List<T>> subManagedOperatorState) {
        List<T> value = subManagedOperatorState.get(instanceID);
        return value != null ? new StateObjectCollection<>(value) : StateObjectCollection.empty();
    }
}
