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
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptySet;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Used by {@link StateAssignmentOperation} to store temporal information while creating {@link
 * OperatorSubtaskState}.
 */
class TaskStateAssignment {
    private static final Logger LOG = LoggerFactory.getLogger(TaskStateAssignment.class);

    final ExecutionJobVertex executionJobVertex;
    final Map<OperatorID, OperatorState> oldState;
    final boolean hasState;
    final boolean hasInputState;
    final boolean hasOutputState;
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
    @Nullable private RescaleMappings outputSubtaskMappings;
    /** The subtask mapping when the input operator was rescaled. */
    @Nullable private RescaleMappings inputSubtaskMappings;
    /**
     * If channel data cannot be safely divided into subtasks (several new subtask indexes are
     * associated with the same old subtask index). Mostly used for range partitioners.
     */
    boolean mayHaveAmbiguousSubtasks;

    @Nullable private TaskStateAssignment[] downstreamAssignments;
    @Nullable private TaskStateAssignment[] upstreamAssignments;

    private final Map<IntermediateDataSetID, TaskStateAssignment> consumerAssignment;
    private final Map<ExecutionJobVertex, TaskStateAssignment> vertexAssignments;

    public TaskStateAssignment(
            ExecutionJobVertex executionJobVertex,
            Map<OperatorID, OperatorState> oldState,
            Map<IntermediateDataSetID, TaskStateAssignment> consumerAssignment,
            Map<ExecutionJobVertex, TaskStateAssignment> vertexAssignments) {

        this.executionJobVertex = executionJobVertex;
        this.oldState = oldState;
        this.hasState =
                oldState.values().stream()
                        .anyMatch(operatorState -> operatorState.getNumberCollectedStates() > 0);

        newParallelism = executionJobVertex.getParallelism();
        this.consumerAssignment = checkNotNull(consumerAssignment);
        this.vertexAssignments = checkNotNull(vertexAssignments);
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

        hasInputState =
                oldState.get(inputOperatorID).getStates().stream()
                        .anyMatch(subState -> !subState.getInputChannelState().isEmpty());
        hasOutputState =
                oldState.get(outputOperatorID).getStates().stream()
                        .anyMatch(subState -> !subState.getResultSubpartitionState().isEmpty());
    }

    public TaskStateAssignment[] getDownstreamAssignments() {
        if (downstreamAssignments == null) {
            downstreamAssignments =
                    Arrays.stream(executionJobVertex.getProducedDataSets())
                            .map(result -> consumerAssignment.get(result.getId()))
                            .toArray(TaskStateAssignment[]::new);
        }
        return downstreamAssignments;
    }

    public TaskStateAssignment[] getUpstreamAssignments() {
        if (upstreamAssignments == null) {
            upstreamAssignments =
                    executionJobVertex.getInputs().stream()
                            .map(result -> vertexAssignments.get(result.getProducer()))
                            .toArray(TaskStateAssignment[]::new);
        }
        return upstreamAssignments;
    }

    public OperatorSubtaskState getSubtaskState(OperatorInstanceID instanceID) {
        checkState(
                subManagedKeyedState.containsKey(instanceID)
                        || !subRawKeyedState.containsKey(instanceID),
                "If an operator has no managed key state, it should also not have a raw keyed state.");

        final StateObjectCollection<InputChannelStateHandle> inputState =
                getState(instanceID, inputChannelStates);
        final StateObjectCollection<ResultSubpartitionStateHandle> outputState =
                getState(instanceID, resultSubpartitionStates);
        return OperatorSubtaskState.builder()
                .setManagedOperatorState(getState(instanceID, subManagedOperatorState))
                .setRawOperatorState(getState(instanceID, subRawOperatorState))
                .setManagedKeyedState(getState(instanceID, subManagedKeyedState))
                .setRawKeyedState(getState(instanceID, subRawKeyedState))
                .setInputChannelState(inputState)
                .setResultSubpartitionState(outputState)
                .setInputRescalingDescriptor(
                        createRescalingDescriptor(
                                instanceID,
                                inputOperatorID,
                                getUpstreamAssignments(),
                                assignment -> assignment.outputSubtaskMappings,
                                assignment ->
                                        assignment.getOutputMapping(
                                                Arrays.asList(assignment.getDownstreamAssignments())
                                                        .indexOf(this)),
                                inputSubtaskMappings,
                                () -> getInputMapping(0),
                                inputState,
                                () -> mayHaveAmbiguousSubtasks))
                .setOutputRescalingDescriptor(
                        createRescalingDescriptor(
                                instanceID,
                                outputOperatorID,
                                getDownstreamAssignments(),
                                assignment -> assignment.inputSubtaskMappings,
                                assignment ->
                                        assignment.getInputMapping(
                                                Arrays.asList(assignment.getUpstreamAssignments())
                                                        .indexOf(this)),
                                outputSubtaskMappings,
                                () -> getOutputMapping(0),
                                outputState,
                                () -> false))
                .build();
    }

    private InflightDataRescalingDescriptor log(
            InflightDataRescalingDescriptor descriptor, int subtask) {
        LOG.debug(
                "created {} for task={} subtask={}",
                descriptor,
                executionJobVertex.getName(),
                subtask);
        return descriptor;
    }

    private InflightDataRescalingDescriptor createRescalingDescriptor(
            OperatorInstanceID instanceID,
            OperatorID expectedOperatorID,
            TaskStateAssignment[] connectedAssignments,
            Function<TaskStateAssignment, RescaleMappings> mappingRetriever,
            Function<TaskStateAssignment, RescaleMappings> mappingCalculator,
            @Nullable RescaleMappings subtaskMappings,
            Supplier<RescaleMappings> subtaskMappingCalculator,
            StateObjectCollection<?> state,
            BooleanSupplier mayHaveAmbiguousSubtasks) {
        if (!expectedOperatorID.equals(instanceID.getOperatorId())) {
            return InflightDataRescalingDescriptor.NO_RESCALE;
        }

        RescaleMappings[] rescaledChannelsMappings =
                Arrays.stream(connectedAssignments)
                        .map(mappingRetriever)
                        .toArray(RescaleMappings[]::new);

        // no state on input and output, especially for any aligned checkpoint
        if (subtaskMappings == null
                && Arrays.stream(rescaledChannelsMappings).allMatch(Objects::isNull)) {
            return InflightDataRescalingDescriptor.NO_RESCALE;
        }

        // no state for this assignment, but state on connected assignment
        if (subtaskMappings == null) {
            // calculate subtask mapping now
            subtaskMappings = subtaskMappingCalculator.get();
        }
        int[] oldSubtaskInstances = subtaskMappings.getMappedIndexes(instanceID.getSubtaskId());
        // No old task is mapped, so no data at all.
        if (oldSubtaskInstances.length == 0) {
            checkState(state.isEmpty(), "Unmapped new subtask should not have any state assigned");
            return log(InflightDataRescalingDescriptor.NO_RESCALE, instanceID.getSubtaskId());
        }

        for (int partition = 0; partition < rescaledChannelsMappings.length; partition++) {
            if (rescaledChannelsMappings[partition] == null) {
                rescaledChannelsMappings[partition] =
                        mappingCalculator.apply(connectedAssignments[partition]);
            }
        }

        // no scaling or simple scale-up without the need of virtual channels.
        if (subtaskMappings.isIdentity()
                && Arrays.stream(rescaledChannelsMappings).allMatch(RescaleMappings::isIdentity)) {
            return log(InflightDataRescalingDescriptor.NO_RESCALE, instanceID.getSubtaskId());
        }

        final Set<Integer> ambiguousSubtasks =
                mayHaveAmbiguousSubtasks.getAsBoolean()
                        ? subtaskMappings.getAmbiguousTargets()
                        : emptySet();
        return log(
                new InflightDataRescalingDescriptor(
                        oldSubtaskInstances, rescaledChannelsMappings, ambiguousSubtasks),
                instanceID.getSubtaskId());
    }

    private <T extends StateObject> StateObjectCollection<T> getState(
            OperatorInstanceID instanceID,
            Map<OperatorInstanceID, List<T>> subManagedOperatorState) {
        List<T> value = subManagedOperatorState.get(instanceID);
        return value != null ? new StateObjectCollection<>(value) : StateObjectCollection.empty();
    }

    public RescaleMappings getOutputMapping(int partitionIndex) {
        final TaskStateAssignment downstreamAssignment = getDownstreamAssignments()[partitionIndex];
        final IntermediateResult output = executionJobVertex.getProducedDataSets()[partitionIndex];
        final int gateIndex = downstreamAssignment.executionJobVertex.getInputs().indexOf(output);

        final SubtaskStateMapper mapper =
                checkNotNull(
                        downstreamAssignment
                                .executionJobVertex
                                .getJobVertex()
                                .getInputs()
                                .get(gateIndex)
                                .getUpstreamSubtaskStateMapper(),
                        "No channel rescaler found during rescaling of channel state");
        final RescaleMappings mapping =
                mapper.getNewToOldSubtasksMapping(
                        oldState.get(outputOperatorID).getParallelism(), newParallelism);
        outputSubtaskMappings = checkSubtaskMapping(outputSubtaskMappings, mapping);
        return outputSubtaskMappings;
    }

    public RescaleMappings getInputMapping(int gateIndex) {
        final SubtaskStateMapper mapper =
                checkNotNull(
                        executionJobVertex
                                .getJobVertex()
                                .getInputs()
                                .get(gateIndex)
                                .getDownstreamSubtaskStateMapper(),
                        "No channel rescaler found during rescaling of channel state");
        final RescaleMappings mapping =
                mapper.getNewToOldSubtasksMapping(
                        oldState.get(inputOperatorID).getParallelism(), newParallelism);

        inputSubtaskMappings = checkSubtaskMapping(inputSubtaskMappings, mapping);
        mayHaveAmbiguousSubtasks |= mapper.isAmbiguous();
        return inputSubtaskMappings;
    }

    @Override
    public String toString() {
        return "TaskStateAssignment for " + executionJobVertex.getName();
    }

    private static RescaleMappings checkSubtaskMapping(
            @Nullable RescaleMappings oldMapping, RescaleMappings mapping) {
        if (oldMapping == null) {
            return mapping;
        }
        if (!oldMapping.equals(mapping)) {
            throw new IllegalStateException(
                    "Incompatible subtask mappings: are multiple operators "
                            + "ingesting/producing intermediate results with varying degrees of parallelism?"
                            + "Found "
                            + oldMapping
                            + " and "
                            + mapping
                            + ".");
        }
        return oldMapping;
    }
}
