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
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor.MappingType;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

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
    final boolean hasNonFinishedState;
    final boolean isFullyFinished;
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
    private final Map<Integer, SubtasksRescaleMapping> outputSubtaskMappings = new HashMap<>();
    /** The subtask mapping when the input operator was rescaled. */
    private final Map<Integer, SubtasksRescaleMapping> inputSubtaskMappings = new HashMap<>();

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
        this.hasNonFinishedState =
                oldState.values().stream()
                        .anyMatch(operatorState -> operatorState.getNumberCollectedStates() > 0);
        this.isFullyFinished = oldState.values().stream().anyMatch(OperatorState::isFullyFinished);
        if (isFullyFinished) {
            checkState(
                    oldState.values().stream().allMatch(OperatorState::isFullyFinished),
                    "JobVertex could not have mixed finished and unfinished operators");
        }

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

    private static int getAssignmentIndex(
            TaskStateAssignment[] assignments, TaskStateAssignment assignment) {
        return Arrays.asList(assignments).indexOf(assignment);
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
                                (assignment, recompute) -> {
                                    int assignmentIndex =
                                            getAssignmentIndex(
                                                    assignment.getDownstreamAssignments(), this);
                                    return assignment.getOutputMapping(assignmentIndex, recompute);
                                },
                                inputSubtaskMappings,
                                this::getInputMapping))
                .setOutputRescalingDescriptor(
                        createRescalingDescriptor(
                                instanceID,
                                outputOperatorID,
                                getDownstreamAssignments(),
                                (assignment, recompute) -> {
                                    int assignmentIndex =
                                            getAssignmentIndex(
                                                    assignment.getUpstreamAssignments(), this);
                                    return assignment.getInputMapping(assignmentIndex, recompute);
                                },
                                outputSubtaskMappings,
                                this::getOutputMapping))
                .build();
    }

    private InflightDataGateOrPartitionRescalingDescriptor log(
            InflightDataGateOrPartitionRescalingDescriptor descriptor, int subtask, int partition) {
        LOG.debug(
                "created {} for task={} subtask={} partition={}",
                descriptor,
                executionJobVertex.getName(),
                subtask,
                partition);
        return descriptor;
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
            BiFunction<TaskStateAssignment, Boolean, SubtasksRescaleMapping> mappingRetriever,
            Map<Integer, SubtasksRescaleMapping> subtaskGateOrPartitionMappings,
            Function<Integer, SubtasksRescaleMapping> subtaskMappingCalculator) {
        if (!expectedOperatorID.equals(instanceID.getOperatorId())) {
            return InflightDataRescalingDescriptor.NO_RESCALE;
        }

        SubtasksRescaleMapping[] rescaledChannelsMappings =
                Arrays.stream(connectedAssignments)
                        .map(assignment -> mappingRetriever.apply(assignment, false))
                        .toArray(SubtasksRescaleMapping[]::new);

        // no state on input and output, especially for any aligned checkpoint
        if (subtaskGateOrPartitionMappings.isEmpty()
                && Arrays.stream(rescaledChannelsMappings).allMatch(Objects::isNull)) {
            return InflightDataRescalingDescriptor.NO_RESCALE;
        }

        InflightDataGateOrPartitionRescalingDescriptor[] gateOrPartitionDescriptors =
                createGateOrPartitionRescalingDescriptors(
                        instanceID,
                        connectedAssignments,
                        assignment -> mappingRetriever.apply(assignment, true),
                        subtaskGateOrPartitionMappings,
                        subtaskMappingCalculator,
                        rescaledChannelsMappings);

        if (Arrays.stream(gateOrPartitionDescriptors)
                .allMatch(InflightDataGateOrPartitionRescalingDescriptor::isIdentity)) {
            return log(InflightDataRescalingDescriptor.NO_RESCALE, instanceID.getSubtaskId());
        } else {
            return log(
                    new InflightDataRescalingDescriptor(gateOrPartitionDescriptors),
                    instanceID.getSubtaskId());
        }
    }

    private InflightDataGateOrPartitionRescalingDescriptor[]
            createGateOrPartitionRescalingDescriptors(
                    OperatorInstanceID instanceID,
                    TaskStateAssignment[] connectedAssignments,
                    Function<TaskStateAssignment, SubtasksRescaleMapping> mappingCalculator,
                    Map<Integer, SubtasksRescaleMapping> subtaskGateOrPartitionMappings,
                    Function<Integer, SubtasksRescaleMapping> subtaskMappingCalculator,
                    SubtasksRescaleMapping[] rescaledChannelsMappings) {
        return IntStream.range(0, rescaledChannelsMappings.length)
                .mapToObj(
                        partition -> {
                            TaskStateAssignment connectedAssignment =
                                    connectedAssignments[partition];
                            SubtasksRescaleMapping rescaleMapping =
                                    Optional.ofNullable(rescaledChannelsMappings[partition])
                                            .orElseGet(
                                                    () ->
                                                            mappingCalculator.apply(
                                                                    connectedAssignment));
                            SubtasksRescaleMapping subtaskMapping =
                                    Optional.ofNullable(
                                                    subtaskGateOrPartitionMappings.get(partition))
                                            .orElseGet(
                                                    () ->
                                                            subtaskMappingCalculator.apply(
                                                                    partition));
                            return getInflightDataGateOrPartitionRescalingDescriptor(
                                    instanceID, partition, rescaleMapping, subtaskMapping);
                        })
                .toArray(InflightDataGateOrPartitionRescalingDescriptor[]::new);
    }

    private InflightDataGateOrPartitionRescalingDescriptor
            getInflightDataGateOrPartitionRescalingDescriptor(
                    OperatorInstanceID instanceID,
                    int partition,
                    SubtasksRescaleMapping rescaleMapping,
                    SubtasksRescaleMapping subtaskMapping) {

        int[] oldSubtaskInstances =
                subtaskMapping.rescaleMappings.getMappedIndexes(instanceID.getSubtaskId());

        // no scaling or simple scale-up without the need of virtual
        // channels.
        boolean isIdentity =
                (subtaskMapping.rescaleMappings.isIdentity()
                                && rescaleMapping.getRescaleMappings().isIdentity())
                        || oldSubtaskInstances.length == 0;

        final Set<Integer> ambiguousSubtasks =
                subtaskMapping.mayHaveAmbiguousSubtasks
                        ? subtaskMapping.rescaleMappings.getAmbiguousTargets()
                        : emptySet();
        return log(
                new InflightDataGateOrPartitionRescalingDescriptor(
                        oldSubtaskInstances,
                        rescaleMapping.getRescaleMappings(),
                        ambiguousSubtasks,
                        isIdentity ? MappingType.IDENTITY : MappingType.RESCALING),
                instanceID.getSubtaskId(),
                partition);
    }

    private <T extends StateObject> StateObjectCollection<T> getState(
            OperatorInstanceID instanceID,
            Map<OperatorInstanceID, List<T>> subManagedOperatorState) {
        List<T> value = subManagedOperatorState.get(instanceID);
        return value != null ? new StateObjectCollection<>(value) : StateObjectCollection.empty();
    }

    private SubtasksRescaleMapping getOutputMapping(int assignmentIndex, boolean recompute) {
        SubtasksRescaleMapping mapping = outputSubtaskMappings.get(assignmentIndex);
        if (recompute && mapping == null) {
            return getOutputMapping(assignmentIndex);
        } else {
            return mapping;
        }
    }

    private SubtasksRescaleMapping getInputMapping(int assignmentIndex, boolean recompute) {
        SubtasksRescaleMapping mapping = inputSubtaskMappings.get(assignmentIndex);
        if (recompute && mapping == null) {
            return getInputMapping(assignmentIndex);
        } else {
            return mapping;
        }
    }

    public SubtasksRescaleMapping getOutputMapping(int partitionIndex) {
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
        return outputSubtaskMappings.compute(
                partitionIndex,
                (idx, oldMapping) ->
                        checkSubtaskMapping(oldMapping, mapping, mapper.isAmbiguous()));
    }

    public SubtasksRescaleMapping getInputMapping(int gateIndex) {
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

        return inputSubtaskMappings.compute(
                gateIndex,
                (idx, oldMapping) ->
                        checkSubtaskMapping(oldMapping, mapping, mapper.isAmbiguous()));
    }

    @Override
    public String toString() {
        return "TaskStateAssignment for " + executionJobVertex.getName();
    }

    private static @Nonnull SubtasksRescaleMapping checkSubtaskMapping(
            @Nullable SubtasksRescaleMapping oldMapping,
            RescaleMappings mapping,
            boolean mayHaveAmbiguousSubtasks) {
        if (oldMapping == null) {
            return new SubtasksRescaleMapping(mapping, mayHaveAmbiguousSubtasks);
        }
        if (!oldMapping.rescaleMappings.equals(mapping)) {
            throw new IllegalStateException(
                    "Incompatible subtask mappings: are multiple operators "
                            + "ingesting/producing intermediate results with varying degrees of parallelism?"
                            + "Found "
                            + oldMapping
                            + " and "
                            + mapping
                            + ".");
        }
        return new SubtasksRescaleMapping(
                mapping, oldMapping.mayHaveAmbiguousSubtasks || mayHaveAmbiguousSubtasks);
    }

    static class SubtasksRescaleMapping {
        private final RescaleMappings rescaleMappings;
        /**
         * If channel data cannot be safely divided into subtasks (several new subtask indexes are
         * associated with the same old subtask index). Mostly used for range partitioners.
         */
        private final boolean mayHaveAmbiguousSubtasks;

        private SubtasksRescaleMapping(
                RescaleMappings rescaleMappings, boolean mayHaveAmbiguousSubtasks) {
            this.rescaleMappings = rescaleMappings;
            this.mayHaveAmbiguousSubtasks = mayHaveAmbiguousSubtasks;
        }

        public RescaleMappings getRescaleMappings() {
            return rescaleMappings;
        }

        public boolean isMayHaveAmbiguousSubtasks() {
            return mayHaveAmbiguousSubtasks;
        }
    }
}
