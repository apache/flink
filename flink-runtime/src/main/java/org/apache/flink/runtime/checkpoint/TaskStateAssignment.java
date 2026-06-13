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
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionDistributor;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.MergedInputChannelStateHandle;
import org.apache.flink.runtime.state.MergedResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.CollectionUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static org.apache.flink.runtime.state.ChannelStateHelper.castToInputStateCollection;
import static org.apache.flink.runtime.state.ChannelStateHelper.castToOutputStateCollection;
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
    final int newParallelism;
    final OperatorID inputOperatorID;
    final OperatorID outputOperatorID;

    /** The InputGate set that containing input buffer state. */
    private final Set<Integer> inputStateGates;

    /** The ResultPartition set that containing input buffer state. */
    private final Set<Integer> outputStatePartitions;

    final Map<OperatorInstanceID, List<OperatorStateHandle>> subManagedOperatorState;
    final Map<OperatorInstanceID, List<OperatorStateHandle>> subRawOperatorState;
    final Map<OperatorInstanceID, List<KeyedStateHandle>> subManagedKeyedState;
    final Map<OperatorInstanceID, List<KeyedStateHandle>> subRawKeyedState;

    final Map<OperatorInstanceID, List<InputChannelStateHandle>> inputChannelStates;
    final Map<OperatorInstanceID, List<ResultSubpartitionStateHandle>> resultSubpartitionStates;

    /**
     * Stores input channel states that come from upstream task's output buffers. It takes effect
     * when {@link
     * org.apache.flink.configuration.CheckpointingOptions#UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM}
     * is enabled.
     */
    private final Map<OperatorInstanceID, List<InputChannelStateHandle>> upstreamOutputBufferStates;

    /** The subtask mapping when the output operator was rescaled. */
    private final Map<Integer, SubtasksRescaleMapping> outputSubtaskMappings = new HashMap<>();

    /** The subtask mapping when the input operator was rescaled. */
    private final Map<Integer, SubtasksRescaleMapping> inputSubtaskMappings = new HashMap<>();

    /** InflightDataRescalingDescriptor for each subtask. */
    private final Map<OperatorInstanceID, InflightDataRescalingDescriptor>
            outputRescalingDescriptors = new HashMap<>();

    private final boolean recoverOutputOnDownstreamTask;
    @Nullable private final EdgeDistributionPatternSnapshot oldEdgePatterns;

    @Nullable private TaskStateAssignment[] downstreamAssignments;
    @Nullable private TaskStateAssignment[] upstreamAssignments;
    @Nullable private Boolean hasUpstreamOutputStates;
    @Nullable private Boolean hasDownstreamInputStates;

    private final Map<IntermediateDataSetID, TaskStateAssignment> consumerAssignment;
    private final Map<ExecutionJobVertex, TaskStateAssignment> vertexAssignments;

    public TaskStateAssignment(
            ExecutionJobVertex executionJobVertex,
            Map<OperatorID, OperatorState> oldState,
            Map<IntermediateDataSetID, TaskStateAssignment> consumerAssignment,
            Map<ExecutionJobVertex, TaskStateAssignment> vertexAssignments,
            boolean recoverOutputOnDownstreamTask) {
        this(
                executionJobVertex,
                oldState,
                consumerAssignment,
                vertexAssignments,
                recoverOutputOnDownstreamTask,
                null);
    }

    public TaskStateAssignment(
            ExecutionJobVertex executionJobVertex,
            Map<OperatorID, OperatorState> oldState,
            Map<IntermediateDataSetID, TaskStateAssignment> consumerAssignment,
            Map<ExecutionJobVertex, TaskStateAssignment> vertexAssignments,
            boolean recoverOutputOnDownstreamTask,
            @Nullable EdgeDistributionPatternSnapshot oldEdgePatterns) {

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
        this.recoverOutputOnDownstreamTask = recoverOutputOnDownstreamTask;
        this.oldEdgePatterns = oldEdgePatterns;
        final int expectedNumberOfSubtasks = newParallelism * oldState.size();

        subManagedOperatorState =
                CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        subRawOperatorState = CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        inputChannelStates = CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        resultSubpartitionStates =
                CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        upstreamOutputBufferStates =
                CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        subManagedKeyedState = CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        subRawKeyedState = CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);

        final List<OperatorIDPair> operatorIDs = executionJobVertex.getOperatorIDs();
        outputOperatorID = operatorIDs.get(0).getGeneratedOperatorID();
        inputOperatorID = operatorIDs.get(operatorIDs.size() - 1).getGeneratedOperatorID();

        inputStateGates = extractInputStateGates(oldState.get(inputOperatorID));
        outputStatePartitions = extractOutputStatePartitions(oldState.get(outputOperatorID));
    }

    private static Set<Integer> extractInputStateGates(OperatorState operatorState) {
        return operatorState.getStates().stream()
                .map(OperatorSubtaskState::getInputChannelState)
                .flatMap(Collection::stream)
                .flatMapToInt(
                        handle -> {
                            if (handle instanceof InputChannelStateHandle) {
                                return IntStream.of(
                                        ((InputChannelStateHandle) handle).getInfo().getGateIdx());
                            } else if (handle instanceof MergedInputChannelStateHandle) {
                                return ((MergedInputChannelStateHandle) handle)
                                        .getInfos().stream().mapToInt(InputChannelInfo::getGateIdx);
                            } else {
                                throw new IllegalStateException(
                                        "Invalid input channel state : " + handle.getClass());
                            }
                        })
                .distinct()
                .boxed()
                .collect(Collectors.toSet());
    }

    private static Set<Integer> extractOutputStatePartitions(OperatorState operatorState) {
        return operatorState.getStates().stream()
                .map(OperatorSubtaskState::getResultSubpartitionState)
                .flatMap(Collection::stream)
                .flatMapToInt(
                        handle -> {
                            if (handle instanceof ResultSubpartitionStateHandle) {
                                return IntStream.of(
                                        ((ResultSubpartitionStateHandle) handle)
                                                .getInfo()
                                                .getPartitionIdx());
                            } else if (handle instanceof MergedResultSubpartitionStateHandle) {
                                return ((MergedResultSubpartitionStateHandle) handle)
                                        .getInfos().stream()
                                                .mapToInt(ResultSubpartitionInfo::getPartitionIdx);
                            } else {
                                throw new IllegalStateException(
                                        "Invalid output channel state : " + handle.getClass());
                            }
                        })
                .distinct()
                .boxed()
                .collect(Collectors.toSet());
    }

    public boolean hasInputState() {
        return !inputStateGates.isEmpty();
    }

    public boolean hasOutputState() {
        return !outputStatePartitions.isEmpty();
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

        return OperatorSubtaskState.builder()
                .setManagedOperatorState(getState(instanceID, subManagedOperatorState))
                .setRawOperatorState(getState(instanceID, subRawOperatorState))
                .setManagedKeyedState(getState(instanceID, subManagedKeyedState))
                .setRawKeyedState(getState(instanceID, subRawKeyedState))
                .setInputChannelState(
                        castToInputStateCollection(inputChannelStates.get(instanceID)))
                .setUpstreamOutputBufferState(
                        new StateObjectCollection<>(upstreamOutputBufferStates.get(instanceID)))
                .setResultSubpartitionState(
                        // If recoverOutputOnDownstreamTask is enabled, clear own output buffers as
                        // they are migrated to downstream
                        recoverOutputOnDownstreamTask
                                ? castToOutputStateCollection(null)
                                : castToOutputStateCollection(
                                        resultSubpartitionStates.get(instanceID)))
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
                                this::getInputMapping,
                                true))
                .setOutputRescalingDescriptor(getOutputRescalingDescriptor(instanceID))
                .build();
    }

    public boolean hasUpstreamOutputStates() {
        if (hasUpstreamOutputStates == null) {
            hasUpstreamOutputStates =
                    Arrays.stream(getUpstreamAssignments())
                            .anyMatch(TaskStateAssignment::hasOutputState);
        }
        return hasUpstreamOutputStates;
    }

    public boolean hasDownstreamInputStates() {
        if (hasDownstreamInputStates == null) {
            hasDownstreamInputStates =
                    Arrays.stream(getDownstreamAssignments())
                            .anyMatch(TaskStateAssignment::hasInputState);
        }
        return hasDownstreamInputStates;
    }

    /**
     * Gets the output rescaling descriptor for a specific instance with caching. Each descriptor is
     * computed only once and cached for subsequent access.
     */
    public InflightDataRescalingDescriptor getOutputRescalingDescriptor(
            OperatorInstanceID instanceID) {
        return outputRescalingDescriptors.computeIfAbsent(
                instanceID, this::computeOutputRescalingDescriptor);
    }

    /** Computes the output rescaling descriptor for a single subtask. */
    private InflightDataRescalingDescriptor computeOutputRescalingDescriptor(
            OperatorInstanceID instanceID) {
        return createRescalingDescriptor(
                instanceID,
                outputOperatorID,
                getDownstreamAssignments(),
                (downstreamAssignment, recompute) -> {
                    int assignmentIndex =
                            getAssignmentIndex(downstreamAssignment.getUpstreamAssignments(), this);
                    return downstreamAssignment.getInputMapping(assignmentIndex, recompute);
                },
                outputSubtaskMappings,
                this::getOutputMapping,
                false);
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
            Function<Integer, SubtasksRescaleMapping> subtaskMappingCalculator,
            boolean isInput) {
        if (!expectedOperatorID.equals(instanceID.getOperatorId())) {
            return InflightDataRescalingDescriptor.NO_RESCALE;
        }

        SubtasksRescaleMapping[] rescaledChannelsMappings =
                Arrays.stream(connectedAssignments)
                        .map(assignment -> mappingRetriever.apply(assignment, false))
                        .toArray(SubtasksRescaleMapping[]::new);

        // PW edges: pattern change alters subpartition coverage even at same parallelism
        if (subtaskGateOrPartitionMappings.isEmpty()
                && Arrays.stream(rescaledChannelsMappings).allMatch(Objects::isNull)
                && !hasPointwiseEdge(connectedAssignments, isInput)) {
            return InflightDataRescalingDescriptor.NO_RESCALE;
        }

        InflightDataGateOrPartitionRescalingDescriptor[] gateOrPartitionDescriptors =
                createGateOrPartitionRescalingDescriptors(
                        instanceID,
                        connectedAssignments,
                        assignment -> mappingRetriever.apply(assignment, true),
                        subtaskGateOrPartitionMappings,
                        subtaskMappingCalculator,
                        rescaledChannelsMappings,
                        isInput);

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
                    SubtasksRescaleMapping[] rescaledChannelsMappings,
                    boolean isInput) {
        return IntStream.range(0, rescaledChannelsMappings.length)
                .mapToObj(
                        partition -> {
                            if (!hasInFlightData(isInput, partition)) {
                                return InflightDataGateOrPartitionRescalingDescriptor.NO_STATE;
                            }
                            TaskStateAssignment connectedAssignment =
                                    connectedAssignments[partition];

                            DistributionPattern oldPattern =
                                    resolveOldDistributionPattern(
                                            isInput, partition, connectedAssignment);
                            DistributionPattern newPattern =
                                    resolveNewDistributionPattern(isInput, partition);

                            if (oldEdgePatterns != null
                                    && (oldPattern == DistributionPattern.POINTWISE
                                            || newPattern == DistributionPattern.POINTWISE)) {
                                return createPointwiseRescalingDescriptor(
                                        instanceID,
                                        partition,
                                        isInput,
                                        connectedAssignment,
                                        oldPattern,
                                        newPattern);
                            }

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

    private boolean hasInFlightData(boolean isInput, int gateOrPartitionIndex) {
        if (isInput) {
            return hasInFlightDataForInputGate(gateOrPartitionIndex);
        } else {
            return hasInFlightDataForResultPartition(gateOrPartitionIndex);
        }
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

        final RescaleMappings mapping;
        if (mapper == SubtaskStateMapper.POINTWISE_UPSTREAM) {
            PointwiseRescaleParams params =
                    buildOutputPointwiseRescaleParams(partitionIndex, downstreamAssignment);
            int oldParallelism = oldState.get(outputOperatorID).getParallelism();
            boolean isIdentity =
                    oldParallelism == newParallelism
                            && params.getOldUpParallelism() == params.getNewUpParallelism()
                            && params.getOldDownParallelism() == params.getNewDownParallelism()
                            && params.getOldDistributionPattern()
                                    == params.getNewDistributionPattern();
            if (isIdentity) {
                mapping =
                        RescaleMappings.of(
                                IntStream.range(0, newParallelism).mapToObj(idx -> new int[] {idx}),
                                oldParallelism);
            } else {
                mapping =
                        RescaleMappings.of(
                                IntStream.range(0, newParallelism)
                                        .mapToObj(
                                                idx ->
                                                        PointwiseChannelMappingUtils
                                                                .computeInheritedOldUpstreams(
                                                                        idx, params)),
                                oldParallelism);
            }
        } else {
            mapping =
                    mapper.getNewToOldSubtasksMapping(
                            oldState.get(outputOperatorID).getParallelism(), newParallelism);
        }
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

    public boolean hasInFlightDataForInputGate(int gateIndex) {
        // Check own input state for this gate
        if (inputStateGates.contains(gateIndex)) {
            return true;
        }

        // Check upstream output state for this gate
        TaskStateAssignment upstreamAssignment = getUpstreamAssignments()[gateIndex];
        if (upstreamAssignment != null && upstreamAssignment.hasOutputState()) {
            IntermediateResult inputResult = executionJobVertex.getInputs().get(gateIndex);
            IntermediateDataSetID resultId = inputResult.getId();
            IntermediateResult[] producedDataSets = inputResult.getProducer().getProducedDataSets();
            for (int i = 0; i < producedDataSets.length; i++) {
                if (producedDataSets[i].getId().equals(resultId)) {
                    return upstreamAssignment.outputStatePartitions.contains(i);
                }
            }
        }

        return false;
    }

    public boolean hasInFlightDataForResultPartition(int partitionIndex) {
        // Check own output state for this partition
        if (outputStatePartitions.contains(partitionIndex)) {
            return true;
        }

        // Check downstream input state for this partition
        TaskStateAssignment downstreamAssignment = getDownstreamAssignments()[partitionIndex];

        if (downstreamAssignment != null && downstreamAssignment.hasInputState()) {
            IntermediateResult producedResult =
                    executionJobVertex.getProducedDataSets()[partitionIndex];
            IntermediateDataSetID resultId = producedResult.getId();
            List<IntermediateResult> inputs = downstreamAssignment.executionJobVertex.getInputs();
            for (int i = 0; i < inputs.size(); i++) {
                if (inputs.get(i).getId().equals(resultId)) {
                    return downstreamAssignment.inputStateGates.contains(i);
                }
            }
        }
        return false;
    }

    boolean hasPointwiseEdge(TaskStateAssignment[] connectedAssignments, boolean isInput) {
        if (oldEdgePatterns == null) {
            return false;
        }
        for (int i = 0; i < connectedAssignments.length; i++) {
            if (!hasInFlightData(isInput, i)) {
                continue;
            }
            DistributionPattern oldPattern =
                    resolveOldDistributionPattern(isInput, i, connectedAssignments[i]);
            DistributionPattern newPattern = resolveNewDistributionPattern(isInput, i);
            if (oldPattern == DistributionPattern.POINTWISE
                    || newPattern == DistributionPattern.POINTWISE) {
                return true;
            }
        }
        return false;
    }

    private DistributionPattern resolveOldDistributionPattern(
            boolean isInput, int gateOrPartitionIdx, TaskStateAssignment connectedAssignment) {
        if (oldEdgePatterns == null) {
            return DistributionPattern.ALL_TO_ALL;
        }
        if (isInput) {
            int partitionIdxInUpstream =
                    Arrays.asList(connectedAssignment.getDownstreamAssignments()).indexOf(this);
            if (partitionIdxInUpstream < 0) {
                return DistributionPattern.ALL_TO_ALL;
            }
            DistributionPattern pattern =
                    oldEdgePatterns.getOutputPattern(
                            connectedAssignment.outputOperatorID, partitionIdxInUpstream);
            return pattern != null ? pattern : DistributionPattern.ALL_TO_ALL;
        } else {
            DistributionPattern pattern =
                    oldEdgePatterns.getOutputPattern(outputOperatorID, gateOrPartitionIdx);
            return pattern != null ? pattern : DistributionPattern.ALL_TO_ALL;
        }
    }

    private DistributionPattern resolveNewDistributionPattern(
            boolean isInput, int gateOrPartitionIdx) {
        if (isInput) {
            return executionJobVertex
                    .getInputs()
                    .get(gateOrPartitionIdx)
                    .getConsumingDistributionPattern();
        } else {
            return executionJobVertex.getProducedDataSets()[gateOrPartitionIdx]
                    .getConsumingDistributionPattern();
        }
    }

    private InflightDataGateOrPartitionRescalingDescriptor createPointwiseRescalingDescriptor(
            OperatorInstanceID instanceID,
            int partition,
            boolean isInput,
            TaskStateAssignment connectedAssignment,
            DistributionPattern oldPattern,
            DistributionPattern newPattern) {
        final int oldUpParallelism;
        final int oldDownParallelism;
        final int newUpParallelism;
        final int newDownParallelism;

        if (isInput) {
            oldUpParallelism =
                    connectedAssignment
                            .oldState
                            .get(connectedAssignment.outputOperatorID)
                            .getParallelism();
            oldDownParallelism = oldState.get(inputOperatorID).getParallelism();
            newUpParallelism = connectedAssignment.newParallelism;
            newDownParallelism = newParallelism;
        } else {
            oldUpParallelism = oldState.get(outputOperatorID).getParallelism();
            oldDownParallelism =
                    connectedAssignment
                            .oldState
                            .get(connectedAssignment.inputOperatorID)
                            .getParallelism();
            newUpParallelism = newParallelism;
            newDownParallelism = connectedAssignment.newParallelism;
        }

        PointwiseRescaleParams params =
                new PointwiseRescaleParams(
                        oldPattern,
                        newPattern,
                        oldUpParallelism,
                        oldDownParallelism,
                        newUpParallelism,
                        newDownParallelism);

        int[] oldSubtaskInstances =
                computePointwiseOldSubtaskInstances(instanceID.getSubtaskId(), isInput, params);

        // identity: no old state to recover, or both parallelism + pattern unchanged (in-place)
        boolean isIdentity =
                oldSubtaskInstances.length == 0
                        || (oldUpParallelism == newUpParallelism
                                && oldDownParallelism == newDownParallelism
                                && oldPattern == newPattern);

        // PW edges: channel mapping and ambiguous are unused on TM (TM re-routes via params)
        return log(
                new InflightDataGateOrPartitionRescalingDescriptor(
                        oldSubtaskInstances,
                        RescaleMappings.SYMMETRIC_IDENTITY,
                        emptySet(),
                        isIdentity ? MappingType.IDENTITY : MappingType.RESCALING,
                        params),
                instanceID.getSubtaskId(),
                partition);
    }

    private static int[] computePointwiseOldSubtaskInstances(
            int newSubtaskIdx, boolean isInput, PointwiseRescaleParams params) {
        if (isInput) {
            // input: ROUND_ROBIN exact mapping, new downstream inherits a set of old downstreams
            return SubtaskStateMapper.ROUND_ROBIN.getOldSubtasks(
                    newSubtaskIdx, params.getOldDownParallelism(), params.getNewDownParallelism());
        } else {
            // output: computeInheritedOldUpstreams traces historical upstreams, may overlap
            return PointwiseChannelMappingUtils.computeInheritedOldUpstreams(newSubtaskIdx, params);
        }
    }

    private PointwiseRescaleParams buildOutputPointwiseRescaleParams(
            int partitionIndex, TaskStateAssignment downstreamAssignment) {
        DistributionPattern oldPattern =
                resolveOldDistributionPattern(false, partitionIndex, downstreamAssignment);
        DistributionPattern newPattern = resolveNewDistributionPattern(false, partitionIndex);
        int oldUpParallelism = oldState.get(outputOperatorID).getParallelism();
        int oldDownParallelism =
                downstreamAssignment
                        .oldState
                        .get(downstreamAssignment.inputOperatorID)
                        .getParallelism();
        return new PointwiseRescaleParams(
                oldPattern,
                newPattern,
                oldUpParallelism,
                oldDownParallelism,
                newParallelism,
                downstreamAssignment.newParallelism);
    }

    void distributeOutputBuffersToDownstream() {
        for (Map.Entry<OperatorInstanceID, List<ResultSubpartitionStateHandle>> entry :
                resultSubpartitionStates.entrySet()) {
            OperatorInstanceID operatorInstanceID = entry.getKey();
            int newUpstreamSubtaskIndex = operatorInstanceID.getSubtaskId();
            List<ResultSubpartitionStateHandle> stateHandles = entry.getValue();

            ResultSubpartitionDistributor distributor =
                    new ResultSubpartitionDistributor(
                            getOutputRescalingDescriptor(operatorInstanceID));

            for (final ResultSubpartitionStateHandle stateHandle : stateHandles) {
                ResultSubpartitionInfo info = stateHandle.getInfo();
                int partitionIdx = info.getPartitionIdx();
                TaskStateAssignment downstreamAssignment = getDownstreamAssignments()[partitionIdx];

                DistributionPattern oldPattern =
                        resolveOldDistributionPattern(false, partitionIdx, downstreamAssignment);
                DistributionPattern newPattern = resolveNewDistributionPattern(false, partitionIdx);
                if (oldEdgePatterns != null
                        && (oldPattern == DistributionPattern.POINTWISE
                                || newPattern == DistributionPattern.POINTWISE)) {
                    // PW edge: subpartition index ≠ global subtask ID, topology-aware routing
                    // needed
                    distributePointwiseOutputBufferToDownstream(
                            stateHandle,
                            partitionIdx,
                            downstreamAssignment,
                            newUpstreamSubtaskIndex);
                } else {
                    // A2A edge: local index == global subtask ID, original distributor suffices
                    distributeA2AOutputBufferToDownstream(
                            stateHandle, distributor, partitionIdx, downstreamAssignment);
                }
            }
        }
    }

    private void distributeA2AOutputBufferToDownstream(
            ResultSubpartitionStateHandle stateHandle,
            ResultSubpartitionDistributor distributor,
            int partitionIdx,
            TaskStateAssignment downstreamAssignment) {
        ResultSubpartitionInfo info = stateHandle.getInfo();
        // A2A path: oldUpstreamSubtaskIndex is the inputChannelIdx, and
        // info.getSubPartitionIdx() is the oldDownstreamSubtaskIndex.
        int oldUpstreamSubtaskIndex = stateHandle.getSubtaskIndex();
        int oldDownstreamSubtaskIndex = info.getSubPartitionIdx();

        int gateIdxResultPartition = findInputGateIdxForResultPartition(partitionIdx);

        List<ResultSubpartitionInfo> mappedSubpartitions = distributor.getMappedSubpartitions(info);
        for (final ResultSubpartitionInfo mappedSubpartition : mappedSubpartitions) {
            int targetDownstreamSubtaskId = mappedSubpartition.getSubPartitionIdx();

            OperatorInstanceID downstreamOperatorInstance =
                    new OperatorInstanceID(
                            targetDownstreamSubtaskId, downstreamAssignment.inputOperatorID);

            InputChannelInfo inputChannelInfo =
                    new InputChannelInfo(gateIdxResultPartition, oldUpstreamSubtaskIndex);

            InputChannelStateHandle upstreamOutputBufferHandle =
                    new InputChannelStateHandle(
                            oldDownstreamSubtaskIndex,
                            inputChannelInfo,
                            stateHandle.getDelegate(),
                            stateHandle.getOffsets(),
                            stateHandle.getStateSize());

            List<InputChannelStateHandle> upstreamOutputBufferHandles =
                    downstreamAssignment.upstreamOutputBufferStates.computeIfAbsent(
                            downstreamOperatorInstance, k -> new ArrayList<>());
            upstreamOutputBufferHandles.add(upstreamOutputBufferHandle);
        }
    }

    private void distributePointwiseOutputBufferToDownstream(
            ResultSubpartitionStateHandle stateHandle,
            int partitionIdx,
            TaskStateAssignment downstreamAssignment,
            int newUpstreamSubtaskIndex) {
        int oldUpstreamSubtaskIndex = stateHandle.getSubtaskIndex();
        ResultSubpartitionInfo info = stateHandle.getInfo();
        int subPartitionIdx = info.getSubPartitionIdx();

        PointwiseRescaleParams params =
                buildOutputPointwiseRescaleParams(partitionIdx, downstreamAssignment);

        int oldUpPar = params.getOldUpParallelism();
        int oldDownPar = params.getOldDownParallelism();
        int newDownPar = params.getNewDownParallelism();

        DistributionPattern oldPattern = params.getOldDistributionPattern();

        int globalOldDownstream =
                PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                        oldUpstreamSubtaskIndex,
                        subPartitionIdx,
                        oldUpPar,
                        oldDownPar,
                        false,
                        oldPattern);

        int targetDownstreamSubtaskId =
                PointwiseChannelMappingUtils.newSubtaskAssignedFrom(
                        globalOldDownstream, newDownPar);

        if (!getOutputMapping(partitionIdx).rescaleMappings.isIdentity()) {
            // identity: computeInheritedOldUpstreams(N)={N}, each upstream recovers itself only;
            // non-identity: multiple upstreams may trace the same old upstream, only primary
            // recovers
            int upstreamSubpartitionIfResponsive =
                    PointwiseChannelMappingUtils.computeNewLocalSubpartitionIndex(
                            targetDownstreamSubtaskId, newUpstreamSubtaskIndex, params);
            if (upstreamSubpartitionIfResponsive < 0) {
                return;
            }
        }

        int oldDownstreamChannelIndex =
                PointwiseChannelMappingUtils.globalSubtaskIndexToLocalIndex(
                        globalOldDownstream,
                        oldUpstreamSubtaskIndex,
                        oldUpPar,
                        oldDownPar,
                        true,
                        oldPattern);
        checkState(
                oldDownstreamChannelIndex >= 0,
                "Failed to find a subtask channel for "
                        + stateHandle
                        + " when distributing output buffers for "
                        + params);

        int gateIdxResultPartition = findInputGateIdxForResultPartition(partitionIdx);

        OperatorInstanceID downstreamOperatorInstance =
                new OperatorInstanceID(
                        targetDownstreamSubtaskId, downstreamAssignment.inputOperatorID);

        InputChannelInfo inputChannelInfo =
                new InputChannelInfo(gateIdxResultPartition, oldDownstreamChannelIndex);

        InputChannelStateHandle upstreamOutputBufferHandle =
                new InputChannelStateHandle(
                        globalOldDownstream,
                        inputChannelInfo,
                        stateHandle.getDelegate(),
                        stateHandle.getOffsets(),
                        stateHandle.getStateSize());

        List<InputChannelStateHandle> upstreamOutputBufferHandles =
                downstreamAssignment.upstreamOutputBufferStates.computeIfAbsent(
                        downstreamOperatorInstance, k -> new ArrayList<>());
        upstreamOutputBufferHandles.add(upstreamOutputBufferHandle);
    }

    private int findInputGateIdxForResultPartition(int partitionIndex) {
        // Check downstream input state for this partition
        TaskStateAssignment downstreamAssignment = getDownstreamAssignments()[partitionIndex];

        IntermediateResult producedResult =
                executionJobVertex.getProducedDataSets()[partitionIndex];
        IntermediateDataSetID resultId = producedResult.getId();
        List<IntermediateResult> inputs = downstreamAssignment.executionJobVertex.getInputs();
        for (int i = 0; i < inputs.size(); i++) {
            if (inputs.get(i).getId().equals(resultId)) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                "No channel rescaler found during rescaling of channel state");
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
