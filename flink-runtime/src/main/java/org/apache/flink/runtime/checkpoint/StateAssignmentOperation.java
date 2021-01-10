/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class encapsulates the operation of assigning restored state when restoring from a
 * checkpoint.
 */
@Internal
public class StateAssignmentOperation {

    private static final Logger LOG = LoggerFactory.getLogger(StateAssignmentOperation.class);

    private final Set<ExecutionJobVertex> tasks;
    private final Map<OperatorID, OperatorState> operatorStates;

    private final long restoreCheckpointId;
    private final boolean allowNonRestoredState;

    /** The state assignments for each ExecutionJobVertex that will be filled in multiple passes. */
    private final Map<ExecutionJobVertex, TaskStateAssignment> vertexAssignments;
    /**
     * Stores the assignment of a consumer. {@link IntermediateResult} only allows to traverse
     * producer.
     */
    private final Map<IntermediateDataSetID, TaskStateAssignment> consumerAssignment =
            new HashMap<>();

    public StateAssignmentOperation(
            long restoreCheckpointId,
            Set<ExecutionJobVertex> tasks,
            Map<OperatorID, OperatorState> operatorStates,
            boolean allowNonRestoredState) {

        this.restoreCheckpointId = restoreCheckpointId;
        this.tasks = Preconditions.checkNotNull(tasks);
        this.operatorStates = Preconditions.checkNotNull(operatorStates);
        this.allowNonRestoredState = allowNonRestoredState;
        vertexAssignments = new HashMap<>(tasks.size());
    }

    public void assignStates() {
        Map<OperatorID, OperatorState> localOperators = new HashMap<>(operatorStates);

        checkStateMappingCompleteness(allowNonRestoredState, operatorStates, tasks);

        // find the states of all operators belonging to this task and compute additional
        // information in first pass
        for (ExecutionJobVertex executionJobVertex : this.tasks) {
            List<OperatorIDPair> operatorIDPairs = executionJobVertex.getOperatorIDs();
            Map<OperatorID, OperatorState> operatorStates = new HashMap<>(operatorIDPairs.size());
            for (OperatorIDPair operatorIDPair : operatorIDPairs) {
                OperatorID operatorID =
                        operatorIDPair
                                .getUserDefinedOperatorID()
                                .orElse(operatorIDPair.getGeneratedOperatorID());

                OperatorState operatorState = localOperators.remove(operatorID);
                if (operatorState == null) {
                    operatorState =
                            new OperatorState(
                                    operatorID,
                                    executionJobVertex.getParallelism(),
                                    executionJobVertex.getMaxParallelism());
                }
                operatorStates.put(operatorIDPair.getGeneratedOperatorID(), operatorState);
            }

            final TaskStateAssignment stateAssignment =
                    new TaskStateAssignment(executionJobVertex, operatorStates);
            vertexAssignments.put(executionJobVertex, stateAssignment);
            for (final IntermediateResult producedDataSet : executionJobVertex.getInputs()) {
                consumerAssignment.put(producedDataSet.getId(), stateAssignment);
            }
        }

        // repartition state
        for (TaskStateAssignment stateAssignment : vertexAssignments.values()) {
            if (stateAssignment.hasState) {
                assignAttemptState(stateAssignment);
            }
        }

        // actually assign the state
        for (TaskStateAssignment stateAssignment : vertexAssignments.values()) {
            if (stateAssignment.hasState) {
                assignTaskStateToExecutionJobVertices(stateAssignment);
            }
        }
    }

    private void assignAttemptState(TaskStateAssignment taskStateAssignment) {

        // 1. first compute the new parallelism
        checkParallelismPreconditions(taskStateAssignment);

        List<KeyGroupRange> keyGroupPartitions =
                createKeyGroupPartitions(
                        taskStateAssignment.executionJobVertex.getMaxParallelism(),
                        taskStateAssignment.newParallelism);

        /*
         * Redistribute ManagedOperatorStates and RawOperatorStates from old parallelism to new parallelism.
         *
         * The old ManagedOperatorStates with old parallelism 3:
         *
         * 		parallelism0 parallelism1 parallelism2
         * op0   states0,0    state0,1	   state0,2
         * op1
         * op2   states2,0    state2,1	   state1,2
         * op3   states3,0    state3,1     state3,2
         *
         * The new ManagedOperatorStates with new parallelism 4:
         *
         * 		parallelism0 parallelism1 parallelism2 parallelism3
         * op0   state0,0	  state0,1 	   state0,2		state0,3
         * op1
         * op2   state2,0	  state2,1 	   state2,2		state2,3
         * op3   state3,0	  state3,1 	   state3,2		state3,3
         */
        reDistributePartitionableStates(
                taskStateAssignment.oldState,
                taskStateAssignment.newParallelism,
                OperatorSubtaskState::getManagedOperatorState,
                RoundRobinOperatorStateRepartitioner.INSTANCE,
                taskStateAssignment.subManagedOperatorState);
        reDistributePartitionableStates(
                taskStateAssignment.oldState,
                taskStateAssignment.newParallelism,
                OperatorSubtaskState::getRawOperatorState,
                RoundRobinOperatorStateRepartitioner.INSTANCE,
                taskStateAssignment.subRawOperatorState);

        reDistributeInputChannelStates(taskStateAssignment);
        reDistributeResultSubpartitionStates(taskStateAssignment);

        reDistributeKeyedStates(keyGroupPartitions, taskStateAssignment);
    }

    private void assignTaskStateToExecutionJobVertices(TaskStateAssignment assignment) {
        ExecutionJobVertex executionJobVertex = assignment.executionJobVertex;

        List<OperatorIDPair> operatorIDs = executionJobVertex.getOperatorIDs();
        final int newParallelism = executionJobVertex.getParallelism();

        /*
         *  An executionJobVertex's all state handles needed to restore are something like a matrix
         *
         * 		parallelism0 parallelism1 parallelism2 parallelism3
         * op0   sh(0,0)     sh(0,1)       sh(0,2)	    sh(0,3)
         * op1   sh(1,0)	 sh(1,1)	   sh(1,2)	    sh(1,3)
         * op2   sh(2,0)	 sh(2,1)	   sh(2,2)		sh(2,3)
         * op3   sh(3,0)	 sh(3,1)	   sh(3,2)		sh(3,3)
         *
         */
        for (int subTaskIndex = 0; subTaskIndex < newParallelism; subTaskIndex++) {

            Execution currentExecutionAttempt =
                    executionJobVertex.getTaskVertices()[subTaskIndex].getCurrentExecutionAttempt();

            TaskStateSnapshot taskState = new TaskStateSnapshot(operatorIDs.size());
            boolean statelessTask = true;

            for (OperatorIDPair operatorID : operatorIDs) {
                OperatorInstanceID instanceID =
                        OperatorInstanceID.of(subTaskIndex, operatorID.getGeneratedOperatorID());

                OperatorSubtaskState operatorSubtaskState =
                        operatorSubtaskStateFrom(instanceID, assignment);

                if (operatorSubtaskState.hasState()) {
                    statelessTask = false;
                }
                taskState.putSubtaskStateByOperatorID(
                        operatorID.getGeneratedOperatorID(), operatorSubtaskState);
            }

            if (!statelessTask) {
                JobManagerTaskRestore taskRestore =
                        new JobManagerTaskRestore(restoreCheckpointId, taskState);
                currentExecutionAttempt.setInitialState(taskRestore);
            }
        }
    }

    public static OperatorSubtaskState operatorSubtaskStateFrom(
            OperatorInstanceID instanceID, TaskStateAssignment assignment) {

        return assignment.getSubtaskState(instanceID);
    }

    public void checkParallelismPreconditions(TaskStateAssignment taskStateAssignment) {
        for (OperatorState operatorState : taskStateAssignment.oldState.values()) {
            checkParallelismPreconditions(operatorState, taskStateAssignment.executionJobVertex);
        }
    }

    private void reDistributeKeyedStates(
            List<KeyGroupRange> keyGroupPartitions, TaskStateAssignment stateAssignment) {
        stateAssignment.oldState.forEach(
                (operatorID, operatorState) -> {
                    for (int subTaskIndex = 0;
                            subTaskIndex < stateAssignment.newParallelism;
                            subTaskIndex++) {
                        OperatorInstanceID instanceID =
                                OperatorInstanceID.of(subTaskIndex, operatorID);
                        Tuple2<List<KeyedStateHandle>, List<KeyedStateHandle>> subKeyedStates =
                                reAssignSubKeyedStates(
                                        operatorState,
                                        keyGroupPartitions,
                                        subTaskIndex,
                                        stateAssignment.newParallelism,
                                        operatorState.getParallelism());
                        stateAssignment.subManagedKeyedState.put(instanceID, subKeyedStates.f0);
                        stateAssignment.subRawKeyedState.put(instanceID, subKeyedStates.f1);
                    }
                });
    }

    // TODO rewrite based on operator id
    private Tuple2<List<KeyedStateHandle>, List<KeyedStateHandle>> reAssignSubKeyedStates(
            OperatorState operatorState,
            List<KeyGroupRange> keyGroupPartitions,
            int subTaskIndex,
            int newParallelism,
            int oldParallelism) {

        List<KeyedStateHandle> subManagedKeyedState;
        List<KeyedStateHandle> subRawKeyedState;

        if (newParallelism == oldParallelism) {
            if (operatorState.getState(subTaskIndex) != null) {
                subManagedKeyedState =
                        operatorState.getState(subTaskIndex).getManagedKeyedState().asList();
                subRawKeyedState = operatorState.getState(subTaskIndex).getRawKeyedState().asList();
            } else {
                subManagedKeyedState = emptyList();
                subRawKeyedState = emptyList();
            }
        } else {
            subManagedKeyedState =
                    getManagedKeyedStateHandles(
                            operatorState, keyGroupPartitions.get(subTaskIndex));
            subRawKeyedState =
                    getRawKeyedStateHandles(operatorState, keyGroupPartitions.get(subTaskIndex));
        }

        if (subManagedKeyedState.isEmpty() && subRawKeyedState.isEmpty()) {
            return new Tuple2<>(emptyList(), emptyList());
        } else {
            return new Tuple2<>(subManagedKeyedState, subRawKeyedState);
        }
    }

    public static <T extends StateObject> void reDistributePartitionableStates(
            Map<OperatorID, OperatorState> oldOperatorStates,
            int newParallelism,
            Function<OperatorSubtaskState, StateObjectCollection<T>> extractHandle,
            OperatorStateRepartitioner<T> stateRepartitioner,
            Map<OperatorInstanceID, List<T>> result) {

        // The nested list wraps as the level of operator -> subtask -> state object collection
        Map<OperatorID, List<List<T>>> oldStates =
                splitManagedAndRawOperatorStates(oldOperatorStates, extractHandle);

        oldOperatorStates.forEach(
                (operatorID, oldOperatorState) ->
                        result.putAll(
                                applyRepartitioner(
                                        operatorID,
                                        stateRepartitioner,
                                        oldStates.get(operatorID),
                                        oldOperatorState.getParallelism(),
                                        newParallelism)));
    }

    public <I, T extends AbstractChannelStateHandle<I>> void reDistributeResultSubpartitionStates(
            TaskStateAssignment assignment) {
        final ExecutionJobVertex executionJobVertex = assignment.executionJobVertex;
        final OperatorID outputOperatorID =
                executionJobVertex.getOperatorIDs().get(0).getGeneratedOperatorID();
        final List<List<ResultSubpartitionStateHandle>> outputOperatorState =
                getChannelState(
                        assignment.oldState,
                        OperatorSubtaskState::getResultSubpartitionState,
                        outputOperatorID);
        if (outputOperatorState == null) {
            return;
        }

        final List<IntermediateDataSet> outputs =
                executionJobVertex.getJobVertex().getProducedDataSets();
        if (outputOperatorState.size() == executionJobVertex.getParallelism()) {
            assignment.resultSubpartitionStates.putAll(
                    toInstanceMap(outputOperatorID, outputOperatorState));
        } else {
            // Parallelism of this vertex changed, distribute ResultSubpartitionStateHandle
            // according to SubtaskStateMapper.
            for (int partitionIndex = 0; partitionIndex < outputs.size(); partitionIndex++) {
                final TaskStateAssignment downstreamAssignment =
                        consumerAssignment.get(
                                executionJobVertex.getProducedDataSets()[partitionIndex].getId());
                final IntermediateResult output =
                        executionJobVertex.getProducedDataSets()[partitionIndex];
                final int gateIndex =
                        downstreamAssignment.executionJobVertex.getInputs().indexOf(output);
                checkState(gateIndex >= 0, "Gate index not found for IntermediateResult");
                downstreamAssignment.upstreamAssignments.put(gateIndex, assignment);

                final SubtaskStateMapper mapper =
                        checkNotNull(
                                downstreamAssignment
                                        .executionJobVertex
                                        .getJobVertex()
                                        .getInputs()
                                        .get(gateIndex)
                                        .getUpstreamSubtaskStateMapper(),
                                "No channel rescaler found during rescaling of channel state");
                final Map<Integer, Set<Integer>> mapping =
                        mapper.getNewToOldSubtasksMapping(
                                outputOperatorState.size(), executionJobVertex.getParallelism());
                assignment.outputSubtaskMappings =
                        checkSubtaskMapping(assignment.outputSubtaskMappings, mapping);

                final List<List<ResultSubpartitionStateHandle>> partitionState =
                        outputs.size() == 1
                                ? outputOperatorState
                                : getPartitionState(
                                        outputOperatorState,
                                        ResultSubpartitionInfo::getPartitionIdx,
                                        partitionIndex);
                final MappingBasedRepartitioner<ResultSubpartitionStateHandle> repartitioner =
                        new MappingBasedRepartitioner(mapping);
                final Map<OperatorInstanceID, List<ResultSubpartitionStateHandle>> repartitioned =
                        applyRepartitioner(
                                outputOperatorID,
                                repartitioner,
                                partitionState,
                                outputOperatorState.size(),
                                executionJobVertex.getParallelism());
                addToSubtasks(assignment.resultSubpartitionStates, repartitioned);
            }
        }
    }

    private Map<Integer, Set<Integer>> checkSubtaskMapping(
            Map<Integer, Set<Integer>> oldMapping, Map<Integer, Set<Integer>> mapping) {
        if (oldMapping.isEmpty()) {
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

    public void reDistributeInputChannelStates(TaskStateAssignment stateAssignment) {
        @Nullable
        final List<List<InputChannelStateHandle>> inputOperatorState =
                getChannelState(
                        stateAssignment.oldState,
                        OperatorSubtaskState::getInputChannelState,
                        stateAssignment.inputOperatorID);
        if (inputOperatorState == null) {
            return;
        }

        final ExecutionJobVertex executionJobVertex = stateAssignment.executionJobVertex;
        final List<IntermediateResult> inputs = executionJobVertex.getInputs();
        if (inputOperatorState.size() == executionJobVertex.getParallelism()) {
            stateAssignment.inputChannelStates.putAll(
                    toInstanceMap(stateAssignment.inputOperatorID, inputOperatorState));
        } else {
            // if this subtask has a different degree of parallelism, use the partitioner to figure
            // out to which
            // which subtasks the state should be reassigned. In some cases, state is replicated to
            // multiple subtasks
            // and filtered during recovery.
            // example: if this task is downscaled from 3 to 2 and it uses a range partitioner over
            // [0;128)
            // old assignment: 0 -> [0;43); 1 -> [43;87); 2 -> [87;128)
            // new assignment: 0 -> [0;64]; 1 -> [64;128)
            // subtask 0 recovers data from old subtask 0 + 1 and subtask 1 recovers data from old
            // subtask 0 + 2
            for (int gateIndex = 0; gateIndex < inputs.size(); gateIndex++) {
                final SubtaskStateMapper mapper =
                        checkNotNull(
                                executionJobVertex
                                        .getJobVertex()
                                        .getInputs()
                                        .get(gateIndex)
                                        .getDownstreamSubtaskStateMapper(),
                                "No channel rescaler found during rescaling of channel state");
                final Map<Integer, Set<Integer>> mapping =
                        mapper.getNewToOldSubtasksMapping(
                                inputOperatorState.size(), stateAssignment.newParallelism);
                stateAssignment.inputSubtaskMappings =
                        checkSubtaskMapping(stateAssignment.inputSubtaskMappings, mapping);

                final List<List<InputChannelStateHandle>> gateState =
                        inputs.size() == 1
                                ? inputOperatorState
                                : getPartitionState(
                                        inputOperatorState,
                                        InputChannelInfo::getGateIdx,
                                        gateIndex);
                final MappingBasedRepartitioner<InputChannelStateHandle> repartitioner =
                        new MappingBasedRepartitioner(mapping);
                final Map<OperatorInstanceID, List<InputChannelStateHandle>> repartitioned =
                        applyRepartitioner(
                                stateAssignment.inputOperatorID,
                                repartitioner,
                                gateState,
                                inputOperatorState.size(),
                                stateAssignment.newParallelism);
                addToSubtasks(stateAssignment.inputChannelStates, repartitioned);

                // Setup virtual channels on upstream side to use the same mapping
                final IntermediateResult input = executionJobVertex.getInputs().get(gateIndex);
                final TaskStateAssignment upstreamAssignment =
                        vertexAssignments.get(
                                executionJobVertex.getInputs().get(gateIndex).getProducer());
                final int partitionIndex =
                        Arrays.asList(upstreamAssignment.executionJobVertex.getProducedDataSets())
                                .indexOf(input);
                checkState(partitionIndex >= 0, "Partition index not found for IntermediateResult");
                upstreamAssignment.downstreamAssignments.put(partitionIndex, stateAssignment);
            }
        }
    }

    private static <K, V> void addToSubtasks(Map<K, List<V>> target, Map<K, List<V>> toAdd) {
        toAdd.forEach(
                (key, values) ->
                        target.computeIfAbsent(key, (unused) -> new ArrayList<>(values.size()))
                                .addAll(values));
    }

    @Nullable
    private <T extends AbstractChannelStateHandle<?>> List<List<T>> getChannelState(
            Map<OperatorID, OperatorState> oldOperatorStates,
            Function<OperatorSubtaskState, StateObjectCollection<T>> extractHandle,
            OperatorID statefulOperatorID) {
        final List<OperatorID> unexpectedState =
                oldOperatorStates.entrySet().stream()
                        .filter(
                                operatorIDAndState ->
                                        !operatorIDAndState.getKey().equals(statefulOperatorID))
                        .filter(
                                operatorState ->
                                        operatorState.getValue().getSubtaskStates().values()
                                                .stream()
                                                .anyMatch(
                                                        operatorSubtaskState ->
                                                                extractHandle
                                                                        .apply(operatorSubtaskState)
                                                                        .stream()
                                                                        .anyMatch(
                                                                                state ->
                                                                                        !state.getOffsets()
                                                                                                .isEmpty())))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
        if (!unexpectedState.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot recover from unaligned checkpoint when topology changes, such that "
                            + "data exchanges with persisted data are now chained.\n"
                            + "The following operators contain channel state: "
                            + unexpectedState);
        }

        final OperatorState operatorState = oldOperatorStates.get(statefulOperatorID);
        // no channel state, nothing to recover
        if (operatorState.getSubtaskStates().values().stream()
                .allMatch(
                        operatorSubtaskState ->
                                extractHandle.apply(operatorSubtaskState).stream()
                                        .allMatch(state -> state.getOffsets().isEmpty()))) {
            return null;
        }

        return splitBySubtasks(operatorState, extractHandle);
    }

    private static <T extends AbstractChannelStateHandle<I>, I> List<List<T>> getPartitionState(
            List<List<T>> subtaskStates, Function<I, Integer> partitionExtractor, int partitionId) {
        return subtaskStates.stream()
                .map(
                        subtaskState ->
                                subtaskState.stream()
                                        .filter(
                                                state ->
                                                        partitionExtractor.apply(state.getInfo())
                                                                == partitionId)
                                        .collect(Collectors.toList()))
                .collect(Collectors.toList());
    }

    private static <T extends StateObject>
            Map<OperatorID, List<List<T>>> splitManagedAndRawOperatorStates(
                    Map<OperatorID, OperatorState> operatorStates,
                    Function<OperatorSubtaskState, StateObjectCollection<T>> extractHandle) {
        return operatorStates.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                operatorIdAndState ->
                                        splitBySubtasks(
                                                operatorIdAndState.getValue(), extractHandle)));
    }

    private static <T extends StateObject> List<List<T>> splitBySubtasks(
            OperatorState operatorState,
            Function<OperatorSubtaskState, StateObjectCollection<T>> extractHandle) {
        List<List<T>> statePerSubtask = new ArrayList<>(operatorState.getParallelism());

        for (int subTaskIndex = 0; subTaskIndex < operatorState.getParallelism(); subTaskIndex++) {
            OperatorSubtaskState subtaskState = operatorState.getState(subTaskIndex);
            statePerSubtask.add(
                    subtaskState == null
                            ? emptyList()
                            : extractHandle.apply(subtaskState).asList());
        }

        return statePerSubtask;
    }

    /**
     * Collect {@link KeyGroupsStateHandle managedKeyedStateHandles} which have intersection with
     * given {@link KeyGroupRange} from {@link TaskState operatorState}.
     *
     * @param operatorState all state handles of a operator
     * @param subtaskKeyGroupRange the KeyGroupRange of a subtask
     * @return all managedKeyedStateHandles which have intersection with given KeyGroupRange
     */
    public static List<KeyedStateHandle> getManagedKeyedStateHandles(
            OperatorState operatorState, KeyGroupRange subtaskKeyGroupRange) {

        final int parallelism = operatorState.getParallelism();

        List<KeyedStateHandle> subtaskKeyedStateHandles = null;

        for (int i = 0; i < parallelism; i++) {
            if (operatorState.getState(i) != null) {

                Collection<KeyedStateHandle> keyedStateHandles =
                        operatorState.getState(i).getManagedKeyedState();

                if (subtaskKeyedStateHandles == null) {
                    subtaskKeyedStateHandles =
                            new ArrayList<>(parallelism * keyedStateHandles.size());
                }

                extractIntersectingState(
                        keyedStateHandles, subtaskKeyGroupRange, subtaskKeyedStateHandles);
            }
        }

        return subtaskKeyedStateHandles;
    }

    /**
     * Collect {@link KeyGroupsStateHandle rawKeyedStateHandles} which have intersection with given
     * {@link KeyGroupRange} from {@link TaskState operatorState}.
     *
     * @param operatorState all state handles of a operator
     * @param subtaskKeyGroupRange the KeyGroupRange of a subtask
     * @return all rawKeyedStateHandles which have intersection with given KeyGroupRange
     */
    public static List<KeyedStateHandle> getRawKeyedStateHandles(
            OperatorState operatorState, KeyGroupRange subtaskKeyGroupRange) {

        final int parallelism = operatorState.getParallelism();

        List<KeyedStateHandle> extractedKeyedStateHandles = null;

        for (int i = 0; i < parallelism; i++) {
            if (operatorState.getState(i) != null) {

                Collection<KeyedStateHandle> rawKeyedState =
                        operatorState.getState(i).getRawKeyedState();

                if (extractedKeyedStateHandles == null) {
                    extractedKeyedStateHandles =
                            new ArrayList<>(parallelism * rawKeyedState.size());
                }

                extractIntersectingState(
                        rawKeyedState, subtaskKeyGroupRange, extractedKeyedStateHandles);
            }
        }

        return extractedKeyedStateHandles;
    }

    /**
     * Extracts certain key group ranges from the given state handles and adds them to the
     * collector.
     */
    @VisibleForTesting
    public static void extractIntersectingState(
            Collection<? extends KeyedStateHandle> originalSubtaskStateHandles,
            KeyGroupRange rangeToExtract,
            List<KeyedStateHandle> extractedStateCollector) {

        for (KeyedStateHandle keyedStateHandle : originalSubtaskStateHandles) {

            if (keyedStateHandle != null) {

                KeyedStateHandle intersectedKeyedStateHandle =
                        keyedStateHandle.getIntersection(rangeToExtract);

                if (intersectedKeyedStateHandle != null) {
                    extractedStateCollector.add(intersectedKeyedStateHandle);
                }
            }
        }
    }

    /**
     * Groups the available set of key groups into key group partitions. A key group partition is
     * the set of key groups which is assigned to the same task. Each set of the returned list
     * constitutes a key group partition.
     *
     * <p><b>IMPORTANT</b>: The assignment of key groups to partitions has to be in sync with the
     * KeyGroupStreamPartitioner.
     *
     * @param numberKeyGroups Number of available key groups (indexed from 0 to numberKeyGroups - 1)
     * @param parallelism Parallelism to generate the key group partitioning for
     * @return List of key group partitions
     */
    public static List<KeyGroupRange> createKeyGroupPartitions(
            int numberKeyGroups, int parallelism) {
        Preconditions.checkArgument(numberKeyGroups >= parallelism);
        List<KeyGroupRange> result = new ArrayList<>(parallelism);

        for (int i = 0; i < parallelism; ++i) {
            result.add(
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                            numberKeyGroups, parallelism, i));
        }
        return result;
    }

    /**
     * Verifies conditions in regards to parallelism and maxParallelism that must be met when
     * restoring state.
     *
     * @param operatorState state to restore
     * @param executionJobVertex task for which the state should be restored
     */
    private static void checkParallelismPreconditions(
            OperatorState operatorState, ExecutionJobVertex executionJobVertex) {
        // ----------------------------------------max parallelism
        // preconditions-------------------------------------

        if (operatorState.getMaxParallelism() < executionJobVertex.getParallelism()) {
            throw new IllegalStateException(
                    "The state for task "
                            + executionJobVertex.getJobVertexId()
                            + " can not be restored. The maximum parallelism ("
                            + operatorState.getMaxParallelism()
                            + ") of the restored state is lower than the configured parallelism ("
                            + executionJobVertex.getParallelism()
                            + "). Please reduce the parallelism of the task to be lower or equal to the maximum parallelism.");
        }

        // check that the number of key groups have not changed or if we need to override it to
        // satisfy the restored state
        if (operatorState.getMaxParallelism() != executionJobVertex.getMaxParallelism()) {

            if (!executionJobVertex.isMaxParallelismConfigured()) {
                // if the max parallelism was not explicitly specified by the user, we derive it
                // from the state

                LOG.debug(
                        "Overriding maximum parallelism for JobVertex {} from {} to {}",
                        executionJobVertex.getJobVertexId(),
                        executionJobVertex.getMaxParallelism(),
                        operatorState.getMaxParallelism());

                executionJobVertex.setMaxParallelism(operatorState.getMaxParallelism());
            } else {
                // if the max parallelism was explicitly specified, we complain on mismatch
                throw new IllegalStateException(
                        "The maximum parallelism ("
                                + operatorState.getMaxParallelism()
                                + ") with which the latest "
                                + "checkpoint of the execution job vertex "
                                + executionJobVertex
                                + " has been taken and the current maximum parallelism ("
                                + executionJobVertex.getMaxParallelism()
                                + ") changed. This "
                                + "is currently not supported.");
            }
        }
    }

    /**
     * Verifies that all operator states can be mapped to an execution job vertex.
     *
     * @param allowNonRestoredState if false an exception will be thrown if a state could not be
     *     mapped
     * @param operatorStates operator states to map
     * @param tasks task to map to
     */
    private static void checkStateMappingCompleteness(
            boolean allowNonRestoredState,
            Map<OperatorID, OperatorState> operatorStates,
            Set<ExecutionJobVertex> tasks) {

        Set<OperatorID> allOperatorIDs = new HashSet<>();
        for (ExecutionJobVertex executionJobVertex : tasks) {
            for (OperatorIDPair operatorIDPair : executionJobVertex.getOperatorIDs()) {
                allOperatorIDs.add(operatorIDPair.getGeneratedOperatorID());
                operatorIDPair.getUserDefinedOperatorID().ifPresent(allOperatorIDs::add);
            }
        }
        for (Map.Entry<OperatorID, OperatorState> operatorGroupStateEntry :
                operatorStates.entrySet()) {
            OperatorState operatorState = operatorGroupStateEntry.getValue();
            // ----------------------------------------find operator for
            // state---------------------------------------------

            if (!allOperatorIDs.contains(operatorGroupStateEntry.getKey())) {
                if (allowNonRestoredState) {
                    LOG.info(
                            "Skipped checkpoint state for operator {}.",
                            operatorState.getOperatorID());
                } else {
                    throw new IllegalStateException(
                            "There is no operator for the state " + operatorState.getOperatorID());
                }
            }
        }
    }

    public static <T> Map<OperatorInstanceID, List<T>> applyRepartitioner(
            OperatorID operatorID,
            OperatorStateRepartitioner<T> opStateRepartitioner,
            List<List<T>> chainOpParallelStates,
            int oldParallelism,
            int newParallelism) {

        List<List<T>> states =
                applyRepartitioner(
                        opStateRepartitioner,
                        chainOpParallelStates,
                        oldParallelism,
                        newParallelism);

        return toInstanceMap(operatorID, states);
    }

    private static <T> Map<OperatorInstanceID, List<T>> toInstanceMap(
            OperatorID operatorID, List<List<T>> states) {
        Map<OperatorInstanceID, List<T>> result = new HashMap<>(states.size());

        for (int subtaskIndex = 0; subtaskIndex < states.size(); subtaskIndex++) {
            checkNotNull(states.get(subtaskIndex) != null, "states.get(subtaskIndex) is null");
            result.put(OperatorInstanceID.of(subtaskIndex, operatorID), states.get(subtaskIndex));
        }

        return result;
    }

    /**
     * Repartitions the given operator state using the given {@link OperatorStateRepartitioner} with
     * respect to the new parallelism.
     *
     * @param opStateRepartitioner partitioner to use
     * @param chainOpParallelStates state to repartition
     * @param oldParallelism parallelism with which the state is currently partitioned
     * @param newParallelism parallelism with which the state should be partitioned
     * @return repartitioned state
     */
    // TODO rewrite based on operator id
    public static <T> List<List<T>> applyRepartitioner(
            OperatorStateRepartitioner<T> opStateRepartitioner,
            List<List<T>> chainOpParallelStates,
            int oldParallelism,
            int newParallelism) {

        if (chainOpParallelStates == null) {
            return emptyList();
        }

        return opStateRepartitioner.repartitionState(
                chainOpParallelStates, oldParallelism, newParallelism);
    }
}
