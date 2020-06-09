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
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class encapsulates the operation of assigning restored state when restoring from a checkpoint.
 */
@Internal
public class StateAssignmentOperation {

	private static final Logger LOG = LoggerFactory.getLogger(StateAssignmentOperation.class);

	private final Set<ExecutionJobVertex> tasks;
	private final Map<OperatorID, OperatorState> operatorStates;

	private final long restoreCheckpointId;
	private final boolean allowNonRestoredState;

	public StateAssignmentOperation(
		long restoreCheckpointId,
		Set<ExecutionJobVertex> tasks,
		Map<OperatorID, OperatorState> operatorStates,
		boolean allowNonRestoredState) {

		this.restoreCheckpointId = restoreCheckpointId;
		this.tasks = Preconditions.checkNotNull(tasks);
		this.operatorStates = Preconditions.checkNotNull(operatorStates);
		this.allowNonRestoredState = allowNonRestoredState;
	}

	public void assignStates() {
		Map<OperatorID, OperatorState> localOperators = new HashMap<>(operatorStates);

		checkStateMappingCompleteness(allowNonRestoredState, operatorStates, tasks);

		for (ExecutionJobVertex executionJobVertex : this.tasks) {

			// find the states of all operators belonging to this task
			List<OperatorIDPair> operatorIDPairs = executionJobVertex.getOperatorIDs();
			List<OperatorState> operatorStates = new ArrayList<>(operatorIDPairs.size());
			boolean statelessSubTasks = true;
			for (OperatorIDPair operatorIDPair : operatorIDPairs) {
				OperatorID operatorID = operatorIDPair.getUserDefinedOperatorID().orElse(operatorIDPair.getGeneratedOperatorID());

				OperatorState operatorState = localOperators.remove(operatorID);
				if (operatorState == null) {
					operatorState = new OperatorState(
						operatorID,
						executionJobVertex.getParallelism(),
						executionJobVertex.getMaxParallelism());
				} else if (operatorState.getNumberCollectedStates() > 0) {
					statelessSubTasks = false;
				}
				operatorStates.add(operatorState);
			}
			if (!statelessSubTasks) { // skip tasks where no operator has any state
				assignAttemptState(executionJobVertex, operatorStates);
			}
		}

	}

	private void assignAttemptState(ExecutionJobVertex executionJobVertex, List<OperatorState> operatorStates) {

		List<OperatorIDPair> operatorIDs = executionJobVertex.getOperatorIDs();

		//1. first compute the new parallelism
		checkParallelismPreconditions(operatorStates, executionJobVertex);

		int newParallelism = executionJobVertex.getParallelism();

		List<KeyGroupRange> keyGroupPartitions = createKeyGroupPartitions(
			executionJobVertex.getMaxParallelism(),
			newParallelism);

		final int expectedNumberOfSubTasks = newParallelism * operatorIDs.size();

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
		Map<OperatorInstanceID, List<OperatorStateHandle>> newManagedOperatorStates = reDistributePartitionableStates(
			operatorStates,
			newParallelism,
			operatorIDs,
			OperatorSubtaskState::getManagedOperatorState,
			RoundRobinOperatorStateRepartitioner.INSTANCE);
		Map<OperatorInstanceID, List<OperatorStateHandle>> newRawOperatorStates = reDistributePartitionableStates(
			operatorStates,
			newParallelism,
			operatorIDs,
			OperatorSubtaskState::getRawOperatorState,
			RoundRobinOperatorStateRepartitioner.INSTANCE);
		final Map<OperatorInstanceID, List<InputChannelStateHandle>> newInputChannelState = reDistributePartitionableStates(
			operatorStates,
			newParallelism,
			operatorIDs,
			OperatorSubtaskState::getInputChannelState,
			channelStateNonRescalingRepartitioner("input channel"));
		final Map<OperatorInstanceID, List<ResultSubpartitionStateHandle>> newResultSubpartitionState = reDistributePartitionableStates(
			operatorStates,
			newParallelism,
			operatorIDs,
			OperatorSubtaskState::getResultSubpartitionState,
			channelStateNonRescalingRepartitioner("result subpartition"));

		Map<OperatorInstanceID, List<KeyedStateHandle>> newManagedKeyedState = new HashMap<>(expectedNumberOfSubTasks);
		Map<OperatorInstanceID, List<KeyedStateHandle>> newRawKeyedState = new HashMap<>(expectedNumberOfSubTasks);

		reDistributeKeyedStates(
			operatorStates,
			newParallelism,
			operatorIDs,
			keyGroupPartitions,
			newManagedKeyedState,
			newRawKeyedState);

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
		assignTaskStateToExecutionJobVertices(
			executionJobVertex,
			newManagedOperatorStates,
			newRawOperatorStates,
			newInputChannelState,
			newResultSubpartitionState,
			newManagedKeyedState,
			newRawKeyedState,
			newParallelism);
	}

	private void assignTaskStateToExecutionJobVertices(
			ExecutionJobVertex executionJobVertex,
			Map<OperatorInstanceID, List<OperatorStateHandle>> subManagedOperatorState,
			Map<OperatorInstanceID, List<OperatorStateHandle>> subRawOperatorState,
			Map<OperatorInstanceID, List<InputChannelStateHandle>> inputChannelStates,
			Map<OperatorInstanceID, List<ResultSubpartitionStateHandle>> resultSubpartitionStates,
			Map<OperatorInstanceID, List<KeyedStateHandle>> subManagedKeyedState,
			Map<OperatorInstanceID, List<KeyedStateHandle>> subRawKeyedState,
			int newParallelism) {

		List<OperatorIDPair> operatorIDs = executionJobVertex.getOperatorIDs();

		for (int subTaskIndex = 0; subTaskIndex < newParallelism; subTaskIndex++) {

			Execution currentExecutionAttempt = executionJobVertex.getTaskVertices()[subTaskIndex]
				.getCurrentExecutionAttempt();

			TaskStateSnapshot taskState = new TaskStateSnapshot(operatorIDs.size());
			boolean statelessTask = true;

			for (OperatorIDPair operatorID : operatorIDs) {
				OperatorInstanceID instanceID = OperatorInstanceID.of(subTaskIndex, operatorID.getGeneratedOperatorID());

				OperatorSubtaskState operatorSubtaskState = operatorSubtaskStateFrom(
					instanceID,
					subManagedOperatorState,
					subRawOperatorState,
					inputChannelStates,
					resultSubpartitionStates,
					subManagedKeyedState,
					subRawKeyedState);

				if (operatorSubtaskState.hasState()) {
					statelessTask = false;
				}
				taskState.putSubtaskStateByOperatorID(operatorID.getGeneratedOperatorID(), operatorSubtaskState);
			}

			if (!statelessTask) {
				JobManagerTaskRestore taskRestore = new JobManagerTaskRestore(restoreCheckpointId, taskState);
				currentExecutionAttempt.setInitialState(taskRestore);
			}
		}
	}

	public static OperatorSubtaskState operatorSubtaskStateFrom(
			OperatorInstanceID instanceID,
			Map<OperatorInstanceID, List<OperatorStateHandle>> subManagedOperatorState,
			Map<OperatorInstanceID, List<OperatorStateHandle>> subRawOperatorState,
			Map<OperatorInstanceID, List<InputChannelStateHandle>> inputChannelStates,
			Map<OperatorInstanceID, List<ResultSubpartitionStateHandle>> resultSubpartitionStates,
			Map<OperatorInstanceID, List<KeyedStateHandle>> subManagedKeyedState,
			Map<OperatorInstanceID, List<KeyedStateHandle>> subRawKeyedState) {

		if (!subManagedOperatorState.containsKey(instanceID) &&
			!subRawOperatorState.containsKey(instanceID) &&
			!inputChannelStates.containsKey(instanceID) &&
			!resultSubpartitionStates.containsKey(instanceID) &&
			!subManagedKeyedState.containsKey(instanceID) &&
			!subRawKeyedState.containsKey(instanceID)) {

			return new OperatorSubtaskState();
		}
		if (!subManagedKeyedState.containsKey(instanceID)) {
			checkState(!subRawKeyedState.containsKey(instanceID));
		}
		return new OperatorSubtaskState(
			new StateObjectCollection<>(subManagedOperatorState.getOrDefault(instanceID, emptyList())),
			new StateObjectCollection<>(subRawOperatorState.getOrDefault(instanceID, emptyList())),
			new StateObjectCollection<>(subManagedKeyedState.getOrDefault(instanceID, emptyList())),
			new StateObjectCollection<>(subRawKeyedState.getOrDefault(instanceID, emptyList())),
			new StateObjectCollection<>(inputChannelStates.getOrDefault(instanceID, emptyList())),
			new StateObjectCollection<>(resultSubpartitionStates.getOrDefault(instanceID, emptyList())));
	}

	public void checkParallelismPreconditions(List<OperatorState> operatorStates, ExecutionJobVertex executionJobVertex) {
		for (OperatorState operatorState : operatorStates) {
			checkParallelismPreconditions(operatorState, executionJobVertex);
		}
	}

	private void reDistributeKeyedStates(
			List<OperatorState> oldOperatorStates,
			int newParallelism,
			List<OperatorIDPair> newOperatorIDs,
			List<KeyGroupRange> newKeyGroupPartitions,
			Map<OperatorInstanceID, List<KeyedStateHandle>> newManagedKeyedState,
			Map<OperatorInstanceID, List<KeyedStateHandle>> newRawKeyedState) {
		//TODO: rewrite this method to only use OperatorID
		checkState(newOperatorIDs.size() == oldOperatorStates.size(),
			"This method still depends on the order of the new and old operators");

		for (int operatorIndex = 0; operatorIndex < newOperatorIDs.size(); operatorIndex++) {
			OperatorState operatorState = oldOperatorStates.get(operatorIndex);
			int oldParallelism = operatorState.getParallelism();
			for (int subTaskIndex = 0; subTaskIndex < newParallelism; subTaskIndex++) {
				OperatorInstanceID instanceID = OperatorInstanceID.of(subTaskIndex, newOperatorIDs.get(operatorIndex).getGeneratedOperatorID());
				Tuple2<List<KeyedStateHandle>, List<KeyedStateHandle>> subKeyedStates = reAssignSubKeyedStates(
					operatorState,
					newKeyGroupPartitions,
					subTaskIndex,
					newParallelism,
					oldParallelism);
				newManagedKeyedState.put(instanceID, subKeyedStates.f0);
				newRawKeyedState.put(instanceID, subKeyedStates.f1);
			}
		}
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
				subManagedKeyedState = operatorState.getState(subTaskIndex).getManagedKeyedState().asList();
				subRawKeyedState = operatorState.getState(subTaskIndex).getRawKeyedState().asList();
			} else {
				subManagedKeyedState = emptyList();
				subRawKeyedState = emptyList();
			}
		} else {
			subManagedKeyedState = getManagedKeyedStateHandles(operatorState, keyGroupPartitions.get(subTaskIndex));
			subRawKeyedState = getRawKeyedStateHandles(operatorState, keyGroupPartitions.get(subTaskIndex));
		}

		if (subManagedKeyedState.isEmpty() && subRawKeyedState.isEmpty()) {
			return new Tuple2<>(emptyList(), emptyList());
		} else {
			return new Tuple2<>(subManagedKeyedState, subRawKeyedState);
		}
	}

	public static <T extends StateObject> Map<OperatorInstanceID, List<T>>  reDistributePartitionableStates(
			List<OperatorState> oldOperatorStates,
			int newParallelism,
			List<OperatorIDPair> newOperatorIDs,
			Function<OperatorSubtaskState, StateObjectCollection<T>> extractHandle,
			OperatorStateRepartitioner<T> stateRepartitioner) {

		//TODO: rewrite this method to only use OperatorID
		checkState(newOperatorIDs.size() == oldOperatorStates.size(),
			"This method still depends on the order of the new and old operators");

		// The nested list wraps as the level of operator -> subtask -> state object collection
		List<List<List<T>>> oldStates = splitManagedAndRawOperatorStates(oldOperatorStates, extractHandle);

		Map<OperatorInstanceID, List<T>> result = new HashMap<>();
		for (int operatorIndex = 0; operatorIndex < newOperatorIDs.size(); operatorIndex++) {
			result.putAll(applyRepartitioner(
				newOperatorIDs.get(operatorIndex).getGeneratedOperatorID(),
				stateRepartitioner,
				oldStates.get(operatorIndex),
				oldOperatorStates.get(operatorIndex).getParallelism(),
				newParallelism));
		}

		return result;
	}

	private static <T extends StateObject> List<List<List<T>>> splitManagedAndRawOperatorStates(
			List<OperatorState> operatorStates,
			Function<OperatorSubtaskState, StateObjectCollection<T>> extracthandle) {
		List<List<List<T>>> result = new ArrayList<>();

		for (OperatorState operatorState : operatorStates) {
			List<List<T>> statePerSubtask = new ArrayList<>(operatorState.getParallelism());

			for (int subTaskIndex = 0; subTaskIndex < operatorState.getParallelism(); subTaskIndex++) {
				OperatorSubtaskState subtaskState = operatorState.getState(subTaskIndex);
				statePerSubtask.add(subtaskState == null ? emptyList() : extracthandle.apply(subtaskState).asList());
			}
			result.add(statePerSubtask);
		}
		return result;
	}

	/**
	 * Collect {@link KeyGroupsStateHandle  managedKeyedStateHandles} which have intersection with given
	 * {@link KeyGroupRange} from {@link TaskState operatorState}.
	 *
	 * @param operatorState        all state handles of a operator
	 * @param subtaskKeyGroupRange the KeyGroupRange of a subtask
	 * @return all managedKeyedStateHandles which have intersection with given KeyGroupRange
	 */
	public static List<KeyedStateHandle> getManagedKeyedStateHandles(
			OperatorState operatorState,
			KeyGroupRange subtaskKeyGroupRange) {

		final int parallelism = operatorState.getParallelism();

		List<KeyedStateHandle> subtaskKeyedStateHandles = null;

		for (int i = 0; i < parallelism; i++) {
			if (operatorState.getState(i) != null) {

				Collection<KeyedStateHandle> keyedStateHandles = operatorState.getState(i).getManagedKeyedState();

				if (subtaskKeyedStateHandles == null) {
					subtaskKeyedStateHandles = new ArrayList<>(parallelism * keyedStateHandles.size());
				}

				extractIntersectingState(
					keyedStateHandles,
					subtaskKeyGroupRange,
					subtaskKeyedStateHandles);
			}
		}

		return subtaskKeyedStateHandles;
	}

	/**
	 * Collect {@link KeyGroupsStateHandle  rawKeyedStateHandles} which have intersection with given
	 * {@link KeyGroupRange} from {@link TaskState operatorState}.
	 *
	 * @param operatorState        all state handles of a operator
	 * @param subtaskKeyGroupRange the KeyGroupRange of a subtask
	 * @return all rawKeyedStateHandles which have intersection with given KeyGroupRange
	 */
	public static List<KeyedStateHandle> getRawKeyedStateHandles(
			OperatorState operatorState,
			KeyGroupRange subtaskKeyGroupRange) {

		final int parallelism = operatorState.getParallelism();

		List<KeyedStateHandle> extractedKeyedStateHandles = null;

		for (int i = 0; i < parallelism; i++) {
			if (operatorState.getState(i) != null) {

				Collection<KeyedStateHandle> rawKeyedState = operatorState.getState(i).getRawKeyedState();

				if (extractedKeyedStateHandles == null) {
					extractedKeyedStateHandles = new ArrayList<>(parallelism * rawKeyedState.size());
				}

				extractIntersectingState(
					rawKeyedState,
					subtaskKeyGroupRange,
					extractedKeyedStateHandles);
			}
		}

		return extractedKeyedStateHandles;
	}

	/**
	 * Extracts certain key group ranges from the given state handles and adds them to the collector.
	 */
	@VisibleForTesting
	public static void extractIntersectingState(
			Collection<? extends KeyedStateHandle> originalSubtaskStateHandles,
			KeyGroupRange rangeToExtract,
			List<KeyedStateHandle> extractedStateCollector) {

		for (KeyedStateHandle keyedStateHandle : originalSubtaskStateHandles) {

			if (keyedStateHandle != null) {

				KeyedStateHandle intersectedKeyedStateHandle = keyedStateHandle.getIntersection(rangeToExtract);

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
	 * @param parallelism     Parallelism to generate the key group partitioning for
	 * @return List of key group partitions
	 */
	public static List<KeyGroupRange> createKeyGroupPartitions(int numberKeyGroups, int parallelism) {
		Preconditions.checkArgument(numberKeyGroups >= parallelism);
		List<KeyGroupRange> result = new ArrayList<>(parallelism);

		for (int i = 0; i < parallelism; ++i) {
			result.add(KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(numberKeyGroups, parallelism, i));
		}
		return result;
	}

	/**
	 * Verifies conditions in regards to parallelism and maxParallelism that must be met when restoring state.
	 *
	 * @param operatorState      state to restore
	 * @param executionJobVertex task for which the state should be restored
	 */
	private static void checkParallelismPreconditions(OperatorState operatorState, ExecutionJobVertex executionJobVertex) {
		//----------------------------------------max parallelism preconditions-------------------------------------

		if (operatorState.getMaxParallelism() < executionJobVertex.getParallelism()) {
			throw new IllegalStateException("The state for task " + executionJobVertex.getJobVertexId() +
				" can not be restored. The maximum parallelism (" + operatorState.getMaxParallelism() +
				") of the restored state is lower than the configured parallelism (" + executionJobVertex.getParallelism() +
				"). Please reduce the parallelism of the task to be lower or equal to the maximum parallelism."
			);
		}

		// check that the number of key groups have not changed or if we need to override it to satisfy the restored state
		if (operatorState.getMaxParallelism() != executionJobVertex.getMaxParallelism()) {

			if (!executionJobVertex.isMaxParallelismConfigured()) {
				// if the max parallelism was not explicitly specified by the user, we derive it from the state

				LOG.debug("Overriding maximum parallelism for JobVertex {} from {} to {}",
					executionJobVertex.getJobVertexId(), executionJobVertex.getMaxParallelism(), operatorState.getMaxParallelism());

				executionJobVertex.setMaxParallelism(operatorState.getMaxParallelism());
			} else {
				// if the max parallelism was explicitly specified, we complain on mismatch
				throw new IllegalStateException("The maximum parallelism (" +
					operatorState.getMaxParallelism() + ") with which the latest " +
					"checkpoint of the execution job vertex " + executionJobVertex +
					" has been taken and the current maximum parallelism (" +
					executionJobVertex.getMaxParallelism() + ") changed. This " +
					"is currently not supported.");
			}
		}
	}

	/**
	 * Verifies that all operator states can be mapped to an execution job vertex.
	 *
	 * @param allowNonRestoredState if false an exception will be thrown if a state could not be mapped
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
		for (Map.Entry<OperatorID, OperatorState> operatorGroupStateEntry : operatorStates.entrySet()) {
			OperatorState operatorState = operatorGroupStateEntry.getValue();
			//----------------------------------------find operator for state---------------------------------------------

			if (!allOperatorIDs.contains(operatorGroupStateEntry.getKey())) {
				if (allowNonRestoredState) {
					LOG.info("Skipped checkpoint state for operator {}.", operatorState.getOperatorID());
				} else {
					throw new IllegalStateException("There is no operator for the state " + operatorState.getOperatorID());
				}
			}
		}
	}

	public static <T extends StateObject> Map<OperatorInstanceID, List<T>> applyRepartitioner(
			OperatorID operatorID,
			OperatorStateRepartitioner<T> opStateRepartitioner,
			List<List<T>> chainOpParallelStates,
			int oldParallelism,
			int newParallelism) {

		List<List<T>> states = applyRepartitioner(
			opStateRepartitioner,
			chainOpParallelStates,
			oldParallelism,
			newParallelism);

		Map<OperatorInstanceID, List<T>> result = new HashMap<>(states.size());

		for (int subtaskIndex = 0; subtaskIndex < states.size(); subtaskIndex++) {
			checkNotNull(states.get(subtaskIndex) != null, "states.get(subtaskIndex) is null");
			result.put(OperatorInstanceID.of(subtaskIndex, operatorID), states.get(subtaskIndex));
		}

		return result;
	}

	/**
	 * Repartitions the given operator state using the given {@link OperatorStateRepartitioner} with respect to the new
	 * parallelism.
	 *
	 * @param opStateRepartitioner  partitioner to use
	 * @param chainOpParallelStates state to repartition
	 * @param oldParallelism        parallelism with which the state is currently partitioned
	 * @param newParallelism        parallelism with which the state should be partitioned
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
			chainOpParallelStates,
			oldParallelism,
			newParallelism);
		}

	static <T extends AbstractChannelStateHandle<?>> OperatorStateRepartitioner<T> channelStateNonRescalingRepartitioner(String logStateName) {
		return (previousParallelSubtaskStates, oldParallelism, newParallelism) -> {
			Preconditions.checkArgument(
				oldParallelism == newParallelism ||
					previousParallelSubtaskStates.stream()
						.flatMap(s -> s.stream().map(l -> l.getOffsets()))
						.allMatch(List::isEmpty),
				String.format("rescaling not supported for %s state (old: %d, new: %d)", logStateName, oldParallelism, newParallelism));
			return previousParallelSubtaskStates;
		};
	}

}
