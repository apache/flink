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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class encapsulates the operation of assigning restored state when restoring from a checkpoint.
 */
public class StateAssignmentOperation {

	private static final Logger LOG = LoggerFactory.getLogger(StateAssignmentOperation.class);

	private final Map<JobVertexID, ExecutionJobVertex> tasks;
	private final Map<OperatorID, OperatorState> operatorStates;
	private final boolean allowNonRestoredState;

	public StateAssignmentOperation(
			Map<JobVertexID, ExecutionJobVertex> tasks,
			Map<OperatorID, OperatorState> operatorStates,
			boolean allowNonRestoredState) {

		this.tasks = Preconditions.checkNotNull(tasks);
		this.operatorStates = Preconditions.checkNotNull(operatorStates);
		this.allowNonRestoredState = allowNonRestoredState;
	}

	public boolean assignStates() throws Exception {
		Map<OperatorID, OperatorState> localOperators = new HashMap<>(operatorStates);
		Map<JobVertexID, ExecutionJobVertex> localTasks = this.tasks;

		checkStateMappingCompleteness(allowNonRestoredState, operatorStates, tasks);

		for (Map.Entry<JobVertexID, ExecutionJobVertex> task : localTasks.entrySet()) {
			final ExecutionJobVertex executionJobVertex = task.getValue();

			// find the states of all operators belonging to this task
			List<OperatorID> operatorIDs = executionJobVertex.getOperatorIDs();
			List<OperatorID> altOperatorIDs = executionJobVertex.getUserDefinedOperatorIDs();
			List<OperatorState> operatorStates = new ArrayList<>();
			boolean statelessTask = true;
			for (int x = 0; x < operatorIDs.size(); x++) {
				OperatorID operatorID = altOperatorIDs.get(x) == null
					? operatorIDs.get(x)
					: altOperatorIDs.get(x);

				OperatorState operatorState = localOperators.remove(operatorID);
				if (operatorState == null) {
					operatorState = new OperatorState(
						operatorID,
						executionJobVertex.getParallelism(),
						executionJobVertex.getMaxParallelism());
				} else {
					statelessTask = false;
				}
				operatorStates.add(operatorState);
			}
			if (statelessTask) { // skip tasks where no operator has any state
				continue;
			}

			assignAttemptState(task.getValue(), operatorStates);
		}

		return true;
	}

	private void assignAttemptState(ExecutionJobVertex executionJobVertex, List<OperatorState> operatorStates) {

		List<OperatorID> operatorIDs = executionJobVertex.getOperatorIDs();

		//1. first compute the new parallelism
		checkParallelismPreconditions(operatorStates, executionJobVertex);

		int newParallelism = executionJobVertex.getParallelism();

		List<KeyGroupRange> keyGroupPartitions = createKeyGroupPartitions(
			executionJobVertex.getMaxParallelism(),
			newParallelism);

		//2. Redistribute the operator state.
		/**
		 *
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
		List<List<Collection<OperatorStateHandle>>> newManagedOperatorStates = new ArrayList<>();
		List<List<Collection<OperatorStateHandle>>> newRawOperatorStates = new ArrayList<>();

		reDistributePartitionableStates(operatorStates, newParallelism, newManagedOperatorStates, newRawOperatorStates);


		//3. Compute TaskStateHandles of every subTask in the executionJobVertex
		/**
		 *  An executionJobVertex's all state handles needed to restore are something like a matrix
		 *
		 * 		parallelism0 parallelism1 parallelism2 parallelism3
		 * op0   sh(0,0)     sh(0,1)       sh(0,2)	    sh(0,3)
		 * op1   sh(1,0)	 sh(1,1)	   sh(1,2)	    sh(1,3)
		 * op2   sh(2,0)	 sh(2,1)	   sh(2,2)		sh(2,3)
		 * op3   sh(3,0)	 sh(3,1)	   sh(3,2)		sh(3,3)
		 *
		 * we will compute the state handles column by column.
		 *
		 */
		for (int subTaskIndex = 0; subTaskIndex < newParallelism; subTaskIndex++) {

			Execution currentExecutionAttempt = executionJobVertex.getTaskVertices()[subTaskIndex]
				.getCurrentExecutionAttempt();

			Tuple2<Collection<KeyedStateHandle>, Collection<KeyedStateHandle>> subKeyedState = null;

			List<Collection<OperatorStateHandle>> subManagedOperatorState = new ArrayList<>();
			List<Collection<OperatorStateHandle>> subRawOperatorState = new ArrayList<>();


			for (int operatorIndex = 0; operatorIndex < operatorIDs.size(); operatorIndex++) {
				OperatorState operatorState = operatorStates.get(operatorIndex);
				int oldParallelism = operatorState.getParallelism();

				// PartitionedState
				reAssignSubPartitionableState(
					newManagedOperatorStates,
					newRawOperatorStates,
					subTaskIndex,
					operatorIndex,
					subManagedOperatorState,
					subRawOperatorState);

				// KeyedState
				if (isHeadOperator(operatorIndex, operatorIDs)) {
					subKeyedState = reAssignSubKeyedStates(
						operatorState,
						keyGroupPartitions,
						subTaskIndex,
						newParallelism,
						oldParallelism);
				}
			}

			// check if a stateless task
			if (!allElementsAreNull(subManagedOperatorState) ||
				!allElementsAreNull(subRawOperatorState) ||
				subKeyedState != null) {

				TaskStateSnapshot taskState = new TaskStateSnapshot();

				for (int i = 0; i < operatorIDs.size(); ++i) {

					OperatorID operatorID = operatorIDs.get(i);

					Collection<KeyedStateHandle> rawKeyed = Collections.emptyList();
					Collection<KeyedStateHandle> managedKeyed = Collections.emptyList();

					// keyed state case
					if (subKeyedState != null) {
						managedKeyed = subKeyedState.f0;
						rawKeyed = subKeyedState.f1;
					}

					OperatorSubtaskState operatorSubtaskState =
						new OperatorSubtaskState(
							subManagedOperatorState.get(i),
							subRawOperatorState.get(i),
							managedKeyed,
							rawKeyed
						);

					taskState.putSubtaskStateByOperatorID(operatorID, operatorSubtaskState);
				}

				currentExecutionAttempt.setInitialState(taskState);
			}
		}
	}

	private static boolean isHeadOperator(int opIdx, List<OperatorID> operatorIDs) {
		return opIdx == operatorIDs.size() - 1;
	}

	public void checkParallelismPreconditions(List<OperatorState> operatorStates, ExecutionJobVertex executionJobVertex) {

		for (OperatorState operatorState : operatorStates) {
			checkParallelismPreconditions(operatorState, executionJobVertex);
		}
	}


	private void reAssignSubPartitionableState(
			List<List<Collection<OperatorStateHandle>>> newMangedOperatorStates,
			List<List<Collection<OperatorStateHandle>>> newRawOperatorStates,
			int subTaskIndex, int operatorIndex,
			List<Collection<OperatorStateHandle>> subManagedOperatorState,
			List<Collection<OperatorStateHandle>> subRawOperatorState) {

		if (newMangedOperatorStates.get(operatorIndex) != null && !newMangedOperatorStates.get(operatorIndex).isEmpty()) {
			Collection<OperatorStateHandle> operatorStateHandles = newMangedOperatorStates.get(operatorIndex).get(subTaskIndex);
			subManagedOperatorState.add(operatorStateHandles != null ? operatorStateHandles : Collections.<OperatorStateHandle>emptyList());
		} else {
			subManagedOperatorState.add(Collections.<OperatorStateHandle>emptyList());
		}
		if (newRawOperatorStates.get(operatorIndex) != null && !newRawOperatorStates.get(operatorIndex).isEmpty()) {
			Collection<OperatorStateHandle> operatorStateHandles = newRawOperatorStates.get(operatorIndex).get(subTaskIndex);
			subRawOperatorState.add(operatorStateHandles != null ? operatorStateHandles : Collections.<OperatorStateHandle>emptyList());
		} else {
			subRawOperatorState.add(Collections.<OperatorStateHandle>emptyList());
		}
	}

	private Tuple2<Collection<KeyedStateHandle>, Collection<KeyedStateHandle>> reAssignSubKeyedStates(
			OperatorState operatorState,
			List<KeyGroupRange> keyGroupPartitions,
			int subTaskIndex,
			int newParallelism,
			int oldParallelism) {

		Collection<KeyedStateHandle> subManagedKeyedState;
		Collection<KeyedStateHandle> subRawKeyedState;

		if (newParallelism == oldParallelism) {
			if (operatorState.getState(subTaskIndex) != null) {
				subManagedKeyedState = operatorState.getState(subTaskIndex).getManagedKeyedState();
				subRawKeyedState = operatorState.getState(subTaskIndex).getRawKeyedState();
			} else {
				subManagedKeyedState = Collections.emptyList();
				subRawKeyedState = Collections.emptyList();
			}
		} else {
			subManagedKeyedState = getManagedKeyedStateHandles(operatorState, keyGroupPartitions.get(subTaskIndex));
			subRawKeyedState = getRawKeyedStateHandles(operatorState, keyGroupPartitions.get(subTaskIndex));
		}

		if (subManagedKeyedState.isEmpty() && subRawKeyedState.isEmpty()) {
			return null;
		} else {
			return new Tuple2<>(subManagedKeyedState, subRawKeyedState);
		}
	}


	private <X> boolean allElementsAreNull(List<X> nonPartitionableStates) {
		for (Object streamStateHandle : nonPartitionableStates) {
			if (streamStateHandle != null) {
				return false;
			}
		}
		return true;
	}

	private void reDistributePartitionableStates(
			List<OperatorState> operatorStates, int newParallelism,
			List<List<Collection<OperatorStateHandle>>> newManagedOperatorStates,
			List<List<Collection<OperatorStateHandle>>> newRawOperatorStates) {

		//collect the old partitionable state
		List<List<OperatorStateHandle>> oldManagedOperatorStates = new ArrayList<>();
		List<List<OperatorStateHandle>> oldRawOperatorStates = new ArrayList<>();

		collectPartionableStates(operatorStates, oldManagedOperatorStates, oldRawOperatorStates);


		//redistribute
		OperatorStateRepartitioner opStateRepartitioner = RoundRobinOperatorStateRepartitioner.INSTANCE;

		for (int operatorIndex = 0; operatorIndex < operatorStates.size(); operatorIndex++) {
			int oldParallelism = operatorStates.get(operatorIndex).getParallelism();
			newManagedOperatorStates.add(applyRepartitioner(opStateRepartitioner,
				oldManagedOperatorStates.get(operatorIndex), oldParallelism, newParallelism));
			newRawOperatorStates.add(applyRepartitioner(opStateRepartitioner,
				oldRawOperatorStates.get(operatorIndex), oldParallelism, newParallelism));

		}
	}


	private void collectPartionableStates(
			List<OperatorState> operatorStates,
			List<List<OperatorStateHandle>> managedOperatorStates,
			List<List<OperatorStateHandle>> rawOperatorStates) {

		for (OperatorState operatorState : operatorStates) {
			List<OperatorStateHandle> managedOperatorState = null;
			List<OperatorStateHandle> rawOperatorState = null;

			for (int i = 0; i < operatorState.getParallelism(); i++) {
				OperatorSubtaskState operatorSubtaskState = operatorState.getState(i);
				if (operatorSubtaskState != null) {

					if (managedOperatorState == null) {
						managedOperatorState = new ArrayList<>();
					}
					managedOperatorState.addAll(operatorSubtaskState.getManagedOperatorState());

					if (rawOperatorState == null) {
						rawOperatorState = new ArrayList<>();
					}
					rawOperatorState.addAll(operatorSubtaskState.getRawOperatorState());
				}

			}
			managedOperatorStates.add(managedOperatorState);
			rawOperatorStates.add(rawOperatorState);
		}
	}


	/**
	 * Collect {@link KeyGroupsStateHandle  managedKeyedStateHandles} which have intersection with given
	 * {@link KeyGroupRange} from {@link TaskState operatorState}
	 *
	 * @param operatorState        all state handles of a operator
	 * @param subtaskKeyGroupRange the KeyGroupRange of a subtask
	 * @return all managedKeyedStateHandles which have intersection with given KeyGroupRange
	 */
	public static List<KeyedStateHandle> getManagedKeyedStateHandles(
		OperatorState operatorState,
		KeyGroupRange subtaskKeyGroupRange) {

		List<KeyedStateHandle> subtaskKeyedStateHandles = new ArrayList<>();

		for (int i = 0; i < operatorState.getParallelism(); i++) {
			if (operatorState.getState(i) != null) {

				Collection<KeyedStateHandle> keyedStateHandles = operatorState.getState(i).getManagedKeyedState();
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
	 * {@link KeyGroupRange} from {@link TaskState operatorState}
	 *
	 * @param operatorState        all state handles of a operator
	 * @param subtaskKeyGroupRange the KeyGroupRange of a subtask
	 * @return all rawKeyedStateHandles which have intersection with given KeyGroupRange
	 */
	public static List<KeyedStateHandle> getRawKeyedStateHandles(
		OperatorState operatorState,
		KeyGroupRange subtaskKeyGroupRange) {

		List<KeyedStateHandle> extractedKeyedStateHandles = new ArrayList<>();

		for (int i = 0; i < operatorState.getParallelism(); i++) {
			if (operatorState.getState(i) != null) {
				Collection<KeyedStateHandle> rawKeyedState = operatorState.getState(i).getRawKeyedState();
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
	private static void extractIntersectingState(
		Collection<KeyedStateHandle> originalSubtaskStateHandles,
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
	 * <p>
	 * <b>IMPORTANT</b>: The assignment of key groups to partitions has to be in sync with the
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
			Map<JobVertexID, ExecutionJobVertex> tasks) {

		Set<OperatorID> allOperatorIDs = new HashSet<>();
		for (ExecutionJobVertex executionJobVertex : tasks.values()) {
			allOperatorIDs.addAll(executionJobVertex.getOperatorIDs());
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
	public static List<Collection<OperatorStateHandle>> applyRepartitioner(
			OperatorStateRepartitioner opStateRepartitioner,
			List<OperatorStateHandle> chainOpParallelStates,
			int oldParallelism,
			int newParallelism) {

		if (chainOpParallelStates == null) {
			return Collections.emptyList();
		}

		//We only redistribute if the parallelism of the operator changed from previous executions
		if (newParallelism != oldParallelism) {

			return opStateRepartitioner.repartitionState(
					chainOpParallelStates,
					newParallelism);
		} else {
			List<Collection<OperatorStateHandle>> repackStream = new ArrayList<>(newParallelism);
			for (OperatorStateHandle operatorStateHandle : chainOpParallelStates) {

				if (operatorStateHandle != null) {
					Map<String, OperatorStateHandle.StateMetaInfo> partitionOffsets =
						operatorStateHandle.getStateNameToPartitionOffsets();


					for (OperatorStateHandle.StateMetaInfo metaInfo : partitionOffsets.values()) {

						// if we find any broadcast state, we cannot take the shortcut and need to go through repartitioning
						if (OperatorStateHandle.Mode.BROADCAST.equals(metaInfo.getDistributionMode())) {
							return opStateRepartitioner.repartitionState(
								chainOpParallelStates,
								newParallelism);
						}
					}

					repackStream.add(Collections.singletonList(operatorStateHandle));
				}
			}
			return repackStream;
		}
	}

	/**
	 * Determine the subset of {@link KeyGroupsStateHandle KeyGroupsStateHandles} with correct
	 * key group index for the given subtask {@link KeyGroupRange}.
	 * <p>
	 * <p>This is publicly visible to be used in tests.
	 */
	public static List<KeyedStateHandle> getKeyedStateHandles(
		Collection<? extends KeyedStateHandle> keyedStateHandles,
		KeyGroupRange subtaskKeyGroupRange) {

		List<KeyedStateHandle> subtaskKeyedStateHandles = new ArrayList<>();

		for (KeyedStateHandle keyedStateHandle : keyedStateHandles) {
			KeyedStateHandle intersectedKeyedStateHandle = keyedStateHandle.getIntersection(subtaskKeyGroupRange);

			if (intersectedKeyedStateHandle != null) {
				subtaskKeyedStateHandles.add(intersectedKeyedStateHandle);
			}
		}

		return subtaskKeyedStateHandles;
	}
}
