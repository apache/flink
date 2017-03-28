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

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This class encapsulates the operation of assigning restored state when restoring from a checkpoint.
 */
public class StateAssignmentOperation {

	private final Logger logger;
	private final Map<JobVertexID, ExecutionJobVertex> tasks;
	private final Map<JobVertexID, TaskState> taskStates;
	private final boolean allowNonRestoredState;

	public StateAssignmentOperation(
			Logger logger,
			Map<JobVertexID, ExecutionJobVertex> tasks,
			Map<JobVertexID, TaskState> taskStates,
			boolean allowNonRestoredState) {

		this.logger = Preconditions.checkNotNull(logger);
		this.tasks = Preconditions.checkNotNull(tasks);
		this.taskStates = Preconditions.checkNotNull(taskStates);
		this.allowNonRestoredState = allowNonRestoredState;
	}

	public boolean assignStates() throws Exception {

		// this tracks if we find missing node hash ids and already use secondary mappings
		boolean expandedToLegacyIds = false;

		Map<JobVertexID, ExecutionJobVertex> localTasks = this.tasks;

		for (Map.Entry<JobVertexID, TaskState> taskGroupStateEntry : taskStates.entrySet()) {

			TaskState taskState = taskGroupStateEntry.getValue();

			//----------------------------------------find vertex for state---------------------------------------------

			ExecutionJobVertex executionJobVertex = localTasks.get(taskGroupStateEntry.getKey());

			// on the first time we can not find the execution job vertex for an id, we also consider alternative ids,
			// for example as generated from older flink versions, to provide backwards compatibility.
			if (executionJobVertex == null && !expandedToLegacyIds) {
				localTasks = ExecutionJobVertex.includeLegacyJobVertexIDs(localTasks);
				executionJobVertex = localTasks.get(taskGroupStateEntry.getKey());
				expandedToLegacyIds = true;
				logger.info("Could not find ExecutionJobVertex. Including legacy JobVertexIDs in search.");
			}

			if (executionJobVertex == null) {
				if (allowNonRestoredState) {
					logger.info("Skipped checkpoint state for operator {}.", taskState.getJobVertexID());
					continue;
				} else {
					throw new IllegalStateException("There is no execution job vertex for the job" +
							" vertex ID " + taskGroupStateEntry.getKey());
				}
			}

			checkParallelismPreconditions(taskState, executionJobVertex);

			assignTaskStatesToOperatorInstances(taskState, executionJobVertex);
		}

		return true;
	}

	private void checkParallelismPreconditions(TaskState taskState, ExecutionJobVertex executionJobVertex) {
		//----------------------------------------max parallelism preconditions-------------------------------------

		// check that the number of key groups have not changed or if we need to override it to satisfy the restored state
		if (taskState.getMaxParallelism() != executionJobVertex.getMaxParallelism()) {

			if (!executionJobVertex.isMaxParallelismConfigured()) {
				// if the max parallelism was not explicitly specified by the user, we derive it from the state

				if (logger.isDebugEnabled()) {
					logger.debug("Overriding maximum parallelism for JobVertex " + executionJobVertex.getJobVertexId()
							+ " from " + executionJobVertex.getMaxParallelism() + " to " + taskState.getMaxParallelism());
				}

				executionJobVertex.setMaxParallelism(taskState.getMaxParallelism());
			} else {
				// if the max parallelism was explicitly specified, we complain on mismatch
				throw new IllegalStateException("The maximum parallelism (" +
						taskState.getMaxParallelism() + ") with which the latest " +
						"checkpoint of the execution job vertex " + executionJobVertex +
						" has been taken and the current maximum parallelism (" +
						executionJobVertex.getMaxParallelism() + ") changed. This " +
						"is currently not supported.");
			}
		}

		//----------------------------------------parallelism preconditions-----------------------------------------

		final int oldParallelism = taskState.getParallelism();
		final int newParallelism = executionJobVertex.getParallelism();

		if (taskState.hasNonPartitionedState() && (oldParallelism != newParallelism)) {
			throw new IllegalStateException("Cannot restore the latest checkpoint because " +
					"the operator " + executionJobVertex.getJobVertexId() + " has non-partitioned " +
					"state and its parallelism changed. The operator " + executionJobVertex.getJobVertexId() +
					" has parallelism " + newParallelism + " whereas the corresponding " +
					"state object has a parallelism of " + oldParallelism);
		}
	}

	private static void assignTaskStatesToOperatorInstances(
			TaskState taskState, ExecutionJobVertex executionJobVertex) {

		final int oldParallelism = taskState.getParallelism();
		final int newParallelism = executionJobVertex.getParallelism();

		List<KeyGroupRange> keyGroupPartitions = createKeyGroupPartitions(
				executionJobVertex.getMaxParallelism(),
				newParallelism);

		final int chainLength = taskState.getChainLength();

		// operator chain idx -> list of the stored op states from all parallel instances for this chain idx
		@SuppressWarnings("unchecked")
		List<OperatorStateHandle>[] parallelOpStatesBackend = new List[chainLength];
		@SuppressWarnings("unchecked")
		List<OperatorStateHandle>[] parallelOpStatesStream = new List[chainLength];

		List<KeyedStateHandle> parallelKeyedStatesBackend = new ArrayList<>(oldParallelism);
		List<KeyedStateHandle> parallelKeyedStateStream = new ArrayList<>(oldParallelism);

		for (int p = 0; p < oldParallelism; ++p) {
			SubtaskState subtaskState = taskState.getState(p);

			if (null != subtaskState) {
				collectParallelStatesByChainOperator(
						parallelOpStatesBackend, subtaskState.getManagedOperatorState());

				collectParallelStatesByChainOperator(
						parallelOpStatesStream, subtaskState.getRawOperatorState());

				KeyedStateHandle keyedStateBackend = subtaskState.getManagedKeyedState();
				if (null != keyedStateBackend) {
					parallelKeyedStatesBackend.add(keyedStateBackend);
				}

				KeyedStateHandle keyedStateStream = subtaskState.getRawKeyedState();
				if (null != keyedStateStream) {
					parallelKeyedStateStream.add(keyedStateStream);
				}
			}
		}

		// operator chain index -> lists with collected states (one collection for each parallel subtasks)
		@SuppressWarnings("unchecked")
		List<Collection<OperatorStateHandle>>[] partitionedParallelStatesBackend = new List[chainLength];

		@SuppressWarnings("unchecked")
		List<Collection<OperatorStateHandle>>[] partitionedParallelStatesStream = new List[chainLength];

		//TODO here we can employ different redistribution strategies for state, e.g. union state.
		// For now we only offer round robin as the default.
		OperatorStateRepartitioner opStateRepartitioner = RoundRobinOperatorStateRepartitioner.INSTANCE;

		for (int chainIdx = 0; chainIdx < chainLength; ++chainIdx) {

			List<OperatorStateHandle> chainOpParallelStatesBackend = parallelOpStatesBackend[chainIdx];
			List<OperatorStateHandle> chainOpParallelStatesStream = parallelOpStatesStream[chainIdx];

			partitionedParallelStatesBackend[chainIdx] = applyRepartitioner(
					opStateRepartitioner,
					chainOpParallelStatesBackend,
					oldParallelism,
					newParallelism);

			partitionedParallelStatesStream[chainIdx] = applyRepartitioner(
					opStateRepartitioner,
					chainOpParallelStatesStream,
					oldParallelism,
					newParallelism);
		}

		for (int subTaskIdx = 0; subTaskIdx < newParallelism; ++subTaskIdx) {
			// non-partitioned state
			ChainedStateHandle<StreamStateHandle> nonPartitionableState = null;

			if (oldParallelism == newParallelism) {
				if (taskState.getState(subTaskIdx) != null) {
					nonPartitionableState = taskState.getState(subTaskIdx).getLegacyOperatorState();
				}
			}

			// partitionable state
			@SuppressWarnings("unchecked")
			Collection<OperatorStateHandle>[] iab = new Collection[chainLength];
			@SuppressWarnings("unchecked")
			Collection<OperatorStateHandle>[] ias = new Collection[chainLength];
			List<Collection<OperatorStateHandle>> operatorStateFromBackend = Arrays.asList(iab);
			List<Collection<OperatorStateHandle>> operatorStateFromStream = Arrays.asList(ias);

			for (int chainIdx = 0; chainIdx < partitionedParallelStatesBackend.length; ++chainIdx) {
				List<Collection<OperatorStateHandle>> redistributedOpStateBackend =
						partitionedParallelStatesBackend[chainIdx];

				List<Collection<OperatorStateHandle>> redistributedOpStateStream =
						partitionedParallelStatesStream[chainIdx];

				if (redistributedOpStateBackend != null) {
					operatorStateFromBackend.set(chainIdx, redistributedOpStateBackend.get(subTaskIdx));
				}

				if (redistributedOpStateStream != null) {
					operatorStateFromStream.set(chainIdx, redistributedOpStateStream.get(subTaskIdx));
				}
			}

			Execution currentExecutionAttempt = executionJobVertex
					.getTaskVertices()[subTaskIdx]
					.getCurrentExecutionAttempt();

			List<KeyedStateHandle> newKeyedStatesBackend;
			List<KeyedStateHandle> newKeyedStateStream;
			if (oldParallelism == newParallelism) {
				SubtaskState subtaskState = taskState.getState(subTaskIdx);
				if (subtaskState != null) {
					KeyedStateHandle oldKeyedStatesBackend = subtaskState.getManagedKeyedState();
					KeyedStateHandle oldKeyedStatesStream = subtaskState.getRawKeyedState();
					newKeyedStatesBackend = oldKeyedStatesBackend != null ? Collections.singletonList(
							oldKeyedStatesBackend) : null;
					newKeyedStateStream = oldKeyedStatesStream != null ? Collections.singletonList(
							oldKeyedStatesStream) : null;
				} else {
					newKeyedStatesBackend = null;
					newKeyedStateStream = null;
				}
			} else {
				KeyGroupRange subtaskKeyGroupIds = keyGroupPartitions.get(subTaskIdx);
				newKeyedStatesBackend = getKeyedStateHandles(parallelKeyedStatesBackend, subtaskKeyGroupIds);
				newKeyedStateStream = getKeyedStateHandles(parallelKeyedStateStream, subtaskKeyGroupIds);
			}

			TaskStateHandles taskStateHandles = new TaskStateHandles(
					nonPartitionableState,
					operatorStateFromBackend,
					operatorStateFromStream,
					newKeyedStatesBackend,
					newKeyedStateStream);

			currentExecutionAttempt.setInitialState(taskStateHandles);
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
	 * @param chainParallelOpStates array = chain ops, array[idx] = parallel states for this chain op.
	 * @param chainOpState the operator chain
	 */
	private static void collectParallelStatesByChainOperator(
			List<OperatorStateHandle>[] chainParallelOpStates, ChainedStateHandle<OperatorStateHandle> chainOpState) {

		if (null != chainOpState) {

			int chainLength = chainOpState.getLength();
			Preconditions.checkState(chainLength >= chainParallelOpStates.length,
					"Found more states than operators in the chain. Chain length: " + chainLength +
							", States: " + chainParallelOpStates.length);

			for (int chainIdx = 0; chainIdx < chainParallelOpStates.length; ++chainIdx) {
				OperatorStateHandle operatorState = chainOpState.get(chainIdx);

				if (null != operatorState) {

					List<OperatorStateHandle> opParallelStatesForOneChainOp = chainParallelOpStates[chainIdx];

					if (null == opParallelStatesForOneChainOp) {
						opParallelStatesForOneChainOp = new ArrayList<>();
						chainParallelOpStates[chainIdx] = opParallelStatesForOneChainOp;
					}
					opParallelStatesForOneChainOp.add(operatorState);
				}
			}
		}
	}

	private static List<Collection<OperatorStateHandle>> applyRepartitioner(
			OperatorStateRepartitioner opStateRepartitioner,
			List<OperatorStateHandle> chainOpParallelStates,
			int oldParallelism,
			int newParallelism) {

		if (chainOpParallelStates == null) {
			return null;
		}

		//We only redistribute if the parallelism of the operator changed from previous executions
		if (newParallelism != oldParallelism) {

			return opStateRepartitioner.repartitionState(
					chainOpParallelStates,
					newParallelism);
		} else {
			List<Collection<OperatorStateHandle>> repackStream = new ArrayList<>(newParallelism);
			for (OperatorStateHandle operatorStateHandle : chainOpParallelStates) {

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
			return repackStream;
		}
	}
}