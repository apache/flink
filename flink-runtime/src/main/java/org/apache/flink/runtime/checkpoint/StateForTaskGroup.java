/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.SerializedValue;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Simple container class which contains the task state and key-value state handles for the sub
 * tasks of a {@link org.apache.flink.runtime.jobgraph.JobVertex}.
 *
 * This class basically groups all tasks and key groups belonging to the same job vertex together.
 */
public class StateForTaskGroup implements Serializable {

	private static final long serialVersionUID = -4845578005863201810L;

	private final JobVertexID jobVertexID;

	/** Map of task states which can be accessed by their sub task index */
	private final Map<Integer, StateForTask> taskStates;

	/** Map of key-value states which can be accessed by their key group index */
	private final Map<Integer, KvStateForTasks> kvStateForTasksMap;

	/** Parallelism of the operator when it was checkpointed */
	private final int parallelism;

	public StateForTaskGroup(JobVertexID jobVertexID, int parallelism) {
		this.jobVertexID = jobVertexID;

		this.taskStates = new HashMap<>(parallelism);

		this.kvStateForTasksMap = new HashMap<>();

		this.parallelism = parallelism;
	}

	public JobVertexID getJobVertexID() {
		return jobVertexID;
	}

	public void putState(int subtaskIndex, StateForTask stateForTask) {
		if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
			throw new IndexOutOfBoundsException("The given sub task index " + subtaskIndex +
				" exceeds the maximum number of sub tasks " + taskStates.size());
		} else {
			taskStates.put(subtaskIndex, stateForTask);
		}
	}

	public StateForTask getState(int subtaskIndex) {
		if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
			throw new IndexOutOfBoundsException("The given sub task index " + subtaskIndex +
				" exceeds the maximum number of sub tasks " + taskStates.size());
		} else {
			return taskStates.get(subtaskIndex);
		}
	}

	public Collection<StateForTask> getStates() {
		return taskStates.values();
	}

	public long getStateSize() {
		long result = 0L;

		for (StateForTask stateForTask : taskStates.values()) {
			result += stateForTask.getStateSize();
		}

		for (KvStateForTasks kvStateForTasks : kvStateForTasksMap.values()) {
			result += kvStateForTasks.getStateSize();
		}

		return result;
	}

	public int getNumberCollectedStates() {
		return taskStates.size();
	}

	public int getParallelism() {
		return parallelism;
	}

	public void putKvState(int keyGroupId, KvStateForTasks kvStateForTasks) {
		kvStateForTasksMap.put(keyGroupId, kvStateForTasks);
	}

	public KvStateForTasks getKvState(int keyGroupId) {
		return kvStateForTasksMap.get(keyGroupId);
	}

	/**
	 * Retrieve the set of key-value state key groups specified by the given key group partition set.
	 * The key groups are returned as a map where the key group index maps to the serialized state
	 * handle of the key group.
	 *
	 * @param keyGroupPartition Set of key group indices
	 * @return Map of serialized key group state handles indexed by their key group index.
	 */
	public Map<Integer, SerializedValue<StateHandle<?>>> getUnwrappedKvStates(Set<Integer> keyGroupPartition) {
		HashMap<Integer, SerializedValue<StateHandle<?>>> result = new HashMap<>(keyGroupPartition.size());

		for (Integer keyGroupId : keyGroupPartition) {
			KvStateForTasks kvStateForTasks = kvStateForTasksMap.get(keyGroupId);

			if (kvStateForTasks != null) {
				result.put(keyGroupId, kvStateForTasksMap.get(keyGroupId).getKeyGroupState());
			}
		}

		return result;
	}

	public int getNumberCollectedKvStates() {
		return kvStateForTasksMap.size();
	}

	public void discard(ClassLoader classLoader) {
		for (StateForTask stateForTask: taskStates.values()) {
			stateForTask.discard(classLoader);
		}

		for (KvStateForTasks kvStateForTasks: kvStateForTasksMap.values()) {
			kvStateForTasks.discard(classLoader);
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof StateForTaskGroup) {
			StateForTaskGroup other = (StateForTaskGroup) obj;

			return jobVertexID.equals(other.jobVertexID) && parallelism == other.parallelism &&
				taskStates.equals(other.taskStates) && kvStateForTasksMap.equals(other.kvStateForTasksMap);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return parallelism + 31 * Objects.hash(jobVertexID, taskStates, kvStateForTasksMap);
	}
}
