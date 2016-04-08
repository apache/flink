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
public class TaskState implements Serializable {

	private static final long serialVersionUID = -4845578005863201810L;

	private final JobVertexID jobVertexID;

	/** Map of task states which can be accessed by their sub task index */
	private final Map<Integer, SubtaskState> subtaskStates;

	/** Map of key-value states which can be accessed by their key group index */
	private final Map<Integer, KeyGroupState> kvStates;

	/** Parallelism of the operator when it was checkpointed */
	private final int parallelism;

	public TaskState(JobVertexID jobVertexID, int parallelism) {
		this.jobVertexID = jobVertexID;

		this.subtaskStates = new HashMap<>(parallelism);

		this.kvStates = new HashMap<>();

		this.parallelism = parallelism;
	}

	public JobVertexID getJobVertexID() {
		return jobVertexID;
	}

	public void putState(int subtaskIndex, SubtaskState subtaskState) {
		if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
			throw new IndexOutOfBoundsException("The given sub task index " + subtaskIndex +
				" exceeds the maximum number of sub tasks " + subtaskStates.size());
		} else {
			subtaskStates.put(subtaskIndex, subtaskState);
		}
	}

	public SubtaskState getState(int subtaskIndex) {
		if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
			throw new IndexOutOfBoundsException("The given sub task index " + subtaskIndex +
				" exceeds the maximum number of sub tasks " + subtaskStates.size());
		} else {
			return subtaskStates.get(subtaskIndex);
		}
	}

	public Collection<SubtaskState> getStates() {
		return subtaskStates.values();
	}

	public long getStateSize() {
		long result = 0L;

		for (SubtaskState subtaskState : subtaskStates.values()) {
			result += subtaskState.getStateSize();
		}

		for (KeyGroupState keyGroupState : kvStates.values()) {
			result += keyGroupState.getStateSize();
		}

		return result;
	}

	public int getNumberCollectedStates() {
		return subtaskStates.size();
	}

	public int getParallelism() {
		return parallelism;
	}

	public void putKvState(int keyGroupId, KeyGroupState keyGroupState) {
		kvStates.put(keyGroupId, keyGroupState);
	}

	public KeyGroupState getKvState(int keyGroupId) {
		return kvStates.get(keyGroupId);
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
			KeyGroupState keyGroupState = kvStates.get(keyGroupId);

			if (keyGroupState != null) {
				result.put(keyGroupId, kvStates.get(keyGroupId).getKeyGroupState());
			}
		}

		return result;
	}

	public int getNumberCollectedKvStates() {
		return kvStates.size();
	}

	public void discard(ClassLoader classLoader) {
		for (SubtaskState subtaskState : subtaskStates.values()) {
			subtaskState.discard(classLoader);
		}

		for (KeyGroupState keyGroupState : kvStates.values()) {
			keyGroupState.discard(classLoader);
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TaskState) {
			TaskState other = (TaskState) obj;

			return jobVertexID.equals(other.jobVertexID) && parallelism == other.parallelism &&
				subtaskStates.equals(other.subtaskStates) && kvStates.equals(other.kvStates);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return parallelism + 31 * Objects.hash(jobVertexID, subtaskStates, kvStates);
	}
}
