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

import com.google.common.collect.Iterables;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Simple container class which contains the task state and key-group state handles for the sub
 * tasks of a {@link org.apache.flink.runtime.jobgraph.JobVertex}.
 *
 * This class basically groups all non-partitioned state and key-group state belonging to the same job vertex together.
 */
public class TaskState implements StateObject {

	private static final long serialVersionUID = -4845578005863201810L;

	private final JobVertexID jobVertexID;

	/** handles to non-partitioned states, subtaskindex -> subtaskstate */
	private final Map<Integer, SubtaskState> subtaskStates;

	/** handles to partitionable states, subtaskindex -> partitionable state */
	private final Map<Integer, ChainedStateHandle<OperatorStateHandle>> partitionableStates;

	/** handles to key-partitioned states, subtaskindex -> keyed state */
	private final Map<Integer, KeyGroupsStateHandle> keyGroupsStateHandles;


	/** parallelism of the operator when it was checkpointed */
	private final int parallelism;

	/** maximum parallelism of the operator when the job was first created */
	private final int maxParallelism;

	private final int chainLength;

	public TaskState(JobVertexID jobVertexID, int parallelism, int maxParallelism, int chainLength) {
		Preconditions.checkArgument(
				parallelism <= maxParallelism,
				"Parallelism " + parallelism + " is not smaller or equal to max parallelism " + maxParallelism + ".");
		Preconditions.checkArgument(chainLength > 0, "There has to be at least one operator in the operator chain.");

		this.jobVertexID = jobVertexID;

		this.subtaskStates = new HashMap<>(parallelism);
		this.partitionableStates = new HashMap<>(parallelism);
		this.keyGroupsStateHandles = new HashMap<>(parallelism);

		this.parallelism = parallelism;
		this.maxParallelism = maxParallelism;
		this.chainLength = chainLength;
	}

	public JobVertexID getJobVertexID() {
		return jobVertexID;
	}

	public void putState(int subtaskIndex, SubtaskState subtaskState) {
		Preconditions.checkNotNull(subtaskState);

		if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
			throw new IndexOutOfBoundsException("The given sub task index " + subtaskIndex +
				" exceeds the maximum number of sub tasks " + subtaskStates.size());
		} else {
			subtaskStates.put(subtaskIndex, subtaskState);
		}
	}

	public void putPartitionableState(
			int subtaskIndex,
			ChainedStateHandle<OperatorStateHandle> partitionableState) {

		Preconditions.checkNotNull(partitionableState);

		if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
			throw new IndexOutOfBoundsException("The given sub task index " + subtaskIndex +
					" exceeds the maximum number of sub tasks " + subtaskStates.size());
		} else {
			partitionableStates.put(subtaskIndex, partitionableState);
		}
	}

	public void putKeyedState(int subtaskIndex, KeyGroupsStateHandle keyGroupsStateHandle) {
		Preconditions.checkNotNull(keyGroupsStateHandle);

		if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
			throw new IndexOutOfBoundsException("The given sub task index " + subtaskIndex +
					" exceeds the maximum number of sub tasks " + subtaskStates.size());
		} else {
			keyGroupsStateHandles.put(subtaskIndex, keyGroupsStateHandle);
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

	public ChainedStateHandle<OperatorStateHandle> getPartitionableState(int subtaskIndex) {
		if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
			throw new IndexOutOfBoundsException("The given sub task index " + subtaskIndex +
					" exceeds the maximum number of sub tasks " + subtaskStates.size());
		} else {
			return partitionableStates.get(subtaskIndex);
		}
	}

	public KeyGroupsStateHandle getKeyGroupState(int subtaskIndex) {
		if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
			throw new IndexOutOfBoundsException("The given sub task index " + subtaskIndex +
					" exceeds the maximum number of sub tasks " + keyGroupsStateHandles.size());
		} else {
			return keyGroupsStateHandles.get(subtaskIndex);
		}
	}

	public Collection<SubtaskState> getStates() {
		return subtaskStates.values();
	}

	public int getNumberCollectedStates() {
		return subtaskStates.size();
	}

	public int getParallelism() {
		return parallelism;
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	public int getChainLength() {
		return chainLength;
	}

	public Collection<KeyGroupsStateHandle> getKeyGroupStates() {
		return keyGroupsStateHandles.values();
	}

	public boolean hasNonPartitionedState() {
		for(SubtaskState sts : subtaskStates.values()) {
			if (sts != null && !sts.getChainedStateHandle().isEmpty()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void discardState() throws Exception {
		StateUtil.bestEffortDiscardAllStateObjects(
				Iterables.concat(subtaskStates.values(), partitionableStates.values(), keyGroupsStateHandles.values()));
	}


	@Override
	public long getStateSize() throws IOException {
		long result = 0L;

		for (int i = 0; i < parallelism; i++) {
			SubtaskState subtaskState = subtaskStates.get(i);
			if (subtaskState != null) {
				result += subtaskState.getStateSize();
			}

			ChainedStateHandle<OperatorStateHandle> partitionableState = partitionableStates.get(i);
			if (partitionableState != null) {
				result += partitionableState.getStateSize();
			}

			KeyGroupsStateHandle keyGroupsState = keyGroupsStateHandles.get(i);
			if (keyGroupsState != null) {
				result += keyGroupsState.getStateSize();
			}
		}

		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TaskState) {
			TaskState other = (TaskState) obj;

			return jobVertexID.equals(other.jobVertexID)
					&& parallelism == other.parallelism
					&& subtaskStates.equals(other.subtaskStates)
					&& partitionableStates.equals(other.partitionableStates)
					&& keyGroupsStateHandles.equals(other.keyGroupsStateHandles);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return parallelism + 31 * Objects.hash(jobVertexID, subtaskStates, partitionableStates, keyGroupsStateHandles);
	}

	public Map<Integer, SubtaskState> getSubtaskStates() {
		return Collections.unmodifiableMap(subtaskStates);
	}

	public Map<Integer, KeyGroupsStateHandle> getKeyGroupsStateHandles() {
		return Collections.unmodifiableMap(keyGroupsStateHandles);
	}

	public Map<Integer, ChainedStateHandle<OperatorStateHandle>> getPartitionableStates() {
		return partitionableStates;
	}
}
