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

package org.apache.flink.migration.v0.runtime;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Deprecated
@SuppressWarnings("deprecation")
public class TaskStateV0 implements Serializable {

	private static final long serialVersionUID = -4845578005863201810L;

	private final JobVertexID jobVertexID;

	/** Map of task states which can be accessed by their sub task index */
	private final Map<Integer, SubtaskStateV0> subtaskStates;

	/** Map of key-value states which can be accessed by their key group index */
	private final Map<Integer, KeyGroupStateV0> kvStates;

	/** Parallelism of the operator when it was checkpointed */
	private final int parallelism;

	public TaskStateV0(JobVertexID jobVertexID, int parallelism) {
		this.jobVertexID = jobVertexID;

		this.subtaskStates = new HashMap<>(parallelism);

		this.kvStates = new HashMap<>();

		this.parallelism = parallelism;
	}

	public JobVertexID getJobVertexID() {
		return jobVertexID;
	}

	public void putState(int subtaskIndex, SubtaskStateV0 subtaskState) {
		if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
			throw new IndexOutOfBoundsException("The given sub task index " + subtaskIndex +
				" exceeds the maximum number of sub tasks " + subtaskStates.size());
		} else {
			subtaskStates.put(subtaskIndex, subtaskState);
		}
	}

	public SubtaskStateV0 getState(int subtaskIndex) {
		if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
			throw new IndexOutOfBoundsException("The given sub task index " + subtaskIndex +
				" exceeds the maximum number of sub tasks " + subtaskStates.size());
		} else {
			return subtaskStates.get(subtaskIndex);
		}
	}

	public Collection<SubtaskStateV0> getStates() {
		return subtaskStates.values();
	}

	public Map<Integer, SubtaskStateV0> getSubtaskStatesById() {
		return subtaskStates;
	}

	public int getNumberCollectedStates() {
		return subtaskStates.size();
	}

	public int getNumberCollectedKvStates() {
		return kvStates.size();
	}

	public int getParallelism() {
		return parallelism;
	}

	public void putKvState(int keyGroupId, KeyGroupStateV0 keyGroupState) {
		kvStates.put(keyGroupId, keyGroupState);
	}

	public KeyGroupStateV0 getKvState(int keyGroupId) {
		return kvStates.get(keyGroupId);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TaskStateV0) {
			TaskStateV0 other = (TaskStateV0) obj;

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
