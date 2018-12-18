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

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CompositeStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class encapsulates state handles to the snapshots of all operator instances executed within one task. A task
 * can run multiple operator instances as a result of operator chaining, and all operator instances from the chain can
 * register their state under their operator id. Each operator instance is a physical execution responsible for
 * processing a partition of the data that goes through a logical operator. This partitioning happens to parallelize
 * execution of logical operators, e.g. distributing a map function.
 *
 * <p>One instance of this class contains the information that one task will send to acknowledge a checkpoint request by
 * the checkpoint coordinator. Tasks run operator instances in parallel, so the union of all
 * {@link TaskStateSnapshot} that are collected by the checkpoint coordinator from all tasks represent the whole
 * state of a job at the time of the checkpoint.
 *
 * <p>This class should be called TaskState once the old class with this name that we keep for backwards
 * compatibility goes away.
 */
public class TaskStateSnapshot implements CompositeStateHandle {

	private static final long serialVersionUID = 1L;

	/**
	 * Mapping from an operator id to the state of one subtask of this operator.
	 */
	private final Map<OperatorID, OperatorSubtaskState> subtaskStatesByOperatorID;

	public TaskStateSnapshot() {
		this(10);
	}

	public TaskStateSnapshot(int size) {
		this(new HashMap<>(size));
	}

	public TaskStateSnapshot(Map<OperatorID, OperatorSubtaskState> subtaskStatesByOperatorID) {
		this.subtaskStatesByOperatorID = Preconditions.checkNotNull(subtaskStatesByOperatorID);
	}

	/**
	 * Returns the subtask state for the given operator id (or null if not contained).
	 */
	@Nullable
	public OperatorSubtaskState getSubtaskStateByOperatorID(OperatorID operatorID) {
		return subtaskStatesByOperatorID.get(operatorID);
	}

	/**
	 * Maps the given operator id to the given subtask state. Returns the subtask state of a previous mapping, if such
	 * a mapping existed or null otherwise.
	 */
	public OperatorSubtaskState putSubtaskStateByOperatorID(
		@Nonnull OperatorID operatorID,
		@Nonnull OperatorSubtaskState state) {

		return subtaskStatesByOperatorID.put(operatorID, Preconditions.checkNotNull(state));
	}

	/**
	 * Returns the set of all mappings from operator id to the corresponding subtask state.
	 */
	public Set<Map.Entry<OperatorID, OperatorSubtaskState>> getSubtaskStateMappings() {
		return subtaskStatesByOperatorID.entrySet();
	}

	/**
	 * Returns true if at least one {@link OperatorSubtaskState} in subtaskStatesByOperatorID has state.
	 */
	public boolean hasState() {
		for (OperatorSubtaskState operatorSubtaskState : subtaskStatesByOperatorID.values()) {
			if (operatorSubtaskState != null && operatorSubtaskState.hasState()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void discardState() throws Exception {
		StateUtil.bestEffortDiscardAllStateObjects(subtaskStatesByOperatorID.values());
	}

	@Override
	public long getStateSize() {
		long size = 0L;

		for (OperatorSubtaskState subtaskState : subtaskStatesByOperatorID.values()) {
			if (subtaskState != null) {
				size += subtaskState.getStateSize();
			}
		}

		return size;
	}

	@Override
	public void registerSharedStates(SharedStateRegistry stateRegistry) {
		for (OperatorSubtaskState operatorSubtaskState : subtaskStatesByOperatorID.values()) {
			if (operatorSubtaskState != null) {
				operatorSubtaskState.registerSharedStates(stateRegistry);
			}
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TaskStateSnapshot that = (TaskStateSnapshot) o;

		return subtaskStatesByOperatorID.equals(that.subtaskStatesByOperatorID);
	}

	@Override
	public int hashCode() {
		return subtaskStatesByOperatorID.hashCode();
	}

	@Override
	public String toString() {
		return "TaskOperatorSubtaskStates{" +
			"subtaskStatesByOperatorID=" + subtaskStatesByOperatorID +
			'}';
	}
}
