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
import org.apache.flink.runtime.util.SerializedValue;

/**
 * Simple bean to describe the state belonging to a parallel operator.
 * Since we hold the state across execution attempts, we identify a task by its
 * JobVertexId and subtask index.
 * 
 * The state itself is kept in serialized from, since the checkpoint coordinator itself
 * is never looking at it anyways and only sends it back out in case of a recovery.
 * Furthermore, the state may involve user-defined classes that are not accessible without
 * the respective classloader.
 */
public class StateForTask {

	/** The state of the parallel operator */
	private final SerializedValue<StateHandle<?>> state;

	/** The vertex id of the parallel operator */
	private final JobVertexID operatorId;
	
	/** The index of the parallel subtask */
	private final int subtask;

	public StateForTask(SerializedValue<StateHandle<?>> state, JobVertexID operatorId, int subtask) {
		if (state == null || operatorId == null || subtask < 0) {
			throw new IllegalArgumentException();
		}
		
		this.state = state;
		this.operatorId = operatorId;
		this.subtask = subtask;
	}

	// --------------------------------------------------------------------------------------------
	
	public SerializedValue<StateHandle<?>> getState() {
		return state;
	}

	public JobVertexID getOperatorId() {
		return operatorId;
	}

	public int getSubtask() {
		return subtask;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		else if (o instanceof StateForTask) {
			StateForTask that = (StateForTask) o;
			return this.subtask == that.subtask && this.operatorId.equals(that.operatorId)
					&& this.state.equals(that.state);
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return state.hashCode() + 31 * operatorId.hashCode() + 43 * subtask;
	}

	@Override
	public String toString() {
		return String.format("StateForTask %s-%d : %s", operatorId, subtask, state);
	}
}
