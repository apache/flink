/**
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

package org.apache.flink.runtime.execution;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex2;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * An attempt to execute a task for a {@link ExecutionVertex2}.
 */
public class ExecutionAttempt implements java.io.Serializable {
	
	private static final long serialVersionUID = 1L;
	

	private final JobVertexID vertexId;
	
	private final int subtaskIndex;
	
	private final ExecutionAttemptID executionId;
	
	private final int attempt;

	// --------------------------------------------------------------------------------------------
	
	public ExecutionAttempt(JobVertexID vertexId, int subtaskIndex, ExecutionAttemptID executionId, int attempt) {
		if (vertexId == null || executionId == null || subtaskIndex < 0 || attempt < 1) {
			throw new IllegalArgumentException();
		}
		
		this.vertexId = vertexId;
		this.subtaskIndex = subtaskIndex;
		this.executionId = executionId;
		this.attempt = attempt;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public JobVertexID getVertexId() {
		return vertexId;
	}
	
	public int getSubtaskIndex() {
		return subtaskIndex;
	}
	
	public ExecutionAttemptID getExecutionId() {
		return executionId;
	}
	
	public int getAttempt() {
		return attempt;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return vertexId.hashCode() +
				executionId.hashCode() +
				31 * subtaskIndex +
				17 * attempt;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ExecutionAttempt) {
			ExecutionAttempt other = (ExecutionAttempt) obj;
			return this.executionId.equals(other.executionId) &&
					this.vertexId.equals(other.vertexId) &&
					this.subtaskIndex == other.subtaskIndex &&
					this.attempt == other.attempt;
		} else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return String.format("ExecutionAttempt (vertex=%s, subtask=%d, executionAttemptId=%s, attempt=%d)",
				vertexId, subtaskIndex, executionId, attempt);
	}
}
