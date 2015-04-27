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

package org.apache.flink.runtime.profiling.impl.types;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

public abstract class InternalExecutionVertexProfilingData implements InternalProfilingData {

	private final JobID jobId;
	
	private final JobVertexID vertexId;
	
	private int subtask;
	
	private final ExecutionAttemptID executionId;

	
	public InternalExecutionVertexProfilingData() {
		this.jobId = new JobID();
		this.vertexId = new JobVertexID();
		this.executionId = new ExecutionAttemptID();
		this.subtask = -1;
	}

	
	public InternalExecutionVertexProfilingData(JobID jobId, JobVertexID vertexId, int subtask, ExecutionAttemptID executionId) {
		this.jobId = jobId;
		this.vertexId = vertexId;
		this.subtask = subtask;
		this.executionId = executionId;
	}

	// --------------------------------------------------------------------------------------------
	
	public JobID getJobID() {
		return this.jobId;
	}
	
	public JobVertexID getVertexId() {
		return vertexId;
	}
	
	public int getSubtask() {
		return subtask;
	}
	
	public ExecutionAttemptID getExecutionAttemptId() {
		return this.executionId;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void read(DataInputView in) throws IOException {
		this.jobId.read(in);
		this.vertexId.read(in);
		this.executionId.read(in);
		this.subtask = in.readInt();
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		this.jobId.write(out);
		this.vertexId.write(out);
		this.executionId.write(out);
		out.writeInt(this.subtask);
	}
}
