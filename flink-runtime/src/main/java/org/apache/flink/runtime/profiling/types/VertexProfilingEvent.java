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


package org.apache.flink.runtime.profiling.types;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * This interface is a base interface for profiling data which
 * pertains to the execution of tasks.
 */
public abstract class VertexProfilingEvent extends ProfilingEvent {

	private static final long serialVersionUID = -5364961557518174880L;

	private final JobVertexID vertexId;
	
	private int subtask;
	
	private final ExecutionAttemptID executionId;

	private int profilingInterval;

	
	public VertexProfilingEvent(JobVertexID vertexId, int subtask, ExecutionAttemptID executionId,
			int profilingInterval, JobID jobID, long timestamp, long profilingTimestamp)
	{
		super(jobID, timestamp, profilingTimestamp);

		this.vertexId = vertexId;
		this.subtask = subtask;
		this.executionId = executionId;
		this.profilingInterval = profilingInterval;
	}

	public VertexProfilingEvent() {
		super();
		this.vertexId = new JobVertexID();
		this.executionId = new ExecutionAttemptID();
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the ID of the vertex this profiling information
	 * belongs to.
	 * 
	 * @return the ID of the vertex this profiling information belongs to
	 */
	public JobVertexID getVertexID() {
		return this.vertexId;
	}

	/**
	 * The interval in milliseconds to which the rest
	 * of the profiling data relates to.
	 * 
	 * @return the profiling interval given in milliseconds
	 */
	public int getProfilingInterval() {
		return this.profilingInterval;
	}
	
	public int getSubtask() {
		return subtask;
	}
	
	public ExecutionAttemptID getExecutionId() {
		return executionId;
	}

	// --------------------------------------------------------------------------------------------
	//  Serialization
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		this.vertexId.read(in);
		this.executionId.read(in);
		this.subtask = in.readInt();
		this.profilingInterval = in.readInt();
	}


	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		this.vertexId.write(out);
		this.executionId.write(out);
		out.writeInt(subtask);
		out.writeInt(this.profilingInterval);
	}

	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof VertexProfilingEvent) {
			final VertexProfilingEvent other = (VertexProfilingEvent) obj;
			
			return super.equals(other) && this.subtask == other.subtask &&
					this.profilingInterval == other.profilingInterval &&
					this.vertexId.equals(other.vertexId) &&
					this.executionId.equals(other.executionId);
		}
		else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return super.hashCode() ^ vertexId.hashCode() ^ (31*subtask) ^ executionId.hashCode();
	}
}
