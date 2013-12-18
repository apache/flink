/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.profiling.impl.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

public abstract class InternalExecutionVertexProfilingData implements InternalProfilingData {

	private final ExecutionVertexID executionVertexID;

	private final JobID jobId;

	public InternalExecutionVertexProfilingData() {
		this.jobId = new JobID();
		this.executionVertexID = new ExecutionVertexID();
	}

	public InternalExecutionVertexProfilingData(JobID jobID, ExecutionVertexID executionVertexID) {
		this.jobId = jobID;
		this.executionVertexID = executionVertexID;
	}

	public ExecutionVertexID getExecutionVertexID() {

		return this.executionVertexID;
	}

	public JobID getJobID() {

		return this.jobId;
	}

	@Override
	public void read(DataInput in) throws IOException {

		this.jobId.read(in);
		this.executionVertexID.read(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {

		this.jobId.write(out);
		this.executionVertexID.write(out);
	}
}
