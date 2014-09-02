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


package org.apache.flink.runtime.profiling.impl.types;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.executiongraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobID;

public class InternalInputGateProfilingData implements InternalProfilingData {

	private final ExecutionVertexID executionVertexID;

	private final JobID jobId;

	private int gateIndex;

	private int profilingInternval;

	private int noRecordsAvailableCounter;

	public InternalInputGateProfilingData() {
		this.jobId = new JobID();
		this.executionVertexID = new ExecutionVertexID();
		this.gateIndex = 0;
		this.profilingInternval = 0;
		this.noRecordsAvailableCounter = 0;
	}

	public InternalInputGateProfilingData(JobID jobID, ExecutionVertexID executionVertexID, int gateIndex,
			int profilingInterval, int noRecordsAvailableCounter) {
		this.jobId = jobID;
		this.executionVertexID = executionVertexID;
		this.gateIndex = gateIndex;
		this.profilingInternval = profilingInterval;
		this.noRecordsAvailableCounter = noRecordsAvailableCounter;
	}

	@Override
	public void read(DataInputView in) throws IOException {

		this.jobId.read(in);
		this.executionVertexID.read(in);
		this.gateIndex = in.readInt();
		this.profilingInternval = in.readInt();
		this.noRecordsAvailableCounter = in.readInt();
	}

	@Override
	public void write(DataOutputView out) throws IOException {

		this.jobId.write(out);
		this.executionVertexID.write(out);
		out.writeInt(this.gateIndex);
		out.writeInt(this.profilingInternval);
		out.writeInt(this.noRecordsAvailableCounter);
	}

	public JobID getJobID() {
		return this.jobId;
	}

	public ExecutionVertexID getExecutionVertexID() {
		return this.executionVertexID;
	}

	public int getGateIndex() {
		return this.gateIndex;
	}

	public int getProfilingInterval() {
		return this.profilingInternval;
	}

	public int getNoRecordsAvailableCounter() {
		return this.noRecordsAvailableCounter;
	}
}
