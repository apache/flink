/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.profiling.impl.types;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

public class InternalInputGateProfilingData implements InternalProfilingData {

	private final ExecutionVertexID executionVertexID;

	private final JobID jobId;

	private final int gateIndex;

	private final int profilingInternval;

	private final int noRecordsAvailableCounter;

	/**
	 * Default constructor required by kryo.
	 */
	@SuppressWarnings("unused")
	private InternalInputGateProfilingData() {
		this.jobId = null;
		this.executionVertexID = null;
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
