/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.profiling.impl;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.OutputGateListener;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;

public class OutputGateListenerImpl implements OutputGateListener {

	private final JobID jobID;

	private final ExecutionVertexID executionVertexID;

	private final int gateIndex;

	private volatile int counter = 0;

	public OutputGateListenerImpl(JobID jobID, ExecutionVertexID executionVertexID, int gateIndex) {
		this.jobID = jobID;
		this.executionVertexID = executionVertexID;
		this.gateIndex = gateIndex;
	}

	@Override
	public void channelCapacityExhausted(int channelIndex) {
		++this.counter;
	}

	public JobID getJobID() {
		return this.jobID;
	}

	public ExecutionVertexID getExecutionVertexID() {
		return this.executionVertexID;
	}

	public int getGateIndex() {
		return this.gateIndex;
	}

	public int getAndResetCounter() {

		final int retval = this.counter;
		this.counter = 0;

		return retval;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void recordEmitted(final Record record) {
		// Nothing to do here
	}
}
