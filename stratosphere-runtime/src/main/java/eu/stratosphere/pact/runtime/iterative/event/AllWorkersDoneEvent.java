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

package eu.stratosphere.pact.runtime.iterative.event;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.services.accumulators.AccumulatorEvent;

/**
 * This event is sent by the JobManager to all worker nodes that take part in an iteration.
 * It contains all Accumulators of the Iteration with there current globally updated value.
 *
 */
public class AllWorkersDoneEvent implements IOReadableWritable {
	
	private AccumulatorEvent accumulators;

	public AllWorkersDoneEvent() {
		super();
	}
	
	public AllWorkersDoneEvent(AccumulatorEvent accumulators) {
		this.accumulators = accumulators;
	}
	
	public Map<String, Accumulator<?, ?>> getAccumulators() {
		return this.accumulators.getAccumulators();
	}
	
	public JobID getJobId() {
		return this.accumulators.getJobID();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.accumulators.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.accumulators = new AccumulatorEvent();
		accumulators.read(in);
	}
	
}
